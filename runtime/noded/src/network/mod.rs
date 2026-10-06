//! Direct-egress network leases, as ucloud_sandboxes/direct_network.py's
//! `DirectNetworkManager.ensure` does them for a create: the same slot state
//! file and flocks (shared with the Python agent), the same names and
//! addresses and the same `iptables` host rules. The netns and veth pair are
//! made over rtnetlink ([`kernel`]), with IPv6 off on both ends, and a
//! background pool keeps pairs ready ([`pool`]). Relay egress policies and
//! DNS-named egress endpoints stay with Python: a create that needs them is
//! forwarded to the agent.

pub mod kernel;
pub mod netlink;
mod pool;
#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet};
use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};

use crate::fsutil::{FileLock, atomic_write, atomic_write_unsynced_directory};
use crate::storage::canonical_json;

pub use kernel::{Kernel, NetlinkKernel, interface_present};
pub use pool::DEFAULT_POOL_SIZE;

pub const STATE_VERSION: u64 = 1;
pub const MAX_SLOTS: u32 = 32767;
pub const MTU: u32 = 1420;
pub const CIDR: &str = "100.96.0.0/16";
const RELAY_FORWARD_MARK: &str = "0x1000000/0x1000000";
const DENIED_DESTINATIONS: [&str; 6] =
    ["10.0.0.0/8", "100.64.0.0/10", "127.0.0.0/8", "169.254.0.0/16", "172.16.0.0/12", "192.168.0.0/16"];

#[derive(Debug)]
pub enum NetworkError {
    /// Python: DirectNetworkError.
    Network(String),
    Io(std::io::Error),
}

impl std::fmt::Display for NetworkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NetworkError::Network(message) => f.write_str(message),
            NetworkError::Io(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for NetworkError {}

impl From<std::io::Error> for NetworkError {
    fn from(error: std::io::Error) -> Self {
        NetworkError::Io(error)
    }
}

fn fail(message: impl Into<String>) -> NetworkError {
    NetworkError::Network(message.into())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lease {
    pub sandbox_id: String,
    pub sandbox_generation: u64,
    pub slot: u32,
    pub namespace: String,
    pub namespace_path: PathBuf,
    pub host_interface: String,
    pub host_ip: Ipv4Addr,
    pub guest_ip: Ipv4Addr,
}

pub fn key(sandbox_id: &str, generation: u64) -> Result<String, NetworkError> {
    if sandbox_id.is_empty() || sandbox_id.contains('\0') {
        return Err(fail("sandbox id is invalid"));
    }
    Ok(format!("{sandbox_id}\0{generation}"))
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes).iter().map(|b| format!("{b:02x}")).collect()
}

/// An IPv4 TCP endpoint the sandboxes may reach (`--direct-network-allow-tcp`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct TcpEgress {
    pub address: Ipv4Addr,
    pub port: u16,
}

impl TcpEgress {
    /// Only literal IPv4 endpoints; DNS names keep the create in Python.
    pub fn parse(value: &str) -> Option<TcpEgress> {
        let (host, port) = value.rsplit_once(':')?;
        Some(TcpEgress { address: host.parse().ok()?, port: port.parse().ok().filter(|p| *p > 0)? })
    }

    fn endpoint(&self) -> String {
        format!("{}:{}", self.address, self.port)
    }
}

pub struct NetworkManager {
    state_path: PathBuf,
    lock_path: PathBuf,
    namespace_root: PathBuf,
    egress: Vec<TcpEgress>,
    relays_configured: bool,
    /// The last host-rule reconciliation's start; creates received before it share it.
    host_rules: Mutex<Option<Instant>>,
    kernel: Arc<dyn Kernel>,
    pool: pool::Pool,
}

impl NetworkManager {
    /// A manager with no pool (`with_pool_size` sets one) over the real kernel.
    pub fn new(state_path: PathBuf, namespace_root: PathBuf, egress: Vec<TcpEgress>, relays_configured: bool) -> Self {
        let kernel = Arc::new(NetlinkKernel::new(namespace_root.clone()));
        Self::with_kernel(state_path, namespace_root, egress, relays_configured, kernel)
    }

    pub fn with_kernel(
        state_path: PathBuf,
        namespace_root: PathBuf,
        egress: Vec<TcpEgress>,
        relays_configured: bool,
        kernel: Arc<dyn Kernel>,
    ) -> Self {
        let lock_path = PathBuf::from(format!("{}.lock", state_path.display()));
        NetworkManager {
            state_path,
            lock_path,
            namespace_root,
            egress,
            relays_configured,
            host_rules: Mutex::new(None),
            kernel,
            pool: pool::Pool::new(0),
        }
    }

    /// Keep `size` configured pairs ready once `start_pool` runs (Python's
    /// `pool_size`, 0..=1024).
    pub fn with_pool_size(mut self, size: usize) -> Result<Self, NetworkError> {
        if size > pool::MAX_POOL_SIZE {
            return Err(fail(format!("direct network pool size must be in 0..={}", pool::MAX_POOL_SIZE)));
        }
        self.pool = pool::Pool::new(size);
        Ok(self)
    }

    /// Tests and benches only: as if host rules were reconciled after every
    /// request, so none touches this machine's firewall.
    #[doc(hidden)]
    pub fn assume_host_rules_current(&self) {
        *self.host_rules.lock().expect("not poisoned") = Some(Instant::now() + std::time::Duration::from_secs(86400 * 365));
    }

    pub fn lease(&self, sandbox_id: &str, generation: u64, slot: u32) -> Result<Lease, NetworkError> {
        if !(1..=MAX_SLOTS).contains(&slot) {
            return Err(fail("direct network state contains an invalid slot"));
        }
        let host = u32::from(Ipv4Addr::new(100, 96, 0, 0)) + slot * 2;
        let digest = sha256_hex(format!("{sandbox_id}\0{generation}").as_bytes());
        let namespace = format!("ucloud-{}", &digest[..20]);
        Ok(Lease {
            sandbox_id: sandbox_id.to_string(),
            sandbox_generation: generation,
            slot,
            namespace_path: self.namespace_root.join(&namespace),
            namespace,
            host_interface: format!("us{slot}h"),
            host_ip: Ipv4Addr::from(host),
            guest_ip: Ipv4Addr::from(host + 1),
        })
    }

    /// Python `_pool_lease`: the slot's addresses and interface, in namespace
    /// `ucloud-pool-<slot>`.
    pub fn pool_lease(&self, slot: u32) -> Lease {
        let name = format!("ucloud-pool-{slot}");
        let host = u32::from(Ipv4Addr::new(100, 96, 0, 0)) + slot * 2;
        Lease {
            sandbox_id: String::new(),
            sandbox_generation: 0,
            slot,
            namespace_path: self.namespace_root.join(&name),
            namespace: name,
            host_interface: format!("us{slot}h"),
            host_ip: Ipv4Addr::from(host),
            guest_ip: Ipv4Addr::from(host + 1),
        }
    }

    fn lease_lock_path(&self, key: &str) -> PathBuf {
        let directory = PathBuf::from(format!("{}.leases", self.lock_path.display()));
        directory.join(format!("{}.lock", sha256_hex(key.as_bytes())))
    }

    fn locked(path: &Path) -> Result<FileLock, NetworkError> {
        if let Some(parent) = path.parent() {
            use std::os::unix::fs::DirBuilderExt;
            std::fs::DirBuilder::new().recursive(true).mode(0o700).create(parent)?;
        }
        Ok(FileLock::acquire(path, false)?)
    }

    /// Python `_load`: validated, with leased slots dropped from the pool.
    fn load(&self) -> Result<Map<String, Value>, NetworkError> {
        let text = match std::fs::read_to_string(&self.state_path) {
            Ok(text) => text,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let Value::Object(state) = json!({"leases": {}, "pool": [], "version": STATE_VERSION}) else { unreachable!() };
                return Ok(state);
            }
            Err(error) => return Err(error.into()),
        };
        let invalid = || fail("direct network state is invalid");
        let Value::Object(mut state) = serde_json::from_str(&text).map_err(|_| invalid())? else { return Err(invalid()) };
        let leases = match state.get("leases") {
            Some(Value::Object(leases)) if state.get("version") == Some(&json!(STATE_VERSION)) => leases.clone(),
            _ => return Err(invalid()),
        };
        let mut slots = BTreeSet::new();
        for value in leases.values() {
            let slot = value.as_u64().ok_or_else(invalid)?;
            if !slots.insert(slot) {
                return Err(fail("direct network state double-allocates a slot"));
            }
        }
        if let Some(policies) = state.get("policies") {
            let Value::Object(policies) = policies else { return Err(fail("direct network policy state is invalid")) };
            if policies.keys().any(|key| !leases.contains_key(key)) {
                return Err(fail("direct network policy state is invalid"));
            }
        }
        let pool = match state.get("pool") {
            None => Vec::new(),
            Some(Value::Array(items)) => items.iter().map(|item| item.as_u64().filter(|s| (1..=MAX_SLOTS as u64).contains(s)))
                .collect::<Option<Vec<u64>>>().ok_or_else(|| fail("direct network pool state is invalid"))?,
            _ => return Err(fail("direct network pool state is invalid")),
        };
        let unique: BTreeSet<u64> = pool.iter().copied().collect();
        if unique.len() != pool.len() {
            return Err(fail("direct network pool state is invalid"));
        }
        state.insert("pool".into(), json!(unique.difference(&slots).copied().collect::<Vec<u64>>()));
        Ok(state)
    }

    /// Python `_write_durably`: sort_keys, compact, ensure_ascii, then "\n".
    /// Pool-only writes skip the directory fsync, as Python's do: after an OS
    /// crash the name holds a complete later write or the last synced one,
    /// with the same leases and policies, and pooled pairs died with the
    /// kernel (the pool rechecks them).
    fn store(&self, state: &Map<String, Value>, pool_only: bool) -> Result<(), NetworkError> {
        let mut bytes = canonical_json(&Value::Object(state.clone()));
        bytes.push(b'\n');
        if let Some(parent) = self.state_path.parent() {
            use std::os::unix::fs::DirBuilderExt;
            std::fs::DirBuilder::new().recursive(true).mode(0o700).create(parent)?;
        }
        if pool_only {
            Ok(atomic_write_unsynced_directory(&self.state_path, &bytes)?)
        } else {
            Ok(atomic_write(&self.state_path, &bytes)?)
        }
    }

    /// Allocate (or reuse) this incarnation's direct-egress lease and make its
    /// netns and veth pair, from the pool when it has one ready. Blocking:
    /// call from a blocking thread.
    pub fn ensure_direct(&self, sandbox_id: &str, generation: u64) -> Result<Lease, NetworkError> {
        let requested_at = Instant::now();
        let key = key(sandbox_id, generation)?;
        let _incarnation = Self::locked(&self.lease_lock_path(&key))?;
        let _foreground = self.pool.foreground();
        let (lease, pooled) = {
            let _state_lock = Self::locked(&self.lock_path)?;
            let mut state = self.load()?;
            let leases = state["leases"].as_object().cloned().unwrap_or_default();
            if state.get("policies").and_then(|p| p.get(&key)).is_some() {
                return Err(fail("network policy is immutable for a sandbox generation"));
            }
            let (slot, pooled) = match leases.get(&key).and_then(Value::as_u64) {
                Some(slot) => (slot as u32, false),
                None => {
                    let pooled = self.pool.claim(&mut state);
                    let slot = match pooled {
                        Some(slot) => slot,
                        None => {
                            let mut used: BTreeSet<u64> = leases.values().filter_map(Value::as_u64).collect();
                            used.extend(pool_slots(&state));
                            (1..=MAX_SLOTS).find(|slot| !used.contains(&u64::from(*slot)))
                                .ok_or_else(|| fail("direct network slot capacity is exhausted"))?
                        }
                    };
                    // One durable write moves a pooled slot to this lease.
                    let mut leases = leases;
                    leases.insert(key.clone(), json!(slot));
                    state.insert("leases".into(), Value::Object(leases));
                    self.store(&state, false)?;
                    (slot, pooled.is_some())
                }
            };
            let lease = self.lease(sandbox_id, generation, slot)?;
            self.ensure_host_rules_since(requested_at)?;
            (lease, pooled)
        };
        // A pair the pool configured needs only its name; any other lease,
        // and a hand-off a crash interrupted, is checked and repaired.
        let adopted = self.adopt_pooled(&lease);
        if !(pooled && adopted && self.kernel.interface_present(&lease.host_interface)) {
            self.ensure_kernel_lease(&lease)?;
        }
        Ok(lease)
    }

    /// Reconcile host rules unless one started after this request arrived.
    fn ensure_host_rules_since(&self, requested_at: Instant) -> Result<(), NetworkError> {
        let mut observed = self.host_rules.lock().expect("not poisoned");
        if observed.is_some_and(|at| at >= requested_at) {
            return Ok(());
        }
        let started = Instant::now();
        *observed = None;
        self.ensure_host_rules()?;
        *observed = Some(started);
        Ok(())
    }

    /// Python `_ensure_host_rules`, for literal IPv4 egress endpoints.
    pub fn ensure_host_rules(&self) -> Result<(), NetworkError> {
        let snapshot = iptables_snapshot();
        run(&["sysctl", "-q", "-w", "net.ipv4.ip_forward=1"])?;
        ensure_iptables(&["iptables", "-C", "INPUT", "-s", CIDR, "-j", "DROP"],
                        &["iptables", "-I", "INPUT", "1", "-s", CIDR, "-j", "DROP"], snapshot.as_ref())?;
        let mut reordered = false;
        for destination in DENIED_DESTINATIONS {
            reordered |= ensure_iptables(
                &["iptables", "-C", "FORWARD", "-s", CIDR, "-d", destination, "-j", "DROP"],
                &["iptables", "-I", "FORWARD", "1", "-s", CIDR, "-d", destination, "-j", "DROP"],
                snapshot.as_ref(),
            )?;
        }
        self.reconcile_tcp_egress(snapshot.as_ref())?;
        if self.relays_configured {
            let rule = ["FORWARD", "-s", CIDR, "-m", "mark", "--mark", RELAY_FORWARD_MARK, "-j", "ACCEPT"];
            let mut snapshot = snapshot.as_ref();
            if reordered {
                // A DROP was just inserted above it; restore precedence.
                best_effort(&[&["iptables", "-D"][..], &rule[..]].concat());
                snapshot = None;
            }
            ensure_iptables(&[&["iptables", "-C"][..], &rule[..]].concat(),
                            &[&["iptables", "-I", "FORWARD", "1"][..], &rule[1..]].concat(), snapshot)?;
        }
        ensure_iptables(&["iptables", "-C", "FORWARD", "-s", CIDR, "-j", "ACCEPT"],
                        &["iptables", "-A", "FORWARD", "-s", CIDR, "-j", "ACCEPT"], snapshot.as_ref())?;
        let established = ["-d", CIDR, "-m", "conntrack", "--ctstate", "RELATED,ESTABLISHED", "-j", "ACCEPT"];
        ensure_iptables(&[&["iptables", "-C", "FORWARD"][..], &established[..]].concat(),
                        &[&["iptables", "-A", "FORWARD"][..], &established[..]].concat(), snapshot.as_ref())?;
        ensure_iptables(&["iptables", "-t", "nat", "-C", "POSTROUTING", "-s", CIDR, "-j", "MASQUERADE"],
                        &["iptables", "-t", "nat", "-A", "POSTROUTING", "-s", CIDR, "-j", "MASQUERADE"], snapshot.as_ref())?;
        Ok(())
    }

    fn egress_path(&self) -> PathBuf {
        PathBuf::from(format!("{}.egress.json", self.state_path.display()))
    }

    fn reconcile_tcp_egress(&self, snapshot: Option<&BTreeSet<Vec<String>>>) -> Result<(), NetworkError> {
        let previous: BTreeMap<String, Vec<String>> = std::fs::read_to_string(self.egress_path()).ok()
            .and_then(|text| serde_json::from_str::<Value>(&text).ok())
            .and_then(|value| value.get("endpoints").cloned())
            .and_then(|endpoints| serde_json::from_value(endpoints).ok())
            .unwrap_or_default();
        let resolved: BTreeMap<String, Vec<String>> =
            self.egress.iter().map(|e| (e.endpoint(), vec![e.address.to_string()])).collect();
        let rules = |map: &BTreeMap<String, Vec<String>>| -> BTreeSet<(String, u16)> {
            map.iter().filter_map(|(endpoint, addresses)| {
                let port: u16 = endpoint.rsplit_once(':')?.1.parse().ok()?;
                Some(addresses.iter().map(move |a| (a.clone(), port)))
            }).flatten().collect()
        };
        let (old, new) = (rules(&previous), rules(&resolved));
        for (address, port) in &new {
            let rule = tcp_egress_rule(address, *port);
            ensure_iptables(&[&["iptables", "-C", "FORWARD"][..], &rule.iter().map(String::as_str).collect::<Vec<_>>()[..]].concat(),
                            &[&["iptables", "-I", "FORWARD", "1"][..], &rule.iter().map(String::as_str).collect::<Vec<_>>()[..]].concat(),
                            snapshot)?;
        }
        for (address, port) in old.difference(&new) {
            let rule = tcp_egress_rule(address, *port);
            best_effort(&[&["iptables", "-D", "FORWARD"][..], &rule.iter().map(String::as_str).collect::<Vec<_>>()[..]].concat());
        }
        if resolved != previous {
            let mut bytes = canonical_json(&json!({"endpoints": resolved, "version": 1}));
            bytes.push(b'\n');
            atomic_write(&self.egress_path(), &bytes)?;
        }
        Ok(())
    }

    /// Python `_ensure_kernel_lease`: keep a complete pair (configuration is
    /// idempotent), else remove any part and create it afresh.
    fn ensure_kernel_lease(&self, lease: &Lease) -> Result<(), NetworkError> {
        let namespace_exists = lease.namespace_path.exists();
        let interface_exists = self.kernel.interface_present(&lease.host_interface);
        if namespace_exists && interface_exists && self.kernel.configure_pair(lease).is_ok() {
            // runsc consumes the external netns wiring at checkpoint; a partial
            // pair fails configuration and is recreated below.
            return Ok(());
        }
        if namespace_exists || interface_exists {
            self.cleanup_kernel_lease(lease);
        }
        let created = self.kernel.create_pair(lease);
        if created.is_err() {
            self.cleanup_kernel_lease(lease);
        }
        created
    }

    /// Python `_cleanup_kernel_lease`, best effort.
    fn cleanup_kernel_lease(&self, lease: &Lease) {
        // The link first: dropping a namespace frees its veth asynchronously,
        // which could race a recreation of this name.
        if let Err(error) = self.kernel.delete_link(&lease.host_interface) {
            eprintln!("ucloud-noded: {error}");
        }
        if let Err(error) = self.kernel.detach_namespace(&lease.namespace_path) {
            eprintln!("ucloud-noded: cannot delete namespace {}: {error}", lease.namespace);
        }
        let pooled = self.pool_lease(lease.slot).namespace_path;
        if pooled != lease.namespace_path && pooled.exists() {
            // Left by a crash after this lease took the slot from the pool.
            if let Err(error) = self.kernel.detach_namespace(&pooled) {
                eprintln!("ucloud-noded: cannot delete namespace {}: {error}", pooled.display());
            }
        }
    }

    /// Python `_adopt_pooled`: name the slot's pooled namespace for `lease`,
    /// then drop the pool's name. The pool's name is dropped only once the
    /// namespace has another, so cleanup still deletes the link before the
    /// namespace. Whether this call attached it.
    fn adopt_pooled(&self, lease: &Lease) -> bool {
        let source = self.pool_lease(lease.slot).namespace_path;
        if !source.exists() {
            return false;
        }
        let adopted = !lease.namespace_path.exists();
        if adopted {
            if let Err(error) = self.kernel.attach_namespace(&source, &lease.namespace_path) {
                eprintln!("ucloud-noded: cannot attach pooled namespace {}: {error}", source.display());
                return false;
            }
        } else if !same_file(&source, &lease.namespace_path) {
            return false;
        }
        if let Err(error) = self.kernel.detach_namespace(&source) {
            eprintln!("ucloud-noded: cannot drop pooled name {}: {error}", source.display());
        }
        adopted
    }
}

fn same_file(a: &Path, b: &Path) -> bool {
    use std::os::unix::fs::MetadataExt;
    match (std::fs::metadata(a), std::fs::metadata(b)) {
        (Ok(a), Ok(b)) => (a.dev(), a.ino()) == (b.dev(), b.ino()),
        _ => false,
    }
}

/// The durable pool's slots (validated by `load`).
fn pool_slots(state: &Map<String, Value>) -> Vec<u64> {
    state.get("pool").and_then(Value::as_array).into_iter().flatten().filter_map(Value::as_u64).collect()
}

fn tcp_egress_rule(address: &str, port: u16) -> Vec<String> {
    ["-s", CIDR, "-d", &format!("{address}/32"), "-p", "tcp", "--dport", &port.to_string(), "-j", "ACCEPT"]
        .iter().map(|s| s.to_string()).collect()
}

fn run(argv: &[&str]) -> Result<(), NetworkError> {
    let output = Command::new(argv[0]).args(&argv[1..]).stdin(Stdio::null()).output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        let detail = if stderr.trim().is_empty() { stdout.trim().to_string() } else { stderr.trim().to_string() };
        return Err(fail(format!("direct network command failed ({}): {}: {detail}",
                                output.status.code().unwrap_or(-1), argv.join(" "))));
    }
    Ok(())
}

fn command_ok(argv: &[&str]) -> bool {
    Command::new(argv[0]).args(&argv[1..]).stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::null())
        .status().map(|s| s.success()).unwrap_or(false)
}

fn best_effort(argv: &[&str]) {
    let _ = command_ok(argv);
}

/// Install a missing rule; whether it was installed.
fn ensure_iptables(check: &[&str], install: &[&str], snapshot: Option<&BTreeSet<Vec<String>>>) -> Result<bool, NetworkError> {
    if snapshot.is_some_and(|rules| rules.contains(&rule_key(check))) || command_ok(check) {
        return Ok(false);
    }
    run(install)?;
    Ok(true)
}

/// Python `_iptables_rule_key`: (table, "-A", chain, args...) with `-m tcp` after `-p tcp` dropped.
pub fn rule_key(command: &[&str]) -> Vec<String> {
    let mut words: Vec<String> = command.iter().map(|s| s.to_string()).collect();
    if words.first().map(String::as_str) == Some("iptables") {
        words.remove(0);
    }
    let mut table = "filter".to_string();
    if words.first().map(String::as_str) == Some("-t") && words.len() >= 2 {
        table = words[1].clone();
        words.drain(..2);
    }
    if matches!(words.first().map(String::as_str), Some("-C" | "-A")) {
        words[0] = "-A".into();
    }
    if let Some(p) = words.iter().position(|w| w == "-p") {
        if words.get(p + 1).map(String::as_str) == Some("tcp") {
            if let Some(i) = (0..words.len().saturating_sub(1)).find(|&i| words[i] == "-m" && words[i + 1] == "tcp") {
                words.drain(i..i + 2);
            }
        }
    }
    std::iter::once(table).chain(words).collect()
}

/// `iptables-save` parsed into rule keys; None when it cannot be trusted.
fn iptables_snapshot() -> Option<BTreeSet<Vec<String>>> {
    let output = Command::new("iptables-save").stdin(Stdio::null()).stderr(Stdio::null()).output().ok()?;
    if !output.status.success() {
        return None;
    }
    parse_iptables_save(&String::from_utf8(output.stdout).ok()?)
}

pub fn parse_iptables_save(text: &str) -> Option<BTreeSet<Vec<String>>> {
    let mut rules = BTreeSet::new();
    let mut table: Option<String> = None;
    for line in text.lines() {
        if let Some(name) = line.strip_prefix('*') {
            if table.is_some() {
                return None;
            }
            table = Some(name.to_string());
        } else if line == "COMMIT" {
            table.take()?;
        } else if line.starts_with("-A ") {
            let current = table.as_ref()?;
            let words = shlex::split(line)?;
            let mut command: Vec<&str> = vec!["-t", current];
            command.extend(words.iter().map(String::as_str));
            rules.insert(rule_key(&command));
        }
    }
    if table.is_some() { None } else { Some(rules) }
}
