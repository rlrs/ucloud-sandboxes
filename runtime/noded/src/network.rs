//! Direct-egress network leases, as ucloud_sandboxes/direct_network.py's
//! `DirectNetworkManager.ensure` does them for a create: the same slot state
//! file and flocks (shared with the Python agent), the same names, addresses
//! and `ip`/`iptables` commands. Relay egress policies, DNS-named egress
//! endpoints and the pre-created pool stay with Python in phase 1: a create
//! that needs them is forwarded to the agent.

use std::collections::{BTreeMap, BTreeSet};
use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Mutex;
use std::time::Instant;

use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};

use crate::fsutil::{FileLock, atomic_write};
use crate::storage::canonical_json;

pub const STATE_VERSION: u64 = 1;
pub const MAX_SLOTS: u32 = 32767;
pub const MTU: u32 = 1420;
pub const CIDR: &str = "100.96.0.0/16";
const RELAY_FORWARD_MARK: &str = "0x1000000/0x1000000";
const DENIED_DESTINATIONS: [&str; 6] =
    ["10.0.0.0/8", "100.64.0.0/10", "127.0.0.0/8", "169.254.0.0/16", "172.16.0.0/12", "192.168.0.0/16"];
const ONE_QUEUE: [&str; 4] = ["numtxqueues", "1", "numrxqueues", "1"];

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
}

impl NetworkManager {
    pub fn new(state_path: PathBuf, namespace_root: PathBuf, egress: Vec<TcpEgress>, relays_configured: bool) -> Self {
        let lock_path = PathBuf::from(format!("{}.lock", state_path.display()));
        NetworkManager { state_path, lock_path, namespace_root, egress, relays_configured, host_rules: Mutex::new(None) }
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
    fn store(&self, state: &Map<String, Value>) -> Result<(), NetworkError> {
        let mut bytes = canonical_json(&Value::Object(state.clone()));
        bytes.push(b'\n');
        if let Some(parent) = self.state_path.parent() {
            use std::os::unix::fs::DirBuilderExt;
            std::fs::DirBuilder::new().recursive(true).mode(0o700).create(parent)?;
        }
        Ok(atomic_write(&self.state_path, &bytes)?)
    }

    /// Allocate (or reuse) this incarnation's direct-egress lease and make its
    /// netns and veth pair. Blocking: call from a blocking thread.
    pub fn ensure_direct(&self, sandbox_id: &str, generation: u64) -> Result<Lease, NetworkError> {
        let requested_at = Instant::now();
        let key = key(sandbox_id, generation)?;
        let _incarnation = Self::locked(&self.lease_lock_path(&key))?;
        let lease = {
            let _state_lock = Self::locked(&self.lock_path)?;
            let mut state = self.load()?;
            let leases = state["leases"].as_object().cloned().unwrap_or_default();
            if state.get("policies").and_then(|p| p.get(&key)).is_some() {
                return Err(fail("network policy is immutable for a sandbox generation"));
            }
            let slot = match leases.get(&key).and_then(Value::as_u64) {
                Some(slot) => slot as u32,
                None => {
                    let mut used: BTreeSet<u64> = leases.values().filter_map(Value::as_u64).collect();
                    used.extend(state["pool"].as_array().into_iter().flatten().filter_map(Value::as_u64));
                    let slot = (1..=MAX_SLOTS as u64).find(|slot| !used.contains(slot))
                        .ok_or_else(|| fail("direct network slot capacity is exhausted"))?;
                    let mut leases = leases;
                    leases.insert(key.clone(), json!(slot));
                    state.insert("leases".into(), Value::Object(leases));
                    self.store(&state)?;
                    slot as u32
                }
            };
            let lease = self.lease(sandbox_id, generation, slot)?;
            self.ensure_host_rules_since(requested_at)?;
            lease
        };
        self.ensure_kernel_lease(&lease)?;
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

    fn ensure_kernel_lease(&self, lease: &Lease) -> Result<(), NetworkError> {
        let namespace_exists = lease.namespace_path.exists();
        let interface_exists = interface_present(&lease.host_interface);
        if namespace_exists && interface_exists && self.configure_kernel_lease(lease).is_ok() {
            // runsc consumes the external netns wiring at checkpoint; a partial
            // pair fails configuration and is recreated below.
            return Ok(());
        }
        if namespace_exists || interface_exists {
            self.cleanup_kernel_lease(lease);
        }
        {
            use std::os::unix::fs::DirBuilderExt;
            std::fs::DirBuilder::new().recursive(true).mode(0o755).create(&self.namespace_root)?;
        }
        let created = (|| {
            run(&["ip", "netns", "add", &lease.namespace])?;
            run(&[&["ip", "link", "add", &lease.host_interface][..], &ONE_QUEUE[..], &["type", "veth", "peer", "name", "eth0"][..],
                  &ONE_QUEUE[..], &["netns", &lease.namespace][..]].concat())?;
            self.configure_kernel_lease(lease)
        })();
        if created.is_err() {
            self.cleanup_kernel_lease(lease);
        }
        created
    }

    fn configure_kernel_lease(&self, lease: &Lease) -> Result<(), NetworkError> {
        ip_batch(&["ip", "-batch", "-"], &format!(
            "link set dev {iface} mtu {MTU} up\naddress replace {host}/31 dev {iface}\n",
            iface = lease.host_interface, host = lease.host_ip))?;
        ip_batch(&["ip", "-n", &lease.namespace, "-batch", "-"], &format!(
            "link set lo up\nlink set dev eth0 mtu {MTU} up\naddress replace {guest}/31 dev eth0\nroute replace default via {host} dev eth0\n",
            guest = lease.guest_ip, host = lease.host_ip))
    }

    fn cleanup_kernel_lease(&self, lease: &Lease) {
        // The link first: dropping a namespace frees its veth asynchronously.
        best_effort(&["ip", "link", "delete", &lease.host_interface]);
        best_effort(&["ip", "netns", "delete", &lease.namespace]);
    }
}

fn tcp_egress_rule(address: &str, port: u16) -> Vec<String> {
    ["-s", CIDR, "-d", &format!("{address}/32"), "-p", "tcp", "--dport", &port.to_string(), "-j", "ACCEPT"]
        .iter().map(|s| s.to_string()).collect()
}

/// Python tests presence with sysfs, never if_nametoindex (which modprobes).
pub fn interface_present(name: &str) -> bool {
    !name.contains('/') && std::fs::symlink_metadata(format!("/sys/class/net/{name}")).is_ok()
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

fn ip_batch(argv: &[&str], commands: &str) -> Result<(), NetworkError> {
    use std::io::Write;
    let mut child = Command::new(argv[0]).args(&argv[1..]).stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped()).spawn()?;
    child.stdin.take().expect("piped").write_all(commands.as_bytes())?;
    let output = child.wait_with_output()?;
    if !output.status.success() {
        return Err(fail(format!("ip batch failed: {}", String::from_utf8_lossy(&output.stderr).trim())));
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

#[cfg(test)]
mod tests {
    use super::*;

    fn manager(root: &Path) -> NetworkManager {
        NetworkManager::new(root.join("network-slots.json"), root.join("netns"), vec![], true)
    }

    #[test]
    fn names_and_addresses_match_python() {
        let m = manager(Path::new("/tmp/x"));
        let lease = m.lease("sb-1", 1, 1).unwrap();
        assert_eq!((lease.host_ip.to_string(), lease.guest_ip.to_string()), ("100.96.0.2".into(), "100.96.0.3".into()));
        assert_eq!(lease.namespace, "ucloud-3019d619ced61cc29ced");
        assert_eq!(lease.host_interface, "us1h");
        let last = m.lease("sb", 1, MAX_SLOTS).unwrap();
        assert_eq!((last.host_ip.to_string(), last.guest_ip.to_string()), ("100.96.255.254".into(), "100.96.255.255".into()));
        assert!(m.lease("sb", 1, 0).is_err());
    }

    #[test]
    fn state_round_trips_in_python_format() {
        let root = std::env::temp_dir().join(format!("noded-net-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let m = manager(&root);
        // Python wrote this: a leased slot in the pool is dropped on load; policies are kept.
        std::fs::write(root.join("network-slots.json"),
            "{\"leases\":{\"a\\u00001\":3},\"policies\":{},\"pool\":[3,5],\"version\":1}\n").unwrap();
        let mut state = m.load().unwrap();
        assert_eq!(state["pool"], json!([5]));
        state["leases"].as_object_mut().unwrap().insert("b\u{0}2".into(), json!(1));
        m.store(&state).unwrap();
        assert_eq!(std::fs::read_to_string(root.join("network-slots.json")).unwrap(),
                   "{\"leases\":{\"a\\u00001\":3,\"b\\u00002\":1},\"policies\":{},\"pool\":[5],\"version\":1}\n");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn iptables_save_keys_match_install_checks() {
        let saved = "*filter\n:INPUT ACCEPT [0:0]\n-A INPUT -s 100.96.0.0/16 -j DROP\n\
            -A FORWARD -s 100.96.0.0/16 -d 10.36.101.16/32 -p tcp -m tcp --dport 8092 -j ACCEPT\nCOMMIT\n\
            *nat\n-A POSTROUTING -s 100.96.0.0/16 -j MASQUERADE\nCOMMIT\n";
        let rules = parse_iptables_save(saved).unwrap();
        assert!(rules.contains(&rule_key(&["iptables", "-C", "INPUT", "-s", CIDR, "-j", "DROP"])));
        let egress = tcp_egress_rule("10.36.101.16", 8092);
        let check: Vec<&str> = ["iptables", "-C", "FORWARD"].into_iter().chain(egress.iter().map(String::as_str)).collect();
        assert!(rules.contains(&rule_key(&check)));
        assert!(rules.contains(&rule_key(&["iptables", "-t", "nat", "-C", "POSTROUTING", "-s", CIDR, "-j", "MASQUERADE"])));
        assert!(parse_iptables_save("*filter\n-A INPUT -j DROP\n").is_none());
    }

    #[test]
    fn egress_endpoints_are_literal_ipv4_only() {
        assert_eq!(TcpEgress::parse("10.36.101.16:8092"), Some(TcpEgress { address: Ipv4Addr::new(10, 36, 101, 16), port: 8092 }));
        assert_eq!(TcpEgress::parse("relay.example:8092"), None);
    }
}
