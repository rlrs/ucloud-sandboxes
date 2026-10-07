//! `SandboxSpec` (ucloud_sandboxes/sandbox.py) as the registry stores it.
//!
//! `from_dict` ports Python's `SandboxSpec.from_dict` (defaults, profile
//! merges, exact JSON types) and keeps the result's `to_dict()` and its
//! canonical encoding, so a stored spec is valid exactly when re-encoding it
//! reproduces its bytes. Equality is equality of that encoding, which equals
//! Python's dataclass equality: `to_dict` omits a key only at its default.
//!
//! `validate` ports `SandboxSpec.validate` except the environment catalog check
//! (`environment_contract.validate_requirements`), which only the node API can
//! make. JSON integers must fit an i64 (SQLite's integer); Python's are
//! unbounded. A lone UTF-16 surrogate, which Python's `json` accepts, cannot
//! be a Rust string: such a spec or stored record is refused, never rewritten.

use std::collections::BTreeMap;

use serde_json::{Map, Number, Value};

use crate::pyjson;

pub const DEFAULT_SANDBOX_USER: &str = "1000:1000";
pub const DEFAULT_PIDS_LIMIT: i64 = 256;
pub const RESERVED_LABEL_PREFIX: &str = "ucloud-sandboxes.";
pub const DEFAULT_LINUX_HOST_WRITABLE_PATHS: [&str; 17] = [
    "/run",
    "/run/lock",
    "/run/sshd",
    "/tmp",
    "/var/tmp",
    "/var/run",
    "/var/lock",
    "/var/spool/cron",
    "/var/spool/cron/crontabs",
    "/etc/cron.d",
    "/logs",
    "/logs/agent",
    "/logs/verifier",
    "/tests",
    "/task",
    "/oracle",
    "/workspace",
];
/// `hibernation.HIBERNATION_ALLOCATOR_CHUNK_MB` and `HIBERNATION_FIXED_OVERHEAD_MB`.
const ALLOCATOR_CHUNK_MB: i64 = 1024;
const FIXED_OVERHEAD_MB: i64 = 64;

type Raw = Map<String, Value>;
type SpecResult<T> = Result<T, String>;

#[derive(Clone, Debug, PartialEq)]
struct Security {
    user: Option<String>,
    cap_drop: Vec<String>,
    cap_add: Vec<String>,
    supplementary_groups: Vec<String>,
    no_new_privileges: bool,
    pids_limit: Option<i64>,
    read_only_rootfs: bool,
    init: bool,
}

#[derive(Clone, Debug, PartialEq)]
struct Filesystem {
    enforce_disk_quota: bool,
    workspace_path: String,
    tmpfs_mb: i64,
    run_tmpfs_mb: i64,
    shm_mb: i64,
    workspace_storage: Option<String>,
    management_helper: String,
}

#[derive(Clone, Debug, PartialEq)]
struct LinuxHost {
    enable_cron: bool,
    enable_sshd: bool,
    keep_alive: bool,
    writable_paths: Vec<String>,
}

#[derive(Clone, Debug, PartialEq)]
struct Ssh {
    enabled: bool,
    user: String,
    host: String,
    host_port: Option<i64>,
    container_port: i64,
    authorized_keys: Vec<String>,
}

/// `SandboxNetworkPolicy`: `relay` is set exactly when egress is relay.
#[derive(Clone, Debug, PartialEq)]
struct NetworkPolicy {
    relay: Option<String>,
}

#[derive(Clone, Debug)]
pub struct SandboxSpec {
    id: String,
    image: String,
    profile: String,
    required_features: Vec<String>,
    command: Vec<String>,
    env: BTreeMap<String, String>,
    working_dir: Option<String>,
    memory_mb: Option<i64>,
    cpus: Option<f64>,
    disk_mb: Option<i64>,
    network: String,
    dns_servers: Vec<String>,
    network_policy: NetworkPolicy,
    ttl_seconds: Option<i64>,
    parkable: bool,
    managed_process: bool,
    ssh: Ssh,
    security: Security,
    filesystem: Filesystem,
    linux_host: LinuxHost,
    labels: BTreeMap<String, String>,
    environment_root: Option<String>,
    /// Toolkits the gateway pinned (`name@sha256:<root>`); omitted when empty.
    toolkits: Vec<String>,
    /// `to_dict()` and its canonical JSON, fixed at construction.
    dict: Value,
    encoded: String,
}

impl PartialEq for SandboxSpec {
    fn eq(&self, other: &Self) -> bool {
        self.encoded == other.encoded
    }
}

impl Eq for SandboxSpec {}

fn field<'a>(raw: &'a Raw, key: &str) -> Option<&'a Value> {
    raw.get(key)
}

/// `raw.get(key) is not None`.
fn present<'a>(raw: &'a Raw, key: &str) -> Option<&'a Value> {
    raw.get(key).filter(|value| !value.is_null())
}

fn json_object<'a>(raw: &'a Value, name: &str, fields: &[&str]) -> SpecResult<&'a Raw> {
    let Value::Object(map) = raw else {
        return Err(format!("{name} must be a JSON object"));
    };
    let mut unsupported: Vec<&str> = map.keys().map(String::as_str).filter(|key| !fields.contains(key)).collect();
    unsupported.sort_unstable();
    if !unsupported.is_empty() {
        return Err(format!("unsupported {name} fields: {}", unsupported.join(", ")));
    }
    Ok(map)
}

fn json_bool(raw: &Value, name: &str) -> SpecResult<bool> {
    raw.as_bool().ok_or_else(|| format!("{name} must be a boolean"))
}

fn json_int(raw: &Value, name: &str) -> SpecResult<i64> {
    raw.as_i64().ok_or_else(|| format!("{name} must be an integer"))
}

fn json_optional_int(raw: Option<&Value>, name: &str) -> SpecResult<Option<i64>> {
    match raw {
        None | Some(Value::Null) => Ok(None),
        Some(value) => json_int(value, name).map(Some),
    }
}

fn json_number(raw: &Value, name: &str) -> SpecResult<f64> {
    match raw {
        Value::Number(number) => number.as_f64().ok_or_else(|| format!("{name} must be a number")),
        _ => Err(format!("{name} must be a number")),
    }
}

fn json_string(raw: &Value, name: &str) -> SpecResult<String> {
    raw.as_str().map(str::to_owned).ok_or_else(|| format!("{name} must be a string"))
}

fn json_string_list(raw: &Value, name: &str) -> SpecResult<Vec<String>> {
    let error = || format!("{name} must be a list of strings");
    raw.as_array().ok_or_else(error)?.iter().map(|item| item.as_str().map(str::to_owned).ok_or_else(error)).collect()
}

fn json_string_map(raw: &Value, name: &str) -> SpecResult<BTreeMap<String, String>> {
    let error = || format!("{name} must be an object of string values");
    raw.as_object()
        .ok_or_else(error)?
        .iter()
        .map(|(key, value)| Ok((key.clone(), value.as_str().ok_or_else(error)?.to_owned())))
        .collect()
}

fn strings(items: &[String]) -> Value {
    Value::Array(items.iter().cloned().map(Value::String).collect())
}

fn optional_string(value: &Option<String>) -> Value {
    value.clone().map_or(Value::Null, Value::String)
}

fn optional_int(value: Option<i64>) -> Value {
    value.map_or(Value::Null, Value::from)
}

fn string_map(map: &BTreeMap<String, String>) -> Value {
    Value::Object(map.iter().map(|(k, v)| (k.clone(), Value::String(v.clone()))).collect())
}

impl Security {
    fn linux_host_default() -> Security {
        Security {
            user: None,
            cap_drop: vec![],
            cap_add: vec![],
            supplementary_groups: vec![],
            no_new_privileges: false,
            pids_limit: None,
            read_only_rootfs: false,
            init: true,
        }
    }

    fn from_dict(raw: Option<&Value>) -> SpecResult<Security> {
        let Some(raw) = raw.filter(|raw| !raw.is_null()) else {
            return Ok(Security {
                user: Some(DEFAULT_SANDBOX_USER.into()),
                cap_drop: vec!["ALL".into()],
                cap_add: vec![],
                supplementary_groups: vec![],
                no_new_privileges: true,
                pids_limit: Some(DEFAULT_PIDS_LIMIT),
                read_only_rootfs: false,
                init: true,
            });
        };
        let raw = json_object(
            raw,
            "security",
            &[
                "supplementary_groups",
                "cap_add",
                "cap_drop",
                "init",
                "no_new_privileges",
                "pids_limit",
                "read_only_rootfs",
                "user",
            ],
        )?;
        let user = match field(raw, "user") {
            None => Some(DEFAULT_SANDBOX_USER.to_string()),
            Some(Value::Null) => None,
            Some(Value::String(user)) => Some(user.clone()).filter(|user| !user.is_empty()),
            Some(_) => return Err("security user must be a string or null".into()),
        };
        Ok(Security {
            user,
            cap_drop: field(raw, "cap_drop").map_or(Ok(vec!["ALL".into()]), |v| json_string_list(v, "cap_drop"))?,
            cap_add: field(raw, "cap_add").map_or(Ok(vec![]), |v| json_string_list(v, "cap_add"))?,
            supplementary_groups: field(raw, "supplementary_groups")
                .map_or(Ok(vec![]), |v| json_string_list(v, "supplementary_groups"))?,
            no_new_privileges: field(raw, "no_new_privileges")
                .map_or(Ok(true), |v| json_bool(v, "no_new_privileges"))?,
            pids_limit: match field(raw, "pids_limit") {
                None => Some(DEFAULT_PIDS_LIMIT),
                value => json_optional_int(value, "pids_limit")?,
            },
            read_only_rootfs: field(raw, "read_only_rootfs").map_or(Ok(false), |v| json_bool(v, "read_only_rootfs"))?,
            init: field(raw, "init").map_or(Ok(true), |v| json_bool(v, "init"))?,
        })
    }

    fn to_dict(&self) -> Value {
        let mut out = Map::new();
        out.insert("user".into(), optional_string(&self.user));
        out.insert("cap_drop".into(), strings(&self.cap_drop));
        out.insert("cap_add".into(), strings(&self.cap_add));
        if !self.supplementary_groups.is_empty() {
            out.insert("supplementary_groups".into(), strings(&self.supplementary_groups));
        }
        out.insert("no_new_privileges".into(), self.no_new_privileges.into());
        out.insert("pids_limit".into(), optional_int(self.pids_limit));
        out.insert("read_only_rootfs".into(), self.read_only_rootfs.into());
        out.insert("init".into(), self.init.into());
        Value::Object(out)
    }

    fn validate(&self) -> SpecResult<()> {
        if let Some(user) = &self.user {
            validate_security_value("security user", user)?;
        }
        if self.supplementary_groups.len() > 64 {
            return Err("at most 64 supplementary groups are supported".into());
        }
        for item in &self.supplementary_groups {
            validate_security_value("supplementary group", item)?;
        }
        for item in &self.cap_drop {
            validate_security_value("cap_drop", item)?;
        }
        for item in &self.cap_add {
            validate_security_value("cap_add", item)?;
        }
        if self.pids_limit.is_some_and(|limit| limit <= 0) {
            return Err("pids_limit must be positive.".into());
        }
        Ok(())
    }
}

impl Filesystem {
    fn container_default() -> Filesystem {
        Filesystem {
            enforce_disk_quota: false,
            workspace_path: "/workspace".into(),
            tmpfs_mb: 64,
            run_tmpfs_mb: 16,
            shm_mb: 64,
            workspace_storage: None,
            management_helper: "shell".into(),
        }
    }

    fn linux_host_default() -> Filesystem {
        Filesystem { tmpfs_mb: 256, run_tmpfs_mb: 64, ..Filesystem::container_default() }
    }

    fn from_dict(raw: Option<&Value>) -> SpecResult<Filesystem> {
        let Some(raw) = raw.filter(|raw| !raw.is_null()) else {
            return Ok(Filesystem::container_default());
        };
        let raw = json_object(
            raw,
            "filesystem",
            &[
                "enforce_disk_quota",
                "run_tmpfs_mb",
                "tmpfs_mb",
                "workspace_path",
                "shm_mb",
                "workspace_storage",
                "management_helper",
            ],
        )?;
        Ok(Filesystem {
            enforce_disk_quota: field(raw, "enforce_disk_quota")
                .map_or(Ok(false), |v| json_bool(v, "enforce_disk_quota"))?,
            workspace_path: field(raw, "workspace_path")
                .map_or(Ok("/workspace".into()), |v| json_string(v, "workspace_path"))?,
            tmpfs_mb: field(raw, "tmpfs_mb").map_or(Ok(64), |v| json_int(v, "tmpfs_mb"))?,
            run_tmpfs_mb: field(raw, "run_tmpfs_mb").map_or(Ok(16), |v| json_int(v, "run_tmpfs_mb"))?,
            shm_mb: field(raw, "shm_mb").map_or(Ok(64), |v| json_int(v, "shm_mb"))?,
            management_helper: field(raw, "management_helper")
                .map_or(Ok("shell".into()), |v| json_string(v, "management_helper"))?,
            workspace_storage: present(raw, "workspace_storage")
                .map(|v| json_string(v, "workspace_storage"))
                .transpose()?,
        })
    }

    fn to_dict(&self) -> Value {
        let mut out = Map::new();
        out.insert("enforce_disk_quota".into(), self.enforce_disk_quota.into());
        out.insert("workspace_path".into(), self.workspace_path.clone().into());
        if self.management_helper != "shell" {
            out.insert("management_helper".into(), self.management_helper.clone().into());
        }
        out.insert("tmpfs_mb".into(), self.tmpfs_mb.into());
        out.insert("run_tmpfs_mb".into(), self.run_tmpfs_mb.into());
        if self.shm_mb != 64 {
            out.insert("shm_mb".into(), self.shm_mb.into());
        }
        if let Some(storage) = &self.workspace_storage {
            out.insert("workspace_storage".into(), storage.clone().into());
        }
        Value::Object(out)
    }

    fn validate(&self) -> SpecResult<()> {
        validate_workspace_path(&self.workspace_path)?;
        if !matches!(self.management_helper.as_str(), "shell" | "static") {
            return Err("management_helper must be shell or static".into());
        }
        if !matches!(self.workspace_storage.as_deref(), None | Some("image") | Some("tmpfs")) {
            return Err("workspace_storage must be image or tmpfs".into());
        }
        if self.enforce_disk_quota && self.workspace_storage.as_deref() == Some("image") {
            return Err("legacy enforce_disk_quota conflicts with workspace_storage=image".into());
        }
        if self.shm_mb <= 0 {
            return Err("shm_mb must be positive".into());
        }
        if self.tmpfs_mb <= 0 {
            return Err("tmpfs_mb must be positive.".into());
        }
        if self.run_tmpfs_mb <= 0 {
            return Err("run_tmpfs_mb must be positive.".into());
        }
        Ok(())
    }
}

impl LinuxHost {
    fn from_dict(raw: Option<&Value>) -> SpecResult<LinuxHost> {
        let default_paths = || DEFAULT_LINUX_HOST_WRITABLE_PATHS.iter().map(|p| p.to_string()).collect::<Vec<_>>();
        let Some(raw) = raw.filter(|raw| !raw.is_null()) else {
            return Ok(LinuxHost {
                enable_cron: false,
                enable_sshd: false,
                keep_alive: true,
                writable_paths: default_paths(),
            });
        };
        let raw = json_object(raw, "linux_host", &["enable_cron", "enable_sshd", "keep_alive", "writable_paths"])?;
        Ok(LinuxHost {
            enable_cron: field(raw, "enable_cron").map_or(Ok(false), |v| json_bool(v, "enable_cron"))?,
            enable_sshd: field(raw, "enable_sshd").map_or(Ok(false), |v| json_bool(v, "enable_sshd"))?,
            keep_alive: field(raw, "keep_alive").map_or(Ok(true), |v| json_bool(v, "keep_alive"))?,
            writable_paths: field(raw, "writable_paths")
                .map_or(Ok(default_paths()), |v| json_string_list(v, "writable_paths"))?,
        })
    }

    fn to_dict(&self) -> Value {
        let mut out = Map::new();
        out.insert("enable_cron".into(), self.enable_cron.into());
        out.insert("enable_sshd".into(), self.enable_sshd.into());
        out.insert("keep_alive".into(), self.keep_alive.into());
        out.insert("writable_paths".into(), strings(&self.writable_paths));
        Value::Object(out)
    }
}

impl Ssh {
    fn from_dict(raw: Option<&Value>) -> SpecResult<Ssh> {
        let Some(raw) = raw.filter(|raw| !raw.is_null()) else {
            return Ok(Ssh {
                enabled: false,
                user: "root".into(),
                host: "127.0.0.1".into(),
                host_port: None,
                container_port: 22,
                authorized_keys: vec![],
            });
        };
        let raw =
            json_object(raw, "ssh", &["authorized_keys", "container_port", "enabled", "host", "host_port", "user"])?;
        Ok(Ssh {
            enabled: field(raw, "enabled").map_or(Ok(false), |v| json_bool(v, "enabled"))?,
            user: field(raw, "user").map_or(Ok("root".into()), |v| json_string(v, "user"))?,
            host: field(raw, "host").map_or(Ok("127.0.0.1".into()), |v| json_string(v, "host"))?,
            host_port: json_optional_int(field(raw, "host_port"), "host_port")?,
            container_port: field(raw, "container_port").map_or(Ok(22), |v| json_int(v, "container_port"))?,
            authorized_keys: field(raw, "authorized_keys")
                .map_or(Ok(vec![]), |v| json_string_list(v, "authorized_keys"))?,
        })
    }

    fn to_dict(&self) -> Value {
        let mut out = Map::new();
        out.insert("enabled".into(), self.enabled.into());
        out.insert("user".into(), self.user.clone().into());
        out.insert("host".into(), self.host.clone().into());
        out.insert("host_port".into(), optional_int(self.host_port));
        out.insert("container_port".into(), self.container_port.into());
        out.insert("authorized_keys".into(), strings(&self.authorized_keys));
        Value::Object(out)
    }

    fn validate(&self) -> SpecResult<()> {
        if !self.enabled {
            return Ok(());
        }
        if python_strip(&self.user).is_empty() {
            return Err("ssh user cannot be empty.".into());
        }
        if self.host_port.is_some_and(|port| !(1..=65535).contains(&port)) {
            return Err("ssh host_port must be in [1, 65535].".into());
        }
        if !(1..=65535).contains(&self.container_port) {
            return Err("ssh container_port must be in [1, 65535].".into());
        }
        Ok(())
    }
}

impl NetworkPolicy {
    fn from_dict(raw: Option<&Value>) -> SpecResult<NetworkPolicy> {
        let shape = || "network_policy must be an object with egress and relay fields".to_string();
        let raw = match raw {
            None => return Ok(NetworkPolicy { relay: None }),
            Some(Value::Object(raw)) => raw,
            Some(_) => return Err(shape()),
        };
        if raw.keys().any(|key| key != "egress" && key != "relay") {
            return Err(shape());
        }
        let relay = present(raw, "relay");
        match raw.get("egress") {
            None => {}
            Some(Value::String(egress)) if egress == "direct" => {}
            Some(Value::String(egress)) if egress == "relay" => {
                return match relay {
                    Some(Value::String(name)) if is_relay_name(name) => Ok(NetworkPolicy { relay: Some(name.clone()) }),
                    _ => Err("network_policy.relay must be a valid relay name".into()),
                };
            }
            Some(_) => return Err("network_policy.egress must be 'direct' or 'relay'".into()),
        }
        if relay.is_some() {
            return Err("network_policy.relay requires egress='relay'".into());
        }
        Ok(NetworkPolicy { relay: None })
    }
}

impl SandboxSpec {
    /// Python's `SandboxSpec.from_dict` (no validation), then `to_dict()`.
    pub fn from_dict(raw: &Value) -> SpecResult<SandboxSpec> {
        let Value::Object(raw) = raw else {
            return Err("sandbox must be a JSON object".into());
        };
        const ALLOWED: [&str; 23] = [
            "command",
            "cpus",
            "disk_mb",
            "env",
            "filesystem",
            "id",
            "image",
            "labels",
            "linux_host",
            "managed_process",
            "memory_mb",
            "network",
            "dns_servers",
            "environment_root",
            "network_policy",
            "parkable",
            "profile",
            "required_features",
            "security",
            "ssh",
            "toolkits",
            "ttl_seconds",
            "working_dir",
        ];
        let mut unsupported: Vec<&str> = raw.keys().map(String::as_str).filter(|key| !ALLOWED.contains(key)).collect();
        unsupported.sort_unstable();
        if !unsupported.is_empty() {
            return Err(format!("unsupported sandbox fields: {}", unsupported.join(", ")));
        }
        let profile = field(raw, "profile").map_or(Ok("container".into()), |v| json_string(v, "profile"))?;
        let command = field(raw, "command").map_or(Ok(vec![]), |v| json_string_list(v, "command"))?;
        let env = field(raw, "env").map_or(Ok(BTreeMap::new()), |v| json_string_map(v, "env"))?;
        let labels = field(raw, "labels").map_or(Ok(BTreeMap::new()), |v| json_string_map(v, "labels"))?;
        // Profile defaults sit under an object (or null) the request gives.
        let merged = |raw: Option<&Value>, defaults: Value| -> Option<Value> {
            match raw {
                None | Some(Value::Null) => Some(defaults),
                Some(Value::Object(given)) => {
                    let mut out = defaults.as_object().cloned().unwrap_or_default();
                    out.extend(given.iter().map(|(k, v)| (k.clone(), v.clone())));
                    Some(Value::Object(out))
                }
                Some(other) => Some(other.clone()),
            }
        };
        let security_raw = if profile == "linux_host" {
            merged(present(raw, "security"), Security::linux_host_default().to_dict())
        } else {
            present(raw, "security").cloned()
        };
        let filesystem_raw = if profile == "linux_host" || profile == "linux_session" {
            merged(present(raw, "filesystem"), Filesystem::linux_host_default().to_dict())
        } else {
            present(raw, "filesystem").cloned()
        };
        let security = Security::from_dict(security_raw.as_ref())?;
        let filesystem = Filesystem::from_dict(filesystem_raw.as_ref())?;
        let linux_host_raw = if profile == "linux_session" {
            merged(present(raw, "linux_host"), serde_json::json!({"writable_paths": []}))
        } else {
            present(raw, "linux_host").cloned()
        };
        let id = field(raw, "id").map_or(Ok(String::new()), |v| json_string(v, "id"))?;
        let image = field(raw, "image").map_or(Ok(String::new()), |v| json_string(v, "image"))?;
        let required_features =
            field(raw, "required_features").map_or(Ok(vec![]), |v| json_string_list(v, "required_features"))?;
        let working_dir = present(raw, "working_dir").map(|v| json_string(v, "working_dir")).transpose()?;
        let memory_mb = present(raw, "memory_mb").map(|v| json_int(v, "memory_mb")).transpose()?;
        let cpus = present(raw, "cpus").map(|v| json_number(v, "cpus")).transpose()?;
        let disk_mb = present(raw, "disk_mb").map(|v| json_int(v, "disk_mb")).transpose()?;
        let network = field(raw, "network").map_or(Ok("bridge".into()), |v| json_string(v, "network"))?;
        let dns_servers = field(raw, "dns_servers").map_or(Ok(vec![]), |v| json_string_list(v, "dns_servers"))?;
        let network_policy = NetworkPolicy::from_dict(field(raw, "network_policy"))?;
        let ttl_seconds = present(raw, "ttl_seconds").map(|v| json_int(v, "ttl_seconds")).transpose()?;
        let parkable = field(raw, "parkable").map_or(Ok(false), |v| json_bool(v, "parkable"))?;
        let managed_process = field(raw, "managed_process").map_or(Ok(false), |v| json_bool(v, "managed_process"))?;
        let ssh = Ssh::from_dict(field(raw, "ssh"))?;
        let linux_host = LinuxHost::from_dict(linux_host_raw.as_ref())?;
        let environment_root =
            present(raw, "environment_root").map(|v| json_string(v, "environment_root")).transpose()?;
        let toolkits = field(raw, "toolkits").map_or(Ok(vec![]), |v| json_string_list(v, "toolkits"))?;
        let mut spec = SandboxSpec {
            id,
            image,
            profile,
            required_features,
            command,
            env,
            working_dir,
            memory_mb,
            cpus,
            disk_mb,
            network,
            dns_servers,
            network_policy,
            ttl_seconds,
            parkable,
            managed_process,
            ssh,
            security,
            filesystem,
            linux_host,
            labels,
            environment_root,
            toolkits,
            dict: Value::Null,
            encoded: String::new(),
        };
        spec.dict = spec.build_dict()?;
        spec.encoded = pyjson::dumps(&spec.dict);
        Ok(spec)
    }

    fn build_dict(&self) -> SpecResult<Value> {
        let mut out = Map::new();
        out.insert("id".into(), self.id.clone().into());
        out.insert("image".into(), self.image.clone().into());
        out.insert("profile".into(), self.profile.clone().into());
        if !self.required_features.is_empty() {
            out.insert("required_features".into(), strings(&self.required_features));
        }
        out.insert("command".into(), strings(&self.command));
        out.insert("env".into(), string_map(&self.env));
        out.insert("working_dir".into(), optional_string(&self.working_dir));
        out.insert("memory_mb".into(), optional_int(self.memory_mb));
        let cpus = match self.cpus {
            None => Value::Null,
            // Python's json.dumps writes NaN/Infinity, which json_valid refuses.
            Some(cpus) => Value::Number(Number::from_f64(cpus).ok_or("cpus must be finite")?),
        };
        out.insert("cpus".into(), cpus);
        out.insert("disk_mb".into(), optional_int(self.disk_mb));
        out.insert("network".into(), self.network.clone().into());
        if !self.dns_servers.is_empty() {
            out.insert("dns_servers".into(), strings(&self.dns_servers));
        }
        if let Some(relay) = &self.network_policy.relay {
            out.insert("network_policy".into(), serde_json::json!({"egress": "relay", "relay": relay}));
        }
        out.insert("ttl_seconds".into(), optional_int(self.ttl_seconds));
        out.insert("parkable".into(), self.parkable.into());
        out.insert("managed_process".into(), self.managed_process.into());
        out.insert("ssh".into(), self.ssh.to_dict());
        out.insert("security".into(), self.security.to_dict());
        out.insert("filesystem".into(), self.filesystem.to_dict());
        out.insert("linux_host".into(), self.linux_host.to_dict());
        out.insert("labels".into(), string_map(&self.labels));
        if let Some(root) = &self.environment_root {
            out.insert("environment_root".into(), root.clone().into());
        }
        if !self.toolkits.is_empty() {
            out.insert("toolkits".into(), strings(&self.toolkits));
        }
        Ok(Value::Object(out))
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn memory_mb(&self) -> Option<i64> {
        self.memory_mb
    }

    pub fn disk_mb(&self) -> Option<i64> {
        self.disk_mb
    }

    pub fn parkable(&self) -> bool {
        self.parkable
    }

    /// `to_dict()`.
    pub fn to_dict(&self) -> &Value {
        &self.dict
    }

    /// The canonical JSON of `to_dict()`: the bytes `spec_sha256` hashes.
    pub fn canonical_json(&self) -> &str {
        &self.encoded
    }

    /// `sandbox_spec_fingerprint(spec)`.
    pub fn sha256(&self) -> String {
        pyjson::sha256_hex(&self.encoded)
    }

    /// `requested_resources().disk_mb`: parkable incarnations also reserve
    /// their memory backing (rounded up to the allocator chunk), a private
    /// checkpoint as large as memory, and a fixed overhead.
    pub fn requested_disk_mb(&self) -> SpecResult<i64> {
        if !self.parkable {
            return Ok(self.disk_mb.unwrap_or(0));
        }
        match (self.memory_mb, self.disk_mb) {
            (Some(memory), Some(disk)) if memory > 0 && disk > 0 => {
                Ok(disk + (memory / ALLOCATOR_CHUNK_MB + 1) * ALLOCATOR_CHUNK_MB + memory + FIXED_OVERHEAD_MB)
            }
            (Some(memory), Some(_)) if memory <= 0 => Err("memory_mb must be positive".into()),
            (Some(_), Some(_)) => Err("writable_disk_mb must be positive".into()),
            _ => Err("parkable sandbox resources require memory_mb and disk_mb".into()),
        }
    }

    /// `SandboxSpec.validate()`, except the environment catalog check.
    pub fn validate(&self) -> SpecResult<()> {
        if !python_match_end(&self.id, is_sandbox_id) {
            return Err(
                "sandbox id must be 1-64 characters of letters, digits, _, . or - and start with a letter or digit."
                    .into(),
            );
        }
        if python_strip(&self.image).is_empty() || self.image.contains('\0') {
            return Err("sandbox image is required.".into());
        }
        if self.environment_root.as_deref().is_some_and(|root| !is_prefixed_digest(root)) {
            return Err("environment_root must be a sha256 digest.".into());
        }
        if self.command.iter().any(|argument| argument.contains('\0')) {
            return Err("sandbox command cannot contain NUL.".into());
        }
        for (key, value) in &self.env {
            if !python_match_end(key, is_env_key) {
                return Err(format!("invalid environment variable name: {}", python_repr(key)));
            }
            if value.contains('\0') {
                return Err("sandbox environment values cannot contain NUL.".into());
            }
        }
        if self.labels.iter().any(|(key, value)| key.contains('\0') || value.contains('\0')) {
            return Err("sandbox labels cannot contain NUL.".into());
        }
        if let Some(key) = self.labels.keys().find(|key| key.to_lowercase().starts_with(RESERVED_LABEL_PREFIX)) {
            return Err(format!(
                "sandbox labels must not use the reserved {} prefix: {}",
                python_repr(RESERVED_LABEL_PREFIX),
                python_repr(key)
            ));
        }
        if self.memory_mb.is_some_and(|value| value <= 0) {
            return Err("memory_mb must be positive.".into());
        }
        if self.cpus.is_some_and(|value| value <= 0.0) {
            return Err("cpus must be positive.".into());
        }
        if self.disk_mb.is_some_and(|value| value <= 0) {
            return Err("disk_mb must be positive.".into());
        }
        let requested_disk_mb = self.requested_disk_mb()?;
        if self.cpus.unwrap_or(0.0) == 0.0 && self.memory_mb.unwrap_or(0) == 0 && requested_disk_mb == 0 {
            return Err("sandbox resources are required.".into());
        }
        if self.ttl_seconds.is_some_and(|value| value <= 0) {
            return Err("ttl_seconds must be positive.".into());
        }
        if self.parkable && self.memory_mb.is_none() {
            return Err("parkable sandboxes require an explicit memory_mb limit.".into());
        }
        if self.parkable && self.disk_mb.is_none() {
            return Err("parkable sandboxes require an explicit disk_mb limit.".into());
        }
        if self.parkable && self.ssh.enabled {
            return Err("parkable sandboxes cannot expose SSH because direct host-port sessions bypass the hibernation lifecycle barrier.".into());
        }
        if self.managed_process && !self.parkable {
            return Err("managed_process requires a parkable sandbox.".into());
        }
        if self.managed_process && self.profile != "container" {
            return Err("managed_process currently requires the container profile.".into());
        }
        if self.managed_process && !self.command.is_empty() {
            return Err("managed_process sandboxes start their primary command through the job API.".into());
        }
        if self.managed_process && self.security.read_only_rootfs {
            return Err("managed_process requires a writable rootfs for its checkpointed ledger.".into());
        }
        if !matches!(self.profile.as_str(), "container" | "linux_host" | "linux_session") {
            return Err("profile must be one of: container, linux_host, linux_session".into());
        }
        if self.network_policy.relay.is_some() {
            if self.network != "bridge" {
                return Err("relay egress requires bridge networking".into());
            }
            if !self.dns_servers.is_empty() || self.ssh.enabled {
                return Err("relay egress does not allow custom DNS or inbound SSH".into());
            }
        }
        if self.network != "none" && self.network != "bridge" {
            return Err("network must be either 'none' or 'bridge'.".into());
        }
        if self.managed_process && !self.security.supplementary_groups.is_empty() {
            return Err("managed_process does not yet support supplementary groups".into());
        }
        if self.required_features.len() > 64 || !self.required_features.iter().all(|name| is_feature_name(name)) {
            return Err("required_features must contain at most 64 feature names".into());
        }
        if self.dns_servers.len() > 3 {
            return Err("at most three DNS servers are supported".into());
        }
        for server in &self.dns_servers {
            parse_ipv4(server)?;
        }
        if !self.dns_servers.is_empty() && self.network == "none" {
            return Err("DNS servers require bridge networking".into());
        }
        self.ssh.validate()?;
        self.security.validate()?;
        self.filesystem.validate()?;
        for path in &self.linux_host.writable_paths {
            validate_setup_path("linux_host writable path", path)?;
        }
        if let Some(working_dir) = &self.working_dir {
            validate_guest_path("working_dir", working_dir)?;
        }
        if self.ssh.enabled && self.network != "bridge" {
            return Err("ssh-enabled sandboxes must use bridge networking.".into());
        }
        Ok(())
    }
}

/// `str.strip()`: Python's whitespace also includes U+001C..U+001F.
pub(crate) fn python_strip(text: &str) -> &str {
    text.trim_matches(|c: char| c.is_whitespace() || ('\u{1c}'..='\u{1f}').contains(&c))
}

/// `re.match(pattern + "$")`: `$` also matches before one final newline.
fn python_match_end(text: &str, matches: fn(&str) -> bool) -> bool {
    matches(text) || text.strip_suffix('\n').is_some_and(matches)
}

/// `[A-Za-z0-9][A-Za-z0-9_.-]{0,63}`.
pub(crate) fn is_sandbox_id(text: &str) -> bool {
    let bytes = text.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 64
        && bytes[0].is_ascii_alphanumeric()
        && bytes[1..].iter().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'-'))
}

/// `[A-Za-z_][A-Za-z0-9_]*`.
fn is_env_key(text: &str) -> bool {
    let bytes = text.as_bytes();
    !bytes.is_empty()
        && (bytes[0].is_ascii_alphabetic() || bytes[0] == b'_')
        && bytes.iter().all(|b| b.is_ascii_alphanumeric() || *b == b'_')
}

/// `[a-z][a-z0-9-]{0,63}`.
fn is_feature_name(text: &str) -> bool {
    is_lower_name(text, 64)
}

/// `RELAY_NAME`: `[a-z][a-z0-9-]{0,31}`.
fn is_relay_name(text: &str) -> bool {
    is_lower_name(text, 32)
}

fn is_lower_name(text: &str, max: usize) -> bool {
    let bytes = text.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= max
        && bytes[0].is_ascii_lowercase()
        && bytes.iter().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-')
}

/// `[0-9a-f]{64}`.
pub(crate) fn is_digest(text: &str) -> bool {
    text.len() == 64 && text.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// `sha256:[0-9a-f]{64}`.
pub(crate) fn is_prefixed_digest(text: &str) -> bool {
    text.strip_prefix("sha256:").is_some_and(is_digest)
}

fn validate_security_value(name: &str, value: &str) -> SpecResult<()> {
    if value.is_empty() {
        return Err(format!("{name} cannot be empty."));
    }
    if value.contains('\n') || value.contains('\r') {
        return Err(format!("{name} cannot contain newlines."));
    }
    let allowed = |b: u8| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b':' | b'@' | b'/' | b'-');
    if !value.bytes().all(allowed) {
        return Err(format!("{name} contains unsupported characters."));
    }
    Ok(())
}

/// guest_paths.validate_guest_path.
fn validate_guest_path(name: &str, value: &str) -> SpecResult<()> {
    if !value.starts_with('/') {
        return Err(format!("{name} must be an absolute container path."));
    }
    if value.chars().any(|c| c <= '\u{1f}' || c == '\u{7f}') {
        return Err(format!("{name} contains unsupported control characters."));
    }
    if value.split('/').any(|part| part == "..") {
        return Err(format!("{name} cannot contain '..'."));
    }
    Ok(())
}

/// guest_paths.validate_setup_path.
fn validate_setup_path(name: &str, value: &str) -> SpecResult<()> {
    validate_guest_path(name, value)?;
    if value != "/" && value[1..].split('/').any(|part| part.is_empty() || part == ".") {
        return Err(format!("{name} must be a canonical absolute path."));
    }
    if value.contains(':') || value.contains(',') {
        return Err(format!("{name} contains unsupported delimiters."));
    }
    Ok(())
}

/// guest_paths.validate_workspace_path.
fn validate_workspace_path(value: &str) -> SpecResult<()> {
    validate_setup_path("workspace_path", value)?;
    const ROOTS: [&str; 15] = [
        "/",
        "/etc",
        "/bin",
        "/sbin",
        "/lib",
        "/lib64",
        "/usr",
        "/var",
        "/home",
        "/root",
        "/tmp",
        "/opt",
        "/boot",
        "/.ucloud-init",
        "/.ucloud-job-init",
    ];
    const TREES: [&str; 5] = ["/proc", "/sys", "/dev", "/run", "/.ucloud-managed"];
    let under = |root: &str| value == root || value.strip_prefix(root).is_some_and(|rest| rest.starts_with('/'));
    if ROOTS.contains(&value) || TREES.iter().any(|root| under(root)) {
        return Err("workspace_path overlaps a reserved system or runtime path.".into());
    }
    Ok(())
}

/// `ipaddress.IPv4Address(text)`, with its messages.
fn parse_ipv4(text: &str) -> SpecResult<()> {
    if text.contains('/') {
        return Err(format!("Unexpected '/' in {}", python_repr(text)));
    }
    if text.is_empty() {
        return Err("Address cannot be empty".into());
    }
    let octets: Vec<&str> = text.split('.').collect();
    if octets.len() != 4 {
        return Err(format!("Expected 4 octets in {}", python_repr(text)));
    }
    for octet in octets {
        let problem = if octet.is_empty() {
            "Empty octet not permitted".to_string()
        } else if !octet.bytes().all(|b| b.is_ascii_digit()) {
            format!("Only decimal digits permitted in {}", python_repr(octet))
        } else if octet.len() > 3 {
            format!("At most 3 characters permitted in {}", python_repr(octet))
        } else if octet != "0" && octet.starts_with('0') {
            format!("Leading zeros are not permitted in {}", python_repr(octet))
        } else {
            match octet.parse::<u32>() {
                Ok(value) if value > 255 => format!("Octet {value} (> 255) not permitted"),
                _ => continue,
            }
        };
        return Err(format!("{problem} in {}", python_repr(text)));
    }
    Ok(())
}

/// `repr(str)` for messages; non-ASCII characters print as themselves except
/// the C0/C1 controls and separators Python also escapes.
pub(crate) fn python_repr(text: &str) -> String {
    let quote = if text.contains('\'') && !text.contains('"') { '"' } else { '\'' };
    let mut out = String::with_capacity(text.len() + 2);
    out.push(quote);
    for c in text.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\t' => out.push_str("\\t"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            c if c == quote => {
                out.push('\\');
                out.push(c);
            }
            c if (c as u32) < 0x20 || (0x7f..=0xa0).contains(&(c as u32)) || c == '\u{ad}' => {
                out.push_str(&format!("\\x{:02x}", c as u32))
            }
            '\u{2028}' | '\u{2029}' | '\u{feff}' => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push(quote);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn spec(raw: Value) -> SandboxSpec {
        SandboxSpec::from_dict(&raw).unwrap()
    }

    #[test]
    fn canonical_form_drops_defaults_and_floats_cpus() {
        // node-api-create-spec.md 4.1, verified against Python.
        let demo = spec(json!({"id":"demo-1","image":"registry.example/img:1","memory_mb":512,"cpus":1,
            "disk_mb":1024,"env":{"A":"ä"},"parkable":true,"managed_process":true}));
        assert!(demo.canonical_json().starts_with(r#"{"command":[],"cpus":1.0,"disk_mb":1024,"env":{"A":"\u00e4"},"#));
        assert_eq!(demo.sha256(), "ecec28ec936f12b137cfba56d7568f760d35a0702a44b7d1fd4774c50da3caa1");
        assert_eq!(demo.requested_disk_mb().unwrap(), 2624);
        demo.validate().unwrap();
        // The canonical form is a fixed point.
        assert_eq!(spec(demo.to_dict().clone()).canonical_json(), demo.canonical_json());
    }

    #[test]
    fn profile_defaults_and_conditional_keys() {
        let host = spec(json!({"id":"h","image":"i","profile":"linux_host","memory_mb":1}));
        let dict = host.to_dict();
        assert_eq!(
            dict["security"],
            json!({"user":null,"cap_drop":[],"cap_add":[],"no_new_privileges":false,
            "pids_limit":null,"read_only_rootfs":false,"init":true})
        );
        assert_eq!(
            dict["filesystem"],
            json!({"enforce_disk_quota":false,"workspace_path":"/workspace",
            "tmpfs_mb":256,"run_tmpfs_mb":64})
        );
        assert_eq!(dict["linux_host"]["writable_paths"].as_array().unwrap().len(), 17);
        assert!(dict.get("network_policy").is_none() && dict.get("dns_servers").is_none());
        let session = spec(json!({"id":"s","image":"i","profile":"linux_session","memory_mb":1,
            "security":{"user":"","supplementary_groups":[]},"filesystem":{"shm_mb":64,"workspace_storage":null,
            "management_helper":"shell"},"network_policy":{"egress":"direct"}}));
        let dict = session.to_dict();
        assert_eq!(dict["security"]["user"], Value::Null);
        assert!(dict["security"].get("supplementary_groups").is_none());
        assert_eq!(
            dict["filesystem"],
            json!({"enforce_disk_quota":false,"workspace_path":"/workspace",
            "tmpfs_mb":256,"run_tmpfs_mb":64})
        );
        assert_eq!(dict["linux_host"]["writable_paths"], json!([]));
        let relay = spec(json!({"id":"r","image":"i","network_policy":{"egress":"relay","relay":"default"},
            "required_features":["x"],"dns_servers":[],"environment_root":null,"filesystem":{"shm_mb":128}}));
        let dict = relay.to_dict();
        assert_eq!(dict["network_policy"], json!({"egress":"relay","relay":"default"}));
        assert_eq!(dict["required_features"], json!(["x"]));
        assert_eq!(dict["filesystem"]["shm_mb"], 128);
        assert!(dict.get("dns_servers").is_none() && dict.get("environment_root").is_none());
    }

    #[test]
    fn strict_types_and_messages() {
        let error = |raw: Value| SandboxSpec::from_dict(&raw).unwrap_err();
        assert_eq!(error(json!([])), "sandbox must be a JSON object");
        assert_eq!(error(json!({"zeta":1,"alpha":2})), "unsupported sandbox fields: alpha, zeta");
        assert_eq!(error(json!({"memory_mb":512.0})), "memory_mb must be an integer");
        assert_eq!(error(json!({"memory_mb":true})), "memory_mb must be an integer");
        assert_eq!(error(json!({"cpus":true})), "cpus must be a number");
        assert_eq!(error(json!({"env":{"A":1}})), "env must be an object of string values");
        assert_eq!(error(json!({"security":{"user":5}})), "security user must be a string or null");
        assert_eq!(error(json!({"ssh":{"port":1}})), "unsupported ssh fields: port");
        assert_eq!(
            error(json!({"network_policy":null})),
            "network_policy must be an object with egress and relay fields"
        );
        assert_eq!(
            error(json!({"network_policy":{"egress":"relay"}})),
            "network_policy.relay must be a valid relay name"
        );
        assert_eq!(error(json!({"network_policy":{"relay":"x"}})), "network_policy.relay requires egress='relay'");
        let invalid = |raw: Value| spec(raw).validate().unwrap_err();
        assert!(invalid(json!({"id":"-x","image":"i","memory_mb":1})).starts_with("sandbox id must be"));
        assert_eq!(invalid(json!({"id":"x","image":"i"})), "sandbox resources are required.");
        assert_eq!(
            invalid(json!({"id":"x","image":"i","memory_mb":1,"env":{"1A":"v"}})),
            "invalid environment variable name: '1A'"
        );
        assert_eq!(
            invalid(json!({"id":"x","image":"i","memory_mb":1,"labels":{"UCloud-Sandboxes.x":"v"}})),
            "sandbox labels must not use the reserved 'ucloud-sandboxes.' prefix: 'UCloud-Sandboxes.x'"
        );
        assert_eq!(
            invalid(json!({"id":"x","image":"i","memory_mb":1,"dns_servers":["1.2.03.4"]})),
            "Leading zeros are not permitted in '03' in '1.2.03.4'"
        );
        // requested_resources() runs first and refuses a parkable spec without both bounds.
        assert_eq!(
            invalid(json!({"id":"x","image":"i","parkable":true,"memory_mb":1})),
            "parkable sandbox resources require memory_mb and disk_mb"
        );
        assert_eq!(
            invalid(json!({"id":"x","image":"i","memory_mb":1,"filesystem":{"workspace_path":"/run/x"}})),
            "workspace_path overlaps a reserved system or runtime path."
        );
        // Python's `$` accepts one trailing newline in the ID.
        spec(json!({"id":"x\n","image":"i","memory_mb":1})).validate().unwrap();
    }
}
