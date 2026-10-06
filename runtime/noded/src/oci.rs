//! The sandbox spec as the gateway sends it and its OCI `config.json`, as
//! ucloud_sandboxes/sandbox.py (`SandboxSpec.from_dict`, `validate`),
//! guest_identity.py and direct_oci.py (`DirectOciConfigBuilder.build`,
//! `validate_management_helper`) define them. Spec §1.1 and S6 of the phase 1
//! create-pipeline specification.
//!
//! Python reads `annotations`, `process.cwd` and `linux.cgroupsPath` back from
//! the config, so the content must match; its bytes come from
//! `rootfs::json_indent2` in `rootfs::OverlayManager::prepare`.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{self, Read};
use std::net::Ipv4Addr;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use serde_json::{Map, Value, json};

use crate::guest;
use crate::image::{ImageConfig, MaterializedRootfs};

/// Python `DirectOciConfigError` and the spec's `ValueError`s: a 400.
#[derive(Debug)]
pub enum OciError {
    Config(String),
    /// A request the Rust create does not implement (relay egress); forward it to Python.
    Unsupported(String),
    Io(io::Error),
}

impl std::fmt::Display for OciError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OciError::Config(message) | OciError::Unsupported(message) => f.write_str(message),
            OciError::Io(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for OciError {}

impl From<io::Error> for OciError {
    fn from(error: io::Error) -> Self {
        OciError::Io(error)
    }
}

fn config(message: impl Into<String>) -> OciError {
    OciError::Config(message.into())
}

/// Python's `repr` of a str, as error messages quote names.
pub(crate) fn py_repr(text: &str) -> String {
    let quote = if text.contains('\'') && !text.contains('"') { '"' } else { '\'' };
    let mut out = String::from(quote);
    for character in text.chars() {
        match character {
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c == quote => {
                out.push('\\');
                out.push(c);
            }
            c if (c as u32) < 0x20 || c as u32 == 0x7f => out.push_str(&format!("\\x{:02x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push(quote);
    out
}

// ---------------------------------------------------------------------------
// SandboxSpec (sandbox.py)

pub const DEFAULT_SANDBOX_USER: &str = "1000:1000";
pub const DEFAULT_PIDS_LIMIT: i64 = 256;
pub const DEFAULT_DNS_SERVERS: [&str; 2] = ["1.1.1.1", "8.8.8.8"];
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

#[derive(Debug, Clone, PartialEq)]
pub struct SecuritySpec {
    pub user: Option<String>,
    pub cap_drop: Vec<String>,
    pub cap_add: Vec<String>,
    pub supplementary_groups: Vec<String>,
    pub no_new_privileges: bool,
    pub pids_limit: Option<i64>,
    pub read_only_rootfs: bool,
    pub init: bool,
}

impl Default for SecuritySpec {
    fn default() -> Self {
        SecuritySpec {
            user: Some(DEFAULT_SANDBOX_USER.into()),
            cap_drop: vec!["ALL".into()],
            cap_add: vec![],
            supplementary_groups: vec![],
            no_new_privileges: true,
            pids_limit: Some(DEFAULT_PIDS_LIMIT),
            read_only_rootfs: false,
            init: true,
        }
    }
}

impl SecuritySpec {
    /// `linux_host_default_security().to_dict()`, merged under a linux_host request.
    fn linux_host_defaults() -> Map<String, Value> {
        let value = json!({"user": null, "cap_drop": [], "cap_add": [], "no_new_privileges": false,
            "pids_limit": null, "read_only_rootfs": false, "init": true});
        value.as_object().cloned().expect("an object")
    }

    fn from_value(raw: Option<&Value>) -> Result<Self, OciError> {
        let Some(raw) = raw else { return Ok(Self::default()) };
        let raw = json_object(
            raw,
            "security",
            &["supplementary_groups", "cap_add", "cap_drop", "init", "no_new_privileges", "pids_limit", "read_only_rootfs", "user"],
        )?;
        let user = match raw.get("user") {
            None => Some(DEFAULT_SANDBOX_USER.to_string()),
            Some(Value::Null) => None,
            Some(Value::String(user)) => Some(user.clone()).filter(|user| !user.is_empty()),
            Some(_) => return Err(config("security user must be a string or null")),
        };
        Ok(SecuritySpec {
            user,
            cap_drop: optional(raw, "cap_drop", json_string_list, vec!["ALL".into()])?,
            cap_add: optional(raw, "cap_add", json_string_list, vec![])?,
            supplementary_groups: optional(raw, "supplementary_groups", json_string_list, vec![])?,
            no_new_privileges: optional(raw, "no_new_privileges", json_bool, true)?,
            pids_limit: match raw.get("pids_limit") {
                None => Some(DEFAULT_PIDS_LIMIT),
                Some(Value::Null) => None,
                Some(value) => Some(json_int(value, "pids_limit")?),
            },
            read_only_rootfs: optional(raw, "read_only_rootfs", json_bool, false)?,
            init: optional(raw, "init", json_bool, true)?,
        })
    }

    fn validate(&self) -> Result<(), OciError> {
        if let Some(user) = &self.user {
            validate_security_value("security user", user)?;
        }
        if self.supplementary_groups.len() > 64 {
            return Err(config("at most 64 supplementary groups are supported"));
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
            return Err(config("pids_limit must be positive."));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FilesystemSpec {
    pub enforce_disk_quota: bool,
    pub workspace_path: String,
    pub tmpfs_mb: i64,
    pub run_tmpfs_mb: i64,
    pub shm_mb: i64,
    pub workspace_storage: Option<String>,
    pub management_helper: String,
}

impl Default for FilesystemSpec {
    fn default() -> Self {
        FilesystemSpec {
            enforce_disk_quota: false,
            workspace_path: "/workspace".into(),
            tmpfs_mb: 64,
            run_tmpfs_mb: 16,
            shm_mb: 64,
            workspace_storage: None,
            management_helper: "shell".into(),
        }
    }
}

impl FilesystemSpec {
    /// `linux_host_default_filesystem().to_dict()`.
    fn linux_host_defaults() -> Map<String, Value> {
        let value = json!({"enforce_disk_quota": false, "workspace_path": "/workspace", "tmpfs_mb": 256, "run_tmpfs_mb": 64});
        value.as_object().cloned().expect("an object")
    }

    fn from_value(raw: Option<&Value>) -> Result<Self, OciError> {
        let Some(raw) = raw else { return Ok(Self::default()) };
        let raw = json_object(
            raw,
            "filesystem",
            &["enforce_disk_quota", "run_tmpfs_mb", "tmpfs_mb", "workspace_path", "shm_mb", "workspace_storage", "management_helper"],
        )?;
        Ok(FilesystemSpec {
            enforce_disk_quota: optional(raw, "enforce_disk_quota", json_bool, false)?,
            workspace_path: optional(raw, "workspace_path", json_string, "/workspace".into())?,
            tmpfs_mb: optional(raw, "tmpfs_mb", json_int, 64)?,
            run_tmpfs_mb: optional(raw, "run_tmpfs_mb", json_int, 16)?,
            shm_mb: optional(raw, "shm_mb", json_int, 64)?,
            management_helper: optional(raw, "management_helper", json_string, "shell".into())?,
            workspace_storage: nullable(raw, "workspace_storage", json_string)?,
        })
    }

    pub fn validate(&self) -> Result<(), OciError> {
        validate_workspace_path(&self.workspace_path)?;
        if self.management_helper != "shell" && self.management_helper != "static" {
            return Err(config("management_helper must be shell or static"));
        }
        if !matches!(self.workspace_storage.as_deref(), None | Some("image") | Some("tmpfs")) {
            return Err(config("workspace_storage must be image or tmpfs"));
        }
        if self.enforce_disk_quota && self.workspace_storage.as_deref() == Some("image") {
            return Err(config("legacy enforce_disk_quota conflicts with workspace_storage=image"));
        }
        if self.shm_mb <= 0 {
            return Err(config("shm_mb must be positive"));
        }
        if self.tmpfs_mb <= 0 {
            return Err(config("tmpfs_mb must be positive."));
        }
        if self.run_tmpfs_mb <= 0 {
            return Err(config("run_tmpfs_mb must be positive."));
        }
        Ok(())
    }

    pub fn workspace_is_tmpfs(&self) -> bool {
        self.workspace_storage.as_deref() == Some("tmpfs") || self.enforce_disk_quota
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct LinuxHostSpec {
    pub enable_cron: bool,
    pub enable_sshd: bool,
    pub keep_alive: bool,
    pub writable_paths: Vec<String>,
}

impl LinuxHostSpec {
    fn from_value(raw: Option<&Value>) -> Result<Self, OciError> {
        let defaults = || DEFAULT_LINUX_HOST_WRITABLE_PATHS.iter().map(|path| path.to_string()).collect::<Vec<_>>();
        let Some(raw) = raw else {
            return Ok(LinuxHostSpec { enable_cron: false, enable_sshd: false, keep_alive: true, writable_paths: defaults() });
        };
        let raw = json_object(raw, "linux_host", &["enable_cron", "enable_sshd", "keep_alive", "writable_paths"])?;
        Ok(LinuxHostSpec {
            enable_cron: optional(raw, "enable_cron", json_bool, false)?,
            enable_sshd: optional(raw, "enable_sshd", json_bool, false)?,
            keep_alive: optional(raw, "keep_alive", json_bool, true)?,
            writable_paths: optional(raw, "writable_paths", json_string_list, defaults())?,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SshSpec {
    pub enabled: bool,
    pub user: String,
    pub host: String,
    pub host_port: Option<i64>,
    pub container_port: i64,
    pub authorized_keys: Vec<String>,
}

impl SshSpec {
    fn from_value(raw: Option<&Value>) -> Result<Self, OciError> {
        let raw = match raw {
            None | Some(Value::Null) => &Map::new(),
            Some(raw) => json_object(raw, "ssh", &["authorized_keys", "container_port", "enabled", "host", "host_port", "user"])?,
        };
        Ok(SshSpec {
            enabled: optional(raw, "enabled", json_bool, false)?,
            user: optional(raw, "user", json_string, "root".into())?,
            host: optional(raw, "host", json_string, "127.0.0.1".into())?,
            host_port: nullable(raw, "host_port", json_int)?,
            container_port: optional(raw, "container_port", json_int, 22)?,
            authorized_keys: optional(raw, "authorized_keys", json_string_list, vec![])?,
        })
    }

    fn validate(&self) -> Result<(), OciError> {
        if !self.enabled {
            return Ok(());
        }
        if self.user.trim().is_empty() {
            return Err(config("ssh user cannot be empty."));
        }
        if self.host_port.is_some_and(|port| !(1..=65535).contains(&port)) {
            return Err(config("ssh host_port must be in [1, 65535]."));
        }
        if !(1..=65535).contains(&self.container_port) {
            return Err(config("ssh container_port must be in [1, 65535]."));
        }
        Ok(())
    }
}

/// network_policy.py `SandboxNetworkPolicy`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetworkPolicy {
    pub egress: String,
    pub relay: Option<String>,
}

impl NetworkPolicy {
    fn from_value(raw: &Value) -> Result<Self, OciError> {
        let invalid = || config("network_policy must be an object with egress and relay fields");
        let raw = raw.as_object().ok_or_else(invalid)?;
        if raw.keys().any(|key| key != "egress" && key != "relay") {
            return Err(invalid());
        }
        let egress = match raw.get("egress") {
            None => "direct".to_string(),
            Some(Value::String(egress)) if egress == "direct" || egress == "relay" => egress.clone(),
            Some(_) => return Err(config("network_policy.egress must be 'direct' or 'relay'")),
        };
        let relay = match raw.get("relay") {
            None | Some(Value::Null) => None,
            Some(Value::String(relay)) => Some(relay.clone()),
            Some(_) if egress == "relay" => return Err(config("network_policy.relay must be a valid relay name")),
            Some(_) => return Err(config("network_policy.relay requires egress='relay'")),
        };
        if egress == "relay" {
            let valid = relay.as_deref().is_some_and(|name| {
                let bytes = name.as_bytes();
                (1..=32).contains(&bytes.len())
                    && bytes[0].is_ascii_lowercase()
                    && bytes.iter().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-')
            });
            if !valid {
                return Err(config("network_policy.relay must be a valid relay name"));
            }
        } else if relay.is_some() {
            return Err(config("network_policy.relay requires egress='relay'"));
        }
        Ok(NetworkPolicy { egress, relay })
    }
}

/// The create-relevant view of `SandboxSpec`, parsed with `from_dict`'s rules
/// and defaults (profile-dependent security, filesystem and linux_host).
#[derive(Debug, Clone, PartialEq)]
pub struct SandboxSpec {
    pub id: String,
    pub image: String,
    pub profile: String,
    pub required_features: Vec<String>,
    pub command: Vec<String>,
    pub env: BTreeMap<String, String>,
    pub working_dir: Option<String>,
    pub memory_mb: Option<i64>,
    pub cpus: Option<f64>,
    pub disk_mb: Option<i64>,
    pub network: String,
    pub dns_servers: Vec<String>,
    pub network_policy: NetworkPolicy,
    pub ttl_seconds: Option<i64>,
    pub parkable: bool,
    pub managed_process: bool,
    pub ssh: SshSpec,
    pub security: SecuritySpec,
    pub filesystem: FilesystemSpec,
    pub linux_host: LinuxHostSpec,
    pub labels: BTreeMap<String, String>,
    pub environment_root: Option<String>,
}

const SPEC_FIELDS: [&str; 22] = [
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
    "ttl_seconds",
    "working_dir",
];

fn merged_defaults(defaults: Map<String, Value>, raw: Option<&Value>) -> Option<Value> {
    match raw {
        None | Some(Value::Null) => Some(Value::Object(defaults)),
        Some(Value::Object(raw)) => {
            let mut merged = defaults;
            merged.extend(raw.iter().map(|(key, value)| (key.clone(), value.clone())));
            Some(Value::Object(merged))
        }
        Some(other) => Some(other.clone()),
    }
}

impl SandboxSpec {
    /// `SandboxSpec.from_dict`: strict field and type checks, Python's defaults.
    pub fn from_value(raw: &Value) -> Result<Self, OciError> {
        let raw = raw.as_object().ok_or_else(|| config("sandbox must be a JSON object"))?;
        let mut unsupported: Vec<&str> = raw.keys().map(String::as_str).filter(|key| !SPEC_FIELDS.contains(key)).collect();
        if !unsupported.is_empty() {
            unsupported.sort();
            return Err(config(format!("unsupported sandbox fields: {}", unsupported.join(", "))));
        }
        let profile = optional(raw, "profile", json_string, "container".into())?;
        let command = optional(raw, "command", json_string_list, vec![])?;
        let env = optional(raw, "env", json_string_map, BTreeMap::new())?;
        let labels = optional(raw, "labels", json_string_map, BTreeMap::new())?;
        // A null security or filesystem is "absent" in from_dict.
        let present = |key: &str| raw.get(key).filter(|value| !value.is_null());
        let mut security_raw = present("security").cloned();
        let mut filesystem_raw = present("filesystem").cloned();
        if profile == "linux_host" && security_raw.as_ref().is_none_or(Value::is_object) {
            security_raw = merged_defaults(SecuritySpec::linux_host_defaults(), security_raw.as_ref());
        }
        if (profile == "linux_host" || profile == "linux_session") && filesystem_raw.as_ref().is_none_or(Value::is_object) {
            filesystem_raw = merged_defaults(FilesystemSpec::linux_host_defaults(), filesystem_raw.as_ref());
        }
        let security = SecuritySpec::from_value(security_raw.as_ref())?;
        let filesystem = FilesystemSpec::from_value(filesystem_raw.as_ref())?;
        let mut linux_host_raw = present("linux_host").cloned();
        if profile == "linux_session" && linux_host_raw.as_ref().is_none_or(Value::is_object) {
            let defaults = json!({"writable_paths": []}).as_object().cloned().expect("an object");
            linux_host_raw = merged_defaults(defaults, linux_host_raw.as_ref());
        }
        Ok(SandboxSpec {
            id: optional(raw, "id", json_string, String::new())?,
            image: optional(raw, "image", json_string, String::new())?,
            required_features: optional(raw, "required_features", json_string_list, vec![])?,
            command,
            env,
            working_dir: nullable(raw, "working_dir", json_string)?,
            memory_mb: nullable(raw, "memory_mb", json_int)?,
            cpus: nullable(raw, "cpus", json_number)?,
            disk_mb: nullable(raw, "disk_mb", json_int)?,
            network: optional(raw, "network", json_string, "bridge".into())?,
            dns_servers: optional(raw, "dns_servers", json_string_list, vec![])?,
            network_policy: NetworkPolicy::from_value(raw.get("network_policy").unwrap_or(&json!({})))?,
            ttl_seconds: nullable(raw, "ttl_seconds", json_int)?,
            parkable: optional(raw, "parkable", json_bool, false)?,
            managed_process: optional(raw, "managed_process", json_bool, false)?,
            ssh: SshSpec::from_value(raw.get("ssh"))?,
            security,
            filesystem,
            linux_host: LinuxHostSpec::from_value(linux_host_raw.as_ref())?,
            labels,
            environment_root: nullable(raw, "environment_root", json_string)?,
            profile,
        })
    }

    /// `SandboxSpec.validate`, environment_contract.validate_requirements included.
    pub fn validate(&self) -> Result<(), OciError> {
        let id = self.id.as_bytes();
        if id.is_empty()
            || id.len() > 64
            || !id[0].is_ascii_alphanumeric()
            || !id.iter().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'-'))
        {
            return Err(config(
                "sandbox id must be 1-64 characters of letters, digits, _, . or - and start with a letter or digit.",
            ));
        }
        if self.image.trim().is_empty() || self.image.contains('\0') {
            return Err(config("sandbox image is required."));
        }
        if self.environment_root.as_deref().is_some_and(|root| !is_digest(root)) {
            return Err(config("environment_root must be a sha256 digest."));
        }
        if self.command.iter().any(|argument| argument.contains('\0')) {
            return Err(config("sandbox command cannot contain NUL."));
        }
        for (key, value) in &self.env {
            if !is_env_key(key) {
                return Err(config(format!("invalid environment variable name: {}", py_repr(key))));
            }
            if value.contains('\0') {
                return Err(config("sandbox environment values cannot contain NUL."));
            }
        }
        if self.labels.iter().any(|(key, value)| key.contains('\0') || value.contains('\0')) {
            return Err(config("sandbox labels cannot contain NUL."));
        }
        // BTreeMap order is code point order, as Python's sorted().
        if let Some(key) = self.labels.keys().find(|key| key.to_lowercase().starts_with("ucloud-sandboxes.")) {
            return Err(config(format!(
                "sandbox labels must not use the reserved 'ucloud-sandboxes.' prefix: {}",
                py_repr(key)
            )));
        }
        if self.memory_mb.is_some_and(|value| value <= 0) {
            return Err(config("memory_mb must be positive."));
        }
        if self.cpus.is_some_and(|value| value <= 0.0) {
            return Err(config("cpus must be positive."));
        }
        if self.disk_mb.is_some_and(|value| value <= 0) {
            return Err(config("disk_mb must be positive."));
        }
        // requested_resources() == ResourceQuantity()
        if self.parkable && (self.memory_mb.is_none() || self.disk_mb.is_none()) {
            return Err(config("parkable sandbox resources require memory_mb and disk_mb"));
        }
        if self.cpus.is_none() && self.memory_mb.is_none() && self.disk_mb.is_none() {
            return Err(config("sandbox resources are required."));
        }
        if self.ttl_seconds.is_some_and(|value| value <= 0) {
            return Err(config("ttl_seconds must be positive."));
        }
        if self.parkable && self.ssh.enabled {
            return Err(config(
                "parkable sandboxes cannot expose SSH because direct host-port sessions bypass the hibernation lifecycle barrier.",
            ));
        }
        if self.managed_process && !self.parkable {
            return Err(config("managed_process requires a parkable sandbox."));
        }
        if self.managed_process && self.profile != "container" {
            return Err(config("managed_process currently requires the container profile."));
        }
        if self.managed_process && !self.command.is_empty() {
            return Err(config("managed_process sandboxes start their primary command through the job API."));
        }
        if self.managed_process && self.security.read_only_rootfs {
            return Err(config("managed_process requires a writable rootfs for its checkpointed ledger."));
        }
        if !matches!(self.profile.as_str(), "container" | "linux_host" | "linux_session") {
            return Err(config("profile must be one of: container, linux_host, linux_session"));
        }
        if self.network_policy.egress == "relay" {
            if self.network != "bridge" {
                return Err(config("relay egress requires bridge networking"));
            }
            if !self.dns_servers.is_empty() || self.ssh.enabled {
                return Err(config("relay egress does not allow custom DNS or inbound SSH"));
            }
        }
        if self.network != "none" && self.network != "bridge" {
            return Err(config("network must be either 'none' or 'bridge'."));
        }
        if self.managed_process && !self.security.supplementary_groups.is_empty() {
            return Err(config("managed_process does not yet support supplementary groups"));
        }
        let feature_name = |name: &str| {
            let bytes = name.as_bytes();
            (1..=64).contains(&bytes.len())
                && bytes[0].is_ascii_lowercase()
                && bytes.iter().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-')
        };
        if self.required_features.len() > 64 || !self.required_features.iter().all(|name| feature_name(name)) {
            return Err(config("required_features must contain at most 64 feature names"));
        }
        self.validate_requirements()?;
        if self.dns_servers.len() > 3 {
            return Err(config("at most three DNS servers are supported"));
        }
        for server in &self.dns_servers {
            parse_ipv4(server)?;
        }
        if !self.dns_servers.is_empty() && self.network == "none" {
            return Err(config("DNS servers require bridge networking"));
        }
        self.ssh.validate()?;
        self.security.validate()?;
        self.filesystem.validate()?;
        for path in &self.linux_host.writable_paths {
            validate_setup_path("linux_host writable path", path)?;
        }
        if let Some(directory) = &self.working_dir {
            validate_guest_path("working_dir", directory)?;
        }
        if self.ssh.enabled && self.network != "bridge" {
            return Err(config("ssh-enabled sandboxes must use bridge networking."));
        }
        Ok(())
    }

    /// environment_contract.py: only `network-off` and `static-file-management`
    /// can be configured; every other named feature is a problem.
    fn validate_requirements(&self) -> Result<(), OciError> {
        let mut problems = Vec::new();
        for name in &self.required_features {
            let (status, reason): (&str, String) = match name.as_str() {
                "linux-kernel" => ("unsupported", "The current backend runs gVisor, not a guest Linux kernel.".into()),
                "system-boot" => ("unsupported", "No booted-system backend is implemented.".into()),
                "framework-network-policy" => ("unsupported", "Framework-aware egress enforcement is not implemented.".into()),
                "posix-acl" => ("unqualified", "Requires placement on a qualified runtime and filesystem; configuration alone does not establish support.".into()),
                "filesystem-xattrs" => ("unqualified", "Support depends on the filesystem and runtime version.".into()),
                "filesystem-locks" => ("unqualified", "Requires placement on a runtime qualified for the requested lock and lifecycle operations.".into()),
                "filesystem-notifications" => ("unqualified", "Notification behavior has not been qualified across lifecycle operations.".into()),
                "nested-containers" => ("unqualified", "Nested container requirements have not been qualified.".into()),
                "network-off" => (
                    if self.network == "none" { "configured" } else { "unsupported" },
                    format!("Selected network mode is {}.", self.network),
                ),
                "static-file-management" => (
                    if self.filesystem.management_helper == "static" { "configured" } else { "unsupported" },
                    "Requires the updated static supervisor artifact on the selected node.".into(),
                ),
                _ => ("unknown", "Unknown feature name.".into()),
            };
            if status != "configured" {
                problems.push(format!("{name} ({status}): {reason}"));
            }
        }
        if problems.is_empty() {
            return Ok(());
        }
        Err(config(format!("environment requirements cannot be satisfied: {}", problems.join("; "))))
    }

    /// `"none" if network == "none" else "sandbox"`, the node mode a spec needs.
    pub fn network_mode(&self) -> NetworkMode {
        if self.network == "none" { NetworkMode::None } else { NetworkMode::Sandbox }
    }
}

fn optional<T>(
    raw: &Map<String, Value>,
    key: &str,
    parse: fn(&Value, &str) -> Result<T, OciError>,
    default: T,
) -> Result<T, OciError> {
    match raw.get(key) {
        None => Ok(default),
        Some(value) => parse(value, key),
    }
}

/// `parse(raw[key]) if raw.get(key) is not None else None`.
fn nullable<T>(raw: &Map<String, Value>, key: &str, parse: fn(&Value, &str) -> Result<T, OciError>) -> Result<Option<T>, OciError> {
    match raw.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => parse(value, key).map(Some),
    }
}

fn json_object<'a>(raw: &'a Value, name: &str, fields: &[&str]) -> Result<&'a Map<String, Value>, OciError> {
    let raw = raw.as_object().ok_or_else(|| config(format!("{name} must be a JSON object")))?;
    let mut unsupported: Vec<&str> = raw.keys().map(String::as_str).filter(|key| !fields.contains(key)).collect();
    if !unsupported.is_empty() {
        unsupported.sort();
        return Err(config(format!("unsupported {name} fields: {}", unsupported.join(", "))));
    }
    Ok(raw)
}

fn json_bool(raw: &Value, name: &str) -> Result<bool, OciError> {
    raw.as_bool().ok_or_else(|| config(format!("{name} must be a boolean")))
}

/// `type(raw) is int`. Integers beyond i64 are rejected here (Python accepts them).
fn json_int(raw: &Value, name: &str) -> Result<i64, OciError> {
    raw.as_i64().ok_or_else(|| config(format!("{name} must be an integer")))
}

fn json_number(raw: &Value, name: &str) -> Result<f64, OciError> {
    raw.as_f64().ok_or_else(|| config(format!("{name} must be a number")))
}

fn json_string(raw: &Value, name: &str) -> Result<String, OciError> {
    raw.as_str().map(str::to_string).ok_or_else(|| config(format!("{name} must be a string")))
}

fn json_string_list(raw: &Value, name: &str) -> Result<Vec<String>, OciError> {
    let error = || config(format!("{name} must be a list of strings"));
    raw.as_array().ok_or_else(error)?.iter().map(|item| item.as_str().map(str::to_string).ok_or_else(error)).collect()
}

fn json_string_map(raw: &Value, name: &str) -> Result<BTreeMap<String, String>, OciError> {
    let error = || config(format!("{name} must be an object of string values"));
    raw.as_object()
        .ok_or_else(error)?
        .iter()
        .map(|(key, value)| value.as_str().map(|value| (key.clone(), value.to_string())).ok_or_else(error))
        .collect()
}

pub(crate) fn is_digest(value: &str) -> bool {
    value.len() == 71
        && value.starts_with("sha256:")
        && value.as_bytes()[7..].iter().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(b))
}

fn is_env_key(key: &str) -> bool {
    let bytes = key.as_bytes();
    !bytes.is_empty()
        && (bytes[0].is_ascii_alphabetic() || bytes[0] == b'_')
        && bytes.iter().all(|b| b.is_ascii_alphanumeric() || *b == b'_')
}

/// `ipaddress.IPv4Address(text)`: four decimal octets, no leading zeros.
pub(crate) fn parse_ipv4(text: &str) -> Result<Ipv4Addr, OciError> {
    text.parse::<Ipv4Addr>().map_err(|_| config(format!("Expected 4 octets in {}", py_repr(text))))
}

fn validate_security_value(name: &str, value: &str) -> Result<(), OciError> {
    if value.is_empty() {
        return Err(config(format!("{name} cannot be empty.")));
    }
    if value.contains('\n') || value.contains('\r') {
        return Err(config(format!("{name} cannot contain newlines.")));
    }
    if !value.bytes().all(|b| b.is_ascii_alphanumeric() || b"_.:@/-".contains(&b)) {
        return Err(config(format!("{name} contains unsupported characters.")));
    }
    Ok(())
}

/// guest_paths.py `validate_guest_path`.
pub fn validate_guest_path(name: &str, value: &str) -> Result<(), OciError> {
    if !value.starts_with('/') {
        return Err(config(format!("{name} must be an absolute container path.")));
    }
    if value.chars().any(|c| (c as u32) < 0x20 || c as u32 == 0x7f) {
        return Err(config(format!("{name} contains unsupported control characters.")));
    }
    if value.split('/').any(|part| part == "..") {
        return Err(config(format!("{name} cannot contain '..'.")));
    }
    Ok(())
}

fn validate_setup_path(name: &str, value: &str) -> Result<(), OciError> {
    validate_guest_path(name, value)?;
    if value != "/" && value[1..].split('/').any(|part| part.is_empty() || part == ".") {
        return Err(config(format!("{name} must be a canonical absolute path.")));
    }
    if value.contains(':') || value.contains(',') {
        return Err(config(format!("{name} contains unsupported delimiters.")));
    }
    Ok(())
}

fn validate_workspace_path(value: &str) -> Result<(), OciError> {
    const ROOTS: [&str; 15] = [
        "/", "/etc", "/bin", "/sbin", "/lib", "/lib64", "/usr", "/var", "/home", "/root", "/tmp", "/opt", "/boot",
        "/.ucloud-init", "/.ucloud-job-init",
    ];
    const TREES: [&str; 5] = ["/proc", "/sys", "/dev", "/run", "/.ucloud-managed"];
    validate_setup_path("workspace_path", value)?;
    if ROOTS.contains(&value) || TREES.iter().any(|root| value == *root || value.starts_with(&format!("{root}/"))) {
        return Err(config("workspace_path overlaps a reserved system or runtime path."));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Guest identity (guest_identity.py)

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct GuestIdentity {
    pub uid: u32,
    pub gid: u32,
    pub name: String,
    pub home: String,
    pub shell: String,
}

const ACCOUNT_FILE_LIMIT: u64 = 1024 * 1024;

/// Python `str.splitlines()`.
fn split_lines(text: &str) -> Vec<&str> {
    let mut lines = Vec::new();
    let mut start = 0;
    let mut chars = text.char_indices().peekable();
    while let Some((index, character)) = chars.next() {
        if matches!(
            character,
            '\n' | '\r' | '\u{0b}' | '\u{0c}' | '\u{1c}' | '\u{1d}' | '\u{1e}' | '\u{85}' | '\u{2028}' | '\u{2029}'
        ) {
            lines.push(&text[start..index]);
            let mut end = index + character.len_utf8();
            if character == '\r' && chars.peek().is_some_and(|(_, next)| *next == '\n') {
                chars.next();
                end += 1;
            }
            start = end;
        }
    }
    if start < text.len() {
        lines.push(&text[start..]);
    }
    lines
}

/// `_account_file`: `<rootfs>/etc/<name>` without following symlinks, at most
/// 1 MiB, split into `:` fields. A missing file is empty.
fn account_file(rootfs: &Path, name: &str) -> Result<Vec<Vec<String>>, OciError> {
    let unsafe_read = || config("cannot safely read image account database");
    let root = guest::open_directory(rootfs)?;
    let etc = match guest::open_at(&root, "etc", libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_DIRECTORY, 0) {
        Ok(fd) => fd,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(vec![]),
        Err(_) => return Err(unsafe_read()),
    };
    let file = match guest::open_at(&etc, name, libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_NONBLOCK, 0) {
        Ok(fd) => std::fs::File::from(fd),
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(vec![]),
        Err(_) => return Err(unsafe_read()),
    };
    if !file.metadata().map_err(|_| unsafe_read())?.is_file() {
        return Err(config("image account database must be a regular file"));
    }
    let mut data = Vec::new();
    (&file).take(ACCOUNT_FILE_LIMIT + 1).read_to_end(&mut data).map_err(|_| unsafe_read())?;
    if data.len() as u64 > ACCOUNT_FILE_LIMIT {
        return Err(config("image account database exceeds 1 MiB"));
    }
    let text = String::from_utf8(data).map_err(|_| unsafe_read())?;
    Ok(split_lines(&text).into_iter().map(|line| line.split(':').map(str::to_string).collect()).collect())
}

fn is_ascii_decimal(value: &str) -> bool {
    !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit())
}

/// `_number`: an OCI uid/gid.
fn number(value: &str) -> Result<u32, OciError> {
    let out_of_range = || config("OCI uid/gid is out of range");
    if value.len() > 10 || !is_ascii_decimal(value) {
        return Err(out_of_range());
    }
    let parsed: u64 = value.parse().map_err(|_| out_of_range())?;
    if parsed > (1u64 << 32) - 2 {
        return Err(out_of_range());
    }
    Ok(parsed as u32)
}

fn group_id(groups: &mut Option<Vec<Vec<String>>>, rootfs: &Path, value: &str) -> Result<u32, OciError> {
    if is_ascii_decimal(value) {
        return number(value);
    }
    if groups.is_none() {
        *groups = Some(account_file(rootfs, "group")?);
    }
    let entry = groups.as_ref().expect("loaded").iter().find(|row| row.len() == 4 && row[0] == value);
    match entry {
        Some(entry) => number(&entry[2]),
        None => Err(config(format!("image group {} is absent from /etc/group", py_repr(value)))),
    }
}

/// `resolve_identity(rootfs, "user[:group]")` from the image's own databases.
pub fn resolve_identity(rootfs: &Path, value: &str) -> Result<GuestIdentity, OciError> {
    let (user, group) = match value.split_once(':') {
        Some((user, group)) => (user, Some(group)),
        None => (value, None),
    };
    if user.is_empty() || group.is_some_and(str::is_empty) {
        return Err(config("image user must be a name or numeric OCI user, optionally with group"));
    }
    let numeric_uid = if is_ascii_decimal(user) { Some(number(user)?) } else { None };
    let accounts = account_file(rootfs, "passwd")?;
    let matched = accounts.iter().find(|row| {
        row.len() == 7
            && match numeric_uid {
                Some(uid) => row[2] == uid.to_string(),
                None => row[0] == user,
            }
    });
    let mut identity = match (matched, numeric_uid) {
        (Some(row), _) => GuestIdentity {
            uid: number(&row[2])?,
            gid: number(&row[3])?,
            name: row[0].clone(),
            home: row[5].clone(),
            shell: row[6].clone(),
        },
        (None, Some(uid)) => GuestIdentity {
            uid,
            gid: uid,
            name: if uid == 0 { "root".into() } else { String::new() },
            home: if uid == 0 { "/root".into() } else { String::new() },
            shell: String::new(),
        },
        (None, None) => {
            return Err(config(format!(
                "image user {} is absent from /etc/passwd; specify a numeric OCI user",
                py_repr(user)
            )));
        }
    };
    if let Some(group) = group {
        identity.gid = group_id(&mut None, rootfs, group)?;
    }
    Ok(identity)
}

/// `resolve_groups`: explicit supplementary groups, deduplicated in order.
pub fn resolve_groups(rootfs: &Path, values: &[String]) -> Result<Vec<u32>, OciError> {
    let mut groups = None;
    let mut result = Vec::new();
    for value in values {
        let gid = group_id(&mut groups, rootfs, value)?;
        if !result.contains(&gid) {
            result.push(gid);
        }
    }
    Ok(result)
}

// ---------------------------------------------------------------------------
// The OCI config (direct_oci.py)

const LINUX_CAPABILITIES: [&str; 41] = [
    "AUDIT_CONTROL",
    "AUDIT_READ",
    "AUDIT_WRITE",
    "BLOCK_SUSPEND",
    "BPF",
    "CHECKPOINT_RESTORE",
    "CHOWN",
    "DAC_OVERRIDE",
    "DAC_READ_SEARCH",
    "FOWNER",
    "FSETID",
    "IPC_LOCK",
    "IPC_OWNER",
    "KILL",
    "LEASE",
    "LINUX_IMMUTABLE",
    "MAC_ADMIN",
    "MAC_OVERRIDE",
    "MKNOD",
    "NET_ADMIN",
    "NET_BIND_SERVICE",
    "NET_BROADCAST",
    "NET_RAW",
    "PERFMON",
    "SETFCAP",
    "SETGID",
    "SETPCAP",
    "SETUID",
    "SYSLOG",
    "SYS_ADMIN",
    "SYS_BOOT",
    "SYS_CHROOT",
    "SYS_MODULE",
    "SYS_NICE",
    "SYS_PACCT",
    "SYS_PTRACE",
    "SYS_RAWIO",
    "SYS_RESOURCE",
    "SYS_TIME",
    "SYS_TTY_CONFIG",
    "WAKE_ALARM",
];

const DEFAULT_CAPABILITIES: [&str; 14] = [
    "AUDIT_WRITE",
    "CHOWN",
    "DAC_OVERRIDE",
    "FOWNER",
    "FSETID",
    "KILL",
    "MKNOD",
    "NET_BIND_SERVICE",
    "NET_RAW",
    "SETFCAP",
    "SETGID",
    "SETPCAP",
    "SETUID",
    "SYS_CHROOT",
];

const DEFAULT_PATH: &str = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin";
pub const MEMORY_DIRECTORY_ANNOTATION: &str = "dev.gvisor.internal.application-memory-directory";

/// The `--network` mode of the node: `none` or `sandbox`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NetworkMode {
    None,
    Sandbox,
}

/// `(?:CAP_)?([A-Z0-9_]+)` over the upper-cased name, a known capability.
fn capability(raw: &str) -> Result<String, OciError> {
    let upper = raw.to_uppercase();
    let valid = |name: &str| !name.is_empty() && name.bytes().all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_');
    let name = match upper.strip_prefix("CAP_") {
        Some(rest) if valid(rest) => rest,
        _ if valid(&upper) => upper.as_str(),
        _ => return Err(config(format!("unsupported Linux capability: {raw}"))),
    };
    if !LINUX_CAPABILITIES.contains(&name) {
        return Err(config(format!("unsupported Linux capability: {raw}")));
    }
    Ok(format!("CAP_{name}"))
}

fn capabilities(security: &SecuritySpec) -> Result<BTreeSet<String>, OciError> {
    let mut set: BTreeSet<String> = DEFAULT_CAPABILITIES.iter().map(|name| format!("CAP_{name}")).collect();
    for raw in &security.cap_drop {
        if raw.to_uppercase() == "ALL" {
            set.clear();
            continue;
        }
        set.remove(&capability(raw)?);
    }
    for raw in &security.cap_add {
        if raw.to_uppercase() == "ALL" {
            set.extend(LINUX_CAPABILITIES.iter().map(|name| format!("CAP_{name}")));
            continue;
        }
        set.insert(capability(raw)?);
    }
    Ok(set)
}

/// `_validate_init_stat`: a root-owned, executable regular file nobody else may write.
pub fn validate_init_metadata(meta: &std::fs::Metadata) -> Result<(), OciError> {
    let mode = meta.permissions().mode();
    if !meta.is_file() || meta.uid() != 0 || mode & 0o111 == 0 || mode & 0o022 != 0 {
        return Err(config("direct-runtime init binary must be root-owned, executable, and immutable"));
    }
    Ok(())
}

/// `_validate_init_binary`: the trusted node artifact, never a symlink.
pub fn validate_init_binary(path: &Path) -> Result<(), OciError> {
    let meta = std::fs::metadata(path).map_err(|_| config("direct-runtime init binary is unavailable"))?;
    if std::fs::symlink_metadata(path).map(|link| link.file_type().is_symlink()).unwrap_or(false) {
        return Err(config("direct-runtime init binary must be root-owned, executable, and immutable"));
    }
    validate_init_metadata(&meta)
}

fn process_args(spec: &SandboxSpec, image: &ImageConfig) -> Result<Vec<String>, OciError> {
    let command = if spec.command.is_empty() { &image.command } else { &spec.command };
    let args: Vec<String> = image.entrypoint.iter().chain(command.iter()).cloned().collect();
    if args.first().is_none_or(String::is_empty) {
        return Err(config("sandbox image and request do not define an initial process"));
    }
    if args.iter().any(|item| item.contains('\0')) {
        return Err(config("sandbox process arguments contain NUL"));
    }
    Ok(args)
}

fn image_environment(spec: &SandboxSpec, image: &ImageConfig) -> Result<BTreeMap<String, String>, OciError> {
    let mut environment = BTreeMap::new();
    for item in &image.env {
        let Some((key, value)) = item.split_once('=') else {
            return Err(config("Docker image contains an invalid environment"));
        };
        if !is_env_key(key) || value.contains('\0') {
            return Err(config("Docker image contains an invalid environment"));
        }
        environment.insert(key.to_string(), value.to_string());
    }
    environment.extend(spec.env.iter().map(|(key, value)| (key.clone(), value.clone())));
    Ok(environment)
}

/// `_working_directory`: the OCI cwd (empty strings fall through, as in Python).
pub fn working_directory(spec: &SandboxSpec, image: &ImageConfig) -> Result<String, OciError> {
    let directory = spec
        .working_dir
        .as_deref()
        .filter(|directory| !directory.is_empty())
        .or(Some(image.working_dir.as_str()).filter(|directory| !directory.is_empty()))
        .unwrap_or(if spec.profile == "linux_session" { spec.filesystem.workspace_path.as_str() } else { "/" });
    if !directory.starts_with('/') || directory.contains('\0') {
        return Err(config("sandbox working directory must be absolute"));
    }
    Ok(directory.to_string())
}

fn linux_host_environment(spec: &SandboxSpec) -> BTreeMap<String, String> {
    let flag = |value: bool| if value { "1" } else { "0" }.to_string();
    let mut values = BTreeMap::from([
        ("UCLOUD_SANDBOX_ENABLE_CRON".to_string(), flag(spec.linux_host.enable_cron)),
        ("UCLOUD_SANDBOX_ENABLE_SSHD".to_string(), flag(spec.linux_host.enable_sshd || spec.ssh.enabled)),
        ("UCLOUD_SANDBOX_KEEP_ALIVE".to_string(), flag(spec.linux_host.keep_alive)),
        ("UCLOUD_SANDBOX_LINUX_HOST_PATHS".to_string(), spec.linux_host.writable_paths.join(":")),
        ("UCLOUD_SANDBOX_PROFILE".to_string(), spec.profile.clone()),
        ("UCLOUD_SANDBOX_SSH_PORT".to_string(), spec.ssh.container_port.to_string()),
        ("UCLOUD_SANDBOX_SSH_USER".to_string(), spec.ssh.user.clone()),
    ]);
    if !spec.ssh.authorized_keys.is_empty() {
        values.insert("UCLOUD_SANDBOX_SSH_AUTHORIZED_KEYS".into(), spec.ssh.authorized_keys.join("\n"));
    }
    values
}

fn mebibytes(value: i64, name: &str) -> Result<i64, OciError> {
    value.checked_mul(1024 * 1024).ok_or_else(|| config(format!("{name} is out of range")))
}

fn mounts(spec: &SandboxSpec) -> Result<Vec<Value>, OciError> {
    let filesystem = &spec.filesystem;
    let mount = |destination: &str, options: Vec<String>, source: &str, kind: &str| {
        json!({"destination": destination, "options": options, "source": source, "type": kind})
    };
    let strings = |items: &[&str]| items.iter().map(|item| item.to_string()).collect::<Vec<_>>();
    let shm_kib = filesystem.shm_mb.checked_mul(1024).ok_or_else(|| config("shm_mb is out of range"))?;
    let mut mounts = vec![
        mount("/proc", strings(&["nosuid", "noexec", "nodev"]), "proc", "proc"),
        mount("/dev", strings(&["nosuid", "strictatime", "mode=755", "size=65536k"]), "tmpfs", "tmpfs"),
        mount(
            "/dev/pts",
            strings(&["nosuid", "noexec", "newinstance", "ptmxmode=0666", "mode=0620", "gid=5"]),
            "devpts",
            "devpts",
        ),
        mount(
            "/dev/shm",
            [strings(&["nosuid", "noexec", "nodev", "mode=1777"]), vec![format!("size={shm_kib}k")]].concat(),
            "shm",
            "tmpfs",
        ),
        mount("/dev/mqueue", strings(&["nosuid", "noexec", "nodev"]), "mqueue", "mqueue"),
        mount("/sys", strings(&["nosuid", "noexec", "nodev", "ro"]), "sysfs", "sysfs"),
        mount(
            "/tmp",
            [strings(&["nosuid", "nodev", "mode=1777"]), vec![format!("size={}", mebibytes(filesystem.tmpfs_mb, "tmpfs_mb")?)]]
                .concat(),
            "tmpfs",
            "tmpfs",
        ),
        mount(
            "/run",
            [strings(&["nosuid", "nodev", "mode=755"]), vec![format!("size={}", mebibytes(filesystem.run_tmpfs_mb, "run_tmpfs_mb")?)]]
                .concat(),
            "tmpfs",
            "tmpfs",
        ),
    ];
    if filesystem.workspace_is_tmpfs() {
        let disk_mb = spec.disk_mb.ok_or_else(|| config("direct sandboxes require explicit memory_mb and disk_mb limits"))?;
        mounts.push(mount(
            &filesystem.workspace_path,
            [strings(&["nosuid", "nodev", "mode=1777"]), vec![format!("size={}", mebibytes(disk_mb, "disk_mb")?)]].concat(),
            "tmpfs",
            "tmpfs",
        ));
    }
    Ok(mounts)
}

/// `DirectOciConfigBuilder`: translate the sandbox contract into a deterministic OCI config.
#[derive(Debug, Clone)]
pub struct OciBuilder {
    pub init_binary: Option<PathBuf>,
    pub managed_init_binary: Option<PathBuf>,
    pub network_mode: NetworkMode,
}

impl OciBuilder {
    pub fn new(init_binary: Option<PathBuf>, managed_init_binary: Option<PathBuf>, network_mode: NetworkMode) -> Result<Self, OciError> {
        if init_binary.as_ref().is_some_and(|path| !path.is_absolute()) {
            return Err(config("direct runtime init binary must be absolute"));
        }
        if managed_init_binary.as_ref().is_some_and(|path| !path.is_absolute()) {
            return Err(config("managed-process init binary must be absolute"));
        }
        Ok(OciBuilder { init_binary, managed_init_binary, network_mode })
    }

    /// `build(spec, image, network_namespace_path)`. Validates the spec as
    /// Python does. Deviation: the static file helper's `files ready` probe is
    /// not rerun here; run `validate_management_helper` once before (S2.1,
    /// where Python runs it too). The binary's ownership checks still run.
    pub fn build(&self, spec: &SandboxSpec, image: &MaterializedRootfs, network_namespace_path: Option<&Path>) -> Result<Value, OciError> {
        spec.validate()?;
        let (Some(memory_mb), Some(_)) = (spec.memory_mb, spec.disk_mb) else {
            return Err(config("direct sandboxes require explicit memory_mb and disk_mb limits"));
        };
        self.validate_management_helper_binary(spec)?;
        if spec.ssh.enabled {
            return Err(config("direct runtime SSH requires node network integration"));
        }
        let image_config = &image.image_config;
        let user = spec
            .security
            .user
            .as_deref()
            .or(Some(image_config.user.as_str()).filter(|user| !user.is_empty()))
            .unwrap_or("0");
        let identity = resolve_identity(&image.rootfs, user)?;
        let supplementary_gids = resolve_groups(&image.rootfs, &spec.security.supplementary_groups)?;
        let linux_profile = spec.profile == "linux_host" || spec.profile == "linux_session";
        let mut args = if spec.managed_process || linux_profile { vec![] } else { process_args(spec, image_config)? };
        let mut environment = image_environment(spec, image_config)?;
        let absolute_home = identity.home.starts_with('/');
        environment.entry("PATH".into()).or_insert_with(|| DEFAULT_PATH.into());
        if absolute_home {
            environment.entry("HOME".into()).or_insert_with(|| identity.home.clone());
        } else if spec.profile == "linux_session" {
            environment.entry("HOME".into()).or_insert_with(|| spec.filesystem.workspace_path.clone());
        }
        if !identity.name.is_empty() {
            environment.entry("USER".into()).or_insert_with(|| identity.name.clone());
            environment.entry("LOGNAME".into()).or_insert_with(|| identity.name.clone());
        }
        if identity.shell.starts_with('/') {
            environment.entry("SHELL".into()).or_insert_with(|| identity.shell.clone());
        }
        if spec.profile == "linux_session" {
            let home = spec.env.get("HOME").cloned().unwrap_or_else(|| {
                if absolute_home { identity.home.clone() } else { spec.filesystem.workspace_path.clone() }
            });
            environment.insert("HOME".into(), home);
        }
        if linux_profile {
            args = ["/bin/sh", "-c", LINUX_HOST_ENTRYPOINT_SCRIPT, "ucloud-linux-host"]
                .iter()
                .map(|item| item.to_string())
                .chain(spec.command.iter().cloned())
                .collect();
            environment.extend(linux_host_environment(spec));
        }
        let mounts = mounts(spec)?;
        let (mut managed_uid, mut managed_gid) = (0, 0);
        if spec.managed_process {
            let binary = self
                .managed_init_binary
                .as_ref()
                .ok_or_else(|| config("managed_process requires a configured managed-process init binary"))?;
            validate_init_binary(binary)?;
            (managed_uid, managed_gid) = (identity.uid, identity.gid);
            args = ["/.ucloud-job-init", "supervise", "--state-dir", "/.ucloud-managed"].iter().map(|item| item.to_string()).collect();
        } else if spec.security.init {
            let binary =
                self.init_binary.as_ref().ok_or_else(|| config("security.init requires a configured direct-runtime init binary"))?;
            validate_init_binary(binary)?;
            args.splice(0..0, ["/.ucloud-init".to_string(), "--".to_string()]);
        }
        let (uid, gid) = if spec.managed_process { (0, 0) } else { (identity.uid, identity.gid) };
        let mut capabilities = capabilities(&spec.security)?;
        if spec.managed_process {
            capabilities.extend(["CAP_SETUID".to_string(), "CAP_SETGID".to_string()]);
        }
        let memory_bytes = mebibytes(memory_mb, "memory_mb")?;
        let swap = memory_bytes.checked_mul(2).ok_or_else(|| config("memory_mb is out of range"))?;
        let mut resources = json!({"memory": {"limit": memory_bytes, "swap": swap}});
        if let Some(cpus) = spec.cpus {
            // Python's round() is round-half-to-even.
            let quota = (cpus * 100_000.0).round_ties_even().max(1.0);
            if !quota.is_finite() || quota > i64::MAX as f64 {
                return Err(config("cpus is out of range"));
            }
            resources["cpu"] = json!({"period": 100_000, "quota": quota as i64});
        }
        if let Some(limit) = spec.security.pids_limit {
            resources["pids"] = json!({"limit": limit});
        }
        let cwd = working_directory(spec, image_config)?;
        let mut annotations = BTreeMap::from([
            ("dev.ucloud-sandboxes.image-id".to_string(), image.image_id.clone()),
            ("dev.ucloud-sandboxes.profile".to_string(), spec.profile.clone()),
            ("dev.ucloud-sandboxes.sandbox-id".to_string(), spec.id.clone()),
        ]);
        if spec.filesystem.management_helper == "static" {
            annotations.insert("dev.ucloud-sandboxes.file-helper".into(), "v1".into());
        }
        if spec.managed_process {
            annotations.insert("dev.ucloud-sandboxes.managed-process".into(), "v1".into());
            annotations.insert("dev.ucloud-sandboxes.managed-process.uid".into(), managed_uid.to_string());
            annotations.insert("dev.ucloud-sandboxes.managed-process.gid".into(), managed_gid.to_string());
            annotations.insert("dev.ucloud-sandboxes.managed-process.cwd".into(), cwd.clone());
        }
        for (key, value) in &spec.labels {
            annotations.insert(format!("dev.ucloud-sandboxes.label.{key}"), value.clone());
        }
        let network = match self.network_mode {
            NetworkMode::Sandbox => {
                if spec.network == "none" {
                    return Err(config("network=none sandbox cannot use the node sandbox network mode"));
                }
                let path = network_namespace_path
                    .filter(|path| path.is_absolute())
                    .and_then(Path::to_str)
                    .filter(|path| !path.contains('\0'))
                    .ok_or_else(|| config("sandbox networking requires an absolute network namespace path"))?;
                json!({"type": "network", "path": path})
            }
            NetworkMode::None => {
                if network_namespace_path.is_some() {
                    return Err(config("network namespace path requires sandbox networking"));
                }
                json!({"type": "network"})
            }
        };
        let mut user = json!({"gid": gid, "uid": uid});
        if !supplementary_gids.is_empty() {
            user["additionalGids"] = json!(supplementary_gids);
        }
        let capability_list: Vec<&String> = capabilities.iter().collect();
        Ok(json!({
            "annotations": annotations,
            "hostname": spec.id,
            "linux": {
                "maskedPaths": [
                    "/proc/acpi", "/proc/asound", "/proc/kcore", "/proc/keys", "/proc/latency_stats",
                    "/proc/timer_list", "/proc/timer_stats", "/proc/sched_debug", "/sys/firmware",
                ],
                "namespaces": [{"type": "pid"}, network, {"type": "ipc"}, {"type": "uts"}, {"type": "mount"}],
                "readonlyPaths": ["/proc/bus", "/proc/fs", "/proc/irq", "/proc/sys", "/proc/sysrq-trigger"],
                "resources": resources,
            },
            "mounts": mounts,
            "ociVersion": "1.0.2",
            "process": {
                "args": args,
                "capabilities": {
                    "bounding": capability_list,
                    "effective": capability_list,
                    "inheritable": capability_list,
                    "permitted": capability_list,
                },
                "cwd": cwd,
                "env": environment.iter().map(|(key, value)| format!("{key}={value}")).collect::<Vec<_>>(),
                "noNewPrivileges": spec.security.no_new_privileges,
                "rlimits": [{"hard": 1_048_576, "soft": 1_048_576, "type": "RLIMIT_NOFILE"}],
                "terminal": false,
                "user": user,
            },
            "root": {"path": "rootfs", "readonly": spec.security.read_only_rootfs},
        }))
    }

    fn validate_management_helper_binary(&self, spec: &SandboxSpec) -> Result<Option<PathBuf>, OciError> {
        if spec.filesystem.management_helper != "static" {
            return Ok(None);
        }
        let binary = self
            .managed_init_binary
            .as_ref()
            .ok_or_else(|| config("static file management requires an updated managed-process binary"))?;
        validate_init_binary(binary)?;
        Ok(Some(binary.clone()))
    }

    /// `validate_management_helper`: for the static helper, the trusted
    /// managed init must pass its checks and answer `files ready` with exit 0
    /// within 5 s (stdin, stdout and stderr on /dev/null).
    pub async fn validate_management_helper(&self, spec: &SandboxSpec) -> Result<(), OciError> {
        let Some(binary) = self.validate_management_helper_binary(spec)? else { return Ok(()) };
        let probe_failed = || config("static file helper protocol probe failed");
        let mut child = tokio::process::Command::new(&binary)
            .args(["files", "ready"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .map_err(|_| probe_failed())?;
        let status = match tokio::time::timeout(Duration::from_secs(5), child.wait()).await {
            Ok(status) => status.map_err(|_| probe_failed())?,
            Err(_) => {
                let _ = child.kill().await;
                return Err(probe_failed());
            }
        };
        if !status.success() {
            return Err(config("configured binary does not support static file management"));
        }
        Ok(())
    }
}

/// sandbox.py `linux_host_entrypoint_script()`, verbatim.
pub const LINUX_HOST_ENTRYPOINT_SCRIPT: &str = r#"set -eu

prepare_paths() {
  old_ifs="$IFS"
  IFS=:
  set -f
  for path in ${UCLOUD_SANDBOX_LINUX_HOST_PATHS:-}; do
    [ -n "$path" ] || continue
    if [ ! -d "$path" ]; then
      mkdir -p -- "$path" || { echo "cannot prepare sandbox directory: $path" >&2; exit 1; }
    fi
  done
  set +f
  IFS="$old_ifs"
}

start_cron() {
  [ "${UCLOUD_SANDBOX_ENABLE_CRON:-0}" = "1" ] || return 0
  if command -v cron >/dev/null 2>&1; then
    cron
  elif command -v crond >/dev/null 2>&1; then
    crond
  else
    echo "cron was requested but neither cron nor crond is installed" >&2
    exit 1
  fi
}

start_sshd() {
  [ "${UCLOUD_SANDBOX_ENABLE_SSHD:-0}" = "1" ] || return 0
  user="${UCLOUD_SANDBOX_SSH_USER:-root}"
  home_dir="$(getent passwd "$user" | awk -F: '{print $6}')"
  [ -n "$home_dir" ] || { echo "SSH user has no home: $user" >&2; exit 1; }
  mkdir -p "$home_dir/.ssh" /run/sshd
  if [ -n "${UCLOUD_SANDBOX_SSH_AUTHORIZED_KEYS:-}" ]; then
    printf '%s\n' "$UCLOUD_SANDBOX_SSH_AUTHORIZED_KEYS" > "$home_dir/.ssh/authorized_keys"
    chmod 700 "$home_dir/.ssh"
    chmod 600 "$home_dir/.ssh/authorized_keys"
    chown "$user" "$home_dir/.ssh" "$home_dir/.ssh/authorized_keys"
  fi
  ssh-keygen -A
  if command -v sshd >/dev/null 2>&1; then
    sshd_path="$(command -v sshd)"
  elif [ -x /usr/sbin/sshd ]; then
    sshd_path=/usr/sbin/sshd
  else
    echo "sshd was requested but is not installed" >&2
    exit 1
  fi
  "$sshd_path" -t
  "$sshd_path" -p "${UCLOUD_SANDBOX_SSH_PORT:-22}"
}

prepare_paths
start_cron
start_sshd

if [ "$#" -gt 0 ]; then
  exec "$@"
fi

[ "${UCLOUD_SANDBOX_KEEP_ALIVE:-1}" = "1" ] || exit 0
trap 'exit 0' INT TERM
while :; do
  sleep 3600 &
  wait "$!" || true
done
"#;
