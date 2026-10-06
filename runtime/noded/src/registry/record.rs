//! Stored records: `DirectSandboxRegistration` and its `record_json`, the
//! drain metadata, and the small value types of direct_registry.py.
//!
//! A stored row is valid only when decoding and re-encoding it reproduces its
//! bytes (Python's `_decode`), so the encoder here must be Python's exactly:
//! canonical JSON of `to_dict()`, with v3 records omitting the split fields.

use serde_json::{Map, Value};

use super::error::{RegistryError, Result};
use super::spec::{SandboxSpec, is_digest, is_prefixed_digest, python_strip};
use crate::pyjson;

pub const DIRECT_REGISTRATION_VERSION: i64 = 3;
pub const SPLIT_REGISTRATION_VERSION: i64 = 4;
pub const MIB: i64 = 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Phase {
    Planned,
    ImportPlanned,
    QuotaReady,
    Importing,
    RootfsReady,
    ImportReady,
    Owned,
    MovingOut,
    Deleting,
}

impl Phase {
    pub const ALL: [Phase; 9] = [
        Phase::Planned,
        Phase::ImportPlanned,
        Phase::QuotaReady,
        Phase::Importing,
        Phase::RootfsReady,
        Phase::ImportReady,
        Phase::Owned,
        Phase::MovingOut,
        Phase::Deleting,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Phase::Planned => "planned",
            Phase::ImportPlanned => "import_planned",
            Phase::QuotaReady => "quota_ready",
            Phase::Importing => "importing",
            Phase::RootfsReady => "rootfs_ready",
            Phase::ImportReady => "import_ready",
            Phase::Owned => "owned",
            Phase::MovingOut => "moving_out",
            Phase::Deleting => "deleting",
        }
    }

    pub fn parse(text: &str) -> Option<Phase> {
        Phase::ALL.into_iter().find(|phase| phase.as_str() == text)
    }

    /// `_ROOTFS_PHASES`: phases that own a quota and a rootfs.
    pub fn owns_rootfs(self) -> bool {
        matches!(self, Phase::RootfsReady | Phase::ImportReady | Phase::Owned | Phase::MovingOut)
    }
}

/// `DirectSandboxRegistration`. Fields are public for transitions; every
/// write re-runs `validate` (Python's `__post_init__`) before encoding.
#[derive(Clone, Debug, PartialEq)]
pub struct Registration {
    pub spec: SandboxSpec,
    pub sandbox_generation: i64,
    pub operation_id: String,
    pub runtime_compatibility_sha256: String,
    pub phase: Phase,
    pub revision: i64,
    pub created_ns: i64,
    pub updated_ns: i64,
    pub quota_project_id: Option<i64>,
    pub quota_total_mb: Option<i64>,
    pub quota_path: String,
    pub image_id: String,
    pub rootfs_sha256: String,
    pub container_id: String,
    pub bundle: String,
    pub memory_directory: String,
    pub workspace_directory: String,
    pub memory_allocation_id: String,
    pub migration_id: String,
    pub migration_sha256: String,
    pub version: i64,
}

/// `OPERATION_ID_RE.fullmatch`: `[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}`.
pub fn is_operation_id(text: &str) -> bool {
    let bytes = text.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 128
        && bytes[0].is_ascii_alphanumeric()
        && bytes[1..].iter().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b':' | b'-'))
}

const FIELDS: [&str; 21] = [
    "bundle",
    "container_id",
    "created_ns",
    "image_id",
    "memory_allocation_id",
    "memory_directory",
    "migration_id",
    "migration_sha256",
    "operation_id",
    "phase",
    "quota_path",
    "quota_project_id",
    "quota_total_mb",
    "revision",
    "rootfs_sha256",
    "runtime_compatibility_sha256",
    "sandbox_generation",
    "spec",
    "updated_ns",
    "version",
    "workspace_directory",
];
const SPLIT_FIELDS: [&str; 2] = ["memory_allocation_id", "workspace_directory"];
const INTEGER_FIELDS: [&str; 5] = ["created_ns", "revision", "sandbox_generation", "updated_ns", "version"];
const OPTIONAL_INTEGER_FIELDS: [&str; 2] = ["quota_project_id", "quota_total_mb"];

impl Registration {
    pub fn sandbox_id(&self) -> &str {
        self.spec.id()
    }

    pub fn spec_sha256(&self) -> String {
        self.spec.sha256()
    }

    /// The incarnation name, `"{id}.sandbox-{generation}"`.
    pub fn incarnation(&self) -> String {
        format!("{}.sandbox-{}", self.sandbox_id(), self.sandbox_generation)
    }

    /// Canonical storage identity, including pre-materialization records.
    pub fn workspace_volume_id(&self) -> String {
        if !self.workspace_directory.is_empty() {
            self.workspace_directory.clone()
        } else if !self.memory_directory.is_empty() {
            self.memory_directory.clone()
        } else {
            self.incarnation()
        }
    }

    pub fn has_direct_sandbox(&self) -> bool {
        !self.container_id.is_empty()
    }

    /// `memory_reference`: the split allocation's ID and its byte bound.
    pub fn memory_reference(&self) -> Option<(String, i64)> {
        if self.memory_allocation_id.is_empty() {
            return None;
        }
        let requested = self.spec.requested_disk_mb().ok()?;
        Some((self.memory_allocation_id.clone(), (requested - self.spec.disk_mb()?) * MIB))
    }

    /// Python's `__post_init__`, in its order; the first failure wins.
    pub fn validate(&self) -> std::result::Result<(), String> {
        if self.version != DIRECT_REGISTRATION_VERSION && self.version != SPLIT_REGISTRATION_VERSION {
            return Err("unsupported direct registration version".into());
        }
        if self.version == DIRECT_REGISTRATION_VERSION
            && (!self.workspace_directory.is_empty() || !self.memory_allocation_id.is_empty())
        {
            return Err("legacy registration cannot contain split backing".into());
        }
        let incarnation = self.incarnation();
        if self.version == SPLIT_REGISTRATION_VERSION
            && (self.workspace_directory != format!("workspace-{incarnation}")
                || self.memory_allocation_id != incarnation)
        {
            return Err("split registration has invalid component identities".into());
        }
        self.spec.validate()?;
        if self.sandbox_generation <= 0 {
            return Err("sandbox generation must be positive".into());
        }
        if !is_operation_id(&self.operation_id) {
            return Err("direct registration operation id is invalid".into());
        }
        if !is_digest(&self.runtime_compatibility_sha256) {
            return Err("direct registration runtime compatibility is invalid".into());
        }
        if !self.migration_id.is_empty() && !is_operation_id(&self.migration_id) {
            return Err("direct registration migration id is invalid".into());
        }
        if !self.migration_sha256.is_empty() && !is_digest(&self.migration_sha256) {
            return Err("direct registration migration digest is invalid".into());
        }
        let migrating = !self.migration_id.is_empty();
        let migration_phase =
            matches!(self.phase, Phase::ImportPlanned | Phase::Importing | Phase::ImportReady | Phase::MovingOut)
                || (matches!(self.phase, Phase::RootfsReady | Phase::Owned | Phase::Deleting) && migrating);
        if migrating != !self.migration_sha256.is_empty() || migration_phase != migrating {
            return Err("direct registration migration ownership is invalid".into());
        }
        if self.revision < 1 || self.created_ns < 1 || self.updated_ns < 1 {
            return Err("direct registration revision/timestamp is invalid".into());
        }
        let quota_parts = [self.quota_project_id.is_some(), self.quota_total_mb.is_some(), !self.quota_path.is_empty()];
        let quota_present = quota_parts.iter().all(|part| *part);
        if quota_parts.iter().any(|part| *part) != quota_present {
            return Err("direct registration quota identity is incomplete".into());
        }
        if quota_present {
            if self.quota_project_id < Some(1) || self.quota_total_mb < Some(1) {
                return Err("direct registration quota bounds are invalid".into());
            }
            if !self.quota_path.starts_with('/') {
                return Err("direct registration quota path must be absolute".into());
            }
        }
        let rootfs_parts =
            [&self.image_id, &self.rootfs_sha256, &self.container_id, &self.bundle, &self.memory_directory]
                .map(|value| !value.is_empty());
        let rootfs_present = rootfs_parts.iter().all(|part| *part);
        if rootfs_parts.iter().any(|part| *part) != rootfs_present {
            return Err("direct registration rootfs identity is incomplete".into());
        }
        if rootfs_present {
            if !is_prefixed_digest(&self.image_id) {
                return Err("direct registration image id is invalid".into());
            }
            if !is_digest(&self.rootfs_sha256) {
                return Err("direct registration rootfs digest is invalid".into());
            }
            if !is_digest(&self.container_id) {
                return Err("direct registration container id is invalid".into());
            }
            if !self.bundle.starts_with('/') {
                return Err("direct registration bundle must be absolute".into());
            }
            if self.memory_directory.contains('/') {
                return Err("direct registration memory directory is invalid".into());
            }
        }
        if matches!(self.phase, Phase::Planned | Phase::ImportPlanned) && (quota_present || rootfs_present) {
            return Err("planned direct registration owns external state".into());
        }
        if matches!(self.phase, Phase::QuotaReady | Phase::Importing) && (!quota_present || rootfs_present) {
            return Err("quota-ready direct registration is inconsistent".into());
        }
        if self.phase.owns_rootfs() && (!quota_present || !rootfs_present) {
            return Err("direct registration is missing owned resources".into());
        }
        Ok(())
    }

    pub fn to_dict(&self) -> Value {
        let mut raw = Map::new();
        let text = |value: &str| Value::String(value.to_owned());
        raw.insert("bundle".into(), text(&self.bundle));
        raw.insert("container_id".into(), text(&self.container_id));
        raw.insert("created_ns".into(), self.created_ns.into());
        raw.insert("image_id".into(), text(&self.image_id));
        raw.insert("memory_directory".into(), text(&self.memory_directory));
        raw.insert("migration_id".into(), text(&self.migration_id));
        raw.insert("migration_sha256".into(), text(&self.migration_sha256));
        raw.insert("operation_id".into(), text(&self.operation_id));
        raw.insert("phase".into(), text(self.phase.as_str()));
        raw.insert("quota_path".into(), text(&self.quota_path));
        raw.insert("quota_project_id".into(), self.quota_project_id.map_or(Value::Null, Value::from));
        raw.insert("quota_total_mb".into(), self.quota_total_mb.map_or(Value::Null, Value::from));
        raw.insert("revision".into(), self.revision.into());
        raw.insert("rootfs_sha256".into(), text(&self.rootfs_sha256));
        raw.insert("runtime_compatibility_sha256".into(), text(&self.runtime_compatibility_sha256));
        raw.insert("sandbox_generation".into(), self.sandbox_generation.into());
        raw.insert("spec".into(), self.spec.to_dict().clone());
        raw.insert("updated_ns".into(), self.updated_ns.into());
        raw.insert("version".into(), self.version.into());
        if self.version != DIRECT_REGISTRATION_VERSION {
            raw.insert("memory_allocation_id".into(), text(&self.memory_allocation_id));
            raw.insert("workspace_directory".into(), text(&self.workspace_directory));
        }
        Value::Object(raw)
    }

    /// `_encode`: the stored `record_json`.
    pub fn encode(&self) -> String {
        pyjson::dumps(&self.to_dict())
    }

    /// `from_dict`: exact key set and JSON types, then the spec, then `validate`.
    pub fn from_dict(raw: &Value) -> Result<Registration> {
        let Value::Object(raw) = raw else {
            return Err(RegistryError::registry("direct registration must be an object"));
        };
        let schema = || RegistryError::registry("direct registration schema is invalid");
        let version = raw.get("version").and_then(Value::as_i64);
        let legacy = version == Some(DIRECT_REGISTRATION_VERSION);
        let expected = FIELDS.iter().filter(|name| !(legacy && SPLIT_FIELDS.contains(name)));
        if !matches!(version, Some(3 | 4))
            || raw.len() != expected.clone().count()
            || !expected.into_iter().all(|name| raw.contains_key(*name))
            || !raw["spec"].is_object()
        {
            return Err(schema());
        }
        for (name, value) in raw {
            let valid = match name.as_str() {
                "spec" => true,
                name if INTEGER_FIELDS.contains(&name) => value.as_i64().is_some(),
                name if OPTIONAL_INTEGER_FIELDS.contains(&name) => value.is_null() || value.as_i64().is_some(),
                _ => value.is_string(),
            };
            if !valid {
                return Err(schema());
            }
        }
        let invalid = |_| RegistryError::registry("direct registration is invalid");
        let text = |name: &str| raw.get(name).and_then(Value::as_str).unwrap_or_default().to_owned();
        let integer = |name: &str| raw[name].as_i64().expect("type checked above");
        let record = Registration {
            spec: SandboxSpec::from_dict(&raw["spec"]).map_err(invalid)?,
            sandbox_generation: integer("sandbox_generation"),
            operation_id: text("operation_id"),
            runtime_compatibility_sha256: text("runtime_compatibility_sha256"),
            phase: Phase::parse(raw["phase"].as_str().unwrap_or_default())
                .ok_or_else(|| RegistryError::registry("direct registration is invalid"))?,
            revision: integer("revision"),
            created_ns: integer("created_ns"),
            updated_ns: integer("updated_ns"),
            quota_project_id: raw["quota_project_id"].as_i64(),
            quota_total_mb: raw["quota_total_mb"].as_i64(),
            quota_path: text("quota_path"),
            image_id: text("image_id"),
            rootfs_sha256: text("rootfs_sha256"),
            container_id: text("container_id"),
            bundle: text("bundle"),
            memory_directory: text("memory_directory"),
            workspace_directory: text("workspace_directory"),
            memory_allocation_id: text("memory_allocation_id"),
            migration_id: text("migration_id"),
            migration_sha256: text("migration_sha256"),
            version: integer("version"),
        };
        record.validate().map_err(invalid)?;
        Ok(record)
    }

    /// `_decode`: a stored row is valid only if it re-encodes to its own bytes.
    pub fn decode(sandbox_id: &str, image_id: &str, encoded: &str) -> Result<Registration> {
        let encoding = || RegistryError::registry("direct registration encoding is invalid");
        let raw: Value = serde_json::from_str(encoded).map_err(|_| encoding())?;
        let record = Registration::from_dict(&raw)?;
        if record.sandbox_id() != sandbox_id || record.image_id != image_id || record.encode() != encoded {
            return Err(encoding());
        }
        Ok(record)
    }
}

/// `DiskClaim`: a registration's current physical promise, in MiB.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DiskClaim {
    pub workspace_mb: i64,
    pub memory_mb: i64,
}

impl DiskClaim {
    pub fn new(workspace_mb: i64, memory_mb: i64) -> Result<DiskClaim> {
        if workspace_mb < 0 || memory_mb < 0 {
            return Err(RegistryError::invalid("disk claim components must be non-negative integers"));
        }
        Ok(DiskClaim { workspace_mb, memory_mb })
    }

    pub fn total_mb(&self) -> i64 {
        self.workspace_mb + self.memory_mb
    }
}

/// `ManagedGrowthIntent`, in `managed_growth` column order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GrowthIntent {
    pub sandbox_id: String,
    pub generation: i64,
    pub job_id: String,
    pub launch_sha256: String,
    pub memory_bytes: i64,
    pub phase: String,
    pub request_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReflinkOverlapClaim {
    pub sandbox_id: String,
    pub sandbox_generation: i64,
    pub hibernation_generation: i64,
    pub allocated_bytes: i64,
    pub manifest_sha256: String,
}

/// `NodeDrainState`; stored canonical, so `admission_open == !draining`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DrainState {
    pub draining: bool,
    pub token: String,
    pub drain_activity_epoch: i64,
    pub admission_open: bool,
}

impl Default for DrainState {
    fn default() -> Self {
        DrainState { draining: false, token: String::new(), drain_activity_epoch: 0, admission_open: true }
    }
}

impl DrainState {
    pub fn to_dict(&self) -> Value {
        serde_json::json!({
            "admission_open": self.admission_open,
            "drain_activity_epoch": self.drain_activity_epoch,
            "draining": self.draining,
            "token": self.token,
        })
    }

    pub fn encode(&self) -> String {
        pyjson::dumps(&self.to_dict())
    }

    /// `_decode_drain`: Python accepts exactly the canonical encodings of
    /// states its `from_dict` can produce, which are the ones checked here.
    pub fn decode(encoded: &str) -> Result<DrainState> {
        let invalid = || RegistryError::registry("direct registry metadata is invalid");
        let raw: Value = serde_json::from_str(encoded).map_err(|_| invalid())?;
        let object = raw.as_object().filter(|object| object.len() == 4).ok_or_else(invalid)?;
        let state = DrainState {
            draining: object.get("draining").and_then(Value::as_bool).ok_or_else(invalid)?,
            token: object.get("token").and_then(Value::as_str).ok_or_else(invalid)?.to_owned(),
            drain_activity_epoch: object.get("drain_activity_epoch").and_then(Value::as_i64).ok_or_else(invalid)?,
            admission_open: object.get("admission_open").and_then(Value::as_bool).ok_or_else(invalid)?,
        };
        if state.drain_activity_epoch < 0
            || python_strip(&state.token) != state.token
            || (state.draining && state.token.is_empty())
            || state.admission_open == state.draining
            || state.encode() != encoded
        {
            return Err(invalid());
        }
        Ok(state)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// The spec's golden record (registry-spec.md 2.4), written by Python.
    pub(crate) const GOLDEN: &str = r#"{"bundle":"/b/sandbox","container_id":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc","created_ns":1700000000000000000,"image_id":"sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee","memory_allocation_id":"sandbox.sandbox-7","memory_directory":"sandbox.sandbox-7","migration_id":"","migration_sha256":"","operation_id":"create:7","phase":"owned","quota_path":"/q/sandbox","quota_project_id":200000,"quota_total_mb":4096,"revision":4,"rootfs_sha256":"dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd","runtime_compatibility_sha256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","sandbox_generation":7,"spec":{"command":[],"cpus":2.0,"disk_mb":2048,"env":{},"filesystem":{"enforce_disk_quota":false,"run_tmpfs_mb":16,"tmpfs_mb":64,"workspace_path":"/workspace"},"id":"sandbox","image":"registry/image@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","labels":{},"linux_host":{"enable_cron":false,"enable_sshd":false,"keep_alive":true,"writable_paths":["/run","/run/lock","/run/sshd","/tmp","/var/tmp","/var/run","/var/lock","/var/spool/cron","/var/spool/cron/crontabs","/etc/cron.d","/logs","/logs/agent","/logs/verifier","/tests","/task","/oracle","/workspace"]},"managed_process":false,"memory_mb":1024,"network":"bridge","network_policy":{"egress":"relay","relay":"default"},"parkable":true,"profile":"container","security":{"cap_add":[],"cap_drop":["ALL"],"init":true,"no_new_privileges":true,"pids_limit":256,"read_only_rootfs":false,"user":"1000:1000"},"ssh":{"authorized_keys":[],"container_port":22,"enabled":false,"host":"127.0.0.1","host_port":null,"user":"root"},"ttl_seconds":null,"working_dir":null},"updated_ns":1700000000000000123,"version":4,"workspace_directory":"workspace-sandbox.sandbox-7"}"#;

    #[test]
    fn golden_record_round_trips() {
        let image = "sha256:".to_string() + &"e".repeat(64);
        let record = Registration::decode("sandbox", &image, GOLDEN).unwrap();
        assert_eq!(record.encode(), GOLDEN);
        assert_eq!(record.phase, Phase::Owned);
        assert_eq!(record.spec_sha256(), "162d41f80862af3f8788adbe621188a759d782e226f60573ee71f277e5b8fe4d");
        assert_eq!(record.spec.requested_disk_mb().unwrap(), 5184);
        assert_eq!(record.memory_reference(), Some(("sandbox.sandbox-7".into(), 3136 * MIB)));
        // `cpus` given as JSON 2 is stored as 2.0; an int never round-trips.
        let wrong = GOLDEN.replace(r#""cpus":2.0"#, r#""cpus":2"#);
        assert_eq!(
            Registration::decode("sandbox", &image, &wrong).unwrap_err().message(),
            "direct registration encoding is invalid"
        );
        let pretty = serde_json::to_string_pretty(&serde_json::from_str::<Value>(GOLDEN).unwrap()).unwrap();
        assert_eq!(
            Registration::decode("sandbox", &image, &pretty).unwrap_err().message(),
            "direct registration encoding is invalid"
        );
        assert_eq!(
            Registration::decode("other", &image, GOLDEN).unwrap_err().message(),
            "direct registration encoding is invalid"
        );
    }

    #[test]
    fn from_dict_checks_keys_types_and_invariants() {
        let golden: Value = serde_json::from_str(GOLDEN).unwrap();
        let message = |edit: &dyn Fn(&mut Map<String, Value>)| {
            let mut raw = golden.as_object().unwrap().clone();
            edit(&mut raw);
            Registration::from_dict(&Value::Object(raw)).unwrap_err().message().to_string()
        };
        assert_eq!(Registration::from_dict(&json!([])).unwrap_err().message(), "direct registration must be an object");
        assert_eq!(
            message(&|raw| {
                raw.remove("bundle");
            }),
            "direct registration schema is invalid"
        );
        assert_eq!(
            message(&|raw| {
                raw.insert("version".into(), json!(3));
            }),
            "direct registration schema is invalid"
        );
        assert_eq!(
            message(&|raw| {
                raw.insert("revision".into(), json!(true));
            }),
            "direct registration schema is invalid"
        );
        assert_eq!(
            message(&|raw| {
                raw.insert("revision".into(), json!(4.0));
            }),
            "direct registration schema is invalid"
        );
        assert_eq!(
            message(&|raw| {
                raw.insert("bundle".into(), json!(null));
            }),
            "direct registration schema is invalid"
        );
        assert_eq!(
            message(&|raw| {
                raw.insert("phase".into(), json!("flying"));
            }),
            "direct registration is invalid"
        );
        assert_eq!(
            message(&|raw| {
                raw.insert("bundle".into(), json!("relative"));
            }),
            "direct registration is invalid"
        );
        let mut record = Registration::from_dict(&golden).unwrap();
        let check = |record: &Registration| record.validate().unwrap_err();
        record.migration_id = "m".into();
        assert_eq!(check(&record), "direct registration migration ownership is invalid");
        record.migration_id.clear();
        record.quota_path.clear();
        assert_eq!(check(&record), "direct registration quota identity is incomplete");
        record.quota_path = "/q".into();
        record.phase = Phase::Planned;
        assert_eq!(check(&record), "planned direct registration owns external state");
        record.phase = Phase::QuotaReady;
        assert_eq!(check(&record), "quota-ready direct registration is inconsistent");
        record.phase = Phase::Owned;
        record.memory_allocation_id = "x".into();
        assert_eq!(check(&record), "split registration has invalid component identities");
        record.version = 3;
        assert_eq!(check(&record), "legacy registration cannot contain split backing");
    }

    #[test]
    fn drain_is_canonical_only() {
        let initial = r#"{"admission_open":true,"drain_activity_epoch":0,"draining":false,"token":""}"#;
        assert_eq!(DrainState::decode(initial).unwrap(), DrainState::default());
        assert_eq!(DrainState::default().encode(), initial);
        let draining = r#"{"admission_open":false,"drain_activity_epoch":3,"draining":true,"token":"t"}"#;
        assert!(DrainState::decode(draining).unwrap().draining);
        for bad in [
            r#"{"admission_open":true,"drain_activity_epoch":0,"draining":"true","token":"t"}"#,
            r#"{"admission_open":true,"drain_activity_epoch":0,"draining":true,"token":"t"}"#,
            r#"{"admission_open":false,"drain_activity_epoch":0,"draining":true,"token":""}"#,
            r#"{"admission_open":true,"drain_activity_epoch":-1,"draining":false,"token":""}"#,
            r#"{"admission_open":true,"drain_activity_epoch":0,"draining":false,"token":" t"}"#,
            r#"{"draining":false,"admission_open":true,"drain_activity_epoch":0,"token":""}"#,
            "[]",
        ] {
            assert_eq!(DrainState::decode(bad).unwrap_err().message(), "direct registry metadata is invalid", "{bad}");
        }
    }
}
