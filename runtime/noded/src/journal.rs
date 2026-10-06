//! The per-incarnation lifecycle journal (ucloud_sandboxes/hibernation.py,
//! `HibernationJournalStore`): one canonical-JSON record per sandbox generation.
//! Phase 1 writes only the running record a create commits; every later
//! transition stays with the Python agent, which reads these files unchanged.

use std::path::{Path, PathBuf};

use serde_json::{Map, Value, json};

use crate::fsutil::{FileLock, atomic_write, ensure_private_dir, read_owned_file};
use crate::storage::canonical_json;

pub const SCHEMA_VERSION: u64 = 1;
pub const MAX_JSON_BYTES: u64 = 1024 * 1024;

const KEYS: [&str; 17] = [
    "authority",
    "candidate_pid",
    "candidate_start_time_ticks",
    "hibernation_generation",
    "manifest_sha256",
    "operation_id",
    "operation_kind",
    "recovery_reason",
    "revision",
    "sandbox_generation",
    "sandbox_id",
    "sentry_pid",
    "sentry_start_time_ticks",
    "spec_sha256",
    "state",
    "updated_ns",
    "version",
];

#[derive(Debug)]
pub enum JournalError {
    /// The journal holds another incarnation (Python: HibernationConflictError).
    Conflict(String),
    Invalid(String),
    Io(std::io::Error),
}

impl std::fmt::Display for JournalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JournalError::Conflict(m) | JournalError::Invalid(m) => f.write_str(m),
            JournalError::Io(e) => write!(f, "hibernation journal I/O failed: {e}"),
        }
    }
}

impl std::error::Error for JournalError {}

impl From<std::io::Error> for JournalError {
    fn from(error: std::io::Error) -> Self {
        JournalError::Io(error)
    }
}

/// `[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}`, not `.` or `..`.
pub fn is_safe_id(value: &str) -> bool {
    let bytes = value.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 128
        && bytes[0].is_ascii_alphanumeric()
        && bytes.iter().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b':' | b'-'))
        && value != "."
        && value != ".."
}

fn is_digest(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

#[derive(Clone, Debug)]
pub struct JournalStore {
    root: PathBuf,
}

impl JournalStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        JournalStore { root: root.into() }
    }

    fn paths(&self, sandbox_id: &str, generation: u64) -> Result<(PathBuf, PathBuf), JournalError> {
        if !is_safe_id(sandbox_id) || generation < 1 {
            return Err(JournalError::Invalid("invalid hibernation journal identity".into()));
        }
        let name = format!("{sandbox_id}.sandbox-{generation}.json");
        Ok((self.root.join(&name), self.root.join(format!(".{name}.lock"))))
    }

    pub fn load(&self, sandbox_id: &str, generation: u64) -> Result<Option<Map<String, Value>>, JournalError> {
        let (path, _) = self.paths(sandbox_id, generation)?;
        load_record(&path)
    }

    /// Commit the running record of a just-started sandbox. Idempotent: a
    /// record of the same incarnation is returned unchanged, never rewritten.
    pub fn initialize_running(
        &self,
        sandbox_id: &str,
        generation: u64,
        spec_sha256: &str,
        operation_id: &str,
        sentry_pid: u64,
        sentry_start_time_ticks: u64,
    ) -> Result<Map<String, Value>, JournalError> {
        if !is_digest(spec_sha256) || !is_safe_id(operation_id) || sentry_pid == 0 || sentry_start_time_ticks == 0 {
            return Err(JournalError::Invalid("invalid running journal record".into()));
        }
        let (path, lock_path) = self.paths(sandbox_id, generation)?;
        ensure_private_dir(&self.root)?;
        let _lock = FileLock::acquire(&lock_path, true)?;
        if let Some(existing) = load_record(&path)? {
            let same = existing["sandbox_id"] == json!(sandbox_id)
                && existing["sandbox_generation"] == json!(generation)
                && existing["spec_sha256"] == json!(spec_sha256);
            if !same {
                return Err(JournalError::Conflict(
                    "hibernation journal belongs to another sandbox incarnation".into(),
                ));
            }
            return Ok(existing);
        }
        let updated_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| JournalError::Invalid("clock before epoch".into()))?
            .as_nanos() as u64;
        let record = json!({
            "authority": "live",
            "candidate_pid": null,
            "candidate_start_time_ticks": null,
            "hibernation_generation": 0,
            "manifest_sha256": "",
            "operation_id": operation_id,
            "operation_kind": "initialize",
            "recovery_reason": "",
            "revision": 0,
            "sandbox_generation": generation,
            "sandbox_id": sandbox_id,
            "sentry_pid": sentry_pid,
            "sentry_start_time_ticks": sentry_start_time_ticks,
            "spec_sha256": spec_sha256,
            "state": "running",
            "updated_ns": updated_ns,
            "version": SCHEMA_VERSION,
        });
        let mut bytes = canonical_json(&record);
        bytes.push(b'\n');
        atomic_write(&path, &bytes)?;
        let Value::Object(record) = record else { unreachable!() };
        Ok(record)
    }
}

fn load_record(path: &Path) -> Result<Option<Map<String, Value>>, JournalError> {
    let Some(bytes) = read_owned_file(path, MAX_JSON_BYTES)? else { return Ok(None) };
    let value: Value = serde_json::from_slice(&bytes)
        .map_err(|_| JournalError::Invalid("hibernation journal is not valid JSON".into()))?;
    let Value::Object(record) = value else {
        return Err(JournalError::Invalid("hibernation journal must be a JSON object".into()));
    };
    if record.len() != KEYS.len() || KEYS.iter().any(|key| !record.contains_key(*key)) {
        return Err(JournalError::Invalid("hibernation journal has unexpected keys".into()));
    }
    if record["version"] != json!(SCHEMA_VERSION) {
        return Err(JournalError::Invalid("unsupported hibernation journal version".into()));
    }
    Ok(Some(record))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> (JournalStore, PathBuf) {
        let root = std::env::temp_dir().join(format!(
            "noded-journal-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        (JournalStore::new(root.join("journals")), root)
    }

    #[test]
    fn running_record_is_canonical_and_idempotent() {
        let (store, root) = store();
        let spec = "a".repeat(64);
        let first = store.initialize_running("sb-1", 1, &spec, "op-1", 4242, 987654).unwrap();
        let path = root.join("journals/sb-1.sandbox-1.json");
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(text.ends_with("}\n") && !text[..text.len() - 1].contains('\n'));
        assert!(text.starts_with(r#"{"authority":"live","candidate_pid":null,"candidate_start_time_ticks":null,"hibernation_generation":0,"manifest_sha256":"","operation_id":"op-1","operation_kind":"initialize","recovery_reason":"","revision":0,"sandbox_generation":1,"sandbox_id":"sb-1","sentry_pid":4242,"sentry_start_time_ticks":987654,"spec_sha256":"aaaa"#));
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(std::fs::metadata(&path).unwrap().permissions().mode() & 0o777, 0o600);
        // A replay (another sentry pid) returns the committed record unchanged.
        let again = store.initialize_running("sb-1", 1, &spec, "op-1", 1, 1).unwrap();
        assert_eq!(again, first);
        assert_eq!(std::fs::read_to_string(&path).unwrap(), text);
        // Another incarnation's spec conflicts.
        assert!(matches!(
            store.initialize_running("sb-1", 1, &"b".repeat(64), "op-1", 1, 1),
            Err(JournalError::Conflict(_))
        ));
        assert!(std::fs::read_dir(root.join("journals")).unwrap().all(|e| !e.unwrap().file_name().to_string_lossy().ends_with(".tmp")));
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn identities_are_validated() {
        let (store, root) = store();
        assert!(store.initialize_running("../x", 1, &"a".repeat(64), "op", 1, 1).is_err());
        assert!(store.initialize_running("sb", 0, &"a".repeat(64), "op", 1, 1).is_err());
        assert!(store.initialize_running("sb", 1, "short", "op", 1, 1).is_err());
        let _ = std::fs::remove_dir_all(root);
    }
}
