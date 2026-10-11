//! The managed supervisor's files as the host reads them through the state
//! mount (docs/managed-process-reads.md, "Safe opens" and "Record
//! derivation"). The guest writes these files, so every open refuses
//! symlinks and anything but a regular file, and `state.json` is parsed
//! strictly.

use std::fs::File;
use std::io;
use std::os::fd::OwnedFd;
use std::os::unix::fs::FileExt;
use std::path::Path;

use serde_json::{Map, Value, json};

use crate::guest::{open_at, open_directory};
use crate::oci::MANAGED_STATE_NAME;

pub const STATE_FILE: &str = "state.json";
/// `state.json` is a few hundred bytes; anything past this is not the supervisor's.
pub const MAX_STATE_BYTES: u64 = 64 * 1024;
/// Python's `MAX_LOG_READ_BYTES`, the largest log chunk one read returns.
pub const MAX_LOG_READ_BYTES: u64 = 1024 * 1024;
const PROTOCOL_VERSION: u64 = 1;
const TERMINAL_STATES: [&str; 3] = ["exited", "signaled", "failed"];
const STATES: [&str; 5] = ["starting", "running", "exited", "signaled", "failed"];

#[derive(Debug)]
pub enum ReadError {
    /// The rootfs, the state directory or `state.json` is not there: a
    /// parked or deleted sandbox, or no primary yet. The agent answers.
    Missing,
    /// A symlink, FIFO, oversize or malformed file: 409
    /// `managed_process_state_invalid`.
    Invalid(String),
    Io(io::Error),
}

impl std::fmt::Display for ReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReadError::Missing => f.write_str("managed process state is missing"),
            ReadError::Invalid(message) => f.write_str(message),
            ReadError::Io(error) => write!(f, "managed process state read failed: {error}"),
        }
    }
}

fn classify(error: io::Error, name: &str) -> ReadError {
    match error.raw_os_error() {
        Some(libc::ENOENT) => ReadError::Missing,
        // O_NOFOLLOW on a symlink, a non-directory, a socket.
        Some(libc::ELOOP | libc::ENOTDIR | libc::ENXIO | libc::EOPNOTSUPP) => {
            ReadError::Invalid(format!("managed process {name} is not a regular file"))
        }
        _ => ReadError::Io(error),
    }
}

/// `<bundle>/rootfs/.ucloud-managed`, opened without following a symlink at
/// either step. Every read in one pass goes through this descriptor; it is
/// closed before any wait, so a held read never keeps the overlay busy.
pub struct StateDir {
    fd: OwnedFd,
}

/// One stream's retained log, opened safely.
pub struct Log {
    file: File,
    pub size: u64,
}

impl StateDir {
    pub fn open(bundle: &Path) -> Result<StateDir, ReadError> {
        let flags = libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW;
        let rootfs = open_directory(&bundle.join("rootfs")).map_err(|error| classify(error, "rootfs"))?;
        let fd = open_at(&rootfs, MANAGED_STATE_NAME, flags, 0).map_err(|error| classify(error, "state directory"))?;
        Ok(StateDir { fd })
    }

    pub fn fd(&self) -> &OwnedFd {
        &self.fd
    }

    /// `openat(name, O_RDONLY|O_NOFOLLOW|O_NONBLOCK|O_CLOEXEC)`, then fstat:
    /// a regular file or nothing. `Ok(None)` when it does not exist.
    fn open_file(&self, name: &str) -> Result<Option<(File, u64)>, ReadError> {
        let fd = match open_at(&self.fd, name, libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_NONBLOCK, 0) {
            Ok(fd) => fd,
            Err(error) if error.raw_os_error() == Some(libc::ENOENT) => return Ok(None),
            Err(error) => return Err(classify(error, name)),
        };
        let file = File::from(fd);
        let meta = file.metadata().map_err(ReadError::Io)?;
        if !meta.file_type().is_file() {
            return Err(ReadError::Invalid(format!("managed process {name} is not a regular file")));
        }
        Ok(Some((file, meta.len())))
    }

    /// `state.json`, strictly. A missing file is the agent's to answer.
    pub fn state(&self) -> Result<JobState, ReadError> {
        let (file, size) = self.open_file(STATE_FILE)?.ok_or(ReadError::Missing)?;
        if size > MAX_STATE_BYTES {
            return Err(ReadError::Invalid("managed process state.json is oversize".into()));
        }
        let bytes = read_range(&file, 0, MAX_STATE_BYTES + 1).map_err(ReadError::Io)?;
        if bytes.len() as u64 > MAX_STATE_BYTES {
            return Err(ReadError::Invalid("managed process state.json is oversize".into()));
        }
        JobState::parse(&bytes).map_err(ReadError::Invalid)
    }

    pub fn log(&self, stream: &str) -> Result<Option<Log>, ReadError> {
        Ok(self.open_file(&format!("{stream}.log"))?.map(|(file, size)| Log { file, size }))
    }

    fn log_size(&self, stream: &str) -> Result<u64, ReadError> {
        Ok(self.log(stream)?.map_or(0, |log| log.size))
    }

    /// The record as Python's status read derives it, from the files: while
    /// the job runs its byte counts are the logs' sizes; once terminal, the
    /// persisted values.
    pub fn record(&self, sandbox_id: &str, sandbox_generation: u64) -> Result<JobRecord, ReadError> {
        let mut state = self.state()?;
        if !state.terminal() {
            state.stdout_bytes = self.log_size("stdout")?;
            state.stderr_bytes = self.log_size("stderr")?;
        }
        Ok(JobRecord {
            sandbox_id: sandbox_id.to_string(),
            sandbox_generation,
            state,
            updated_at: crate::exec::isoformat(std::time::SystemTime::now()),
        })
    }
}

impl Log {
    /// Up to `limit` bytes from `offset`, never past the size seen at open.
    pub fn read(&self, offset: u64, limit: u64) -> io::Result<Vec<u8>> {
        let available = self.size.saturating_sub(offset);
        read_range(&self.file, offset, available.min(limit))
    }
}

fn read_range(file: &File, offset: u64, limit: u64) -> io::Result<Vec<u8>> {
    let mut bytes = vec![0u8; limit as usize];
    let mut filled = 0;
    while filled < bytes.len() {
        match file.read_at(&mut bytes[filled..], offset + filled as u64) {
            Ok(0) => break,
            Ok(read) => filled += read,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
    bytes.truncate(filled);
    Ok(bytes)
}

/// `state.json` as the supervisor writes it (Go `jobRecord`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JobState {
    pub job_id: String,
    pub spec_sha256: String,
    pub state: String,
    pub pid: u64,
    pub started_at: String,
    pub completed_at: String,
    pub exit_code: Option<i64>,
    pub signal: u64,
    pub stdout_bytes: u64,
    pub stderr_bytes: u64,
    pub stdout_truncated: bool,
    pub stderr_truncated: bool,
    pub sequence: u64,
}

/// `[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}` (Python `_JOB_ID`).
pub fn is_job_id(value: &str) -> bool {
    crate::journal::is_safe_id(value)
}

/// What Python's `parse_iso_datetime` accepts from Go's RFC 3339 timestamps:
/// `YYYY-MM-DDTHH:MM:SS`, an optional fraction of up to nine digits, then `Z`
/// or `±HH:MM`.
fn is_timestamp(value: &str) -> bool {
    let bytes = value.as_bytes();
    let digits = |range: std::ops::Range<usize>| bytes.get(range.clone()).is_some_and(|part| part.iter().all(u8::is_ascii_digit));
    let number = |range: std::ops::Range<usize>| value[range].parse::<u32>().unwrap_or(u32::MAX);
    if bytes.len() < 20 || !(digits(0..4) && digits(5..7) && digits(8..10) && digits(11..13) && digits(14..16) && digits(17..19)) {
        return false;
    }
    if bytes[4] != b'-' || bytes[7] != b'-' || bytes[10] != b'T' || bytes[13] != b':' || bytes[16] != b':' {
        return false;
    }
    let (month, day, hour, minute, second) = (number(5..7), number(8..10), number(11..13), number(14..16), number(17..19));
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) || hour > 23 || minute > 59 || second > 59 {
        return false;
    }
    let mut rest = &value[19..];
    if let Some(fraction) = rest.strip_prefix('.') {
        let count = fraction.bytes().take_while(u8::is_ascii_digit).count();
        if !(1..=9).contains(&count) {
            return false;
        }
        rest = &fraction[count..];
    }
    match rest.as_bytes() {
        b"Z" => true,
        [sign, h1, h2, b':', m1, m2] if matches!(sign, b'+' | b'-') => {
            [h1, h2, m1, m2].iter().all(|b| b.is_ascii_digit()) && number(rest.len() - 5 + 19..rest.len() - 3 + 19) <= 23
        }
        _ => false,
    }
}

impl JobState {
    pub fn terminal(&self) -> bool {
        TERMINAL_STATES.contains(&self.state.as_str())
    }

    /// Version 1 only, known keys only, each of its Go type; then Python's
    /// `ManagedProcessRecord.validate`.
    pub fn parse(bytes: &[u8]) -> Result<JobState, String> {
        let invalid = |what: &str| format!("managed process state.json has an invalid {what}");
        let value: Value = serde_json::from_slice(bytes).map_err(|_| "managed process state.json is not JSON".to_string())?;
        let Value::Object(raw) = value else { return Err("managed process state.json is not an object".into()) };
        const KEYS: [&str; 14] = [
            "version", "job_id", "spec_sha256", "state", "pid", "started_at", "completed_at", "exit_code", "signal",
            "stdout_bytes", "stderr_bytes", "stdout_truncated", "stderr_truncated", "sequence",
        ];
        if let Some(key) = raw.keys().find(|key| !KEYS.contains(&key.as_str())) {
            return Err(format!("managed process state.json has an unknown field {}", crate::oci::py_repr(key)));
        }
        let required = |key: &str| raw.get(key).ok_or_else(|| format!("managed process state.json lacks {key}"));
        let unsigned = |key: &str, default: Option<u64>| -> Result<u64, String> {
            match raw.get(key) {
                None => default.ok_or_else(|| format!("managed process state.json lacks {key}")),
                Some(value) => value.as_u64().ok_or_else(|| invalid(key)),
            }
        };
        let text = |key: &str| -> Result<String, String> {
            match raw.get(key) {
                None => Ok(String::new()),
                Some(value) => value.as_str().map(str::to_string).ok_or_else(|| invalid(key)),
            }
        };
        let flag = |key: &str| required(key)?.as_bool().ok_or_else(|| invalid(key));
        if required("version")?.as_u64() != Some(PROTOCOL_VERSION) {
            return Err("managed process state.json has an unsupported version".into());
        }
        let state = JobState {
            job_id: required("job_id")?.as_str().filter(|id| is_job_id(id)).ok_or_else(|| invalid("job_id"))?.to_string(),
            spec_sha256: required("spec_sha256")?
                .as_str()
                .filter(|digest| digest.len() == 64 && digest.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')))
                .ok_or_else(|| invalid("spec_sha256"))?
                .to_string(),
            state: required("state")?.as_str().filter(|state| STATES.contains(state)).ok_or_else(|| invalid("state"))?.to_string(),
            pid: unsigned("pid", Some(0))?,
            started_at: text("started_at")?,
            completed_at: text("completed_at")?,
            exit_code: match raw.get("exit_code") {
                None => None,
                Some(value) => Some(value.as_i64().ok_or_else(|| invalid("exit_code"))?),
            },
            signal: unsigned("signal", Some(0))?,
            stdout_bytes: unsigned("stdout_bytes", None)?,
            stderr_bytes: unsigned("stderr_bytes", None)?,
            stdout_truncated: flag("stdout_truncated")?,
            stderr_truncated: flag("stderr_truncated")?,
            sequence: unsigned("sequence", None)?,
        };
        if state.sequence < 1 {
            return Err(invalid("sequence"));
        }
        for (key, value) in [("started_at", &state.started_at), ("completed_at", &state.completed_at)] {
            if !value.is_empty() && !is_timestamp(value) {
                return Err(invalid(key));
            }
        }
        Ok(state)
    }
}

/// Python's `ManagedProcessRecord` for one served read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JobRecord {
    pub sandbox_id: String,
    pub sandbox_generation: u64,
    pub state: JobState,
    pub updated_at: String,
}

/// Why a supervisor's control answer is not a record: Python's
/// `ManagedProcessError` (409) or the record's `ValueError` (400).
#[derive(Debug, PartialEq, Eq)]
pub enum ControlAnswerError {
    Conflict(String),
    Invalid(String),
}

impl JobRecord {
    /// `ManagedProcessRecord.from_control_response`: the supervisor's own
    /// snapshot, read as leniently as Python reads it, then validated.
    pub fn from_control(raw: &Value, sandbox_id: &str, sandbox_generation: u64) -> Result<JobRecord, ControlAnswerError> {
        let object = raw.as_object();
        if object.and_then(|raw| raw.get("ok")) != Some(&Value::Bool(true)) {
            let error = object.and_then(|raw| raw.get("error")).filter(|error| super::start::truthy(error));
            let message = match error {
                Some(Value::String(text)) => text.clone(),
                Some(other) => other.to_string(),
                None => "invalid managed process response".into(),
            };
            return Err(ControlAnswerError::Conflict(message));
        }
        let Some(Value::Object(job)) = object.and_then(|raw| raw.get("job")) else {
            return Err(ControlAnswerError::Conflict("managed process response omitted job state".into()));
        };
        let malformed = || ControlAnswerError::Conflict("invalid managed process response".into());
        // str(job.get(key) or "")
        let text = |key: &str| match job.get(key) {
            Some(Value::String(text)) => Ok(text.clone()),
            Some(value) if super::start::truthy(value) => Err(malformed()),
            _ => Ok(String::new()),
        };
        // int(job.get(key) or 0)
        let integer = |key: &str| -> Result<i64, ControlAnswerError> {
            match job.get(key) {
                Some(Value::Number(number)) => number.as_i64().or_else(|| number.as_f64().map(|n| n.trunc() as i64)).ok_or_else(malformed),
                Some(Value::Bool(flag)) => Ok(i64::from(*flag)),
                Some(value) if super::start::truthy(value) => Err(malformed()),
                _ => Ok(0),
            }
        };
        let unsigned = |key: &str| integer(key).map(|value| value.max(0) as u64);
        let flag = |key: &str| job.get(key).is_some_and(super::start::truthy);
        let exit_code = match job.get("exit_code") {
            None | Some(Value::Null) => None,
            Some(_) => Some(integer("exit_code")?),
        };
        let state = JobState {
            job_id: text("job_id")?,
            spec_sha256: text("spec_sha256")?,
            state: text("state")?,
            pid: unsigned("pid")?,
            started_at: text("started_at")?,
            completed_at: text("completed_at")?,
            exit_code,
            signal: unsigned("signal")?,
            stdout_bytes: unsigned("stdout_bytes")?,
            stderr_bytes: unsigned("stderr_bytes")?,
            stdout_truncated: flag("stdout_truncated"),
            stderr_truncated: flag("stderr_truncated"),
            sequence: unsigned("sequence")?,
        };
        // ManagedProcessRecord.validate
        let invalid = |message: &str| Err(ControlAnswerError::Invalid(message.into()));
        if !is_job_id(&state.job_id) {
            return invalid("managed process job identity is invalid");
        }
        if !(state.spec_sha256.len() == 64 && state.spec_sha256.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))) {
            return invalid("managed process spec digest is invalid");
        }
        if !STATES.contains(&state.state.as_str()) {
            return invalid("managed process state is invalid");
        }
        if [&state.started_at, &state.completed_at].iter().any(|value| !value.is_empty() && !is_timestamp(value)) {
            return invalid("managed process timestamp is invalid");
        }
        if state.sequence < 1 {
            return invalid("managed process sequence is invalid");
        }
        Ok(JobRecord {
            sandbox_id: sandbox_id.to_string(),
            sandbox_generation,
            state,
            updated_at: crate::exec::isoformat(std::time::SystemTime::now()),
        })
    }

    pub fn terminal(&self) -> bool {
        self.state.terminal()
    }

    /// `"<sequence>.<stdout_bytes>.<stderr_bytes>"`: changes whenever the
    /// record does in a way a follower cares about.
    pub fn cursor(&self) -> String {
        format!("{}.{}.{}", self.state.sequence, self.state.stdout_bytes, self.state.stderr_bytes)
    }

    pub fn stream_bytes(&self, stream: &str) -> u64 {
        if stream == "stderr" { self.state.stderr_bytes } else { self.state.stdout_bytes }
    }

    /// `ManagedProcessRecord.to_dict()`, in its key order, plus `cursor`.
    pub fn to_json(&self) -> Value {
        let state = &self.state;
        let mut map = Map::new();
        map.insert("sandbox_id".into(), json!(self.sandbox_id));
        map.insert("sandbox_generation".into(), json!(self.sandbox_generation));
        map.insert("job_id".into(), json!(state.job_id));
        map.insert("spec_sha256".into(), json!(state.spec_sha256));
        map.insert("state".into(), json!(state.state));
        map.insert("pid".into(), json!(state.pid));
        map.insert("started_at".into(), json!(state.started_at));
        map.insert("completed_at".into(), json!(state.completed_at));
        map.insert("exit_code".into(), json!(state.exit_code));
        map.insert("signal".into(), json!(state.signal));
        map.insert("stdout_bytes".into(), json!(state.stdout_bytes));
        map.insert("stderr_bytes".into(), json!(state.stderr_bytes));
        map.insert("stdout_truncated".into(), json!(state.stdout_truncated));
        map.insert("stderr_truncated".into(), json!(state.stderr_truncated));
        map.insert("sequence".into(), json!(state.sequence));
        map.insert("updated_at".into(), json!(self.updated_at));
        map.insert("cursor".into(), json!(self.cursor()));
        Value::Object(map)
    }
}

/// Standard base64 with padding (Python `base64.b64encode`).
pub fn base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let triple = (u32::from(chunk[0]) << 16) | (u32::from(*chunk.get(1).unwrap_or(&0)) << 8) | u32::from(*chunk.get(2).unwrap_or(&0));
        for index in 0..4 {
            if index <= chunk.len() {
                out.push(ALPHABET[(triple >> (18 - 6 * index) & 0x3f) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const GO_RUNNING: &str = r#"{"version":1,"job_id":"job-1","spec_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","state":"running","pid":42,"started_at":"2026-10-11T20:00:00.12345678Z","stdout_bytes":0,"stderr_bytes":0,"stdout_truncated":false,"stderr_truncated":false,"sequence":2}"#;

    #[test]
    fn go_records_parse_and_strangers_do_not() {
        let state = JobState::parse(GO_RUNNING.as_bytes()).unwrap();
        assert_eq!((state.pid, state.sequence, state.exit_code, state.completed_at.as_str()), (42, 2, None, ""));
        assert!(!state.terminal());
        let exited = GO_RUNNING
            .replace(r#""state":"running","pid":42,"#, r#""state":"signaled","completed_at":"2026-10-11T20:00:01Z","exit_code":-1,"signal":9,"#);
        let exited = JobState::parse(exited.as_bytes()).unwrap();
        assert!(exited.terminal());
        assert_eq!((exited.exit_code, exited.signal, exited.pid), (Some(-1), 9, 0));
        for (from, to) in [
            (r#""version":1"#, r#""version":2"#),
            (r#""job_id":"job-1""#, r#""job_id":"../x""#),
            (r#""spec_sha256":"aaaa"#, r#""spec_sha256":"AAAA"#),
            (r#""state":"running""#, r#""state":"paused""#),
            (r#""pid":42"#, r#""pid":-1"#),
            (r#""pid":42"#, r#""pid":4.5"#),
            (r#""sequence":2"#, r#""sequence":0"#),
            (r#""stdout_truncated":false"#, r#""stdout_truncated":0"#),
            (r#""started_at":"2026-10-11T20:00:00.12345678Z""#, r#""started_at":"yesterday""#),
            (r#""started_at":"2026-10-11T20:00:00.12345678Z""#, r#""started_at":"2026-10-11T20:00:00.1234567890Z""#),
            (r#""sequence":2"#, r#""sequence":2,"extra":1"#),
            (r#","stdout_bytes":0"#, ""),
        ] {
            let text = GO_RUNNING.replace(from, to);
            assert_ne!(text, GO_RUNNING, "{to}");
            assert!(JobState::parse(text.as_bytes()).is_err(), "{to}");
        }
        assert!(JobState::parse(b"[]").is_err());
        assert!(JobState::parse(b"{").is_err());
        assert!(is_timestamp("2026-10-11T20:00:00+02:00"));
        assert!(!is_timestamp("2026-13-11T20:00:00Z"));
    }

    #[test]
    fn records_keep_pythons_key_order_and_add_a_cursor() {
        let record = JobRecord {
            sandbox_id: "sb".into(),
            sandbox_generation: 3,
            state: JobState::parse(GO_RUNNING.as_bytes()).unwrap(),
            updated_at: "2026-10-11T20:00:02.000001+00:00".into(),
        };
        assert_eq!(
            crate::exec::json::dumps(&record.to_json()),
            r#"{"sandbox_id":"sb","sandbox_generation":3,"job_id":"job-1","spec_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","state":"running","pid":42,"started_at":"2026-10-11T20:00:00.12345678Z","completed_at":"","exit_code":null,"signal":0,"stdout_bytes":0,"stderr_bytes":0,"stdout_truncated":false,"stderr_truncated":false,"sequence":2,"updated_at":"2026-10-11T20:00:02.000001+00:00","cursor":"2.0.0"}"#
        );
    }

    #[test]
    fn base64_matches_python() {
        // python3 -c 'import base64; print(base64.b64encode(b"..."))'
        assert_eq!(base64(b""), "");
        assert_eq!(base64(b"f"), "Zg==");
        assert_eq!(base64(b"fo"), "Zm8=");
        assert_eq!(base64(b"foo"), "Zm9v");
        assert_eq!(base64(&[0xff, 0xfe, 0x00, 0x01]), "//4AAQ==");
    }
}
