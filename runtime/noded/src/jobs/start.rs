//! A managed start's request as Python reads it (`ManagedProcessStart` in
//! ucloud_sandboxes/managed_process.py) and the control payload and launch
//! identity it sends (`control_payload`, `control_request_bytes`): the same
//! checks in the same order with the same messages, and the same bytes.

use std::collections::BTreeMap;

use serde_json::{Map, Value, json};

pub const DEFAULT_MAX_LOG_BYTES: i64 = 16 * 1024 * 1024;
const MAX_ARGV: usize = 4096;
const FIELDS: [&str; 6] = ["argv", "cwd", "env", "job_id", "max_stderr_bytes", "max_stdout_bytes"];
pub const UID_ANNOTATION: &str = "dev.ucloud-sandboxes.managed-process.uid";
pub const GID_ANNOTATION: &str = "dev.ucloud-sandboxes.managed-process.gid";
pub const CWD_ANNOTATION: &str = "dev.ucloud-sandboxes.managed-process.cwd";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ManagedStart {
    pub job_id: String,
    pub argv: Vec<String>,
    pub env: BTreeMap<String, String>,
    pub cwd: Option<String>,
    pub max_stdout_bytes: i64,
    pub max_stderr_bytes: i64,
}

/// Python `str.isspace` (what `str.strip()` removes).
fn py_space(c: char) -> bool {
    c.is_whitespace() || ('\u{1c}'..='\u{1f}').contains(&c)
}

/// Python truthiness of a JSON value.
pub(crate) fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(flag) => *flag,
        Value::Number(number) => number.as_f64().is_some_and(|n| n != 0.0),
        Value::String(text) => !text.is_empty(),
        Value::Array(items) => !items.is_empty(),
        Value::Object(map) => !map.is_empty(),
    }
}

/// `validate_guest_path(name, value)` (guest_paths.py).
pub fn validate_guest_path(name: &str, value: Option<&str>) -> Result<(), String> {
    let Some(value) = value.filter(|value| value.starts_with('/')) else {
        return Err(format!("{name} must be an absolute container path."));
    };
    if value.chars().any(|c| (c as u32) < 0x20 || c as u32 == 0x7f) {
        return Err(format!("{name} contains unsupported control characters."));
    }
    if value.split('/').any(|part| part == "..") {
        return Err(format!("{name} cannot contain '..'."));
    }
    Ok(())
}

fn is_env_key(key: &str) -> bool {
    let bytes = key.as_bytes();
    !bytes.is_empty()
        && (bytes[0].is_ascii_alphabetic() || bytes[0] == b'_')
        && bytes.iter().all(|b| b.is_ascii_alphanumeric() || *b == b'_')
}

impl ManagedStart {
    /// `ManagedProcessStart.from_dict`: each error is Python's `ValueError`
    /// message (a 400).
    pub fn from_value(raw: &Value) -> Result<ManagedStart, String> {
        let Value::Object(raw) = raw else { return Err("managed process payload must be a JSON object".into()) };
        let mut unsupported: Vec<&String> = raw.keys().filter(|key| !FIELDS.contains(&key.as_str())).collect();
        if !unsupported.is_empty() {
            unsupported.sort();
            let names: Vec<&str> = unsupported.iter().map(|key| key.as_str()).collect();
            return Err(format!("unsupported managed process fields: {}", names.join(", ")));
        }
        let argv = match raw.get("argv") {
            Some(Value::Array(items)) if items.iter().all(Value::is_string) => {
                items.iter().map(|item| item.as_str().expect("checked").to_string()).collect::<Vec<_>>()
            }
            _ => return Err("managed process argv must be a JSON string array".into()),
        };
        let env = match raw.get("env").filter(|env| truthy(env)) {
            None => BTreeMap::new(),
            Some(Value::Object(env)) if env.values().all(Value::is_string) => {
                env.iter().map(|(key, value)| (key.clone(), value.as_str().expect("checked").to_string())).collect()
            }
            Some(_) => return Err("managed process env must contain strings".into()),
        };
        let cwd = match raw.get("cwd") {
            None | Some(Value::Null) => None,
            Some(Value::String(cwd)) => Some(cwd.clone()),
            Some(_) => return Err("managed process cwd must be a string".into()),
        };
        let Some(Value::String(job_id)) = raw.get("job_id") else {
            return Err("managed process job_id must be a string".into());
        };
        let limit = |key: &str| match raw.get(key) {
            None => Ok(DEFAULT_MAX_LOG_BYTES),
            Some(value) => value.as_i64().ok_or_else(|| "managed process log limits must be integers".to_string()),
        };
        let start = ManagedStart {
            job_id: job_id.trim_matches(py_space).to_string(),
            argv,
            env,
            cwd,
            max_stdout_bytes: limit("max_stdout_bytes")?,
            max_stderr_bytes: limit("max_stderr_bytes")?,
        };
        start.validate()?;
        Ok(start)
    }

    /// `ManagedProcessStart.validate`.
    pub fn validate(&self) -> Result<(), String> {
        if !super::files::is_job_id(&self.job_id) {
            return Err("managed process job_id is invalid".into());
        }
        if self.argv.is_empty() || self.argv.len() > MAX_ARGV {
            return Err("managed process argv must be non-empty and bounded".into());
        }
        if self.argv.iter().any(|item| item.contains('\0')) {
            return Err("managed process argv contains NUL".into());
        }
        if let Some(cwd) = &self.cwd {
            validate_guest_path("managed process cwd", Some(cwd))?;
        }
        if self.env.iter().any(|(key, value)| !is_env_key(key) || value.contains('\0')) {
            return Err("managed process environment is invalid".into());
        }
        if self.max_stdout_bytes < 1 || self.max_stderr_bytes < 1 {
            return Err("managed process log limits must be positive".into());
        }
        Ok(())
    }

    /// `control_payload(uid=, gid=, default_cwd=)`. `default_cwd` is the
    /// annotation's JSON value, whatever its type, as Python passes it on.
    pub fn control_payload(&self, uid: u32, gid: u32, default_cwd: Option<&Value>) -> Result<Value, String> {
        self.validate()?;
        let cwd = match &self.cwd {
            Some(cwd) => Some(cwd.as_str()),
            None => default_cwd.and_then(Value::as_str),
        };
        validate_guest_path("resolved managed process cwd", cwd)?;
        let mut payload = Map::new();
        payload.insert("version".into(), json!(1));
        payload.insert("action".into(), json!("start"));
        payload.insert("job_id".into(), json!(self.job_id));
        payload.insert("argv".into(), json!(self.argv));
        payload.insert("env".into(), json!(self.env));
        payload.insert("cwd".into(), json!(cwd.expect("validated")));
        payload.insert("uid".into(), json!(uid));
        payload.insert("gid".into(), json!(gid));
        payload.insert("max_stdout_bytes".into(), json!(self.max_stdout_bytes));
        payload.insert("max_stderr_bytes".into(), json!(self.max_stderr_bytes));
        Ok(Value::Object(payload))
    }
}

/// `control_request_bytes`: sorted keys, compact, ASCII escapes.
pub fn control_request_bytes(payload: &Value) -> Vec<u8> {
    crate::pyjson::dumps(payload).into_bytes()
}

/// The launch identity: `sha256(control_request_bytes(payload))`.
pub fn launch_sha256(payload: &Value) -> String {
    crate::pyjson::sha256_hex(&crate::pyjson::dumps(payload))
}

/// `_managed_workload_credentials` and the default cwd, from the bundle's
/// `config.json` annotations. Errors are Python's `ManagedProcessError`s (409).
pub fn workload_credentials(config: &Value) -> Result<(u32, u32), String> {
    let unavailable = || "managed process workload credentials are unavailable".to_string();
    let annotations = config.get("annotations").filter(|a| a.is_object()).ok_or_else(unavailable)?;
    let id = |key: &str| -> Result<i64, String> {
        match annotations.get(key).ok_or_else(unavailable)? {
            Value::String(text) => crate::exec::py_int(text).ok_or_else(unavailable),
            Value::Number(number) if number.is_i64() || number.is_u64() => number.as_i64().ok_or_else(unavailable),
            _ => Err(unavailable()),
        }
    };
    let (uid, gid) = (id(UID_ANNOTATION)?, id(GID_ANNOTATION)?);
    match (u32::try_from(uid), u32::try_from(gid)) {
        (Ok(uid), Ok(gid)) => Ok((uid, gid)),
        _ => Err("managed process workload credentials are invalid".into()),
    }
}

pub fn default_cwd(config: &Value) -> Result<&Value, String> {
    config
        .get("annotations")
        .and_then(|annotations| annotations.get(CWD_ANNOTATION))
        .ok_or_else(|| "managed process default cwd is unavailable".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Vectors from the repository's Python (ManagedProcessStart.from_dict,
    /// control_payload, control_request_bytes, hashlib.sha256).
    #[test]
    fn payload_bytes_and_launch_identity_match_python() {
        let cases = [
            (
                json!({"job_id": "job-1", "argv": ["python3", "-c", "print('hi')"]}),
                (1000, 1000),
                Some(json!("/workspace")),
                r#"{"action":"start","argv":["python3","-c","print('hi')"],"cwd":"/workspace","env":{},"gid":1000,"job_id":"job-1","max_stderr_bytes":16777216,"max_stdout_bytes":16777216,"uid":1000,"version":1}"#,
                "8a56e1ed0415717117dbe87a7f996dc18c5fae629842d26d18b875a04fe1be9d",
            ),
            (
                json!({"job_id": "  job:2.x_y-z\n", "argv": ["sh", "-c", "echo \u{e9}\u{2603} \u{1f600} \"q\" \\\\ \t\u{7f}\u{1c}"],
                       "env": {"ZED": "1", "A_B": "caf\u{e9}", "_x": ""}, "cwd": "/srv/app", "max_stdout_bytes": 5, "max_stderr_bytes": 7}),
                (0, 0),
                None,
                r#"{"action":"start","argv":["sh","-c","echo \u00e9\u2603 \ud83d\ude00 \"q\" \\\\ \t\u007f\u001c"],"cwd":"/srv/app","env":{"A_B":"caf\u00e9","ZED":"1","_x":""},"gid":0,"job_id":"job:2.x_y-z","max_stderr_bytes":7,"max_stdout_bytes":5,"uid":0,"version":1}"#,
                "4de6747ad5addc96a6d33721b8c3bb70b642ac0dfb3bdd9a8f321b15795bfb25",
            ),
        ];
        for (raw, (uid, gid), cwd, bytes, digest) in cases {
            let start = ManagedStart::from_value(&raw).unwrap();
            let payload = start.control_payload(uid, gid, cwd.as_ref()).unwrap();
            assert_eq!(String::from_utf8(control_request_bytes(&payload)).unwrap(), bytes);
            assert_eq!(launch_sha256(&payload), digest);
        }
    }

    #[test]
    fn invalid_requests_get_pythons_messages() {
        let cases = [
            (json!([]), "managed process payload must be a JSON object"),
            (json!({"argv": ["x"], "job_id": "j", "zz": 1, "aa": 2}), "unsupported managed process fields: aa, zz"),
            (json!({"argv": "x", "job_id": "j"}), "managed process argv must be a JSON string array"),
            (json!({"argv": ["x"], "env": {"A": 1}, "job_id": "j"}), "managed process env must contain strings"),
            (json!({"argv": ["x"], "cwd": 5, "job_id": "j"}), "managed process cwd must be a string"),
            (json!({"argv": ["x"], "job_id": 5}), "managed process job_id must be a string"),
            (json!({"argv": ["x"], "job_id": "j", "max_stdout_bytes": true}), "managed process log limits must be integers"),
            (json!({"argv": ["x"], "job_id": "j", "max_stdout_bytes": null}), "managed process log limits must be integers"),
            (json!({"argv": ["x"], "job_id": " "}), "managed process job_id is invalid"),
            (json!({"argv": [], "job_id": "j"}), "managed process argv must be non-empty and bounded"),
            (json!({"argv": ["a\u{0}"], "job_id": "j"}), "managed process argv contains NUL"),
            (json!({"argv": ["x"], "job_id": "j", "cwd": "rel"}), "managed process cwd must be an absolute container path."),
            (json!({"argv": ["x"], "job_id": "j", "cwd": "/a\n"}), "managed process cwd contains unsupported control characters."),
            (json!({"argv": ["x"], "job_id": "j", "cwd": "/a/../b"}), "managed process cwd cannot contain '..'."),
            (json!({"argv": ["x"], "job_id": "j", "env": {"1A": "x"}}), "managed process environment is invalid"),
            (json!({"argv": ["x"], "job_id": "j", "env": {"A": "\u{0}"}}), "managed process environment is invalid"),
            (json!({"argv": ["x"], "job_id": "j", "max_stderr_bytes": 0}), "managed process log limits must be positive"),
            (json!({"argv": vec!["x"; 4097], "job_id": "j"}), "managed process argv must be non-empty and bounded"),
        ];
        for (raw, message) in cases {
            assert_eq!(ManagedStart::from_value(&raw).unwrap_err(), message, "{raw}");
        }
        for raw in [
            json!({"argv": ["x"], "job_id": "j", "env": []}),
            json!({"argv": ["x"], "job_id": "j", "env": 0}),
            json!({"argv": ["x"], "job_id": "\u{1c}j\u{1f}"}),
        ] {
            assert!(ManagedStart::from_value(&raw).is_ok(), "{raw}");
        }
        let start = ManagedStart::from_value(&json!({"argv": ["x"], "job_id": "j"})).unwrap();
        for cwd in [None, Some(json!(5)), Some(json!("rel"))] {
            assert_eq!(
                start.control_payload(0, 0, cwd.as_ref()).unwrap_err(),
                "resolved managed process cwd must be an absolute container path."
            );
        }
    }

    #[test]
    fn credentials_come_from_the_annotations() {
        let config = |uid: Value, gid: Value| json!({"annotations": {UID_ANNOTATION: uid, GID_ANNOTATION: gid, CWD_ANNOTATION: "/w"}});
        assert_eq!(workload_credentials(&config(json!("1000"), json!("100"))), Ok((1000, 100)));
        assert_eq!(workload_credentials(&config(json!("x"), json!("100"))).unwrap_err(), "managed process workload credentials are unavailable");
        assert_eq!(workload_credentials(&config(json!("-1"), json!("100"))).unwrap_err(), "managed process workload credentials are invalid");
        assert_eq!(workload_credentials(&json!({})).unwrap_err(), "managed process workload credentials are unavailable");
        assert_eq!(default_cwd(&config(json!("0"), json!("0"))), Ok(&json!("/w")));
        assert!(default_cwd(&json!({"annotations": {}})).is_err());
    }
}
