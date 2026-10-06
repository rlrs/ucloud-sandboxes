//! Request parsing for the exec routes, with Python's checks, order and
//! messages: `SandboxExecSpec.from_dict` and `validate` (sandbox_exec.py), the
//! route bodies and queries in node_agent.py, `parse_qs`, `int()` and `float()`.

use std::time::Duration;

use serde_json::{Map, Value, json};

use super::error::ExecError;
use crate::pyjson::float_repr;

/// The R1 body (`SandboxExecSpec`) plus the route's query and prefix header.
#[derive(Clone, Debug, PartialEq)]
pub struct ExecRequest {
    pub sandbox_id: String,
    pub command: Vec<String>,
    /// In request order; a key repeated in the JSON keeps its first position
    /// and its last value, as a Python dict does.
    pub env: Vec<(String, String)>,
    pub working_dir: Option<String>,
    pub stdin: bool,
    pub tty: bool,
    /// `?initial_wait_seconds=`, in seconds (0..=0.05).
    pub initial_wait: Option<f64>,
    /// The `X-UCloud-Exec-Session-Prefix` header as received (shape-checked
    /// only when the session is named; never trusted for anything else).
    pub session_prefix: Option<String>,
}

impl ExecRequest {
    /// R1 up to the point where Python takes the lifecycle fence, in Python's
    /// order: body JSON, `from_dict`, `initial_wait_seconds`, `validate()`.
    ///
    /// `sandbox_id` is the decoded path segment. `query` is the raw query
    /// string (without `?`). Errors are [`ExecError::BadRequest`] with Python's
    /// message, or [`ExecError::Forward`] where the message would differ.
    pub fn parse(sandbox_id: &str, body: &[u8], query: &str, session_prefix: Option<&str>) -> Result<Self, ExecError> {
        let raw = read_json_body(body)?;
        let Value::Object(raw) = raw else {
            return Err(bad("exec payload must be a JSON object"));
        };
        const KEYS: [&str; 5] = ["command", "env", "working_dir", "stdin", "tty"];
        if raw.len() != KEYS.len() || !KEYS.iter().all(|key| raw.contains_key(*key)) {
            return Err(bad("exec payload has an invalid schema"));
        }
        let command = match &raw["command"] {
            Value::Array(items) if items.iter().all(Value::is_string) => {
                items.iter().map(|item| item.as_str().unwrap_or_default().to_owned()).collect::<Vec<_>>()
            }
            _ => return Err(bad("exec command must be a JSON string array")),
        };
        let env = match &raw["env"] {
            Value::Object(map) if map.values().all(Value::is_string) => map
                .iter()
                .map(|(key, value)| (key.clone(), value.as_str().unwrap_or_default().to_owned()))
                .collect::<Vec<_>>(),
            _ => return Err(bad("exec env must be a JSON string map")),
        };
        let working_dir = match &raw["working_dir"] {
            Value::Null => None,
            Value::String(text) => Some(text.clone()),
            _ => return Err(bad("exec working_dir must be a string or null")),
        };
        let (Value::Bool(stdin), Value::Bool(tty)) = (&raw["stdin"], &raw["tty"]) else {
            return Err(bad("exec stdin and tty must be booleans"));
        };
        let initial_wait = match first(&parse_qs(query), "initial_wait_seconds") {
            None => None,
            Some(text) => {
                let Some(value) = py_float(text) else {
                    return Err(match py_repr(text) {
                        Some(repr) => bad(&format!("could not convert string to float: {repr}")),
                        None => ExecError::Forward(format!("could not convert string to float: {text:?}")),
                    });
                };
                if !value.is_finite() || !(0.0..=0.05).contains(&value) {
                    return Err(bad("initial_wait_seconds must be between 0 and 0.05"));
                }
                Some(value)
            }
        };
        let request = ExecRequest {
            sandbox_id: sandbox_id.to_owned(),
            command,
            env,
            working_dir,
            stdin: *stdin,
            tty: *tty,
            initial_wait,
            session_prefix: session_prefix.map(str::to_owned),
        };
        request.validate()?;
        Ok(request)
    }

    /// `SandboxExecSpec.validate()`.
    fn validate(&self) -> Result<(), ExecError> {
        if self.sandbox_id.is_empty() {
            return Err(bad("sandbox id is required."));
        }
        if self.command.is_empty() {
            return Err(bad("exec command cannot be empty."));
        }
        if self.command.iter().any(|item| item.contains('\0')) {
            return Err(bad("exec command cannot contain NUL bytes."));
        }
        if self.env.iter().any(|(key, value)| key.contains('\0') || value.contains('\0')) {
            return Err(bad("exec environment cannot contain NUL bytes."));
        }
        if self.working_dir.as_deref().is_some_and(|dir| dir.contains('\0')) {
            return Err(bad("exec working_dir cannot contain NUL bytes."));
        }
        Ok(())
    }

    /// The checks Python makes after its lifecycle fence, in its order:
    /// `DirectExecRuntime.exec_command` (TTY) and `DirectRunscWarden.exec_lease`
    /// (working directory, environment names). Python answers 400 for these
    /// after a possible wake or thaw; call this only for requests the daemon
    /// takes (owned, running), so the answer is the same.
    pub fn check_direct(&self) -> Result<(), ExecError> {
        if self.tty {
            return Err(bad("direct runtime TTY exec is not yet qualified"));
        }
        if self.working_dir.as_deref().is_some_and(|dir| !dir.starts_with('/') || dir.contains('\0')) {
            return Err(bad("exec working directory must be absolute"));
        }
        if self.env.iter().any(|(key, value)| !valid_env_name(key) || value.contains('\0')) {
            return Err(bad("direct exec environment is invalid"));
        }
        Ok(())
    }

    /// `SandboxExecSpec.to_dict()`.
    pub fn spec_json(&self) -> Value {
        let env: Map<String, Value> = self.env.iter().map(|(key, value)| (key.clone(), json!(value))).collect();
        json!({
            "sandbox_id": self.sandbox_id,
            "command": self.command,
            "env": env,
            "working_dir": self.working_dir,
            "stdin": self.stdin,
            "tty": self.tty,
        })
    }
}

/// `[A-Za-z_][A-Za-z0-9_]*`.
fn valid_env_name(name: &str) -> bool {
    let mut bytes = name.bytes();
    matches!(bytes.next(), Some(b'A'..=b'Z' | b'a'..=b'z' | b'_'))
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

/// R3's query: `after` and `limit` through `_int_query` (default on a parse
/// error), `wait_seconds` clamped to [0, 30].
///
/// Python's `float(wait_seconds)` sits outside the handler's `try`, so a
/// non-numeric value drops the connection (spec §7.1); here it reads as the
/// default 0, as `_int_query` treats `after` and `limit`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct EventsQuery {
    pub after: i64,
    pub limit: i64,
    pub wait: Duration,
}

impl EventsQuery {
    pub const MAX_WAIT_SECONDS: f64 = 30.0;

    pub fn parse(query: &str) -> Self {
        let pairs = parse_qs(query);
        let int = |key, default| first(&pairs, key).map_or(Some(default), py_int).unwrap_or(default);
        let wait = first(&pairs, "wait_seconds").map_or(Some(0.0), py_float).unwrap_or(0.0);
        // Python's min(30.0, max(0.0, v)): NaN reads as 0.
        let wait = 0.0f64.max(wait).min(Self::MAX_WAIT_SECONDS);
        EventsQuery { after: int("after", 0), limit: int("limit", 100), wait: Duration::from_secs_f64(wait) }
    }
}

/// R4's body: `str(raw.get("data") or "")` and `raw.get("eof")` truthiness.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StdinRequest {
    pub data: String,
    pub eof: bool,
}

impl StdinRequest {
    /// Python `str()`s any truthy `data`; this port does so for scalars
    /// (`true` → `"True"`, `1` → `"1"`, floats by `repr`) and refuses
    /// non-empty arrays and objects (400), whose Python `repr` it does not
    /// reproduce. No SDK sends those (spec §7.7).
    pub fn parse(body: &[u8]) -> Result<Self, ExecError> {
        let Value::Object(raw) = read_json_body(body)? else {
            return Err(bad("stdin payload must be a JSON object"));
        };
        let data = match raw.get("data") {
            Some(value) if !truthy(value) => String::new(),
            None => String::new(),
            Some(Value::String(text)) => text.clone(),
            Some(Value::Bool(_)) => "True".to_owned(),
            Some(Value::Number(number)) => match (number.as_i64(), number.as_u64()) {
                (Some(value), _) => value.to_string(),
                (None, Some(value)) => value.to_string(),
                _ => integer_literal(body, "data").unwrap_or_else(|| float_repr(number.as_f64().unwrap_or(f64::NAN))),
            },
            Some(_) => return Err(bad("stdin data must be a string")),
        };
        Ok(StdinRequest { data, eof: raw.get("eof").is_some_and(truthy) })
    }
}

/// An integer too large for serde's integers, as written: JSON integer
/// literals are Python's `str(int)` (no `+`, no leading zeros).
fn integer_literal(body: &[u8], key: &str) -> Option<String> {
    let raw: std::collections::HashMap<String, Box<serde_json::value::RawValue>> = serde_json::from_slice(body).ok()?;
    let literal = raw.get(key)?.get();
    let digits = literal.strip_prefix('-').unwrap_or(literal);
    (!digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit())).then(|| literal.to_owned())
}

/// R6's body: exactly `{"signal": n}` with an integer (not a boolean) n in
/// [1, 64] (node_agent.py `_signal_exec`, `ExecSessionManager.signal`).
pub fn parse_signal(body: &[u8]) -> Result<i32, ExecError> {
    let raw = read_json_body(body)?;
    let signal = match &raw {
        Value::Object(map) if map.len() == 1 && map.contains_key("signal") => &map["signal"],
        _ => return Err(bad("signal payload must contain exactly signal")),
    };
    match signal.as_i64() {
        Some(value @ 1..=64) => Ok(value as i32),
        _ => Err(bad("signal must be an integer in [1, 64]")),
    }
}

/// http_server.py `_read_json_body` after the framing checks: UTF-8, not
/// empty, `json.loads`. Decoder failures are [`ExecError::Forward`]: Python's
/// text differs (`invalid JSON: Expecting value: line 1 column 1 (char 0)`),
/// and `json.loads` takes NaN and Infinity, which serde does not.
fn read_json_body(body: &[u8]) -> Result<Value, ExecError> {
    let text = std::str::from_utf8(body).map_err(|error| ExecError::Forward(format!("invalid UTF-8 body: {error}")))?;
    if text.is_empty() {
        return Err(bad("empty request body"));
    }
    serde_json::from_str(text).map_err(|error| ExecError::Forward(format!("invalid JSON: {error}")))
}

fn bad(message: &str) -> ExecError {
    ExecError::BadRequest(message.to_owned())
}

/// Python truthiness of a JSON value.
fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Number(number) => number.as_f64().is_some_and(|value| value != 0.0),
        Value::String(text) => !text.is_empty(),
        Value::Array(items) => !items.is_empty(),
        Value::Object(map) => !map.is_empty(),
    }
}

/// `urllib.parse.parse_qs(query)` flattened in order: `&` separates fields,
/// fields without `=` or with an empty value are dropped, `+` is a space,
/// percent escapes decode as UTF-8 with replacement.
pub fn parse_qs(query: &str) -> Vec<(String, String)> {
    query
        .split('&')
        .filter_map(|field| field.split_once('='))
        .filter(|(_, value)| !value.is_empty())
        .map(|(name, value)| (unquote_plus(name), unquote_plus(value)))
        .collect()
}

/// `parse_qs(...).get(key)[0]`.
fn first<'a>(pairs: &'a [(String, String)], key: &str) -> Option<&'a str> {
    pairs.iter().find(|(name, _)| name == key).map(|(_, value)| value.as_str())
}

fn unquote_plus(text: &str) -> String {
    unquote(&text.replace('+', " "))
}

/// `urllib.parse.unquote`: each ASCII run is percent-decoded and decoded as
/// UTF-8 with replacement; other characters pass through.
fn unquote(text: &str) -> String {
    if !text.contains('%') {
        return text.to_owned();
    }
    let bytes = text.as_bytes();
    let mut out = String::with_capacity(text.len());
    let mut start = 0;
    while start < bytes.len() {
        let ascii = bytes[start].is_ascii();
        let end = bytes[start..].iter().position(|byte| byte.is_ascii() != ascii).map_or(bytes.len(), |n| start + n);
        if ascii {
            let run = &bytes[start..end];
            let mut decoded = Vec::with_capacity(run.len());
            let mut index = 0;
            while index < run.len() {
                let hex = |at: usize| run.get(at).and_then(|byte| (*byte as char).to_digit(16));
                match (run[index], hex(index + 1), hex(index + 2)) {
                    (b'%', Some(high), Some(low)) => {
                        decoded.push((high * 16 + low) as u8);
                        index += 3;
                    }
                    (byte, _, _) => {
                        decoded.push(byte);
                        index += 1;
                    }
                }
            }
            out.push_str(&String::from_utf8_lossy(&decoded));
        } else {
            out.push_str(&text[start..end]);
        }
        start = end;
    }
    out
}

/// `str.isspace()`, as `int()` and `float()` strip it.
fn py_space(c: char) -> bool {
    c.is_whitespace() || ('\u{1c}'..='\u{1f}').contains(&c)
}

/// Python `int(text)` for base-10 ASCII digits, saturating to i64 (callers
/// only compare and clamp). Unicode decimal digits, which Python accepts,
/// read as invalid; so do more than 4,300 digits (CPython's conversion limit).
pub fn py_int(text: &str) -> Option<i64> {
    let text = text.trim_matches(py_space);
    let (negative, digits) = match text.as_bytes().first() {
        Some(b'-') => (true, &text[1..]),
        Some(b'+') => (false, &text[1..]),
        _ => (false, text),
    };
    let end = digit_part(digits.as_bytes(), 0)?;
    if end != digits.len() {
        return None;
    }
    if digits.bytes().filter(u8::is_ascii_digit).count() > 4300 {
        return None;
    }
    let mut value: i64 = 0;
    for digit in digits.bytes().filter(u8::is_ascii_digit) {
        let digit = i64::from(digit - b'0');
        value = value.saturating_mul(10);
        value = if negative { value.saturating_sub(digit) } else { value.saturating_add(digit) };
    }
    Some(value)
}

/// Python `float(text)` for ASCII input: decimal literals with PEP 515
/// underscores, `inf`, `infinity` and `nan` in any case, with a sign.
pub fn py_float(text: &str) -> Option<f64> {
    let text = text.trim_matches(py_space);
    let (negative, body) = match text.as_bytes().first() {
        Some(b'-') => (true, &text[1..]),
        Some(b'+') => (false, &text[1..]),
        _ => (false, text),
    };
    let lower = body.to_ascii_lowercase();
    let value = match lower.as_str() {
        "inf" | "infinity" => f64::INFINITY,
        "nan" => f64::NAN,
        _ => {
            let bytes = body.as_bytes();
            let mut index = match bytes.first() {
                Some(b'0'..=b'9') => {
                    let mut index = digit_part(bytes, 0)?;
                    if bytes.get(index) == Some(&b'.') {
                        index += 1;
                        if bytes.get(index).is_some_and(u8::is_ascii_digit) {
                            index = digit_part(bytes, index)?;
                        }
                    }
                    index
                }
                Some(b'.') => digit_part(bytes, 1)?,
                _ => return None,
            };
            if matches!(bytes.get(index), Some(b'e' | b'E')) {
                index += 1;
                if matches!(bytes.get(index), Some(b'+' | b'-')) {
                    index += 1;
                }
                index = digit_part(bytes, index)?;
            }
            if index != bytes.len() {
                return None;
            }
            body.replace('_', "").parse::<f64>().ok()?
        }
    };
    Some(if negative { -value } else { value })
}

/// `digit ("_"? digit)*` from `start`; the end index.
fn digit_part(bytes: &[u8], start: usize) -> Option<usize> {
    if !bytes.get(start).is_some_and(u8::is_ascii_digit) {
        return None;
    }
    let mut index = start + 1;
    loop {
        match bytes.get(index) {
            Some(b'0'..=b'9') => index += 1,
            Some(b'_') if bytes.get(index + 1).is_some_and(u8::is_ascii_digit) => index += 2,
            _ => return Some(index),
        }
    }
}

/// Python `repr(str)` for ASCII text; None when the text is not ASCII (its
/// `repr` depends on Unicode printability).
pub(crate) fn py_repr(text: &str) -> Option<String> {
    if !text.is_ascii() {
        return None;
    }
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
            c if (c as u32) < 0x20 || c as u32 == 0x7f => out.push_str(&format!("\\x{:02x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push(quote);
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    const BODY: &[u8] = br#"{"command":["sh","-c","echo hi"],"env":{"B":"2","A":"1"},"working_dir":null,"stdin":false,"tty":false}"#;

    fn error(body: &[u8], query: &str) -> String {
        ExecRequest::parse("sbx", body, query, None).unwrap_err().to_string()
    }

    #[test]
    fn parses_the_exec_body() {
        let request = ExecRequest::parse("sbx", BODY, "initial_wait_seconds=0.05", Some("p")).unwrap();
        assert_eq!(request.command, ["sh", "-c", "echo hi"]);
        assert_eq!(request.env, [("B".to_owned(), "2".to_owned()), ("A".to_owned(), "1".to_owned())]);
        assert_eq!(request.initial_wait, Some(0.05));
        assert_eq!(request.session_prefix.as_deref(), Some("p"));
        assert_eq!(
            crate::exec::json::dumps(&request.spec_json()),
            r#"{"sandbox_id":"sbx","command":["sh","-c","echo hi"],"env":{"B":"2","A":"1"},"working_dir":null,"stdin":false,"tty":false}"#
        );
        request.check_direct().unwrap();
    }

    #[test]
    fn rejects_like_python_in_python_order() {
        assert_eq!(error(b"[]", ""), "exec payload must be a JSON object");
        assert_eq!(error(b"{}", ""), "exec payload has an invalid schema");
        assert_eq!(error(b"", ""), "empty request body");
        let with = |command: &str| {
            format!(r#"{{"command":{command},"env":{{}},"working_dir":null,"stdin":false,"tty":false}}"#).into_bytes()
        };
        assert_eq!(error(&with("[1]"), ""), "exec command must be a JSON string array");
        // The query is read before validate(): its error wins over an empty command.
        assert_eq!(error(&with("[]"), "initial_wait_seconds=x"), "could not convert string to float: 'x'");
        assert_eq!(error(&with("[]"), "initial_wait_seconds=0.06"), "initial_wait_seconds must be between 0 and 0.05");
        assert_eq!(error(&with("[]"), "initial_wait_seconds=nan"), "initial_wait_seconds must be between 0 and 0.05");
        assert_eq!(error(&with("[]"), "initial_wait_seconds="), "exec command cannot be empty.");
        assert_eq!(error(&with(r#"["a\u0000"]"#), ""), "exec command cannot contain NUL bytes.");
        assert!(matches!(ExecRequest::parse("sbx", b"{", "", None), Err(ExecError::Forward(_))));
    }

    #[test]
    fn direct_checks() {
        let mut request = ExecRequest::parse("sbx", BODY, "", None).unwrap();
        request.working_dir = Some("tmp".into());
        assert_eq!(request.check_direct().unwrap_err().to_string(), "exec working directory must be absolute");
        request.working_dir = None;
        request.env.push(("1A".into(), "x".into()));
        assert_eq!(request.check_direct().unwrap_err().to_string(), "direct exec environment is invalid");
        request.tty = true;
        assert_eq!(request.check_direct().unwrap_err().to_string(), "direct runtime TTY exec is not yet qualified");
    }

    #[test]
    fn events_query() {
        let query = EventsQuery::parse("after=3&limit=+7&wait_seconds=1_0.5");
        assert_eq!((query.after, query.limit, query.wait), (3, 7, Duration::from_secs_f64(10.5)));
        let query = EventsQuery::parse("after=x&limit=&wait_seconds=bogus");
        assert_eq!((query.after, query.limit, query.wait), (0, 100, Duration::ZERO));
        assert_eq!(EventsQuery::parse("wait_seconds=inf").wait, Duration::from_secs(30));
        assert_eq!(EventsQuery::parse("wait_seconds=-nan").wait, Duration::ZERO);
        assert_eq!(EventsQuery::parse("after=99999999999999999999999").after, i64::MAX);
    }

    #[test]
    fn stdin_and_signal_bodies() {
        let parse = |body: &str| StdinRequest::parse(body.as_bytes());
        assert_eq!(parse(r#"{"data":"x","eof":1}"#).unwrap(), StdinRequest { data: "x".into(), eof: true });
        assert_eq!(parse(r#"{"data":true}"#).unwrap().data, "True");
        assert_eq!(parse(r#"{"data":1.5,"eof":[]}"#).unwrap(), StdinRequest { data: "1.5".into(), eof: false });
        assert_eq!(parse(r#"{"data":0}"#).unwrap().data, "");
        assert_eq!(parse("[]").unwrap_err().to_string(), "stdin payload must be a JSON object");
        assert_eq!(parse_signal(br#"{"signal":15}"#).unwrap(), 15);
        assert_eq!(parse_signal(br#"{"signal":true}"#).unwrap_err().to_string(), "signal must be an integer in [1, 64]");
        assert_eq!(parse_signal(br#"{"signal":65}"#).unwrap_err().to_string(), "signal must be an integer in [1, 64]");
        assert_eq!(
            parse_signal(br#"{"signal":1,"x":2}"#).unwrap_err().to_string(),
            "signal payload must contain exactly signal"
        );
    }

    #[test]
    fn query_decoding() {
        assert_eq!(parse_qs("a=1&b&c=&a=2&d=%41+%zz%e2%82"), [
            ("a".to_owned(), "1".to_owned()),
            ("a".to_owned(), "2".to_owned()),
            ("d".to_owned(), "A %zz\u{FFFD}".to_owned()),
        ]);
    }
}
