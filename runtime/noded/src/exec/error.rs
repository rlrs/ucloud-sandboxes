//! Route answers and the errors of the exec routes, mapped as node_agent.py
//! maps them (`_start_exec`'s own admission cases, then `_write_exception`).

use serde_json::{Value, json};

/// One HTTP answer: status, JSON body (encode with [`super::json::dumps`]) and
/// the headers besides `Content-Type: application/json` and `Content-Length`.
#[derive(Clone, Debug, PartialEq)]
pub struct Reply {
    pub status: u16,
    pub body: Value,
    pub headers: Vec<(&'static str, &'static str)>,
}

impl Reply {
    pub fn new(status: u16, body: Value) -> Self {
        Reply { status, body, headers: Vec::new() }
    }

    pub fn ok(body: Value) -> Self {
        Reply::new(200, body)
    }

    /// The body as Python's `_write_json` writes it.
    pub fn body_bytes(&self) -> Vec<u8> {
        super::json::dumps(&self.body).into_bytes()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExecError {
    /// A `ValueError`: 400 `{"error"}`.
    BadRequest(String),
    /// 404 `{"error"}` (R2, R3 unknown sessions).
    NotFound(String),
    /// R1's `SandboxCapacityUnavailableError`/`ExecSessionCapacityError`:
    /// 503 `node_active_exec_deferred`, `Retry-After: 1` only.
    ExecDeferred(String),
    /// R1's `SandboxAdmissionClosedError`: 503 `node_admission_closed`,
    /// `Retry-After: 1` only.
    AdmissionClosed(String),
    /// Any other `RuntimeError`: 503 `{"error"}`.
    Unavailable(String),
    /// Python would answer, but with a message this port does not reproduce
    /// (a JSON decoder error, a `repr` of non-ASCII text). Forward the request
    /// to Python when possible; [`ExecError::reply`] gives a 400 otherwise.
    Forward(String),
}

impl ExecError {
    pub fn message(&self) -> &str {
        match self {
            ExecError::BadRequest(message)
            | ExecError::NotFound(message)
            | ExecError::ExecDeferred(message)
            | ExecError::AdmissionClosed(message)
            | ExecError::Unavailable(message)
            | ExecError::Forward(message) => message,
        }
    }

    pub fn reply(&self) -> Reply {
        let error = |status, message: &str| Reply::new(status, json!({"error": message}));
        let retryable = |code: &str, message: &str| Reply {
            status: 503,
            body: json!({"error": message, "error_code": code, "retryable": true}),
            headers: vec![("Retry-After", "1")],
        };
        match self {
            ExecError::BadRequest(message) | ExecError::Forward(message) => error(400, message),
            ExecError::NotFound(message) => error(404, message),
            ExecError::Unavailable(message) => error(503, message),
            ExecError::ExecDeferred(message) => retryable("node_active_exec_deferred", message),
            ExecError::AdmissionClosed(message) => retryable("node_admission_closed", message),
        }
    }
}

impl std::fmt::Display for ExecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.message())
    }
}

impl std::error::Error for ExecError {}
