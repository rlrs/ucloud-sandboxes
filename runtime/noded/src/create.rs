//! `POST /v1/sandboxes` in the daemon (phase 1). The Python agent keeps
//! admission: the daemon asks it to admit (startup slot, capacity, the
//! lifecycle lock, spec validation) and to finish (release, activity, the
//! response record and epochs), and runs everything in between itself.
//!
//! The daemon creates only what it fully supports. Any other request goes to
//! the agent unchanged, so every edge of the contract (framing errors, invalid
//! JSON, relay egress, the legacy image store) keeps Python's exact answer.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::header::{self, HeaderMap, HeaderValue};
use hyper::{Method, Request, Response, StatusCode};
use serde_json::{Map, Value, json};
use serde_json::value::RawValue;

use crate::Body;
use crate::agent_rpc::{AgentClient, Reply, RpcError};
use crate::timings::Timings;

pub const MAX_BODY_BYTES: u64 = 16 * 1024 * 1024;
const ADMISSION_WAIT_HEADER: &str = "x-ucloud-admission-wait";

/// What the pipeline failed with, in the node API's terms (node-api spec §1.6).
#[derive(Debug)]
pub enum CreateError {
    /// 400 `{"error"}`.
    Invalid(String),
    /// 409 `sandbox_registration_conflict`, not retryable.
    RegistrationOwned(String),
    /// Registry or storage capacity: the agent rolls the incarnation back and
    /// answers `node_active_admission_deferred`.
    Capacity(String),
    /// 503 `memory_publication_draining`.
    MemoryDraining(String),
    /// 503 `{"error"}`: ambiguous, the gateway keeps the route.
    Unavailable(String),
    /// Nothing durable was done and the daemon cannot do it: the agent
    /// releases the admission and creates the sandbox itself.
    Unsupported(String),
}

impl CreateError {
    fn response(&self) -> (StatusCode, Value) {
        match self {
            CreateError::Invalid(message) => (StatusCode::BAD_REQUEST, json!({"error": message})),
            CreateError::RegistrationOwned(message) => (
                StatusCode::CONFLICT,
                json!({"error": message, "error_code": "sandbox_registration_conflict", "retryable": false}),
            ),
            CreateError::MemoryDraining(message) => (
                StatusCode::SERVICE_UNAVAILABLE,
                json!({"error": message, "error_code": "memory_publication_draining", "retryable": true}),
            ),
            CreateError::Capacity(message) | CreateError::Unavailable(message) | CreateError::Unsupported(message) => {
                (StatusCode::SERVICE_UNAVAILABLE, json!({"error": message}))
            }
        }
    }
}

/// An admission the agent holds for this create, with Python's canonical view
/// of the request.
#[derive(Debug, Clone)]
pub struct Admitted {
    pub token: String,
    pub sandbox_id: String,
    pub generation: u64,
    pub operation_id: String,
    pub spec_hash: String,
    /// `SandboxSpec.to_dict()` as the agent parsed and validated it.
    pub spec: Value,
    pub requested_resources: Value,
    pub initial_claim: Option<Value>,
    pub split: bool,
    /// The digest of the agent's create configuration at admission.
    pub config_sha256: String,
}

/// Runs an admitted create from plan to `owned`; `Ok(true)` when it started
/// the runtime (a replay of an owned sandbox does not).
pub trait Pipeline: Send + Sync + 'static {
    fn create<'a>(
        &'a self,
        admitted: &'a Admitted,
        timings: &'a mut Timings,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<bool, CreateError>> + Send + 'a>>;

    /// Whether the daemon creates this spec itself (else the agent does).
    fn supports(&self, spec: &Map<String, Value>) -> bool;

    /// Whether the pipeline was built from this configuration digest.
    fn accepts_config(&self, _config_sha256: &str) -> bool {
        true
    }
}

pub enum Outcome {
    Response(Response<Body>),
    /// Not ours: send this request to the agent.
    Forward(Request<Body>),
}

pub struct CreateFront {
    agent: Arc<AgentClient>,
    pipeline: Arc<dyn Pipeline>,
    token: Bytes,
}

fn full(bytes: Bytes) -> Body {
    Full::new(bytes).map_err(|never| match never {}).boxed()
}

fn json_response(status: StatusCode, body: Bytes, extra: &HeaderMap) -> Response<Body> {
    let mut response = Response::new(full(body));
    *response.status_mut() = status;
    response.headers_mut().insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
    for (name, value) in extra {
        response.headers_mut().insert(name, value.clone());
    }
    response
}

/// Python `float(value)` for the admission-wait header, kept only when finite
/// and non-negative (admission.parse_admission_wait).
pub fn parse_admission_wait(value: &str) -> Option<f64> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    let bytes = trimmed.as_bytes();
    for (index, byte) in bytes.iter().enumerate() {
        // Python allows `_` only between digits.
        if *byte == b'_' {
            let digit = |i: Option<usize>| i.and_then(|i| bytes.get(i)).is_some_and(u8::is_ascii_digit);
            if !digit(index.checked_sub(1)) || !digit(Some(index + 1)) {
                return None;
            }
        }
    }
    let lowered = trimmed.replace('_', "").to_ascii_lowercase();
    if lowered.contains("nan") || lowered.contains("inf") {
        return None;
    }
    lowered.parse::<f64>().ok().filter(|wait| wait.is_finite() && *wait >= 0.0)
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter().zip(right).fold(0u8, |acc, (a, b)| acc | (a ^ b)) == 0
}

/// The strict `SandboxOperation.from_dict`; anything else is the agent's to reject.
fn parse_operation(raw: &Value) -> Option<(u64, String, String)> {
    let object = raw.as_object()?;
    if object.len() != 4 || ["generation", "kind", "operation_id", "spec_hash"].iter().any(|k| !object.contains_key(*k)) {
        return None;
    }
    let generation = object["generation"].as_u64().filter(|g| *g > 0 && object["generation"].is_u64())?;
    let operation_id = object["operation_id"].as_str()?.trim().to_string();
    let valid_id = !operation_id.is_empty()
        && operation_id.len() <= 128
        && operation_id.as_bytes()[0].is_ascii_alphanumeric()
        && operation_id.bytes().all(|b| b.is_ascii_alphanumeric() || b"_.:-".contains(&b));
    if !valid_id || object["kind"].as_str()? != "create" {
        return None;
    }
    let spec_hash = object["spec_hash"].as_str().filter(|hash| !hash.is_empty())?.to_string();
    Some((generation, operation_id, spec_hash))
}

impl CreateFront {
    pub fn new(agent: Arc<AgentClient>, pipeline: Arc<dyn Pipeline>, token: &str) -> Self {
        CreateFront { agent, pipeline, token: Bytes::copy_from_slice(token.as_bytes()) }
    }

    /// Whether to read this request ourselves: an authorized create with a
    /// body the agent would read too. Everything else streams to the agent.
    pub fn intercepts<B>(&self, request: &Request<B>) -> bool {
        if request.method() != Method::POST || request.uri().path() != "/v1/sandboxes" {
            return false;
        }
        let headers = request.headers();
        if headers.contains_key(header::TRANSFER_ENCODING) {
            return false;
        }
        let length = headers.get(header::CONTENT_LENGTH).and_then(|v| v.to_str().ok()).and_then(|v| v.parse::<u64>().ok());
        if !length.is_some_and(|length| length > 0 && length <= MAX_BODY_BYTES) {
            return false;
        }
        let supplied = headers.get(header::AUTHORIZATION).map(|v| v.as_bytes()).and_then(|v| v.strip_prefix(b"Bearer "));
        supplied.is_some_and(|supplied| !supplied.is_empty() && constant_time_eq(supplied, &self.token))
    }

    pub async fn handle(self: &Arc<Self>, request: Request<Body>) -> Outcome {
        let started = Instant::now();
        let (parts, body) = request.into_parts();
        let bytes = match Limited::new(body, MAX_BODY_BYTES as usize).collect().await {
            Ok(collected) => collected.to_bytes(),
            Err(_) => {
                let body = json!({"error": "request body ended before Content-Length bytes were read"});
                return Outcome::Response(json_response(StatusCode::BAD_REQUEST, Bytes::from(body.to_string()), &HeaderMap::new()));
            }
        };
        let read_request_ms = started.elapsed().as_millis() as u64;
        let forward = |bytes: Bytes| Outcome::Forward(Request::from_parts(parts.clone(), full(bytes)));
        let parse_started = Instant::now();
        let Ok(Value::Object(mut raw)) = serde_json::from_slice::<Value>(&bytes) else { return forward(bytes) };
        let Some((generation, operation_id, spec_hash)) = raw.remove("_ucloud_operation").as_ref().and_then(parse_operation) else {
            return forward(bytes);
        };
        if !self.pipeline.supports(&raw) {
            return forward(bytes);
        }
        let sandbox_id = raw.get("id").and_then(Value::as_str).unwrap_or_default().to_string();
        let wait = parts.headers.get(ADMISSION_WAIT_HEADER).and_then(|v| v.to_str().ok()).and_then(parse_admission_wait);
        let parse_spec_ms = parse_started.elapsed().as_millis() as u64;

        let admit = json!({
            "sandbox_id": sandbox_id, "generation": generation, "operation_id": operation_id,
            "spec": Value::Object(raw), "spec_hash": spec_hash, "admission_wait_seconds": wait,
        });
        // Past admission the create runs in its own task: a client that goes
        // away must not abandon a half-built incarnation and its admission.
        let front = self.clone();
        let ids = (sandbox_id, generation, operation_id, spec_hash);
        let task = tokio::spawn(async move { front.admitted(admit, ids, read_request_ms, parse_spec_ms, started).await });
        match task.await {
            Ok(Some(response)) => Outcome::Response(response),
            Ok(None) => Outcome::Forward(Request::from_parts(parts, full(bytes))),
            Err(error) => Outcome::Response(self.error(CreateError::Unavailable(format!("create task failed: {error}")))),
        }
    }

    /// Admit, run and finish; `None` hands the request to the agent.
    async fn admitted(
        &self,
        admit: Value,
        (sandbox_id, generation, operation_id, spec_hash): (String, u64, String, String),
        read_request_ms: u64,
        parse_spec_ms: u64,
        started: Instant,
    ) -> Option<Response<Body>> {
        let manager_started = Instant::now();
        let mut timings = Timings::default();
        let reply = match self.agent.call(Method::POST, "/internal/v1/creates/admit", Some(&admit)).await {
            Ok(reply) => reply,
            // Nothing reached the agent: it can still create this itself.
            Err(RpcError::Unavailable(_)) => return None,
            // It may hold an admission now: only a replay can tell.
            Err(error) => return Some(self.error(CreateError::Unavailable(error.to_string()))),
        };
        if reply.status != StatusCode::OK {
            // Admission refusals and validation errors are the agent's answer.
            return Some(relay(&reply));
        }
        let admitted = match admitted(&reply, sandbox_id, generation, operation_id, spec_hash) {
            Ok(admitted) => admitted,
            Err(error) => return Some(self.error(CreateError::Unavailable(error.to_string()))),
        };
        let result = if self.pipeline.accepts_config(&admitted.config_sha256) {
            self.pipeline.create(&admitted, &mut timings).await
        } else {
            Err(CreateError::Unsupported("the agent's create configuration changed".into()))
        };
        let finish = match &result {
            Ok(runtime_started) => json!({"outcome": "created", "runtime_started": runtime_started}),
            Err(CreateError::Capacity(message)) => json!({"outcome": "capacity_rejected", "message": message}),
            Err(error) => {
                let (status, body) = error.response();
                json!({"outcome": "failed", "status": status.as_u16(), "body": body})
            }
        };
        let finished = self.finish(&admitted.token, &finish).await;
        Some(match (result, finished) {
            (Ok(_), Ok(reply)) if reply.status == StatusCode::OK => {
                let manager_ms = manager_started.elapsed().as_millis() as u64;
                match self.created(&reply, read_request_ms, parse_spec_ms, manager_ms, &timings, started) {
                    Ok(response) => response,
                    Err(error) => self.error(CreateError::Unavailable(error.to_string())),
                }
            }
            // The sandbox runs but the agent did not record the answer (an
            // expired token, a crash): ambiguous, never a definite reject.
            (Ok(_), Ok(reply)) => self.error(CreateError::Unavailable(format!(
                "node agent did not finish the create ({}): {}",
                reply.status.as_u16(),
                String::from_utf8_lossy(&reply.body)
            ))),
            (Ok(_), Err(error)) => self.error(CreateError::Unavailable(error.to_string())),
            // The agent rolled back and answered the deferral itself; any
            // other answer means no rollback ran, so the outcome is ambiguous.
            (Err(CreateError::Capacity(message)), Ok(reply)) => {
                let deferred = reply.status == StatusCode::SERVICE_UNAVAILABLE
                    && reply.json().ok().and_then(|body| body.get("error_code").cloned())
                        == Some(Value::from("node_active_admission_deferred"));
                if deferred { relay(&reply) } else { self.error(CreateError::Unavailable(message)) }
            }
            (Err(CreateError::Capacity(message)), Err(_)) => self.error(CreateError::Unavailable(message)),
            // Released: the agent creates it the way it always has.
            (Err(CreateError::Unsupported(_)), Ok(reply)) if reply.status == StatusCode::OK => return None,
            (Err(CreateError::Unsupported(message)), _) => self.error(CreateError::Unavailable(message)),
            (Err(error), _) => self.error(error),
        })
    }

    /// Finish is the only way the agent learns the outcome, so it is retried
    /// while the agent is unreachable or out of request threads.
    async fn finish(&self, token: &str, body: &Value) -> Result<Reply, RpcError> {
        let path = format!("/internal/v1/creates/{token}/finish");
        let mut delay = Duration::from_millis(50);
        for _ in 0..40 {
            match self.agent.call(Method::POST, &path, Some(body)).await {
                Err(RpcError::Unavailable(_)) => {}
                Ok(reply) if reply.status == StatusCode::SERVICE_UNAVAILABLE
                    && reply.json().ok().and_then(|body| body.get("error_code").cloned())
                        == Some(Value::from("http_request_capacity_exhausted")) => {}
                other => return other,
            }
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(Duration::from_secs(1));
        }
        Err(RpcError::Unavailable("the agent did not take the create's finish".into()))
    }

    fn error(&self, error: CreateError) -> Response<Body> {
        let (status, body) = error.response();
        json_response(status, Bytes::from(body.to_string()), &HeaderMap::new())
    }

    /// `{"sandbox": <the agent's record, verbatim>, "timings": {...}}`.
    fn created(
        &self,
        reply: &Reply,
        read_request_ms: u64,
        parse_spec_ms: u64,
        manager_ms: u64,
        timings: &Timings,
        started: Instant,
    ) -> Result<Response<Body>, RpcError> {
        #[derive(serde::Deserialize)]
        struct Finished {
            status: u16,
            sandbox: Box<RawValue>,
            #[serde(default)]
            phases: Map<String, Value>,
        }
        let finished: Finished = serde_json::from_slice(&reply.body).map_err(|e| RpcError::Protocol(e.to_string()))?;
        let status = StatusCode::from_u16(finished.status).map_err(|e| RpcError::Protocol(e.to_string()))?;
        let mut phases = finished.phases;
        for (name, value) in timings.phases() {
            phases.insert(name.clone(), Value::from(*value));
        }
        let timings = json!({
            "total_ms": started.elapsed().as_millis() as u64,
            "phases": {"read_request_ms": read_request_ms, "parse_spec_ms": parse_spec_ms, "manager_create_ms": manager_ms},
            "manager": {"idempotent": status == StatusCode::OK, "total_ms": manager_ms, "phases": phases},
        });
        let body = format!("{{\"sandbox\":{},\"timings\":{}}}", finished.sandbox.get(), timings);
        Ok(json_response(status, Bytes::from(body), &HeaderMap::new()))
    }
}

fn admitted(reply: &Reply, sandbox_id: String, generation: u64, operation_id: String, spec_hash: String) -> Result<Admitted, RpcError> {
    let value = reply.json()?;
    let field = |name: &str| value.get(name).cloned().ok_or_else(|| RpcError::Protocol(format!("admit lacks {name}")));
    let token = field("token")?.as_str().map(str::to_string).ok_or_else(|| RpcError::Protocol("admit token".into()))?;
    let spec = field("spec")?;
    if spec.get("id").and_then(Value::as_str) != Some(sandbox_id.as_str()) {
        return Err(RpcError::Protocol("admitted spec has another id".into()));
    }
    Ok(Admitted {
        token,
        sandbox_id,
        generation,
        operation_id,
        spec_hash,
        spec,
        requested_resources: field("requested_resources")?,
        initial_claim: value.get("initial_claim").filter(|claim| !claim.is_null()).cloned(),
        split: value.get("split").and_then(Value::as_bool).unwrap_or(false),
        config_sha256: value.get("config_sha256").and_then(Value::as_str).unwrap_or_default().to_string(),
    })
}

/// The agent's own response, status, headers and body.
fn relay(reply: &Reply) -> Response<Body> {
    let mut headers = reply.headers.clone();
    for name in [header::CONNECTION, header::TRANSFER_ENCODING, header::CONTENT_LENGTH] {
        headers.remove(name);
    }
    let mut response = json_response(reply.status, reply.body.clone(), &HeaderMap::new());
    *response.headers_mut() = headers;
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admission_wait_parses_like_python_float() {
        for (raw, expected) in [
            (" 0.5 ", Some(0.5)), ("1_0", Some(10.0)), ("2", Some(2.0)), ("1e1", Some(10.0)), (".5", Some(0.5)),
            ("nan", None), ("inf", None), ("-1", None), ("soon", None), ("", None), ("_1", None), ("1__0", None),
        ] {
            assert_eq!(parse_admission_wait(raw), expected, "{raw:?}");
        }
    }

    #[test]
    fn operation_parse_is_strict() {
        let good = json!({"generation": 1, "kind": "create", "operation_id": " create-ab ", "spec_hash": "x"});
        assert_eq!(parse_operation(&good), Some((1, "create-ab".into(), "x".into())));
        for bad in [
            json!({"generation": true, "kind": "create", "operation_id": "a", "spec_hash": "x"}),
            json!({"generation": 0, "kind": "create", "operation_id": "a", "spec_hash": "x"}),
            json!({"generation": 1.0, "kind": "create", "operation_id": "a", "spec_hash": "x"}),
            json!({"generation": 1, "kind": "delete", "operation_id": "a", "spec_hash": "x"}),
            json!({"generation": 1, "kind": "create", "operation_id": "-a", "spec_hash": "x"}),
            json!({"generation": 1, "kind": "create", "operation_id": "a", "spec_hash": ""}),
            json!({"generation": 1, "kind": "create", "operation_id": "a", "spec_hash": "x", "extra": 1}),
        ] {
            assert_eq!(parse_operation(&bad), None, "{bad}");
        }
    }
}
