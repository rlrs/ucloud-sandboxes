//! The agent's admission routes on noded's internal socket
//! (docs/admission-port-3c.md §3.3), on top of [`AdmissionService`].
//! `internal.rs` owns the transport (auth, sessions, framing); this module
//! parses bodies and answers. Bodies are JSON objects with exactly the keys
//! listed; errors are `{error, error_code, retryable}`.
//!
//! - `POST admission/startup` `{owner, requested: {vcpu, memory_mb}, managed,
//!   wait_seconds: null | f64}` → `200 {token, phases: {startup_admission_ms,
//!   active_capacity_ms}}`; `503 node_admission_closed`;
//!   `503 node_active_admission_deferred` (capacity, or no slot in time).
//! - `POST admission/restore` `{owner, requested, cost, deadline_ms}` →
//!   `200 {token}`; `503 node_restore_busy`; `503 node_admission_closed`;
//!   `409 generation_mismatch`.
//! - `POST admission/observe` `{owner, kind: "capture", cost}` → `200 {token}`.
//! - `POST admission/publication` `{owner}` → `200 {token}`;
//!   `409 publication_deferred`.
//! - `POST admission/{token}/release` `{}` → `200 {}`;
//!   `404 admission_token_unknown`.
//! - `POST admission/cancel` `{owner}` → `200 {cancelled}`.
//! - `POST admission/close` and `admission/open` `{drain_token}` →
//!   `200 {admission_open, transient_revision}`.
//! - `GET admission/snapshot` → `200 {noded_session, admission_open,
//!   transient_revision, resource_reservations: [{owner, vcpu, memory_mb}],
//!   active_reservations, transitions: {kind: {active, waiting, memory_bytes,
//!   read_bytes, write_bytes}}, resident_demand: {…}}`.
//! - `POST growth/events` `{items: [{action, owner, request_id?, job_id?}]}`
//!   → `200 {results: [{intent} | {error, error_code}]}`.
//! - `POST growth/continuation` `{owner, request_id, deadline_ms}` →
//!   `200 {intent}`; `503 node_startup_busy`; `503 node_admission_closed`;
//!   `409 generation_mismatch`.
//!
//! Every grant-making route answers `503 admission_recovering` while the
//! service recovers, and `503 agent_hello_required` to a session that has
//! not said hello. `owner` is `{sandbox_id, generation}`; `cost` is
//! `{memory_bytes, ram_backing_bytes?, read_bytes?, write_bytes?,
//! provenance?}` with `null` for unknown; `deadline_ms` is the wait budget
//! from receipt.

use std::sync::Arc;

use hyper::{Method, StatusCode};
use serde_json::{Map, Value, json};

use super::Owner;
use super::ledger::{Cost, Kind, Shape};
use super::service::{AdmissionService, Event, EventAction, Holder, Reassert, Refusal};
use crate::registry::{GrowthIntent, is_operation_id, is_sandbox_id};

/// Items one events request may carry (the sender's batch bound).
pub const MAX_GROWTH_EVENTS: usize = 4096;

/// An answer, and the grant its body carries: released unless the body is
/// handed to the connection.
pub struct Answer {
    pub status: StatusCode,
    pub body: Value,
    pub grant: Option<Grant>,
}

/// A granted token that has not reached the agent yet. Dropping it releases
/// the claim (a client that disconnected never learns its token).
pub struct Grant {
    service: Arc<AdmissionService>,
    holder: Holder,
    token: String,
    delivered: bool,
}

impl Grant {
    pub fn delivered(&mut self) {
        self.delivered = true;
    }
}

impl Drop for Grant {
    fn drop(&mut self) {
        if !self.delivered {
            self.service.release(&self.token, &self.holder);
        }
    }
}

fn answer(status: StatusCode, body: Value) -> Answer {
    Answer { status, body, grant: None }
}

pub fn error(status: StatusCode, code: &str, message: &str, retryable: bool) -> Answer {
    answer(status, json!({"error": message, "error_code": code, "retryable": retryable}))
}

fn invalid(message: &str) -> Answer {
    error(StatusCode::BAD_REQUEST, "invalid_request", message, false)
}

/// Which route a refusal answers, for its capacity code.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Context {
    Startup,
    Restore,
    Continuation,
    Other,
}

fn refused(context: Context, refusal: Refusal) -> Answer {
    let message = refusal.message();
    let unavailable = StatusCode::SERVICE_UNAVAILABLE;
    match refusal {
        Refusal::Closed(_) => error(unavailable, "node_admission_closed", &message, true),
        Refusal::Capacity(_) | Refusal::Busy(_) => match context {
            Context::Restore => error(unavailable, "node_restore_busy", &message, true),
            Context::Continuation => error(unavailable, "node_startup_busy", &message, true),
            Context::Startup | Context::Other => error(unavailable, "node_active_admission_deferred", &message, true),
        },
        Refusal::Conflict(_) => error(StatusCode::CONFLICT, "generation_mismatch", &message, false),
        Refusal::Gone(_) => error(StatusCode::CONFLICT, "sandbox_unavailable", &message, false),
        Refusal::NotManaged(_) => error(StatusCode::CONFLICT, "not_managed_process", &message, false),
        Refusal::PrimaryOwned(job_id) => answer(
            StatusCode::CONFLICT,
            json!({"error": message, "error_code": "primary_already_owned", "retryable": false, "job_id": job_id}),
        ),
        Refusal::Deferred => error(StatusCode::CONFLICT, "publication_deferred", &message, true),
        Refusal::Recovering => error(unavailable, "admission_recovering", &message, true),
        Refusal::HelloRequired => error(unavailable, "agent_hello_required", &message, true),
        Refusal::Invalid(_) => error(StatusCode::BAD_REQUEST, "invalid_request", &message, false),
        Refusal::Unavailable(_) => error(unavailable, "admission_unavailable", &message, true),
    }
}

/// A growth event's error code (`{error, error_code}` in its result).
fn event_code(refusal: &Refusal) -> &'static str {
    match refusal {
        Refusal::Gone(_) => "sandbox_unavailable",
        Refusal::Conflict(_) => "growth_conflict",
        Refusal::PrimaryOwned(_) => "primary_already_owned",
        Refusal::Invalid(_) => "invalid_request",
        _ => "registry_unavailable",
    }
}

pub fn intent_json(intent: &GrowthIntent) -> Value {
    json!({
        "sandbox_id": intent.sandbox_id,
        "generation": intent.generation,
        "job_id": intent.job_id,
        "launch_sha256": intent.launch_sha256,
        "memory_bytes": intent.memory_bytes,
        "phase": intent.phase,
        "request_id": intent.request_id,
    })
}

fn owner_json(owner: &Owner) -> Value {
    json!({"sandbox_id": owner.0, "generation": owner.1})
}

// Parsing.

/// A JSON object with exactly `required` keys, plus any of `optional`.
fn fields<'a>(value: &'a Value, required: &[&str], optional: &[&str], what: &str) -> Result<&'a Map<String, Value>, String> {
    let object = value.as_object().ok_or_else(|| format!("{what} must be a JSON object"))?;
    let known = |key: &String| required.contains(&key.as_str()) || optional.contains(&key.as_str());
    if !required.iter().all(|key| object.contains_key(*key)) || !object.keys().all(known) {
        let mut keys: Vec<&str> = required.to_vec();
        keys.extend(optional.iter().copied());
        return Err(format!("{what} must be {{{}}}", keys.join(", ")));
    }
    Ok(object)
}

fn body(bytes: &[u8]) -> Result<Value, String> {
    if bytes.is_empty() {
        return Ok(Value::Object(Map::new()));
    }
    serde_json::from_slice(bytes).map_err(|_| "the body must be JSON".to_string())
}

pub fn parse_owner(value: Option<&Value>) -> Result<Owner, String> {
    let object = fields(value.unwrap_or(&Value::Null), &["sandbox_id", "generation"], &[], "owner")?;
    let sandbox_id = object["sandbox_id"].as_str().filter(|id| is_sandbox_id(id)).ok_or("owner.sandbox_id is invalid")?;
    let generation = object["generation"].as_u64().filter(|generation| *generation >= 1 && *generation <= i64::MAX as u64);
    let generation = generation.ok_or("owner.generation must be a positive integer")?;
    Ok((sandbox_id.to_string(), generation))
}

fn parse_shape(value: Option<&Value>) -> Result<Shape, String> {
    let object = fields(value.unwrap_or(&Value::Null), &["vcpu", "memory_mb"], &[], "requested")?;
    let vcpu = object["vcpu"].as_f64().filter(|vcpu| vcpu.is_finite() && *vcpu >= 0.0).ok_or("requested.vcpu must be a non-negative number")?;
    let memory_mb = object["memory_mb"].as_u64().ok_or("requested.memory_mb must be a non-negative integer")?;
    Ok(Shape { vcpu, memory_mb, disk_mb: 0 })
}

/// `null` is an unknown quantity, never zero.
fn quantity(object: &Map<String, Value>, key: &str) -> Result<Option<u64>, String> {
    match object.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => value.as_u64().map(Some).ok_or_else(|| format!("cost.{key} must be a non-negative integer or null")),
    }
}

fn parse_cost(value: Option<&Value>, kind: Kind) -> Result<Cost, String> {
    let optional = ["ram_backing_bytes", "read_bytes", "write_bytes", "provenance"];
    let object = fields(value.unwrap_or(&Value::Null), &["memory_bytes"], &optional, "cost")?;
    let provenance = match object.get("provenance") {
        None => "unknown".to_string(),
        Some(value) => value.as_str().filter(|text| !text.is_empty() && text.len() <= 128).ok_or("cost.provenance must be a short string")?.to_string(),
    };
    Ok(Cost {
        kind,
        memory_bytes: quantity(object, "memory_bytes")?,
        read_bytes: quantity(object, "read_bytes")?,
        write_bytes: quantity(object, "write_bytes")?,
        provenance,
        ram_backing_bytes: quantity(object, "ram_backing_bytes")?,
    })
}

/// `deadline_ms`: the wait budget, in milliseconds from receipt.
fn parse_budget(value: Option<&Value>) -> Result<f64, String> {
    value
        .and_then(Value::as_f64)
        .filter(|milliseconds| milliseconds.is_finite() && *milliseconds >= 0.0)
        .map(|milliseconds| milliseconds / 1000.0)
        .ok_or_else(|| "deadline_ms must be a non-negative number".to_string())
}

fn is_token(text: &str) -> bool {
    text.len() == 32 && text.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

/// hello's `reassert`: `[{owner, kind, cost, requested, agent_token}]`.
pub fn parse_reassert(value: Option<&Value>) -> Result<Vec<Reassert>, String> {
    let Some(value) = value else { return Ok(Vec::new()) };
    let items = value.as_array().ok_or("reassert must be a list")?;
    items
        .iter()
        .map(|item| {
            let object = fields(item, &["owner", "kind", "cost", "requested", "agent_token"], &[], "a reasserted claim")?;
            let kind = object["kind"].as_str().and_then(Kind::parse).ok_or("a reasserted claim's kind is invalid")?;
            let requested = match &object["requested"] {
                Value::Null => None,
                requested => Some(parse_shape(Some(requested))?),
            };
            let token = object["agent_token"].as_str().filter(|token| is_token(token)).ok_or("agent_token must be 32 lowercase hex digits")?;
            Ok(Reassert { owner: parse_owner(object.get("owner"))?, cost: parse_cost(object.get("cost"), kind)?, requested, token: token.to_string() })
        })
        .collect()
}

fn parse_drain_token(value: &Value) -> Result<(), String> {
    let object = fields(value, &["drain_token"], &[], "the body")?;
    match object["drain_token"].as_str() {
        Some(token) if is_operation_id(token) => Ok(()),
        _ => Err("drain_token is invalid".into()),
    }
}

fn parse_events(value: &Value) -> Result<Vec<Event>, String> {
    let object = fields(value, &["items"], &[], "the body")?;
    let items = object["items"].as_array().filter(|items| items.len() <= MAX_GROWTH_EVENTS);
    let items = items.ok_or_else(|| format!("items must be a list of at most {MAX_GROWTH_EVENTS}"))?;
    items
        .iter()
        .map(|item| {
            let object = fields(item, &["action", "owner"], &["request_id", "job_id"], "a growth event")?;
            let action = object["action"].as_str().and_then(EventAction::parse).ok_or("a growth event's action is invalid")?;
            let text = |key: &str| -> Result<String, String> {
                match object.get(key) {
                    None => Ok(String::new()),
                    Some(value) => match value.as_str() {
                        Some("") => Ok(String::new()),
                        Some(text) if is_operation_id(text) => Ok(text.to_string()),
                        _ => Err(format!("a growth event's {key} is invalid")),
                    },
                }
            };
            let (request_id, job_id) = (text("request_id")?, text("job_id")?);
            if action == EventAction::Wait && request_id.is_empty() {
                return Err("a wait needs its request_id".into());
            }
            if action == EventAction::Terminal && job_id.is_empty() {
                return Err("a terminal needs its job_id".into());
            }
            Ok(Event { action, owner: parse_owner(object.get("owner"))?, request_id, job_id })
        })
        .collect()
}

macro_rules! parsed {
    ($result:expr) => {
        match $result {
            Ok(value) => value,
            Err(message) => return invalid(&message),
        }
    };
}

fn granted(service: &Arc<AdmissionService>, holder: Holder, token: String, body: Value) -> Answer {
    let grant = Grant { service: service.clone(), holder, token, delivered: false };
    Answer { status: StatusCode::OK, body, grant: Some(grant) }
}

/// One admission route (`route` after `/internal/v1/admission/`, or after
/// `/internal/v1/growth/` when `growth`) for `agent_session`.
pub async fn handle(
    service: &Arc<AdmissionService>,
    noded_session: &str,
    agent_session: &str,
    growth: bool,
    route: &str,
    method: &Method,
    bytes: &[u8],
) -> Answer {
    let holder = Holder::Agent(agent_session.to_string());
    let known = if growth {
        matches!(route, "events" | "continuation")
    } else {
        matches!(route, "startup" | "restore" | "observe" | "publication" | "cancel" | "close" | "open" | "snapshot")
            || route.strip_suffix("/release").is_some_and(|token| !token.is_empty() && !token.contains('/'))
    };
    if !known {
        let prefix = if growth { "growth" } else { "admission" };
        return error(StatusCode::NOT_FOUND, "admission_route_unknown", &format!("no admission route /internal/v1/{prefix}/{route}"), false);
    }
    let expected = if !growth && route == "snapshot" { Method::GET } else { Method::POST };
    if method != expected {
        return error(StatusCode::METHOD_NOT_ALLOWED, "method_not_allowed", "method not allowed", false);
    }
    let value = parsed!(body(bytes));
    if growth {
        return match route {
            "events" => events(service, &value).await,
            _ => continuation(service, &holder, &value).await,
        };
    }
    match route {
        "startup" => startup(service, holder, &value).await,
        "restore" => restore(service, holder, &value).await,
        "observe" => {
            let object = parsed!(fields(&value, &["owner", "kind", "cost"], &[], "the body"));
            if object["kind"].as_str() != Some("capture") {
                return invalid("only a capture is observed");
            }
            let owner = parsed!(parse_owner(object.get("owner")));
            let cost = parsed!(parse_cost(object.get("cost"), Kind::Capture));
            match service.observe(&holder, &owner, cost) {
                Ok(token) => granted(service, holder, token.clone(), json!({"token": token})),
                Err(refusal) => refused(Context::Other, refusal),
            }
        }
        "publication" => {
            let object = parsed!(fields(&value, &["owner"], &[], "the body"));
            let owner = parsed!(parse_owner(object.get("owner")));
            match service.publication(&holder, &owner) {
                Ok(token) => granted(service, holder, token.clone(), json!({"token": token})),
                Err(refusal) => refused(Context::Other, refusal),
            }
        }
        "cancel" => {
            let object = parsed!(fields(&value, &["owner"], &[], "the body"));
            let owner = parsed!(parse_owner(object.get("owner")));
            answer(StatusCode::OK, json!({"cancelled": service.cancel(&owner)}))
        }
        "close" | "open" => {
            parsed!(parse_drain_token(&value));
            let (open, revision) = if route == "close" { service.close() } else { service.open_admission() };
            answer(StatusCode::OK, json!({"admission_open": open, "transient_revision": revision}))
        }
        "snapshot" => snapshot(service, noded_session),
        _ => {
            parsed!(fields(&value, &[], &[], "the body"));
            let token = route.strip_suffix("/release").unwrap_or_default();
            if service.release(token, &holder) {
                answer(StatusCode::OK, json!({}))
            } else {
                error(StatusCode::NOT_FOUND, "admission_token_unknown", "this session holds no such admission token", false)
            }
        }
    }
}

async fn startup(service: &Arc<AdmissionService>, holder: Holder, value: &Value) -> Answer {
    let object = parsed!(fields(value, &["owner", "requested", "managed"], &["wait_seconds"], "the body"));
    let owner = parsed!(parse_owner(object.get("owner")));
    let requested = parsed!(parse_shape(object.get("requested")));
    let Some(managed) = object["managed"].as_bool() else { return invalid("managed must be a boolean") };
    let wait = match object.get("wait_seconds") {
        None | Some(Value::Null) => None,
        Some(value) => Some(parsed!(value.as_f64().filter(|wait| wait.is_finite() && *wait >= 0.0).ok_or("wait_seconds must be a non-negative number or null"))),
    };
    match service.startup(&holder, &owner, requested, managed, wait).await {
        Ok(grant) => {
            let body = json!({
                "token": grant.token,
                "phases": {"startup_admission_ms": grant.startup_admission_ms, "active_capacity_ms": grant.active_capacity_ms},
            });
            granted(service, holder, grant.token, body)
        }
        Err(refusal) => refused(Context::Startup, refusal),
    }
}

async fn restore(service: &Arc<AdmissionService>, holder: Holder, value: &Value) -> Answer {
    let object = parsed!(fields(value, &["owner", "requested", "cost", "deadline_ms"], &[], "the body"));
    let owner = parsed!(parse_owner(object.get("owner")));
    let requested = parsed!(parse_shape(object.get("requested")));
    let cost = parsed!(parse_cost(object.get("cost"), Kind::Restore));
    let budget = parsed!(parse_budget(object.get("deadline_ms")));
    match service.restore(&holder, &owner, requested, cost, budget).await {
        Ok(token) => granted(service, holder, token.clone(), json!({"token": token})),
        Err(refusal) => refused(Context::Restore, refusal),
    }
}

fn snapshot(service: &Arc<AdmissionService>, noded_session: &str) -> Answer {
    let snapshot = match service.snapshot() {
        Ok(snapshot) => snapshot,
        Err(refusal) => return refused(Context::Other, refusal),
    };
    let reservations: Vec<Value> = snapshot
        .resource_reservations
        .iter()
        .map(|(owner, shape)| json!({"owner": owner_json(owner), "vcpu": shape.vcpu, "memory_mb": shape.memory_mb}))
        .collect();
    let transitions: Map<String, Value> = snapshot
        .transitions
        .iter()
        .map(|(kind, row)| (kind.as_str().to_string(), serde_json::to_value(row).expect("snapshot rows encode")))
        .collect();
    answer(StatusCode::OK, json!({
        "noded_session": noded_session,
        "admission_open": snapshot.admission_open,
        "transient_revision": snapshot.transient_revision,
        "active_reservations": reservations.len(),
        "resource_reservations": reservations,
        "transitions": transitions,
        "resident_demand": serde_json::to_value(&snapshot.resident_demand).expect("demand encodes"),
    }))
}

async fn events(service: &Arc<AdmissionService>, value: &Value) -> Answer {
    let items = parsed!(parse_events(value));
    let results = match service.growth_events(items).await {
        Ok(results) => results,
        Err(refusal) => return refused(Context::Other, refusal),
    };
    let results: Vec<Value> = results
        .into_iter()
        .map(|result| match result {
            Ok(intent) => json!({"intent": intent.as_ref().map(intent_json)}),
            Err(refusal) => json!({"error": refusal.message(), "error_code": event_code(&refusal)}),
        })
        .collect();
    answer(StatusCode::OK, json!({"results": results}))
}

async fn continuation(service: &Arc<AdmissionService>, holder: &Holder, value: &Value) -> Answer {
    let object = parsed!(fields(value, &["owner", "request_id", "deadline_ms"], &[], "the body"));
    let owner = parsed!(parse_owner(object.get("owner")));
    let Some(request_id) = object["request_id"].as_str().filter(|id| is_operation_id(id)) else {
        return invalid("request_id is invalid");
    };
    let budget = parsed!(parse_budget(object.get("deadline_ms")));
    match service.continuation(holder, &owner, request_id, budget).await {
        Ok(intent) => answer(StatusCode::OK, json!({"intent": intent.as_ref().map(intent_json)})),
        Err(refusal) => refused(Context::Continuation, refusal),
    }
}
