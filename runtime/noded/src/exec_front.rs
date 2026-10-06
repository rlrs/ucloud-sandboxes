//! Exec routes in the daemon (phase 2a, exec spec §6.1). The daemon starts
//! execs on owned, running, unpaused sandboxes and serves the sessions it
//! started. Everything else goes to the agent unchanged: paused, parked or
//! transitioning sandboxes, `tty`, drain, memory pressure, unknown sessions,
//! and any request it cannot parse exactly.
//!
//! The lifecycle fence is shared with the agent through the kernel
//! ([`crate::exec_fence`]); there is no per-exec RPC.

use std::path::PathBuf;
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::header::{self, HeaderValue};
use hyper::{Method, Request, Response, StatusCode};
use serde_json::Value;

use crate::Body;
use crate::exec::{ExecError, ExecLimits, ExecManager, ExecRequest, Reply, StartTimings, runsc_exec_argv};
use crate::exec_fence::{ActivityLease, ExecFence, Fenced};
use crate::node_pipeline::NodePipeline;
use crate::registry::Phase;
use crate::warden::Sandbox;

const MAX_BODY_BYTES: usize = 16 * 1024 * 1024;
const SESSION_PREFIX_HEADER: &str = "x-ucloud-exec-session-prefix";

pub enum Outcome {
    Response(Response<Body>),
    Forward(Request<Body>),
}

pub struct ExecFront {
    node: Arc<OnceLock<Arc<NodePipeline>>>,
    manager: Arc<ExecManager>,
    token: Bytes,
}

fn full(bytes: impl Into<Bytes>) -> Body {
    Full::new(bytes.into()).map_err(|never| match never {}).boxed()
}

fn respond(reply: Reply) -> Response<Body> {
    let mut response = Response::new(full(reply.body_bytes()));
    *response.status_mut() = StatusCode::from_u16(reply.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let headers = response.headers_mut();
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
    for (name, value) in reply.headers {
        if let (Ok(name), Ok(value)) = (header::HeaderName::from_bytes(name.as_bytes()), HeaderValue::from_str(value)) {
            headers.insert(name, value);
        }
    }
    response
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    left.len() == right.len() && left.iter().zip(right).fold(0u8, |acc, (a, b)| acc | (a ^ b)) == 0
}

/// `/v1/exec/{sid}` and its sub-routes: (sid, sub-route).
fn session_route(path: &str) -> Option<(&str, &str)> {
    let rest = path.strip_prefix("/v1/exec/")?;
    let (sid, sub) = rest.split_once('/').unwrap_or((rest, ""));
    (!sid.is_empty()).then_some((sid, sub))
}

/// `MemAvailable` in MiB.
fn available_mib() -> Option<u64> {
    let text = std::fs::read_to_string("/proc/meminfo").ok()?;
    let line = text.lines().find(|line| line.starts_with("MemAvailable:"))?;
    let kib: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kib / 1024)
}

/// Why the daemon does not start this exec itself.
enum Decline {
    /// The agent answers it (exactly as it always has).
    Forward(&'static str),
    Reply(Reply),
}

/// Counts forwards per reason; logs the first of each and every 1,000th.
fn forwarded(reason: &'static str) {
    static COUNTS: std::sync::Mutex<Vec<(&'static str, u64)>> = std::sync::Mutex::new(Vec::new());
    let mut counts = COUNTS.lock().expect("not poisoned");
    let count = match counts.iter_mut().find(|(name, _)| *name == reason) {
        Some((_, count)) => {
            *count += 1;
            *count
        }
        None => {
            counts.push((reason, 1));
            1
        }
    };
    if count == 1 || count % 1000 == 0 {
        eprintln!("ucloud-noded: exec forwarded to the agent ({reason}): {count}");
    }
}

impl ExecFront {
    pub fn new(node: Arc<OnceLock<Arc<NodePipeline>>>, token: &str) -> Arc<Self> {
        let hook: crate::exec::ActivityHook = Arc::new(|_, guard| {
            if let Some(lease) = guard.downcast_ref::<ActivityLease>() {
                lease.touch();
            }
        });
        Arc::new(ExecFront {
            node,
            manager: ExecManager::new(ExecLimits::default(), Some(hook)),
            token: Bytes::copy_from_slice(token.as_bytes()),
        })
    }

    fn authorized<B>(&self, request: &Request<B>) -> bool {
        let supplied = request.headers().get(header::AUTHORIZATION).map(|v| v.as_bytes()).and_then(|v| v.strip_prefix(b"Bearer "));
        supplied.is_some_and(|supplied| !supplied.is_empty() && constant_time_eq(supplied, &self.token))
    }

    /// The node, when it serves execs: the agent enabled them and the session
    /// limits are the ones this build implements.
    fn node(&self) -> Option<&Arc<NodePipeline>> {
        let node = self.node.get()?;
        let exec = node.config().exec.as_ref()?;
        let limits = ExecLimits::default();
        let same = exec.sessions.max_sessions == limits.max_sessions
            && exec.sessions.max_events_per_session == limits.max_events_per_session;
        (exec.rust_execs_enabled && same).then_some(node)
    }

    pub fn intercepts<B>(&self, request: &Request<B>) -> bool {
        let path = request.uri().path();
        let start = request.method() == Method::POST && path.starts_with("/v1/sandboxes/") && path.ends_with("/exec");
        let session = session_route(path).is_some_and(|(sid, _)| self.manager.owns(sid));
        if !(start || session) {
            return false;
        }
        if !self.authorized(request) {
            forwarded("not authorized");
            return false;
        }
        if request.headers().contains_key(header::TRANSFER_ENCODING) {
            forwarded("transfer-encoding");
            return false;
        }
        if start && self.node().is_none() {
            forwarded("node not configured for execs");
            return false;
        }
        true
    }

    pub async fn handle(&self, request: Request<Body>) -> Outcome {
        let path = request.uri().path().to_string();
        let query = request.uri().query().unwrap_or("").to_string();
        let method = request.method().clone();
        if let Some((sid, sub)) = session_route(&path) {
            let sid = sid.to_string();
            let reply = match (method, sub) {
                (Method::GET, "") => self.manager.get(&sid),
                (Method::GET, "events") => self.manager.events(&sid, &query).await,
                (Method::POST, "close-stdin") => self.manager.close_stdin(&sid).await,
                (Method::POST, "stdin" | "signal") => {
                    let (parts, body) = request.into_parts();
                    let bytes = match Limited::new(body, MAX_BODY_BYTES).collect().await {
                        Ok(collected) => collected.to_bytes(),
                        Err(_) => return Outcome::Forward(Request::from_parts(parts, full(Bytes::new()))),
                    };
                    if sub == "stdin" { self.manager.stdin(&sid, &bytes).await } else { self.manager.signal(&sid, &bytes).await }
                }
                _ => return Outcome::Forward(request),
            };
            return Outcome::Response(respond(reply));
        }
        self.start(request, &path, &query).await
    }

    async fn start(&self, request: Request<Body>, path: &str, query: &str) -> Outcome {
        let timings = StartTimings::now();
        let (parts, body) = request.into_parts();
        let bytes = match Limited::new(body, MAX_BODY_BYTES).collect().await {
            Ok(collected) => collected.to_bytes(),
            Err(_) => {
                let reply = Reply::new(400, serde_json::json!({"error": "request body ended before Content-Length bytes were read"}));
                return Outcome::Response(respond(reply));
            }
        };
        let forward = |bytes: Bytes| Outcome::Forward(Request::from_parts(parts.clone(), full(bytes)));
        // Sandbox ids are plain; an escaped or nested path is the agent's.
        let sandbox_id = path.trim_start_matches("/v1/sandboxes/").trim_end_matches("/exec").to_string();
        if sandbox_id.contains(['%', '/']) {
            return forward(bytes);
        }
        let prefix = parts.headers.get(SESSION_PREFIX_HEADER).and_then(|value| value.to_str().ok());
        // Python's own answer for anything malformed, tty and post-fence checks.
        let exec = match ExecRequest::parse(&sandbox_id, &bytes, query, prefix) {
            Ok(exec) => exec,
            Err(_) => {
                forwarded("request");
                return forward(bytes);
            }
        };
        if exec.tty || exec.check_direct().is_err() {
            forwarded("tty or post-fence checks");
            return forward(bytes);
        }
        let Some(node) = self.node().cloned() else {
            forwarded("node not configured");
            return forward(bytes);
        };
        match self.launch(&node, exec, timings).await {
            Ok(reply) => Outcome::Response(respond(reply)),
            Err(Decline::Reply(reply)) => Outcome::Response(respond(reply)),
            Err(Decline::Forward(reason)) => {
                forwarded(reason);
                forward(bytes)
            }
        }
    }

    async fn launch(&self, node: &Arc<NodePipeline>, exec: ExecRequest, mut timings: StartTimings) -> Result<Reply, Decline> {
        let config = node.config().exec.clone().ok_or(Decline::Forward("no exec configuration"))?;
        let registry = node.registry().clone();
        let id = exec.sandbox_id.clone();
        let (registration, drain) = tokio::task::spawn_blocking(move || (registry.get(&id), registry.load_drain()))
            .await
            .map_err(|_| Decline::Forward("registry worker"))?;
        let registration = registration
            .ok()
            .flatten()
            .filter(|r| r.phase == Phase::Owned)
            .ok_or(Decline::Forward("not an owned registration"))?;
        // A drain committed before its answer closes admission for execs too.
        if !drain.map(|drain| drain.admission_open).unwrap_or(false) {
            return Err(Decline::Forward("admission closed or unreadable"));
        }
        let fence = ExecFence::new(&config.warden_locks_dir);
        let lease = match fence.acquire(&exec.sandbox_id) {
            Ok(Fenced::Held(lease)) => lease,
            Ok(Fenced::Busy) => return Err(Decline::Forward("lifecycle fence busy")),
            Err(_) => return Err(Decline::Forward("lifecycle fence unreadable")),
        };
        // With A held no pause or park can start; check that the runtime runs.
        let sandbox = Sandbox {
            sandbox_id: exec.sandbox_id.clone(),
            generation: registration.sandbox_generation as u64,
            container_id: registration.container_id.clone(),
            bundle: PathBuf::from(&registration.bundle),
            memory_directory: registration.memory_directory.clone(),
            spec_sha256: registration.spec_sha256(),
        };
        let marker = config.warden_paused_dir.join(format!("{}.sandbox-{}", sandbox.sandbox_id, sandbox.generation));
        // A paused sandbox is the agent's unless the daemon owns the pause tier.
        let tier = node.pause_tier().cloned();
        if marker.exists() && tier.is_none() {
            return Err(Decline::Forward("paused"));
        }
        if !running(node, &sandbox) {
            return Err(Decline::Forward("not running"));
        }
        if config.active_capacity_configured && available_mib().is_none_or(|mib| mib < config.memory_floor_mib) {
            return Err(Decline::Forward("memory floor"));
        }
        // Python's start fence: the warden flock across the spawn only.
        let lock = node.warden().lock(&sandbox).await.map_err(|_| Decline::Forward("warden lock"))?;
        if marker.exists() {
            // Thaw-on-exec (Python's exec_lease: `_thaw_locked(prefetch=True)`
            // under the warden flock). The exec's A lock already keeps a new
            // pause out until the session ends.
            let Some(tier) = tier else { return Err(Decline::Forward("paused")) };
            match tier.thaw_locked(&sandbox, &lock, true).await {
                Ok(thawed) => {
                    let ms = thawed.map_or(0.0, |elapsed| elapsed.as_secs_f64() * 1000.0);
                    timings.manager.insert("thaw".into(), serde_json::json!(ms));
                }
                Err(_) => return Err(Decline::Forward("thaw failed")),
            }
        }
        let argv = runsc_exec_argv(
            &config.runsc.to_string_lossy(),
            &config.runtime_root.to_string_lossy(),
            &sandbox.container_id,
            &exec,
        );
        timings.manager.insert("daemon".into(), Value::Bool(true));
        match self.manager.start(exec, argv, Box::new(lease), Some(Box::new(lock)), timings).await {
            Ok(reply) => Ok(reply),
            Err(ExecError::Forward(_)) => Err(Decline::Forward("exec manager")),
            Err(error) => Err(Decline::Reply(error.reply())),
        }
    }
}

/// The journal says RUNNING with live authority and its sentry is the
/// process it names (`running_snapshot`: pid and start ticks).
fn running(node: &NodePipeline, sandbox: &Sandbox) -> bool {
    let Ok(Some(journal)) = node.warden().journal().load(&sandbox.sandbox_id, sandbox.generation) else { return false };
    let field = |name: &str| journal.get(name);
    if field("state").and_then(Value::as_str) != Some("running") || field("authority").and_then(Value::as_str) != Some("live") {
        return false;
    }
    let (Some(pid), Some(ticks)) = (
        field("sentry_pid").and_then(Value::as_u64),
        field("sentry_start_time_ticks").and_then(Value::as_u64),
    ) else {
        return false;
    };
    crate::runsc::start_time_ticks(std::path::Path::new("/proc"), pid as u32).is_ok_and(|now| now == ticks)
}

