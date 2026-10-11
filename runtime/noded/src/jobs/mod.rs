//! Managed-process job routes in the daemon (docs/managed-process-reads.md):
//! status and logs read from the supervisor's files through the state mount,
//! with held waits; start and signal dispatch the supervisor's `ctl` under the
//! exec fences. Growth admission (launch, bind, terminal) goes through an
//! [`AdmissionPort`]: the agent's `/internal/v1/managed/{launch,bind,terminal}`,
//! or noded's own admission with `--rust-admission` (step 3c.2).
//!
//! The daemon answers for owned sandboxes that are running (paused included)
//! and carry the state mount. Everything else goes to the agent unchanged:
//! legacy sandboxes (no mount), parked or transitioning ones, and requests it
//! cannot read exactly as Python does.
//!
//! Reads never enter, thaw or lock the guest and do not count as activity.

pub mod files;
pub mod start;
pub mod watch;

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::io;
use std::path::PathBuf;
use std::pin::Pin;
use std::process::Stdio;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::header::{self, HeaderValue};
use hyper::{Method, Request, Response, StatusCode};
use serde_json::{Map, Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;

use crate::Body;
use crate::admission::port::{AdmissionPort, AgentAdmission, BindError, LaunchError};
use crate::agent_rpc::AgentClient;
pub use crate::exec_front::Outcome;
use crate::exec_fence::{ExecFence, Fenced};
use crate::node_pipeline::NodePipeline;
use crate::registry::Phase;
use crate::warden::Sandbox;
use files::{ControlAnswerError, JobRecord, ReadError, StateDir};
use start::ManagedStart;
use watch::{Change, WatchError, Waiter, Watcher};

pub type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

const MAX_BODY_BYTES: usize = 16 * 1024 * 1024;
const MAX_CONFIG_BYTES: u64 = 4 * 1024 * 1024;
const MAX_CONTROL_STDOUT: usize = 2 * 1024 * 1024;
const MAX_CONTROL_STDERR: usize = 64 * 1024;
const MOUNT_CACHE_ENTRIES: usize = 16 * 1024;
const TERMINAL_SEEN_ENTRIES: usize = 64 * 1024;

/// An owned, running incarnation of a managed sandbox.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Incarnation {
    pub sandbox_id: String,
    pub generation: u64,
    pub bundle: PathBuf,
    pub container_id: String,
}

pub enum Lookup {
    Serve(Incarnation),
    /// The agent answers, as it always has.
    Forward(&'static str),
}

/// One `/.ucloud-job-init ctl` exchange's process result.
#[derive(Clone, Debug, Default)]
pub struct ControlOutput {
    pub exit_code: Option<i32>,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

#[derive(Debug)]
pub enum ControlFailure {
    /// Nothing was dispatched: the agent answers instead.
    Forward(&'static str),
    /// Dispatched, and no answer within the deadline: ambiguous.
    TimedOut,
    Failed(String),
}

/// What the routes need from the node; the daemon's is [`NodeJobs`].
pub trait JobNode: Send + Sync + 'static {
    /// Blocking (registry, journal, /proc): called off the reactor.
    fn lookup(&self, sandbox_id: &str) -> Lookup;
    /// Whether starts and signals can run here: the exec fence is shared
    /// with the agent (`--rust-execs`).
    fn controls(&self) -> bool;
    /// One `ctl` exchange with `request` on stdin, under the exec fences,
    /// thawing a paused sandbox (with prefetch) and leaving it running.
    /// Marks activity when the helper exits 0.
    fn control(&self, incarnation: &Incarnation, request: Vec<u8>, guest_timeout_ms: u64, timeout: Duration)
    -> BoxFuture<Result<ControlOutput, ControlFailure>>;
}

#[derive(Clone, Debug)]
pub struct JobLimits {
    /// Held reads at once; past it a waiting read is a retryable 503.
    pub max_waiters: usize,
    pub max_wait: Duration,
    /// Python's `_MANAGED_CONTROL_DEADLINE_SECONDS`.
    pub control_deadline: Duration,
}

impl Default for JobLimits {
    fn default() -> Self {
        JobLimits { max_waiters: 16 * 1024, max_wait: Duration::from_millis(25_000), control_deadline: Duration::from_secs(15) }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Route {
    Status { sandbox_id: String, job_id: String },
    Logs { sandbox_id: String, job_id: String, stream: &'static str },
    Start { sandbox_id: String },
    Signal { sandbox_id: String, job_id: String },
}

impl Route {
    fn sandbox_id(&self) -> &str {
        match self {
            Route::Status { sandbox_id, .. }
            | Route::Logs { sandbox_id, .. }
            | Route::Start { sandbox_id }
            | Route::Signal { sandbox_id, .. } => sandbox_id,
        }
    }
}

/// The job routes with plain ids (an escaped or odd path is the agent's).
fn route(method: &Method, path: &str) -> Option<Route> {
    let rest = path.strip_prefix("/v1/sandboxes/")?;
    let parts: Vec<&str> = rest.split('/').collect();
    let plain = |id: &str| files::is_job_id(id);
    let route = match (method, parts.as_slice()) {
        (&Method::GET, [sandbox, "jobs", job]) => Route::Status { sandbox_id: sandbox.to_string(), job_id: job.to_string() },
        (&Method::GET, [sandbox, "jobs", job, "logs", stream @ ("stdout" | "stderr")]) => Route::Logs {
            sandbox_id: sandbox.to_string(),
            job_id: job.to_string(),
            stream: if *stream == "stdout" { "stdout" } else { "stderr" },
        },
        (&Method::POST, [sandbox, "jobs"]) => Route::Start { sandbox_id: sandbox.to_string() },
        (&Method::POST, [sandbox, "jobs", job, "signal"]) => Route::Signal { sandbox_id: sandbox.to_string(), job_id: job.to_string() },
        _ => return None,
    };
    let job_ok = match &route {
        Route::Status { job_id, .. } | Route::Logs { job_id, .. } | Route::Signal { job_id, .. } => plain(job_id),
        Route::Start { .. } => true,
    };
    (plain(route.sandbox_id()) && job_ok).then_some(route)
}

/// `parse_qs(query, keep_blank_values=True)[key][0]`, undecoded.
fn query_value<'a>(query: &'a str, key: &str) -> Option<&'a str> {
    query.split('&').filter_map(|field| field.split_once('=')).find(|(name, _)| *name == key).map(|(_, value)| value)
}

/// Python `int(value)` for what this port reads exactly; anything else is
/// the agent's (its own `ValueError` message).
fn query_int(value: &str) -> Option<i64> {
    if value.contains(['%', '+']) || value.bytes().filter(u8::is_ascii_digit).count() > 18 {
        return None;
    }
    crate::exec::py_int(value)
}

fn full(bytes: impl Into<Bytes>) -> Body {
    Full::new(bytes.into()).map_err(|never| match never {}).boxed()
}

fn reply(status: u16, body: Value) -> Response<Body> {
    let mut response = Response::new(full(crate::exec::json::dumps(&body)));
    *response.status_mut() = StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    response.headers_mut().insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
    response
}

fn retryable(status: u16, body: Value) -> Response<Body> {
    let mut response = reply(status, body);
    response.headers_mut().insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
    response.headers_mut().insert("x-ucloud-sandbox-retryable", HeaderValue::from_static("true"));
    response
}

fn error(status: u16, message: &str) -> Response<Body> {
    reply(status, json!({"error": message}))
}

/// `ManagedProcessReadUnavailable` (`_write_exception`).
fn read_unavailable(message: &str) -> Response<Body> {
    retryable(503, json!({"error": message, "error_code": "managed_process_read_unavailable", "retryable": true}))
}

/// `SandboxStartupBusyError` (`_write_exception`).
fn startup_busy(message: &str) -> Response<Body> {
    retryable(503, json!({"error": message, "error_code": "node_startup_busy", "retryable": true}))
}

/// `ManagedPrimaryOwnedError` (`_start_managed_process`).
fn primary_owned(message: &str, job_id: &str) -> Response<Body> {
    reply(409, json!({"error": message, "error_code": "primary_already_owned", "job_id": job_id}))
}

fn read_error(error: ReadError) -> Response<Body> {
    match error {
        ReadError::Invalid(message) => {
            reply(409, json!({"error": message, "error_code": "managed_process_state_invalid", "retryable": false}))
        }
        other => read_unavailable(&other.to_string()),
    }
}

/// Counts forwards per reason; logs the first of each and every 1,000th.
fn forwarded(reason: &'static str) {
    static COUNTS: Mutex<Vec<(&'static str, u64)>> = Mutex::new(Vec::new());
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
        eprintln!("ucloud-noded: job request forwarded to the agent ({reason}): {count}");
    }
}

/// Whether the incarnation's bundle carries the state mount, cached per
/// incarnation (a config never gains or loses it in place).
struct MountCache {
    known: Mutex<HashMap<(String, u64, PathBuf), bool>>,
}

impl MountCache {
    fn has_mount(&self, incarnation: &Incarnation) -> bool {
        let key = (incarnation.sandbox_id.clone(), incarnation.generation, incarnation.bundle.clone());
        if let Some(known) = self.known.lock().expect("not poisoned").get(&key) {
            return *known;
        }
        let Ok(Some(config)) = read_config(&incarnation.bundle) else { return false };
        let mounted = crate::oci::has_managed_state_mount(&config, &incarnation.bundle);
        let mut known = self.known.lock().expect("not poisoned");
        if known.len() >= MOUNT_CACHE_ENTRIES {
            known.clear();
        }
        known.insert(key, mounted);
        mounted
    }
}

fn read_config(bundle: &std::path::Path) -> io::Result<Option<Value>> {
    let Some(bytes) = crate::fsutil::read_owned_file(&bundle.join("config.json"), MAX_CONFIG_BYTES)? else { return Ok(None) };
    Ok(serde_json::from_slice(&bytes).ok())
}

/// The growth bookkeeping for terminal records: once per (sandbox,
/// generation, job), sent in order off the request path.
struct TerminalQueue {
    seen: Mutex<HashSet<(String, u64, String)>>,
    sender: mpsc::UnboundedSender<(String, u64, String)>,
}

impl TerminalQueue {
    fn start(admission: Arc<dyn AdmissionPort>) -> TerminalQueue {
        let (sender, mut receiver) = mpsc::unbounded_channel::<(String, u64, String)>();
        tokio::spawn(async move {
            let mut failures = 0u64;
            while let Some((sandbox_id, generation, job_id)) = receiver.recv().await {
                if let Err(problem) = admission.terminal(&sandbox_id, generation, &job_id).await {
                    failures += 1;
                    if failures == 1 || failures % 1000 == 0 {
                        eprintln!("ucloud-noded: terminal job observation not recorded ({failures}): {problem}");
                    }
                }
            }
        });
        TerminalQueue { seen: Mutex::new(HashSet::new()), sender }
    }

    /// Python's `_observe_managed_terminal`, once per job. A repeat after the
    /// set is cleared is harmless: the agent's transition is idempotent.
    fn observe(&self, record: &JobRecord) {
        if !record.terminal() {
            return;
        }
        let key = (record.sandbox_id.clone(), record.sandbox_generation, record.state.job_id.clone());
        {
            let mut seen = self.seen.lock().expect("not poisoned");
            if seen.contains(&key) {
                return;
            }
            if seen.len() >= TERMINAL_SEEN_ENTRIES {
                seen.clear();
            }
            seen.insert(key.clone());
        }
        let _ = self.sender.send(key);
    }
}

/// What one read pass found.
struct Pass {
    record: JobRecord,
    /// Logs: (offset, data, retained size, whether the log exists).
    chunk: Option<(u64, Vec<u8>, u64, bool)>,
}

enum PassError {
    Read(ReadError),
    /// Another job owns this generation (the supervisor's answer).
    NotFound,
    OffsetBeyond,
}

#[derive(Clone)]
enum ReadKind {
    Status { after: Option<String> },
    Logs { stream: &'static str, offset: u64, limit: u64 },
}

/// One pass: open the state directory, register the waiter first when asked
/// (so no change after the read is missed), then read.
fn read_pass(
    incarnation: &Incarnation,
    job_id: &str,
    kind: &ReadKind,
    watcher: Option<&Watcher>,
) -> (Result<Pass, PassError>, Option<Result<Waiter, WatchError>>) {
    let dir = match StateDir::open(&incarnation.bundle) {
        Ok(dir) => dir,
        Err(error) => return (Err(PassError::Read(error)), None),
    };
    let waiter = watcher.map(|watcher| watcher.watch(dir.fd()));
    let pass = (|| {
        let record = dir.record(&incarnation.sandbox_id, incarnation.generation).map_err(PassError::Read)?;
        if record.state.job_id != job_id {
            return Err(PassError::NotFound);
        }
        let ReadKind::Logs { stream, offset, limit } = kind else { return Ok(Pass { record, chunk: None }) };
        let chunk = match dir.log(stream).map_err(PassError::Read)? {
            // The supervisor's answer for a log it never opened.
            None => (*offset, Vec::new(), *offset, false),
            Some(log) if *offset > log.size => return Err(PassError::OffsetBeyond),
            Some(log) => (*offset, log.read(*offset, *limit).map_err(|error| PassError::Read(ReadError::Io(error)))?, log.size, true),
        };
        Ok(Pass { record, chunk: Some(chunk) })
    })();
    (pass, waiter)
}

fn pass_reply(pass: &Pass, stream: Option<&str>) -> Response<Body> {
    let Some((offset, data, size, exists)) = &pass.chunk else { return reply(200, json!({"job": pass.record.to_json()})) };
    let next = offset + data.len() as u64;
    let mut body = Map::new();
    body.insert("stream".into(), json!(stream.unwrap_or("stdout")));
    body.insert("offset".into(), json!(offset));
    body.insert("next_offset".into(), json!(next));
    body.insert("data".into(), json!(files::base64(data)));
    // The node's definition, which the gateway's cached chunks share.
    body.insert("eof".into(), json!(!exists || next >= *size));
    body.insert("job".into(), pass.record.to_json());
    reply(200, Value::Object(body))
}

pub struct JobFront {
    node: Arc<dyn JobNode>,
    admission: Arc<dyn AdmissionPort>,
    terminal: TerminalQueue,
    mounts: Arc<MountCache>,
    watcher: OnceLock<Option<Arc<Watcher>>>,
    token: Bytes,
    limits: JobLimits,
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    left.len() == right.len() && left.iter().zip(right).fold(0u8, |acc, (a, b)| acc | (a ^ b)) == 0
}

/// Run blocking work off the reactor.
async fn blocking<T: Send + 'static>(work: impl FnOnce() -> T + Send + 'static) -> Option<T> {
    tokio::task::spawn_blocking(work).await.ok()
}

impl JobFront {
    /// Inside the tokio runtime (the terminal queue's sender task). Growth
    /// admission is the agent's.
    pub fn new(node: Arc<dyn JobNode>, agent: Arc<AgentClient>, token: &str, limits: JobLimits) -> Arc<JobFront> {
        Self::with_admission(node, Arc::new(AgentAdmission::new(agent)), token, limits)
    }

    /// With growth admission through `admission` (the daemon's
    /// [`crate::admission::port::NodeAdmission`]).
    pub fn with_admission(node: Arc<dyn JobNode>, admission: Arc<dyn AdmissionPort>, token: &str, limits: JobLimits) -> Arc<JobFront> {
        Arc::new(JobFront {
            node,
            terminal: TerminalQueue::start(admission.clone()),
            admission,
            mounts: Arc::new(MountCache { known: Mutex::new(HashMap::new()) }),
            watcher: OnceLock::new(),
            token: Bytes::copy_from_slice(token.as_bytes()),
            limits,
        })
    }

    /// Held reads now (for tests and status).
    pub fn waiters(&self) -> usize {
        self.watcher.get().and_then(Option::as_ref).map_or(0, |watcher| watcher.waiters())
    }

    fn watcher(&self) -> Option<Arc<Watcher>> {
        self.watcher
            .get_or_init(|| match Watcher::new(self.limits.max_waiters) {
                Ok(watcher) => Some(Arc::new(watcher)),
                Err(error) => {
                    eprintln!("ucloud-noded: job reads answer without waiting (no inotify: {error})");
                    None
                }
            })
            .clone()
    }

    fn authorized<B>(&self, request: &Request<B>) -> bool {
        let supplied = request.headers().get(header::AUTHORIZATION).map(|v| v.as_bytes()).and_then(|v| v.strip_prefix(b"Bearer "));
        supplied.is_some_and(|supplied| !supplied.is_empty() && constant_time_eq(supplied, &self.token))
    }

    pub fn intercepts<B>(&self, request: &Request<B>) -> bool {
        if route(request.method(), request.uri().path()).is_none() {
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
        true
    }

    pub async fn handle(&self, request: Request<Body>) -> Outcome {
        let Some(route) = route(request.method(), request.uri().path()) else { return Outcome::Forward(request) };
        let controls = matches!(route, Route::Start { .. } | Route::Signal { .. });
        if controls && !self.node.controls() {
            forwarded("node does not run managed starts");
            return Outcome::Forward(request);
        }
        let (node, mounts, sandbox_id) = (self.node.clone(), self.mounts.clone(), route.sandbox_id().to_string());
        let lookup = blocking(move || match node.lookup(&sandbox_id) {
            Lookup::Serve(incarnation) if !mounts.has_mount(&incarnation) => Lookup::Forward("legacy (no state mount)"),
            other => other,
        })
        .await
        .unwrap_or(Lookup::Forward("lookup worker"));
        let incarnation = match lookup {
            Lookup::Serve(incarnation) => incarnation,
            Lookup::Forward(reason) => {
                forwarded(reason);
                return Outcome::Forward(request);
            }
        };
        let query = request.uri().query().unwrap_or("").to_string();
        match route {
            Route::Status { job_id, .. } => self.status(request, &incarnation, &job_id, &query).await,
            Route::Logs { job_id, stream, .. } => self.logs(request, &incarnation, &job_id, stream, &query).await,
            Route::Start { .. } => self.start(request, &incarnation).await,
            Route::Signal { job_id, .. } => self.signal(request, &incarnation, &job_id).await,
        }
    }

    /// `wait_ms`, clamped to 0..=max_wait; `Err` is the 400 for a malformed one.
    fn wait(&self, query: &str) -> Result<Option<Duration>, Response<Body>> {
        let Some(raw) = query_value(query, "wait_ms") else { return Ok(None) };
        let Some(ms) = query_int(raw) else { return Err(error(400, "wait_ms must be an integer")) };
        Ok(Some(Duration::from_millis(ms.max(0) as u64).min(self.limits.max_wait)))
    }

    async fn status(&self, request: Request<Body>, incarnation: &Incarnation, job_id: &str, query: &str) -> Outcome {
        let wait = match self.wait(query) {
            Ok(wait) => wait,
            Err(response) => return Outcome::Response(response),
        };
        let after = query_value(query, "after").map(str::to_string);
        self.read(request, incarnation, job_id, ReadKind::Status { after }, wait).await
    }

    async fn logs(&self, request: Request<Body>, incarnation: &Incarnation, job_id: &str, stream: &'static str, query: &str) -> Outcome {
        // Python's defaults and checks, in its order.
        let parse = |key: &str, default: i64| match query_value(query, key) {
            None => Some(default),
            Some(raw) => query_int(raw),
        };
        let (Some(offset), Some(limit)) = (parse("offset", 0), parse("limit", files::MAX_LOG_READ_BYTES as i64)) else {
            forwarded("log query Python reads differently");
            return Outcome::Forward(request);
        };
        if offset < 0 {
            return Outcome::Response(error(400, "managed process log offset cannot be negative"));
        }
        if !(1..=files::MAX_LOG_READ_BYTES as i64).contains(&limit) {
            return Outcome::Response(error(400, &format!("managed process log limit must be in [1, {}]", files::MAX_LOG_READ_BYTES)));
        }
        let wait = match self.wait(query) {
            Ok(wait) => wait,
            Err(response) => return Outcome::Response(response),
        };
        let kind = ReadKind::Logs { stream, offset: offset as u64, limit: limit as u64 };
        self.read(request, incarnation, job_id, kind, wait).await
    }

    /// Answer at once without `wait`; with it, when the record's cursor
    /// differs from `after` (status), the stream has bytes past the offset
    /// (logs), the job is terminal, or the wait ends (the current answer).
    async fn read(&self, request: Request<Body>, incarnation: &Incarnation, job_id: &str, kind: ReadKind, wait: Option<Duration>) -> Outcome {
        let deadline = wait.map(|wait| tokio::time::Instant::now() + wait);
        let watcher = deadline.and_then(|_| self.watcher());
        let stream = match &kind {
            ReadKind::Logs { stream, .. } => Some(*stream),
            ReadKind::Status { .. } => None,
        };
        let mut waiter: Option<Waiter> = None;
        let mut last: Option<Pass> = None;
        let mut final_pass = false;
        loop {
            let register = (waiter.is_none() && !final_pass).then(|| watcher.clone()).flatten();
            let (inc, job, pass_kind) = (incarnation.clone(), job_id.to_string(), kind.clone());
            let Some((pass, registered)) = blocking(move || read_pass(&inc, &job, &pass_kind, register.as_deref())).await else {
                return Outcome::Response(read_unavailable("managed process read worker failed"));
            };
            match registered {
                Some(Ok(registered)) => waiter = Some(registered),
                Some(Err(WatchError::Full)) => {
                    return Outcome::Response(retryable(
                        503,
                        json!({"error": "node holds too many waiting job reads", "error_code": "node_job_reads_busy", "retryable": true}),
                    ));
                }
                Some(Err(WatchError::Io(error))) => eprintln!("ucloud-noded: job read answers without waiting: {error}"),
                None => {}
            }
            let pass = match pass {
                Ok(pass) => pass,
                Err(PassError::Read(ReadError::Missing)) => {
                    // Parked or deleted mid-wait: the last record read stands.
                    return match last {
                        Some(last) => Outcome::Response(pass_reply(&last, stream)),
                        None => {
                            forwarded("no managed state on the host");
                            Outcome::Forward(request)
                        }
                    };
                }
                Err(PassError::Read(error)) => return Outcome::Response(read_error(error)),
                Err(PassError::NotFound) => return Outcome::Response(error(409, "primary process not found")),
                Err(PassError::OffsetBeyond) => return Outcome::Response(error(409, "log offset exceeds retained data")),
            };
            self.terminal.observe(&pass.record);
            let changed = match &kind {
                ReadKind::Status { after } => after.as_deref() != Some(pass.record.cursor().as_str()),
                ReadKind::Logs { .. } => pass.chunk.as_ref().is_some_and(|chunk| !chunk.1.is_empty()),
            };
            let Some(deadline) = deadline else { return Outcome::Response(pass_reply(&pass, stream)) };
            if changed || final_pass || pass.record.terminal() {
                return Outcome::Response(pass_reply(&pass, stream));
            }
            let Some(registered) = waiter.as_mut() else { return Outcome::Response(pass_reply(&pass, stream)) };
            last = Some(pass);
            tokio::select! {
                change = registered.changed() => if change == Change::Gone {
                    return Outcome::Response(pass_reply(last.as_ref().expect("set above"), stream));
                },
                () = tokio::time::sleep_until(deadline) => final_pass = true,
            }
        }
    }

    async fn body(request: Request<Body>) -> Result<(hyper::http::request::Parts, Bytes), Outcome> {
        let (parts, body) = request.into_parts();
        match Limited::new(body, MAX_BODY_BYTES).collect().await {
            Ok(collected) => Ok((parts, collected.to_bytes())),
            Err(_) => Err(Outcome::Response(error(400, "request body ended before Content-Length bytes were read"))),
        }
    }

    /// `POST /v1/sandboxes/{id}/jobs` (`start_managed_process`).
    async fn start(&self, request: Request<Body>, incarnation: &Incarnation) -> Outcome {
        let (parts, bytes) = match Self::body(request).await {
            Ok(body) => body,
            Err(outcome) => return outcome,
        };
        let forward = |reason: &'static str| {
            forwarded(reason);
            Outcome::Forward(Request::from_parts(parts.clone(), full(bytes.clone())))
        };
        // A body Python decodes differently (or not at all) is its to answer.
        let Ok(raw) = serde_json::from_slice::<Value>(&bytes) else { return forward("start body") };
        let spec = match ManagedStart::from_value(&raw) {
            Ok(spec) => spec,
            Err(message) => return Outcome::Response(error(400, &message)),
        };
        let bundle = incarnation.bundle.clone();
        let config = blocking(move || read_config(&bundle)).await.and_then(|config| config.ok().flatten()).unwrap_or(Value::Null);
        let (uid, gid) = match start::workload_credentials(&config) {
            Ok(ids) => ids,
            Err(message) => return Outcome::Response(error(409, &message)),
        };
        let default_cwd = match spec.cwd {
            Some(_) => None,
            None => match start::default_cwd(&config) {
                Ok(cwd) => Some(cwd.clone()),
                Err(message) => return Outcome::Response(error(409, &message)),
            },
        };
        let payload = match spec.control_payload(uid, gid, default_cwd.as_ref()) {
            Ok(payload) => payload,
            Err(message) => return Outcome::Response(error(400, &message)),
        };
        let digest = start::launch_sha256(&payload);
        // The launch intent commits before the ambiguous dispatch.
        let launch = self.admission.launch(&incarnation.sandbox_id, incarnation.generation, &spec.job_id, &digest).await;
        let intent_job = match launch {
            Ok(job_id) => job_id,
            Err(LaunchError::StartupBusy(message)) => return Outcome::Response(startup_busy(&message)),
            Err(LaunchError::PrimaryOwned { message, job_id }) => return Outcome::Response(primary_owned(&message, &job_id)),
            Err(LaunchError::Forward(reason)) => return forward(reason),
        };
        let request_bytes = start::control_request_bytes(&payload);
        let raw = match self.control(incarnation, request_bytes, true).await {
            Ok(raw) => raw,
            Err(ControlError::Forward(reason)) => return forward(reason),
            Err(ControlError::Response(response)) => return Outcome::Response(response),
        };
        let record = match JobRecord::from_control(&raw, &incarnation.sandbox_id, incarnation.generation) {
            Ok(record) => record,
            Err(ControlAnswerError::Conflict(message)) => return Outcome::Response(error(409, &message)),
            Err(ControlAnswerError::Invalid(message)) => return Outcome::Response(error(400, &message)),
        };
        if intent_job.is_empty() {
            match self.admission.bind(&incarnation.sandbox_id, incarnation.generation, &record.state.job_id, &digest).await {
                Ok(()) => {}
                Err(BindError::PrimaryOwned { message, job_id }) => return Outcome::Response(primary_owned(&message, &job_id)),
                Err(BindError::Failed(message)) => return Outcome::Response(error(503, &message)),
            }
        }
        self.terminal.observe(&record);
        Outcome::Response(reply(201, json!({"job": record.to_json()})))
    }

    /// `POST /v1/sandboxes/{id}/jobs/{job}/signal` (`signal_managed_process`).
    async fn signal(&self, request: Request<Body>, incarnation: &Incarnation, job_id: &str) -> Outcome {
        let (parts, bytes) = match Self::body(request).await {
            Ok(body) => body,
            Err(outcome) => return outcome,
        };
        let forward = |reason: &'static str| {
            forwarded(reason);
            Outcome::Forward(Request::from_parts(parts.clone(), full(bytes.clone())))
        };
        let Ok(raw) = serde_json::from_slice::<Value>(&bytes) else { return forward("signal body") };
        let Value::Object(raw) = raw else { return Outcome::Response(error(400, "signal payload must be a JSON object")) };
        // int(raw.get("signal") or 15)
        let signal = match raw.get("signal") {
            Some(value) if start::truthy(value) => match value.as_i64() {
                Some(signal) => signal,
                None => return forward("signal Python converts differently"),
            },
            _ => 15,
        };
        // The supervisor's own refusal, without entering the guest.
        if !(1..=64).contains(&signal) || signal == i64::from(libc::SIGKILL) || signal == i64::from(libc::SIGSTOP) {
            return Outcome::Response(error(409, "signal is not supported"));
        }
        let payload = json!({"version": 1, "action": "signal", "job_id": job_id, "signal": signal});
        let raw = match self.control(incarnation, start::control_request_bytes(&payload), false).await {
            Ok(raw) => raw,
            Err(ControlError::Forward(reason)) => return forward(reason),
            Err(ControlError::Response(response)) => return Outcome::Response(response),
        };
        match JobRecord::from_control(&raw, &incarnation.sandbox_id, incarnation.generation) {
            Ok(record) => {
                self.terminal.observe(&record);
                Outcome::Response(reply(200, json!({"job": record.to_json()})))
            }
            Err(ControlAnswerError::Conflict(message)) => Outcome::Response(error(409, &message)),
            Err(ControlAnswerError::Invalid(message)) => Outcome::Response(error(400, &message)),
        }
    }

    /// `_managed_control` for start (`retry_not_ready`) and signal: the
    /// supervisor's JSON answer, or the response Python gives.
    async fn control(&self, incarnation: &Incarnation, request: Vec<u8>, retry_not_ready: bool) -> Result<Value, ControlError> {
        let deadline = Instant::now() + self.limits.control_deadline;
        let busy = || ControlError::Response(startup_busy("managed process control admission deadline exceeded"));
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(busy());
            }
            let guest_timeout_ms = (remaining.as_millis() as i64 - 100).clamp(1, 10_000) as u64;
            let output = match self.node.control(incarnation, request.clone(), guest_timeout_ms, remaining).await {
                Ok(output) => output,
                Err(ControlFailure::Forward(reason)) => return Err(ControlError::Forward(reason)),
                Err(ControlFailure::TimedOut) => return Err(ControlError::Response(error(503, "direct sandbox exec timed out"))),
                Err(ControlFailure::Failed(message)) => return Err(ControlError::Response(error(503, &message))),
            };
            let parsed = serde_json::from_slice::<Value>(&output.stdout).ok().filter(Value::is_object);
            if output.exit_code == Some(0) {
                return match serde_json::from_slice::<Value>(&output.stdout) {
                    Ok(value) if value.is_object() => Ok(value),
                    Ok(_) => Err(ControlError::Response(error(409, "managed process returned a non-object response"))),
                    Err(_) => Err(ControlError::Response(error(409, "managed process returned invalid JSON"))),
                };
            }
            let mut last_error =
                parsed.as_ref().and_then(|failed| failed.get("error")).map(error_text).unwrap_or_default().trim().to_string();
            if last_error.is_empty() {
                last_error = String::from_utf8_lossy(&output.stderr).trim().to_string();
            }
            let not_connected =
                parsed.as_ref().is_some_and(|failed| failed.get("error_code").and_then(Value::as_str) == Some("control_not_connected"));
            if parsed.is_some() && !not_connected {
                // An authoritative supervisor rejection.
                let message = if last_error.is_empty() { "managed process control request failed" } else { &last_error };
                return Err(ControlError::Response(error(409, message)));
            }
            if !(retry_not_ready && not_connected) {
                let message = if last_error.is_empty() { "managed process control exchange failed" } else { &last_error };
                return Err(ControlError::Response(error(409, message)));
            }
            // Only the helper can prove no request byte reached the supervisor.
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(busy());
            }
            tokio::time::sleep(remaining.min(Duration::from_millis(20))).await;
        }
    }
}

/// `str(value or "")` for a JSON error field.
fn error_text(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        other if start::truthy(other) => other.to_string(),
        _ => String::new(),
    }
}

enum ControlError {
    Forward(&'static str),
    Response(Response<Body>),
}

/// The daemon's node: ownership from the registry, liveness from the
/// journal, and `ctl` exchanges under the exec fences.
pub struct NodeJobs {
    pub node: Arc<OnceLock<Arc<NodePipeline>>>,
}

impl NodeJobs {
    fn sandbox(node: &NodePipeline, incarnation: &Incarnation) -> Option<Sandbox> {
        let registration = node.registry().get(&incarnation.sandbox_id).ok().flatten()?;
        (registration.phase == Phase::Owned && registration.sandbox_generation as u64 == incarnation.generation).then(|| Sandbox {
            sandbox_id: incarnation.sandbox_id.clone(),
            generation: incarnation.generation,
            container_id: registration.container_id.clone(),
            bundle: PathBuf::from(&registration.bundle),
            memory_directory: registration.memory_directory.clone(),
            spec_sha256: registration.spec_sha256(),
        })
    }
}

impl JobNode for NodeJobs {
    fn lookup(&self, sandbox_id: &str) -> Lookup {
        let Some(node) = self.node.get() else { return Lookup::Forward("node not configured") };
        let Ok(Some(registration)) = node.registry().get(sandbox_id) else { return Lookup::Forward("not registered") };
        if registration.phase != Phase::Owned {
            return Lookup::Forward("not an owned registration");
        }
        if !registration.spec.managed_process() {
            return Lookup::Forward("not a managed-process sandbox");
        }
        let sandbox = Sandbox {
            sandbox_id: sandbox_id.to_string(),
            generation: registration.sandbox_generation as u64,
            container_id: registration.container_id.clone(),
            bundle: PathBuf::from(&registration.bundle),
            memory_directory: registration.memory_directory.clone(),
            spec_sha256: registration.spec_sha256(),
        };
        // RUNNING with live authority, its sentry alive; a pause marker is fine.
        if !crate::exec_front::running(node, &sandbox) {
            return Lookup::Forward("not running");
        }
        Lookup::Serve(Incarnation {
            sandbox_id: sandbox.sandbox_id,
            generation: sandbox.generation,
            bundle: sandbox.bundle,
            container_id: sandbox.container_id,
        })
    }

    fn controls(&self) -> bool {
        self.node.get().and_then(|node| node.config().exec.as_ref()).is_some_and(|exec| exec.rust_execs_enabled)
    }

    fn control(&self, incarnation: &Incarnation, request: Vec<u8>, guest_timeout_ms: u64, timeout: Duration)
    -> BoxFuture<Result<ControlOutput, ControlFailure>> {
        let (cell, incarnation) = (self.node.clone(), incarnation.clone());
        Box::pin(async move {
            let node = cell.get().cloned().ok_or(ControlFailure::Forward("node not configured"))?;
            let config = node.config().exec.clone().filter(|exec| exec.rust_execs_enabled).ok_or(ControlFailure::Forward("no exec fence"))?;
            // The exec start's fences: A held for the exchange keeps pauses
            // and parks out; the warden flock spans it, as in Python.
            let fence = ExecFence::new(&config.warden_locks_dir);
            let lease = match fence.acquire(&incarnation.sandbox_id) {
                Ok(Fenced::Held(lease)) => lease,
                Ok(Fenced::Busy) => return Err(ControlFailure::Forward("lifecycle fence busy")),
                Err(_) => return Err(ControlFailure::Forward("lifecycle fence unreadable")),
            };
            let (lookup_node, lookup_incarnation) = (node.clone(), incarnation.clone());
            let sandbox = blocking(move || {
                let sandbox = Self::sandbox(&lookup_node, &lookup_incarnation)?;
                crate::exec_front::running(&lookup_node, &sandbox).then_some(sandbox)
            })
            .await
            .flatten()
            .ok_or(ControlFailure::Forward("not running"))?;
            let marker = config.warden_paused_dir.join(format!("{}.sandbox-{}", sandbox.sandbox_id, sandbox.generation));
            let tier = node.pause_tier().cloned();
            if marker.exists() && tier.is_none() {
                return Err(ControlFailure::Forward("paused"));
            }
            let lock = node.warden().lock(&sandbox).await.map_err(|_| ControlFailure::Forward("warden lock"))?;
            if marker.exists() {
                let Some(tier) = tier else { return Err(ControlFailure::Forward("paused")) };
                tier.thaw_locked(&sandbox, &lock, true).await.map_err(|_| ControlFailure::Forward("thaw failed"))?;
            }
            let argv = [
                config.runsc.to_string_lossy().into_owned(),
                format!("--root={}", config.runtime_root.display()),
                "exec".into(),
                "--user=0:0".into(),
                sandbox.container_id.clone(),
                "/.ucloud-job-init".into(),
                "ctl".into(),
                "--timeout".into(),
                format!("{guest_timeout_ms}ms"),
            ];
            let output = run_control(&argv, request, timeout).await;
            if output.as_ref().is_ok_and(|output| output.exit_code == Some(0)) {
                lease.touch();
            }
            drop(lock);
            output
        })
    }
}

/// Spawn `argv`, write `request` to its stdin, collect bounded output.
pub async fn run_control(argv: &[String], request: Vec<u8>, timeout: Duration) -> Result<ControlOutput, ControlFailure> {
    let mut child = tokio::process::Command::new(&argv[0])
        .args(&argv[1..])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|error| ControlFailure::Failed(format!("managed process control could not start: {error}")))?;
    let (mut stdin, mut stdout, mut stderr) =
        (child.stdin.take().expect("piped"), child.stdout.take().expect("piped"), child.stderr.take().expect("piped"));
    let exchange = async {
        let write = async move {
            let _ = stdin.write_all(&request).await;
            drop(stdin);
        };
        let (out, err, ()) = tokio::join!(read_prefix(&mut stdout, MAX_CONTROL_STDOUT + 1), read_prefix(&mut stderr, MAX_CONTROL_STDERR), write);
        let out = out.map_err(|error| ControlFailure::Failed(error.to_string()))?;
        let err = err.unwrap_or_default();
        if out.len() > MAX_CONTROL_STDOUT {
            return Err(ControlFailure::Failed("managed process control response exceeds its limit".into()));
        }
        let status = child.wait().await.map_err(|error| ControlFailure::Failed(error.to_string()))?;
        Ok(ControlOutput { exit_code: status.code(), stdout: out, stderr: err })
    };
    tokio::time::timeout(timeout, exchange).await.map_err(|_| ControlFailure::TimedOut)?
}

/// Read to the end, keeping the first `keep` bytes (the writer never blocks).
async fn read_prefix(reader: &mut (impl tokio::io::AsyncRead + Unpin), keep: usize) -> io::Result<Vec<u8>> {
    let (mut kept, mut buffer) = (Vec::new(), vec![0u8; 64 * 1024]);
    loop {
        let read = reader.read(&mut buffer).await?;
        if read == 0 {
            return Ok(kept);
        }
        let room = keep.saturating_sub(kept.len());
        kept.extend_from_slice(&buffer[..read.min(room)]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routes_take_plain_ids_only() {
        let get = |path: &str| route(&Method::GET, path);
        assert_eq!(get("/v1/sandboxes/sb-1/jobs/j1"), Some(Route::Status { sandbox_id: "sb-1".into(), job_id: "j1".into() }));
        assert_eq!(
            get("/v1/sandboxes/sb-1/jobs/j1/logs/stderr"),
            Some(Route::Logs { sandbox_id: "sb-1".into(), job_id: "j1".into(), stream: "stderr" })
        );
        assert_eq!(route(&Method::POST, "/v1/sandboxes/sb/jobs"), Some(Route::Start { sandbox_id: "sb".into() }));
        assert_eq!(route(&Method::POST, "/v1/sandboxes/sb/jobs/j/signal"), Some(Route::Signal { sandbox_id: "sb".into(), job_id: "j".into() }));
        for path in [
            "/v1/sandboxes/sb/jobs/j%2F/logs/stdout",
            "/v1/sandboxes/sb/jobs/j/logs/stdin",
            "/v1/sandboxes/sb/jobs/",
            "/v1/sandboxes//jobs/j",
            "/v1/sandboxes/sb/jobs/j/",
            "/v1/sandboxes/sb/exec",
        ] {
            assert_eq!(get(path), None, "{path}");
        }
        assert_eq!(route(&Method::GET, "/v1/sandboxes/sb/jobs"), None);
        assert_eq!(route(&Method::POST, "/v1/sandboxes/sb/jobs/j"), None);
    }

    #[test]
    fn queries_read_like_parse_qs_or_go_to_python() {
        assert_eq!(query_value("offset=5&limit=&offset=7", "offset"), Some("5"));
        assert_eq!(query_value("offset=5&limit=", "limit"), Some(""));
        assert_eq!(query_value("x=1", "offset"), None);
        assert_eq!(query_int("12"), Some(12));
        assert_eq!(query_int("-3"), Some(-3));
        for raw in ["", "1.5", "%31", "+1", "1234567890123456789", "x"] {
            assert_eq!(query_int(raw), None, "{raw}");
        }
    }
}
