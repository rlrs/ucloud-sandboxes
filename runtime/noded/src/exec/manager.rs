//! `ExecSessionManager` (sandbox_exec.py) and the exec route handlers of
//! node_agent.py, for sessions this daemon starts.
//!
//! Semantics kept from Python (spec §2.3, §6.1):
//! - sequences are contiguous from 1, which is always `status`/`started`;
//! - the events cursor is a cumulative acknowledgement: `after` acknowledges
//!   every event up to it, a response alone does not, and re-polling with the
//!   same `after` replays;
//! - a session holds at most `max_events_per_session` events; the producer
//!   prunes acknowledged ones and otherwise waits, and after
//!   `output_idle_timeout` without acknowledgement progress it aborts the
//!   process with an `error` event and exit code 1;
//! - after the process exits, completion waits for both output pipes to close
//!   while output keeps flowing, and gives up after `output_quiet_grace` of
//!   quiet (a descendant holding a pipe); `final_sequence` is set only when
//!   both pipes closed;
//! - sessions leave the table only to make room at capacity: delivered
//!   results first, then terminal sessions idle for `completed_retention`;
//!   running sessions never.
//!
//! Locking: the table lock may be held while taking a session's state lock,
//! never the reverse. Output appends take only their session's lock.

use std::any::Any;
use std::collections::{HashMap, VecDeque};
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::process::ExitStatusExt;
use std::process::Stdio;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, mpsc};
use std::time::{Duration, Instant, SystemTime};

use serde_json::{Map, Value, json};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use super::decode::Utf8Decoder;
use super::error::{ExecError, Reply};
use super::json::encoded_str_len;
use super::request::{EventsQuery, ExecRequest, StdinRequest, parse_signal, py_repr};
use super::time::isoformat;

/// Held for the life of a session's process: dropped once the process is
/// reaped and the session's cleanup is done (or when `start` fails). The
/// caller puts its lifecycle fences (the activity flock) in it.
pub type Guard = Box<dyn Any + Send>;

/// Called with the sandbox id and the session's guard when an exec starts and
/// when it completes, before the guard drops (Python's `mark_activity` at
/// `acquire_shared` and `release_shared`). The guard lets the caller touch its
/// own fence, e.g. `guard.downcast_ref::<ActivityLease>().map(|l| l.touch())`.
/// Runs on a runtime thread: keep it short (a `futimens`).
pub type ActivityHook = Arc<dyn Fn(&str, &(dyn Any + Send)) + Send + Sync>;

const BACKPRESSURE_ABORT: &str = "exec output consumer stopped advancing; output backpressure timed out";
const CAPACITY_REACHED: &str = "exec session capacity reached";

#[derive(Clone, Debug)]
pub struct ExecLimits {
    /// Sessions held at once (Python `max_sessions`).
    pub max_sessions: usize,
    /// Events held per session before the producer waits.
    pub max_events_per_session: usize,
    /// A terminal session updated this long ago may be evicted at capacity.
    pub completed_retention: Duration,
    /// A session whose final event was delivered this long ago may be evicted.
    pub delivered_grace: Duration,
    /// Backpressure without acknowledgement progress this long aborts.
    pub output_idle_timeout: Duration,
    /// After exit, completion waits this long without new output for the pipes.
    pub output_quiet_grace: Duration,
    /// One read of an output pipe, so one event's input bytes.
    pub read_chunk_bytes: usize,
    /// Events in R1's initial snapshot.
    pub initial_events: usize,
    /// The longest initial snapshot wait.
    pub initial_wait_max: Duration,
    /// An events page stops before its events' JSON passes this many bytes
    /// (always at least one event); the gateway refuses replies over 16 MiB.
    pub page_bytes: usize,
}

impl Default for ExecLimits {
    fn default() -> Self {
        ExecLimits {
            max_sessions: 1024,
            max_events_per_session: 512,
            completed_retention: Duration::from_secs(30),
            delivered_grace: Duration::from_secs(2),
            output_idle_timeout: Duration::from_secs(300),
            output_quiet_grace: Duration::from_secs(2),
            read_chunk_bytes: 4096,
            initial_events: 100,
            initial_wait_max: Duration::from_millis(50),
            page_bytes: 4 << 20,
        }
    }
}

impl ExecLimits {
    /// Python's constructor clamps.
    fn normalized(mut self) -> Self {
        self.max_sessions = self.max_sessions.max(1);
        self.max_events_per_session = self.max_events_per_session.max(1);
        self.output_idle_timeout = self.output_idle_timeout.max(Duration::from_millis(10));
        self.read_chunk_bytes = self.read_chunk_bytes.max(1);
        self
    }
}

/// What R1's `timings` reports besides the manager's own marks.
#[derive(Clone, Debug)]
pub struct StartTimings {
    /// When the request arrived: `start_ms` counts from here.
    pub received_at: Instant,
    /// The caller's marks (Python: `consume_exec_start_timings()`).
    pub manager: Map<String, Value>,
}

impl StartTimings {
    pub fn now() -> Self {
        StartTimings { received_at: Instant::now(), manager: Map::new() }
    }
}

/// Python `valid_session_prefix` (exec_session_routes.py): at most 768
/// characters matching `xr1\.[A-Za-z0-9_-]{1,700}\.[A-Za-z0-9_-]{22}`.
pub fn valid_session_prefix(prefix: &str) -> bool {
    let word = |part: &str, lengths: std::ops::RangeInclusive<usize>| {
        lengths.contains(&part.len()) && part.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    };
    let parts: Vec<&str> = prefix.split('.').collect();
    prefix.len() <= 768 && parts.len() == 3 && parts[0] == "xr1" && word(parts[1], 1..=700) && word(parts[2], 22..=22)
}

/// Python `new_exec_session_id`: `<prefix>.<uuid4 hex>` for a well-formed
/// gateway prefix, else `exec-<uuid4 hex>`.
pub fn new_session_id(prefix: Option<&str>) -> String {
    let mut bytes = [0u8; 16];
    let mut filled = 0;
    while filled < bytes.len() {
        let read = unsafe { libc::getrandom(bytes[filled..].as_mut_ptr().cast(), bytes.len() - filled, 0) };
        if read > 0 {
            filled += read as usize;
        } else if io::Error::last_os_error().kind() != io::ErrorKind::Interrupted {
            panic!("getrandom failed: {}", io::Error::last_os_error());
        }
    }
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    let hex: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
    match prefix {
        Some(prefix) if valid_session_prefix(prefix) => format!("{prefix}.{hex}"),
        _ => format!("exec-{hex}"),
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn not_found(session_id: &str) -> ExecError {
    ExecError::BadRequest(format!("exec session not found: {session_id}"))
}

fn millis(since: Instant) -> f64 {
    since.elapsed().as_secs_f64() * 1000.0
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Status {
    Running,
    Exited,
    Failed,
}

impl Status {
    fn terminal(self) -> bool {
        self != Status::Running
    }

    fn as_str(self) -> &'static str {
        match self {
            Status::Running => "running",
            Status::Exited => "exited",
            Status::Failed => "failed",
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Delivered {
    No,
    /// By R1's snapshot: evictable at once (a lost start response leaves no
    /// session id to ask by).
    AtStart,
    At(Instant),
}

struct Event {
    sequence: i64,
    stream: &'static str,
    data: String,
    exit_code: Option<i32>,
    created_at: SystemTime,
}

impl Event {
    fn to_json(&self) -> Value {
        json!({
            "sequence": self.sequence,
            "stream": self.stream,
            "data": self.data,
            "exit_code": self.exit_code,
            "created_at": isoformat(self.created_at),
        })
    }

    /// An upper bound of the event's JSON size.
    fn encoded_len(&self) -> usize {
        128 + encoded_str_len(&self.data)
    }
}

/// The client process, for signals: a pidfd when the kernel has them, so a
/// signal can never reach a recycled pid.
struct Process {
    pid: u32,
    pidfd: Option<OwnedFd>,
}

impl Process {
    fn new(pid: u32) -> Self {
        let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid as libc::pid_t, 0) };
        let pidfd = (fd >= 0).then(|| unsafe { OwnedFd::from_raw_fd(fd as i32) });
        Process { pid, pidfd }
    }

    /// `Popen.send_signal`: a process that is already gone is not an error.
    fn signal(&self, signal: i32) -> io::Result<()> {
        let result = match &self.pidfd {
            Some(fd) => unsafe {
                libc::syscall(
                    libc::SYS_pidfd_send_signal,
                    fd.as_raw_fd(),
                    signal,
                    std::ptr::null::<libc::siginfo_t>(),
                    0,
                ) as i32
            },
            None => unsafe { libc::kill(self.pid as libc::pid_t, signal) },
        };
        match result {
            0 => Ok(()),
            _ => match io::Error::last_os_error() {
                error if error.raw_os_error() == Some(libc::ESRCH) => Ok(()),
                error => Err(error),
            },
        }
    }
}

struct State {
    status: Status,
    exit_code: Option<i32>,
    stdin_open: bool,
    events: VecDeque<Event>,
    next_sequence: i64,
    acknowledged: i64,
    output_waiters: usize,
    output_aborted: bool,
    output_progress_at: Instant,
    output_closed: bool,
    final_sequence: Option<i64>,
    delivered: Delivered,
    updated_at: SystemTime,
    updated_mono: Instant,
    /// Python: `_sessions.get(id) is not session`.
    evicted: bool,
    completing: bool,
    process: Option<Process>,
}

struct Session {
    id: String,
    request: ExecRequest,
    spec: Value,
    argv: Vec<String>,
    created_at: SystemTime,
    state: Mutex<State>,
    /// Wakes long-polls, the initial snapshot and a blocked producer.
    changed: Notify,
    /// Python's `stdin_lock` with the pipe it guards.
    stdin: tokio::sync::Mutex<Option<ChildStdin>>,
    guard: Mutex<Option<Guard>>,
}

impl Session {
    fn state(&self) -> MutexGuard<'_, State> {
        lock(&self.state)
    }

    /// `_append_event_locked`.
    fn append(&self, state: &mut State, stream: &'static str, data: String, exit_code: Option<i32>) {
        let now = SystemTime::now();
        state.events.push_back(Event { sequence: state.next_sequence, stream, data, exit_code, created_at: now });
        state.next_sequence += 1;
        Self::touch(state);
        self.changed.notify_waiters();
    }

    fn touch(state: &mut State) {
        state.updated_at = SystemTime::now();
        state.updated_mono = Instant::now();
    }

    /// `ExecSession.to_dict()`.
    fn to_json(&self, state: &State) -> Value {
        json!({
            "id": self.id,
            "spec": self.spec,
            "argv": self.argv,
            "status": state.status.as_str(),
            "exit_code": state.exit_code,
            "stdin_open": state.stdin_open,
            "created_at": isoformat(self.created_at),
            "updated_at": isoformat(state.updated_at),
            "final_sequence": state.final_sequence,
        })
    }

    fn json(&self) -> Value {
        self.to_json(&self.state())
    }
}

/// `_mark_delivered`: the response carries the final event.
fn mark_delivered(state: &mut State, last: Option<i64>, at: Delivered) {
    if let (Some(final_sequence), Some(last), Delivered::No) = (state.final_sequence, last, state.delivered)
        && last >= final_sequence
    {
        state.delivered = at;
    }
}

/// Events in order up to `limit` and `page_bytes` (at least one), with the
/// last sequence returned.
fn page<'a>(events: impl Iterator<Item = &'a Event>, limit: usize, page_bytes: usize) -> (Vec<Value>, Option<i64>) {
    let mut values = Vec::new();
    let mut bytes = 0;
    let mut last = None;
    for event in events.take(limit) {
        bytes += event.encoded_len();
        if !values.is_empty() && bytes > page_bytes {
            break;
        }
        values.push(event.to_json());
        last = Some(event.sequence);
    }
    (values, last)
}

#[derive(Default)]
struct Table {
    sessions: HashMap<String, Arc<Session>>,
    /// Terminal sessions still in `sessions`: the eviction candidates.
    terminal: HashMap<String, Arc<Session>>,
}

struct Inner {
    limits: ExecLimits,
    on_activity: Option<ActivityHook>,
    table: Mutex<Table>,
}

/// The exec session table and the route handlers over it.
pub struct ExecManager {
    inner: Arc<Inner>,
}

impl ExecManager {
    pub fn new(limits: ExecLimits, on_activity: Option<ActivityHook>) -> Arc<Self> {
        Arc::new(ExecManager {
            inner: Arc::new(Inner { limits: limits.normalized(), on_activity, table: Mutex::default() }),
        })
    }

    pub fn limits(&self) -> &ExecLimits {
        &self.inner.limits
    }

    /// Whether `session_id` is this manager's (else forward the route to Python).
    pub fn owns(&self, session_id: &str) -> bool {
        lock(&self.inner.table).sessions.contains_key(session_id)
    }

    /// Sessions held, running and terminal.
    pub fn session_count(&self) -> usize {
        lock(&self.inner.table).sessions.len()
    }

    /// Sessions whose process has not completed.
    pub fn running_count(&self) -> usize {
        let table = lock(&self.inner.table);
        table.sessions.len() - table.terminal.len()
    }

    /// R1 after the caller's admission: register the session, spawn `argv`
    /// and answer 201 (`{"session", ["events",] "timings"}`).
    ///
    /// `request` must have passed [`ExecRequest::check_direct`]; `argv` is
    /// normally [`super::runsc_exec_argv`]. The process is spawned with
    /// `PR_SET_PDEATHSIG` from a dedicated, never-exiting thread (the signal
    /// follows the forking *thread*), stdin piped only for `stdin: true`
    /// (else `/dev/null`), stdout and stderr piped. A spawn failure is not an
    /// error: the session reports an `error` event and exit code 1, as in
    /// Python. The only error is [`ExecError::ExecDeferred`] at capacity, when
    /// nothing started and `guard` has been dropped.
    ///
    /// `spawn_fence`, if any, is dropped as soon as the spawn returns, before
    /// the initial snapshot wait: the warden flock goes there (Python releases
    /// it in `exec_started`, right after `Popen`).
    ///
    /// Cancel-safe: once called, the session starts even if the caller's
    /// future is dropped.
    pub async fn start(
        &self,
        request: ExecRequest,
        argv: Vec<String>,
        guard: Guard,
        spawn_fence: Option<Guard>,
        timings: StartTimings,
    ) -> Result<Reply, ExecError> {
        self.inner.activity(&request.sandbox_id, &*guard);
        let initial_wait = request.initial_wait;
        let launched = tokio::spawn(self.inner.clone().launch(request, argv, guard, spawn_fence));
        let (session, session_start) = launched
            .await
            .map_err(|error| ExecError::Unavailable(format!("exec start failed: {error}")))??;
        let start_ms = timings.received_at.elapsed().as_millis() as u64;
        let mut body = Map::new();
        match initial_wait {
            Some(wait) => {
                let (session, events) = self.inner.initial_events(&session, wait).await;
                body.insert("session".into(), session);
                body.insert("events".into(), Value::Array(events));
            }
            None => {
                body.insert("session".into(), session.json());
            }
        }
        body.insert(
            "timings".into(),
            json!({"manager": timings.manager, "start_ms": start_ms, "session_start": session_start}),
        );
        Ok(Reply::new(201, Value::Object(body)))
    }

    /// R2: 200 `{"session"}`, or 404 `{"error":"exec session not found"}`.
    pub fn get(&self, session_id: &str) -> Reply {
        match self.inner.lookup(session_id) {
            Some(session) => Reply::ok(json!({"session": session.json()})),
            None => ExecError::NotFound("exec session not found".into()).reply(),
        }
    }

    /// R3: `query` is the raw query string. 200 `{"session", "events"}`
    /// (`session` is null if the session was evicted during the wait), or 404
    /// `{"error":"exec session not found: <sid>"}`.
    pub async fn events(&self, session_id: &str, query: &str) -> Reply {
        let query = EventsQuery::parse(query);
        let Some(session) = self.inner.lookup(session_id) else {
            return ExecError::NotFound(format!("exec session not found: {session_id}")).reply();
        };
        let events = self.inner.events_after(&session, query).await;
        let session = self.inner.lookup(session_id).map_or(Value::Null, |session| session.json());
        Reply::ok(json!({"session": session, "events": events}))
    }

    /// R4: `{"data", "eof"}` written to the process's stdin. 400 for every
    /// error, including an unknown session.
    pub async fn stdin(&self, session_id: &str, body: &[u8]) -> Reply {
        let result = async {
            let request = StdinRequest::parse(body)?;
            let mut session = self.inner.write_stdin(session_id, &request.data).await?;
            if request.eof {
                session = self.inner.close_stdin(session_id).await?;
            }
            Ok::<_, ExecError>(session)
        }
        .await;
        match result {
            Ok(session) => Reply::ok(json!({"session": session.json()})),
            Err(error) => error.reply(),
        }
    }

    /// R5: idempotent; 400 for an unknown session.
    pub async fn close_stdin(&self, session_id: &str) -> Reply {
        match self.inner.close_stdin(session_id).await {
            Ok(session) => Reply::ok(json!({"session": session.json()})),
            Err(error) => error.reply(),
        }
    }

    /// R6: `{"signal": n}` to the client process; a no-op once the session is
    /// terminal. 400 for every error, including an unknown session.
    pub async fn signal(&self, session_id: &str, body: &[u8]) -> Reply {
        let result = parse_signal(body).and_then(|signal| self.inner.signal(session_id, signal));
        match result {
            Ok(session) => Reply::ok(json!({"session": session.json()})),
            Err(error) => error.reply(),
        }
    }
}

impl Inner {
    fn activity(&self, sandbox_id: &str, guard: &(dyn Any + Send)) {
        if let Some(hook) = &self.on_activity {
            hook(sandbox_id, guard);
        }
    }

    fn lookup(&self, session_id: &str) -> Option<Arc<Session>> {
        lock(&self.table).sessions.get(session_id).cloned()
    }

    async fn launch(
        self: Arc<Self>,
        request: ExecRequest,
        argv: Vec<String>,
        guard: Guard,
        spawn_fence: Option<Guard>,
    ) -> Result<(Arc<Session>, Value), ExecError> {
        let started = Instant::now();
        let now = SystemTime::now();
        let session = Arc::new(Session {
            id: new_session_id(request.session_prefix.as_deref()),
            spec: request.spec_json(),
            argv,
            created_at: now,
            state: Mutex::new(State {
                status: Status::Running,
                exit_code: None,
                stdin_open: request.stdin,
                events: VecDeque::new(),
                next_sequence: 1,
                acknowledged: 0,
                output_waiters: 0,
                output_aborted: false,
                output_progress_at: Instant::now(),
                output_closed: false,
                final_sequence: None,
                delivered: Delivered::No,
                updated_at: now,
                updated_mono: Instant::now(),
                evicted: false,
                completing: false,
                process: None,
            }),
            request,
            changed: Notify::new(),
            stdin: tokio::sync::Mutex::new(None),
            guard: Mutex::new(Some(guard)),
        });
        {
            let mut table = lock(&self.table);
            self.make_room(&mut table)?;
            table.sessions.insert(session.id.clone(), session.clone());
            let mut state = session.state();
            session.append(&mut state, "status", "started".into(), None);
        }
        let registry_ms = millis(started);
        let mark = Instant::now();
        // Held across the spawn, so a racing stdin write finds the pipe.
        let mut stdin = session.stdin.lock().await;
        let spawned = spawn(&session.argv, session.request.stdin).await;
        drop(spawn_fence);
        let popen_ms = millis(mark);
        let mark = Instant::now();
        match spawned {
            Err(error) => {
                drop(stdin);
                let message = python_os_error(&error, session.argv.first().map(String::as_str));
                {
                    let mut state = session.state();
                    session.append(&mut state, "error", message, None);
                }
                self.complete(&session, 1);
            }
            Ok(mut child) => {
                *stdin = child.stdin.take();
                drop(stdin);
                session.state().process = child.id().map(Process::new);
                let stdout = tokio::spawn(self.clone().pump(session.clone(), "stdout", child.stdout.take()));
                let stderr = tokio::spawn(self.clone().pump(session.clone(), "stderr", child.stderr.take()));
                tokio::spawn(self.clone().wait(session.clone(), child, [stdout, stderr]));
            }
        }
        let timings = json!({
            "session_registry_ms": registry_ms,
            "popen_ms": popen_ms,
            "pump_threads_ms": millis(mark),
            "total_ms": millis(started),
        });
        Ok((session, timings))
    }

    /// `_make_session_room_locked`: delivered results first, all at once;
    /// then terminal sessions by age past the retention; else refuse.
    fn make_room(&self, table: &mut Table) -> Result<(), ExecError> {
        if table.sessions.len() < self.limits.max_sessions {
            return Ok(());
        }
        let now = Instant::now();
        let delivered: Vec<String> = table
            .terminal
            .values()
            .filter(|session| match session.state().delivered {
                Delivered::No => false,
                Delivered::AtStart => true,
                Delivered::At(at) => now.duration_since(at) >= self.limits.delivered_grace,
            })
            .map(|session| session.id.clone())
            .collect();
        for id in delivered {
            evict(table, &id);
        }
        if table.sessions.len() < self.limits.max_sessions {
            return Ok(());
        }
        let mut aged: Vec<(Instant, String)> =
            table.terminal.values().map(|session| (session.state().updated_mono, session.id.clone())).collect();
        aged.sort();
        for (updated, id) in aged {
            if now.duration_since(updated) < self.limits.completed_retention {
                break;
            }
            evict(table, &id);
            if table.sessions.len() < self.limits.max_sessions {
                return Ok(());
            }
        }
        Err(ExecError::ExecDeferred(CAPACITY_REACHED.into()))
    }

    /// `_pump_stream`: read, decode, append; stop when the session refuses.
    async fn pump(self: Arc<Self>, session: Arc<Session>, stream: &'static str, reader: Option<impl AsyncRead + Unpin>) {
        let Some(mut reader) = reader else { return };
        let mut decoder = Utf8Decoder::new();
        let mut buffer = vec![0u8; self.limits.read_chunk_bytes];
        loop {
            let chunk = match reader.read(&mut buffer).await {
                Ok(0) => {
                    let tail = decoder.finish();
                    if !tail.is_empty() {
                        self.append_output(&session, stream, tail).await;
                    }
                    return;
                }
                Ok(read) => decoder.decode(&buffer[..read]),
                Err(_) => return,
            };
            if !chunk.is_empty() && !self.append_output(&session, stream, chunk).await {
                return;
            }
        }
    }

    /// `_append_stream_chunk`: false when the pump must stop.
    async fn append_output(&self, session: &Session, stream: &'static str, chunk: String) -> bool {
        let started_wait = Instant::now();
        let mut waiting = false;
        loop {
            let notified = session.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let remaining = {
                let mut state = session.state();
                if waiting {
                    state.output_waiters -= 1;
                }
                if state.evicted {
                    return false;
                }
                while state.events.len() >= self.limits.max_events_per_session
                    && state.events.front().is_some_and(|event| event.sequence <= state.acknowledged)
                {
                    state.events.pop_front();
                }
                if state.output_aborted {
                    return false;
                }
                if state.events.len() < self.limits.max_events_per_session {
                    session.append(&mut state, stream, chunk, None);
                    return true;
                }
                let idle = Instant::now().duration_since(started_wait.max(state.output_progress_at));
                if idle >= self.limits.output_idle_timeout {
                    // An abandoned reader must not pin a process (and its
                    // fences) forever. Keep buffered output; say why.
                    state.output_aborted = true;
                    session.append(&mut state, "error", BACKPRESSURE_ABORT.into(), None);
                    if let Some(process) = &state.process {
                        let _ = process.signal(libc::SIGKILL);
                    }
                    return false;
                }
                state.output_waiters += 1;
                waiting = true;
                self.limits.output_idle_timeout - idle
            };
            let _ = tokio::time::timeout(remaining, notified).await;
        }
    }

    /// `_wait_process_unobserved`, then `_complete`.
    async fn wait(self: Arc<Self>, session: Arc<Session>, mut child: Child, pumps: [JoinHandle<()>; 2]) {
        let mut exit_code = match child.wait().await {
            Ok(status) => status.code().unwrap_or_else(|| 128 + status.signal().unwrap_or(0)),
            Err(_) => 1,
        };
        // A full output buffer is not EOF: keep completion fenced while the
        // reader drains it. Give up after a quiet grace (a descendant holds a pipe).
        let [stdout, stderr] = pumps;
        let joined = async move {
            let _ = stdout.await;
            let _ = stderr.await;
        };
        tokio::pin!(joined);
        let mut quiet_since = Instant::now();
        let mut observed = None;
        let output_closed = loop {
            tokio::select! {
                () = &mut joined => break true,
                () = tokio::time::sleep(Duration::from_millis(50)) => {}
            }
            {
                let state = session.state();
                if state.evicted {
                    break false;
                }
                if state.output_waiters > 0 || observed != Some(state.next_sequence) {
                    quiet_since = Instant::now();
                    observed = Some(state.next_sequence);
                }
            }
            if quiet_since.elapsed() >= self.limits.output_quiet_grace {
                break false;
            }
        };
        let mut stdin = session.stdin.lock().await;
        stdin.take();
        {
            let mut state = session.state();
            state.stdin_open = false;
            state.output_closed = output_closed;
            if state.output_aborted {
                exit_code = 1;
            }
        }
        self.complete(&session, exit_code);
        drop(stdin);
    }

    /// `_complete`, once per session: activity, release the guard, then the
    /// terminal status and the `exit` event.
    fn complete(&self, session: &Arc<Session>, exit_code: i32) {
        {
            let mut state = session.state();
            if state.status.terminal() || state.completing {
                return;
            }
            state.completing = true;
            state.stdin_open = false;
            state.process = None;
        }
        // Touch the activity clock while the fences are still held, so a
        // parker that wins the fence next sees this activity.
        let guard = lock(&session.guard).take();
        if let Some(guard) = &guard {
            self.activity(&session.request.sandbox_id, &**guard);
        }
        drop(guard);
        {
            let mut state = session.state();
            state.exit_code = Some(exit_code);
            state.status = if exit_code == 0 { Status::Exited } else { Status::Failed };
            session.append(&mut state, "exit", String::new(), Some(exit_code));
            if state.output_closed {
                state.final_sequence = Some(state.next_sequence - 1);
            }
        }
        let mut table = lock(&self.table);
        if table.sessions.get(&session.id).is_some_and(|held| Arc::ptr_eq(held, session)) {
            table.terminal.insert(session.id.clone(), session.clone());
        }
    }

    /// `events_after`.
    async fn events_after(&self, session: &Session, query: EventsQuery) -> Vec<Value> {
        let deadline = Instant::now() + query.wait;
        {
            let mut state = session.state();
            let acknowledged = query.after.max(0).min(state.next_sequence - 1);
            if acknowledged > state.acknowledged {
                state.acknowledged = acknowledged;
                state.output_progress_at = Instant::now();
                session.changed.notify_waiters();
            }
        }
        let limit = usize::try_from(query.limit.max(0)).unwrap_or(usize::MAX);
        loop {
            let notified = session.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut state = session.state();
                let start = state.events.partition_point(|event| event.sequence <= query.after);
                if start < state.events.len() || state.status.terminal() || query.wait.is_zero() {
                    let (values, last) = page(state.events.range(start..), limit, self.limits.page_bytes);
                    mark_delivered(&mut state, last, Delivered::At(Instant::now()));
                    return values;
                }
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Vec::new();
            }
            let _ = tokio::time::timeout(remaining, notified).await;
        }
    }

    /// `initial_events`: wait up to the bound for the final event or a full
    /// snapshot, never for stdin or tty sessions.
    async fn initial_events(&self, session: &Session, wait: f64) -> (Value, Vec<Value>) {
        let wait = Duration::try_from_secs_f64(wait.max(0.0)).unwrap_or(Duration::ZERO).min(self.limits.initial_wait_max);
        let deadline = Instant::now() + wait;
        loop {
            let notified = session.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let remaining = {
                let state = session.state();
                if state.final_sequence.is_some() || state.events.len() >= self.limits.initial_events {
                    break;
                }
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() || session.request.stdin || session.request.tty {
                    break;
                }
                remaining
            };
            let _ = tokio::time::timeout(remaining, notified).await;
        }
        let mut state = session.state();
        let (events, last) = page(state.events.iter(), self.limits.initial_events, self.limits.page_bytes);
        mark_delivered(&mut state, last, Delivered::AtStart);
        (session.to_json(&state), events)
    }

    /// `write_stdin`.
    async fn write_stdin(&self, session_id: &str, data: &str) -> Result<Arc<Session>, ExecError> {
        let session = self.lookup(session_id).ok_or_else(|| not_found(session_id))?;
        let mut stdin = session.stdin.lock().await;
        {
            let state = session.state();
            if state.evicted {
                return Err(not_found(session_id));
            }
            if !state.stdin_open {
                return Err(ExecError::BadRequest("stdin is closed for this exec session.".into()));
            }
        }
        let Some(pipe) = stdin.as_mut() else {
            return Err(ExecError::BadRequest("stdin pipe is unavailable.".into()));
        };
        let written = match pipe.write_all(data.as_bytes()).await {
            Ok(()) => pipe.flush().await,
            Err(error) => Err(error),
        };
        let mut state = session.state();
        if written.is_err() && !state.evicted {
            state.stdin_open = false;
        }
        if !state.evicted {
            Session::touch(&mut state);
        }
        match written {
            Ok(()) => Ok(session.clone()),
            Err(_) => Err(ExecError::BadRequest("stdin pipe is closed for this exec session.".into())),
        }
    }

    /// `close_stdin`: idempotent.
    async fn close_stdin(&self, session_id: &str) -> Result<Arc<Session>, ExecError> {
        let session = self.lookup(session_id).ok_or_else(|| not_found(session_id))?;
        let mut stdin = session.stdin.lock().await;
        {
            let mut state = session.state();
            if state.evicted {
                return Err(not_found(session_id));
            }
            if !state.stdin_open {
                return Ok(session.clone());
            }
            state.stdin_open = false;
            Session::touch(&mut state);
        }
        stdin.take();
        Ok(session.clone())
    }

    /// `signal`, after the body check.
    fn signal(&self, session_id: &str, signal: i32) -> Result<Arc<Session>, ExecError> {
        let session = self.lookup(session_id).ok_or_else(|| not_found(session_id))?;
        {
            let state = session.state();
            if let (false, Some(process)) = (state.status.terminal(), &state.process) {
                process.signal(signal).map_err(|error| {
                    ExecError::BadRequest(format!(
                        "cannot signal exec session {session_id}: {}",
                        python_os_error(&error, None)
                    ))
                })?;
            }
        }
        Ok(session)
    }
}

fn evict(table: &mut Table, id: &str) {
    if let Some(session) = table.terminal.remove(id) {
        table.sessions.remove(id);
        session.state().evicted = true;
        session.changed.notify_waiters();
    }
}

/// Python's `str(OSError)`: `[Errno 2] No such file or directory: 'runsc'`.
fn python_os_error(error: &io::Error, filename: Option<&str>) -> String {
    let Some(code) = error.raw_os_error() else {
        return error.to_string();
    };
    let text = io::Error::from_raw_os_error(code).to_string();
    let strerror = text.strip_suffix(&format!(" (os error {code})")).unwrap_or(&text);
    match filename {
        Some(name) => format!("[Errno {code}] {strerror}: {}", py_repr(name).unwrap_or_else(|| format!("'{name}'"))),
        None => format!("[Errno {code}] {strerror}"),
    }
}

type SpawnJob = Box<dyn FnOnce() + Send>;

/// Spawns happen on these threads, which never exit: `PR_SET_PDEATHSIG`
/// fires when the forking *thread* exits, so a tokio blocking-pool thread
/// (which retires when idle) would kill its children.
fn forkers() -> &'static Mutex<mpsc::Sender<SpawnJob>> {
    static FORKERS: OnceLock<Mutex<mpsc::Sender<SpawnJob>>> = OnceLock::new();
    FORKERS.get_or_init(|| {
        let (sender, receiver) = mpsc::channel::<SpawnJob>();
        let receiver = Arc::new(Mutex::new(receiver));
        for index in 0..4 {
            let receiver = receiver.clone();
            std::thread::Builder::new()
                .name(format!("noded-exec-fork-{index}"))
                .spawn(move || {
                    loop {
                        let job = lock(&receiver).recv();
                        match job {
                            Ok(job) => job(),
                            Err(_) => return,
                        }
                    }
                })
                .expect("exec fork thread");
        }
        Mutex::new(sender)
    })
}

/// Spawn `argv` with `PR_SET_PDEATHSIG(SIGKILL)` from a fork thread, under
/// the caller's runtime.
async fn spawn(argv: &[String], stdin: bool) -> io::Result<Child> {
    let Some((program, args)) = argv.split_first() else {
        return Err(io::Error::from_raw_os_error(libc::ENOENT));
    };
    let mut command = Command::new(program);
    command
        .args(args)
        .stdin(if stdin { Stdio::piped() } else { Stdio::null() })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let parent = unsafe { libc::getpid() };
    unsafe {
        command.pre_exec(move || {
            if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL as libc::c_ulong, 0, 0, 0) != 0 {
                return Err(io::Error::last_os_error());
            }
            // The daemon died before the prctl: nobody would deliver it.
            if libc::getppid() != parent {
                return Err(io::Error::from_raw_os_error(libc::ESRCH));
            }
            Ok(())
        });
    }
    let handle = tokio::runtime::Handle::current();
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let job: SpawnJob = Box::new(move || {
        let _runtime = handle.enter();
        let _ = sender.send(command.spawn());
    });
    lock(forkers()).send(job).map_err(|_| io::Error::other("exec fork threads are gone"))?;
    receiver.await.unwrap_or_else(|_| Err(io::Error::other("exec fork thread dropped the spawn")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_ids() {
        let prefix = format!("xr1.{}.{}", "a".repeat(10), "b".repeat(22));
        let id = new_session_id(Some(&prefix));
        let (head, hex) = id.rsplit_once('.').unwrap();
        assert_eq!(head, prefix);
        assert_eq!(hex.len(), 32);
        assert!(hex.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)));
        assert_eq!(&hex[12..13], "4");
        assert!("89ab".contains(&hex[16..17]));
        assert!(new_session_id(Some("xr1.a.short")).starts_with("exec-"));
        assert!(new_session_id(None).starts_with("exec-"));
        assert!(!valid_session_prefix(&format!("xr1.{}.{}", "a".repeat(701), "b".repeat(22))));
        assert!(!valid_session_prefix(&format!("xr1.a.{}.", "b".repeat(22))));
        assert!(!valid_session_prefix(&format!("xr1.a+.{}", "b".repeat(22))));
    }

    #[test]
    fn os_errors_read_like_python() {
        let error = io::Error::from_raw_os_error(libc::ENOENT);
        assert_eq!(python_os_error(&error, Some("/x/runsc")), "[Errno 2] No such file or directory: '/x/runsc'");
        assert_eq!(python_os_error(&io::Error::from_raw_os_error(libc::EPERM), None), "[Errno 1] Operation not permitted");
    }
}
