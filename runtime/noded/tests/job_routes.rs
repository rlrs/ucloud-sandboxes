//! The job routes (docs/managed-process-reads.md) through the front door,
//! against a fake agent on its Unix socket and a fake node: reads from the
//! supervisor's files, held waits, refusals, forwarding, and start and signal
//! through the agent's growth endpoints.

use std::collections::VecDeque;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use http_body_util::{BodyExt, Full, combinators::BoxBody};
use hyper::body::Incoming;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use serde_json::{Value, json};
use tokio::net::{TcpListener, TcpStream, UnixListener};
use ucloud_noded::agent_rpc::AgentClient;
use ucloud_noded::jobs::{BoxFuture, ControlFailure, ControlOutput, Incarnation, JobFront, JobLimits, JobNode, Lookup};

type TestBody = BoxBody<Bytes, Infallible>;
const TOKEN: &str = "node-secret";
const DIGEST: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

struct TempDir(PathBuf);

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn temp_dir() -> TempDir {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let path = std::env::temp_dir().join(format!("noded-jobs-{}-{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)));
    std::fs::create_dir_all(&path).unwrap();
    TempDir(path)
}

/// Every request the fake agent saw, and its answer to launches.
#[derive(Default)]
struct AgentState {
    seen: Mutex<Vec<(String, String, Value)>>,
    launch: Mutex<Option<(u16, Value)>>,
}

async fn agent(request: Request<Incoming>, state: Arc<AgentState>) -> Result<Response<TestBody>, Infallible> {
    let (method, path) = (request.method().to_string(), request.uri().to_string());
    let bytes = request.into_body().collect().await.unwrap().to_bytes();
    let body: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    state.seen.lock().unwrap().push((method, path.clone(), body));
    let (status, answer) = match path.as_str() {
        "/internal/v1/managed/launch" => state.launch.lock().unwrap().clone().unwrap_or((200, json!({"job_id": ""}))),
        "/internal/v1/managed/bind" | "/internal/v1/managed/terminal" => (200, json!({})),
        _ => (299, json!({"agent": path})),
    };
    let mut response = Response::new(Full::new(Bytes::from(answer.to_string())).boxed());
    *response.status_mut() = StatusCode::from_u16(status).unwrap();
    Ok(response)
}

/// A node that owns one incarnation and answers `ctl` exchanges from a script.
#[derive(Default)]
struct FakeNode {
    incarnation: Mutex<Option<Incarnation>>,
    answers: Mutex<VecDeque<ControlOutput>>,
    requests: Mutex<Vec<(Vec<u8>, u64)>>,
}

impl JobNode for FakeNode {
    fn lookup(&self, _sandbox_id: &str) -> Lookup {
        match self.incarnation.lock().unwrap().clone() {
            Some(incarnation) => Lookup::Serve(incarnation),
            None => Lookup::Forward("not owned"),
        }
    }

    fn controls(&self) -> bool {
        true
    }

    fn control(&self, _: &Incarnation, request: Vec<u8>, guest_timeout_ms: u64, _: Duration) -> BoxFuture<Result<ControlOutput, ControlFailure>> {
        self.requests.lock().unwrap().push((request, guest_timeout_ms));
        let answer = self.answers.lock().unwrap().pop_front();
        Box::pin(async move { answer.ok_or(ControlFailure::Forward("no scripted answer")) })
    }
}

struct Harness {
    address: SocketAddr,
    agent: Arc<AgentState>,
    node: Arc<FakeNode>,
    front: Arc<JobFront>,
    bundle: PathBuf,
    _dir: TempDir,
}

impl Harness {
    fn state_dir(&self) -> PathBuf {
        self.bundle.join("rootfs/.ucloud-managed")
    }

    fn write_state(&self, state: &Value) {
        let temporary = self.state_dir().join(".state.tmp");
        std::fs::write(&temporary, format!("{state}\n")).unwrap();
        std::fs::rename(&temporary, self.state_dir().join("state.json")).unwrap();
    }

    fn append(&self, stream: &str, bytes: &[u8]) {
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new().create(true).append(true).open(self.state_dir().join(format!("{stream}.log"))).unwrap();
        file.write_all(bytes).unwrap();
    }

    fn seen(&self, path: &str) -> Vec<Value> {
        self.agent.seen.lock().unwrap().iter().filter(|(_, seen, _)| seen == path).map(|(_, _, body)| body.clone()).collect()
    }
}

fn running(sequence: u64) -> Value {
    json!({"version": 1, "job_id": "job-1", "spec_sha256": DIGEST, "state": "running", "pid": 42,
           "started_at": "2026-10-11T20:00:00.123456789Z", "stdout_bytes": 0, "stderr_bytes": 0,
           "stdout_truncated": false, "stderr_truncated": false, "sequence": sequence})
}

fn exited(stdout_bytes: u64) -> Value {
    json!({"version": 1, "job_id": "job-1", "spec_sha256": DIGEST, "state": "exited",
           "started_at": "2026-10-11T20:00:00Z", "completed_at": "2026-10-11T20:00:01.5Z", "exit_code": 0,
           "stdout_bytes": stdout_bytes, "stderr_bytes": 0, "stdout_truncated": false, "stderr_truncated": false, "sequence": 3})
}

async fn harness_with(limits: JobLimits, mounted: bool) -> Harness {
    let dir = temp_dir();
    let socket = dir.0.join("agent.sock");
    let state = Arc::new(AgentState::default());
    let listener = UnixListener::bind(&socket).unwrap();
    let agent_state = state.clone();
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let state = agent_state.clone();
            tokio::spawn(async move {
                let service = hyper::service::service_fn(move |request| agent(request, state.clone()));
                let _ = hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(stream), service).await;
            });
        }
    });
    let bundle = dir.0.join("bundles/sb-1.sandbox-2");
    std::fs::create_dir_all(bundle.join("rootfs/.ucloud-managed")).unwrap();
    let mut mounts = vec![json!({"destination": "/proc", "type": "proc", "source": "proc", "options": []})];
    if mounted {
        mounts.push(ucloud_noded::oci::managed_state_mount(&bundle).unwrap());
    }
    let config = json!({"annotations": {
        "dev.ucloud-sandboxes.managed-process.uid": "1000",
        "dev.ucloud-sandboxes.managed-process.gid": "1000",
        "dev.ucloud-sandboxes.managed-process.cwd": "/workspace",
    }, "mounts": mounts});
    // As the daemon writes it: ours, 0600.
    std::fs::write(bundle.join("config.json"), config.to_string()).unwrap();
    std::fs::set_permissions(bundle.join("config.json"), std::os::unix::fs::PermissionsExt::from_mode(0o600)).unwrap();
    let node = Arc::new(FakeNode::default());
    *node.incarnation.lock().unwrap() =
        Some(Incarnation { sandbox_id: "sb-1".into(), generation: 2, bundle: bundle.clone(), container_id: "c".repeat(64) });
    let client = Arc::new(AgentClient::new(socket.clone(), TOKEN, "session-1").unwrap());
    let front = JobFront::new(node.clone(), client, TOKEN, limits);
    let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = tcp.local_addr().unwrap();
    let fronts = ucloud_noded::Fronts { create: None, exec: None, jobs: Some(front.clone()), node: None };
    tokio::spawn(ucloud_noded::serve_with(tcp, ucloud_noded::Config::new(socket), fronts, std::future::pending()));
    Harness { address, agent: state, node, front, bundle, _dir: dir }
}

async fn harness() -> Harness {
    harness_with(JobLimits::default(), true).await
}

async fn call(address: SocketAddr, method: &str, path: &str, body: Option<&Value>) -> (StatusCode, hyper::HeaderMap, Value) {
    let stream = TcpStream::connect(address).await.unwrap();
    let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream)).await.unwrap();
    tokio::spawn(connection);
    let bytes = body.map(|body| Bytes::from(body.to_string())).unwrap_or_default();
    let mut request = Request::builder().method(method).uri(path).header("host", "node").header("authorization", format!("Bearer {TOKEN}"));
    if body.is_some() {
        request = request.header("content-length", bytes.len());
    }
    let response = sender.send_request(request.body(Full::new(bytes)).unwrap()).await.unwrap();
    let (status, headers) = (response.status(), response.headers().clone());
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, headers, serde_json::from_slice(&bytes).unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into())))
}

async fn get(address: SocketAddr, path: &str) -> (StatusCode, Value) {
    let (status, _, body) = call(address, "GET", path, None).await;
    (status, body)
}

#[tokio::test]
async fn reads_derive_the_record_from_the_files() {
    let h = harness().await;
    h.write_state(&running(2));
    h.append("stdout", b"hello");
    let (status, body) = get(h.address, "/v1/sandboxes/sb-1/jobs/job-1").await;
    assert_eq!(status, StatusCode::OK);
    let job = &body["job"];
    let keys: Vec<&String> = job.as_object().unwrap().keys().collect();
    assert_eq!(
        keys,
        ["sandbox_id", "sandbox_generation", "job_id", "spec_sha256", "state", "pid", "started_at", "completed_at", "exit_code",
         "signal", "stdout_bytes", "stderr_bytes", "stdout_truncated", "stderr_truncated", "sequence", "updated_at", "cursor"]
    );
    assert_eq!((job["sandbox_id"].as_str(), job["sandbox_generation"].as_u64()), (Some("sb-1"), Some(2)));
    // While running, the byte counts are the logs' sizes.
    assert_eq!((job["stdout_bytes"].as_u64(), job["stderr_bytes"].as_u64()), (Some(5), Some(0)));
    assert_eq!((job["exit_code"].clone(), job["completed_at"].as_str()), (Value::Null, Some("")));
    assert_eq!(job["cursor"], "2.5.0");
    assert!(job["updated_at"].as_str().unwrap().ends_with("+00:00"));

    let (status, body) = get(h.address, "/v1/sandboxes/sb-1/jobs/job-1/logs/stdout?offset=1&limit=3").await;
    assert_eq!(status, StatusCode::OK);
    let keys: Vec<&String> = body.as_object().unwrap().keys().collect();
    assert_eq!(keys, ["stream", "offset", "next_offset", "data", "eof", "job"]);
    assert_eq!((body["offset"].as_u64(), body["next_offset"].as_u64(), body["data"].as_str(), body["eof"].as_bool()), (Some(1), Some(4), Some("ZWxs"), Some(false)));
    let (_, body) = get(h.address, "/v1/sandboxes/sb-1/jobs/job-1/logs/stdout?offset=5").await;
    assert_eq!((body["data"].as_str(), body["eof"].as_bool()), (Some(""), Some(true)));
    // A log the supervisor never opened: empty, at the offset, eof.
    let (_, body) = get(h.address, "/v1/sandboxes/sb-1/jobs/job-1/logs/stderr?offset=7").await;
    assert_eq!((body["next_offset"].as_u64(), body["eof"].as_bool(), body["stream"].as_str()), (Some(7), Some(true), Some("stderr")));

    // Python's answers for the supervisor's and the handler's refusals.
    assert_eq!(get(h.address, "/v1/sandboxes/sb-1/jobs/job-1/logs/stdout?offset=6").await, (StatusCode::CONFLICT, json!({"error": "log offset exceeds retained data"})));
    assert_eq!(get(h.address, "/v1/sandboxes/sb-1/jobs/job-2").await, (StatusCode::CONFLICT, json!({"error": "primary process not found"})));
    assert_eq!(
        get(h.address, "/v1/sandboxes/sb-1/jobs/job-1/logs/stdout?offset=-1").await,
        (StatusCode::BAD_REQUEST, json!({"error": "managed process log offset cannot be negative"}))
    );
    assert_eq!(
        get(h.address, "/v1/sandboxes/sb-1/jobs/job-1/logs/stdout?limit=0").await,
        (StatusCode::BAD_REQUEST, json!({"error": "managed process log limit must be in [1, 1048576]"}))
    );
    // What Python parses differently is Python's.
    let (status, body) = get(h.address, "/v1/sandboxes/sb-1/jobs/job-1/logs/stdout?offset=x").await;
    assert_eq!((status.as_u16(), body["agent"].as_str()), (299, Some("/v1/sandboxes/sb-1/jobs/job-1/logs/stdout?offset=x")));

    // Once terminal, the persisted counts.
    h.write_state(&exited(3));
    let (_, body) = get(h.address, "/v1/sandboxes/sb-1/jobs/job-1").await;
    assert_eq!((body["job"]["stdout_bytes"].as_u64(), body["job"]["state"].as_str(), body["job"]["exit_code"].as_i64()), (Some(3), Some("exited"), Some(0)));
    // Reads never dispatch a ctl, so a paused sandbox is served frozen, as is.
    assert!(h.node.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn a_wait_wakes_on_a_change_and_a_timeout_returns_the_record_unchanged() {
    let h = harness().await;
    h.write_state(&running(2));
    let address = h.address;
    let started = Instant::now();
    let (status, body) = get(address, "/v1/sandboxes/sb-1/jobs/job-1?wait_ms=300&after=2.0.0").await;
    assert_eq!((status, body["job"]["cursor"].as_str()), (StatusCode::OK, Some("2.0.0")));
    assert!(started.elapsed() >= Duration::from_millis(300));
    // Without `after` anything differs: at once.
    let started = Instant::now();
    get(address, "/v1/sandboxes/sb-1/jobs/job-1?wait_ms=20000").await;
    assert!(started.elapsed() < Duration::from_secs(2));

    let waiting = tokio::spawn(async move { get(address, "/v1/sandboxes/sb-1/jobs/job-1?wait_ms=20000&after=2.0.0").await });
    let logs = tokio::spawn(async move { get(address, "/v1/sandboxes/sb-1/jobs/job-1/logs/stderr?offset=0&wait_ms=20000").await });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!waiting.is_finished() && !logs.is_finished());
    assert_eq!(h.front.waiters(), 2);
    let started = Instant::now();
    h.append("stderr", b"oops\n");
    let (_, body) = waiting.await.unwrap();
    assert_eq!(body["job"]["cursor"], "2.0.5");
    let (_, body) = logs.await.unwrap();
    assert_eq!((body["data"].as_str(), body["job"]["stderr_bytes"].as_u64()), (Some("b29wcwo="), Some(5)));
    assert!(started.elapsed() < Duration::from_secs(5));
    assert_eq!(h.front.waiters(), 0);

    // A terminal record answers at once, whatever `after` says.
    h.write_state(&exited(0));
    let started = Instant::now();
    let (_, body) = get(address, "/v1/sandboxes/sb-1/jobs/job-1/logs/stdout?offset=0&wait_ms=20000").await;
    assert_eq!((body["eof"].as_bool(), body["job"]["state"].as_str()), (Some(true), Some("exited")));
    assert!(started.elapsed() < Duration::from_secs(2));
    assert_eq!(get(address, "/v1/sandboxes/sb-1/jobs/job-1?wait_ms=x").await.0, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn a_wait_whose_directory_goes_returns_the_last_record() {
    let h = harness().await;
    h.write_state(&running(4));
    let address = h.address;
    let waiting = tokio::spawn(async move { get(address, "/v1/sandboxes/sb-1/jobs/job-1?wait_ms=20000&after=4.0.0").await });
    tokio::time::sleep(Duration::from_millis(200)).await;
    let started = Instant::now();
    // A park seals and unmounts; a delete removes: the directory is gone.
    std::fs::remove_dir_all(h.state_dir()).unwrap();
    let (status, body) = waiting.await.unwrap();
    assert_eq!((status, body["job"]["cursor"].as_str()), (StatusCode::OK, Some("4.0.0")));
    assert!(started.elapsed() < Duration::from_secs(5));
    // Without a directory, the agent answers (a parked sandbox's cache).
    let (status, body) = get(address, "/v1/sandboxes/sb-1/jobs/job-1").await;
    assert_eq!((status.as_u16(), body["agent"].as_str()), (299, Some("/v1/sandboxes/sb-1/jobs/job-1")));
}

#[tokio::test]
async fn symlinks_fifos_and_oversize_state_are_refused() {
    let h = harness().await;
    let invalid = |body: &Value| body["error_code"] == "managed_process_state_invalid" && body["retryable"] == false;
    std::fs::write(h.bundle.join("secret"), running(2).to_string()).unwrap();
    std::os::unix::fs::symlink(h.bundle.join("secret"), h.state_dir().join("state.json")).unwrap();
    let (status, body) = get(h.address, "/v1/sandboxes/sb-1/jobs/job-1").await;
    assert!(status == StatusCode::CONFLICT && invalid(&body), "{body}");
    std::fs::remove_file(h.state_dir().join("state.json")).unwrap();

    h.write_state(&running(2));
    let fifo = std::ffi::CString::new(h.state_dir().join("stdout.log").to_str().unwrap()).unwrap();
    assert_eq!(unsafe { libc::mkfifo(fifo.as_ptr(), 0o600) }, 0);
    let started = Instant::now();
    let (status, body) = get(h.address, "/v1/sandboxes/sb-1/jobs/job-1").await;
    assert!(status == StatusCode::CONFLICT && invalid(&body), "{body}");
    assert!(started.elapsed() < Duration::from_secs(2), "a FIFO must not block the open");
    std::fs::remove_file(h.state_dir().join("stdout.log")).unwrap();

    let mut oversize = running(2);
    oversize["started_at"] = json!(format!("2026-10-11T20:00:00Z{}", " ".repeat(70_000)));
    std::fs::write(h.state_dir().join("state.json"), oversize.to_string()).unwrap();
    let (status, body) = get(h.address, "/v1/sandboxes/sb-1/jobs/job-1").await;
    assert!(status == StatusCode::CONFLICT && invalid(&body), "{body}");
    std::fs::write(h.state_dir().join("state.json"), b"{\"version\":2}").unwrap();
    let (status, body) = get(h.address, "/v1/sandboxes/sb-1/jobs/job-1").await;
    assert!(status == StatusCode::CONFLICT && invalid(&body), "{body}");

    // A symlinked state directory is refused too.
    std::fs::remove_dir_all(h.state_dir()).unwrap();
    std::fs::create_dir(h.bundle.join("elsewhere")).unwrap();
    std::os::unix::fs::symlink(h.bundle.join("elsewhere"), h.state_dir()).unwrap();
    let (status, body) = get(h.address, "/v1/sandboxes/sb-1/jobs/job-1/logs/stdout").await;
    assert!(status == StatusCode::CONFLICT && invalid(&body), "{body}");
}

#[tokio::test]
async fn legacy_unowned_and_unstarted_sandboxes_are_the_agents() {
    let legacy = harness_with(JobLimits::default(), false).await;
    legacy.write_state(&running(2));
    for path in ["/v1/sandboxes/sb-1/jobs/job-1?wait_ms=5000", "/v1/sandboxes/sb-1/jobs/job-1/logs/stdout?offset=0"] {
        let (status, body) = get(legacy.address, path).await;
        assert_eq!((status.as_u16(), body["agent"].as_str()), (299, Some(path)));
    }
    let (status, _, body) = call(legacy.address, "POST", "/v1/sandboxes/sb-1/jobs/job-1/signal", Some(&json!({"signal": 15}))).await;
    assert_eq!((status.as_u16(), body["agent"].as_str()), (299, Some("/v1/sandboxes/sb-1/jobs/job-1/signal")));
    // The forwarded body is the client's, byte for byte.
    assert_eq!(legacy.seen("/v1/sandboxes/sb-1/jobs/job-1/signal"), [json!({"signal": 15})]);

    let h = harness().await;
    // No state.json yet: the supervisor's own answer, through the agent.
    assert_eq!(get(h.address, "/v1/sandboxes/sb-1/jobs/job-1").await.0.as_u16(), 299);
    *h.node.incarnation.lock().unwrap() = None;
    h.write_state(&running(2));
    assert_eq!(get(h.address, "/v1/sandboxes/sb-1/jobs/job-1").await.0.as_u16(), 299);
    // Unauthorized and escaped requests are the agent's to refuse.
    let (status, _) = get(h.address, "/v1/sandboxes/sb-1/jobs/job%2D1").await;
    assert_eq!(status.as_u16(), 299);
}

#[tokio::test]
async fn a_terminal_record_is_reported_to_the_agent_once() {
    let h = harness().await;
    h.write_state(&running(2));
    get(h.address, "/v1/sandboxes/sb-1/jobs/job-1").await;
    h.write_state(&exited(0));
    for path in ["/v1/sandboxes/sb-1/jobs/job-1", "/v1/sandboxes/sb-1/jobs/job-1/logs/stdout", "/v1/sandboxes/sb-1/jobs/job-1?wait_ms=1000"] {
        assert_eq!(get(h.address, path).await.0, StatusCode::OK);
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(h.seen("/internal/v1/managed/terminal"), [json!({"sandbox_id": "sb-1", "generation": 2, "job_id": "job-1"})]);
}

fn control_answer(job: Value) -> ControlOutput {
    ControlOutput { exit_code: Some(0), stdout: json!({"version": 1, "ok": true, "job": job}).to_string().into_bytes(), stderr: vec![] }
}

fn ctl_running() -> Value {
    json!({"version": 1, "job_id": "job-1", "spec_sha256": DIGEST, "state": "running", "pid": 7,
           "started_at": "2026-10-11T20:00:00Z", "stdout_bytes": 0, "stderr_bytes": 0, "stdout_truncated": false,
           "stderr_truncated": false, "sequence": 2})
}

#[tokio::test]
async fn a_start_commits_its_launch_dispatches_and_binds() {
    let h = harness().await;
    // The supervisor's socket is not up yet, then it answers.
    h.node.answers.lock().unwrap().extend([
        ControlOutput { exit_code: Some(1), stdout: br#"{"ok":false,"error":"dial unix: no such file","error_code":"control_not_connected"}"#.to_vec(), stderr: vec![] },
        control_answer(ctl_running()),
    ]);
    let start = json!({"job_id": "job-1", "argv": ["python3", "-c", "print('hi')"]});
    let (status, _, body) = call(h.address, "POST", "/v1/sandboxes/sb-1/jobs", Some(&start)).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!((body["job"]["pid"].as_u64(), body["job"]["cursor"].as_str(), body["job"]["sandbox_generation"].as_u64()), (Some(7), Some("2.0.0"), Some(2)));
    // The payload and identity Python computes (start.rs's golden vector 1).
    let digest = "8a56e1ed0415717117dbe87a7f996dc18c5fae629842d26d18b875a04fe1be9d";
    let identity = json!({"sandbox_id": "sb-1", "generation": 2, "job_id": "job-1", "launch_sha256": digest});
    assert_eq!(h.seen("/internal/v1/managed/launch"), [identity.clone()]);
    assert_eq!(h.seen("/internal/v1/managed/bind"), [identity]);
    let requests = h.node.requests.lock().unwrap().clone();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        String::from_utf8(requests[1].0.clone()).unwrap(),
        r#"{"action":"start","argv":["python3","-c","print('hi')"],"cwd":"/workspace","env":{},"gid":1000,"job_id":"job-1","max_stderr_bytes":16777216,"max_stdout_bytes":16777216,"uid":1000,"version":1}"#
    );
    assert!((1..=10_000).contains(&requests[0].1));

    // An intent that already names its job is not bound again.
    *h.agent.launch.lock().unwrap() = Some((200, json!({"job_id": "job-1"})));
    h.node.answers.lock().unwrap().push_back(control_answer(ctl_running()));
    assert_eq!(call(h.address, "POST", "/v1/sandboxes/sb-1/jobs", Some(&start)).await.0, StatusCode::CREATED);
    assert_eq!(h.seen("/internal/v1/managed/bind").len(), 1);

    // A supervisor refusal is Python's ManagedProcessError.
    h.node.answers.lock().unwrap().push_back(ControlOutput {
        exit_code: Some(1),
        stdout: br#"{"version":1,"ok":false,"error":"sandbox generation already owns another primary process"}"#.to_vec(),
        stderr: vec![],
    });
    let (status, _, body) = call(h.address, "POST", "/v1/sandboxes/sb-1/jobs", Some(&start)).await;
    assert_eq!((status, body), (StatusCode::CONFLICT, json!({"error": "sandbox generation already owns another primary process"})));
}

#[tokio::test]
async fn a_start_relays_admission_refusals_and_validates_like_python() {
    let h = harness().await;
    let start = json!({"job_id": "job-1", "argv": ["sh"]});
    *h.agent.launch.lock().unwrap() = Some((503, json!({"error": "managed growth admission is busy", "error_code": "startup_busy"})));
    let (status, headers, body) = call(h.address, "POST", "/v1/sandboxes/sb-1/jobs", Some(&start)).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body, json!({"error": "managed growth admission is busy", "error_code": "node_startup_busy", "retryable": true}));
    assert_eq!((headers["retry-after"].to_str().unwrap(), headers["x-ucloud-sandbox-retryable"].to_str().unwrap()), ("1", "true"));
    *h.agent.launch.lock().unwrap() =
        Some((409, json!({"error": "sandbox generation already owns another primary process", "error_code": "primary_owned", "job_id": "other"})));
    let (status, _, body) = call(h.address, "POST", "/v1/sandboxes/sb-1/jobs", Some(&start)).await;
    assert_eq!(
        (status, body),
        (StatusCode::CONFLICT, json!({"error": "sandbox generation already owns another primary process", "error_code": "primary_already_owned", "job_id": "other"}))
    );
    // An answer outside the contract (an older agent): the agent runs the start.
    *h.agent.launch.lock().unwrap() = Some((404, json!({"error": "not found"})));
    let (status, _, _) = call(h.address, "POST", "/v1/sandboxes/sb-1/jobs", Some(&start)).await;
    assert_eq!(status.as_u16(), 299);
    assert_eq!(h.seen("/v1/sandboxes/sb-1/jobs"), [start.clone()]);

    let launches = h.seen("/internal/v1/managed/launch").len();
    let (status, _, body) = call(h.address, "POST", "/v1/sandboxes/sb-1/jobs", Some(&json!({"job_id": "j", "argv": []}))).await;
    assert_eq!((status, body), (StatusCode::BAD_REQUEST, json!({"error": "managed process argv must be non-empty and bounded"})));
    assert_eq!(h.seen("/internal/v1/managed/launch").len(), launches, "nothing is admitted for an invalid request");
    assert!(h.node.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn a_signal_is_validated_then_dispatched_once() {
    let h = harness().await;
    for signal in [0, 9, 19, 65] {
        let body = if signal == 0 { json!({"signal": -1}) } else { json!({"signal": signal}) };
        let (status, _, answer) = call(h.address, "POST", "/v1/sandboxes/sb-1/jobs/job-1/signal", Some(&body)).await;
        assert_eq!((status, answer), (StatusCode::CONFLICT, json!({"error": "signal is not supported"})), "{signal}");
    }
    let (status, _, answer) = call(h.address, "POST", "/v1/sandboxes/sb-1/jobs/job-1/signal", Some(&json!([]))).await;
    assert_eq!((status, answer), (StatusCode::BAD_REQUEST, json!({"error": "signal payload must be a JSON object"})));
    assert!(h.node.requests.lock().unwrap().is_empty());

    let mut signaled = ctl_running();
    signaled["state"] = json!("signaled");
    signaled["signal"] = json!(15);
    signaled["exit_code"] = json!(-1);
    h.node.answers.lock().unwrap().push_back(control_answer(signaled));
    let (status, _, answer) = call(h.address, "POST", "/v1/sandboxes/sb-1/jobs/job-1/signal", Some(&json!({}))).await;
    assert_eq!(status, StatusCode::OK, "{answer}");
    assert_eq!((answer["job"]["state"].as_str(), answer["job"]["exit_code"].as_i64()), (Some("signaled"), Some(-1)));
    let requests = h.node.requests.lock().unwrap().clone();
    assert_eq!(String::from_utf8(requests[0].0.clone()).unwrap(), r#"{"action":"signal","job_id":"job-1","signal":15,"version":1}"#);
    // No retry: a not-connected supervisor is Python's 409.
    h.node.answers.lock().unwrap().push_back(ControlOutput {
        exit_code: Some(1),
        stdout: br#"{"ok":false,"error":"dial unix","error_code":"control_not_connected"}"#.to_vec(),
        stderr: vec![],
    });
    let (status, _, answer) = call(h.address, "POST", "/v1/sandboxes/sb-1/jobs/job-1/signal", Some(&json!({"signal": 2}))).await;
    assert_eq!((status, answer), (StatusCode::CONFLICT, json!({"error": "dial unix"})));
    assert_eq!(h.node.requests.lock().unwrap().len(), 2);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(h.seen("/internal/v1/managed/terminal").len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn thousands_of_held_reads_wait_on_one_watch_and_disconnects_free_them() {
    const HELD: usize = 3000;
    let h = harness().await;
    h.write_state(&running(2));
    let address = h.address;
    let mut held = Vec::with_capacity(HELD);
    for index in 0..HELD {
        let path = if index % 2 == 0 { "/v1/sandboxes/sb-1/jobs/job-1?wait_ms=25000&after=2.0.0" } else { "/v1/sandboxes/sb-1/jobs/job-1/logs/stdout?offset=0&wait_ms=25000" };
        held.push(tokio::spawn(async move { get(address, path).await }));
    }
    let deadline = Instant::now() + Duration::from_secs(20);
    while h.front.waiters() < HELD {
        assert!(Instant::now() < deadline, "{} of {HELD} reads held", h.front.waiters());
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let started = Instant::now();
    h.append("stdout", b"x");
    for read in held {
        let (status, body) = read.await.unwrap();
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["job"]["stdout_bytes"], 1);
    }
    assert!(started.elapsed() < Duration::from_secs(10), "{:?}", started.elapsed());
    assert_eq!(h.front.waiters(), 0);

    // A client that goes away frees its waiter.
    let stream = TcpStream::connect(address).await.unwrap();
    let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream)).await.unwrap();
    let connection = tokio::spawn(connection);
    let request = Request::get("/v1/sandboxes/sb-1/jobs/job-1?wait_ms=25000&after=2.1.0")
        .header("host", "node")
        .header("authorization", format!("Bearer {TOKEN}"))
        .body(Full::new(Bytes::new()))
        .unwrap();
    let pending = tokio::spawn(async move { sender.send_request(request).await });
    let deadline = Instant::now() + Duration::from_secs(5);
    while h.front.waiters() < 1 {
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    pending.abort();
    connection.abort();
    let deadline = Instant::now() + Duration::from_secs(5);
    while h.front.waiters() > 0 {
        assert!(Instant::now() < deadline, "a closed connection kept its waiter");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn past_the_waiter_bound_a_wait_is_a_retryable_503() {
    let h = harness_with(JobLimits { max_waiters: 2, ..JobLimits::default() }, true).await;
    h.write_state(&running(2));
    let address = h.address;
    let first = tokio::spawn(async move { get(address, "/v1/sandboxes/sb-1/jobs/job-1?wait_ms=20000&after=2.0.0").await });
    let second = tokio::spawn(async move { get(address, "/v1/sandboxes/sb-1/jobs/job-1?wait_ms=20000&after=2.0.0").await });
    let deadline = Instant::now() + Duration::from_secs(5);
    while h.front.waiters() < 2 {
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let (status, headers, body) = call(address, "GET", "/v1/sandboxes/sb-1/jobs/job-1?wait_ms=20000&after=2.0.0", None).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!((body["error_code"].as_str(), body["retryable"].as_bool()), (Some("node_job_reads_busy"), Some(true)));
    assert_eq!(headers["retry-after"], "1");
    // Reads that do not wait are never refused.
    assert_eq!(get(address, "/v1/sandboxes/sb-1/jobs/job-1").await.0, StatusCode::OK);
    h.append("stdout", b"y");
    assert_eq!(first.await.unwrap().0, StatusCode::OK);
    assert_eq!(second.await.unwrap().0, StatusCode::OK);
}

#[test]
fn the_oci_mount_is_what_the_reads_look_for() {
    let bundle = Path::new("/state/bundles/sb.sandbox-1");
    let mount = ucloud_noded::oci::managed_state_mount(bundle).unwrap();
    assert_eq!(
        mount,
        json!({"destination": "/.ucloud-managed", "type": "bind", "source": "/state/bundles/sb.sandbox-1/rootfs/.ucloud-managed",
               "options": ["rbind", "rw", "nosuid", "nodev", "noexec"]})
    );
    assert!(ucloud_noded::oci::has_managed_state_mount(&json!({"mounts": [mount]}), bundle));
    assert!(!ucloud_noded::oci::has_managed_state_mount(&json!({"mounts": []}), bundle));
    assert!(!ucloud_noded::oci::has_managed_state_mount(&json!({"mounts": [ucloud_noded::oci::managed_state_mount(Path::new("/elsewhere")).unwrap()]}), bundle));
}
