//! The node admission service on the internal socket (docs/admission-port-3c.md
//! §3.3-§3.5, step 3c.2), against a real registry and scripted metrics:
//! recovery, session churn and reassert, the drain gate, cancels, a client
//! that goes away, the publication gate, the snapshot, managed growth, a
//! restart, the in-process port, and `--admission-only`.

use std::collections::HashSet;
use std::os::unix::fs::DirBuilderExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::client::conn::http1::SendRequest;
use hyper::{Method, Request, StatusCode};
use hyper_util::rt::TokioIo;
use serde_json::{Value, json};
use tokio::net::UnixStream;
use ucloud_noded::admission::decide::Metrics;
use ucloud_noded::admission::ledger::Shape;
use ucloud_noded::admission::port::{AdmissionPort, BindError, LaunchError, LocalAdmission};
use ucloud_noded::admission::routes;
use ucloud_noded::admission::service::{AdmissionService, AgentProbe, BoxFuture, MetricsSource, ServiceConfig, Sources};
use ucloud_noded::admission::sources::{LedgerDemandSource, NodeObservations};
use ucloud_noded::internal::{self, AdmissionAccess, Internal, NodeView};
use ucloud_noded::pause_policy::{DemandSource, MemoryBackingCapacity, SystemClock};
use ucloud_noded::registry::{GrowthAction, PlanRequest, Quota, Registry, Rootfs, SandboxSpec};

const TOKEN: &str = "node-secret";
const NODED_SESSION: &str = "00112233445566778899aabbccddeeff";
const AGENT: &str = "agent-one";
const OTHER_AGENT: &str = "agent-two";
const COMPAT: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const GIB: u64 = 1024 * 1024 * 1024;
const MIB: u64 = 1024 * 1024;

struct TempDir(PathBuf);

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn temp_dir() -> TempDir {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let path = std::env::temp_dir().join(format!("noded-admission-{}-{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)));
    let _ = std::fs::remove_dir_all(&path);
    std::fs::DirBuilder::new().recursive(true).mode(0o700).create(&path).unwrap();
    TempDir(path)
}

/// Scripted node metrics.
struct FakeMetrics(Mutex<Option<Metrics>>);

fn roomy() -> Metrics {
    Metrics {
        memory_total_mb: 65536,
        memory_available_mb: 60000,
        swap_total_mb: 0,
        swap_free_mb: 0,
        memory_psi_full_avg10: None,
        memory_backing: Some(MemoryBackingCapacity { total_bytes: Some(64 * GIB), available_bytes: Some(32 * GIB), identity: None }),
    }
}

/// Physical headroom above the 2 GiB floor of `mb` MiB.
fn headroom(mb: u64) -> Metrics {
    Metrics { memory_available_mb: 2048 + mb, ..roomy() }
}

impl FakeMetrics {
    fn set(&self, metrics: Metrics) {
        *self.0.lock().unwrap() = Some(metrics);
    }
}

impl MetricsSource for FakeMetrics {
    fn sample(&self) -> Option<Metrics> {
        self.0.lock().unwrap().clone()
    }
}

struct Probe(AtomicBool);

impl AgentProbe for Probe {
    fn reachable(&self) -> BoxFuture<bool> {
        let reachable = self.0.load(Ordering::SeqCst);
        Box::pin(async move { reachable })
    }
}

struct Served(Arc<AdmissionService>);

impl NodeView for Served {
    fn admission_open(&self) -> Option<bool> {
        Some(self.0.admission_open())
    }

    fn config_sha256(&self) -> Option<String> {
        None
    }

    fn admission(&self) -> AdmissionAccess {
        AdmissionAccess::Ready(self.0.clone())
    }
}

fn config() -> ServiceConfig {
    ServiceConfig {
        startup_slots: 2,
        restore_slots: 1,
        publication_lanes: 2,
        admission_wait_seconds: 2.0,
        capacity: Some(Shape { vcpu: 16.0, memory_mb: 65536, disk_mb: 0 }),
        ram_backing_configured: true,
        recovery_grace: Duration::from_millis(200),
    }
}

struct Node {
    dir: TempDir,
    registry: Arc<Registry>,
    metrics: Arc<FakeMetrics>,
    next_project: AtomicUsize,
}

impl Node {
    fn new() -> Node {
        let dir = temp_dir();
        let registry = Arc::new(Registry::owner(dir.0.join("direct-registry.sqlite"), 1_000_000).unwrap());
        Node { dir, registry, metrics: Arc::new(FakeMetrics(Mutex::new(Some(roomy())))), next_project: AtomicUsize::new(0) }
    }

    /// An owned registration (managed ones are parkable, as they must be).
    fn owned(&self, sandbox_id: &str, generation: i64, managed: bool) {
        let spec = SandboxSpec::from_dict(&json!({
            "id": sandbox_id, "image": format!("registry/image@sha256:{}", "a".repeat(64)),
            "memory_mb": 1024, "disk_mb": 2048, "parkable": managed, "managed_process": managed,
        }))
        .unwrap();
        let request = PlanRequest {
            spec,
            sandbox_generation: generation,
            operation_id: format!("create:{sandbox_id}"),
            runtime_compatibility_sha256: COMPAT.into(),
            split_memory_backing: false,
            initial_claim: None,
        };
        let root = &self.dir.0;
        let project = 200_000 + self.next_project.fetch_add(1, Ordering::Relaxed) as i64;
        let planned = self.registry.plan(request).unwrap();
        let quota = Quota { project_id: project, total_mb: 4096, path: root.join("quota").join(sandbox_id).display().to_string() };
        let ready = self.registry.commit_quota(sandbox_id, planned.revision, &quota).unwrap();
        let rootfs = Rootfs {
            rootfs_sha256: "d".repeat(64),
            container_id: format!("{generation:064x}"),
            bundle: root.join("bundles").join(sandbox_id).display().to_string(),
            memory_directory: format!("{sandbox_id}.{generation}"),
        };
        let ready = self.registry.commit_rootfs(sandbox_id, ready.revision, &format!("sha256:{}", "e".repeat(64)), &rootfs, None).unwrap();
        self.registry.commit_owned(sandbox_id, ready.revision).unwrap();
    }

    fn delete(&self, sandbox_id: &str) {
        let record = self.registry.get(sandbox_id).unwrap().unwrap();
        let deleting = self.registry.begin_delete(sandbox_id, record.revision, None).unwrap();
        self.registry.commit_deleted(sandbox_id, record.sandbox_generation, deleting.revision).unwrap();
    }

    fn service_with(&self, config: ServiceConfig) -> Arc<AdmissionService> {
        let sources = Sources {
            store: self.registry.clone(),
            owners: self.registry.clone(),
            metrics: self.metrics.clone(),
            observations: Arc::new(NodeObservations::new(None, config.ram_backing_configured)),
            clock: Arc::new(SystemClock::new()),
        };
        AdmissionService::open(config, true, sources).unwrap()
    }

    fn service(&self) -> Arc<AdmissionService> {
        self.service_with(config())
    }
}

/// The service on its socket.
struct Served2 {
    socket: PathBuf,
}

fn serve(node: &Node, service: &Arc<AdmissionService>, name: &str) -> Served2 {
    let socket = node.dir.0.join(format!("{name}.sock"));
    let internal = Internal::new(NODED_SESSION, TOKEN, internal::own_uid(), Arc::new(Served(service.clone()))).unwrap();
    let listener = internal::bind(&socket).unwrap();
    tokio::spawn(internal::serve(listener, internal, Duration::from_secs(1), std::future::pending()));
    Served2 { socket }
}

struct Client(SendRequest<Full<Bytes>>, String);

async fn client(served: &Served2, session: &str) -> Client {
    client_at(&served.socket, session).await
}

async fn client_at(socket: &Path, session: &str) -> Client {
    let stream = UnixStream::connect(socket).await.unwrap();
    let (sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream)).await.unwrap();
    tokio::spawn(connection);
    Client(sender, session.to_string())
}

impl Client {
    async fn call(&mut self, method: &str, path: &str, body: Option<Value>) -> (StatusCode, Value) {
        let request = Request::builder()
            .method(method)
            .uri(path)
            .header("host", "localhost")
            .header("authorization", format!("Bearer {TOKEN}"))
            .header("x-ucloud-agent-session", &self.1);
        let bytes = body.map(|body| Bytes::from(body.to_string())).unwrap_or_default();
        let response = self.0.send_request(request.body(Full::new(bytes)).unwrap()).await.unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        (status, serde_json::from_slice(&bytes).unwrap())
    }

    async fn post(&mut self, path: &str, body: Value) -> (StatusCode, Value) {
        self.call("POST", &format!("/internal/v1/{path}"), Some(body)).await
    }

    async fn hello(&mut self, reassert: Option<Value>) -> (StatusCode, Value) {
        let mut body = json!({"agent_session": self.1});
        if let Some(reassert) = reassert {
            body["reassert"] = reassert;
        }
        self.post("admission/hello", body).await
    }

    async fn snapshot(&mut self) -> (StatusCode, Value) {
        self.call("GET", "/internal/v1/admission/snapshot", None).await
    }

    async fn release(&mut self, token: &Value) -> StatusCode {
        self.post(&format!("admission/{}/release", token.as_str().unwrap()), json!({})).await.0
    }
}

fn owner(id: &str, generation: u64) -> Value {
    json!({"sandbox_id": id, "generation": generation})
}

fn startup(id: &str) -> Value {
    json!({"owner": owner(id, 1), "requested": {"vcpu": 1.0, "memory_mb": 512}, "managed": false, "wait_seconds": null})
}

fn restore(id: &str, memory_bytes: Option<u64>, deadline_ms: u64) -> Value {
    json!({
        "owner": owner(id, 1), "requested": {"vcpu": 1.0, "memory_mb": 1024},
        "cost": {"memory_bytes": memory_bytes, "ram_backing_bytes": memory_bytes, "read_bytes": memory_bytes, "provenance": "authenticated-checkpoint-allocation"},
        "deadline_ms": deadline_ms,
    })
}

async fn settle() {
    tokio::time::sleep(Duration::from_millis(100)).await;
}

#[tokio::test]
async fn a_new_service_recovers_at_the_agents_hello_or_after_the_grace() {
    let node = Node::new();
    let service = node.service();
    service.start_recovery(Some(Arc::new(Probe(AtomicBool::new(true)))));
    let served = serve(&node, &service, "recovering");
    let mut agent = client(&served, AGENT).await;
    let (status, body) = agent.snapshot().await;
    assert_eq!((status, body["error_code"].as_str(), body["retryable"].as_bool()), (StatusCode::SERVICE_UNAVAILABLE, Some("admission_recovering"), Some(true)));
    // Before its hello an agent session gets no grant.
    let (status, body) = agent.post("admission/startup", startup("s1")).await;
    assert_eq!((status, body["error_code"].as_str()), (StatusCode::SERVICE_UNAVAILABLE, Some("agent_hello_required")));
    // noded's own grants wait for recovery too: a launch is a safe retry.
    node.owned("m1", 1, true);
    let local = LocalAdmission::new(service.clone());
    assert!(matches!(local.launch("m1", 1, "job-1", &"1".repeat(64)).await, Err(LaunchError::StartupBusy(_))));
    // A reachable agent is waited for past the grace.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(service.recovering());
    let (status, body) = agent.hello(None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!({"noded_session": NODED_SESSION, "admission_open": true, "config_sha256": null}));
    assert!(!service.recovering());
    assert_eq!(agent.snapshot().await.0, StatusCode::OK);
    let (status, body) = agent.post("admission/startup", startup("s1")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["token"].as_str().unwrap().len(), 32);

    // An unreachable agent is presumed down after the grace, claims and all.
    let second = node.service();
    second.start_recovery(Some(Arc::new(Probe(AtomicBool::new(false)))));
    assert!(second.recovering());
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(!second.recovering());
    assert!(second.snapshot().is_ok());
}

#[tokio::test]
async fn a_new_agent_session_drops_the_old_ones_claims_and_waiters() {
    let node = Node::new();
    let service = node.service();
    let served = serve(&node, &service, "churn");
    let mut old = client(&served, AGENT).await;
    old.hello(None).await;
    let first = old.post("admission/startup", startup("s1")).await.1["token"].clone();
    let second = old.post("admission/startup", startup("s2")).await.1["token"].clone();
    assert!(first.is_string() && second.is_string());
    assert_eq!(service.available().0, 0);
    // A third create queues for a slot.
    let mut waiting = client(&served, AGENT).await;
    let queued = tokio::spawn(async move { waiting.post("admission/startup", startup("s3")).await });
    settle().await;
    assert!(!queued.is_finished());

    let mut new = client(&served, OTHER_AGENT).await;
    let (status, _) = new.hello(None).await;
    assert_eq!(status, StatusCode::OK);
    // The old session's claims and slots are gone; its waiter gives up (the
    // freed slot may reach it first, and goes back).
    let (status, body) = tokio::time::timeout(Duration::from_secs(1), queued).await.unwrap().unwrap();
    assert_eq!((status, body["error_code"].as_str()), (StatusCode::SERVICE_UNAVAILABLE, Some("agent_hello_required")));
    assert_eq!((service.held(), service.available().0), (0, 2));
    let (_, snapshot) = new.snapshot().await;
    assert_eq!(snapshot["resource_reservations"], json!([]));
    // The old session can neither release nor admit any more.
    assert_eq!(old.release(&first).await, StatusCode::NOT_FOUND);
    let (status, body) = old.post("admission/startup", startup("s4")).await;
    assert_eq!((status, body["error_code"].as_str()), (StatusCode::SERVICE_UNAVAILABLE, Some("agent_hello_required")));
    // Tokens are bound to their session.
    let mine = new.post("admission/startup", startup("s5")).await.1["token"].clone();
    let mut old_again = client(&served, AGENT).await;
    assert_eq!(old_again.release(&mine).await, StatusCode::NOT_FOUND);
    assert_eq!(new.release(&mine).await, StatusCode::OK);
    assert_eq!(new.release(&mine).await, StatusCode::NOT_FOUND, "a second release is unknown");
}

#[tokio::test]
async fn reasserted_claims_are_held_again_under_their_tokens() {
    let node = Node::new();
    let service = node.service();
    let served = serve(&node, &service, "reassert");
    let mut agent = client(&served, AGENT).await;
    let token = "0123456789abcdef0123456789abcdef";
    let reassert = json!([
        {"owner": owner("s1", 1), "kind": "startup", "requested": {"vcpu": 2.0, "memory_mb": 1024},
         "cost": {"memory_bytes": 1024 * MIB, "provenance": "requested-memory-bound"}, "agent_token": token},
        {"owner": owner("p1", 1), "kind": "capture", "requested": null,
         "cost": {"memory_bytes": null, "provenance": "capture"}, "agent_token": "ffffffffffffffffffffffffffffffff"},
    ]);
    let bad = json!([{"owner": owner("s1", 1), "kind": "startup", "requested": null, "cost": {"memory_bytes": 1}, "agent_token": "short"}]);
    assert_eq!(agent.hello(Some(bad)).await.0, StatusCode::BAD_REQUEST);
    assert!(service.recovering(), "an invalid hello changes nothing");
    let (status, _) = agent.hello(Some(reassert.clone())).await;
    assert_eq!(status, StatusCode::OK);
    assert!(!service.recovering());
    assert_eq!((service.held(), service.available().0), (2, 1));
    let (_, snapshot) = agent.snapshot().await;
    assert_eq!(snapshot["resource_reservations"], json!([{"owner": owner("s1", 1), "vcpu": 2.0, "memory_mb": 1024}]));
    assert_eq!(snapshot["transitions"]["capture"]["active"], 1);
    // The same hello again holds nothing twice.
    agent.hello(Some(reassert)).await;
    assert_eq!(service.held(), 2);
    assert_eq!(agent.release(&json!(token)).await, StatusCode::OK);
    assert_eq!((service.held(), service.available().0), (1, 2));
}

#[tokio::test]
async fn close_ends_queued_admissions_and_keeps_the_admitted_ones_visible() {
    let node = Node::new();
    node.owned("r1", 1, false);
    let service = node.service();
    let served = serve(&node, &service, "close");
    let mut agent = client(&served, AGENT).await;
    agent.hello(None).await;
    agent.post("admission/startup", startup("s1")).await;
    agent.post("admission/startup", startup("s2")).await;
    let mut queued = client(&served, AGENT).await;
    let slot_wait = tokio::spawn(async move { queued.post("admission/startup", startup("s3")).await });
    // A restore waits for memory: 1 GiB against 512 MiB of headroom.
    node.metrics.set(headroom(512));
    let mut memory = client(&served, AGENT).await;
    let memory_wait = tokio::spawn(async move { memory.post("admission/restore", restore("r1", Some(GIB), 5000)).await });
    settle().await;
    assert!(!slot_wait.is_finished() && !memory_wait.is_finished());
    let (_, before) = agent.snapshot().await;

    let started = Instant::now();
    let (status, body) = agent.post("admission/close", json!({"drain_token": "drain:1"})).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["admission_open"], false);
    assert_eq!(body["transient_revision"], before["transient_revision"]);
    let (status, body) = slot_wait.await.unwrap();
    assert_eq!((status, body["error_code"].as_str()), (StatusCode::SERVICE_UNAVAILABLE, Some("node_active_admission_deferred")));
    let (status, body) = memory_wait.await.unwrap();
    assert_eq!((status, body["error_code"].as_str()), (StatusCode::SERVICE_UNAVAILABLE, Some("node_admission_closed")));
    assert!(started.elapsed() < Duration::from_secs(1), "close interrupts waits");
    // What was admitted stays in the snapshot until it is released (I18).
    let (_, snapshot) = agent.snapshot().await;
    assert_eq!((snapshot["admission_open"].as_bool(), snapshot["active_reservations"].as_u64()), (Some(false), Some(2)));
    let (status, body) = agent.post("admission/startup", startup("s4")).await;
    assert_eq!((status, body["error_code"].as_str()), (StatusCode::SERVICE_UNAVAILABLE, Some("node_admission_closed")));
    assert_eq!(agent.post("admission/close", json!({"drain_token": "bad token"})).await.0, StatusCode::BAD_REQUEST);
    let (_, body) = agent.post("admission/open", json!({"drain_token": "drain:1"})).await;
    assert_eq!(body["admission_open"], true);
    node.metrics.set(roomy());
    assert_eq!(agent.post("admission/restore", restore("r1", Some(GIB), 1000)).await.0, StatusCode::OK);
}

#[tokio::test]
async fn a_client_that_goes_away_gives_its_place_and_its_grant_back() {
    let node = Node::new();
    let service = node.service();
    let served = serve(&node, &service, "disconnect");
    let mut agent = client(&served, AGENT).await;
    agent.hello(None).await;
    let first = agent.post("admission/startup", startup("s1")).await.1["token"].clone();
    agent.post("admission/startup", startup("s2")).await;
    // A third create queues, then its client disconnects.
    let mut gone = client(&served, AGENT).await;
    let queued = tokio::spawn(async move { gone.post("admission/startup", startup("s3")).await });
    settle().await;
    queued.abort();
    settle().await;
    // The released slot is not handed to the departed waiter.
    assert_eq!(agent.release(&first).await, StatusCode::OK);
    assert_eq!((service.held(), service.available().0), (1, 1));

    // A grant whose answer never reaches the client is released with it.
    let body = serde_json::to_vec(&startup("s6")).unwrap();
    let answer = routes::handle(&service, NODED_SESSION, AGENT, false, "startup", &Method::POST, &body).await;
    assert_eq!((answer.status, service.held()), (StatusCode::OK, 2));
    drop(answer);
    assert_eq!((service.held(), service.available().0), (1, 1));
}

#[tokio::test]
async fn publications_keep_one_lane_while_a_foreground_transition_waits() {
    let node = Node::new();
    for id in ["r1", "r2"] {
        node.owned(id, 1, false);
    }
    let service = node.service();
    let served = serve(&node, &service, "publication");
    let mut agent = client(&served, AGENT).await;
    agent.hello(None).await;
    let publication = |id: &str| json!({"owner": owner(id, 1)});
    let (status, first) = agent.post("admission/publication", publication("p1")).await;
    assert_eq!(status, StatusCode::OK);
    // The single restore slot is taken; a second restore queues for it.
    let restoring = agent.post("admission/restore", restore("r1", Some(GIB), 1000)).await.1["token"].clone();
    let mut queued = client(&served, AGENT).await;
    let waiting = tokio::spawn(async move { queued.post("admission/restore", restore("r2", Some(GIB), 5000)).await });
    settle().await;
    let (status, body) = agent.post("admission/publication", publication("p2")).await;
    assert_eq!((status, body["error_code"].as_str(), body["retryable"].as_bool()), (StatusCode::CONFLICT, Some("publication_deferred"), Some(true)));
    // Without an active publication, one lane still starts.
    assert_eq!(agent.release(&first["token"]).await, StatusCode::OK);
    let (status, second) = agent.post("admission/publication", publication("p2")).await;
    assert_eq!(status, StatusCode::OK);
    // Once nothing waits, every lane is usable, and no more than that.
    assert_eq!(agent.release(&restoring).await, StatusCode::OK);
    assert_eq!(waiting.await.unwrap().0, StatusCode::OK);
    assert_eq!(agent.post("admission/publication", publication("p3")).await.0, StatusCode::OK);
    assert_eq!(agent.post("admission/publication", publication("p4")).await.1["error_code"], "publication_deferred");
    assert_eq!(agent.release(&second["token"]).await, StatusCode::OK);
}

#[tokio::test]
async fn cancel_wakes_the_owners_queued_waiters() {
    let node = Node::new();
    for id in ["r1", "r2"] {
        node.owned(id, 1, false);
    }
    let service = node.service();
    let served = serve(&node, &service, "cancel");
    let mut agent = client(&served, AGENT).await;
    agent.hello(None).await;
    let restoring = agent.post("admission/restore", restore("r1", Some(GIB), 1000)).await.1["token"].clone();
    let mut queued = client(&served, AGENT).await;
    let started = Instant::now();
    let waiting = tokio::spawn(async move { queued.post("admission/restore", restore("r2", Some(GIB), 10_000)).await });
    settle().await;
    let (status, body) = agent.post("admission/cancel", json!({"owner": owner("r2", 1)})).await;
    assert_eq!((status, body), (StatusCode::OK, json!({"cancelled": 1})));
    let (status, body) = waiting.await.unwrap();
    assert_eq!((status, body["error_code"].as_str()), (StatusCode::SERVICE_UNAVAILABLE, Some("node_restore_busy")));
    assert!(started.elapsed() < Duration::from_secs(2));
    assert_eq!(agent.post("admission/cancel", json!({"owner": owner("r2", 1)})).await.1, json!({"cancelled": 0}));

    // A memory wait (the slot is free) revalidates its owner when woken: a
    // deleted owner ends it as a lost incarnation.
    assert_eq!(agent.release(&restoring).await, StatusCode::OK);
    node.metrics.set(headroom(0));
    let mut memory = client(&served, AGENT).await;
    let started = Instant::now();
    let waiting = tokio::spawn(async move { memory.post("admission/restore", restore("r2", Some(GIB), 10_000)).await });
    settle().await;
    assert!(!waiting.is_finished());
    node.delete("r2");
    agent.post("admission/cancel", json!({"owner": owner("r2", 1)})).await;
    let (status, body) = waiting.await.unwrap();
    assert_eq!((status, body["error_code"].as_str()), (StatusCode::CONFLICT, Some("generation_mismatch")));
    assert!(started.elapsed() < Duration::from_secs(2));
    assert_eq!((service.held(), service.available().1), (0, 1));
}

#[tokio::test]
async fn the_snapshot_composes_reservations_transitions_and_demand() {
    let node = Node::new();
    node.owned("r1", 1, false);
    let service = node.service();
    let served = serve(&node, &service, "snapshot");
    let mut agent = client(&served, AGENT).await;
    agent.hello(None).await;
    let (_, empty) = agent.snapshot().await;
    let base = empty["transient_revision"].as_u64().unwrap();
    assert_eq!(empty["noded_session"], NODED_SESSION);
    let create = json!({"owner": owner("new", 1), "requested": {"vcpu": 2.0, "memory_mb": 1024}, "managed": false, "wait_seconds": 1.0});
    let (status, created) = agent.post("admission/startup", create).await;
    assert_eq!(status, StatusCode::OK);
    assert!(created["phases"]["startup_admission_ms"].as_f64().unwrap() >= 0.0);
    assert!(created["phases"]["active_capacity_ms"].as_f64().unwrap() >= 0.0);
    agent.post("admission/restore", restore("r1", Some(2 * GIB), 1000)).await;
    let capture = json!({"owner": owner("c1", 1), "kind": "capture", "cost": {"memory_bytes": null, "provenance": "capture"}});
    let (status, captured) = agent.post("admission/observe", capture).await;
    assert_eq!(status, StatusCode::OK);
    agent.post("admission/publication", json!({"owner": owner("p1", 1)})).await;
    let (status, snapshot) = agent.snapshot().await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(snapshot["admission_open"], true);
    assert_eq!(snapshot["transient_revision"].as_u64().unwrap(), base + 2, "creates and restores move the clock");
    assert_eq!(snapshot["resource_reservations"], json!([
        {"owner": owner("new", 1), "vcpu": 2.0, "memory_mb": 1024},
        {"owner": owner("r1", 1), "vcpu": 1.0, "memory_mb": 1024},
    ]));
    assert_eq!(snapshot["active_reservations"], 2);
    let row = |active: u64, memory: Value, read: Value| json!({"active": active, "waiting": 0, "memory_bytes": memory, "read_bytes": read, "write_bytes": null});
    assert_eq!(snapshot["transitions"], json!({
        "startup": {"active": 1, "waiting": 0, "memory_bytes": 1024 * MIB, "read_bytes": null, "write_bytes": null},
        "restore": row(1, json!(2 * GIB), json!(2 * GIB)),
        "capture": row(1, Value::Null, Value::Null),
        "publication": row(1, Value::Null, Value::Null),
    }));
    assert_eq!(snapshot["resident_demand"], json!({
        "admitted_demand_bytes": 1024 * MIB + 2 * GIB,
        "pending_demand_bytes": 0,
        "unknown_transition_memory_costs": 2,
        "admitted_ram_backing_bytes": 1024 * MIB + 2 * GIB,
        "pending_ram_backing_bytes": 0,
    }));
    // Observed captures move nothing; releasing a create does.
    assert_eq!(agent.release(&captured["token"]).await, StatusCode::OK);
    assert_eq!(agent.release(&created["token"]).await, StatusCode::OK);
    let (_, after) = agent.snapshot().await;
    assert_eq!(after["transient_revision"].as_u64().unwrap(), base + 3);
    assert_eq!(after["active_reservations"], 1);
    // Routes and bodies are checked.
    assert_eq!(agent.call("POST", "/internal/v1/admission/snapshot", None).await.0, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(agent.post("admission/startup", json!({"owner": owner("x", 1)})).await.0, StatusCode::BAD_REQUEST);
    assert_eq!(agent.post("admission/nothing", json!({})).await.1["error_code"], "admission_route_unknown");
    assert_eq!(agent.post("growth/nothing", json!({})).await.1["error_code"], "admission_route_unknown");
}

#[tokio::test]
async fn growth_events_and_continuations_follow_the_durable_rows() {
    let node = Node::new();
    node.owned("m1", 1, true);
    node.owned("plain", 1, false);
    let service = node.service();
    let served = serve(&node, &service, "growth");
    let mut agent = client(&served, AGENT).await;
    agent.hello(None).await;
    let event = |action: &str, id: &str, extra: Value| {
        let mut item = json!({"action": action, "owner": owner(id, 1)});
        item.as_object_mut().unwrap().extend(extra.as_object().unwrap().clone());
        item
    };
    let (status, body) = agent
        .post("growth/events", json!({"items": [
            event("wait", "m1", json!({"request_id": "req-1"})),
            event("wait", "plain", json!({"request_id": "req-1"})),
            event("wait", "missing", json!({"request_id": "req-1"})),
        ]}))
        .await;
    assert_eq!(status, StatusCode::OK);
    let results = body["results"].as_array().unwrap();
    assert_eq!(results[0]["intent"]["phase"], "safe");
    assert_eq!(results[0]["intent"]["memory_bytes"], 1024 * MIB);
    assert_eq!(results[1], json!({"intent": null}), "not managed: skipped");
    assert_eq!(results[2]["error_code"], "sandbox_unavailable");
    assert_eq!(service.forecast(&("m1".into(), 1)), None, "a safe wait charges nothing");

    // A continuation of the waiting request is not covered: it is admitted
    // against memory, and its activation commits the request's wake fence.
    let continuation = |request: &str, deadline_ms: u64| json!({"owner": owner("m1", 1), "request_id": request, "deadline_ms": deadline_ms});
    node.metrics.set(headroom(512));
    let started = Instant::now();
    let (status, body) = agent.post("growth/continuation", continuation("req-1", 300)).await;
    assert_eq!((status, body["error_code"].as_str()), (StatusCode::SERVICE_UNAVAILABLE, Some("node_startup_busy")));
    assert!(started.elapsed() >= Duration::from_millis(200), "it waited for headroom");
    assert!(!node.registry.relay_wake_fence("m1", 1, "req-1", false).unwrap());
    assert_eq!(service.intent(&("m1".into(), 1)).unwrap().phase, "safe", "nothing was granted");
    node.metrics.set(roomy());
    let (status, body) = agent.post("growth/continuation", continuation("req-1", 1000)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!((body["intent"]["phase"].as_str(), body["intent"]["request_id"].as_str()), (Some("active"), Some("req-1")));
    assert!(node.registry.relay_wake_fence("m1", 1, "req-1", false).unwrap(), "the fence committed with the grant");
    // No sample yet: the whole bound is charged, on RAM backing too.
    let forecast = service.forecast(&("m1".into(), 1)).unwrap();
    assert_eq!((forecast.memory_bytes, forecast.ram_backing_bytes), (Some(1024 * MIB), Some(1024 * MIB)));

    // Covered: an active primary needs no admission, even with no headroom,
    // and a new request's fence is recorded.
    node.metrics.set(headroom(0));
    let (status, body) = agent.post("growth/continuation", continuation("req-2", 0)).await;
    assert_eq!((status, body["intent"]["phase"].as_str()), (StatusCode::OK, Some("active")));
    assert!(node.registry.relay_wake_fence("m1", 1, "req-2", false).unwrap());
    // A later safe wait cannot take over a fenced request.
    let (_, body) = agent.post("growth/events", json!({"items": [event("wait", "m1", json!({"request_id": "req-2"}))]})).await;
    assert_eq!(body["results"][0]["error_code"], "growth_conflict");
    // Not managed: nothing to admit. Another generation: a conflict.
    assert_eq!(agent.post("growth/continuation", json!({"owner": owner("plain", 1), "request_id": "r", "deadline_ms": 0})).await.1, json!({"intent": null}));
    let (status, body) = agent.post("growth/continuation", json!({"owner": owner("m1", 2), "request_id": "r", "deadline_ms": 0})).await;
    assert_eq!((status, body["error_code"].as_str()), (StatusCode::CONFLICT, Some("generation_mismatch")));

    // Park, terminal and forget.
    let (_, body) = agent.post("growth/events", json!({"items": [event("park", "m1", json!({}))]})).await;
    assert_eq!(body["results"][0]["intent"]["phase"], "parked");
    assert_eq!(service.forecast(&("m1".into(), 1)), None);
    let (_, body) = agent.post("growth/events", json!({"items": [event("terminal", "m1", json!({"job_id": "job-1"}))]})).await;
    assert_eq!(body["results"][0]["intent"]["phase"], "terminal");
    assert_eq!(agent.post("growth/events", json!({"items": [event("terminal", "m1", json!({}))]})).await.0, StatusCode::BAD_REQUEST);
    let (_, body) = agent.post("growth/events", json!({"items": [event("forget", "m1", json!({}))]})).await;
    assert_eq!(body["results"][0], json!({"intent": null}));
    assert_eq!(service.intent(&("m1".into(), 1)), None);
    assert_eq!(node.registry.growth_intents().unwrap().len(), 1, "forget is in memory only");
}

#[tokio::test]
async fn the_agents_own_launches_and_binds_use_the_same_ledger() {
    let node = Node::new();
    node.owned("m1", 1, true);
    node.owned("m2", 1, true);
    node.owned("plain", 1, false);
    let service = node.service();
    let served = serve(&node, &service, "launch");
    let mut agent = client(&served, AGENT).await;
    agent.hello(None).await;
    let digest = "1".repeat(64);
    let launch = |id: &str, job: &str, digest: &str, deadline_ms: u64| {
        json!({"owner": owner(id, 1), "job_id": job, "launch_sha256": digest, "deadline_ms": deadline_ms})
    };
    // Queued, admitted against memory, active; a replay is idempotent.
    let (status, body) = agent.post("growth/launch", launch("m1", "job-1", &digest, 1000)).await;
    assert_eq!((status, body), (StatusCode::OK, json!({"job_id": "job-1"})));
    assert_eq!(service.intent(&("m1".into(), 1)).unwrap().phase, "active");
    assert_eq!(service.forecast(&("m1".into(), 1)).unwrap().memory_bytes, Some(1024 * MIB));
    assert_eq!(agent.post("growth/launch", launch("m1", "job-1", &digest, 0)).await.1, json!({"job_id": "job-1"}));
    // Another launch identity is refused with the owner's job.
    let (status, body) = agent.post("growth/launch", launch("m1", "job-2", &"2".repeat(64), 0)).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!((body["error_code"].as_str(), body["job_id"].as_str()), (Some("primary_already_owned"), Some("job-1")));
    // Without headroom a queued launch is a safe retry and stays queued.
    node.metrics.set(headroom(0));
    let (status, body) = agent.post("growth/launch", launch("m2", "job-1", &digest, 300)).await;
    assert_eq!((status, body["error_code"].as_str()), (StatusCode::SERVICE_UNAVAILABLE, Some("node_startup_busy")));
    assert_eq!(service.intent(&("m2".into(), 1)).unwrap().phase, "queued");
    assert_eq!(service.forecast(&("m2".into(), 1)), None, "no provisional charge is left behind");
    // Closed admission refuses it as closed; not managed and lost incarnations conflict.
    agent.post("admission/close", json!({"drain_token": "drain:1"})).await;
    let (status, body) = agent.post("growth/launch", launch("m2", "job-1", &digest, 300)).await;
    assert_eq!((status, body["error_code"].as_str()), (StatusCode::SERVICE_UNAVAILABLE, Some("node_admission_closed")));
    agent.post("admission/open", json!({"drain_token": "drain:1"})).await;
    assert_eq!(agent.post("growth/launch", launch("plain", "job-1", &digest, 0)).await.1["error_code"], "not_managed_process");
    let (status, body) = agent.post("growth/launch", json!({"owner": owner("m1", 2), "job_id": "job-1", "launch_sha256": digest, "deadline_ms": 0})).await;
    assert_eq!((status, body["error_code"].as_str()), (StatusCode::CONFLICT, Some("generation_mismatch")));
    assert_eq!(agent.post("growth/launch", launch("m1", "-bad", &digest, 0)).await.0, StatusCode::BAD_REQUEST);
    assert_eq!(agent.post("growth/launch", launch("m1", "job-1", "ABC", 0)).await.0, StatusCode::BAD_REQUEST);

    // An imported primary (active, no identity) launches as itself; the
    // agent binds the supervisor's answer with a bind event.
    node.metrics.set(roomy());
    node.owned("imported", 1, true);
    agent.post("growth/continuation", json!({"owner": owner("imported", 1), "request_id": "req-1", "deadline_ms": 1000})).await;
    assert_eq!(agent.post("growth/launch", launch("imported", "job-9", &digest, 0)).await.1, json!({"job_id": ""}));
    let bind = |job: &str, digest: &str| json!({"items": [{"action": "bind", "owner": owner("imported", 1), "job_id": job, "launch_sha256": digest}]});
    let (_, body) = agent.post("growth/events", bind("job-9", &digest)).await;
    assert_eq!((body["results"][0]["intent"]["job_id"].as_str(), body["results"][0]["intent"]["phase"].as_str()), (Some("job-9"), Some("active")));
    let (_, body) = agent.post("growth/events", bind("job-8", &"8".repeat(64))).await;
    assert_eq!(body["results"][0]["error_code"], "primary_already_owned");
    let missing = json!({"items": [{"action": "bind", "owner": owner("imported", 1), "job_id": "job-9"}]});
    assert_eq!(agent.post("growth/events", missing).await.0, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn the_snapshot_reports_each_pools_slots_and_waiters() {
    let node = Node::new();
    let service = node.service();
    let served = serve(&node, &service, "slots");
    let mut agent = client(&served, AGENT).await;
    agent.hello(None).await;
    let (_, snapshot) = agent.snapshot().await;
    assert_eq!(snapshot["slots"], json!({
        "startup": {"capacity": 2, "available": 2, "waiting": 0},
        "restore": {"capacity": 1, "available": 1, "waiting": 0},
        "publication": {"capacity": 2, "available": 2, "waiting": 0},
    }));
    agent.post("admission/startup", startup("s1")).await;
    agent.post("admission/startup", startup("s2")).await;
    let mut queued = client(&served, AGENT).await;
    let waiter = tokio::spawn(async move { queued.post("admission/startup", startup("s3")).await });
    settle().await;
    let (_, snapshot) = agent.snapshot().await;
    assert_eq!(snapshot["slots"]["startup"], json!({"capacity": 2, "available": 0, "waiting": 1}));
    agent.post("admission/cancel", json!({"owner": owner("s3", 1)})).await;
    assert_eq!(waiter.await.unwrap().1["error_code"], "node_active_admission_deferred");
    let (_, snapshot) = agent.snapshot().await;
    assert_eq!(snapshot["slots"]["startup"]["waiting"], 0);
}

#[tokio::test]
async fn local_waits_and_the_reclaim_demand_read_the_same_ledger() {
    let node = Node::new();
    node.owned("m1", 1, true);
    node.owned("r1", 1, false);
    node.owned("r2", 1, false);
    let service = node.service();
    service.hello(AGENT, Vec::new());
    let demand = LedgerDemandSource { service: service.clone(), admission_open: Box::new(|| true) };
    assert_eq!(demand.demand().physical_bytes, 0);
    // An activated continuation is a forecast; a queued restore is the next demand.
    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(ucloud_noded::admission::sources::apply_local_growth(service.clone(), receiver));
    for action in [ucloud_noded::local_wait::GrowthAction::Wait, ucloud_noded::local_wait::GrowthAction::Activate] {
        sender.send(ucloud_noded::local_wait::GrowthEvent { action, sandbox_id: "m1".into(), generation: 1, request_id: "req-1".into() }).unwrap();
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(service.intent(&("m1".into(), 1)).unwrap().phase, "active");
    assert_eq!(demand.demand().physical_bytes, 1024 * MIB);
    let holder = ucloud_noded::admission::service::Holder::Agent(AGENT.into());
    let cost = |bytes| ucloud_noded::admission::ledger::Cost {
        ram_backing_bytes: Some(bytes),
        ..ucloud_noded::admission::ledger::Cost::new(ucloud_noded::admission::ledger::Kind::Restore, Some(bytes))
    };
    let shape = Shape { vcpu: 1.0, memory_mb: 1024, disk_mb: 0 };
    let held = service.restore(&holder, &("r1".into(), 1), shape.clone(), cost(GIB), 1.0).await.unwrap();
    let waiting = {
        let service = service.clone();
        let (holder, shape, cost) = (holder.clone(), shape.clone(), cost(2 * GIB));
        tokio::spawn(async move { service.restore(&holder, &("r2".into(), 1), shape, cost, 5.0).await })
    };
    settle().await;
    // Admitted bytes, plus the next owner only once it is eligible (I7): a
    // restore queued behind the only slot bills nothing yet.
    assert_eq!(demand.demand().physical_bytes, 1024 * MIB + GIB);
    node.metrics.set(headroom(0));
    assert!(service.release(&held, &holder));
    settle().await;
    assert!(!waiting.is_finished(), "it now waits for memory");
    assert_eq!(demand.demand().physical_bytes, 1024 * MIB + 2 * GIB);
    service.close();
    assert_eq!(demand.demand().physical_bytes, 0, "closed admission bills nothing");
    assert!(matches!(waiting.await.unwrap(), Err(ucloud_noded::admission::service::Refusal::Closed(_))));
    let closed_row = LedgerDemandSource { service: service.clone(), admission_open: Box::new(|| false) };
    service.open_admission();
    assert_eq!(closed_row.demand().physical_bytes, 0, "the drain row closes it too");
    assert_eq!(demand.demand().physical_bytes, 1024 * MIB);
}

#[tokio::test]
async fn noded_launches_bind_and_terminal_in_process() {
    let node = Node::new();
    node.owned("m1", 1, true);
    node.owned("plain", 1, false);
    let service = node.service();
    service.hello(AGENT, Vec::new());
    let local = LocalAdmission::new(service.clone());
    let digest = "1".repeat(64);
    // The first launch queues, is admitted against memory, and is active.
    assert_eq!(local.launch("m1", 1, "job-1", &digest).await, Ok("job-1".into()));
    let intent = service.intent(&("m1".into(), 1)).unwrap();
    assert_eq!((intent.phase.as_str(), intent.job_id.as_str()), ("active", "job-1"));
    assert_eq!(service.forecast(&("m1".into(), 1)).unwrap().memory_bytes, Some(1024 * MIB));
    // A retry of the same launch is idempotent; another one is refused.
    assert_eq!(local.launch("m1", 1, "job-1", &digest).await, Ok("job-1".into()));
    assert_eq!(
        local.launch("m1", 1, "job-2", &"2".repeat(64)).await,
        Err(LaunchError::PrimaryOwned { message: "sandbox generation already owns another primary process".into(), job_id: "job-1".into() })
    );
    assert_eq!(local.launch("plain", 1, "job-1", &digest).await, Err(LaunchError::Forward("launch declined")));
    assert_eq!(local.launch("m1", 2, "job-1", &digest).await, Err(LaunchError::Forward("launch declined")));
    assert!(matches!(local.bind("m1", 1, "job-2", &"2".repeat(64)).await, Err(BindError::PrimaryOwned { .. })));
    assert_eq!(local.terminal("m1", 1, "job-1").await, Ok(()));
    assert_eq!(service.intent(&("m1".into(), 1)).unwrap().phase, "terminal");

    // Without headroom a queued launch is a safe retry, and stays queued.
    node.owned("m2", 1, true);
    node.metrics.set(headroom(0));
    let quick = node.service_with(ServiceConfig { admission_wait_seconds: 0.3, ..config() });
    quick.hello(AGENT, Vec::new());
    let local = LocalAdmission::new(quick.clone());
    assert!(matches!(local.launch("m2", 1, "job-1", &digest).await, Err(LaunchError::StartupBusy(_))));
    assert_eq!(node.registry.growth_intents().unwrap().iter().find(|row| row.sandbox_id == "m2").unwrap().phase, "queued");
    assert_eq!(quick.forecast(&("m2".into(), 1)), None, "no provisional charge is left behind");
}

#[tokio::test]
async fn a_restarted_service_reloads_its_durable_intents() {
    let node = Node::new();
    node.owned("m1", 1, true);
    node.owned("m2", 1, true);
    let service = node.service();
    service.hello(AGENT, Vec::new());
    for id in ["m1", "m2"] {
        let owner = (id.to_string(), 1);
        let step = ucloud_noded::admission::growth::Step::new(&owner, GrowthAction::Activate).request("req-1");
        service.transition(step).await.unwrap();
    }
    drop(service);
    node.delete("m2");
    let restarted = node.service();
    let intent = restarted.intent(&("m1".into(), 1)).unwrap();
    assert_eq!((intent.phase.as_str(), intent.request_id.as_str()), ("active", "req-1"));
    assert_eq!(restarted.forecast(&("m1".into(), 1)).unwrap().memory_bytes, Some(1024 * MIB), "whole bound until a sample");
    assert_eq!(restarted.intent(&("m2".into(), 1)), None, "deleted with its registration");
    assert!(restarted.recovering());
    drop(restarted);

    // The same registry under `ucloud-noded --admission-only`.
    let path = node.registry.path().to_path_buf();
    node.registry.close();
    let token_file = node.dir.0.join("token");
    std::fs::write(&token_file, TOKEN).unwrap();
    // The agent's block, with a RAM backing root: forecasts are RAM's.
    let config_file = node.dir.0.join("admission.json");
    let block = json!({
        "rust_admission_enabled": true, "max_concurrent_startups": 2, "max_concurrent_restores": 1,
        "admission_wait_seconds": 1.0, "active_capacity": null, "physical_memory_floor_mb": 2048,
        "growth_observation_max_age_seconds": 30.0, "prefetch_max_bytes": GIB, "publication_lanes": 16,
        "retry_seconds": 0.21, "application_memory_root": "/dev/shm/ucloud-memory",
    });
    std::fs::write(&config_file, block.to_string()).unwrap();
    let socket = node.dir.0.join("standalone").join("noded.sock");
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_ucloud-noded"))
        .args(["--admission-only", "--internal-unix"])
        .arg(&socket)
        .arg("--registry")
        .arg(&path)
        .arg("--node-control-token-file")
        .arg(&token_file)
        .arg("--admission-config")
        .arg(&config_file)
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while UnixStream::connect(&socket).await.is_err() {
        assert!(Instant::now() < deadline, "the standalone service did not start");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let mut agent = client_at(&socket, AGENT).await;
    assert_eq!(agent.snapshot().await.1["error_code"], "admission_recovering");
    let (status, hello) = agent.hello(None).await;
    assert_eq!((status, hello["admission_open"].as_bool()), (StatusCode::OK, Some(true)));
    let (_, snapshot) = agent.snapshot().await;
    assert_eq!(snapshot["resident_demand"]["admitted_demand_bytes"], 1024 * MIB);
    // Slots only (no node shape): a create is admitted at once.
    assert_eq!(agent.post("admission/startup", startup("s1")).await.0, StatusCode::OK);
    child.start_kill().unwrap();
    let _ = child.wait().await;
    let keys: HashSet<String> = Registry::owner(&path, 0).unwrap().growth_intents().unwrap().into_iter().map(|row| row.sandbox_id).collect();
    assert_eq!(keys, HashSet::from(["m1".to_string()]));
}
