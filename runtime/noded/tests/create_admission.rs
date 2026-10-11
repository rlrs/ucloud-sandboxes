//! Step 3c.4 (docs/admission-port-3c.md §3.3): where the node runs its own
//! admission, the create front takes the startup slot and capacity claim in
//! process, hands the agent its token (`"admitted"`), and releases the claim
//! only after finish returns (I18). Refusals answer as the agent's create
//! does, byte for byte.

use std::convert::Infallible;
use std::os::unix::fs::DirBuilderExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full, combinators::BoxBody};
use hyper::body::Incoming;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use serde_json::{Map, Value, json};
use tokio::net::{TcpListener, TcpStream, UnixListener};
use ucloud_noded::admission::decide::Metrics;
use ucloud_noded::admission::ledger::Shape;
use ucloud_noded::admission::service::{AdmissionService, Holder, MetricsSource, ServiceConfig, Sources};
use ucloud_noded::admission::sources::NodeObservations;
use ucloud_noded::agent_rpc::AgentClient;
use ucloud_noded::create::{Admitted, CreateError, CreateFront, Pipeline};
use ucloud_noded::pause_policy::SystemClock;
use ucloud_noded::registry::Registry;
use ucloud_noded::timings::Timings;

type TestBody = BoxBody<Bytes, Infallible>;
const TOKEN: &str = "node-secret";

struct TempDir(PathBuf);

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn temp_dir() -> TempDir {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let path = std::env::temp_dir().join(format!("noded-create-admission-{}-{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)));
    let _ = std::fs::remove_dir_all(&path);
    std::fs::DirBuilder::new().recursive(true).mode(0o700).create(&path).unwrap();
    TempDir(path)
}

struct NoMetrics;

impl MetricsSource for NoMetrics {
    fn sample(&self) -> Option<Metrics> {
        None
    }
}

/// What the fake agent saw, and the claims noded held at each request.
#[derive(Default)]
struct Seen {
    requests: Vec<(String, Value)>,
    held_at_finish: Vec<usize>,
}

type Shared = Arc<Mutex<Seen>>;

fn reply(status: StatusCode, body: Value) -> Response<TestBody> {
    let mut response = Response::new(Full::new(Bytes::from(body.to_string())).boxed());
    *response.status_mut() = status;
    response
}

async fn agent(request: Request<Incoming>, seen: Shared, service: Arc<AdmissionService>) -> Result<Response<TestBody>, Infallible> {
    let path = request.uri().path().to_string();
    let bytes = request.into_body().collect().await.unwrap().to_bytes();
    let body: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    seen.lock().unwrap().requests.push((path.clone(), body.clone()));
    Ok(match path.as_str() {
        "/internal/v1/creates/admit" if body["sandbox_id"] == "invalid" => reply(StatusCode::BAD_REQUEST, json!({"error": "sandbox image is invalid"})),
        "/internal/v1/creates/admit" => reply(StatusCode::OK, json!({
            "token": "t1", "existing": null, "spec": body["spec"],
            "requested_resources": {"vcpu": 1.0, "memory_mb": 512, "disk_mb": 1024},
            "initial_claim": null, "split": false, "config_sha256": "config-1",
        })),
        "/internal/v1/creates/t1/finish" => {
            seen.lock().unwrap().held_at_finish.push(service.held());
            reply(StatusCode::OK, json!({"status": 201, "sandbox": {"id": "demo", "state": "running"}, "phases": {"request_lock_ms": 0}}))
        }
        _ => reply(StatusCode::CREATED, json!({"forwarded": body})),
    })
}

struct AdmittingPipeline(Arc<AdmissionService>);

impl Pipeline for AdmittingPipeline {
    fn create<'a>(
        &'a self,
        admitted: &'a Admitted,
        _timings: &'a mut Timings,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<bool, CreateError>> + Send + 'a>> {
        Box::pin(async move {
            match admitted.sandbox_id.as_str() {
                "odd" => Err(CreateError::Unsupported("spec: unreadable".into())),
                _ => Ok(true),
            }
        })
    }

    fn supports(&self, _spec: &Map<String, Value>) -> bool {
        true
    }

    fn admission(&self) -> Option<Arc<AdmissionService>> {
        Some(self.0.clone())
    }
}

struct Harness {
    address: std::net::SocketAddr,
    seen: Shared,
    service: Arc<AdmissionService>,
    _dir: TempDir,
}

async fn start() -> Harness {
    let dir = temp_dir();
    let registry = Arc::new(Registry::owner(dir.0.join("direct-registry.sqlite"), 0).unwrap());
    let config = ServiceConfig {
        startup_slots: 1,
        restore_slots: 1,
        publication_lanes: 1,
        admission_wait_seconds: 30.0,
        capacity: None,
        ram_backing_configured: false,
        recovery_grace: Duration::from_secs(60),
    };
    let sources = Sources {
        store: registry.clone(),
        owners: registry,
        metrics: Arc::new(NoMetrics),
        observations: Arc::new(NodeObservations::new(None, false)),
        clock: Arc::new(SystemClock::new()),
    };
    let service = AdmissionService::open(config, true, sources).unwrap();
    service.hello("agent-one", Vec::new());
    let socket = dir.0.join("agent.sock");
    let seen: Shared = Arc::default();
    let listener = UnixListener::bind(&socket).unwrap();
    let (agent_seen, agent_service) = (seen.clone(), service.clone());
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let (seen, service) = (agent_seen.clone(), agent_service.clone());
            tokio::spawn(async move {
                let handler = hyper::service::service_fn(move |request| agent(request, seen.clone(), service.clone()));
                let _ = hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(stream), handler).await;
            });
        }
    });
    let client = Arc::new(AgentClient::new(socket.clone(), TOKEN, "session-1").unwrap());
    let front = Arc::new(CreateFront::new(client, Arc::new(AdmittingPipeline(service.clone())), TOKEN));
    let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = tcp.local_addr().unwrap();
    let fronts = ucloud_noded::Fronts { create: Some(front), exec: None, jobs: None, node: None, internal: None };
    tokio::spawn(ucloud_noded::serve_with(tcp, ucloud_noded::Config::new(socket), fronts, std::future::pending()));
    Harness { address, seen, service, _dir: dir }
}

async fn post(address: std::net::SocketAddr, body: &Value, wait: &str) -> (StatusCode, hyper::HeaderMap, Bytes) {
    let stream = TcpStream::connect(address).await.unwrap();
    let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream)).await.unwrap();
    tokio::spawn(connection);
    let bytes = Bytes::from(body.to_string());
    let request = Request::post("/v1/sandboxes")
        .header("host", "node")
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-length", bytes.len())
        .header("x-ucloud-admission-wait", wait)
        .body(Full::new(bytes))
        .unwrap();
    let response = sender.send_request(request).await.unwrap();
    let (status, headers) = (response.status(), response.headers().clone());
    (status, headers, response.into_body().collect().await.unwrap().to_bytes())
}

fn create(id: &str, extra: Value) -> Value {
    let mut spec = json!({"id": id, "image": "img", "cpus": 2, "memory_mb": 512, "disk_mb": 1024,
                          "_ucloud_operation": {"generation": 1, "kind": "create", "operation_id": "create-1", "spec_hash": "h"}});
    spec.as_object_mut().unwrap().extend(extra.as_object().unwrap().clone());
    spec
}

#[tokio::test]
async fn a_noded_create_holds_its_claim_until_finish_returns() {
    let harness = start().await;
    let (status, _, body) = post(harness.address, &create("demo", json!({})), "2").await;
    assert_eq!(status, StatusCode::CREATED, "{}", String::from_utf8_lossy(&body));
    let body: Value = serde_json::from_slice(&body).unwrap();
    let phases = &body["timings"]["manager"]["phases"];
    assert!(phases["startup_admission_ms"].is_u64() && phases["active_capacity_ms"].is_u64(), "{phases}");
    let seen = harness.seen.lock().unwrap();
    let admit = &seen.requests[0].1;
    assert_eq!(admit["admitted"]["token"].as_str().unwrap().len(), 32, "{admit}");
    // The claim was held while the agent finished, and released after.
    assert_eq!(seen.held_at_finish, vec![1]);
    assert_eq!((harness.service.held(), harness.service.available().0), (0, 1));
}

#[tokio::test]
async fn refusals_answer_as_the_agents_create_does() {
    let harness = start().await;
    // The only slot is taken: the create waits out its wait and is deferred.
    let holder = harness.service.startup(&Holder::Noded, &("other".into(), 1), Shape { vcpu: 0.0, memory_mb: 0, disk_mb: 0 }, false, None).await.unwrap();
    let (status, headers, body) = post(harness.address, &create("demo", json!({})), "0.2").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(&body[..], br#"{"error":"node startup admission wait deadline exceeded","error_code":"node_active_admission_deferred","retryable":true}"#);
    assert_eq!((headers["retry-after"].to_str().unwrap(), headers["x-ucloud-sandbox-retryable"].to_str().unwrap()), ("1", "true"));
    assert!(harness.seen.lock().unwrap().requests.is_empty(), "nothing reached the agent");
    harness.service.release(&holder.token, &Holder::Noded);
    // A closed node: no Retry-After, exactly like create.
    harness.service.close();
    let (status, headers, body) = post(harness.address, &create("demo", json!({})), "2").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(&body[..], br#"{"error":"direct node admission is closed","error_code":"node_admission_closed","retryable":true}"#);
    assert!(headers.get("retry-after").is_none());
    harness.service.open_admission();
    // The agent's validation refusal is relayed, and the claim released.
    let (status, _, body) = post(harness.address, &create("invalid", json!({})), "2").await;
    assert_eq!((status, &body[..]), (StatusCode::BAD_REQUEST, &br#"{"error":"sandbox image is invalid"}"#[..]));
    assert_eq!(harness.service.held(), 0);
}

#[tokio::test]
async fn unparsed_shapes_and_unsupported_creates_go_to_the_agent_without_a_claim() {
    let harness = start().await;
    // A shape Python would read differently: the agent admits it itself.
    let (status, _, _) = post(harness.address, &create("demo", json!({"memory_mb": "512"})), "2").await;
    assert_eq!(status, StatusCode::CREATED);
    assert!(harness.seen.lock().unwrap().requests[0].1.get("admitted").is_none());
    // The pipeline cannot create it: released before the agent creates it.
    harness.seen.lock().unwrap().requests.clear();
    let (status, _, body) = post(harness.address, &create("odd", json!({})), "2").await;
    assert_eq!(status, StatusCode::CREATED);
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["forwarded"]["id"], "odd");
    assert_eq!(harness.service.held(), 0);
    let paths: Vec<String> = harness.seen.lock().unwrap().requests.iter().map(|(path, _)| path.clone()).collect();
    assert_eq!(paths, ["/internal/v1/creates/admit", "/internal/v1/creates/t1/finish", "/v1/sandboxes"]);
}
