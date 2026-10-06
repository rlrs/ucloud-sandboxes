//! The create front against a fake agent: what it forwards, what it relays,
//! and the admit/finish exchange around a fake pipeline.

use std::convert::Infallible;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use http_body_util::{BodyExt, Full, combinators::BoxBody};
use hyper::body::Incoming;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use serde_json::{Map, Value, json};
use tokio::net::{TcpListener, TcpStream, UnixListener};
use ucloud_noded::agent_rpc::AgentClient;
use ucloud_noded::create::{Admitted, CreateError, CreateFront, Pipeline};
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
    let path = std::env::temp_dir().join(format!("noded-create-{}-{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)));
    std::fs::create_dir_all(&path).unwrap();
    TempDir(path)
}

/// Every request the fake agent saw: (path, body).
type Seen = Arc<Mutex<Vec<(String, Value)>>>;

fn reply(status: StatusCode, body: Value, headers: &[(&'static str, &str)]) -> Response<TestBody> {
    let mut response = Response::new(Full::new(Bytes::from(body.to_string())).boxed());
    *response.status_mut() = status;
    for (name, value) in headers {
        response.headers_mut().insert(*name, value.parse().unwrap());
    }
    response
}

async fn agent(request: Request<Incoming>, seen: Seen) -> Result<Response<TestBody>, Infallible> {
    let path = request.uri().path().to_string();
    if path.starts_with("/internal/") {
        assert_eq!(request.headers()["authorization"], format!("Bearer {TOKEN}"));
    }
    let session = request.headers().get("x-ucloud-noded-session").is_some();
    let bytes = request.into_body().collect().await.unwrap().to_bytes();
    let body: Value = serde_json::from_slice(&bytes).unwrap_or(Value::String(String::from_utf8_lossy(&bytes).into()));
    seen.lock().unwrap().push((path.clone(), body.clone()));
    Ok(match path.as_str() {
        "/internal/v1/creates/admit" => {
            assert!(session);
            if body["sandbox_id"] == "busy" {
                reply(StatusCode::SERVICE_UNAVAILABLE,
                      json!({"error": "sandbox lifecycle is busy", "error_code": "node_active_admission_deferred", "retryable": true}),
                      &[("retry-after", "1"), ("x-ucloud-sandbox-retryable", "true")])
            } else {
                let token = if body["sandbox_id"] == "full-expired" { "gone" } else { "t1" };
                let config = if body["sandbox_id"] == "moved" { "config-2" } else { "config-1" };
                reply(StatusCode::OK, json!({
                    "token": token, "existing": null, "spec": body["spec"],
                    "requested_resources": {"vcpu": 1.0, "memory_mb": 512, "disk_mb": 1024},
                    "initial_claim": null, "split": false, "config_sha256": config,
                }), &[])
            }
        }
        "/internal/v1/creates/t1/finish" => match body["outcome"].as_str().unwrap() {
            "created" => reply(StatusCode::OK, json!({
                "status": 201,
                "sandbox": {"id": "demo", "state": "running", "image": "caf\u{e9}", "activity_epoch": 7},
                "phases": {"startup_admission_ms": 1},
            }), &[]),
            "capacity_rejected" => reply(StatusCode::SERVICE_UNAVAILABLE,
                json!({"error": body["message"], "error_code": "node_active_admission_deferred", "retryable": true}),
                &[("retry-after", "1")]),
            _ => reply(StatusCode::OK, json!({}), &[]),
        },
        "/internal/v1/creates/gone/finish" => reply(StatusCode::NOT_FOUND,
            json!({"error": "unknown", "error_code": "create_token_unknown", "retryable": false}), &[]),
        // The agent's own create (forwarded requests).
        _ => reply(StatusCode::CREATED, json!({"forwarded": body}), &[]),
    })
}

struct FakePipeline;

impl Pipeline for FakePipeline {
    fn create<'a>(
        &'a self,
        admitted: &'a Admitted,
        timings: &'a mut Timings,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<bool, CreateError>> + Send + 'a>> {
        Box::pin(async move {
            timings.add("runsc_create", timings.start());
            match admitted.sandbox_id.as_str() {
                "full" | "full-expired" => Err(CreateError::Capacity("combined workspace and memory backing capacity exhausted".into())),
                "broken" => Err(CreateError::Unavailable("runsc create failed".into())),
                "odd" => Err(CreateError::Unsupported("spec: unreadable".into())),
                _ => Ok(true),
            }
        })
    }

    fn accepts_config(&self, config_sha256: &str) -> bool {
        config_sha256 == "config-1"
    }

    fn supports(&self, spec: &Map<String, Value>) -> bool {
        !spec.contains_key("network_policy")
    }
}

struct Harness {
    address: std::net::SocketAddr,
    seen: Seen,
    _dir: TempDir,
}

async fn start() -> Harness {
    let dir = temp_dir();
    let socket = dir.0.join("agent.sock");
    let seen: Seen = Arc::default();
    let listener = UnixListener::bind(&socket).unwrap();
    let agent_seen = seen.clone();
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let seen = agent_seen.clone();
            tokio::spawn(async move {
                let service = hyper::service::service_fn(move |request| agent(request, seen.clone()));
                let _ = hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(stream), service).await;
            });
        }
    });
    let client = Arc::new(AgentClient::new(socket.clone(), TOKEN, "session-1").unwrap());
    let front = Arc::new(CreateFront::new(client, Arc::new(FakePipeline), TOKEN));
    let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = tcp.local_addr().unwrap();
    let fronts = ucloud_noded::Fronts { create: Some(front), exec: None };
    tokio::spawn(ucloud_noded::serve_with(tcp, ucloud_noded::Config::new(socket), fronts, std::future::pending()));
    Harness { address, seen, _dir: dir }
}

async fn post(address: std::net::SocketAddr, token: &str, body: &Value) -> (StatusCode, hyper::HeaderMap, Value) {
    let stream = TcpStream::connect(address).await.unwrap();
    let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream)).await.unwrap();
    tokio::spawn(connection);
    let bytes = Bytes::from(body.to_string());
    let request = Request::post("/v1/sandboxes")
        .header("host", "node")
        .header("authorization", format!("Bearer {token}"))
        .header("content-length", bytes.len())
        .header("x-ucloud-admission-wait", " 2.5 ")
        .body(Full::new(bytes))
        .unwrap();
    let response = sender.send_request(request).await.unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, headers, serde_json::from_slice(&bytes).unwrap())
}

fn create(id: &str) -> Value {
    json!({"id": id, "image": "img", "memory_mb": 512, "disk_mb": 1024,
           "_ucloud_operation": {"generation": 1, "kind": "create", "operation_id": "create-1", "spec_hash": "h"}})
}

#[tokio::test]
async fn a_supported_create_runs_between_admit_and_finish() {
    let harness = start().await;
    let (status, _, body) = post(harness.address, TOKEN, &create("demo")).await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(body["sandbox"]["activity_epoch"], 7);
    assert_eq!(body["sandbox"]["image"], "caf\u{e9}");
    assert_eq!(body["timings"]["manager"]["idempotent"], false);
    assert_eq!(body["timings"]["manager"]["phases"]["startup_admission_ms"], 1);
    assert!(body["timings"]["manager"]["phases"]["runsc_create_ms"].is_u64());
    let keys: Vec<&String> = body.as_object().unwrap().keys().collect();
    assert_eq!(keys, ["sandbox", "timings"]);
    let seen = harness.seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 2);
    let admit = &seen[0].1;
    assert_eq!(admit["admission_wait_seconds"], 2.5);
    assert_eq!(admit["operation_id"], "create-1");
    assert!(admit["spec"].get("_ucloud_operation").is_none());
    assert_eq!(seen[1], ("/internal/v1/creates/t1/finish".into(), json!({"outcome": "created", "runtime_started": true})));
}

#[tokio::test]
async fn admission_refusals_are_relayed_verbatim() {
    let harness = start().await;
    let (status, headers, body) = post(harness.address, TOKEN, &create("busy")).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(headers["retry-after"], "1");
    assert_eq!(headers["x-ucloud-sandbox-retryable"], "true");
    assert_eq!(body["error_code"], "node_active_admission_deferred");
    assert_eq!(harness.seen.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn capacity_rejections_roll_back_through_the_agent() {
    let harness = start().await;
    let (status, headers, body) = post(harness.address, TOKEN, &create("full")).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(headers["retry-after"], "1");
    assert_eq!(body["error"], "combined workspace and memory backing capacity exhausted");
    let seen = harness.seen.lock().unwrap().clone();
    assert_eq!(seen[1].1["outcome"], "capacity_rejected");
}

#[tokio::test]
async fn pipeline_failures_release_and_answer_ambiguously() {
    let harness = start().await;
    let (status, headers, body) = post(harness.address, TOKEN, &create("broken")).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(!headers.contains_key("retry-after"));
    assert_eq!(body, json!({"error": "runsc create failed"}));
    let seen = harness.seen.lock().unwrap().clone();
    assert_eq!(seen[1].1["outcome"], "failed");
    assert_eq!(seen[1].1["status"], 503);
}

#[tokio::test]
async fn unsupported_or_unauthorized_creates_go_to_the_agent_unchanged() {
    let harness = start().await;
    let mut relay = create("demo");
    relay["network_policy"] = json!({"egress": "relay", "relay": "default"});
    let mut bad_operation = create("demo");
    bad_operation["_ucloud_operation"]["generation"] = json!(0);
    for (token, body) in [(TOKEN, relay), ("wrong", create("demo")), (TOKEN, bad_operation)] {
        let (status, _, answer) = post(harness.address, token, &body).await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(answer["forwarded"], body);
    }
    let seen = harness.seen.lock().unwrap().clone();
    assert!(seen.iter().all(|(path, _)| path == "/v1/sandboxes"));
}

#[tokio::test]
async fn internal_endpoints_are_not_reachable_over_tcp() {
    let harness = start().await;
    let stream = TcpStream::connect(harness.address).await.unwrap();
    let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream)).await.unwrap();
    tokio::spawn(connection);
    let request = Request::post("/internal/v1/creates/t1/finish")
        .header("host", "node")
        .header("authorization", format!("Bearer {TOKEN}"))
        .body(Full::new(Bytes::from_static(b"{}")))
        .unwrap();
    let response = sender.send_request(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert!(harness.seen.lock().unwrap().is_empty());
}

#[tokio::test]
async fn a_capacity_answer_without_a_rollback_is_ambiguous() {
    let harness = start().await;
    let (status, headers, body) = post(harness.address, TOKEN, &create("full-expired")).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(!headers.contains_key("retry-after"));
    assert!(body.get("error_code").is_none(), "{body}");
}

#[tokio::test]
async fn unsupported_specs_and_changed_configurations_go_to_the_agent_after_release() {
    let harness = start().await;
    for id in ["odd", "moved"] {
        let body = create(id);
        let (status, _, answer) = post(harness.address, TOKEN, &body).await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(answer["forwarded"], body);
    }
    let seen = harness.seen.lock().unwrap().clone();
    let paths: Vec<&str> = seen.iter().map(|(path, _)| path.as_str()).collect();
    assert_eq!(paths, [
        "/internal/v1/creates/admit", "/internal/v1/creates/t1/finish", "/v1/sandboxes",
        "/internal/v1/creates/admit", "/internal/v1/creates/t1/finish", "/v1/sandboxes",
    ]);
    assert!(seen.iter().filter(|(path, _)| path.ends_with("/finish")).all(|(_, body)| body["outcome"] == "failed"));
}
