//! The agent's internal socket (`--internal-unix`, docs/admission-port-3c.md
//! §3.3, step 3c.0): the handshake, its refusals, the socket's permissions,
//! and the agent's own client (ucloud_sandboxes/noded_client.py) against it.

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::client::conn::http1::SendRequest;
use hyper::{Request, StatusCode};
use hyper_util::rt::TokioIo;
use serde_json::{Value, json};
use tokio::net::UnixStream;
use ucloud_noded::internal::{self, Internal, NodeView};
use ucloud_noded::pipeline::CreateConfig;

const TOKEN: &str = "node-secret";
const NODED_SESSION: &str = "0123456789abcdef0123456789abcdef";
const AGENT_SESSION: &str = "6f1c1a52-7d6e-4a43-9a1b-1f0c1e2d3c4b";

struct TempDir(PathBuf);

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn temp_dir() -> TempDir {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let path = std::env::temp_dir().join(format!("noded-internal-{}-{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)));
    std::fs::create_dir_all(&path).unwrap();
    TempDir(path)
}

struct FixedNode(Option<bool>, Option<String>);

impl NodeView for FixedNode {
    fn admission_open(&self) -> Option<bool> {
        self.0
    }

    fn config_sha256(&self) -> Option<String> {
        self.1.clone()
    }
}

fn serve(socket: &Path, peer_uid: u32, node: FixedNode) -> Arc<Internal> {
    let internal = Internal::new(NODED_SESSION, TOKEN, peer_uid, Arc::new(node)).unwrap();
    let listener = internal::bind(socket).unwrap();
    tokio::spawn(internal::serve(listener, internal.clone(), Duration::from_secs(1), std::future::pending()));
    internal
}

async fn connect(socket: &Path) -> SendRequest<Full<Bytes>> {
    let stream = UnixStream::connect(socket).await.unwrap();
    let (sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream)).await.unwrap();
    tokio::spawn(connection);
    sender
}

async fn call(
    sender: &mut SendRequest<Full<Bytes>>,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: Option<Value>,
) -> (StatusCode, hyper::HeaderMap, Value) {
    let mut request = Request::builder().method(method).uri(path).header("host", "localhost");
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    let bytes = body.map(|body| Bytes::from(body.to_string())).unwrap_or_default();
    let response = sender.send_request(request.body(Full::new(bytes)).unwrap()).await.unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, headers, serde_json::from_slice(&bytes).unwrap())
}

const AUTHORIZED: [(&str, &str); 2] = [("authorization", "Bearer node-secret"), ("x-ucloud-agent-session", AGENT_SESSION)];

#[tokio::test]
async fn hello_and_ping_share_one_kept_alive_connection() {
    let dir = temp_dir();
    let socket = dir.0.join("noded").join("noded.sock");
    let internal = serve(&socket, internal::own_uid(), FixedNode(Some(false), Some("c".repeat(64))));
    let mut sender = connect(&socket).await;

    let (status, headers, body) = call(&mut sender, "GET", "/internal/v1/admission/ping", &AUTHORIZED, None).await;
    assert_eq!((status, &body), (StatusCode::OK, &json!({"noded_session": NODED_SESSION, "hello": false})));
    assert_eq!(headers["x-ucloud-noded-session"], NODED_SESSION);
    assert_eq!(internal.agent_session(), None);

    let hello = json!({"agent_session": AGENT_SESSION});
    let (status, headers, body) = call(&mut sender, "POST", "/internal/v1/admission/hello", &AUTHORIZED, Some(hello)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!({"noded_session": NODED_SESSION, "admission_open": false, "config_sha256": "c".repeat(64)}));
    assert_eq!(headers["x-ucloud-noded-session"], NODED_SESSION);
    assert_eq!(internal.agent_session().as_deref(), Some(AGENT_SESSION));

    let (_, _, body) = call(&mut sender, "GET", "/internal/v1/admission/ping", &AUTHORIZED, None).await;
    assert_eq!(body["hello"], true);
    // Another agent start says hello under its own session.
    let other = [("authorization", "Bearer node-secret"), ("x-ucloud-agent-session", "second")];
    let (_, _, body) = call(&mut sender, "GET", "/internal/v1/admission/ping", &other, None).await;
    assert_eq!(body["hello"], false);
    let (status, _, _) = call(&mut sender, "POST", "/internal/v1/admission/hello", &other, Some(json!({"agent_session": "second"}))).await;
    assert_eq!((status, internal.agent_session().as_deref()), (StatusCode::OK, Some("second")));
}

#[tokio::test]
async fn an_unloaded_node_reports_unknowns() {
    let dir = temp_dir();
    let socket = dir.0.join("noded.sock");
    serve(&socket, internal::own_uid(), FixedNode(None, None));
    let mut sender = connect(&socket).await;
    let (status, _, body) =
        call(&mut sender, "POST", "/internal/v1/admission/hello", &AUTHORIZED, Some(json!({"agent_session": AGENT_SESSION}))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!({"noded_session": NODED_SESSION, "admission_open": null, "config_sha256": null}));
}

#[tokio::test]
async fn refusals_are_typed_and_carry_the_session() {
    let dir = temp_dir();
    let socket = dir.0.join("noded.sock");
    let internal = serve(&socket, internal::own_uid(), FixedNode(Some(true), None));
    let mut sender = connect(&socket).await;
    let hello = Some(json!({"agent_session": AGENT_SESSION}));
    let cases: Vec<(&str, &str, Vec<(&str, &str)>, Option<Value>, StatusCode, &str)> = vec![
        ("POST", "/internal/v1/admission/hello", vec![("x-ucloud-agent-session", AGENT_SESSION)], hello.clone(), StatusCode::UNAUTHORIZED, "unauthorized"),
        ("POST", "/internal/v1/admission/hello", vec![("authorization", "Bearer wrong"), ("x-ucloud-agent-session", AGENT_SESSION)], hello.clone(), StatusCode::UNAUTHORIZED, "unauthorized"),
        ("POST", "/internal/v1/admission/hello", vec![("authorization", "Bearer node-secret")], hello.clone(), StatusCode::BAD_REQUEST, "invalid_request"),
        ("POST", "/internal/v1/admission/hello", vec![("authorization", "Bearer node-secret"), ("x-ucloud-agent-session", "a b")], hello.clone(), StatusCode::BAD_REQUEST, "invalid_request"),
        ("POST", "/internal/v1/admission/hello", AUTHORIZED.to_vec(), Some(json!({"agent_session": "other"})), StatusCode::BAD_REQUEST, "invalid_request"),
        ("POST", "/internal/v1/admission/hello", AUTHORIZED.to_vec(), Some(json!({"agent_session": AGENT_SESSION, "extra": 1})), StatusCode::BAD_REQUEST, "invalid_request"),
        ("POST", "/internal/v1/admission/hello", AUTHORIZED.to_vec(), None, StatusCode::BAD_REQUEST, "invalid_request"),
        ("GET", "/internal/v1/admission/hello", AUTHORIZED.to_vec(), None, StatusCode::METHOD_NOT_ALLOWED, "method_not_allowed"),
        ("POST", "/internal/v1/admission/ping", AUTHORIZED.to_vec(), None, StatusCode::METHOD_NOT_ALLOWED, "method_not_allowed"),
        // Later steps add these; until then they are unknown, not the agent's.
        ("POST", "/internal/v1/admission/startup", AUTHORIZED.to_vec(), Some(json!({})), StatusCode::NOT_FOUND, "admission_route_unknown"),
        ("GET", "/internal/v1/admission/snapshot", AUTHORIZED.to_vec(), None, StatusCode::NOT_FOUND, "admission_route_unknown"),
        ("GET", "/internal/v1/admission/", AUTHORIZED.to_vec(), None, StatusCode::NOT_FOUND, "admission_route_unknown"),
        ("GET", "/internal/v1/creates/config", AUTHORIZED.to_vec(), None, StatusCode::NOT_FOUND, "not_found"),
        ("GET", "/v1/sandboxes", AUTHORIZED.to_vec(), None, StatusCode::NOT_FOUND, "not_found"),
    ];
    for (method, path, headers, body, expected, code) in cases {
        let (status, response_headers, answer) = call(&mut sender, method, path, &headers, body).await;
        assert_eq!((status, answer["error_code"].as_str()), (expected, Some(code)), "{method} {path} {headers:?}");
        assert_eq!(answer["retryable"], false);
        assert_eq!(response_headers["x-ucloud-noded-session"], NODED_SESSION);
    }
    // None of them was a hello.
    assert_eq!(internal.agent_session(), None);
}

#[tokio::test]
async fn another_uid_is_closed_unread() {
    let dir = temp_dir();
    let socket = dir.0.join("noded.sock");
    serve(&socket, internal::own_uid().wrapping_add(1), FixedNode(Some(true), None));
    let stream = UnixStream::connect(&socket).await.unwrap();
    let (mut sender, connection) = hyper::client::conn::http1::handshake::<_, Full<Bytes>>(TokioIo::new(stream)).await.unwrap();
    tokio::spawn(connection);
    let request = Request::get("/internal/v1/admission/ping")
        .header("host", "localhost")
        .header("authorization", "Bearer node-secret")
        .header("x-ucloud-agent-session", AGENT_SESSION)
        .body(Full::new(Bytes::new()))
        .unwrap();
    assert!(sender.send_request(request).await.is_err());
}

#[tokio::test]
async fn the_socket_is_private_and_only_a_stale_socket_is_replaced() {
    let dir = temp_dir();
    let socket = dir.0.join("state").join("noded").join("noded.sock");
    let first = internal::bind(&socket).unwrap();
    let mode = |path: &Path| std::fs::metadata(path).unwrap().permissions().mode() & 0o777;
    assert_eq!((mode(&socket), mode(socket.parent().unwrap())), (0o600, 0o700));
    drop(first); // A crashed daemon leaves its socket file behind.
    assert!(socket.exists());
    let second = internal::bind(&socket).unwrap();
    assert_eq!(mode(&socket), 0o600);
    drop(second);
    let file = dir.0.join("file");
    std::fs::write(&file, b"keep").unwrap();
    assert_eq!(internal::bind(&file).unwrap_err().kind(), std::io::ErrorKind::AlreadyExists);
    assert_eq!(std::fs::read(&file).unwrap(), b"keep");
}

fn python() -> Option<PathBuf> {
    let path = std::env::var_os("UCLOUD_SANDBOXES_PYTHON")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../../.venv/bin/python")));
    path.exists().then_some(path)
}

async fn run_python(python: &Path, script: &str, args: &[&str]) -> String {
    let output = tokio::process::Command::new(python)
        .current_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/../.."))
        .arg("-c")
        .arg(script)
        .args(args)
        .output()
        .await
        .unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    String::from_utf8_lossy(&output.stdout).trim().to_string()
}

#[tokio::test]
async fn the_agents_client_says_hello_to_the_daemon() {
    let Some(python) = python() else { return };
    let dir = temp_dir();
    let socket = dir.0.join("noded").join("noded.sock");
    let internal = serve(&socket, internal::own_uid(), FixedNode(Some(true), Some("e".repeat(64))));
    let script = r#"
import json, sys
from ucloud_sandboxes.noded_client import NodedClient, NodedRefused
client = NodedClient(sys.argv[1], sys.argv[2], agent_session=sys.argv[3])
hello = client.hello()
ping = client.ping()
try:
    client.call("POST", "/internal/v1/admission/startup", {})
except NodedRefused as exc:
    refused = [exc.status, exc.error_code, exc.retryable]
print(json.dumps({"hello": hello, "ping": ping, "refused": refused, "session": client.noded_session,
                  "connections": client.connections_opened}))
client.close()
"#;
    let path = socket.to_string_lossy().to_string();
    let output = run_python(&python, script, &[&path, TOKEN, AGENT_SESSION]).await;
    let answer: Value = serde_json::from_str(&output).unwrap();
    assert_eq!(answer["hello"], json!({"noded_session": NODED_SESSION, "admission_open": true, "config_sha256": "e".repeat(64)}));
    assert_eq!(answer["ping"], json!({"noded_session": NODED_SESSION, "hello": true}));
    assert_eq!(answer["refused"], json!([404, "admission_route_unknown", false]));
    assert_eq!((answer["session"].as_str(), answer["connections"].as_u64()), (Some(NODED_SESSION), Some(1)));
    assert_eq!(internal.agent_session().as_deref(), Some(AGENT_SESSION));
}

#[tokio::test]
async fn the_agents_create_configuration_parses_with_its_admission_block() {
    let Some(python) = python() else { return };
    let dir = temp_dir();
    let script = r#"
import hashlib, json, sys
from pathlib import Path
from types import SimpleNamespace
from tests import test_direct_provisioner as fixtures
from ucloud_sandboxes.create_handoff import create_config
from ucloud_sandboxes.direct_service import DirectSandboxService
root = Path(sys.argv[1])
provisioner, _, storage, images, warden = fixtures.DirectProvisionerTests().make(root)
warden.config.runsc, warden.config.runtime_root = Path("/opt/runsc"), root / "runsc"
warden.config.journal_root = root / "journals"
storage.socket_path, images.root = Path("/run/storage.sock"), root / "cache"
service = DirectSandboxService(provisioner, max_concurrent_startups=12, max_concurrent_restores=3)
service.configure_active_capacity(fixtures.ResourceQuantity(vcpu=6, memory_mb=24576), runtime_metrics_provider=lambda: None)
sessions = SimpleNamespace(max_sessions=1024, max_events_per_session=512, completed_retention_seconds=30.0,
                           delivered_grace_seconds=2.0, output_idle_timeout_seconds=300.0)
config = create_config(service, node_epoch="epoch", rust_creates_enabled=True, rust_execs_enabled=True,
                       exec_sessions=sessions, rust_pause_enabled=True, rust_admission_enabled=True)
digest = hashlib.sha256(json.dumps(config, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
print(json.dumps(dict(config, config_sha256=digest)))
"#;
    // The registry's directory must be private, as TemporaryDirectory makes it.
    std::fs::set_permissions(&dir.0, std::fs::Permissions::from_mode(0o700)).unwrap();
    let root = dir.0.to_string_lossy().to_string();
    let output = run_python(&python, script, &[&root]).await;
    let mut config: Value = serde_json::from_str(&output).unwrap();
    // The fixture assembles no init binaries; a node always has both.
    for binary in ["init_binary", "managed_init_binary"] {
        assert_eq!(config[binary], Value::Null);
        config[binary] = json!("/usr/libexec/init");
    }
    let config: CreateConfig = serde_json::from_value(config).unwrap();
    let admission = config.admission.expect("the admission block");
    assert!(admission.rust_admission_enabled);
    assert_eq!((admission.max_concurrent_startups, admission.max_concurrent_restores), (12, 3));
    let capacity = admission.active_capacity.expect("active capacity");
    assert_eq!((capacity.vcpu, capacity.memory_mb), (6.0, 24576));
    assert_eq!((admission.physical_memory_floor_mb, admission.prefetch_max_bytes, admission.publication_lanes), (2048, 1 << 30, 16));
    assert_eq!((admission.admission_wait_seconds, admission.growth_observation_max_age_seconds, admission.retry_seconds), (30.0, 30.0, 0.21));
    assert_eq!(config.config_sha256.len(), 64);
}
