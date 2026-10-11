//! The agent's way in: noded's internal Unix socket (`--internal-unix`,
//! docs/admission-port-3c.md §3.3), the reverse of [`crate::agent_rpc`].
//!
//! The node's TCP port is reachable from outside, so these routes never live
//! there. The socket is mode 0600 in a 0700 directory; a connection from
//! another uid is closed unread (`SO_PEERCRED`), and every request carries the
//! node control bearer token. Requests carry `X-UCloud-Agent-Session` (one
//! per agent start) and every answer carries `X-UCloud-Noded-Session`, so
//! each side notices the other's restart.
//!
//! Step 3c.0 serves only the handshake: `POST /internal/v1/admission/hello`
//! and `GET /internal/v1/admission/ping`. Anything else under
//! `/internal/v1/admission/` is a 404 until later steps add its route.

use std::future::Future;
use std::os::unix::fs::{DirBuilderExt, FileTypeExt, PermissionsExt};
use std::path::Path;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::body::Incoming;
use hyper::header::{self, HeaderValue};
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::{TokioIo, TokioTimer};
use hyper_util::server::graceful::GracefulShutdown;
use serde_json::{Value, json};
use tokio::net::UnixListener;

use crate::Body;
use crate::agent_rpc::SESSION_HEADER;
use crate::node_pipeline::NodePipeline;

/// One agent start. From step 3c.2 a hello under a new one drops whatever an
/// earlier agent session held; in 3c.0 it is only recorded.
pub const AGENT_SESSION_HEADER: &str = "x-ucloud-agent-session";
const ADMISSION_PREFIX: &str = "/internal/v1/admission/";
/// Handshake bodies are tiny; later routes stay far below this too.
const MAX_BODY_BYTES: usize = 1024 * 1024;
const MAX_SESSION_BYTES: usize = 128;

/// What the handshake reports about the node, as noded knows it.
pub trait NodeView: Send + Sync {
    /// The drain row's `admission_open`; `None` until the node is loaded or
    /// when the registry cannot be read. Blocking (a registry read).
    fn admission_open(&self) -> Option<bool>;
    /// The digest of the create configuration the node was loaded from.
    fn config_sha256(&self) -> Option<String>;
}

/// The view of the node `start_fronts` loads once the agent reports its
/// configuration.
pub struct LoadedNode(pub Arc<OnceLock<Arc<NodePipeline>>>);

impl NodeView for LoadedNode {
    fn admission_open(&self) -> Option<bool> {
        let node = self.0.get()?;
        node.registry().drain_since(Instant::now()).ok().map(|drain| drain.admission_open)
    }

    fn config_sha256(&self) -> Option<String> {
        self.0.get().map(|node| node.config().config_sha256.clone()).filter(|digest| !digest.is_empty())
    }
}

pub struct Internal {
    session: HeaderValue,
    token: Bytes,
    peer_uid: u32,
    node: Arc<dyn NodeView>,
    /// The agent session of the last hello.
    agent_session: Mutex<Option<String>>,
}

fn full(bytes: Bytes) -> Body {
    Full::new(bytes).map_err(|never| match never {}).boxed()
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter().zip(right).fold(0u8, |acc, (a, b)| acc | (a ^ b)) == 0
}

fn valid_session(value: &str) -> bool {
    !value.is_empty() && value.len() <= MAX_SESSION_BYTES && value.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
}

/// The euid of this process: the agent runs as the same user.
pub fn own_uid() -> u32 {
    // SAFETY: geteuid has no preconditions and cannot fail.
    unsafe { libc::geteuid() }
}

impl Internal {
    /// `peer_uid`: the only uid whose connections are served (the agent's).
    pub fn new(session: &str, token: &str, peer_uid: u32, node: Arc<dyn NodeView>) -> Result<Arc<Self>, String> {
        let session = HeaderValue::from_str(session).map_err(|_| "invalid noded session".to_string())?;
        Ok(Arc::new(Internal { session, token: Bytes::copy_from_slice(token.as_bytes()), peer_uid, node, agent_session: Mutex::new(None) }))
    }

    pub fn session(&self) -> &str {
        self.session.to_str().unwrap_or_default()
    }

    /// The agent session that said hello last, if any.
    pub fn agent_session(&self) -> Option<String> {
        self.agent_session.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).clone()
    }

    fn respond(&self, status: StatusCode, body: Value) -> Response<Body> {
        let mut response = Response::new(full(Bytes::from(body.to_string())));
        *response.status_mut() = status;
        let headers = response.headers_mut();
        headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
        headers.insert(SESSION_HEADER, self.session.clone());
        if status == StatusCode::UNAUTHORIZED {
            headers.insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
        }
        response
    }

    fn error(&self, status: StatusCode, code: &str, message: &str, retryable: bool) -> Response<Body> {
        self.respond(status, json!({"error": message, "error_code": code, "retryable": retryable}))
    }

    pub async fn handle(self: &Arc<Self>, request: Request<Incoming>) -> Response<Body> {
        let (parts, body) = request.into_parts();
        let supplied = parts.headers.get(header::AUTHORIZATION).map(|v| v.as_bytes()).and_then(|v| v.strip_prefix(b"Bearer "));
        if !supplied.is_some_and(|supplied| !supplied.is_empty() && constant_time_eq(supplied, &self.token)) {
            return self.error(StatusCode::UNAUTHORIZED, "unauthorized", "unauthorized", false);
        }
        let path = parts.uri.path();
        let Some(route) = path.strip_prefix(ADMISSION_PREFIX) else {
            return self.error(StatusCode::NOT_FOUND, "not_found", "not found", false);
        };
        let agent_session = parts.headers.get(AGENT_SESSION_HEADER).and_then(|v| v.to_str().ok()).filter(|v| valid_session(v));
        let Some(agent_session) = agent_session.map(str::to_string) else {
            return self.error(StatusCode::BAD_REQUEST, "invalid_request", "X-UCloud-Agent-Session is missing or invalid", false);
        };
        let bytes = match Limited::new(body, MAX_BODY_BYTES).collect().await {
            Ok(collected) => collected.to_bytes(),
            Err(_) => return self.error(StatusCode::BAD_REQUEST, "invalid_request", "the request body is unreadable or too large", false),
        };
        match (route, &parts.method) {
            ("hello", &Method::POST) => self.hello(&agent_session, &bytes).await,
            ("ping", &Method::GET) => {
                let current = self.agent_session().as_deref() == Some(agent_session.as_str());
                self.respond(StatusCode::OK, json!({"noded_session": self.session(), "hello": current}))
            }
            ("hello" | "ping", _) => self.error(StatusCode::METHOD_NOT_ALLOWED, "method_not_allowed", "method not allowed", false),
            _ => self.error(StatusCode::NOT_FOUND, "admission_route_unknown", &format!("no admission route {path}"), false),
        }
    }

    /// `{agent_session}` -> `{noded_session, admission_open, config_sha256}`.
    /// The body names the header's session, so a proxy cannot mix them up.
    async fn hello(self: &Arc<Self>, agent_session: &str, bytes: &[u8]) -> Response<Body> {
        let named = serde_json::from_slice::<Value>(bytes).ok().and_then(|raw| {
            let object = raw.as_object()?;
            (object.len() == 1).then(|| object.get("agent_session")?.as_str().map(str::to_string))?
        });
        if named.as_deref() != Some(agent_session) {
            return self.error(StatusCode::BAD_REQUEST, "invalid_request", "hello must be {\"agent_session\"} naming the header's session", false);
        }
        let previous = self.agent_session.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).replace(agent_session.to_string());
        if previous.as_deref() != Some(agent_session) {
            eprintln!("ucloud-noded: agent session {agent_session} said hello");
        }
        let node = self.node.clone();
        let (admission_open, config_sha256) = tokio::task::spawn_blocking(move || (node.admission_open(), node.config_sha256()))
            .await
            .unwrap_or((None, None));
        self.respond(StatusCode::OK, json!({
            "noded_session": self.session(), "admission_open": admission_open, "config_sha256": config_sha256,
        }))
    }
}

/// Bind `path` for the agent: its directory is created 0700 if missing, a
/// stale socket from an earlier start is replaced, and the socket is 0600.
/// Anything else at `path` is refused, never removed.
pub fn bind(path: &Path) -> std::io::Result<UnixListener> {
    let parent = path.parent().filter(|parent| !parent.as_os_str().is_empty()).unwrap_or(Path::new("."));
    std::fs::DirBuilder::new().recursive(true).mode(0o700).create(parent)?;
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_socket() => std::fs::remove_file(path)?,
        Ok(_) => {
            return Err(std::io::Error::new(std::io::ErrorKind::AlreadyExists, format!("{} exists and is not a socket", path.display())));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let listener = UnixListener::bind(path)?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    Ok(listener)
}

/// Serve the agent until `shutdown` resolves; in-flight requests then get
/// `grace` to finish.
pub async fn serve(listener: UnixListener, internal: Arc<Internal>, grace: Duration, shutdown: impl Future<Output = ()>) {
    let graceful = GracefulShutdown::new();
    let mut builder = hyper::server::conn::http1::Builder::new();
    builder.timer(TokioTimer::new()).header_read_timeout(Duration::from_secs(30));
    let mut refused_logged = false;
    tokio::pin!(shutdown);
    loop {
        let (stream, _) = tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok(accepted) => accepted,
                Err(error) => {
                    eprintln!("ucloud-noded: internal accept failed: {error}");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
            },
            () = &mut shutdown => break,
        };
        let uid = stream.peer_cred().map(|credentials| credentials.uid());
        if uid.as_ref().ok() != Some(&internal.peer_uid) {
            if !refused_logged {
                eprintln!("ucloud-noded: refused an internal connection from uid {uid:?}; only uid {} is served", internal.peer_uid);
                refused_logged = true;
            }
            continue;
        }
        let internal = internal.clone();
        let service = hyper::service::service_fn(move |request: Request<Incoming>| {
            let internal = internal.clone();
            async move { Ok::<_, std::convert::Infallible>(internal.handle(request).await) }
        });
        let connection = graceful.watch(builder.serve_connection(TokioIo::new(stream), service));
        tokio::spawn(async move {
            let _ = connection.await;
        });
    }
    drop(listener);
    if tokio::time::timeout(grace, graceful.shutdown()).await.is_err() {
        eprintln!("ucloud-noded: internal requests still running after the shutdown grace");
    }
}
