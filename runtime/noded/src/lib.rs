//! ucloud-noded: the sandbox node's front door (phase 0 of docs/rust-node-daemon-plan.md).
//!
//! It owns the node's TCP port and forwards every request to the Python node
//! agent on its Unix socket, streaming both bodies. Each client connection gets
//! its own upstream connection, reused while both stay alive, so HTTP/1.1
//! keep-alive maps one to one and requests on a connection stay in order.
//!
//! Failure semantics match a direct connection to the agent:
//! - agent unreachable (nothing was sent): 503 `node_agent_unavailable`;
//! - upstream failed after the request was written: the client connection
//!   closes without a response, as when the agent dies mid-request.

pub mod fsutil;
pub mod journal;
pub mod network;
pub mod pyjson;
pub mod runsc;
pub mod storage;
pub mod warden;
pub mod timings;

use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full, combinators::BoxBody};
use hyper::body::Incoming;
use hyper::client::conn::http1::SendRequest;
use hyper::header::{self, HeaderMap, HeaderValue};
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::{TokioIo, TokioTimer};
use hyper_util::server::graceful::GracefulShutdown;
use tokio::net::{TcpListener, UnixStream};
use tokio::sync::{Mutex, Semaphore};

pub type Body = BoxBody<Bytes, hyper::Error>;

#[derive(Clone, Debug)]
pub struct Config {
    /// The Python agent's Unix socket.
    pub upstream: PathBuf,
    /// Client connections served at once; further ones wait in the listen backlog.
    pub max_connections: usize,
    /// A client must send its request headers within this time.
    pub header_read_timeout: Duration,
    /// After shutdown starts, in-flight requests get this long to finish.
    pub shutdown_grace: Duration,
}

impl Config {
    pub fn new(upstream: PathBuf) -> Self {
        Config {
            upstream,
            max_connections: 4096,
            header_read_timeout: Duration::from_secs(30),
            shutdown_grace: Duration::from_secs(20),
        }
    }
}

/// Headers that describe one hop. hyper frames each hop's body itself.
const HOP_BY_HOP: [header::HeaderName; 8] = [
    header::CONNECTION,
    header::HeaderName::from_static("keep-alive"),
    header::HeaderName::from_static("proxy-connection"),
    header::PROXY_AUTHENTICATE,
    header::PROXY_AUTHORIZATION,
    header::TE,
    header::TRAILER,
    header::UPGRADE,
];

fn strip_hop_by_hop(headers: &mut HeaderMap) {
    // Headers a Connection header names are hop-by-hop too.
    let named: Vec<header::HeaderName> = headers
        .get_all(header::CONNECTION)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .filter_map(|name| header::HeaderName::from_bytes(name.trim().as_bytes()).ok())
        .collect();
    for name in HOP_BY_HOP.iter().chain(named.iter()) {
        headers.remove(name);
    }
    // The request body is re-framed upstream from its own length.
    headers.remove(header::TRANSFER_ENCODING);
}

fn unavailable(reason: &str) -> Response<Body> {
    let body = format!(
        "{{\"error\":\"node agent is unavailable: {}\",\"error_code\":\"node_agent_unavailable\",\"retryable\":true}}",
        reason.replace(['"', '\\'], "'")
    );
    let mut response = Response::new(Full::new(Bytes::from(body)).map_err(|never| match never {}).boxed());
    *response.status_mut() = StatusCode::SERVICE_UNAVAILABLE;
    let headers = response.headers_mut();
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers.insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
    response
}

/// One client connection's upstream connection, opened on first use and
/// replaced when the agent closes it.
struct Upstream {
    path: PathBuf,
    sender: Mutex<Option<SendRequest<Incoming>>>,
}

#[derive(Debug)]
pub struct UpstreamFailed(pub String);

impl std::fmt::Display for UpstreamFailed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "upstream failed after the request was sent: {}", self.0)
    }
}

impl std::error::Error for UpstreamFailed {}

async fn connect(path: &PathBuf) -> std::io::Result<SendRequest<Incoming>> {
    let stream = UnixStream::connect(path).await?;
    let (sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .map_err(std::io::Error::other)?;
    tokio::spawn(async move {
        // Ends when either side closes; errors surface on the next send.
        let _ = connection.await;
    });
    Ok(sender)
}

impl Upstream {
    async fn forward(&self, mut request: Request<Incoming>) -> Result<Response<Body>, UpstreamFailed> {
        strip_hop_by_hop(request.headers_mut());
        let mut guard = self.sender.lock().await;
        // A pooled connection the agent closed while idle is replaced once; the
        // request is resent only when hyper proves it was never written.
        for attempt in 0..2 {
            let usable = match guard.as_mut() {
                Some(sender) => sender.ready().await.is_ok(),
                None => false,
            };
            if !usable {
                match connect(&self.path).await {
                    Ok(sender) => *guard = Some(sender),
                    Err(error) => {
                        *guard = None;
                        return Ok(unavailable(&error.to_string()));
                    }
                }
            }
            let sender = guard.as_mut().expect("connected above");
            match sender.try_send_request(request).await {
                Ok(mut response) => {
                    strip_hop_by_hop(response.headers_mut());
                    return Ok(response.map(|body| body.boxed()));
                }
                Err(mut failed) => {
                    *guard = None;
                    match failed.take_message() {
                        Some(unsent) if attempt == 0 => request = unsent,
                        Some(_) => return Ok(unavailable(&failed.into_error().to_string())),
                        None => return Err(UpstreamFailed(failed.into_error().to_string())),
                    }
                }
            }
        }
        unreachable!("the second attempt always returns")
    }
}

/// Serve until `shutdown` resolves, then let in-flight requests finish within
/// `config.shutdown_grace`.
pub async fn serve(listener: TcpListener, config: Config, shutdown: impl Future<Output = ()>) {
    let slots = Arc::new(Semaphore::new(config.max_connections));
    let graceful = GracefulShutdown::new();
    let mut builder = hyper::server::conn::http1::Builder::new();
    builder.timer(TokioTimer::new()).header_read_timeout(config.header_read_timeout);
    tokio::pin!(shutdown);
    loop {
        let permit = tokio::select! {
            permit = slots.clone().acquire_owned() => permit.expect("never closed"),
            () = &mut shutdown => break,
        };
        let (stream, _) = tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok(accepted) => accepted,
                Err(error) => {
                    // Descriptor exhaustion and aborted handshakes are transient.
                    eprintln!("ucloud-noded: accept failed: {error}");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
            },
            () = &mut shutdown => break,
        };
        let _ = stream.set_nodelay(true);
        let upstream = Arc::new(Upstream { path: config.upstream.clone(), sender: Mutex::new(None) });
        let service = hyper::service::service_fn(move |request| {
            let upstream = upstream.clone();
            async move { upstream.forward(request).await }
        });
        let connection = graceful.watch(builder.serve_connection(TokioIo::new(stream), service));
        tokio::spawn(async move {
            let _ = connection.await;
            drop(permit);
        });
    }
    drop(listener);
    if tokio::time::timeout(config.shutdown_grace, graceful.shutdown()).await.is_err() {
        eprintln!("ucloud-noded: requests still running after the shutdown grace; exiting");
    }
}
