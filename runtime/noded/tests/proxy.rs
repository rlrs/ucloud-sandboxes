//! The front door against a fake agent on a Unix socket.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Empty, Full, StreamBody, combinators::BoxBody};
use hyper::body::{Frame, Incoming};
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::{TcpListener, TcpStream, UnixListener};
use tokio::sync::{Notify, mpsc, oneshot};

type TestBody = BoxBody<Bytes, Infallible>;

struct Agent {
    path: PathBuf,
    accepts: Arc<AtomicUsize>,
    release: Arc<Notify>,
    _dir: TempDir,
}

struct TempDir(PathBuf);

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn temp_dir() -> TempDir {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let path = std::env::temp_dir().join(format!(
        "noded-test-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&path).unwrap();
    TempDir(path)
}

fn full(body: impl Into<Bytes>) -> TestBody {
    Full::new(body.into()).boxed()
}

async fn agent_handle(request: Request<Incoming>, release: Arc<Notify>) -> Response<TestBody> {
    let path = request.uri().path().to_string();
    match path.as_str() {
        "/stream" => {
            let (sender, receiver) = mpsc::channel::<Result<Frame<Bytes>, Infallible>>(2);
            tokio::spawn(async move {
                sender.send(Ok(Frame::data(Bytes::from_static(b"first")))).await.unwrap();
                release.notified().await;
                sender.send(Ok(Frame::data(Bytes::from_static(b"second")))).await.unwrap();
            });
            let stream = tokio_stream::wrappers::ReceiverStream::new(receiver);
            Response::new(StreamBody::new(stream).boxed())
        }
        "/close" => {
            let mut response = Response::new(full("closing"));
            response.headers_mut().insert("connection", "close".parse().unwrap());
            response
        }
        _ => {
            let method = request.method().to_string();
            let query = request.uri().query().unwrap_or("").to_string();
            let hop = request.headers().contains_key("x-hop");
            let keep = request.headers().get("x-keep").map(|v| v.to_str().unwrap().to_string());
            let length = request.into_body().collect().await.unwrap().to_bytes().len();
            let mut response = Response::new(full(format!(
                "{method} {path}?{query} hop={hop} keep={keep:?} length={length}"
            )));
            *response.status_mut() = StatusCode::CREATED;
            response.headers_mut().insert("x-upstream", "yes".parse().unwrap());
            response
        }
    }
}

async fn start_agent() -> Agent {
    let dir = temp_dir();
    let path = dir.0.join("agent.sock");
    let listener = UnixListener::bind(&path).unwrap();
    let accepts = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(Notify::new());
    let (counted, released) = (accepts.clone(), release.clone());
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            counted.fetch_add(1, Ordering::SeqCst);
            let release = released.clone();
            tokio::spawn(async move {
                let service = hyper::service::service_fn(move |request| {
                    let release = release.clone();
                    async move { Ok::<_, Infallible>(agent_handle(request, release).await) }
                });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service)
                    .await;
            });
        }
    });
    Agent { path, accepts, release, _dir: dir }
}

async fn start_front_door(upstream: PathBuf) -> (SocketAddr, oneshot::Sender<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, stopped) = oneshot::channel::<()>();
    tokio::spawn(ucloud_noded::serve(listener, ucloud_noded::Config::new(upstream), async move {
        let _ = stopped.await;
    }));
    (address, stop)
}

type Client = hyper::client::conn::http1::SendRequest<BoxBody<Bytes, Infallible>>;

async fn client(address: SocketAddr) -> Client {
    let stream = TcpStream::connect(address).await.unwrap();
    let (sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream)).await.unwrap();
    tokio::spawn(connection);
    sender
}

fn request(path: &str, body: TestBody) -> Request<TestBody> {
    Request::builder().uri(path).header("host", "node").body(body).unwrap()
}

async fn text(response: Response<Incoming>) -> String {
    String::from_utf8(response.into_body().collect().await.unwrap().to_bytes().to_vec()).unwrap()
}

#[tokio::test]
async fn forwards_requests_and_strips_hop_by_hop_headers() {
    let agent = start_agent().await;
    let (address, _stop) = start_front_door(agent.path.clone()).await;
    let mut sender = client(address).await;
    let mut outgoing = request("/v1/sandboxes?x=1", full("12345"));
    *outgoing.method_mut() = hyper::Method::POST;
    let headers = outgoing.headers_mut();
    headers.insert("connection", "keep-alive, x-hop".parse().unwrap());
    headers.insert("x-hop", "1".parse().unwrap());
    headers.insert("x-keep", "kept".parse().unwrap());
    let response = sender.send_request(outgoing).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    assert_eq!(response.headers()["x-upstream"], "yes");
    assert_eq!(text(response).await, "POST /v1/sandboxes?x=1 hop=false keep=Some(\"kept\") length=5");
}

#[tokio::test]
async fn client_keep_alive_reuses_one_agent_connection_and_survives_its_close() {
    let agent = start_agent().await;
    let (address, _stop) = start_front_door(agent.path.clone()).await;
    let mut sender = client(address).await;
    for _ in 0..3 {
        let response = sender.send_request(request("/healthz", Empty::new().boxed())).await.unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        text(response).await;
    }
    assert_eq!(agent.accepts.load(Ordering::SeqCst), 1);
    // The agent closes its side; the client's connection carries on.
    let response = sender.send_request(request("/close", Empty::new().boxed())).await.unwrap();
    assert!(!response.headers().contains_key("connection"));
    assert_eq!(text(response).await, "closing");
    let response = sender.send_request(request("/healthz", Empty::new().boxed())).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    text(response).await;
    assert_eq!(agent.accepts.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn response_bodies_stream_without_buffering() {
    let agent = start_agent().await;
    let (address, _stop) = start_front_door(agent.path.clone()).await;
    let mut sender = client(address).await;
    let response = sender.send_request(request("/stream", Empty::new().boxed())).await.unwrap();
    let mut body = response.into_body();
    let first = tokio::time::timeout(Duration::from_secs(5), body.frame()).await.unwrap().unwrap().unwrap();
    assert_eq!(first.into_data().unwrap(), Bytes::from_static(b"first"));
    agent.release.notify_one();
    let second = tokio::time::timeout(Duration::from_secs(5), body.frame()).await.unwrap().unwrap().unwrap();
    assert_eq!(second.into_data().unwrap(), Bytes::from_static(b"second"));
}

#[tokio::test]
async fn large_request_bodies_stream_to_the_agent() {
    let agent = start_agent().await;
    let (address, _stop) = start_front_door(agent.path.clone()).await;
    let mut sender = client(address).await;
    let mut outgoing = request("/v1/sandboxes/a/archive", full(vec![7u8; 8 << 20]));
    *outgoing.method_mut() = hyper::Method::PUT;
    let response = sender.send_request(outgoing).await.unwrap();
    assert_eq!(text(response).await, format!("PUT /v1/sandboxes/a/archive? hop=false keep=None length={}", 8 << 20));
}

#[tokio::test]
async fn a_missing_agent_is_a_retryable_503() {
    let dir = temp_dir();
    let (address, _stop) = start_front_door(dir.0.join("absent.sock")).await;
    let mut sender = client(address).await;
    let response = sender.send_request(request("/v1/sandboxes", Empty::new().boxed())).await.unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response.headers()["retry-after"], "1");
    assert!(text(response).await.contains("\"error_code\":\"node_agent_unavailable\""));
}

#[tokio::test]
async fn an_agent_that_dies_mid_request_closes_the_client_connection() {
    let dir = temp_dir();
    let path = dir.0.join("dying.sock");
    let listener = UnixListener::bind(&path).unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut buffer = [0u8; 1024];
        let _ = tokio::io::AsyncReadExt::read(&mut stream, &mut buffer).await;
        drop(stream); // Read the request, never answer.
    });
    let (address, _stop) = start_front_door(path).await;
    let mut sender = client(address).await;
    let outcome = sender.send_request(request("/v1/sandboxes", Empty::new().boxed())).await;
    assert!(outcome.is_err(), "expected a closed connection, got {outcome:?}");
}
