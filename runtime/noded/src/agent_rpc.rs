//! JSON calls to the Python agent's internal create endpoints over its Unix
//! socket: the admission it keeps owning (admit, finish), image
//! materialization and the create configuration. Connections are pooled; the
//! agent serves each on its own thread.

use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::client::conn::http1::SendRequest;
use hyper::header::{self, HeaderMap, HeaderValue};
use hyper::{Method, Request, StatusCode};
use hyper_util::rt::TokioIo;
use serde_json::Value;
use tokio::net::UnixStream;

/// Each daemon start is a new session: the agent releases admissions held
/// under any earlier session when it first sees this one.
pub const SESSION_HEADER: &str = "x-ucloud-noded-session";

#[derive(Debug)]
pub enum RpcError {
    /// Nothing reached the agent: safe to retry or forward.
    Unavailable(String),
    /// The request was written but no answer came back: the agent may have
    /// acted on it.
    Lost(String),
    /// A response that is not the JSON the contract promises.
    Protocol(String),
}

impl std::fmt::Display for RpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RpcError::Unavailable(message) => write!(f, "node agent is unavailable: {message}"),
            RpcError::Lost(message) => write!(f, "node agent did not answer: {message}"),
            RpcError::Protocol(message) => write!(f, "node agent answered outside its contract: {message}"),
        }
    }
}

impl std::error::Error for RpcError {}

/// One agent response, kept whole so an admission refusal can be relayed to
/// the client byte for byte.
#[derive(Debug, Clone)]
pub struct Reply {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: Bytes,
}

impl Reply {
    pub fn json(&self) -> Result<Value, RpcError> {
        serde_json::from_slice(&self.body).map_err(|error| RpcError::Protocol(error.to_string()))
    }
}

type Sender = SendRequest<Full<Bytes>>;

pub struct AgentClient {
    socket: PathBuf,
    authorization: HeaderValue,
    session: HeaderValue,
    timeout: Duration,
    idle: Mutex<Vec<Sender>>,
}

impl AgentClient {
    pub fn new(socket: PathBuf, token: &str, session: &str) -> Result<Self, RpcError> {
        let authorization = HeaderValue::from_str(&format!("Bearer {token}"))
            .map_err(|_| RpcError::Protocol("the node control token is not a valid header value".into()))?;
        let session = HeaderValue::from_str(session).map_err(|_| RpcError::Protocol("invalid session".into()))?;
        Ok(AgentClient { socket, authorization, session, timeout: Duration::from_secs(120), idle: Mutex::new(Vec::new()) })
    }

    async fn sender(&self) -> Result<Sender, RpcError> {
        loop {
            let pooled = self.idle.lock().expect("pool lock").pop();
            let Some(mut sender) = pooled else { break };
            if sender.ready().await.is_ok() {
                return Ok(sender);
            }
        }
        let stream = UnixStream::connect(&self.socket).await.map_err(|e| RpcError::Unavailable(e.to_string()))?;
        let (sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
            .await
            .map_err(|e| RpcError::Unavailable(e.to_string()))?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        Ok(sender)
    }

    pub async fn call(&self, method: Method, path: &str, body: Option<&Value>) -> Result<Reply, RpcError> {
        self.call_within(self.timeout, method, path, body).await
    }

    /// One request; a pooled connection the agent closed while idle is
    /// replaced once, resending only what hyper proves was never written.
    pub async fn call_within(&self, timeout: Duration, method: Method, path: &str, body: Option<&Value>) -> Result<Reply, RpcError> {
        let bytes = body.map(|value| Bytes::from(serde_json::to_vec(value).expect("JSON values encode"))).unwrap_or_default();
        let mut request = Request::builder()
            .method(method)
            .uri(path)
            .header(header::HOST, "localhost")
            .header(header::AUTHORIZATION, self.authorization.clone())
            .header(SESSION_HEADER, self.session.clone());
        if body.is_some() {
            request = request.header(header::CONTENT_TYPE, "application/json");
        }
        let mut request = request.body(Full::new(bytes)).map_err(|e| RpcError::Protocol(e.to_string()))?;
        let exchange = async {
            for attempt in 0..2 {
                let mut sender = self.sender().await?;
                match sender.try_send_request(request).await {
                    Ok(response) => {
                        let (parts, body) = response.into_parts();
                        let body = body.collect().await.map_err(|e| RpcError::Lost(e.to_string()))?.to_bytes();
                        self.idle.lock().expect("pool lock").push(sender);
                        return Ok(Reply { status: parts.status, headers: parts.headers, body });
                    }
                    Err(mut failed) => match failed.take_message() {
                        Some(unsent) if attempt == 0 => request = unsent,
                        Some(_) => return Err(RpcError::Unavailable(failed.into_error().to_string())),
                        None => return Err(RpcError::Lost(failed.into_error().to_string())),
                    },
                }
            }
            unreachable!("the second attempt always returns")
        };
        tokio::time::timeout(timeout, exchange).await.map_err(|_| RpcError::Lost("timed out".into()))?
    }

    pub async fn call_json(&self, method: Method, path: &str, body: Option<&Value>) -> Result<(StatusCode, Value), RpcError> {
        let reply = self.call(method, path, body).await?;
        Ok((reply.status, reply.json()?))
    }
}
