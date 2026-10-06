//! Client for the storage-native node service (`StorageNativeNodeClient` in
//! ucloud_sandboxes/storage_native_daemon.py), the agent's only storage protocol.
//!
//! One request per connection: a 4-byte big-endian length, then JSON, each way
//! (at most 1 MiB). Requests carry `"schema": 4` and always `trace_context`;
//! the server rejects any other key set. Retries are idempotent by the caller's
//! `operation_id`, which the server scopes per step and request.

use std::path::{Path, PathBuf};
use std::time::Duration;

use serde_json::{Map, Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::time::timeout;

pub const PROTOCOL_SCHEMA: u64 = 4;
pub const MAX_FRAME_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageError {
    /// The operation id is mid-flight on the server; it will not be replayed blindly.
    Pending(String),
    Capacity(String),
    /// Includes "volume does not exist" (GetVolume) and revision conflicts.
    Conflict(String),
    Terminal(String),
    /// Any other failure: transport, framing, or an `error` status.
    Node(String),
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageError::Pending(m) => write!(f, "storage-native pending: {m}"),
            StorageError::Capacity(m) => write!(f, "storage-native capacity: {m}"),
            StorageError::Conflict(m) => write!(f, "storage-native conflict: {m}"),
            StorageError::Terminal(m) => write!(f, "storage-native terminal: {m}"),
            StorageError::Node(m) => write!(f, "storage-native error: {m}"),
        }
    }
}

impl std::error::Error for StorageError {}

/// The owner every lifecycle operation names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VolumeOwner {
    pub volume_id: String,
    pub sandbox_id: String,
    pub sandbox_generation: u64,
}

#[derive(Debug, Clone)]
pub struct StorageClient {
    socket: PathBuf,
    /// Bounds connect and each read and write, as Python's `settimeout` does.
    io_timeout: Duration,
}

impl StorageClient {
    pub fn new(socket: impl Into<PathBuf>) -> Self {
        StorageClient { socket: socket.into(), io_timeout: Duration::from_secs(120) }
    }

    pub fn socket(&self) -> &Path {
        &self.socket
    }

    /// Send one operation. `fields` are the operation's own keys; the envelope
    /// keys are added here. `trace_context` holds W3C `traceparent`/`tracestate`.
    pub async fn call(
        &self,
        operation: &str,
        fields: Map<String, Value>,
        trace_context: Map<String, Value>,
    ) -> Result<Map<String, Value>, StorageError> {
        let mut request = fields;
        request.insert("operation".into(), json!(operation));
        request.insert("schema".into(), json!(PROTOCOL_SCHEMA));
        request.insert("trace_context".into(), Value::Object(trace_context));
        let payload = canonical_json(&Value::Object(request));
        if payload.len() > MAX_FRAME_BYTES {
            return Err(StorageError::Node("storage-native service request is too large".into()));
        }
        let node = |error: std::io::Error| StorageError::Node(error.to_string());
        let mut stream = timeout(self.io_timeout, UnixStream::connect(&self.socket))
            .await
            .map_err(|_| StorageError::Node("timed out connecting to the storage-native service".into()))?
            .map_err(node)?;
        let mut frame = Vec::with_capacity(4 + payload.len());
        frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        frame.extend_from_slice(&payload);
        timeout(self.io_timeout, stream.write_all(&frame))
            .await
            .map_err(|_| StorageError::Node("timed out writing to the storage-native service".into()))?
            .map_err(node)?;
        let mut header = [0u8; 4];
        self.read_exact(&mut stream, &mut header).await?;
        let length = u32::from_be_bytes(header) as usize;
        if length > MAX_FRAME_BYTES {
            return Err(StorageError::Node("storage-native service response is too large".into()));
        }
        let mut body = vec![0u8; length];
        self.read_exact(&mut stream, &mut body).await?;
        let response: Value = serde_json::from_slice(&body)
            .map_err(|error| StorageError::Node(format!("invalid storage-native response: {error}")))?;
        parse_response(response)
    }

    async fn read_exact(&self, stream: &mut UnixStream, buffer: &mut [u8]) -> Result<(), StorageError> {
        match timeout(self.io_timeout, stream.read_exact(buffer)).await {
            Err(_) => Err(StorageError::Node("timed out reading from the storage-native service".into())),
            Ok(Err(error)) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
                Err(StorageError::Node("storage-native peer closed the connection early".into()))
            }
            Ok(Err(error)) => Err(StorageError::Node(error.to_string())),
            Ok(Ok(_)) => Ok(()),
        }
    }

    /// `PrepareVolume`: create or converge the sandbox's workspace to mounted.
    /// `granted_size` is sent only when the workspace starts below its full size.
    pub async fn prepare_volume(
        &self,
        owner: &VolumeOwner,
        operation_id: &str,
        virtual_size: u64,
        granted_size: Option<u64>,
        trace_context: Map<String, Value>,
    ) -> Result<Map<String, Value>, StorageError> {
        let mut fields = owner_fields(owner);
        fields.insert("operation_id".into(), json!(operation_id));
        fields.insert("virtual_size".into(), json!(virtual_size));
        if let Some(granted) = granted_size {
            fields.insert("granted_size".into(), json!(granted));
        }
        record(self.call("PrepareVolume", fields, trace_context).await?)
    }

    /// `GetVolume`: `Conflict` means the volume does not exist.
    pub async fn get_volume(
        &self,
        volume_id: &str,
        trace_context: Map<String, Value>,
    ) -> Result<Map<String, Value>, StorageError> {
        let mut fields = Map::new();
        fields.insert("volume_id".into(), json!(volume_id));
        record(self.call("GetVolume", fields, trace_context).await?)
    }
}

fn owner_fields(owner: &VolumeOwner) -> Map<String, Value> {
    let mut fields = Map::new();
    fields.insert("sandbox_generation".into(), json!(owner.sandbox_generation));
    fields.insert("sandbox_id".into(), json!(owner.sandbox_id));
    fields.insert("volume_id".into(), json!(owner.volume_id));
    fields
}

fn record(result: Map<String, Value>) -> Result<Map<String, Value>, StorageError> {
    match result.get("record") {
        Some(Value::Object(record)) => Ok(record.clone()),
        _ => Err(StorageError::Node("storage-native response has no record".into())),
    }
}

/// Map the response envelope: `{"status":"ok","result":{...}}` or a failure status.
pub fn parse_response(response: Value) -> Result<Map<String, Value>, StorageError> {
    let Value::Object(mut response) = response else {
        return Err(StorageError::Node("storage-native response is not an object".into()));
    };
    let status = response.get("status").and_then(Value::as_str).unwrap_or("").to_string();
    if status == "ok" {
        return match response.remove("result") {
            Some(Value::Object(result)) => Ok(result),
            _ => Err(StorageError::Node("storage-native result is not an object".into())),
        };
    }
    let message = response
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("storage-native operation failed")
        .to_string();
    Err(match status.as_str() {
        "pending" => StorageError::Pending(message),
        "capacity" => StorageError::Capacity(message),
        "conflict" => StorageError::Conflict(message),
        "terminal" => StorageError::Terminal(message),
        _ => StorageError::Node(message),
    })
}

/// Python's `json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)`.
pub fn canonical_json(value: &Value) -> Vec<u8> {
    let mut out = Vec::new();
    write_canonical(value, &mut out);
    out
}

fn write_canonical(value: &Value, out: &mut Vec<u8>) {
    match value {
        Value::Object(map) => {
            out.push(b'{');
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            for (index, key) in keys.into_iter().enumerate() {
                if index > 0 {
                    out.push(b',');
                }
                write_string(key, out);
                out.push(b':');
                write_canonical(&map[key], out);
            }
            out.push(b'}');
        }
        Value::Array(items) => {
            out.push(b'[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(b',');
                }
                write_canonical(item, out);
            }
            out.push(b']');
        }
        Value::String(text) => write_string(text, out),
        other => out.extend_from_slice(other.to_string().as_bytes()),
    }
}

fn write_string(text: &str, out: &mut Vec<u8>) {
    out.push(b'"');
    for character in text.chars() {
        match character {
            '"' => out.extend_from_slice(b"\\\""),
            '\\' => out.extend_from_slice(b"\\\\"),
            '\n' => out.extend_from_slice(b"\\n"),
            '\r' => out.extend_from_slice(b"\\r"),
            '\t' => out.extend_from_slice(b"\\t"),
            '\u{08}' => out.extend_from_slice(b"\\b"),
            '\u{0c}' => out.extend_from_slice(b"\\f"),
            c if (c as u32) < 0x20 || (c as u32) > 0x7e => {
                let mut units = [0u16; 2];
                for unit in c.encode_utf16(&mut units) {
                    out.extend_from_slice(format!("\\u{unit:04x}").as_bytes());
                }
            }
            c => out.push(c as u8),
        }
    }
    out.push(b'"');
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::UnixListener;

    fn trace() -> Map<String, Value> {
        let mut context = Map::new();
        context.insert(
            "traceparent".into(),
            json!("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"),
        );
        context
    }

    #[test]
    fn canonical_json_matches_python() {
        let value = json!({"b": 1, "a": ["x\u{0}y", "é", "\u{1F600}"], "c": {"z": true, "y": null}});
        assert_eq!(
            String::from_utf8(canonical_json(&value)).unwrap(),
            r#"{"a":["x\u0000y","\u00e9","\ud83d\ude00"],"b":1,"c":{"y":null,"z":true}}"#
        );
    }

    #[tokio::test]
    async fn prepare_volume_sends_the_exact_frame_and_maps_errors() {
        let dir = std::env::temp_dir().join(format!("noded-storage-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let socket = dir.join("service.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let server = tokio::spawn(async move {
            let mut requests = Vec::new();
            for reply in [
                r#"{"result":{"record":{"accounting_id":12,"state":"mounted"}},"status":"ok"}"#,
                r#"{"message":"storage-native volume does not exist","status":"conflict"}"#,
                r#"{"status":"terminal"}"#,
            ] {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut header = [0u8; 4];
                stream.read_exact(&mut header).await.unwrap();
                let mut body = vec![0u8; u32::from_be_bytes(header) as usize];
                stream.read_exact(&mut body).await.unwrap();
                requests.push(String::from_utf8(body).unwrap());
                let mut frame = (reply.len() as u32).to_be_bytes().to_vec();
                frame.extend_from_slice(reply.as_bytes());
                stream.write_all(&frame).await.unwrap();
            }
            requests
        });
        let client = StorageClient::new(&socket);
        let owner = VolumeOwner { volume_id: "vol-1".into(), sandbox_id: "sb-1".into(), sandbox_generation: 1 };
        let record = client.prepare_volume(&owner, "op-1", 10737418240, None, trace()).await.unwrap();
        assert_eq!(record["accounting_id"], 12);
        assert_eq!(
            client.get_volume("vol-1", Map::new()).await,
            Err(StorageError::Conflict("storage-native volume does not exist".into()))
        );
        assert_eq!(
            client.get_volume("vol-1", Map::new()).await,
            Err(StorageError::Terminal("storage-native operation failed".into()))
        );
        let requests = server.await.unwrap();
        // Byte for byte the Python client's frame (protocols spec §1a).
        assert_eq!(
            requests[0],
            r#"{"operation":"PrepareVolume","operation_id":"op-1","sandbox_generation":1,"sandbox_id":"sb-1","schema":4,"trace_context":{"traceparent":"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"},"virtual_size":10737418240,"volume_id":"vol-1"}"#
        );
        assert_eq!(requests[1], r#"{"operation":"GetVolume","schema":4,"trace_context":{},"volume_id":"vol-1"}"#);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
