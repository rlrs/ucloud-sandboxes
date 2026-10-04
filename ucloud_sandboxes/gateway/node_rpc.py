"""Gateway-to-worker HTTP: pools, bounded proxy RPCs and their error contract."""

from __future__ import annotations

from http import HTTPStatus
import json
import socket
from typing import Any, Callable
from urllib import error, request
from urllib.parse import urlparse

import urllib3
from urllib3.exceptions import HTTPError as Urllib3HTTPError
from urllib3.exceptions import EmptyPoolError

from ..http_contract import match_sandbox_http_route
from ..http_server import DEFAULT_MAX_HTTP_REQUEST_THREADS, RequestBodyStream, TRANSFER_CHUNK_BYTES
from ..telemetry import Telemetry


DEFAULT_PROXY_TIMEOUT_SECONDS = 60
NODE_CONNECT_TIMEOUT_SECONDS = 5
DEFAULT_MAX_PROXY_RESPONSE_BYTES = 16 * 1024 * 1024
DEFAULT_MAX_PROXY_ERROR_BYTES = 1024 * 1024
PROXY_STREAM_CHUNK_BYTES = 64 * 1024
NODE_HTTP_POOL_CONNECTIONS_PER_ORIGIN = 128
NODE_HTTP_POOL_ORIGINS = 64


class ProxyResponseTooLargeError(RuntimeError):
    pass


class ProxiedResponse:
    def __init__(
        self,
        status: int,
        headers: Any,
        body: bytes,
        *,
        transport_error_kind: str = "",
    ) -> None:
        self.status = status
        self.headers = headers
        self.body = body
        self.transport_error_kind = transport_error_kind

    def json(self) -> dict[str, Any]:
        try:
            decoded = json.loads(self.body.decode("utf-8")) if self.body else {}
        except (UnicodeDecodeError, json.JSONDecodeError):
            return {}
        return decoded if isinstance(decoded, dict) else {}


_NODE_HTTP_POOL = urllib3.PoolManager(
    num_pools=NODE_HTTP_POOL_ORIGINS,
    maxsize=NODE_HTTP_POOL_CONNECTIONS_PER_ORIGIN,
    block=True,
    retries=False,
)
# Long-lived agent/tool event polls must not consume the connections needed to
# upload files, launch tools, or perform lifecycle calls on the same worker.
_NODE_EXEC_EVENT_HTTP_POOL = urllib3.PoolManager(
    num_pools=NODE_HTTP_POOL_ORIGINS,
    maxsize=256,
    block=True,
    retries=False,
)
# Uploads spend most of their time moving bytes. Do not let them exhaust the
# control/exec connection pool. maxsize here limits retained connections only;
# the existing HTTP request admission bounds active transfer threads.
_NODE_FILE_UPLOAD_HTTP_POOL = urllib3.PoolManager(
    # Match the framed reader: urllib3 otherwise asks for only 16 KiB per
    # send, multiplying Python/socket handoffs during concurrent uploads.
    blocksize=TRANSFER_CHUNK_BYTES,
    num_pools=NODE_HTTP_POOL_ORIGINS,
    maxsize=DEFAULT_MAX_HTTP_REQUEST_THREADS,
    block=False,
    retries=False,
)


def _node_request_headers(req, *, allow_body_keep_alive=False):
    headers = dict(req.header_items())
    if req.data is not None and (
        not allow_body_keep_alive or isinstance(req.data, RequestBodyStream)
    ):
        # Legacy nodes and streaming uploads remain self-contained. A new node
        # advertises that only completely consumed framed bodies permit reuse.
        headers["Connection"] = "close"
    return headers


def _open_node_request(
    req: request.Request,
    *,
    timeout: float,
    authenticated: bool = False,
    allow_body_keep_alive: bool = False,
    buffer_response_bytes: int | None = None,
) -> Any:
    # Authenticated node calls must never carry the deployment credential to a
    # redirect target selected by a compromised node endpoint.
    if authenticated:
        try:
            headers = _node_request_headers(req, allow_body_keep_alive=allow_body_keep_alive)
            path = urlparse(req.full_url).path
            if buffer_response_bytes is not None and not isinstance(req.data, RequestBodyStream):
                from ..node_http_async import node_http_pool
                return node_http_pool.request(
                    req.get_method(), req.full_url, headers=headers, body=req.data,
                    timeout=timeout, connect_timeout=min(timeout, NODE_CONNECT_TIMEOUT_SECONDS),
                    response_limit=buffer_response_bytes,
                    event_poll=(req.get_method() == "GET" and path.startswith("/v1/exec/")
                                and path.endswith("/events")),
                )
            pool = (
                _NODE_FILE_UPLOAD_HTTP_POOL
                if isinstance(req.data, RequestBodyStream)
                else _NODE_EXEC_EVENT_HTTP_POOL
                if req.get_method() == "GET"
                and path.startswith("/v1/exec/")
                and path.endswith("/events")
                else _NODE_HTTP_POOL
            )
            return pool.request(
                req.get_method(),
                req.full_url,
                body=req.data,
                headers=headers,
                redirect=False,
                retries=False,
                preload_content=False,
                pool_timeout=min(timeout, NODE_CONNECT_TIMEOUT_SECONDS),
                timeout=urllib3.Timeout(
                    connect=min(timeout, NODE_CONNECT_TIMEOUT_SECONDS), read=timeout
                ),
            )
        except Urllib3HTTPError as exc:
            raise error.URLError(exc) from exc
    return request.urlopen(req, timeout=timeout)


def build_request(
    node_url: str, path: str, *, method: str, body: Any, forwarded_headers: Any,
    extra_headers: dict[str, str] | None, node_token: str, telemetry: Telemetry | None,
) -> request.Request:
    """Forward client headers but never a public credential to a worker."""
    headers = {
        key: value
        for key, value in forwarded_headers.items()
        if key.lower() not in {
            "host", "content-length", "connection",
            "authorization", "proxy-authorization", "x-ucloud-sandbox-token",
            "x-ucloud-admission-wait",  # The gateway's own, never a client's.
        }
    }
    headers.update(extra_headers or {})
    # Public gateway credentials are never node credentials. Override any
    # caller-provided auth header with the private control-plane credential.
    for key in list(headers):
        if key.lower() in {"authorization", "proxy-authorization", "x-ucloud-sandbox-token"}:
            del headers[key]
    headers["Authorization"] = f"Bearer {node_token}"
    if telemetry is not None:
        telemetry.inject(headers)
    return request.Request(
        node_url.rstrip("/") + path, data=body, method=method, headers=headers)


def proxy(
    proxied: request.Request, node_url: str, path: str, *, method: str, body: Any,
    timeout_seconds: float, telemetry: Telemetry, allow_body_keep_alive: bool,
    on_upload_consumed: Callable[[], None],
) -> ProxiedResponse:
    """One bounded worker RPC; the caller owns the request's transport flags."""

    proxy_attributes = _node_proxy_span_attributes(method, path, node_url)
    if isinstance(body, RequestBodyStream):
        proxy_attributes.update({"upload.streaming": True, "upload.bytes": body.length})
    try:
        with telemetry.span(
            "gateway.node_response_headers", attributes=proxy_attributes,
        ) as headers_span:
            try:
                response = _open_node_request(
                    proxied, timeout=timeout_seconds, authenticated=True,
                    buffer_response_bytes=DEFAULT_MAX_PROXY_RESPONSE_BYTES,
                    allow_body_keep_alive=allow_body_keep_alive,
                )
            finally:
                if isinstance(body, RequestBodyStream):
                    headers_span.set_attribute("upload.received_bytes", body.length - body.remaining)
                    if body.remaining == 0:
                        # The framed upload was consumed even though it did
                        # not use _read_raw_body. Avoid treating its socket
                        # as an early rejection that still needs draining.
                        on_upload_consumed()
            headers_span.set_attribute("http.response.status_code", response.status)
        with response:
            try:
                with telemetry.span("gateway.node_response_body", attributes={
                    **proxy_attributes, "http.response.status_code": response.status,
                }) as body_span:
                    response_body = _read_bounded_proxy_body(
                        response, max_bytes=DEFAULT_MAX_PROXY_RESPONSE_BYTES)
                    body_span.set_attribute("http.response.body.size", len(response_body))
            except ProxyResponseTooLargeError:
                return _proxy_response_too_large(DEFAULT_MAX_PROXY_RESPONSE_BYTES)
            return ProxiedResponse(response.status, response.headers, response_body)
    except error.HTTPError as exc:
        try:
            with telemetry.span("gateway.node_response_body", attributes={
                **proxy_attributes, "http.response.status_code": exc.code,
            }) as body_span:
                response_body = _read_bounded_proxy_body(
                    exc, max_bytes=DEFAULT_MAX_PROXY_ERROR_BYTES)
                body_span.set_attribute("http.response.body.size", len(response_body))
        except ProxyResponseTooLargeError:
            return _proxy_response_too_large(DEFAULT_MAX_PROXY_ERROR_BYTES)
        return ProxiedResponse(exc.code, exc.headers, response_body)
    except ValueError as exc:
        if not isinstance(body, RequestBodyStream):
            raise
        return ProxiedResponse(
            HTTPStatus.BAD_REQUEST,
            {"Content-Type": "application/json"},
            json.dumps({"error": str(exc)}).encode(),
        )
    except error.URLError as exc:
        return _node_transport_error_response(exc.reason)
    except (OSError, Urllib3HTTPError) as exc:
        # With preload_content=False, read/protocol failures can occur
        # after headers, outside _open_node_request's exception wrapper.
        return _node_transport_error_response(exc)


def _async_proxy_response(response, transport_error):
    """Apply the normal bounded/error contract to a completed async RPC."""
    if transport_error is not None:
        proxied = _node_transport_error_response(transport_error)
    else:
        with response:
            body = response.read()
            proxied = (_proxy_response_too_large(DEFAULT_MAX_PROXY_RESPONSE_BYTES)
                       if len(body) > DEFAULT_MAX_PROXY_RESPONSE_BYTES
                       else ProxiedResponse(response.status, response.headers, body))
    structured = _structured_proxy_error(proxied)
    if structured is not None:
        return proxied.status, {"Content-Type": "application/json"}, json.dumps(structured).encode()
    return proxied.status, proxied.headers, proxied.body


def _node_proxy_span_attributes(
    method: str,
    path: str,
    node_url: str,
) -> dict[str, str]:
    """Return bounded-cardinality attributes for gateway-to-node phases."""

    parsed_path = urlparse(path).path
    sandbox_route = match_sandbox_http_route(method, parsed_path)
    if sandbox_route is not None:
        route = f"sandbox.{sandbox_route.action}"
    elif parsed_path.startswith("/v1/exec/"):
        route = "exec.session"
    elif parsed_path.startswith("/v1/sandboxes/"):
        route = "sandbox.internal"
    else:
        route = parsed_path
    node = urlparse(node_url)
    return {
        "http.request.method": method.upper(),
        "http.route": route,
        "server.address": node.hostname or "",
    }


def _node_transport_error_response(reason: object) -> ProxiedResponse:
    if isinstance(reason, EmptyPoolError):
        # urllib3 failed to acquire a connection: no request bytes were sent.
        # Preserve that certainty so mutations can retry safely.
        return ProxiedResponse(
            HTTPStatus.SERVICE_UNAVAILABLE,
            {"Content-Type": "application/json", "Retry-After": "1"},
            json.dumps({
                "error": "sandbox node HTTP connection capacity is exhausted",
                "error_code": "http_request_capacity_exhausted",
                "retryable": True,
            }).encode("utf-8"),
        )
    message = str(reason)
    lowered = message.lower()
    if isinstance(reason, socket.gaierror) or any(
        marker in lowered
        for marker in (
            "name resolution",
            "name or service not known",
            "nodename nor servname provided",
        )
    ):
        status = HTTPStatus.SERVICE_UNAVAILABLE
        code = "node_dns_unavailable"
        error_message = (
            "sandbox node DNS is temporarily unavailable; its UCloud VM may be "
            "suspended and resuming"
        )
        kind = "dns"
    elif isinstance(reason, (TimeoutError, socket.timeout)) or "timed out" in lowered:
        status = HTTPStatus.GATEWAY_TIMEOUT
        code = "node_request_timeout"
        error_message = "sandbox node request timed out"
        kind = "timeout"
    else:
        status = HTTPStatus.BAD_GATEWAY
        code = "node_transport_error"
        error_message = f"sandbox node request failed: {message}"
        kind = "transport"
    body = json.dumps(
        {
            "error": error_message,
            "code": code,
            "retryable": True,
        }
    ).encode("utf-8")
    return ProxiedResponse(
        status,
        {"Content-Type": "application/json"},
        body,
        transport_error_kind=kind,
    )


def _proxy_response_too_large(max_bytes: int) -> ProxiedResponse:
    body = json.dumps(
        {
            "error": "upstream sandbox node response exceeded the gateway limit",
            "max_bytes": max_bytes,
            "retryable": False,
        }
    ).encode("utf-8")
    return ProxiedResponse(
        HTTPStatus.BAD_GATEWAY,
        {"Content-Type": "application/json"},
        body,
    )


def _proxy_content_length(headers: Any) -> int | None:
    raw = _header_value(headers, "Content-Length").strip()
    if not raw:
        return None
    try:
        length = int(raw)
    except ValueError as exc:
        raise ValueError("invalid Content-Length") from exc
    if length < 0:
        raise ValueError("negative Content-Length")
    return length


def _read_bounded_proxy_body(response: Any, *, max_bytes: int) -> bytes:
    content_length = _proxy_content_length(response.headers)
    if content_length is not None and content_length > max_bytes:
        raise ProxyResponseTooLargeError(
            f"upstream response exceeds the {max_bytes} byte limit"
        )
    chunks: list[bytes] = []
    total = 0
    while True:
        chunk = response.read(min(PROXY_STREAM_CHUNK_BYTES, max_bytes + 1 - total))
        if not chunk:
            break
        total += len(chunk)
        if total > max_bytes:
            raise ProxyResponseTooLargeError(
                f"upstream response exceeds the {max_bytes} byte limit"
            )
        chunks.append(chunk)
    return b"".join(chunks)


def _structured_proxy_error(response: ProxiedResponse) -> dict[str, Any] | None:
    if response.status < 400 or _response_looks_json(response):
        return None
    preview = response.body[:500].decode("utf-8", errors="replace").strip()
    return {
        "error": "upstream sandbox node returned a non-JSON error response",
        "status": int(response.status),
        "retryable": response.status in {408, 425, 429, 500, 502, 503, 504},
        "upstream_content_type": _header_value(response.headers, "Content-Type"),
        "upstream_body_preview": preview,
    }


def _response_looks_json(response: ProxiedResponse) -> bool:
    content_type = _header_value(response.headers, "Content-Type").lower()
    if "json" in content_type:
        return True
    stripped = response.body.lstrip()
    return stripped.startswith(b"{") or stripped.startswith(b"[")


def _header_value(headers: Any, key: str) -> str:
    try:
        value = headers.get(key, "")
    except AttributeError:
        value = ""
    return str(value or "")
