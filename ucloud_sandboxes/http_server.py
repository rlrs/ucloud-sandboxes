from __future__ import annotations

from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from functools import wraps
import json
import selectors
import socket
from threading import BoundedSemaphore, RLock
from time import monotonic
from typing import Any, Callable
from urllib.parse import unquote, urlparse

from opentelemetry import trace
from opentelemetry.trace import SpanKind, Status, StatusCode

from .telemetry import Telemetry, trace_id_hex


DEFAULT_HTTP_REQUEST_QUEUE_SIZE = 4096
DEFAULT_HTTP_CLIENT_SOCKET_TIMEOUT_SECONDS = 60.0
# Each of 128 resident sandboxes can hold an agent event poll and a tool poll.
# Reserve room beyond those sleeping requests for lifecycle and health traffic.
DEFAULT_MAX_HTTP_REQUEST_THREADS = 512
DEFAULT_MAX_JSON_BODY_BYTES = 16 * 1024 * 1024
HTTP_OVERLOAD_RETRY_AFTER_SECONDS = 1
HTTP_OVERLOAD_DRAIN_SECONDS = 2.0
HTTP_OVERLOAD_DRAIN_BYTES = DEFAULT_MAX_JSON_BODY_BYTES + 64 * 1024


class RequestBodyTooLargeError(ValueError):
    pass


class JsonHttpHandler(BaseHTTPRequestHandler):
    # All responses emitted by this base class are explicitly framed with a
    # Content-Length (or close the connection for an unbounded proxy stream),
    # so HTTP/1.1 keep-alive is safe.  This is important for the gateway's hot
    # polling paths, where reconnecting for every request is pure overhead.
    protocol_version = "HTTP/1.1"
    allow_http_keep_alive = True
    max_json_body_bytes = DEFAULT_MAX_JSON_BODY_BYTES
    telemetry: Telemetry | None = None

    def parse_request(self) -> bool:
        parsed = super().parse_request()
        if not parsed:
            return False
        if not self.allow_http_keep_alive:
            # ThreadingHTTPServer dedicates one thread to a connection, not to
            # an individual request. Public reverse proxies can otherwise
            # retain enough idle upstream keep-alives to exhaust the bounded
            # request pool while the gateway is doing no work.
            self.close_connection = True
        content_length = self.headers.get("Content-Length")
        if self.headers.get("Transfer-Encoding") or (
            content_length is not None and content_length.strip() != "0"
        ):
            # This must happen before dispatch: authorization, admission, and
            # overload paths can all answer without consuming the body.
            self.close_connection = True
        return True

    def _read_json_body(self) -> object:
        raw = self._read_raw_body(max_bytes=self.max_json_body_bytes).decode("utf-8")
        if not raw:
            raise ValueError("empty request body")
        try:
            return json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid JSON: {exc}") from exc

    def _read_raw_body(self, *, max_bytes: int) -> bytes:
        length = self._request_content_length(max_bytes=max_bytes)
        body = self.rfile.read(length)
        if len(body) != length:
            raise ValueError("request body ended before Content-Length bytes were read")
        return body

    def _request_content_length(self, *, max_bytes: int) -> int:
        if self.headers.get("Transfer-Encoding"):
            raise ValueError("Transfer-Encoding is not supported; use Content-Length")
        length_header = self.headers.get("Content-Length")
        if length_header is None:
            raise ValueError("Content-Length header is required")
        try:
            length = int(length_header)
        except ValueError as exc:
            raise ValueError("invalid Content-Length") from exc
        if length < 0:
            raise ValueError("Content-Length cannot be negative")
        if length > max_bytes:
            message = f"request body exceeds the {max_bytes} byte limit"
            raise RequestBodyTooLargeError(message)
        return length

    def _write_json(
        self,
        payload: dict[str, Any],
        *,
        status: int = HTTPStatus.OK,
        headers: dict[str, str] | None = None,
    ) -> None:
        # API responses are machine-consumed and some hot paths include large
        # inventories. Avoid sorting and pretty-print whitespace on every
        # heartbeat, exec poll and proxy response; CLI presentation remains
        # responsible for human-readable formatting.
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        self._write_bytes(body, "application/json", status=status, headers=headers)

    def _write_bytes(
        self,
        body: bytes,
        content_type: str,
        *,
        status: int = HTTPStatus.OK,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        for key, value in (headers or {}).items():
            if key.lower() not in {"content-length", "content-type"}:
                self.send_header(key, value)
        self.end_headers()
        self.wfile.write(body)

    def send_response(self, code: int, message: str | None = None) -> None:
        span = trace.get_current_span()
        span.set_attribute("http.response.status_code", int(code))
        if code >= 500:
            span.set_status(Status(StatusCode.ERROR, f"HTTP {code}"))
        super().send_response(code, message)

    def end_headers(self) -> None:
        if self.close_connection:
            self.send_header("Connection", "close")
        telemetry = self.telemetry
        if telemetry is not None:
            for key, value in telemetry.current_trace_headers().items():
                self.send_header(key, value)
            current_trace_id = trace_id_hex()
            if current_trace_id:
                self.send_header("X-Trace-Id", current_trace_id)
        super().end_headers()

    def log_message(self, _format: str, *args: object) -> None:
        del args


def traced_http_request(
    function: Callable[..., None],
) -> Callable[..., None]:
    """Wrap one HTTP handler without adding any work when telemetry is disabled."""

    @wraps(function)
    def wrapped(self: JsonHttpHandler, *args: Any, **kwargs: Any) -> None:
        telemetry = self.telemetry
        if telemetry is None or not telemetry.enabled:
            return function(self, *args, **kwargs)
        parsed = urlparse(self.path)
        route, object_attributes = _normalized_http_route(parsed.path)
        attributes: dict[str, Any] = {
            "http.request.method": self.command,
            "http.route": route,
            "url.path": parsed.path,
            **object_attributes,
        }
        content_length = self.headers.get("Content-Length")
        if content_length and content_length.isdigit():
            attributes["http.request.body.size"] = int(content_length)
        parent_context = telemetry.extracted_context(dict(self.headers.items()))
        with telemetry.span(
            f"{self.command} {route}",
            kind=SpanKind.SERVER,
            attributes=attributes,
            parent_context=parent_context,
            metric_operation="http.server.request",
        ):
            return function(self, *args, **kwargs)

    return wrapped


def _normalized_http_route(path: str) -> tuple[str, dict[str, str]]:
    parts = path.split("/")
    attributes: dict[str, str] = {}
    dynamic_collections = {
        "sandboxes": "sandbox.id",
        "exec": "exec.session.id",
        "jobs": "sandbox.job.id",
        "image-contexts": "image.context.digest",
        "prepare": "capacity.prepare.id",
        "builders": "builder.id",
    }
    for index in range(1, len(parts) - 1):
        label = dynamic_collections.get(parts[index])
        if label is None or not parts[index + 1]:
            continue
        raw = unquote(parts[index + 1])
        attributes[label] = raw[:256]
        placeholder = label.replace(".", "_")
        parts[index + 1] = "{" + placeholder + "}"
    return "/".join(parts), attributes


def _http_overload_response() -> bytes:
    body = json.dumps(
        {
            "error": "HTTP request capacity is exhausted; retry shortly",
            # The server has not dispatched this request to a handler. This
            # fence therefore makes replay safe even for otherwise mutating
            # methods such as exec start and sandbox delete.
            "error_code": "http_request_capacity_exhausted",
            "retryable": True,
        },
        separators=(",", ":"),
    ).encode("utf-8")
    headers = (
        "HTTP/1.1 503 Service Unavailable\r\n"
        "Content-Type: application/json\r\n"
        f"Content-Length: {len(body)}\r\n"
        f"Retry-After: {HTTP_OVERLOAD_RETRY_AFTER_SECONDS}\r\n"
        "X-UCloud-Sandbox-Retryable: true\r\n"
        "Connection: close\r\n"
        "\r\n"
    ).encode("ascii")
    return headers + body


HTTP_OVERLOAD_RESPONSE = _http_overload_response()


class HighBacklogThreadingHTTPServer(ThreadingHTTPServer):
    request_queue_size = DEFAULT_HTTP_REQUEST_QUEUE_SIZE
    daemon_threads = True
    allow_reuse_address = True

    def __init__(
        self,
        *args: Any,
        client_socket_timeout_seconds: float = (
            DEFAULT_HTTP_CLIENT_SOCKET_TIMEOUT_SECONDS
        ),
        max_request_threads: int = DEFAULT_MAX_HTTP_REQUEST_THREADS,
        **kwargs: Any,
    ) -> None:
        if client_socket_timeout_seconds <= 0:
            raise ValueError("client socket timeout must be positive")
        if max_request_threads <= 0:
            raise ValueError("max request threads must be positive")
        self.client_socket_timeout_seconds = float(client_socket_timeout_seconds)
        self.max_request_threads = int(max_request_threads)
        self._request_slots = BoundedSemaphore(self.max_request_threads)
        super().__init__(*args, **kwargs)
        self._overload_selector = selectors.DefaultSelector()
        self._overload_drains: dict[socket.socket, tuple[float, int]] = {}
        self._overload_lock = RLock()
        self._closing = False

    def get_request(self) -> tuple[socket.socket, Any]:
        client, address = super().get_request()
        client.settimeout(self.client_socket_timeout_seconds)
        return client, address

    def process_request(self, request: socket.socket, client_address: Any) -> None:
        if not self._request_slots.acquire(blocking=False):
            self._reject_overload(request)
            return
        try:
            super().process_request(request, client_address)
        except BaseException:
            self._request_slots.release()
            raise

    def _reject_overload(self, request: socket.socket) -> None:
        # Closing a socket with an unread POST body can reset the connection
        # before the caller sees our safe-retry response. Half-close the reply,
        # then discard incoming bytes without blocking the accept loop or
        # allocating another request thread. Time, bytes and sockets are bounded.
        try:
            request.setblocking(False)
            request.sendall(HTTP_OVERLOAD_RESPONSE)
        except OSError:
            super().shutdown_request(request)
            return
        self.shutdown_request(request)

    def shutdown_request(self, request: socket.socket) -> None:
        # Handler-level admission can also reject an unread upload. Apply the
        # same close discipline after handlers finish, not only at thread cap.
        with self._overload_lock:
            if self._closing or len(self._overload_drains) >= self.request_queue_size:
                super().shutdown_request(request)
                return
            try:
                request.setblocking(False)
                request.shutdown(socket.SHUT_WR)
                self._overload_selector.register(request, selectors.EVENT_READ)
                self._overload_drains[request] = (
                    monotonic() + HTTP_OVERLOAD_DRAIN_SECONDS, 0,
                )
            except OSError:
                super().shutdown_request(request)

    def _close_overload_drain(self, request: socket.socket) -> None:
        self._overload_selector.unregister(request)
        self._overload_drains.pop(request, None)
        self.close_request(request)

    def service_actions(self) -> None:
        super().service_actions()
        with self._overload_lock:
            self._drain_closed_requests()

    def _drain_closed_requests(self) -> None:
        for key, _events in self._overload_selector.select(timeout=0):
            request = key.fileobj
            deadline, received = self._overload_drains[request]
            try:
                chunk = request.recv(64 * 1024)
            except BlockingIOError:
                continue
            except OSError:
                chunk = b""
            received += len(chunk)
            if not chunk or received >= HTTP_OVERLOAD_DRAIN_BYTES:
                self._close_overload_drain(request)
            else:
                self._overload_drains[request] = (deadline, received)
        now = monotonic()
        for request, (deadline, _received) in list(self._overload_drains.items()):
            if now >= deadline:
                self._close_overload_drain(request)

    def server_close(self) -> None:
        # HTTPServer.__init__ also calls server_close if bind/activate fails.
        if hasattr(self, "_overload_selector"):
            with self._overload_lock:
                self._closing = True
                for request in list(self._overload_drains):
                    self._close_overload_drain(request)
                self._overload_selector.close()
        super().server_close()

    def process_request_thread(
        self,
        request: socket.socket,
        client_address: Any,
    ) -> None:
        try:
            super().process_request_thread(request, client_address)
        finally:
            self._request_slots.release()
