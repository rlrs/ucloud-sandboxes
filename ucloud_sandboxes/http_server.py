from __future__ import annotations

from http.server import ThreadingHTTPServer
import json
import socket
from threading import BoundedSemaphore
from typing import Any


DEFAULT_HTTP_REQUEST_QUEUE_SIZE = 1024
DEFAULT_HTTP_CLIENT_SOCKET_TIMEOUT_SECONDS = 60.0
DEFAULT_MAX_HTTP_REQUEST_THREADS = 256
HTTP_OVERLOAD_RETRY_AFTER_SECONDS = 1


def _http_overload_response() -> bytes:
    body = json.dumps(
        {
            "error": "HTTP request capacity is exhausted; retry shortly",
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

    def get_request(self) -> tuple[socket.socket, Any]:
        client, address = super().get_request()
        client.settimeout(self.client_socket_timeout_seconds)
        return client, address

    def process_request(self, request: socket.socket, client_address: Any) -> None:
        if not self._request_slots.acquire(blocking=False):
            # Never make an upstream proxy infer service unavailability from a
            # bare connection close. UCloud renders that transport failure as
            # an HTML "Job is unavailable" 503, which callers cannot classify
            # or retry reliably. A small best-effort response keeps overload in
            # the API protocol while the accept loop continues draining the
            # kernel backlog.
            try:
                request.sendall(HTTP_OVERLOAD_RESPONSE)
            except OSError:
                pass
            self.shutdown_request(request)
            return
        try:
            super().process_request(request, client_address)
        except BaseException:
            self._request_slots.release()
            raise

    def process_request_thread(
        self,
        request: socket.socket,
        client_address: Any,
    ) -> None:
        try:
            super().process_request_thread(request, client_address)
        finally:
            self._request_slots.release()
