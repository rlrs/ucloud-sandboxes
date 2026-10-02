"""The request-scoped I/O a gateway use case may drive."""

from __future__ import annotations

from typing import Any, Callable, Protocol

from ..models import NodeHeartbeat
from .node_rpc import ProxiedResponse


class Exchange(Protocol):
    """One in-flight gateway HTTP request; ControlPlaneHandler implements it.

    Use cases are shared by every request thread, so they receive an Exchange
    per call and never store it. ``_proxy_request`` and ``_heartbeat_for_route``
    carry per-request transport state (the pooled body origin and whether the
    request body was consumed), which is why node RPCs go through the Exchange.
    """

    command: str
    path: str
    headers: Any
    rfile: Any
    close_connection: bool

    def _read_raw_body(self, *, max_bytes: int) -> bytes: ...
    def _request_content_length(self, *, max_bytes: int) -> int: ...
    def _write_json(self, payload: dict[str, Any], *, status: int = ...,
                    headers: dict[str, str] | None = None) -> None: ...
    def _write_bytes(self, body: bytes, content_type: str, *, status: int = ...,
                     headers: dict[str, str] | None = None) -> None: ...
    def _send_proxied_response(self, response: ProxiedResponse, *,
                               extra_headers: dict[str, str] | None = None) -> None: ...
    def _stream_proxy_request(self, node_url: str, path: str, *, method: str, body: Any = None,
                              timeout_seconds: float = ...,
                              extra_headers: dict[str, str] | None = None,
                              on_success: Callable[[], None] | None = None) -> None: ...
    def _defer_node_response(self, node_url: str, path: str, *, method: str, body: Any = None,
                             extra_headers: dict[str, str] | None = None,
                             event_poll: bool = False) -> bool: ...
    def _defer_placement(self, kind: str, sandbox_id: str, path: str, body: bytes) -> None: ...
    def _proxy_request(self, node_url: str, path: str, *, method: str, body: Any = None,
                       timeout_seconds: float = ...,
                       extra_headers: dict[str, str] | None = None) -> ProxiedResponse: ...
    def _heartbeat_for_route(self, *, job_id: str,
                             include_inventory: bool = True) -> NodeHeartbeat | None: ...
    def log_error(self, format: str, *args: Any) -> None: ...
