"""Managed-process job reads: held reads and the gateway's record cache.

docs/managed-process-reads.md. Status and log reads pass ``wait_ms``,
``after``, ``offset`` and ``limit`` through to the worker unchanged. A read
with ``wait_ms`` may be held there, so its worker deadline grows by the wait.
The gateway caches the record carried by successful reads; parked and
transitioning sandboxes are answered from that cache when the answer cannot
depend on bytes that exist only in the parked image.
"""

from __future__ import annotations

from http import HTTPStatus
import json
from typing import Any
from urllib.parse import parse_qs, urlparse

from ..http_contract import SandboxHttpRoute
from ..managed_process import MAX_LOG_READ_BYTES, ManagedProcessRecord
from ..routing import SandboxRoute, SandboxRouteConflictError
from .node_rpc import DEFAULT_PROXY_TIMEOUT_SECONDS, ProxiedResponse

JOB_READ_ACTIONS = frozenset({"job_status", "job_logs"})
MAX_JOB_READ_WAIT_MS = 25_000
# Nothing changes while a sandbox is parked: a waiting client backs off instead
# of being held by the gateway.
CACHED_READ_RETRY_AFTER = "1"


def job_read_wait_ms(http_route: SandboxHttpRoute | None, path: str) -> int | None:
    """The clamped wait of a job read, or None when the read does not wait.

    An unparseable value is the worker's to reject; the gateway neither holds
    nor rejects that read.
    """
    if http_route is None or http_route.action not in JOB_READ_ACTIONS:
        return None
    values = parse_qs(urlparse(path).query, keep_blank_values=True).get("wait_ms")
    if not values:
        return None
    try:
        wait_ms = int(values[0])
    except ValueError:
        return None
    return min(max(wait_ms, 0), MAX_JOB_READ_WAIT_MS)


def job_read_timeout_seconds(wait_ms: int | None) -> float:
    """The worker deadline: the wait plus the usual operation budget."""
    return DEFAULT_PROXY_TIMEOUT_SECONDS + (wait_ms or 0) / 1000


def job_record_payload(record: ManagedProcessRecord) -> dict[str, Any]:
    """A cached record as the worker serves it, cursor included."""
    return {
        **record.to_dict(),
        "cursor": f"{record.sequence}.{record.stdout_bytes}.{record.stderr_bytes}",
    }


def cached_job_status_response(
    routing_store: Any,
    route: SandboxRoute,
    http_route: SandboxHttpRoute | None,
    *,
    wait_ms: int | None,
    missing_status: HTTPStatus | None = None,
) -> ProxiedResponse | None:
    """The cached status of a job, or None when the cache cannot answer."""
    if http_route is None or http_route.action != "job_status":
        return None
    cached = routing_store.get_managed_process(
        route.sandbox_id, http_route.job_id, sandbox_generation=route.generation,
    )
    if cached is None:
        if missing_status is None:
            return None
        return json_response(
            {"error": "managed process state is not available"}, status=missing_status,
        )
    return json_response(
        {"job": job_record_payload(cached)},
        headers=_cached_read_headers(wait_ms),
    )


def cached_job_logs_response(
    routing_store: Any,
    route: SandboxRoute,
    http_route: SandboxHttpRoute | None,
    path: str,
    *,
    wait_ms: int | None,
) -> ProxiedResponse | None:
    """An empty chunk for a waiting logs read at or past the cached bytes.

    None when the read must reach the worker: it does not wait, has no cached
    record, is malformed (the worker rejects it), or asks for bytes below the
    cached count, which exist only in the sandbox.
    """
    if http_route is None or http_route.action != "job_logs" or wait_ms is None:
        return None
    query = parse_qs(urlparse(path).query, keep_blank_values=True)
    try:
        offset = int((query.get("offset") or ["0"])[0])
        limit = int((query.get("limit") or [str(MAX_LOG_READ_BYTES)])[0])
    except ValueError:
        return None
    if offset < 0 or not 1 <= limit <= MAX_LOG_READ_BYTES:
        return None
    cached = routing_store.get_managed_process(
        route.sandbox_id, http_route.job_id, sandbox_generation=route.generation,
    )
    if cached is None:
        return None
    retained = cached.stdout_bytes if http_route.stream == "stdout" else cached.stderr_bytes
    if offset < retained:
        return None
    # The worker's chunk shape: eof is next_offset >= the stream's size.
    return json_response(
        {
            "stream": http_route.stream,
            "offset": offset,
            "next_offset": offset,
            "data": "",
            "eof": True,
            "job": job_record_payload(cached),
        },
        headers=_cached_read_headers(wait_ms),
    )


def project_job_response(
    routing_store: Any,
    route: SandboxRoute,
    http_route: SandboxHttpRoute | None,
    path: str,
    response: ProxiedResponse,
    *,
    wait_ms: int | None,
) -> ProxiedResponse | None:
    """The gateway's own answer to a proxied job read, or None to relay it.

    A worker in a lifecycle transition is answered from the cache. A successful
    read's record is cached, fenced on generation and sequence; an invalid
    record is a 502.
    """
    if http_route is None or http_route.action not in JOB_READ_ACTIONS:
        return None
    if (
        response.status in {HTTPStatus.BAD_REQUEST, HTTPStatus.CONFLICT}
        and "lifecycle transition is in progress"
        in str(response.json().get("error") or "").lower()
    ):
        return (
            cached_job_status_response(routing_store, route, http_route, wait_ms=wait_ms)
            or cached_job_logs_response(routing_store, route, http_route, path, wait_ms=wait_ms)
        )
    return record_job_response(routing_store, route, http_route, response)


def record_job_response(
    routing_store: Any,
    route: SandboxRoute,
    http_route: SandboxHttpRoute,
    response: ProxiedResponse,
) -> ProxiedResponse | None:
    """Cache the record a successful job response carries; a 502 if invalid.

    Log responses from workers that predate managed-process reads carry no
    record and are relayed as they are.
    """
    if not 200 <= response.status < 300:
        return None
    payload = response.json()
    if http_route.action == "job_logs" and "job" not in payload:
        return None
    try:
        managed_record = ManagedProcessRecord.from_dict(payload.get("job"))
        if http_route.job_id and managed_record.job_id != http_route.job_id:
            raise ValueError("node returned another managed process")
        routing_store.upsert_managed_process(route, managed_record)
    except (SandboxRouteConflictError, TypeError, ValueError) as exc:
        return json_response(
            {"error": f"invalid managed process state from node: {exc}"},
            status=HTTPStatus.BAD_GATEWAY,
        )
    return None


def json_response(
    payload: dict[str, Any],
    *,
    status: int = HTTPStatus.OK,
    headers: dict[str, str] | None = None,
) -> ProxiedResponse:
    return ProxiedResponse(
        status,
        {"Content-Type": "application/json", **(headers or {})},
        json.dumps(payload, separators=(",", ":")).encode("utf-8"),
    )


def _cached_read_headers(wait_ms: int | None) -> dict[str, str]:
    return {"Retry-After": CACHED_READ_RETRY_AFTER} if wait_ms is not None else {}
