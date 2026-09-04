from __future__ import annotations

import asyncio
import base64
from collections import deque
from contextlib import contextmanager, suppress
from dataclasses import dataclass, field, replace
import heapq
import hashlib
import hmac
import json
import logging
import os
from pathlib import Path
import re
import sqlite3
from threading import Lock
import time
from typing import Any, Awaitable, Callable, TypeVar
from uuid import uuid4

from aiohttp import web
from opentelemetry.trace import SpanKind

from .deployment import service_health
from .telemetry import Telemetry, trace_id_hex


JsonObject = dict[str, Any]
LOGGER = logging.getLogger(__name__)
ROLLOUT_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
WORKER_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@-]{0,127}$")
REGISTRATION_TOKEN_RE = re.compile(r"^[0-9a-f]{32}$")
IDEMPOTENCY_KEY_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,127}$")
SANDBOX_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")
AGENT_LIFECYCLE_METADATA_KEY = "_ucloud_agent_lifecycle"
MANAGED_AGENT_LIFECYCLE = "managed-process-v1"
DEFAULT_RELAY_REQUEST_TIMEOUT_SECONDS = 3600.0
DEFAULT_WORKER_POLL_TIMEOUT_SECONDS = 30.0
DEFAULT_WORKER_LEASE_SECONDS = 600.0
DEFAULT_MAINTENANCE_INTERVAL_SECONDS = 1.0
DEFAULT_COMPLETED_REQUEST_RETENTION_SECONDS = 3600.0
DEFAULT_WORKER_RETENTION_SECONDS = 3600.0
DEFAULT_MAX_INFLIGHT_REQUESTS = 4096
DEFAULT_MAX_INFLIGHT_REQUESTS_PER_ROLLOUT = 1024
DEFAULT_MAX_INFLIGHT_BYTES = 128 * 1024**2
DEFAULT_MAX_COMPLETED_REQUESTS = 8192
DEFAULT_MAX_COMPLETED_BYTES = 256 * 1024**2
DEFAULT_MAX_WORKERS = 4096
EXPIRY_HEAP_COMPACTION_SLACK = 256
LEASE_HEAP_COMPACTION_SLACK = 64
MAX_TRANSIENT_WORKER_DELIVERIES = 3
MAX_RELAY_BODY_BYTES = 32 * 1024**2
MAX_WORKER_RESPONSE_BYTES = 32 * 1024**2
RELAY_TOKEN_HEADER = "X-UCloud-Relay-Token"
RELAY_REQUEST_ID_HEADER = "X-UCloud-Relay-Request-Id"
TUNNEL_HTTP_METHODS = frozenset(
    {"DELETE", "GET", "HEAD", "OPTIONS", "PATCH", "POST", "PUT"}
)
SANDBOX_TOKEN_KEY = web.AppKey("model_relay_sandbox_token", str | None)
WORKER_TOKEN_KEY = web.AppKey("model_relay_worker_token", str | None)
POLL_TIMEOUT_KEY = web.AppKey("model_relay_poll_timeout", float)
REQUEST_TIMEOUT_KEY = web.AppKey("model_relay_request_timeout", float)
LEASE_SECONDS_KEY = web.AppKey("model_relay_worker_lease_seconds", float)
RESULT_NOTIFIER_KEY = web.AppKey(
    "model_relay_result_notifier",
    Callable[["RelayRequest"], Awaitable[str | None]] | None,
)
ACCEPTED_NOTIFIER_KEY = web.AppKey(
    "model_relay_accepted_notifier",
    Callable[["RelayRequest"], Awaitable[str | None]] | None,
)
TELEMETRY_KEY = web.AppKey("model_relay_telemetry", Telemetry)
_DISABLED_TELEMETRY = Telemetry.disabled("model-relay")
_TransitionResult = TypeVar("_TransitionResult")


async def _finish_before_cancellation(
    awaitable: Awaitable[_TransitionResult],
    *,
    publish: Callable[[_TransitionResult], None] | None = None,
) -> _TransitionResult:
    """Finish a transition and its publication before propagating cancellation."""

    transition = asyncio.ensure_future(awaitable)
    cancellation: asyncio.CancelledError | None = None
    while not transition.done():
        try:
            await asyncio.shield(transition)
        except asyncio.CancelledError as exc:
            cancellation = exc
    result = transition.result()
    if publish is not None:
        publish(result)
    if cancellation is not None:
        raise cancellation
    return result


async def _blocking_call(
    function: Callable[..., _TransitionResult],
    *args: object,
    **kwargs: object,
) -> _TransitionResult:
    return await _finish_before_cancellation(
        asyncio.to_thread(function, *args, **kwargs)
    )


@dataclass
class RelayWorkerResponse:
    status: int
    body: object
    headers: dict[str, str] = field(default_factory=dict)


@dataclass
class RelayRequest:
    request_id: str
    rollout_id: str
    registration_token: str
    endpoint: str
    method: str
    body: JsonObject | None
    headers: dict[str, str]
    created_at: float
    future: asyncio.Future[RelayWorkerResponse]
    lifecycle_lock: asyncio.Lock = field(
        default_factory=asyncio.Lock,
        repr=False,
        compare=False,
    )
    expires_at: float | None = None
    payload_bytes: int = 0
    delivered_at: float | None = None
    first_delivered_at: float | None = None
    lease_id: str | None = None
    lease_expires_at: float | None = None
    leased_by: str | None = None
    delivery_count: int = 0
    state: str = "pending"
    idempotency_key: str | None = None
    request_digest: str = ""
    sandbox_id: str | None = None
    sandbox_generation: int | None = None
    completed_at: float | None = None
    completed_response: RelayWorkerResponse | None = None
    completed_bytes: int = 0
    wake_notified_at: float | None = None
    accepted_notified_at: float | None = None
    parked_transport_epoch: str | None = None
    reattachable: bool = False
    delivery_pending: bool = False

    def envelope(self) -> JsonObject:
        if self.body is None:
            raise RuntimeError("completed relay requests cannot be delivered")
        return {
            "request_id": self.request_id,
            "rollout_id": self.rollout_id,
            "registration_token": self.registration_token,
            "endpoint": self.endpoint,
            "method": self.method,
            "headers": dict(self.headers),
            "body": self.body,
            "created_at": self.created_at,
            "expires_at": self.expires_at,
            "delivered_at": self.delivered_at,
            "first_delivered_at": self.first_delivered_at,
            "lease_id": self.lease_id,
            "lease_expires_at": self.lease_expires_at,
            "leased_by": self.leased_by,
            "delivery_count": self.delivery_count,
            "idempotency_key": self.idempotency_key,
            "sandbox_id": self.sandbox_id,
            "sandbox_generation": self.sandbox_generation,
            "reattachable": self.reattachable,
            "accepted_notified_at": self.accepted_notified_at,
            "parked_transport_epoch": self.parked_transport_epoch,
        }


@dataclass(frozen=True)
class RelayRespondResult:
    request: RelayRequest
    duplicate: bool = False


class RelaySqliteStore:
    """Crash-durable single-process relay journal.

    Rows are independent so a response commit rewrites only one bounded request
    rather than a process-wide queue snapshot. The relay still has one writer;
    SQLite/WAL is not presented as a multi-host broker.
    """

    VERSION = 3

    def __init__(self, path: Path) -> None:
        if not path.is_absolute():
            raise ValueError("relay state path must be absolute")
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        self._lock = Lock()
        self._connection = sqlite3.connect(
            path,
            isolation_level=None,
            check_same_thread=False,
        )
        os.chmod(path, 0o600)
        self._connection.execute("PRAGMA journal_mode=WAL")
        self._connection.execute("PRAGMA synchronous=FULL")
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS relay_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS relay_rollouts (
                rollout_id TEXT PRIMARY KEY,
                payload TEXT NOT NULL
            )
            """
        )
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS relay_requests (
                request_id TEXT PRIMARY KEY,
                payload TEXT NOT NULL
            )
            """
        )
        version = self._connection.execute(
            "SELECT value FROM relay_meta WHERE key = 'version'"
        ).fetchone()
        if version is None:
            self._connection.execute(
                "INSERT INTO relay_meta(key, value) VALUES ('version', ?)",
                (str(self.VERSION),),
            )
        elif int(version[0]) != self.VERSION:
            raise ValueError("relay state database has an unsupported version")

    def close(self) -> None:
        with self._lock:
            self._connection.close()

    @contextmanager
    def _transaction_locked(self):
        """Run one logical journal update as one durable SQLite commit.

        Callers must already hold ``self._lock``.  Keeping transaction and
        mutex acquisition in one order prevents another thread from slipping
        an autocommitted row between members of a batch.
        """

        self._connection.execute("BEGIN IMMEDIATE")
        try:
            yield
            self._connection.execute("COMMIT")
        except BaseException:
            if self._connection.in_transaction:
                self._connection.execute("ROLLBACK")
            raise

    def load_rollouts(self) -> list[JsonObject]:
        with self._lock:
            rows = self._connection.execute(
                "SELECT payload FROM relay_rollouts ORDER BY rollout_id"
            ).fetchall()
        return [_json_mapping(row[0]) for row in rows]

    def load_requests(self) -> list[JsonObject]:
        with self._lock:
            rows = self._connection.execute(
                "SELECT payload FROM relay_requests ORDER BY request_id"
            ).fetchall()
        return [_json_mapping(row[0]) for row in rows]

    def save_rollout(self, record: JsonObject) -> None:
        with self._lock:
            self._connection.execute(
                """
                INSERT INTO relay_rollouts(rollout_id, payload) VALUES (?, ?)
                ON CONFLICT(rollout_id) DO UPDATE SET payload = excluded.payload
                """,
                (
                    str(record["rollout_id"]),
                    json.dumps(record, ensure_ascii=False, separators=(",", ":")),
                ),
            )

    def delete_rollout(self, rollout_id: str) -> None:
        with self._lock:
            self._connection.execute(
                "DELETE FROM relay_rollouts WHERE rollout_id = ?",
                (rollout_id,),
            )

    def save_request(self, request: RelayRequest) -> None:
        self.save_requests((request,))

    def save_requests(self, requests: tuple[RelayRequest, ...]) -> None:
        self.commit_request_batch(requests, ())

    def commit_request_batch(
        self,
        requests: tuple[RelayRequest, ...],
        deleted_request_ids: tuple[str, ...],
    ) -> None:
        """Commit one logical group of request upserts and deletions."""

        rows = [self._request_row(request) for request in requests]
        if not rows and not deleted_request_ids:
            return
        with self._lock:
            with self._transaction_locked():
                if rows:
                    self._upsert_request_rows_locked(rows)
                if deleted_request_ids:
                    self._connection.executemany(
                        "DELETE FROM relay_requests WHERE request_id = ?",
                        ((request_id,) for request_id in deleted_request_ids),
                    )

    def delete_requests(self, request_ids: tuple[str, ...]) -> None:
        if not request_ids:
            return
        with self._lock:
            with self._transaction_locked():
                self._connection.executemany(
                    "DELETE FROM relay_requests WHERE request_id = ?",
                    ((request_id,) for request_id in request_ids),
                )

    @staticmethod
    def _request_row(request: RelayRequest) -> tuple[object, ...]:
        payload = _persisted_request_payload(request)
        return (
            request.request_id,
            json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
        )

    def _upsert_request_rows_locked(
        self,
        rows: tuple[tuple[object, ...], ...] | list[tuple[object, ...]],
    ) -> None:
        self._connection.executemany(
            """
            INSERT INTO relay_requests(request_id, payload) VALUES (?, ?)
            ON CONFLICT(request_id) DO UPDATE SET
                payload = excluded.payload
            """,
            rows,
        )


class ModelRelayState:
    def __init__(
        self,
        *,
        state_path: Path | None = None,
        request_timeout_seconds: float = DEFAULT_RELAY_REQUEST_TIMEOUT_SECONDS,
        completed_request_retention_seconds: float = DEFAULT_COMPLETED_REQUEST_RETENTION_SECONDS,
        worker_retention_seconds: float = DEFAULT_WORKER_RETENTION_SECONDS,
        max_inflight_requests: int = DEFAULT_MAX_INFLIGHT_REQUESTS,
        max_inflight_requests_per_rollout: int = DEFAULT_MAX_INFLIGHT_REQUESTS_PER_ROLLOUT,
        max_inflight_bytes: int = DEFAULT_MAX_INFLIGHT_BYTES,
        max_completed_requests: int = DEFAULT_MAX_COMPLETED_REQUESTS,
        max_completed_bytes: int = DEFAULT_MAX_COMPLETED_BYTES,
        max_workers: int = DEFAULT_MAX_WORKERS,
    ) -> None:
        self._lock = asyncio.Lock()
        self._rollout_wakeups: dict[str, asyncio.Event] = {}
        self._request_expiry_heap: list[tuple[float, str]] = []
        self._lease_expiry_heaps: dict[
            str,
            list[tuple[float, str, str]],
        ] = {}
        self._request_timeout_seconds = max(0.001, request_timeout_seconds)
        self._rollouts: dict[str, JsonObject] = {}
        self._pending: dict[str, deque[RelayRequest]] = {}
        self._requests: dict[str, RelayRequest] = {}
        self._completed: dict[str, RelayRequest] = {}
        self._idempotency: dict[tuple[str, str, str], str] = {}
        self._workers: dict[tuple[str, str], JsonObject] = {}
        self._store = RelaySqliteStore(state_path) if state_path is not None else None
        self._loaded = state_path is None
        self._inflight_bytes = 0
        self._completed_bytes = 0
        self._completed_expiry_heap: list[tuple[float, str]] = []
        self._completed_eviction_heap: list[tuple[float, str]] = []
        self._rollout_leased_counts: dict[str, int] = {}
        self._max_inflight_requests = max(1, max_inflight_requests)
        self._max_inflight_requests_per_rollout = max(
            1,
            max_inflight_requests_per_rollout,
        )
        self._max_inflight_bytes = max(1, max_inflight_bytes)
        self._max_completed_requests = max(1, max_completed_requests)
        self._max_completed_bytes = max(1024, max_completed_bytes)
        self._max_workers = max(1, max_workers)
        self._completed_request_retention_seconds = max(
            0.001,
            completed_request_retention_seconds,
        )
        self._worker_retention_seconds = max(0.001, worker_retention_seconds)
        self._counters: dict[str, int] = {
            "enqueued": 0,
            "delivered": 0,
            "completed": 0,
            "duplicate_responses": 0,
            "worker_errors": 0,
            "worker_retries": 0,
            "worker_retry_exhausted": 0,
            "timed_out": 0,
            "lease_expired": 0,
            "lease_renewed": 0,
            "unregister_canceled": 0,
            "polls": 0,
            "empty_polls": 0,
            "admission_rejected": 0,
            "canceled": 0,
            "reattached": 0,
            "restored_requests": 0,
            "wake_notifications": 0,
            "detached_callers": 0,
            "transport_resets": 0,
            "accepted_notifications": 0,
        }
        self._timers: dict[str, float] = {
            "queue_wait_seconds_total": 0.0,
            "request_lifetime_seconds_total": 0.0,
            "worker_processing_seconds_total": 0.0,
        }

    async def register_rollout(
        self,
        rollout_id: str,
        metadata: JsonObject | None = None,
    ) -> JsonObject:
        validate_rollout_id(rollout_id)
        _validate_registration_metadata(metadata)
        async with self._lock:
            await self._ensure_loaded_locked()
            now = time.time()
            await self._prune_completed_locked(now)
            self._prune_workers_locked(now)
            registration_token = uuid4().hex
            previous = self._rollouts.get(rollout_id)
            record = {
                "rollout_id": rollout_id,
                "registration_token": registration_token,
                "metadata": dict(metadata or {}),
                "registered_at": time.time(),
            }
            await _finish_before_cancellation(
                self._register_rollout_transition_locked(
                    record,
                    previous=previous,
                )
            )
            return dict(record)

    async def unregister_rollout(
        self,
        rollout_id: str,
        *,
        registration_token: str,
    ) -> bool:
        validate_rollout_id(rollout_id)
        validate_registration_token(registration_token)
        async with self._lock:
            await self._ensure_loaded_locked()
            current = self._rollouts.get(rollout_id)
            if current is None:
                return False
            self._require_current_registration_locked(
                rollout_id,
                registration_token,
            )
            await _finish_before_cancellation(
                self._unregister_rollout_transition_locked(
                    rollout_id,
                    registration_token,
                )
            )
            return True

    async def require_current_registration(
        self,
        rollout_id: str,
        registration_token: str,
    ) -> None:
        """Authorize a caller against one exact rollout incarnation."""

        validate_rollout_id(rollout_id)
        if not REGISTRATION_TOKEN_RE.fullmatch(registration_token):
            raise web.HTTPUnauthorized(text="invalid rollout registration token")
        async with self._lock:
            await self._ensure_loaded_locked()
            current = self._rollouts.get(rollout_id)
            if current is None or not hmac.compare_digest(
                str(current["registration_token"]),
                registration_token,
            ):
                raise web.HTTPUnauthorized(text="invalid rollout registration token")

    async def list_rollouts(self) -> list[JsonObject]:
        async with self._lock:
            await self._ensure_loaded_locked()
            now = time.time()
            await self._prune_completed_locked(now)
            self._prune_workers_locked(now)
            return [dict(record) for record in self._rollouts.values()]

    async def record_worker_heartbeat(
        self,
        *,
        rollout_id: str,
        registration_token: str,
        worker_id: str,
        metadata: JsonObject | None = None,
    ) -> JsonObject:
        validate_rollout_id(rollout_id)
        validate_registration_token(registration_token)
        validate_worker_id(worker_id)
        async with self._lock:
            await self._ensure_loaded_locked()
            self._prune_workers_locked(time.time())
            self._require_current_registration_locked(
                rollout_id,
                registration_token,
            )
            return self._record_worker_heartbeat_locked(
                rollout_id=rollout_id,
                worker_id=worker_id,
                metadata=metadata,
            )

    async def enqueue(
        self,
        *,
        rollout_id: str,
        endpoint: str,
        body: object,
        headers: dict[str, str],
        method: str = "POST",
        idempotency_key: str | None = None,
        defer_idempotency_until_disconnect: bool = False,
    ) -> RelayRequest:
        validate_rollout_id(rollout_id)
        method = method.upper()
        if method not in TUNNEL_HTTP_METHODS:
            raise web.HTTPMethodNotAllowed(method, sorted(TUNNEL_HTTP_METHODS))
        if not endpoint.startswith("/") or endpoint.startswith("//"):
            raise web.HTTPBadRequest(text="relay endpoint must be an absolute path")
        encoded_body = _encoded_body(body)
        body_bytes = _encoded_body_bytes(encoded_body)
        if len(body_bytes) > MAX_RELAY_BODY_BYTES:
            raise web.HTTPRequestEntityTooLarge(
                max_size=MAX_RELAY_BODY_BYTES,
                actual_size=len(body_bytes),
            )
        if idempotency_key is not None:
            validate_idempotency_key(idempotency_key)
        loop = asyncio.get_running_loop()
        async with self._lock:
            await self._ensure_loaded_locked()
            now = time.time()
            await self._prune_completed_locked(now)
            await self._expire_requests_locked(now)
            if rollout_id not in self._rollouts:
                raise web.HTTPNotFound(text=f"rollout is not registered: {rollout_id}")
            identity_headers = {
                key: value
                for key, value in headers.items()
                if key.lower()
                not in {
                    "baggage",
                    "traceparent",
                    "tracestate",
                    "x-correlation-id",
                    "x-request-id",
                    "x-stainless-read-timeout",
                    "x-stainless-retry-count",
                }
            }
            metadata_bytes = json.dumps(
                {
                    "endpoint": endpoint,
                    "method": method,
                    # Retry and tracing headers may legitimately change when
                    # an SDK recreates a request after migration. They do not
                    # change the logical upstream operation.
                    "headers": identity_headers,
                },
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
            payload_bytes = len(body_bytes) + len(metadata_bytes)
            request_digest = hashlib.sha256(
                b"\0".join(
                    (
                        rollout_id.encode("utf-8"),
                        endpoint.encode("utf-8"),
                        method.encode("ascii"),
                        metadata_bytes,
                        body_bytes,
                    )
                )
            ).hexdigest()
            registration = self._rollouts[rollout_id]
            registration_token = str(registration["registration_token"])
            if idempotency_key is not None:
                identity = (rollout_id, registration_token, idempotency_key)
                existing_id = self._idempotency.get(identity)
                existing = self._requests.get(existing_id or "") or self._completed.get(
                    existing_id or ""
                )
                if existing is not None:
                    if existing.request_digest != request_digest:
                        raise web.HTTPConflict(
                            text=(
                                "idempotency key was already used for a different "
                                "relay request"
                            )
                        )
                    self._counters["reattached"] += 1
                    return existing
            rollout_inflight = len(self._pending.get(rollout_id, ())) + (
                self._rollout_leased_counts.get(rollout_id, 0)
            )
            rejection_reason = ""
            if len(self._requests) >= self._max_inflight_requests:
                rejection_reason = "relay request capacity is exhausted"
            elif rollout_inflight >= self._max_inflight_requests_per_rollout:
                rejection_reason = "rollout request capacity is exhausted"
            elif self._inflight_bytes + payload_bytes > self._max_inflight_bytes:
                rejection_reason = "relay queued-byte capacity is exhausted"
            if rejection_reason:
                self._counters["admission_rejected"] += 1
                raise web.HTTPTooManyRequests(
                    text=rejection_reason,
                    headers={"Retry-After": "1"},
                )
            created_at = now
            request = RelayRequest(
                request_id=uuid4().hex,
                rollout_id=rollout_id,
                registration_token=registration_token,
                endpoint=endpoint,
                method=method,
                body=encoded_body,
                headers=dict(headers),
                created_at=created_at,
                future=loop.create_future(),
                expires_at=created_at + self._request_timeout_seconds,
                payload_bytes=payload_bytes,
                idempotency_key=idempotency_key,
                request_digest=request_digest,
                sandbox_id=_registration_sandbox_id(registration),
                sandbox_generation=_registration_sandbox_generation(registration),
                reattachable=(
                    idempotency_key is not None
                    and not defer_idempotency_until_disconnect
                ),
            )
            await _finish_before_cancellation(self._enqueue_transition_locked(request))
            return request

    async def mark_caller_detached(self, request_id: str) -> None:
        """Keep accepted work alive and let an identical retry reattach."""

        await self._mark_request_reattachable(
            request_id,
            counter="detached_callers",
        )

    async def mark_transport_reset(self, request_id: str) -> None:
        """Publish retry identity before migration severs the saved socket."""

        await self._mark_request_reattachable(
            request_id,
            counter="transport_resets",
        )

    async def _mark_request_reattachable(
        self,
        request_id: str,
        *,
        counter: str,
    ) -> None:
        async with self._lock:
            await self._ensure_loaded_locked()
            request = self._requests.get(request_id) or self._completed.get(request_id)
            if request is None:
                return
            request.reattachable = request.idempotency_key is not None
            if request.reattachable:
                assert request.idempotency_key is not None
                self._idempotency[
                    (
                        request.rollout_id,
                        request.registration_token,
                        request.idempotency_key,
                    )
                ] = request.request_id
            self._counters[counter] += 1
            if self._store is not None:
                await _blocking_call(self._store.save_request, request)

    async def poll(
        self,
        *,
        rollout_id: str,
        registration_token: str,
        timeout_seconds: float,
        limit: int = 1,
        lease_seconds: float = DEFAULT_WORKER_LEASE_SECONDS,
        worker_id: str | None = None,
    ) -> list[RelayRequest]:
        validate_rollout_id(rollout_id)
        validate_registration_token(registration_token)
        if worker_id is not None:
            validate_worker_id(worker_id)
        limit = max(1, min(256, limit))
        lease_seconds = max(0.001, lease_seconds)
        deadline = time.monotonic() + max(0.0, timeout_seconds)
        initialized = False
        while True:
            async with self._lock:
                if not initialized:
                    await self._ensure_loaded_locked()
                    await self._prune_completed_locked(time.time())
                    self._prune_workers_locked(time.time())
                    self._require_current_registration_locked(
                        rollout_id,
                        registration_token,
                    )
                    if worker_id:
                        self._record_worker_heartbeat_locked(
                            rollout_id=rollout_id,
                            worker_id=worker_id,
                            metadata=None,
                        )
                    self._counters["polls"] += 1
                    initialized = True
                self._require_current_registration_locked(
                    rollout_id,
                    registration_token,
                )
                now = time.time()
                await self._expire_requests_locked(now)
                await self._requeue_expired_leases_locked(
                    now,
                    rollout_id=rollout_id,
                )
                queue = self._pending.setdefault(rollout_id, deque())
                if queue:
                    requests = [
                        self._lease_request_locked(
                            queue.popleft(),
                            now=now,
                            lease_seconds=lease_seconds,
                            worker_id=worker_id,
                        )
                        for _ in range(min(limit, len(queue)))
                    ]
                    if self._store is not None:
                        await _blocking_call(
                            self._store.save_requests,
                            tuple(requests),
                        )
                    return requests
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    self._counters["empty_polls"] += 1
                    return []
                next_expiry = self._next_lease_expiry_locked(rollout_id)
                if next_expiry is not None:
                    remaining = min(remaining, max(0.001, next_expiry - time.time()))
                wakeup = self._rollout_wakeup_locked(rollout_id)
            await self._wait_for_rollout_wakeup(wakeup, remaining)

    async def renew_lease(
        self,
        *,
        request_id: str,
        registration_token: str,
        lease_id: str,
        lease_seconds: float,
        worker_id: str | None = None,
    ) -> RelayRequest:
        validate_registration_token(registration_token)
        if worker_id is not None:
            validate_worker_id(worker_id)
        lease_seconds = max(0.001, lease_seconds)
        async with self._lock:
            await self._ensure_loaded_locked()
            now = time.time()
            await self._prune_completed_locked(now)
            await self._expire_requests_locked(now)
            request = self._requests.get(request_id)
            if request is None:
                if request_id in self._completed:
                    self._require_completed_registration_locked(
                        request_id,
                        registration_token,
                    )
                    raise web.HTTPGone(text="request is already completed")
                raise web.HTTPNotFound(text=f"request not found: {request_id}")
            self._require_request_registration_locked(
                request,
                registration_token,
            )
            if request.state != "leased" or request.lease_id != lease_id:
                raise web.HTTPConflict(text="request lease is no longer active")
            now = time.time()
            if request.lease_expires_at is not None and request.lease_expires_at <= now:
                await self._requeue_request_locked(request)
                raise web.HTTPConflict(text="request lease has expired")
            leased_by = request.leased_by
            if worker_id:
                leased_by = worker_id
                self._record_worker_heartbeat_locked(
                    rollout_id=request.rollout_id,
                    worker_id=worker_id,
                    metadata=None,
                )
            lease_expires_at = now + lease_seconds
            await _finish_before_cancellation(
                self._renew_lease_transition_locked(
                    request,
                    leased_by=leased_by,
                    lease_expires_at=lease_expires_at,
                )
            )
            return request

    async def respond(
        self,
        *,
        request_id: str,
        registration_token: str,
        response: RelayWorkerResponse,
        lease_id: str | None,
        error: bool = False,
        defer_delivery: bool = False,
    ) -> RelayRespondResult:
        validate_registration_token(registration_token)
        async with self._lock:
            await self._ensure_loaded_locked()
            now = time.time()
            await self._prune_completed_locked(now)
            await self._expire_requests_locked(now)
            if request_id in self._completed:
                self._require_completed_registration_locked(
                    request_id,
                    registration_token,
                )
                self._counters["duplicate_responses"] += 1
                return RelayRespondResult(
                    request=self._completed[request_id],
                    duplicate=True,
                )
            request = self._requests.get(request_id)
            if request is None:
                raise web.HTTPNotFound(text=f"request not found: {request_id}")
            self._require_request_registration_locked(
                request,
                registration_token,
            )
            if not lease_id:
                raise web.HTTPBadRequest(text="lease_id is required")
            if request.state != "leased" or request.lease_id != lease_id:
                raise web.HTTPConflict(text="request lease is no longer active")
            now = time.time()
            if request.lease_expires_at is not None and request.lease_expires_at <= now:
                await self._requeue_request_locked(request)
                raise web.HTTPConflict(text="request lease has expired")

            def record_completion(
                completed_request: RelayRequest,
                effective_response: RelayWorkerResponse,
            ) -> None:
                self._counters["completed"] += 1
                if error or effective_response.status >= 400:
                    self._counters["worker_errors"] += 1
                self._timers["request_lifetime_seconds_total"] += (
                    now - completed_request.created_at
                )
                if completed_request.delivered_at is not None:
                    self._timers["worker_processing_seconds_total"] += (
                        now - completed_request.delivered_at
                    )

            results = await _finish_before_cancellation(
                self._complete_requests_locked(
                    (request,),
                    completed_at=now,
                    response=response,
                    defer_delivery=defer_delivery,
                ),
                publish=lambda completed: record_completion(*completed[0]),
            )
            return RelayRespondResult(request=results[0][0])

    async def retry_worker_failure(
        self,
        *,
        request_id: str,
        registration_token: str,
        lease_id: str,
    ) -> RelayRequest | None:
        """Release a transient failed lease back to the durable queue."""

        validate_registration_token(registration_token)
        async with self._lock:
            await self._ensure_loaded_locked()
            now = time.time()
            await self._prune_completed_locked(now)
            await self._expire_requests_locked(now)
            request = self._requests.get(request_id)
            if request is None:
                return None
            self._require_request_registration_locked(
                request,
                registration_token,
            )
            if request.state != "leased" or request.lease_id != lease_id:
                raise web.HTTPConflict(text="request lease is no longer active")
            if request.lease_expires_at is not None and request.lease_expires_at <= now:
                await self._requeue_request_locked(request)
                raise web.HTTPConflict(text="request lease has expired")
            if request.delivery_count >= MAX_TRANSIENT_WORKER_DELIVERIES:
                self._counters["worker_retry_exhausted"] += 1
                return None
            self._decrement_rollout_leased_locked(request.rollout_id)
            request.state = "pending"
            request.lease_id = None
            request.lease_expires_at = None
            request.leased_by = None
            request.delivered_at = None
            self._pending.setdefault(request.rollout_id, deque()).append(request)
            self._counters["worker_retries"] += 1
            if self._store is not None:
                await _blocking_call(self._store.save_request, request)
            self._maybe_compact_lease_expiry_heap_locked(request.rollout_id)
            self._wake_rollout_locked(request.rollout_id)
            return request

    async def release_completed_response(self, request_id: str) -> None:
        """Allow the sandbox-facing HTTP handler to deliver a committed result.

        Lifecycle-bound worker routes defer this until the wake callback has
        completed. The response is already durable, so this operation is
        intentionally idempotent for retried worker commits.
        """

        async with self._lock:
            await self._ensure_loaded_locked()
            request = self._completed.get(request_id)
            if request is None or request.completed_response is None:
                raise web.HTTPNotFound(text=f"request not found: {request_id}")
            await _finish_before_cancellation(self._release_completed_locked(request))

    async def cancel_request(
        self,
        *,
        request_id: str,
        response: RelayWorkerResponse,
        reason: str = "canceled",
    ) -> RelayWorkerResponse | None:
        async with self._lock:
            await self._ensure_loaded_locked()
            request = self._requests.get(request_id)
            if request is None:
                return None
            counter = "timed_out" if reason == "timeout" else "canceled"
            results = await _finish_before_cancellation(
                self._complete_requests_locked(
                    (request,),
                    completed_at=time.time(),
                    response=response,
                    defer_delivery=False,
                ),
                publish=lambda _results: self._increment_counter_locked(counter),
            )
            return results[0][1]

    async def wait_for_response(
        self,
        request: RelayRequest,
        *,
        timeout_seconds: float,
    ) -> RelayWorkerResponse:
        if request.completed_response is not None and request.future.done():
            return request.completed_response
        return await asyncio.wait_for(
            asyncio.shield(request.future),
            timeout=timeout_seconds,
        )

    async def stats(self) -> JsonObject:
        async with self._lock:
            await self._ensure_loaded_locked()
            now = time.time()
            await self._prune_completed_locked(now)
            self._prune_workers_locked(now)
            await self._expire_requests_locked(now)
            await self._requeue_expired_leases_locked(now)
            pending = {
                rollout_id: len(queue)
                for rollout_id, queue in sorted(self._pending.items())
            }
            leased_by_rollout = dict(self._rollout_leased_counts)
            counters = dict(self._counters)
            timers = dict(self._timers)
            averages = {
                "queue_wait_seconds": _average(
                    timers["queue_wait_seconds_total"],
                    counters["delivered"],
                ),
                "request_lifetime_seconds": _average(
                    timers["request_lifetime_seconds_total"],
                    counters["completed"],
                ),
                "worker_processing_seconds": _average(
                    timers["worker_processing_seconds_total"],
                    counters["completed"],
                ),
            }
            return {
                "rollouts": len(self._rollouts),
                "pending": pending,
                "leased": leased_by_rollout,
                "inflight": len(self._requests),
                "inflight_bytes": self._inflight_bytes,
                "completed_retained": len(self._completed),
                "completed_bytes": self._completed_bytes,
                "workers": [dict(record) for record in self._workers.values()],
                "counters": counters,
                "timers": timers,
                "averages": averages,
                "limits": {
                    "max_inflight_requests": self._max_inflight_requests,
                    "max_inflight_requests_per_rollout": self._max_inflight_requests_per_rollout,
                    "max_inflight_bytes": self._max_inflight_bytes,
                    "max_completed_requests": self._max_completed_requests,
                    "max_completed_bytes": self._max_completed_bytes,
                    "max_workers": self._max_workers,
                },
            }

    async def maintain(self) -> None:
        """Advance time-based durable state without requiring API traffic."""

        async with self._lock:
            await self._ensure_loaded_locked()
            now = time.time()
            await self._prune_completed_locked(now)
            self._prune_workers_locked(now)
            await self._expire_requests_locked(now)
            await self._requeue_expired_leases_locked(now)

    async def mark_wake_notified(self, request_id: str) -> None:
        async with self._lock:
            await self._ensure_loaded_locked()
            request = self._completed.get(request_id)
            if request is None:
                raise web.HTTPNotFound(text=f"request not found: {request_id}")
            if request.wake_notified_at is None:
                request.wake_notified_at = time.time()
                self._counters["wake_notifications"] += 1
                if self._store is not None:
                    await _blocking_call(self._store.save_request, request)

    async def mark_accepted_notified(
        self,
        request_id: str,
        *,
        transport_epoch: str | None = None,
    ) -> None:
        async with self._lock:
            await self._ensure_loaded_locked()
            request = self._requests.get(request_id) or self._completed.get(request_id)
            if request is None:
                raise web.HTTPNotFound(text=f"request not found: {request_id}")
            if request.accepted_notified_at is None:
                request.accepted_notified_at = time.time()
                request.parked_transport_epoch = transport_epoch
                self._counters["accepted_notifications"] += 1
                if self._store is not None:
                    await _blocking_call(self._store.save_request, request)

    async def aclose(self) -> None:
        store = self._store
        self._store = None
        if store is not None:
            await _blocking_call(store.close)

    async def _register_rollout_transition_locked(
        self,
        record: JsonObject,
        *,
        previous: JsonObject | None,
    ) -> None:
        rollout_id = str(record["rollout_id"])
        if previous is not None:
            await self._cancel_rollout_incarnation_locked(
                rollout_id,
                str(previous["registration_token"]),
                message="rollout registration was replaced",
                error_code="relay_rollout_replaced",
            )
        if self._store is not None:
            await _blocking_call(self._store.save_rollout, record)
        self._rollouts[rollout_id] = record
        self._pending.setdefault(rollout_id, deque())
        self._wake_rollout_locked(rollout_id)

    async def _unregister_rollout_transition_locked(
        self,
        rollout_id: str,
        registration_token: str,
    ) -> None:
        await self._cancel_rollout_incarnation_locked(
            rollout_id,
            registration_token,
            message="rollout unregistered",
            error_code="relay_rollout_closed",
        )
        if self._store is not None:
            await _blocking_call(self._store.delete_rollout, rollout_id)
        self._rollouts.pop(rollout_id, None)
        self._wake_rollout_locked(rollout_id)

    async def _enqueue_transition_locked(self, request: RelayRequest) -> None:
        if self._store is not None:
            await _blocking_call(self._store.save_request, request)
        self._pending.setdefault(request.rollout_id, deque()).append(request)
        self._requests[request.request_id] = request
        if request.expires_at is not None:
            heapq.heappush(
                self._request_expiry_heap,
                (request.expires_at, request.request_id),
            )
        self._inflight_bytes += request.payload_bytes
        if request.idempotency_key is not None and request.reattachable:
            self._idempotency[
                (
                    request.rollout_id,
                    request.registration_token,
                    request.idempotency_key,
                )
            ] = request.request_id
        self._counters["enqueued"] += 1
        self._wake_rollout_locked(request.rollout_id)

    async def _renew_lease_transition_locked(
        self,
        request: RelayRequest,
        *,
        leased_by: str | None,
        lease_expires_at: float,
    ) -> None:
        durable = replace(
            request,
            leased_by=leased_by,
            lease_expires_at=lease_expires_at,
        )
        if self._store is not None:
            await _blocking_call(self._store.save_request, durable)
        request.leased_by = leased_by
        request.lease_expires_at = lease_expires_at
        assert request.lease_id is not None
        heapq.heappush(
            self._lease_expiry_heaps.setdefault(request.rollout_id, []),
            (lease_expires_at, request.request_id, request.lease_id),
        )
        self._maybe_compact_lease_expiry_heap_locked(request.rollout_id)
        self._counters["lease_renewed"] += 1
        self._wake_rollout_locked(request.rollout_id)

    async def _ensure_loaded_locked(self) -> None:
        if self._loaded:
            return
        assert self._store is not None
        loop = asyncio.get_running_loop()
        now = time.time()
        rollout_rows = await _blocking_call(self._store.load_rollouts)
        request_rows = await _blocking_call(self._store.load_requests)

        recovered_rollouts: dict[str, JsonObject] = {}
        recovered_pending: dict[str, deque[RelayRequest]] = {}
        for record in rollout_rows:
            rollout_id = record["rollout_id"]
            registration_token = record["registration_token"]
            metadata = record["metadata"]
            registered_at = record["registered_at"]
            if not isinstance(rollout_id, str):
                raise ValueError("persisted relay rollout_id is invalid")
            if not isinstance(registration_token, str):
                raise ValueError("persisted relay registration token is invalid")
            if not isinstance(metadata, dict):
                raise ValueError("persisted relay registration metadata is invalid")
            if isinstance(registered_at, bool) or not isinstance(
                registered_at, (int, float)
            ):
                raise ValueError("persisted relay registration time is invalid")
            validate_rollout_id(rollout_id)
            validate_registration_token(registration_token)
            if (
                "sandbox_id" in metadata
                and "sandbox_generation" in metadata
                and AGENT_LIFECYCLE_METADATA_KEY not in metadata
            ):
                # Existing deployments wrote the same generation-fenced
                # lifecycle binding before it had an explicit contract tag.
                # Upgrade that durable row once so rollout recovery remains
                # safe; every new registration must use the managed SDK path.
                metadata = dict(metadata)
                metadata[AGENT_LIFECYCLE_METADATA_KEY] = MANAGED_AGENT_LIFECYCLE
                record = dict(record)
                record["metadata"] = metadata
                await _blocking_call(self._store.save_rollout, record)
            _validate_registration_metadata(metadata)
            recovered_rollouts[rollout_id] = record
            recovered_pending[rollout_id] = deque()

        active: list[RelayRequest] = []
        completed: list[RelayRequest] = []
        changed: dict[str, RelayRequest] = {}
        restored_requests = 0
        recovered_timeouts = 0
        recovered_expired_leases = 0
        for payload in request_rows:
            request = _request_from_persisted_payload(payload, loop=loop)
            recovery_changed = False
            # Every non-terminal caller connection was destroyed by this relay
            # restart. A lifecycle-bound completed response also remains
            # replayable because its sandbox was parked before delivery.
            if request.state != "completed" and request.idempotency_key is not None:
                if not request.reattachable:
                    request.reattachable = True
                    recovery_changed = True
            elif (
                request.state == "completed"
                and request.sandbox_id is not None
                and request.accepted_notified_at is not None
                and request.idempotency_key is not None
            ):
                if not request.reattachable:
                    request.reattachable = True
                    recovery_changed = True
            if request.state == "completed":
                if request.completed_response is None:
                    raise ValueError(
                        "completed relay request is missing its persisted response"
                    )
                if request.completed_bytes <= 0:
                    raise ValueError(
                        "completed relay request has an invalid retained byte count"
                    )
                current = recovered_rollouts.get(request.rollout_id)
                if request.delivery_pending and (
                    current is None
                    or current["registration_token"] != request.registration_token
                    or self._completed_deadline(request) < now
                ):
                    self._recover_terminal_request(
                        request,
                        completed_at=now,
                        response=RelayWorkerResponse(
                            410
                            if current is None
                            or current["registration_token"]
                            != request.registration_token
                            else 504,
                            _openai_error(
                                "deferred response abandoned",
                                "relay_delivery_abandoned",
                            ),
                        ),
                    )
                    recovery_changed = True
                completed.append(request)
                if recovery_changed:
                    changed[request.request_id] = request
                continue
            if request.state not in {"pending", "leased"}:
                raise ValueError(
                    f"persisted relay request has invalid state {request.state!r}"
                )
            current_registration = recovered_rollouts.get(request.rollout_id)
            if (
                current_registration is None
                or current_registration["registration_token"]
                != request.registration_token
            ):
                # A rollout deletion and request cancellation are separate
                # durable row updates. A replacement registration can likewise
                # land before its old requests are terminally rewritten.
                # Recover either stranded row instead of exposing it to a new
                # registration incarnation.
                response = RelayWorkerResponse(
                    410,
                    _openai_error(
                        "rollout registration is no longer available",
                        "relay_rollout_closed",
                    ),
                )
                self._recover_terminal_request(
                    request,
                    completed_at=time.time(),
                    response=response,
                )
                completed.append(request)
                changed[request.request_id] = request
                continue
            restored_requests += 1
            if request.expires_at is not None and request.expires_at <= now:
                self._recover_terminal_request(
                    request,
                    completed_at=now,
                    response=RelayWorkerResponse(
                        504,
                        _openai_error(
                            "model relay request timed out",
                            "relay_timeout",
                        ),
                    ),
                )
                completed.append(request)
                changed[request.request_id] = request
                recovered_timeouts += 1
                continue
            if (
                request.state == "leased"
                and request.lease_expires_at is not None
                and request.lease_id is not None
                and request.lease_expires_at <= now
            ):
                request.state = "pending"
                request.lease_id = None
                request.lease_expires_at = None
                request.leased_by = None
                recovery_changed = True
                recovered_expired_leases += 1
            elif request.state == "leased" and (
                request.lease_expires_at is None or request.lease_id is None
            ):
                raise ValueError("persisted leased relay request is missing its lease")
            active.append(request)
            if recovery_changed:
                changed[request.request_id] = request

        cutoff = now - self._completed_request_retention_seconds
        deleted: set[str] = set()
        retained_completed: dict[str, RelayRequest] = {}
        retained_completed_bytes = 0
        pinned = [request for request in completed if request.delivery_pending]
        for request in pinned:
            retained_completed[request.request_id] = request
            retained_completed_bytes += request.completed_bytes
        unpinned = sorted(
            (request for request in completed if not request.delivery_pending),
            key=lambda item: (item.completed_at or 0.0, item.request_id),
            reverse=True,
        )
        for request in unpinned:
            if (request.completed_at or 0.0) < cutoff:
                deleted.add(request.request_id)
                continue
            if (
                len(retained_completed) >= self._max_completed_requests
                or retained_completed_bytes + request.completed_bytes
                > self._max_completed_bytes
            ):
                deleted.add(request.request_id)
                continue
            retained_completed[request.request_id] = request
            retained_completed_bytes += request.completed_bytes

        changed_rows = tuple(
            request
            for request_id, request in changed.items()
            if request_id not in deleted
        )
        if changed_rows or deleted:
            await _blocking_call(
                self._store.commit_request_batch,
                changed_rows,
                tuple(sorted(deleted)),
            )

        recovered_requests: dict[str, RelayRequest] = {}
        recovered_idempotency: dict[tuple[str, str, str], str] = {}
        recovered_request_expiry_heap: list[tuple[float, str]] = []
        recovered_lease_expiry_heaps: dict[
            str,
            list[tuple[float, str, str]],
        ] = {}
        recovered_rollout_leased_counts: dict[str, int] = {}
        recovered_inflight_bytes = 0

        for request in active:
            recovered_requests[request.request_id] = request
            recovered_inflight_bytes += request.payload_bytes
            if request.expires_at is not None:
                recovered_request_expiry_heap.append(
                    (request.expires_at, request.request_id)
                )
            if request.state == "pending":
                recovered_pending.setdefault(request.rollout_id, deque()).append(
                    request
                )
            else:
                assert request.lease_expires_at is not None
                assert request.lease_id is not None
                recovered_rollout_leased_counts[request.rollout_id] = (
                    recovered_rollout_leased_counts.get(request.rollout_id, 0) + 1
                )
                recovered_lease_expiry_heaps.setdefault(request.rollout_id, []).append(
                    (
                        request.lease_expires_at,
                        request.request_id,
                        request.lease_id,
                    )
                )
            self._add_recovered_idempotency(
                recovered_idempotency,
                request,
            )
        heapq.heapify(recovered_request_expiry_heap)
        for heap in recovered_lease_expiry_heaps.values():
            heapq.heapify(heap)
        for request in retained_completed.values():
            self._add_recovered_idempotency(
                recovered_idempotency,
                request,
            )
            if not request.delivery_pending:
                assert request.completed_response is not None
                _set_response(request.future, request.completed_response)

        self._rollouts = recovered_rollouts
        self._pending = recovered_pending
        self._requests = recovered_requests
        self._completed = retained_completed
        self._idempotency = recovered_idempotency
        self._request_expiry_heap = recovered_request_expiry_heap
        self._lease_expiry_heaps = recovered_lease_expiry_heaps
        self._inflight_bytes = recovered_inflight_bytes
        self._completed_bytes = retained_completed_bytes
        self._rebuild_completed_indexes_locked()
        self._rollout_leased_counts = recovered_rollout_leased_counts
        self._counters["restored_requests"] += restored_requests
        self._counters["timed_out"] += recovered_timeouts
        self._counters["lease_expired"] += recovered_expired_leases
        self._loaded = True

    def _recover_terminal_request(
        self,
        request: RelayRequest,
        *,
        completed_at: float,
        response: RelayWorkerResponse,
    ) -> None:
        completed_bytes = _relay_response_retained_bytes(response)
        if completed_bytes > self._max_completed_bytes:
            response = RelayWorkerResponse(
                502,
                _openai_error(
                    "worker response exceeds relay completed-response capacity",
                    "relay_response_capacity_exceeded",
                ),
            )
            completed_bytes = _relay_response_retained_bytes(response)
        request.completed_at = completed_at
        request.completed_response = response
        request.completed_bytes = completed_bytes
        request.state = "completed"
        request.body = None
        request.headers = {}
        request.payload_bytes = 0
        request.delivery_pending = False

    @staticmethod
    def _add_recovered_idempotency(
        recovered: dict[tuple[str, str, str], str],
        request: RelayRequest,
    ) -> None:
        if request.idempotency_key is not None and request.reattachable:
            recovered[
                (
                    request.rollout_id,
                    request.registration_token,
                    request.idempotency_key,
                )
            ] = request.request_id

    def _require_current_registration_locked(
        self,
        rollout_id: str,
        registration_token: str,
    ) -> None:
        current = self._rollouts.get(rollout_id)
        if current is None:
            raise web.HTTPNotFound(text=f"rollout is not registered: {rollout_id}")
        if str(current["registration_token"]) != registration_token:
            raise web.HTTPConflict(text="rollout registration is no longer current")

    def _require_request_registration_locked(
        self,
        request: RelayRequest,
        registration_token: str,
    ) -> None:
        if request.registration_token != registration_token:
            raise web.HTTPConflict(
                text="request belongs to a different rollout registration"
            )
        self._require_current_registration_locked(
            request.rollout_id,
            registration_token,
        )

    def _require_completed_registration_locked(
        self,
        request_id: str,
        registration_token: str,
    ) -> None:
        request = self._completed[request_id]
        if request.registration_token != registration_token:
            raise web.HTTPConflict(
                text="request belongs to a different rollout registration"
            )
        current = next(
            (
                record
                for record in self._rollouts.values()
                if str(record["registration_token"]) == registration_token
            ),
            None,
        )
        if current is None:
            raise web.HTTPConflict(text="rollout registration is no longer current")

    async def _cancel_rollout_incarnation_locked(
        self,
        rollout_id: str,
        registration_token: str,
        *,
        message: str,
        error_code: str,
    ) -> None:
        response = RelayWorkerResponse(410, _openai_error(message, error_code))
        now = time.time()
        abandoned = tuple(
            request.request_id
            for request in self._completed.values()
            if request.delivery_pending
            and request.rollout_id == rollout_id
            and request.registration_token == registration_token
        )
        await self._discard_completed_locked(abandoned, response)
        canceled = tuple(
            request
            for request in self._requests.values()
            if request.rollout_id == rollout_id
            and request.registration_token == registration_token
        )
        if canceled:
            await _finish_before_cancellation(
                self._complete_requests_locked(
                    canceled,
                    completed_at=now,
                    response=response,
                    defer_delivery=False,
                ),
                publish=lambda results: self._counters.__setitem__(
                    "unregister_canceled",
                    self._counters["unregister_canceled"] + len(results),
                ),
            )
        for key in list(self._workers):
            if key[0] == rollout_id:
                self._workers.pop(key, None)
        self._wake_rollout_locked(rollout_id)

    def _record_worker_heartbeat_locked(
        self,
        *,
        rollout_id: str,
        worker_id: str,
        metadata: JsonObject | None,
    ) -> JsonObject:
        now = time.time()
        key = (rollout_id, worker_id)
        previous = self._workers.get(key, {})
        self._make_worker_room_locked(key)
        record = {
            "rollout_id": rollout_id,
            "worker_id": worker_id,
            "metadata": dict(metadata or previous.get("metadata") or {}),
            "first_seen_at": previous.get("first_seen_at") or now,
            "last_seen_at": now,
        }
        self._workers[key] = record
        return dict(record)

    def _lease_request_locked(
        self,
        request: RelayRequest,
        *,
        now: float,
        lease_seconds: float,
        worker_id: str | None,
    ) -> RelayRequest:
        request.state = "leased"
        request.lease_id = uuid4().hex
        request.lease_expires_at = now + lease_seconds
        request.leased_by = worker_id
        request.delivered_at = now
        request.first_delivered_at = request.first_delivered_at or now
        request.delivery_count += 1
        self._increment_rollout_leased_locked(request.rollout_id)
        assert request.lease_id is not None
        heapq.heappush(
            self._lease_expiry_heaps.setdefault(request.rollout_id, []),
            (request.lease_expires_at, request.request_id, request.lease_id),
        )
        self._maybe_compact_lease_expiry_heap_locked(request.rollout_id)
        self._counters["delivered"] += 1
        self._timers["queue_wait_seconds_total"] += now - request.created_at
        return request

    async def _requeue_expired_leases_locked(
        self,
        now: float,
        *,
        rollout_id: str | None = None,
    ) -> None:
        rollout_ids = (
            (rollout_id,) if rollout_id is not None else tuple(self._lease_expiry_heaps)
        )
        expired: list[RelayRequest] = []
        for current_rollout_id in rollout_ids:
            heap = self._lease_expiry_heaps.get(current_rollout_id)
            if heap is None:
                continue
            while heap and heap[0][0] <= now:
                expires_at, request_id, lease_id = heapq.heappop(heap)
                request = self._requests.get(request_id)
                if (
                    request is None
                    or request.state != "leased"
                    or request.lease_id != lease_id
                    or request.lease_expires_at != expires_at
                    or request.future.done()
                ):
                    continue
                request.state = "pending"
                self._decrement_rollout_leased_locked(request.rollout_id)
                request.lease_id = None
                request.lease_expires_at = None
                request.leased_by = None
                self._pending.setdefault(request.rollout_id, deque()).appendleft(
                    request
                )
                self._counters["lease_expired"] += 1
                expired.append(request)
            if not heap:
                self._lease_expiry_heaps.pop(current_rollout_id, None)
            else:
                self._maybe_compact_lease_expiry_heap_locked(current_rollout_id)
        if expired and self._store is not None:
            await _blocking_call(self._store.save_requests, tuple(expired))
        for expired_rollout_id in {request.rollout_id for request in expired}:
            self._wake_rollout_locked(expired_rollout_id)

    async def _requeue_request_locked(self, request: RelayRequest) -> None:
        self._decrement_rollout_leased_locked(request.rollout_id)
        request.state = "pending"
        request.lease_id = None
        request.lease_expires_at = None
        request.leased_by = None
        self._pending.setdefault(request.rollout_id, deque()).appendleft(request)
        self._counters["lease_expired"] += 1
        if self._store is not None:
            await _blocking_call(self._store.save_request, request)
        self._maybe_compact_lease_expiry_heap_locked(request.rollout_id)
        self._wake_rollout_locked(request.rollout_id)

    async def _expire_requests_locked(self, now: float) -> None:
        response = RelayWorkerResponse(
            504,
            _openai_error("model relay request timed out", "relay_timeout"),
        )
        expired: list[RelayRequest] = []
        popped: list[tuple[float, str]] = []
        while self._request_expiry_heap and self._request_expiry_heap[0][0] <= now:
            expires_at, request_id = heapq.heappop(self._request_expiry_heap)
            popped.append((expires_at, request_id))
            request = self._requests.get(request_id)
            if request is None or request.expires_at != expires_at:
                continue
            expired.append(request)
        if not expired:
            return
        try:
            await _finish_before_cancellation(
                self._complete_requests_locked(
                    tuple(expired),
                    completed_at=now,
                    response=response,
                    defer_delivery=False,
                ),
                publish=lambda results: self._counters.__setitem__(
                    "timed_out",
                    self._counters["timed_out"] + len(results),
                ),
            )
        except BaseException:
            for expires_at, request_id in popped:
                request = self._requests.get(request_id)
                if request is not None and request.expires_at == expires_at:
                    heapq.heappush(
                        self._request_expiry_heap,
                        (expires_at, request_id),
                    )
            raise

    def _next_lease_expiry_locked(self, rollout_id: str) -> float | None:
        heap = self._lease_expiry_heaps.get(rollout_id)
        if heap is None:
            return None
        while heap:
            expires_at, request_id, lease_id = heap[0]
            request = self._requests.get(request_id)
            if (
                request is not None
                and request.state == "leased"
                and request.lease_id == lease_id
                and request.lease_expires_at == expires_at
            ):
                return expires_at
            heapq.heappop(heap)
        self._lease_expiry_heaps.pop(rollout_id, None)
        return None

    def _increment_rollout_leased_locked(self, rollout_id: str) -> None:
        self._rollout_leased_counts[rollout_id] = (
            self._rollout_leased_counts.get(rollout_id, 0) + 1
        )

    def _increment_counter_locked(self, counter: str) -> None:
        self._counters[counter] += 1

    def _decrement_rollout_leased_locked(self, rollout_id: str) -> None:
        count = self._rollout_leased_counts.get(rollout_id, 0)
        if count <= 1:
            self._rollout_leased_counts.pop(rollout_id, None)
        else:
            self._rollout_leased_counts[rollout_id] = count - 1

    def _maybe_compact_request_expiry_heap_locked(self) -> None:
        active_count = len(self._requests)
        if len(self._request_expiry_heap) <= (
            2 * active_count + EXPIRY_HEAP_COMPACTION_SLACK
        ):
            return
        self._request_expiry_heap = [
            (request.expires_at, request.request_id)
            for request in self._requests.values()
            if request.expires_at is not None
        ]
        heapq.heapify(self._request_expiry_heap)

    def _maybe_compact_lease_expiry_heap_locked(self, rollout_id: str) -> None:
        heap = self._lease_expiry_heaps.get(rollout_id)
        if heap is None:
            return
        leased_count = self._rollout_leased_counts.get(rollout_id, 0)
        if len(heap) <= 2 * leased_count + LEASE_HEAP_COMPACTION_SLACK:
            return
        current: dict[str, tuple[float, str, str]] = {}
        for expires_at, request_id, lease_id in heap:
            request = self._requests.get(request_id)
            if (
                request is not None
                and request.state == "leased"
                and request.lease_id == lease_id
                and request.lease_expires_at == expires_at
            ):
                current[request_id] = (expires_at, request_id, lease_id)
        compacted = list(current.values())
        if compacted:
            heapq.heapify(compacted)
            self._lease_expiry_heaps[rollout_id] = compacted
        else:
            self._lease_expiry_heaps.pop(rollout_id, None)

    def _completed_deadline(self, request: RelayRequest) -> float:
        deadline = (
            request.completed_at or 0.0
        ) + self._completed_request_retention_seconds
        if request.delivery_pending and request.expires_at is not None:
            deadline = min(deadline, request.expires_at)
        return deadline

    def _index_completed_locked(self, request: RelayRequest) -> None:
        heapq.heappush(
            self._completed_expiry_heap,
            (self._completed_deadline(request), request.request_id),
        )
        if not request.delivery_pending:
            heapq.heappush(
                self._completed_eviction_heap,
                (request.completed_at or 0.0, request.request_id),
            )

    def _rebuild_completed_indexes_locked(self) -> None:
        self._completed_expiry_heap = [
            (self._completed_deadline(r), r.request_id)
            for r in self._completed.values()
        ]
        self._completed_eviction_heap = [
            (r.completed_at or 0.0, r.request_id)
            for r in self._completed.values()
            if not r.delivery_pending
        ]
        heapq.heapify(self._completed_expiry_heap)
        heapq.heapify(self._completed_eviction_heap)

    def _compact_completed_indexes_locked(self) -> None:
        for heap, expiry in (
            (self._completed_expiry_heap, True),
            (self._completed_eviction_heap, False),
        ):
            while heap:
                timestamp, request_id = heap[0]
                request = self._completed.get(request_id)
                if request is not None and (
                    self._completed_deadline(request) == timestamp
                    if expiry
                    else not request.delivery_pending
                ):
                    break
                heapq.heappop(heap)
        limit = 2 * len(self._completed) + EXPIRY_HEAP_COMPACTION_SLACK
        if (
            max(len(self._completed_expiry_heap), len(self._completed_eviction_heap))
            > limit
        ):
            self._rebuild_completed_indexes_locked()

    async def _discard_completed_locked(
        self,
        request_ids: tuple[str, ...],
        response: RelayWorkerResponse,
    ) -> None:
        if not request_ids:
            return

        async def discard() -> None:
            if self._store is not None and request_ids:
                await _blocking_call(self._store.delete_requests, request_ids)
            for request_id in request_ids:
                request = self._remove_completed_locked(request_id)
                if request is not None and request.delivery_pending:
                    request.completed_response = response
                    request.delivery_pending = False
                    _set_response(request.future, response)
            self._compact_completed_indexes_locked()

        await _finish_before_cancellation(discard())

    async def _prune_completed_locked(self, now: float) -> None:
        popped: list[tuple[float, str]] = []
        expired: list[str] = []
        while self._completed_expiry_heap and self._completed_expiry_heap[0][0] < now:
            entry = heapq.heappop(self._completed_expiry_heap)
            popped.append(entry)
            deadline, request_id = entry
            request = self._completed.get(request_id)
            if request is not None and self._completed_deadline(request) == deadline:
                expired.append(request_id)
        try:
            await self._discard_completed_locked(
                tuple(expired),
                RelayWorkerResponse(
                    504, _openai_error("relay delivery timed out", "relay_timeout")
                ),
            )
        except BaseException:
            for entry in popped:
                if entry[1] in self._completed:
                    heapq.heappush(self._completed_expiry_heap, entry)
            raise

    async def _release_completed_locked(self, request: RelayRequest) -> None:
        if request.delivery_pending:
            released = replace(request, delivery_pending=False)
            if self._store is not None:
                await _blocking_call(self._store.save_request, released)
            request.delivery_pending = False
            self._index_completed_locked(request)
        assert request.completed_response is not None
        _set_response(request.future, request.completed_response)

    async def _complete_requests_locked(
        self,
        requests: tuple[RelayRequest, ...],
        *,
        completed_at: float,
        response: RelayWorkerResponse,
        defer_delivery: bool,
        remove_active: bool = True,
    ) -> tuple[tuple[RelayRequest, RelayWorkerResponse], ...]:
        if not requests:
            return ()
        if len({request.request_id for request in requests}) != len(requests):
            raise RuntimeError("relay terminal batch contains duplicate requests")
        if remove_active:
            for request in requests:
                if self._requests.get(request.request_id) is not request:
                    raise RuntimeError("relay request is no longer active")

        planned, evicted_ids, retained_ids = self._plan_terminal_batch_locked(
            requests,
            completed_at=completed_at,
            response=response,
            defer_delivery=defer_delivery,
        )
        durable_requests = tuple(durable for _request, durable, _response in planned)
        if self._store is not None:
            await _blocking_call(
                self._store.commit_request_batch,
                durable_requests,
                evicted_ids,
            )

        removed_ids = {request.request_id for request in requests}
        affected_rollouts: set[str] = set()
        if remove_active:
            for request in requests:
                self._requests.pop(request.request_id)
                affected_rollouts.add(request.rollout_id)
                if request.state == "leased":
                    self._decrement_rollout_leased_locked(request.rollout_id)
                self._inflight_bytes = max(
                    0,
                    self._inflight_bytes - request.payload_bytes,
                )
            for rollout_id in affected_rollouts:
                queue = self._pending.get(rollout_id)
                if queue is None:
                    continue
                kept = deque(
                    queued for queued in queue if queued.request_id not in removed_ids
                )
                if kept:
                    self._pending[rollout_id] = kept
                else:
                    self._pending.pop(rollout_id, None)

        for request_id in evicted_ids:
            self._remove_completed_locked(request_id)
        results: list[tuple[RelayRequest, RelayWorkerResponse]] = []
        for request, durable, normalized_response in planned:
            request.completed_at = durable.completed_at
            request.completed_response = durable.completed_response
            request.completed_bytes = durable.completed_bytes
            request.state = durable.state
            request.body = durable.body
            request.headers = durable.headers
            request.payload_bytes = durable.payload_bytes
            request.delivery_pending = durable.delivery_pending
            if request.request_id in retained_ids:
                self._completed[request.request_id] = request
                self._completed_bytes += request.completed_bytes
                self._index_completed_locked(request)
            elif request.idempotency_key is not None and request.reattachable:
                self._idempotency.pop(
                    (
                        request.rollout_id,
                        request.registration_token,
                        request.idempotency_key,
                    ),
                    None,
                )
            if not defer_delivery:
                _set_response(request.future, normalized_response)
            results.append((request, normalized_response))

        self._compact_completed_indexes_locked()
        if remove_active:
            self._maybe_compact_request_expiry_heap_locked()
            for rollout_id in affected_rollouts:
                self._maybe_compact_lease_expiry_heap_locked(rollout_id)
        return tuple(results)

    def _plan_terminal_batch_locked(
        self,
        requests: tuple[RelayRequest, ...],
        *,
        completed_at: float,
        response: RelayWorkerResponse,
        defer_delivery: bool,
    ) -> tuple[
        tuple[tuple[RelayRequest, RelayRequest, RelayWorkerResponse], ...],
        tuple[str, ...],
        frozenset[str],
    ]:
        # Traverse the existing heap lazily without mutating it: a failed durable
        # commit must leave the eviction index and in-memory state unchanged.
        retained: dict[str, RelayRequest] = {}
        evicted: set[str] = set()
        retained_count = len(self._completed)
        retained_bytes = self._completed_bytes
        frontier: list[tuple[tuple[float, str], int]] = []
        if self._completed_eviction_heap:
            frontier.append((self._completed_eviction_heap[0], 0))
        evictable: list[tuple[float, str]] = []

        def next_existing() -> tuple[float, str] | None:
            while frontier:
                entry, index = frontier[0]
                existing = self._completed.get(entry[1])
                if (
                    existing is not None
                    and not existing.delivery_pending
                    and entry[1] not in evicted
                ):
                    return entry
                advance_existing()
            return None

        def advance_existing() -> None:
            _entry, index = heapq.heappop(frontier)
            for child in (2 * index + 1, 2 * index + 2):
                if child < len(self._completed_eviction_heap):
                    heapq.heappush(
                        frontier, (self._completed_eviction_heap[child], child)
                    )

        def make_room(completed_bytes: int) -> None:
            nonlocal retained_count, retained_bytes
            while (
                retained_count >= self._max_completed_requests
                or retained_bytes + completed_bytes > self._max_completed_bytes
            ):
                entry = next_existing()
                if evictable and (entry is None or evictable[0] < entry):
                    _at, request_id = heapq.heappop(evictable)
                    existing = retained.pop(request_id)
                elif entry is not None:
                    request_id = entry[1]
                    existing = self._completed[request_id]
                    advance_existing()
                else:
                    raise web.HTTPServiceUnavailable(
                        text="relay completed-response capacity is pinned",
                        headers={"Retry-After": "1"},
                    )
                evicted.add(request_id)
                retained_count -= 1
                retained_bytes -= existing.completed_bytes

        normalized_response = response
        completed_bytes = _relay_response_retained_bytes(normalized_response)
        if completed_bytes > self._max_completed_bytes:
            normalized_response = RelayWorkerResponse(
                502,
                _openai_error(
                    "worker response exceeds relay completed-response capacity",
                    "relay_response_capacity_exceeded",
                ),
            )
            completed_bytes = _relay_response_retained_bytes(normalized_response)

        planned: list[tuple[RelayRequest, RelayRequest, RelayWorkerResponse]] = []
        for request in requests:
            make_room(completed_bytes)
            durable = replace(
                request,
                completed_at=completed_at,
                completed_response=normalized_response,
                completed_bytes=completed_bytes,
                state="completed",
                body=None,
                headers={},
                payload_bytes=0,
                delivery_pending=defer_delivery,
            )
            retained[request.request_id] = durable
            retained_count += 1
            retained_bytes += completed_bytes
            if not defer_delivery:
                heapq.heappush(
                    evictable,
                    (completed_at, request.request_id),
                )
            planned.append((request, durable, normalized_response))
        return tuple(planned), tuple(sorted(evicted)), frozenset(retained)

    def _remove_completed_locked(self, request_id: str) -> RelayRequest | None:
        request = self._completed.pop(request_id, None)
        if request is None:
            return None
        self._completed_bytes = max(
            0,
            self._completed_bytes - request.completed_bytes,
        )
        if request.idempotency_key is not None and request.reattachable:
            self._idempotency.pop(
                (
                    request.rollout_id,
                    request.registration_token,
                    request.idempotency_key,
                ),
                None,
            )
        return request

    def _rollout_wakeup_locked(self, rollout_id: str) -> asyncio.Event:
        wakeup = self._rollout_wakeups.get(rollout_id)
        if wakeup is None:
            wakeup = asyncio.Event()
            self._rollout_wakeups[rollout_id] = wakeup
        return wakeup

    def _wake_rollout_locked(self, rollout_id: str) -> None:
        wakeup = self._rollout_wakeups.pop(rollout_id, None)
        if wakeup is not None:
            wakeup.set()

    async def _wait_for_rollout_wakeup(
        self,
        wakeup: asyncio.Event,
        timeout_seconds: float,
    ) -> None:
        try:
            await asyncio.wait_for(wakeup.wait(), timeout_seconds)
        except asyncio.TimeoutError:
            pass

    def _prune_workers_locked(self, now: float) -> None:
        cutoff = now - self._worker_retention_seconds
        for key, record in list(self._workers.items()):
            if float(record.get("last_seen_at") or 0.0) < cutoff:
                self._workers.pop(key, None)

    def _make_worker_room_locked(self, key: tuple[str, str]) -> None:
        if key in self._workers or len(self._workers) < self._max_workers:
            return
        oldest = min(
            self._workers,
            key=lambda item: (
                float(self._workers[item].get("last_seen_at") or 0.0),
                item,
            ),
        )
        self._workers.pop(oldest, None)


STATE_KEY = web.AppKey("model_relay_state", ModelRelayState)


def create_model_relay_app(
    *,
    sandbox_bearer_token: str | None = None,
    worker_bearer_token: str | None = None,
    request_timeout_seconds: float = DEFAULT_RELAY_REQUEST_TIMEOUT_SECONDS,
    worker_poll_timeout_seconds: float = DEFAULT_WORKER_POLL_TIMEOUT_SECONDS,
    worker_lease_seconds: float = DEFAULT_WORKER_LEASE_SECONDS,
    maintenance_interval_seconds: float = DEFAULT_MAINTENANCE_INTERVAL_SECONDS,
    completed_request_retention_seconds: float = DEFAULT_COMPLETED_REQUEST_RETENTION_SECONDS,
    worker_retention_seconds: float = DEFAULT_WORKER_RETENTION_SECONDS,
    max_inflight_requests: int = DEFAULT_MAX_INFLIGHT_REQUESTS,
    max_inflight_requests_per_rollout: int = DEFAULT_MAX_INFLIGHT_REQUESTS_PER_ROLLOUT,
    max_inflight_bytes: int = DEFAULT_MAX_INFLIGHT_BYTES,
    max_completed_requests: int = DEFAULT_MAX_COMPLETED_REQUESTS,
    max_completed_bytes: int = DEFAULT_MAX_COMPLETED_BYTES,
    max_workers: int = DEFAULT_MAX_WORKERS,
    state_path: Path | None = None,
    accepted_notifier: Callable[[RelayRequest], Awaitable[str | None]] | None = None,
    result_notifier: Callable[[RelayRequest], Awaitable[str | None]] | None = None,
    telemetry: Telemetry | None = None,
) -> web.Application:
    # Base64 expands worker response bodies by 4/3 inside the JSON control API.
    resolved_telemetry = telemetry or Telemetry.disabled("model-relay")
    app = web.Application(
        client_max_size=48 * 1024**2,
        middlewares=[_telemetry_middleware] if resolved_telemetry.enabled else (),
    )
    app[TELEMETRY_KEY] = resolved_telemetry
    app[STATE_KEY] = ModelRelayState(
        state_path=state_path,
        request_timeout_seconds=request_timeout_seconds,
        completed_request_retention_seconds=completed_request_retention_seconds,
        worker_retention_seconds=worker_retention_seconds,
        max_inflight_requests=max_inflight_requests,
        max_inflight_requests_per_rollout=max_inflight_requests_per_rollout,
        max_inflight_bytes=max_inflight_bytes,
        max_completed_requests=max_completed_requests,
        max_completed_bytes=max_completed_bytes,
        max_workers=max_workers,
    )
    app[SANDBOX_TOKEN_KEY] = sandbox_bearer_token
    app[WORKER_TOKEN_KEY] = worker_bearer_token
    app[POLL_TIMEOUT_KEY] = worker_poll_timeout_seconds
    app[REQUEST_TIMEOUT_KEY] = request_timeout_seconds
    app[LEASE_SECONDS_KEY] = worker_lease_seconds
    app[ACCEPTED_NOTIFIER_KEY] = accepted_notifier
    app[RESULT_NOTIFIER_KEY] = result_notifier

    async def maintain_state(_app: web.Application):
        interval = max(0.01, maintenance_interval_seconds)
        task = asyncio.create_task(
            _model_relay_maintenance_loop(_app[STATE_KEY], interval)
        )
        try:
            yield
        finally:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

    app.cleanup_ctx.append(maintain_state)

    async def close_state(_app: web.Application) -> None:
        await _app[STATE_KEY].aclose()

    app.on_cleanup.append(close_state)

    app.router.add_get("/healthz", healthz)
    app.router.add_get("/v1/relay/stats", relay_stats)
    app.router.add_get("/v1/relay/rollouts", list_rollouts)
    app.router.add_post("/v1/relay/rollouts", register_rollout)
    app.router.add_delete(
        "/v1/relay/rollouts/{rollout_id}",
        unregister_rollout,
    )
    app.router.add_post("/worker/heartbeat", worker_heartbeat)
    app.router.add_get("/worker/poll", worker_poll)
    app.router.add_post("/worker/renew", worker_renew)
    app.router.add_post("/worker/respond", worker_respond)
    app.router.add_post("/worker/error", worker_error)
    app.router.add_post(
        "/rollouts/{rollout_id}/v1/chat/completions",
        openai_chat_completions,
    )
    app.router.add_post("/rollouts/{rollout_id}/v1/responses", openai_responses)
    app.router.add_route(
        "*",
        "/tunnels/{rollout_id}/_relay/{registration_token}",
        tunnel_http_proxy,
    )
    app.router.add_route(
        "*",
        "/tunnels/{rollout_id}/_relay/{registration_token}/{tunnel_path:.*}",
        tunnel_http_proxy,
    )
    app.router.add_route("*", "/tunnels/{rollout_id}", tunnel_http_proxy)
    app.router.add_route(
        "*",
        "/tunnels/{rollout_id}/{tunnel_path:.*}",
        tunnel_http_proxy,
    )
    return app


async def _model_relay_maintenance_loop(
    state: ModelRelayState,
    interval_seconds: float,
) -> None:
    while True:
        try:
            await state.maintain()
        except asyncio.CancelledError:
            raise
        except Exception:
            LOGGER.exception("model relay background maintenance failed")
        await asyncio.sleep(interval_seconds)


@web.middleware
async def _telemetry_middleware(
    request: web.Request,
    handler: Callable[[web.Request], Awaitable[web.StreamResponse]],
) -> web.StreamResponse:
    telemetry = request.app[TELEMETRY_KEY]
    route = getattr(request.match_info.route.resource, "canonical", request.path)
    attributes: dict[str, Any] = {
        "http.request.method": request.method,
        "http.route": route,
        "url.path": request.path,
    }
    for key, attribute in (("rollout_id", "relay.rollout.id"),):
        value = request.match_info.get(key)
        if value:
            attributes[attribute] = value[:256]
    parent_context = telemetry.extracted_context(dict(request.headers))
    with telemetry.span(
        f"{request.method} {route}",
        kind=SpanKind.SERVER,
        attributes=attributes,
        parent_context=parent_context,
        metric_operation="http.server.request",
    ) as span:
        try:
            response = await handler(request)
        except web.HTTPException as exc:
            span.set_attribute("http.response.status_code", int(exc.status))
            if exc.status >= 500:
                span.status = "error"
            headers: dict[str, str] = {}
            telemetry.inject(headers)
            exc.headers.update(headers)
            trace_id = trace_id_hex()
            if trace_id:
                exc.headers["X-Trace-Id"] = trace_id
            raise
        status = int(response.status)
        span.set_attribute("http.response.status_code", status)
        if status >= 500:
            span.status = "error"
        headers: dict[str, str] = {}
        telemetry.inject(headers)
        response.headers.update(headers)
        trace_id = trace_id_hex()
        if trace_id:
            response.headers["X-Trace-Id"] = trace_id
        return response


async def healthz(_request: web.Request) -> web.Response:
    return web.json_response(service_health("model-relay"))


async def relay_stats(request: web.Request) -> web.Response:
    _require_worker_token(request)
    return web.json_response(await _state(request).stats())


async def list_rollouts(request: web.Request) -> web.Response:
    _require_worker_token(request)
    return web.json_response({"rollouts": await _state(request).list_rollouts()})


async def register_rollout(request: web.Request) -> web.Response:
    _require_worker_token(request)
    payload = await _json_object(request)
    rollout_id = str(payload.get("rollout_id") or "")
    metadata = payload.get("metadata")
    if metadata is not None and not isinstance(metadata, dict):
        raise web.HTTPBadRequest(text="metadata must be a JSON object")
    record = await _state(request).register_rollout(rollout_id, metadata)
    return web.json_response({"ok": True, "rollout": record}, status=201)


async def unregister_rollout(request: web.Request) -> web.Response:
    _require_worker_token(request)
    payload = await _json_object(request)
    rollout_id = str(request.match_info.get("rollout_id") or "")
    registration_token = _registration_token_from_payload(payload)
    existed = await _state(request).unregister_rollout(
        rollout_id,
        registration_token=registration_token,
    )
    return web.json_response(
        {
            "ok": True,
            "rollout_id": rollout_id,
            "existed": existed,
        }
    )


async def worker_poll(request: web.Request) -> web.Response:
    _require_worker_token(request)
    rollout_id = str(request.query.get("rollout_id") or "")
    registration_token = _registration_token_from_request(request)
    worker_id = _worker_id_from_request(request)
    timeout_seconds = _float_query(
        request,
        "timeout_seconds",
        default=request.app[POLL_TIMEOUT_KEY],
    )
    limit = _int_query(request, "limit", default=1, minimum=1, maximum=256)
    lease_seconds = _float_query(
        request,
        "lease_seconds",
        default=request.app[LEASE_SECONDS_KEY],
    )
    relay_requests = await _state(request).poll(
        rollout_id=rollout_id,
        registration_token=registration_token,
        timeout_seconds=timeout_seconds,
        limit=limit,
        lease_seconds=lease_seconds,
        worker_id=worker_id,
    )
    envelopes = [relay_request.envelope() for relay_request in relay_requests]
    return web.json_response({"requests": envelopes})


async def worker_heartbeat(request: web.Request) -> web.Response:
    _require_worker_token(request)
    payload = await _json_object(request)
    rollout_id = str(payload.get("rollout_id") or "")
    registration_token = _registration_token_from_payload(payload)
    worker_id = str(payload.get("worker_id") or "")
    metadata = payload.get("metadata")
    if metadata is not None and not isinstance(metadata, dict):
        raise web.HTTPBadRequest(text="metadata must be a JSON object")
    record = await _state(request).record_worker_heartbeat(
        rollout_id=rollout_id,
        registration_token=registration_token,
        worker_id=worker_id,
        metadata=metadata,
    )
    return web.json_response({"ok": True, "worker": record})


async def worker_renew(request: web.Request) -> web.Response:
    _require_worker_token(request)
    payload = await _json_object(request)
    request_id = str(payload.get("request_id") or "")
    registration_token = _registration_token_from_payload(payload)
    lease_id = str(payload.get("lease_id") or "")
    if not request_id:
        raise web.HTTPBadRequest(text="request_id is required")
    if not lease_id:
        raise web.HTTPBadRequest(text="lease_id is required")
    raw_lease_seconds = payload.get("lease_seconds")
    try:
        lease_seconds = (
            request.app[LEASE_SECONDS_KEY]
            if raw_lease_seconds is None
            else float(raw_lease_seconds)
        )
    except (TypeError, ValueError) as exc:
        raise web.HTTPBadRequest(text="lease_seconds must be a number") from exc
    worker_id = payload.get("worker_id")
    renewed = await _state(request).renew_lease(
        request_id=request_id,
        registration_token=registration_token,
        lease_id=lease_id,
        lease_seconds=lease_seconds,
        worker_id=str(worker_id) if worker_id else None,
    )
    return web.json_response({"ok": True, "request": renewed.envelope()})


async def worker_respond(request: web.Request) -> web.Response:
    _require_worker_token(request)
    payload = await _json_object(request)
    request_id = str(payload.get("request_id") or "")
    registration_token = _registration_token_from_payload(payload)
    if not request_id:
        raise web.HTTPBadRequest(text="request_id is required")
    lease_id = str(payload.get("lease_id") or "")
    try:
        body = _decoded_body(payload["body"])
    except (KeyError, ValueError) as exc:
        raise web.HTTPBadRequest(text="worker response body is invalid") from exc
    if isinstance(body, bytes) and len(body) > MAX_WORKER_RESPONSE_BYTES:
        raise web.HTTPRequestEntityTooLarge(
            max_size=MAX_WORKER_RESPONSE_BYTES,
            actual_size=len(body),
        )
    status = _status_code(payload.get("status"), default=200)
    headers = _string_mapping(payload.get("headers"))
    result = await _state(request).respond(
        request_id=request_id,
        registration_token=registration_token,
        lease_id=lease_id,
        response=RelayWorkerResponse(status=status, body=body, headers=headers),
        defer_delivery=request.app[RESULT_NOTIFIER_KEY] is not None,
    )
    return await _worker_completion_response(request, result)


async def worker_error(request: web.Request) -> web.Response:
    _require_worker_token(request)
    payload = await _json_object(request)
    request_id = str(payload.get("request_id") or "")
    registration_token = _registration_token_from_payload(payload)
    if not request_id:
        raise web.HTTPBadRequest(text="request_id is required")
    lease_id = str(payload.get("lease_id") or "")
    status = _status_code(payload.get("status"), default=502)
    message = str(payload.get("error") or "worker error")
    explicit_retryable = payload.get("retryable")
    retryable = (
        explicit_retryable
        if isinstance(explicit_retryable, bool)
        else _worker_error_is_retryable(status, message)
    )
    if retryable:
        retried = await _state(request).retry_worker_failure(
            request_id=request_id,
            registration_token=registration_token,
            lease_id=lease_id,
        )
        if retried is not None:
            return web.json_response(
                {
                    "ok": True,
                    "request_id": retried.request_id,
                    "retried": True,
                    "delivery_count": retried.delivery_count,
                }
            )
    result = await _state(request).respond(
        request_id=request_id,
        registration_token=registration_token,
        lease_id=lease_id,
        response=RelayWorkerResponse(
            status=status,
            body=_openai_error(message, "relay_worker_error"),
        ),
        error=True,
        defer_delivery=request.app[RESULT_NOTIFIER_KEY] is not None,
    )
    return await _worker_completion_response(request, result)


async def _worker_completion_response(
    request: web.Request,
    result: RelayRespondResult,
) -> web.Response:
    await _notify_result(request, result)
    if request.app[RESULT_NOTIFIER_KEY] is not None:
        await _state(request).release_completed_response(result.request.request_id)
    return web.json_response(
        {
            "ok": True,
            "request_id": result.request.request_id,
            "duplicate": result.duplicate,
        }
    )


def _worker_error_is_retryable(status: int, message: str) -> bool:
    """Classify transport/provider failures that should release their lease."""

    if status in {408, 425, 429, 500, 502, 503, 504}:
        return True
    lowered = message.lower()
    return any(
        marker in lowered
        for marker in (
            "server disconnected",
            "connection reset",
            "connection closed",
            "connection refused",
            "temporarily unavailable",
            "timed out",
            "timeout",
            "unexpected eof",
            "remote protocol error",
        )
    )


async def openai_chat_completions(request: web.Request) -> web.Response:
    return await _openai_proxy(request, endpoint="/v1/chat/completions")


async def openai_responses(request: web.Request) -> web.Response:
    return await _openai_proxy(request, endpoint="/v1/responses")


async def tunnel_http_proxy(request: web.Request) -> web.Response:
    if request.method not in TUNNEL_HTTP_METHODS:
        raise web.HTTPMethodNotAllowed(request.method, sorted(TUNNEL_HTTP_METHODS))
    rollout_id = str(request.match_info.get("rollout_id") or "")
    validate_rollout_id(rollout_id)
    registration_token = request.match_info.get("registration_token")
    if registration_token is None:
        _require_sandbox_token(request)
    else:
        await _state(request).require_current_registration(
            rollout_id,
            str(registration_token),
        )
    endpoint = _tunnel_endpoint(request)
    body_bytes = await request.read()
    if len(body_bytes) > MAX_RELAY_BODY_BYTES:
        raise web.HTTPRequestEntityTooLarge(
            max_size=MAX_RELAY_BODY_BYTES,
            actual_size=len(body_bytes),
        )
    explicit_request_id = request.headers.get(RELAY_REQUEST_ID_HEADER)
    relay_request = await _state(request).enqueue(
        rollout_id=rollout_id,
        endpoint=endpoint,
        method=request.method,
        body=body_bytes,
        headers=_forward_headers(request),
        idempotency_key=(
            explicit_request_id
            or _implicit_idempotency_key(
                request,
                rollout_id=rollout_id,
                endpoint=endpoint,
                body_bytes=body_bytes,
            )
        ),
        defer_idempotency_until_disconnect=explicit_request_id is None,
    )
    await _notify_accepted(request, relay_request)
    response = await _wait_for_worker_response(
        request,
        relay_request,
        openai_errors=False,
    )
    return _generic_http_response(request, response)


async def _openai_proxy(request: web.Request, *, endpoint: str) -> web.Response:
    _require_sandbox_token(request)
    payload = await _json_object(request)
    if payload.get("stream"):
        return web.json_response(
            _openai_error(
                "streaming model relay is not implemented yet",
                "relay_streaming_unsupported",
            ),
            status=400,
        )
    rollout_id = _rollout_id_from_request(request)
    body_bytes = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    explicit_request_id = request.headers.get(RELAY_REQUEST_ID_HEADER)
    relay_request = await _state(request).enqueue(
        rollout_id=rollout_id,
        endpoint=endpoint,
        body=payload,
        headers=_forward_headers(request),
        idempotency_key=(
            explicit_request_id
            or _implicit_idempotency_key(
                request,
                rollout_id=rollout_id,
                endpoint=endpoint,
                body_bytes=body_bytes,
            )
        ),
        defer_idempotency_until_disconnect=explicit_request_id is None,
    )
    await _notify_accepted(request, relay_request)
    response = await _wait_for_worker_response(
        request,
        relay_request,
        openai_errors=True,
    )
    if isinstance(response.body, bytes):
        try:
            response_body = json.loads(response.body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            response_body = _openai_error(
                "relay worker returned a non-JSON OpenAI response",
                "relay_invalid_worker_response",
            )
            response = RelayWorkerResponse(502, response_body)
    else:
        response_body = response.body
    return web.json_response(
        response_body,
        status=response.status,
        headers=_safe_response_headers(response.headers),
    )


async def _wait_for_worker_response(
    request: web.Request,
    relay_request: RelayRequest,
    *,
    openai_errors: bool,
) -> RelayWorkerResponse:
    with _telemetry(request).span(
        "relay.wait_for_worker",
        attributes={
            "relay.request.id": relay_request.request_id,
            "relay.rollout.id": relay_request.rollout_id,
        },
    ) as span:
        try:
            response = await _state(request).wait_for_response(
                relay_request,
                timeout_seconds=request.app[REQUEST_TIMEOUT_KEY],
            )
        except asyncio.TimeoutError:
            span.set_attribute("relay.outcome", "timeout")
            timeout_response = RelayWorkerResponse(
                504,
                _relay_error(
                    "relay request timed out",
                    "relay_timeout",
                    openai=openai_errors,
                ),
            )
            persisted = await _state(request).cancel_request(
                request_id=relay_request.request_id,
                response=timeout_response,
                reason="timeout",
            )
            response = persisted or timeout_response
        except asyncio.CancelledError:
            span.set_attribute("relay.outcome", "caller_detached")
            # A parked or migrated sandbox necessarily loses this TCP connection.
            # The durable request remains claimable and the next byte-identical
            # retry reattaches to it instead of sampling again.
            await _state(request).mark_caller_detached(relay_request.request_id)
            raise
        else:
            span.set_attribute("relay.outcome", "completed")
        return response


async def _notify_accepted(
    request: web.Request,
    relay_request: RelayRequest,
) -> None:
    notifier = request.app[ACCEPTED_NOTIFIER_KEY]
    if notifier is None or relay_request.sandbox_id is None:
        return

    async def notify_and_persist() -> None:
        async with relay_request.lifecycle_lock:
            # A fast worker may commit the result before this task wins the
            # lifecycle lock. Never park after a result is already ready.
            if (
                relay_request.accepted_notified_at is not None
                or relay_request.completed_at is not None
            ):
                return
            try:
                transport_epoch = await notifier(relay_request)
            except Exception:
                # Parking is an optimization. Once the request has been durably
                # accepted, failure to park must not turn it into a model-call
                # failure, but it must remain observable to operators.
                LOGGER.warning(
                    "model relay failed to park sandbox %s for request %s",
                    relay_request.sandbox_id,
                    relay_request.request_id,
                    exc_info=True,
                )
                return
            await _state(request).mark_accepted_notified(
                relay_request.request_id,
                transport_epoch=transport_epoch,
            )

    # Checkpointing the caller can destroy this HTTP connection. Do not let
    # that cancellation interrupt the durable lifecycle transition or leave a
    # successfully parked request looking unacknowledged after restart.
    with _telemetry(request).span(
        "relay.park_caller",
        attributes={
            "relay.request.id": relay_request.request_id,
            "sandbox.id": relay_request.sandbox_id,
        },
    ):
        await _finish_before_cancellation(notify_and_persist())


async def _notify_result(
    request: web.Request,
    result: RelayRespondResult,
) -> None:
    relay_request = result.request
    notifier = request.app[RESULT_NOTIFIER_KEY]
    if (
        notifier is None
        or relay_request.sandbox_id is None
        or relay_request.wake_notified_at is not None
    ):
        return
    telemetry = _telemetry(request)
    original_request_link = telemetry.link_from_headers(relay_request.headers)
    with telemetry.span(
        "relay.wake_caller",
        attributes={
            "relay.request.id": relay_request.request_id,
            "relay.rollout.id": relay_request.rollout_id,
            "sandbox.id": relay_request.sandbox_id,
        },
        links=((original_request_link,) if original_request_link is not None else ()),
    ):
        async with relay_request.lifecycle_lock:
            if relay_request.wake_notified_at is not None:
                return
            try:
                wake_transport_epoch = await notifier(relay_request)
            except Exception as exc:
                # The result is already committed. A 503 makes the worker retry its
                # idempotent response POST, which re-attempts only the wake notification.
                raise web.HTTPServiceUnavailable(
                    text=f"model response committed but sandbox wake failed: {exc}",
                    headers={"Retry-After": "1"},
                ) from exc
            if (
                relay_request.parked_transport_epoch is not None
                and wake_transport_epoch is not None
                and relay_request.parked_transport_epoch != wake_transport_epoch
            ):
                await _state(request).mark_transport_reset(relay_request.request_id)
            await _state(request).mark_wake_notified(relay_request.request_id)


def _state(request: web.Request) -> ModelRelayState:
    return request.app[STATE_KEY]


def _telemetry(request: web.Request) -> Telemetry:
    return request.app.get(TELEMETRY_KEY, _DISABLED_TELEMETRY)


async def _json_object(request: web.Request) -> JsonObject:
    if (
        request.content_length is not None
        and request.content_length > MAX_RELAY_BODY_BYTES
    ):
        raise web.HTTPRequestEntityTooLarge(
            max_size=MAX_RELAY_BODY_BYTES,
            actual_size=request.content_length,
        )
    try:
        payload = await request.json()
    except Exception as exc:  # aiohttp raises different JSON errors by version.
        raise web.HTTPBadRequest(text=f"invalid JSON body: {exc}") from exc
    if not isinstance(payload, dict):
        raise web.HTTPBadRequest(text="request body must be a JSON object")
    return payload


def _rollout_id_from_request(request: web.Request) -> str:
    rollout_id = str(request.match_info.get("rollout_id") or "")
    validate_rollout_id(rollout_id)
    return rollout_id


def validate_rollout_id(value: str) -> None:
    if not ROLLOUT_ID_RE.match(value):
        raise web.HTTPBadRequest(
            text=(
                "rollout_id must be 1-128 characters of letters, digits, "
                "_, ., : or - and start with a letter or digit"
            )
        )


def validate_worker_id(value: str) -> None:
    if not WORKER_ID_RE.match(value):
        raise web.HTTPBadRequest(
            text=(
                "worker_id must be 1-128 characters of letters, digits, "
                "_, ., :, @ or - and start with a letter or digit"
            )
        )


def validate_registration_token(value: str) -> None:
    if not REGISTRATION_TOKEN_RE.fullmatch(value):
        raise web.HTTPBadRequest(
            text="registration_token must be the 32-character token returned by register_rollout"
        )


def _registration_token_from_payload(payload: JsonObject) -> str:
    registration_token = str(payload.get("registration_token") or "")
    validate_registration_token(registration_token)
    return registration_token


def _registration_token_from_request(request: web.Request) -> str:
    registration_token = str(request.query.get("registration_token") or "")
    validate_registration_token(registration_token)
    return registration_token


def _worker_id_from_request(request: web.Request) -> str | None:
    raw = request.query.get("worker_id") or None
    if raw is None:
        return None
    validate_worker_id(raw)
    return raw


def _require_sandbox_token(request: web.Request) -> None:
    _require_bearer_token(request, request.app[SANDBOX_TOKEN_KEY])


def _require_worker_token(request: web.Request) -> None:
    _require_bearer_token(request, request.app[WORKER_TOKEN_KEY])


def _require_bearer_token(request: web.Request, expected: str | None) -> None:
    if expected is None:
        return
    relay_token = request.headers.get(RELAY_TOKEN_HEADER) or ""
    if relay_token in {expected, f"Bearer {expected}"}:
        return
    raw = request.headers.get("Authorization") or ""
    if raw != f"Bearer {expected}":
        raise web.HTTPUnauthorized(text="missing or invalid bearer token")


def _float_query(request: web.Request, name: str, *, default: float) -> float:
    raw = request.query.get(name)
    if raw is None:
        return default
    try:
        return max(0.0, float(raw))
    except ValueError as exc:
        raise web.HTTPBadRequest(text=f"{name} must be a number") from exc


def _int_query(
    request: web.Request,
    name: str,
    *,
    default: int,
    minimum: int,
    maximum: int,
) -> int:
    raw = request.query.get(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise web.HTTPBadRequest(text=f"{name} must be an integer") from exc
    return max(minimum, min(maximum, value))


def _status_code(raw: object, *, default: int) -> int:
    if raw is None:
        return default
    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        raise web.HTTPBadRequest(text="status must be an integer") from exc
    if value < 100 or value > 599:
        raise web.HTTPBadRequest(text="status must be in [100, 599]")
    return value


def _string_mapping(raw: object) -> dict[str, str]:
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise web.HTTPBadRequest(text="headers must be a JSON object")
    return {str(key): str(value) for key, value in raw.items()}


def _forward_headers(request: web.Request) -> dict[str, str]:
    blocked = {
        "connection",
        "content-length",
        "forwarded",
        "host",
        "job-id",
        "proxy-authorization",
        "transfer-encoding",
        RELAY_TOKEN_HEADER.lower(),
        RELAY_REQUEST_ID_HEADER.lower(),
        "x-real-ip",
        "x-ucloud-sandbox-token",
    }
    expected = request.app[SANDBOX_TOKEN_KEY]
    relay_header = request.headers.get(RELAY_TOKEN_HEADER) or ""
    relay_header_authenticated = (
        request.match_info.get("registration_token") is not None
        or expected is None
        or relay_header in {expected, f"Bearer {expected}"}
    )
    if not relay_header_authenticated:
        # OpenAI clients carry relay authentication in Authorization; never
        # leak that credential to the worker-local upstream.
        blocked.add("authorization")
    return {
        key: value
        for key, value in request.headers.items()
        if (key.lower() not in blocked and not key.lower().startswith("x-forwarded-"))
    }


def _safe_response_headers(
    headers: dict[str, str],
    *,
    preserve_content_type: bool = False,
) -> dict[str, str]:
    blocked = {
        "connection",
        "content-length",
        "proxy-authenticate",
        "proxy-authorization",
        "transfer-encoding",
    }
    if not preserve_content_type:
        blocked.add("content-type")
    return {key: value for key, value in headers.items() if key.lower() not in blocked}


def _openai_error(message: str, error_type: str) -> JsonObject:
    return {"error": {"message": message, "type": error_type}}


def _relay_error(message: str, error_type: str, *, openai: bool) -> JsonObject:
    if openai:
        return _openai_error(message, error_type)
    return {"error": message, "code": error_type}


def _tunnel_endpoint(request: web.Request) -> str:
    # Work from raw_path rather than match_info so percent-encoding, repeated
    # query parameters, and literal '+' characters reach the upstream exactly.
    raw_path, separator, raw_query = request.raw_path.partition("?")
    if request.match_info.get("registration_token") is None:
        path_parts = raw_path.split("/", 3)
        tunnel_path = path_parts[3] if len(path_parts) == 4 else ""
    else:
        path_parts = raw_path.split("/", 5)
        tunnel_path = path_parts[5] if len(path_parts) == 6 else ""
    endpoint = f"/{tunnel_path}"
    return f"{endpoint}?{raw_query}" if separator else endpoint


def _generic_http_response(
    request: web.Request,
    response: RelayWorkerResponse,
) -> web.Response:
    headers = _safe_response_headers(
        response.headers,
        preserve_content_type=True,
    )
    if isinstance(response.body, bytes):
        body = response.body
        if body and not any(key.lower() == "content-type" for key in headers):
            headers["Content-Type"] = "application/octet-stream"
    else:
        body = json.dumps(
            response.body,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        if not any(key.lower() == "content-type" for key in headers):
            headers["Content-Type"] = "application/json"
    return web.Response(
        body=b"" if request.method == "HEAD" else body,
        status=response.status,
        headers=headers,
    )


def validate_idempotency_key(value: str) -> None:
    if not IDEMPOTENCY_KEY_RE.fullmatch(value):
        raise web.HTTPBadRequest(
            text=(
                "relay request id must be 1-128 characters of letters, digits, "
                "_, ., :, @, / or - and start with a letter or digit"
            )
        )


def _implicit_idempotency_key(
    request: web.Request,
    *,
    rollout_id: str,
    endpoint: str,
    body_bytes: bytes,
) -> str:
    """Fingerprint a disconnected HTTP attempt without coalescing normal calls.

    The state does not publish this fingerprint until its original handler is
    cancelled. Thus two intentional, identical calls remain distinct, while a
    retry after checkpoint-induced TCP loss can reattach.
    """

    authorization = request.headers.get("Authorization", "")
    api_key = request.headers.get("X-Api-Key", "")
    digest = hashlib.sha256(
        b"\0".join(
            (
                rollout_id.encode("utf-8"),
                request.method.encode("ascii"),
                endpoint.encode("utf-8"),
                authorization.encode("utf-8"),
                api_key.encode("utf-8"),
                body_bytes,
            )
        )
    ).hexdigest()
    return f"auto/{digest}"


def _registration_sandbox_id(record: JsonObject) -> str | None:
    metadata = record.get("metadata")
    if not isinstance(metadata, dict):
        return None
    raw = metadata.get("sandbox_id")
    if raw is None:
        return None
    if not isinstance(raw, str) or SANDBOX_ID_RE.fullmatch(raw) is None:
        raise web.HTTPBadRequest(text="sandbox_id registration metadata is invalid")
    return raw


def _validate_registration_metadata(metadata: JsonObject | None) -> None:
    if metadata is None:
        return
    if "sandboxId" in metadata or "sandboxGeneration" in metadata:
        raise web.HTTPBadRequest(text="registration metadata must use snake_case")
    record: JsonObject = {"metadata": metadata}
    sandbox_id = _registration_sandbox_id(record)
    generation = _registration_sandbox_generation(record)
    if (sandbox_id is None) != (generation is None):
        raise web.HTTPBadRequest(
            text="sandbox_id and sandbox_generation must be supplied together"
        )
    lifecycle = metadata.get(AGENT_LIFECYCLE_METADATA_KEY)
    if sandbox_id is not None and lifecycle != MANAGED_AGENT_LIFECYCLE:
        raise web.HTTPBadRequest(
            text=(
                "sandbox-bound rollouts require the managed agent lifecycle; "
                "use the SDK register_agent_rollout() API"
            )
        )
    if sandbox_id is None and lifecycle is not None:
        raise web.HTTPBadRequest(
            text="managed agent lifecycle metadata requires a sandbox binding"
        )


def _registration_sandbox_generation(record: JsonObject) -> int | None:
    metadata = record.get("metadata")
    if not isinstance(metadata, dict):
        return None
    raw = metadata.get("sandbox_generation")
    if raw is None:
        return None
    if isinstance(raw, bool) or not isinstance(raw, int):
        raise web.HTTPBadRequest(
            text="sandbox_generation registration metadata must be an integer"
        )
    if raw < 1:
        raise web.HTTPBadRequest(
            text="sandbox_generation registration metadata must be positive"
        )
    return raw


def _json_mapping(raw: str) -> JsonObject:
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError("persisted relay row must contain a JSON object")
    return value


def _encoded_body(value: object) -> JsonObject:
    if isinstance(value, bytes):
        return {
            "encoding": "base64",
            "value": base64.b64encode(value).decode("ascii"),
        }
    return {
        "encoding": "json",
        "value": json.loads(json.dumps(value, ensure_ascii=False)),
    }


def _decoded_body(value: object) -> object:
    if not isinstance(value, dict) or set(value) != {"encoding", "value"}:
        raise ValueError("persisted relay body is invalid")
    encoding = value.get("encoding")
    if encoding == "json":
        return value["value"]
    if encoding == "base64":
        raw = value.get("value")
        if not isinstance(raw, str):
            raise ValueError("persisted base64 relay body is invalid")
        return base64.b64decode(raw.encode("ascii"), validate=True)
    raise ValueError("persisted relay body has an unknown encoding")


def _encoded_body_bytes(value: JsonObject) -> bytes:
    decoded = _decoded_body(value)
    if isinstance(decoded, bytes):
        return decoded
    return json.dumps(
        decoded,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")


def _persisted_request_payload(request: RelayRequest) -> JsonObject:
    response = request.completed_response
    completed = request.state == "completed"
    return {
        "request_id": request.request_id,
        "rollout_id": request.rollout_id,
        "registration_token": request.registration_token,
        "endpoint": request.endpoint,
        "method": request.method,
        "body": None if completed else request.body,
        "headers": {} if completed else request.headers,
        "created_at": request.created_at,
        "expires_at": request.expires_at,
        "payload_bytes": 0 if completed else request.payload_bytes,
        "delivered_at": request.delivered_at,
        "first_delivered_at": request.first_delivered_at,
        "lease_id": request.lease_id,
        "lease_expires_at": request.lease_expires_at,
        "leased_by": request.leased_by,
        "delivery_count": request.delivery_count,
        "state": request.state,
        "idempotency_key": request.idempotency_key,
        "request_digest": request.request_digest,
        "sandbox_id": request.sandbox_id,
        "sandbox_generation": request.sandbox_generation,
        "completed_at": request.completed_at,
        "completed_bytes": request.completed_bytes,
        "completed_response": (
            None
            if response is None
            else {
                "status": response.status,
                "body": _encoded_body(response.body),
                "headers": response.headers,
            }
        ),
        "wake_notified_at": request.wake_notified_at,
        "accepted_notified_at": request.accepted_notified_at,
        "parked_transport_epoch": request.parked_transport_epoch,
        "reattachable": request.reattachable,
        "delivery_pending": request.delivery_pending,
    }


def _request_from_persisted_payload(
    payload: JsonObject,
    *,
    loop: asyncio.AbstractEventLoop,
) -> RelayRequest:
    raw_response = payload["completed_response"]
    raw_delivery_pending = payload["delivery_pending"]
    if not isinstance(raw_delivery_pending, bool):
        raise ValueError("persisted relay delivery_pending flag is invalid")
    raw_reattachable = payload["reattachable"]
    if not isinstance(raw_reattachable, bool):
        raise ValueError("persisted relay reattachable flag is invalid")
    response = None
    if raw_response is not None:
        if not isinstance(raw_response, dict):
            raise ValueError("persisted relay response is invalid")
        response = RelayWorkerResponse(
            status=int(raw_response["status"]),
            body=_decoded_body(raw_response["body"]),
            headers=_string_mapping(raw_response["headers"]),
        )
    state = payload["state"]
    if state not in {"pending", "leased", "completed"}:
        raise ValueError("persisted relay request state is invalid")
    raw_body = payload["body"]
    if state == "completed":
        if raw_body is not None:
            raise ValueError("completed relay request retained its body")
    elif not isinstance(raw_body, dict):
        raise ValueError("persisted relay request body is invalid")
    else:
        _encoded_body_bytes(raw_body)
    return RelayRequest(
        request_id=str(payload["request_id"]),
        rollout_id=str(payload["rollout_id"]),
        registration_token=str(payload["registration_token"]),
        endpoint=str(payload["endpoint"]),
        method=str(payload["method"]),
        body=raw_body,
        headers=_string_mapping(payload["headers"]),
        created_at=float(payload["created_at"]),
        future=loop.create_future(),
        expires_at=_optional_float(payload["expires_at"]),
        payload_bytes=int(payload["payload_bytes"]),
        delivered_at=_optional_float(payload["delivered_at"]),
        first_delivered_at=_optional_float(payload["first_delivered_at"]),
        lease_id=_optional_string(payload["lease_id"]),
        lease_expires_at=_optional_float(payload["lease_expires_at"]),
        leased_by=_optional_string(payload["leased_by"]),
        delivery_count=int(payload["delivery_count"]),
        state=state,
        idempotency_key=_optional_string(payload["idempotency_key"]),
        request_digest=str(payload["request_digest"]),
        sandbox_id=_optional_string(payload["sandbox_id"]),
        sandbox_generation=(
            int(payload["sandbox_generation"])
            if payload["sandbox_generation"] is not None
            else None
        ),
        completed_at=_optional_float(payload["completed_at"]),
        completed_response=response,
        completed_bytes=int(payload["completed_bytes"]),
        wake_notified_at=_optional_float(payload["wake_notified_at"]),
        accepted_notified_at=_optional_float(payload["accepted_notified_at"]),
        parked_transport_epoch=_optional_string(payload["parked_transport_epoch"]),
        reattachable=raw_reattachable,
        delivery_pending=raw_delivery_pending,
    )


def _optional_float(value: object) -> float | None:
    return None if value is None else float(value)


def _relay_response_retained_bytes(response: RelayWorkerResponse) -> int:
    if isinstance(response.body, bytes):
        body_bytes = len(response.body)
    else:
        body_bytes = len(
            json.dumps(
                response.body,
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode("utf-8")
        )
    header_bytes = len(
        json.dumps(
            response.headers,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
    )
    return body_bytes + header_bytes + 8


def _optional_string(value: object) -> str | None:
    if value is None:
        return None
    result = str(value)
    return result or None


def _set_response(
    future: asyncio.Future[RelayWorkerResponse],
    response: RelayWorkerResponse,
) -> None:
    if not future.done():
        future.set_result(response)


def _average(total: float, count: int) -> float:
    return total / count if count else 0.0
