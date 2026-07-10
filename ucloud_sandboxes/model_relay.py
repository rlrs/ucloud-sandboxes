from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass, field
import json
import re
import time
from typing import Any
from uuid import uuid4

from aiohttp import web

from .deployment import service_health


JsonObject = dict[str, Any]
ROLLOUT_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
WORKER_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@-]{0,127}$")
REGISTRATION_TOKEN_RE = re.compile(r"^[0-9a-f]{32}$")
DEFAULT_RELAY_REQUEST_TIMEOUT_SECONDS = 3600.0
DEFAULT_WORKER_POLL_TIMEOUT_SECONDS = 30.0
DEFAULT_WORKER_LEASE_SECONDS = 600.0
DEFAULT_COMPLETED_REQUEST_RETENTION_SECONDS = 3600.0
DEFAULT_WORKER_RETENTION_SECONDS = 3600.0
DEFAULT_MAX_INFLIGHT_REQUESTS = 4096
DEFAULT_MAX_INFLIGHT_REQUESTS_PER_ROLLOUT = 1024
DEFAULT_MAX_INFLIGHT_BYTES = 128 * 1024**2
DEFAULT_MAX_COMPLETED_REQUESTS = 8192
DEFAULT_MAX_WORKERS = 4096
SANDBOX_TOKEN_KEY = web.AppKey("model_relay_sandbox_token", str | None)
WORKER_TOKEN_KEY = web.AppKey("model_relay_worker_token", str | None)
POLL_TIMEOUT_KEY = web.AppKey("model_relay_poll_timeout", float)
REQUEST_TIMEOUT_KEY = web.AppKey("model_relay_request_timeout", float)
LEASE_SECONDS_KEY = web.AppKey("model_relay_worker_lease_seconds", float)


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
    body: JsonObject
    headers: dict[str, str]
    created_at: float
    future: asyncio.Future[RelayWorkerResponse]
    expires_at: float | None = None
    payload_bytes: int = 0
    delivered_at: float | None = None
    first_delivered_at: float | None = None
    lease_id: str | None = None
    lease_expires_at: float | None = None
    leased_by: str | None = None
    delivery_count: int = 0
    state: str = "pending"

    def envelope(self) -> JsonObject:
        return {
            "request_id": self.request_id,
            "rollout_id": self.rollout_id,
            "registration_token": self.registration_token,
            "endpoint": self.endpoint,
            "method": self.method,
            "headers": dict(self.headers),
            "body": dict(self.body),
            "created_at": self.created_at,
            "expires_at": self.expires_at,
            "delivered_at": self.delivered_at,
            "first_delivered_at": self.first_delivered_at,
            "lease_id": self.lease_id,
            "lease_expires_at": self.lease_expires_at,
            "leased_by": self.leased_by,
            "delivery_count": self.delivery_count,
        }


@dataclass(frozen=True)
class RelayRespondResult:
    request_id: str
    duplicate: bool = False


class ModelRelayState:
    def __init__(
        self,
        *,
        request_timeout_seconds: float = DEFAULT_RELAY_REQUEST_TIMEOUT_SECONDS,
        completed_request_retention_seconds: float = DEFAULT_COMPLETED_REQUEST_RETENTION_SECONDS,
        worker_retention_seconds: float = DEFAULT_WORKER_RETENTION_SECONDS,
        max_inflight_requests: int = DEFAULT_MAX_INFLIGHT_REQUESTS,
        max_inflight_requests_per_rollout: int = DEFAULT_MAX_INFLIGHT_REQUESTS_PER_ROLLOUT,
        max_inflight_bytes: int = DEFAULT_MAX_INFLIGHT_BYTES,
        max_completed_requests: int = DEFAULT_MAX_COMPLETED_REQUESTS,
        max_workers: int = DEFAULT_MAX_WORKERS,
    ) -> None:
        self._condition = asyncio.Condition()
        self._request_timeout_seconds = max(0.001, request_timeout_seconds)
        self._rollouts: dict[str, JsonObject] = {}
        self._pending: dict[str, deque[RelayRequest]] = {}
        self._requests: dict[str, RelayRequest] = {}
        self._completed: dict[str, tuple[float, str]] = {}
        self._workers: dict[tuple[str, str], JsonObject] = {}
        self._inflight_bytes = 0
        self._rollout_inflight_counts: dict[str, int] = {}
        self._max_inflight_requests = max(1, max_inflight_requests)
        self._max_inflight_requests_per_rollout = max(
            1,
            max_inflight_requests_per_rollout,
        )
        self._max_inflight_bytes = max(1, max_inflight_bytes)
        self._max_completed_requests = max(1, max_completed_requests)
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
            "timed_out": 0,
            "lease_expired": 0,
            "lease_renewed": 0,
            "unregister_canceled": 0,
            "polls": 0,
            "empty_polls": 0,
            "admission_rejected": 0,
            "canceled": 0,
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
        async with self._condition:
            now = time.time()
            self._prune_completed_locked(now)
            self._prune_workers_locked(now)
            registration_token = uuid4().hex
            previous = self._rollouts.get(rollout_id)
            if previous is not None:
                self._cancel_rollout_incarnation_locked(
                    rollout_id,
                    str(previous["registration_token"]),
                    message="rollout registration was replaced",
                    error_code="relay_rollout_replaced",
                )
            record = {
                "rollout_id": rollout_id,
                "registration_token": registration_token,
                "metadata": dict(metadata or {}),
                "registered_at": time.time(),
            }
            self._rollouts[rollout_id] = record
            self._pending.setdefault(rollout_id, deque())
            self._condition.notify_all()
            return dict(record)

    async def unregister_rollout(
        self,
        rollout_id: str,
        *,
        registration_token: str,
    ) -> bool:
        validate_rollout_id(rollout_id)
        validate_registration_token(registration_token)
        async with self._condition:
            current = self._rollouts.get(rollout_id)
            if current is None:
                return False
            self._require_current_registration_locked(
                rollout_id,
                registration_token,
            )
            existed = True
            self._rollouts.pop(rollout_id, None)
            self._cancel_rollout_incarnation_locked(
                rollout_id,
                registration_token,
                message="rollout unregistered",
                error_code="relay_rollout_closed",
            )
            self._condition.notify_all()
            return existed

    async def list_rollouts(self) -> list[JsonObject]:
        async with self._condition:
            now = time.time()
            self._prune_completed_locked(now)
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
        async with self._condition:
            self._prune_workers_locked(time.time())
            self._require_current_registration_locked(
                rollout_id,
                registration_token,
            )
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

    async def enqueue(
        self,
        *,
        rollout_id: str,
        endpoint: str,
        body: JsonObject,
        headers: dict[str, str],
    ) -> RelayRequest:
        validate_rollout_id(rollout_id)
        loop = asyncio.get_running_loop()
        async with self._condition:
            now = time.time()
            self._prune_completed_locked(now)
            self._expire_requests_locked(now)
            if rollout_id not in self._rollouts:
                raise web.HTTPNotFound(
                    text=f"rollout is not registered: {rollout_id}"
                )
            payload_bytes = len(
                json.dumps(
                    {"body": body, "headers": headers},
                    ensure_ascii=False,
                    separators=(",", ":"),
                    sort_keys=True,
                ).encode("utf-8")
            )
            rollout_inflight = self._rollout_inflight_counts.get(rollout_id, 0)
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
                registration_token=str(
                    self._rollouts[rollout_id]["registration_token"]
                ),
                endpoint=endpoint,
                method="POST",
                body=dict(body),
                headers=dict(headers),
                created_at=created_at,
                future=loop.create_future(),
                expires_at=created_at + self._request_timeout_seconds,
                payload_bytes=payload_bytes,
            )
            self._pending.setdefault(rollout_id, deque()).append(request)
            self._requests[request.request_id] = request
            self._inflight_bytes += payload_bytes
            self._rollout_inflight_counts[rollout_id] = rollout_inflight + 1
            self._counters["enqueued"] += 1
            self._condition.notify_all()
            return request

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
        async with self._condition:
            self._prune_completed_locked(time.time())
            self._prune_workers_locked(time.time())
            self._require_current_registration_locked(
                rollout_id,
                registration_token,
            )
            if worker_id:
                await self._record_worker_heartbeat_locked(
                    rollout_id=rollout_id,
                    worker_id=worker_id,
                    metadata=None,
                )
            self._counters["polls"] += 1
            while True:
                self._require_current_registration_locked(
                    rollout_id,
                    registration_token,
                )
                now = time.time()
                self._expire_requests_locked(now)
                self._requeue_expired_leases_locked(now)
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
                    if not requests:
                        request = self._lease_request_locked(
                            queue.popleft(),
                            now=now,
                            lease_seconds=lease_seconds,
                            worker_id=worker_id,
                        )
                        requests = [request]
                    return requests
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    self._counters["empty_polls"] += 1
                    return []
                next_expiry = self._next_lease_expiry_locked()
                if next_expiry is not None:
                    remaining = min(remaining, max(0.001, next_expiry - time.time()))
                try:
                    await asyncio.wait_for(self._condition.wait(), remaining)
                except asyncio.TimeoutError:
                    continue

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
        async with self._condition:
            now = time.time()
            self._prune_completed_locked(now)
            self._expire_requests_locked(now)
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
                self._requeue_expired_leases_locked(now)
                raise web.HTTPConflict(text="request lease has expired")
            if worker_id:
                request.leased_by = worker_id
                await self._record_worker_heartbeat_locked(
                    rollout_id=request.rollout_id,
                    worker_id=worker_id,
                    metadata=None,
                )
            request.lease_expires_at = now + lease_seconds
            self._counters["lease_renewed"] += 1
            self._condition.notify_all()
            return request

    async def respond(
        self,
        *,
        request_id: str,
        registration_token: str,
        response: RelayWorkerResponse,
        lease_id: str | None,
        error: bool = False,
    ) -> RelayRespondResult:
        validate_registration_token(registration_token)
        async with self._condition:
            now = time.time()
            self._prune_completed_locked(now)
            self._expire_requests_locked(now)
            if request_id in self._completed:
                self._require_completed_registration_locked(
                    request_id,
                    registration_token,
                )
                self._counters["duplicate_responses"] += 1
                return RelayRespondResult(request_id=request_id, duplicate=True)
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
                self._requeue_expired_leases_locked(now)
                raise web.HTTPConflict(text="request lease has expired")
            self._pop_request_locked(request_id)
            self._remove_pending_locked(request_id, request.rollout_id)
            request.state = "completed"
            self._remember_completed_locked(
                request_id,
                now,
                request.registration_token,
            )
            self._counters["completed"] += 1
            if error or response.status >= 400:
                self._counters["worker_errors"] += 1
            self._timers["request_lifetime_seconds_total"] += now - request.created_at
            if request.delivered_at is not None:
                self._timers["worker_processing_seconds_total"] += (
                    now - request.delivered_at
                )
            _set_response(request.future, response)
            self._condition.notify_all()
            return RelayRespondResult(request_id=request_id)

    async def cancel_request(
        self,
        *,
        request_id: str,
        response: RelayWorkerResponse,
        reason: str = "canceled",
    ) -> RelayWorkerResponse | None:
        async with self._condition:
            request = self._pop_request_locked(request_id)
            if request is None:
                return None
            self._remove_pending_locked(request_id, request.rollout_id)
            request.state = "completed"
            self._remember_completed_locked(
                request_id,
                time.time(),
                request.registration_token,
            )
            if reason == "timeout":
                self._counters["timed_out"] += 1
            else:
                self._counters["canceled"] += 1
            _set_response(request.future, response)
            self._condition.notify_all()
            return response

    async def wait_for_response(
        self,
        request: RelayRequest,
        *,
        timeout_seconds: float,
    ) -> RelayWorkerResponse:
        return await asyncio.wait_for(
            asyncio.shield(request.future),
            timeout=timeout_seconds,
        )

    async def stats(self) -> JsonObject:
        async with self._condition:
            now = time.time()
            self._prune_completed_locked(now)
            self._prune_workers_locked(now)
            self._expire_requests_locked(now)
            self._requeue_expired_leases_locked(now)
            pending = {
                rollout_id: len(queue)
                for rollout_id, queue in sorted(self._pending.items())
            }
            leased_by_rollout: dict[str, int] = {}
            for request in self._requests.values():
                if request.state == "leased":
                    leased_by_rollout[request.rollout_id] = (
                        leased_by_rollout.get(request.rollout_id, 0) + 1
                    )
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
                "workers": [dict(record) for record in self._workers.values()],
                "counters": counters,
                "timers": timers,
                "averages": averages,
                "limits": {
                    "max_inflight_requests": self._max_inflight_requests,
                    "max_inflight_requests_per_rollout": self._max_inflight_requests_per_rollout,
                    "max_inflight_bytes": self._max_inflight_bytes,
                    "max_completed_requests": self._max_completed_requests,
                    "max_workers": self._max_workers,
                },
            }

    def _pop_request_locked(self, request_id: str) -> RelayRequest | None:
        request = self._requests.pop(request_id, None)
        if request is None:
            return None
        self._inflight_bytes = max(0, self._inflight_bytes - request.payload_bytes)
        count = self._rollout_inflight_counts.get(request.rollout_id, 0)
        if count <= 1:
            self._rollout_inflight_counts.pop(request.rollout_id, None)
        else:
            self._rollout_inflight_counts[request.rollout_id] = count - 1
        return request

    def _require_current_registration_locked(
        self,
        rollout_id: str,
        registration_token: str,
    ) -> None:
        current = self._rollouts.get(rollout_id)
        if current is None:
            raise web.HTTPNotFound(
                text=f"rollout is not registered: {rollout_id}"
            )
        if str(current["registration_token"]) != registration_token:
            raise web.HTTPConflict(text="rollout registration is no longer current")

    def _require_request_registration_locked(
        self,
        request: RelayRequest,
        registration_token: str,
    ) -> None:
        if request.registration_token != registration_token:
            raise web.HTTPConflict(text="request belongs to a different rollout registration")
        self._require_current_registration_locked(
            request.rollout_id,
            registration_token,
        )

    def _require_completed_registration_locked(
        self,
        request_id: str,
        registration_token: str,
    ) -> None:
        _completed_at, request_registration_token = self._completed[request_id]
        if request_registration_token != registration_token:
            raise web.HTTPConflict(text="request belongs to a different rollout registration")
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

    def _cancel_rollout_incarnation_locked(
        self,
        rollout_id: str,
        registration_token: str,
        *,
        message: str,
        error_code: str,
    ) -> None:
        response = RelayWorkerResponse(410, _openai_error(message, error_code))
        now = time.time()
        for request_id, request in list(self._requests.items()):
            if (
                request.rollout_id != rollout_id
                or request.registration_token != registration_token
            ):
                continue
            self._pop_request_locked(request_id)
            request.state = "completed"
            self._remember_completed_locked(
                request_id,
                now,
                registration_token,
            )
            self._counters["unregister_canceled"] += 1
            _set_response(request.future, response)
        queue = self._pending.get(rollout_id)
        if queue is not None:
            kept = deque(
                request
                for request in queue
                if request.registration_token != registration_token
            )
            if kept:
                self._pending[rollout_id] = kept
            else:
                self._pending.pop(rollout_id, None)
        for key in list(self._workers):
            if key[0] == rollout_id:
                self._workers.pop(key, None)

    def _remove_pending_locked(self, request_id: str, rollout_id: str) -> None:
        queue = self._pending.get(rollout_id)
        if not queue:
            return
        kept = deque(request for request in queue if request.request_id != request_id)
        self._pending[rollout_id] = kept

    async def _record_worker_heartbeat_locked(
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
        self._counters["delivered"] += 1
        self._timers["queue_wait_seconds_total"] += now - request.created_at
        return request

    def _requeue_expired_leases_locked(self, now: float) -> None:
        expired = [
            request
            for request in self._requests.values()
            if (
                request.state == "leased"
                and request.lease_expires_at is not None
                and request.lease_expires_at <= now
                and not request.future.done()
            )
        ]
        for request in expired:
            request.state = "pending"
            request.lease_id = None
            request.lease_expires_at = None
            request.leased_by = None
            self._pending.setdefault(request.rollout_id, deque()).appendleft(request)
            self._counters["lease_expired"] += 1
        if expired:
            self._condition.notify_all()

    def _expire_requests_locked(self, now: float) -> None:
        expired = [
            request
            for request in self._requests.values()
            if request.expires_at is not None and request.expires_at <= now
        ]
        if not expired:
            return
        response = RelayWorkerResponse(
            504,
            _openai_error("model relay request timed out", "relay_timeout"),
        )
        for request in expired:
            self._pop_request_locked(request.request_id)
            self._remove_pending_locked(request.request_id, request.rollout_id)
            request.state = "completed"
            self._remember_completed_locked(
                request.request_id,
                now,
                request.registration_token,
            )
            self._counters["timed_out"] += 1
            _set_response(request.future, response)
        self._condition.notify_all()

    def _next_lease_expiry_locked(self) -> float | None:
        expiries = [
            request.lease_expires_at
            for request in self._requests.values()
            if request.state == "leased" and request.lease_expires_at is not None
        ]
        return min(expiries) if expiries else None

    def _prune_completed_locked(self, now: float) -> None:
        cutoff = now - self._completed_request_retention_seconds
        for request_id, (completed_at, _registration_token) in list(
            self._completed.items()
        ):
            if completed_at < cutoff:
                self._completed.pop(request_id, None)

    def _remember_completed_locked(
        self,
        request_id: str,
        completed_at: float,
        registration_token: str,
    ) -> None:
        self._prune_completed_locked(completed_at)
        while len(self._completed) >= self._max_completed_requests:
            oldest = min(
                self._completed,
                key=lambda item: (self._completed[item][0], item),
            )
            self._completed.pop(oldest, None)
        self._completed[request_id] = (completed_at, registration_token)

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
    completed_request_retention_seconds: float = DEFAULT_COMPLETED_REQUEST_RETENTION_SECONDS,
    worker_retention_seconds: float = DEFAULT_WORKER_RETENTION_SECONDS,
    max_inflight_requests: int = DEFAULT_MAX_INFLIGHT_REQUESTS,
    max_inflight_requests_per_rollout: int = DEFAULT_MAX_INFLIGHT_REQUESTS_PER_ROLLOUT,
    max_inflight_bytes: int = DEFAULT_MAX_INFLIGHT_BYTES,
    max_completed_requests: int = DEFAULT_MAX_COMPLETED_REQUESTS,
    max_workers: int = DEFAULT_MAX_WORKERS,
    state: ModelRelayState | None = None,
) -> web.Application:
    app = web.Application(client_max_size=32 * 1024**2)
    app[STATE_KEY] = state or ModelRelayState(
        request_timeout_seconds=request_timeout_seconds,
        completed_request_retention_seconds=completed_request_retention_seconds,
        worker_retention_seconds=worker_retention_seconds,
        max_inflight_requests=max_inflight_requests,
        max_inflight_requests_per_rollout=max_inflight_requests_per_rollout,
        max_inflight_bytes=max_inflight_bytes,
        max_completed_requests=max_completed_requests,
        max_workers=max_workers,
    )
    app[SANDBOX_TOKEN_KEY] = sandbox_bearer_token
    app[WORKER_TOKEN_KEY] = worker_bearer_token
    app[POLL_TIMEOUT_KEY] = worker_poll_timeout_seconds
    app[REQUEST_TIMEOUT_KEY] = request_timeout_seconds
    app[LEASE_SECONDS_KEY] = worker_lease_seconds

    app.router.add_get("/healthz", healthz)
    app.router.add_get("/v1/relay/stats", relay_stats)
    app.router.add_get("/v1/relay/rollouts", list_rollouts)
    app.router.add_post("/register_rollout", register_rollout)
    app.router.add_post("/unregister_rollout", unregister_rollout)
    app.router.add_post("/worker/heartbeat", worker_heartbeat)
    app.router.add_get("/worker/poll", worker_poll)
    app.router.add_post("/worker/renew", worker_renew)
    app.router.add_post("/worker/respond", worker_respond)
    app.router.add_post("/worker/error", worker_error)
    app.router.add_post("/v1/chat/completions", openai_chat_completions)
    app.router.add_post("/v1/responses", openai_responses)
    app.router.add_post(
        "/rollouts/{rollout_id}/v1/chat/completions",
        openai_chat_completions,
    )
    app.router.add_post("/rollouts/{rollout_id}/v1/responses", openai_responses)
    return app


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
    rollout_id = str(payload.get("rollout_id") or payload.get("id") or "")
    metadata = payload.get("metadata")
    if metadata is not None and not isinstance(metadata, dict):
        raise web.HTTPBadRequest(text="metadata must be a JSON object")
    record = await _state(request).register_rollout(rollout_id, metadata)
    return web.json_response({"ok": True, "rollout": record}, status=201)


async def unregister_rollout(request: web.Request) -> web.Response:
    _require_worker_token(request)
    payload = await _json_object(request)
    rollout_id = str(payload.get("rollout_id") or payload.get("id") or "")
    registration_token = _registration_token_from_payload(payload)
    existed = await _state(request).unregister_rollout(
        rollout_id,
        registration_token=registration_token,
    )
    return web.json_response({"ok": True, "rollout_id": rollout_id, "existed": existed})


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
    return web.json_response(
        {
            "request": envelopes[0] if envelopes else None,
            "requests": envelopes,
        }
    )


async def worker_heartbeat(request: web.Request) -> web.Response:
    _require_worker_token(request)
    payload = await _json_object(request)
    rollout_id = str(payload.get("rollout_id") or payload.get("id") or "")
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
    body = payload.get("response", payload.get("body", {}))
    status = _status_code(payload.get("status"), default=200)
    headers = _string_mapping(payload.get("headers"))
    result = await _state(request).respond(
        request_id=request_id,
        registration_token=registration_token,
        lease_id=lease_id,
        response=RelayWorkerResponse(status=status, body=body, headers=headers),
    )
    return web.json_response(
        {
            "ok": True,
            "request_id": result.request_id,
            "duplicate": result.duplicate,
        }
    )


async def worker_error(request: web.Request) -> web.Response:
    _require_worker_token(request)
    payload = await _json_object(request)
    request_id = str(payload.get("request_id") or "")
    registration_token = _registration_token_from_payload(payload)
    if not request_id:
        raise web.HTTPBadRequest(text="request_id is required")
    lease_id = str(payload.get("lease_id") or "")
    status = _status_code(payload.get("status"), default=502)
    message = str(payload.get("error") or payload.get("message") or "worker error")
    result = await _state(request).respond(
        request_id=request_id,
        registration_token=registration_token,
        lease_id=lease_id,
        response=RelayWorkerResponse(
            status=status,
            body=_openai_error(message, "relay_worker_error"),
        ),
        error=True,
    )
    return web.json_response(
        {
            "ok": True,
            "request_id": result.request_id,
            "duplicate": result.duplicate,
        }
    )


async def openai_chat_completions(request: web.Request) -> web.Response:
    return await _openai_proxy(request, endpoint="/v1/chat/completions")


async def openai_responses(request: web.Request) -> web.Response:
    return await _openai_proxy(request, endpoint="/v1/responses")


async def _openai_proxy(request: web.Request, *, endpoint: str) -> web.Response:
    _require_sandbox_token(request)
    payload = await _json_object(request)
    if payload.get("stream"):
        return web.json_response(
            _openai_error("streaming model relay is not implemented yet", "relay_streaming_unsupported"),
            status=400,
        )
    rollout_id = _rollout_id_from_request(request)
    relay_request = await _state(request).enqueue(
        rollout_id=rollout_id,
        endpoint=endpoint,
        body=payload,
        headers=_forward_headers(request),
    )
    try:
        response = await _state(request).wait_for_response(
            relay_request,
            timeout_seconds=request.app[REQUEST_TIMEOUT_KEY],
        )
    except asyncio.TimeoutError:
        timeout_response = RelayWorkerResponse(
            504,
            _openai_error("model relay request timed out", "relay_timeout"),
        )
        persisted = await _state(request).cancel_request(
            request_id=relay_request.request_id,
            response=timeout_response,
            reason="timeout",
        )
        response = persisted or timeout_response
    except asyncio.CancelledError:
        cancel_response = RelayWorkerResponse(
            499,
            _openai_error("model relay request was canceled", "relay_canceled"),
        )
        await _state(request).cancel_request(
            request_id=relay_request.request_id,
            response=cancel_response,
            reason="client_canceled",
        )
        raise
    return web.json_response(
        response.body,
        status=response.status,
        headers=_safe_response_headers(response.headers),
    )


def _state(request: web.Request) -> ModelRelayState:
    return request.app[STATE_KEY]


async def _json_object(request: web.Request) -> JsonObject:
    try:
        payload = await request.json()
    except Exception as exc:  # aiohttp raises different JSON errors by version.
        raise web.HTTPBadRequest(text=f"invalid JSON body: {exc}") from exc
    if not isinstance(payload, dict):
        raise web.HTTPBadRequest(text="request body must be a JSON object")
    return payload


def _rollout_id_from_request(request: web.Request) -> str:
    path_rollout_id = request.match_info.get("rollout_id")
    if path_rollout_id:
        rollout_id = path_rollout_id
    else:
        rollout_id = (
            request.headers.get("X-UCloud-Rollout-Id")
            or request.headers.get("X-Relay-Rollout-Id")
            or request.headers.get("X-Rollout-Id")
            or request.query.get("rollout_id")
            or ""
        )
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
    registration_token = str(
        request.headers.get("X-Relay-Registration-Token")
        or request.query.get("registration_token")
        or ""
    )
    validate_registration_token(registration_token)
    return registration_token


def _worker_id_from_request(request: web.Request) -> str | None:
    raw = (
        request.headers.get("X-Relay-Worker-Id")
        or request.headers.get("X-Worker-Id")
        or request.query.get("worker_id")
        or None
    )
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
        "authorization",
        "connection",
        "content-length",
        "content-type",
        "host",
        "proxy-authorization",
        "transfer-encoding",
        "x-ucloud-sandbox-token",
    }
    return {
        key: value
        for key, value in request.headers.items()
        if key.lower() not in blocked
    }


def _safe_response_headers(headers: dict[str, str]) -> dict[str, str]:
    blocked = {
        "connection",
        "content-length",
        "content-type",
        "proxy-authenticate",
        "proxy-authorization",
        "transfer-encoding",
    }
    return {
        key: value
        for key, value in headers.items()
        if key.lower() not in blocked
    }


def _openai_error(message: str, error_type: str) -> JsonObject:
    return {"error": {"message": message, "type": error_type}}


def _set_response(
    future: asyncio.Future[RelayWorkerResponse],
    response: RelayWorkerResponse,
) -> None:
    if not future.done():
        future.set_result(response)


def _average(total: float, count: int) -> float:
    return total / count if count else 0.0
