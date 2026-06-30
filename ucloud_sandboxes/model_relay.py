from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass, field
import re
import time
from typing import Any
from uuid import uuid4

from aiohttp import web


JsonObject = dict[str, Any]
ROLLOUT_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
DEFAULT_RELAY_REQUEST_TIMEOUT_SECONDS = 300.0
DEFAULT_WORKER_POLL_TIMEOUT_SECONDS = 30.0
SANDBOX_TOKEN_KEY = web.AppKey("model_relay_sandbox_token", str | None)
WORKER_TOKEN_KEY = web.AppKey("model_relay_worker_token", str | None)
POLL_TIMEOUT_KEY = web.AppKey("model_relay_poll_timeout", float)
REQUEST_TIMEOUT_KEY = web.AppKey("model_relay_request_timeout", float)


@dataclass
class RelayWorkerResponse:
    status: int
    body: object
    headers: dict[str, str] = field(default_factory=dict)


@dataclass
class RelayRequest:
    request_id: str
    rollout_id: str
    endpoint: str
    method: str
    body: JsonObject
    headers: dict[str, str]
    created_at: float
    future: asyncio.Future[RelayWorkerResponse]
    delivered_at: float | None = None

    def envelope(self) -> JsonObject:
        return {
            "request_id": self.request_id,
            "rollout_id": self.rollout_id,
            "endpoint": self.endpoint,
            "method": self.method,
            "headers": dict(self.headers),
            "body": dict(self.body),
            "created_at": self.created_at,
            "delivered_at": self.delivered_at,
        }


class ModelRelayState:
    def __init__(self) -> None:
        self._condition = asyncio.Condition()
        self._rollouts: dict[str, JsonObject] = {}
        self._pending: dict[str, deque[RelayRequest]] = {}
        self._requests: dict[str, RelayRequest] = {}

    async def register_rollout(
        self,
        rollout_id: str,
        metadata: JsonObject | None = None,
    ) -> JsonObject:
        validate_rollout_id(rollout_id)
        async with self._condition:
            record = {
                "rollout_id": rollout_id,
                "metadata": dict(metadata or {}),
                "registered_at": time.time(),
            }
            self._rollouts[rollout_id] = record
            self._pending.setdefault(rollout_id, deque())
            self._condition.notify_all()
            return dict(record)

    async def unregister_rollout(self, rollout_id: str) -> bool:
        validate_rollout_id(rollout_id)
        async with self._condition:
            existed = rollout_id in self._rollouts
            self._rollouts.pop(rollout_id, None)
            pending = self._pending.pop(rollout_id, deque())
            for request in list(pending):
                self._requests.pop(request.request_id, None)
                _set_response(
                    request.future,
                    RelayWorkerResponse(
                        410,
                        _openai_error("rollout unregistered", "relay_rollout_closed"),
                    ),
                )
            for request_id, request in list(self._requests.items()):
                if request.rollout_id == rollout_id:
                    self._requests.pop(request_id, None)
                    _set_response(
                        request.future,
                        RelayWorkerResponse(
                            410,
                            _openai_error("rollout unregistered", "relay_rollout_closed"),
                        ),
                    )
            self._condition.notify_all()
            return existed

    async def list_rollouts(self) -> list[JsonObject]:
        async with self._condition:
            return [dict(record) for record in self._rollouts.values()]

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
            if rollout_id not in self._rollouts:
                raise web.HTTPNotFound(
                    text=f"rollout is not registered: {rollout_id}"
                )
            request = RelayRequest(
                request_id=uuid4().hex,
                rollout_id=rollout_id,
                endpoint=endpoint,
                method="POST",
                body=dict(body),
                headers=dict(headers),
                created_at=time.time(),
                future=loop.create_future(),
            )
            self._pending.setdefault(rollout_id, deque()).append(request)
            self._requests[request.request_id] = request
            self._condition.notify_all()
            return request

    async def poll(
        self,
        *,
        rollout_id: str,
        timeout_seconds: float,
    ) -> RelayRequest | None:
        validate_rollout_id(rollout_id)
        deadline = time.monotonic() + max(0.0, timeout_seconds)
        async with self._condition:
            if rollout_id not in self._rollouts:
                raise web.HTTPNotFound(
                    text=f"rollout is not registered: {rollout_id}"
                )
            while True:
                queue = self._pending.setdefault(rollout_id, deque())
                if queue:
                    request = queue.popleft()
                    request.delivered_at = time.time()
                    return request
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return None
                try:
                    await asyncio.wait_for(self._condition.wait(), remaining)
                except asyncio.TimeoutError:
                    return None

    async def respond(
        self,
        *,
        request_id: str,
        response: RelayWorkerResponse,
    ) -> None:
        async with self._condition:
            request = self._requests.pop(request_id, None)
            if request is None:
                raise web.HTTPNotFound(text=f"request not found: {request_id}")
            self._remove_pending_locked(request_id, request.rollout_id)
            _set_response(request.future, response)
            self._condition.notify_all()

    async def cancel_request(
        self,
        *,
        request_id: str,
        response: RelayWorkerResponse,
    ) -> None:
        async with self._condition:
            request = self._requests.pop(request_id, None)
            if request is None:
                return
            self._remove_pending_locked(request_id, request.rollout_id)
            _set_response(request.future, response)
            self._condition.notify_all()

    async def stats(self) -> JsonObject:
        async with self._condition:
            pending = {
                rollout_id: len(queue)
                for rollout_id, queue in sorted(self._pending.items())
            }
            return {
                "rollouts": len(self._rollouts),
                "pending": pending,
                "inflight": len(self._requests),
            }

    def _remove_pending_locked(self, request_id: str, rollout_id: str) -> None:
        queue = self._pending.get(rollout_id)
        if not queue:
            return
        kept = deque(request for request in queue if request.request_id != request_id)
        self._pending[rollout_id] = kept


STATE_KEY = web.AppKey("model_relay_state", ModelRelayState)


def create_model_relay_app(
    *,
    sandbox_bearer_token: str | None = None,
    worker_bearer_token: str | None = None,
    request_timeout_seconds: float = DEFAULT_RELAY_REQUEST_TIMEOUT_SECONDS,
    worker_poll_timeout_seconds: float = DEFAULT_WORKER_POLL_TIMEOUT_SECONDS,
    state: ModelRelayState | None = None,
) -> web.Application:
    app = web.Application(client_max_size=32 * 1024**2)
    app[STATE_KEY] = state or ModelRelayState()
    app[SANDBOX_TOKEN_KEY] = sandbox_bearer_token
    app[WORKER_TOKEN_KEY] = worker_bearer_token
    app[POLL_TIMEOUT_KEY] = worker_poll_timeout_seconds
    app[REQUEST_TIMEOUT_KEY] = request_timeout_seconds

    app.router.add_get("/healthz", healthz)
    app.router.add_get("/v1/relay/stats", relay_stats)
    app.router.add_get("/v1/relay/rollouts", list_rollouts)
    app.router.add_post("/register_rollout", register_rollout)
    app.router.add_post("/unregister_rollout", unregister_rollout)
    app.router.add_get("/worker/poll", worker_poll)
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
    return web.json_response({"ok": True})


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
    existed = await _state(request).unregister_rollout(rollout_id)
    return web.json_response({"ok": True, "rollout_id": rollout_id, "existed": existed})


async def worker_poll(request: web.Request) -> web.Response:
    _require_worker_token(request)
    rollout_id = str(request.query.get("rollout_id") or "")
    timeout_seconds = _float_query(
        request,
        "timeout_seconds",
        default=request.app[POLL_TIMEOUT_KEY],
    )
    relay_request = await _state(request).poll(
        rollout_id=rollout_id,
        timeout_seconds=timeout_seconds,
    )
    if relay_request is None:
        return web.json_response({"request": None})
    return web.json_response({"request": relay_request.envelope()})


async def worker_respond(request: web.Request) -> web.Response:
    _require_worker_token(request)
    payload = await _json_object(request)
    request_id = str(payload.get("request_id") or "")
    if not request_id:
        raise web.HTTPBadRequest(text="request_id is required")
    body = payload.get("response", payload.get("body", {}))
    status = _status_code(payload.get("status"), default=200)
    headers = _string_mapping(payload.get("headers"))
    await _state(request).respond(
        request_id=request_id,
        response=RelayWorkerResponse(status=status, body=body, headers=headers),
    )
    return web.json_response({"ok": True, "request_id": request_id})


async def worker_error(request: web.Request) -> web.Response:
    _require_worker_token(request)
    payload = await _json_object(request)
    request_id = str(payload.get("request_id") or "")
    if not request_id:
        raise web.HTTPBadRequest(text="request_id is required")
    status = _status_code(payload.get("status"), default=502)
    message = str(payload.get("error") or payload.get("message") or "worker error")
    await _state(request).respond(
        request_id=request_id,
        response=RelayWorkerResponse(
            status=status,
            body=_openai_error(message, "relay_worker_error"),
        ),
    )
    return web.json_response({"ok": True, "request_id": request_id})


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
        response = await asyncio.wait_for(
            asyncio.shield(relay_request.future),
            timeout=request.app[REQUEST_TIMEOUT_KEY],
        )
    except asyncio.TimeoutError:
        timeout_response = RelayWorkerResponse(
            504,
            _openai_error("model relay request timed out", "relay_timeout"),
        )
        await _state(request).cancel_request(
            request_id=relay_request.request_id,
            response=timeout_response,
        )
        response = timeout_response
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
        "transfer-encoding",
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
