from __future__ import annotations

import asyncio
import base64
from contextlib import suppress
from dataclasses import dataclass, field
import hashlib
import json
import logging
from pathlib import Path
import re
from typing import Any, Awaitable, Callable, TYPE_CHECKING, TypeVar

from aiohttp import web
from opentelemetry.trace import SpanKind

from .deployment import service_health
from .shared_control.model import DatabaseAdmissionUnavailable
from .telemetry import Telemetry, trace_id_hex


if TYPE_CHECKING:
    from .shared_control.relay import PostgresRelayState


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
RELAY_POSTGRES_REQUIRED = (
    "Model relay 0.5.114 requires PostgreSQL; configure relay_postgres. "
    "For an existing SQLite journal, stop the relay and run "
    "python -m ucloud_sandboxes.shared_control import-idle-relay --sqlite-file PATH "
    "with the target --dsn-file and --deployment-id before restarting. "
    "See docs/postgres-relay.md; do not delete or reset the old journal."
)


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


@dataclass
class RelayWorkerResponse:
    status: int
    body: object
    headers: dict[str, str] = field(default_factory=dict)


class RelayLifecycleDeferred(Exception):
    """An identified retry releases its durable claim until the given delay."""

    def __init__(self, seconds: float, *, transport_epoch: str | None = None):
        super().__init__("lifecycle deferred")
        self.seconds = max(0.05, min(30.0, seconds))
        self.transport_epoch = transport_epoch


class RelayCallerUnavailable(Exception):
    """The gateway definitively rejected this sandbox incarnation's wake."""

    def __init__(self, status: int) -> None:
        self.status = status
        super().__init__(f"sandbox lifecycle is unavailable (HTTP {status})")


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
    response_committed: asyncio.Event = field(
        default_factory=asyncio.Event,
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
    durable_lifecycle: bool = True
    # Refreshed from the current registration when dispatching a park; never
    # persisted in a request or interpreted as execution authority.
    resource_phase: JsonObject | None = field(default=None, repr=False, compare=False)

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


STATE_KEY = web.AppKey("model_relay_state", object)


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
    state_path: Path | None = None,
    postgres_store=None,
    postgres_storage_budget_bytes: int = 64 * 1024**3,
    accepted_notifier: Callable[[RelayRequest], Awaitable[str | None]] | None = None,
    result_notifier: Callable[[RelayRequest], Awaitable[str | None]] | None = None,
    unavailable_callers: Callable[
        [set[tuple[str, int]]], Awaitable[dict[tuple[str, int], str]]
    ]
    | None = None,
    telemetry: Telemetry | None = None,
) -> web.Application:
    # Base64 expands worker response bodies by 4/3 inside the JSON control API.
    resolved_telemetry = telemetry or Telemetry.disabled("model-relay")
    app = web.Application(
        client_max_size=48 * 1024**2,
        # aiohttp otherwise retains idle HTTP connections for about an hour.
        # Repeated park/resume cycles must release ingress connection capacity;
        # this timeout only applies between requests, never to active model calls.
        handler_args={"keepalive_timeout": 5.0},
        middlewares=([_telemetry_middleware] if resolved_telemetry.enabled else [])
        + [_database_admission_middleware],
    )
    app[TELEMETRY_KEY] = resolved_telemetry
    if postgres_store is None or state_path is not None:
        raise ValueError(RELAY_POSTGRES_REQUIRED)
    from .shared_control.relay import PostgresRelayState

    app[STATE_KEY] = PostgresRelayState(
        postgres_store,
        request_timeout_seconds=request_timeout_seconds,
        completed_request_retention_seconds=completed_request_retention_seconds,
        worker_retention_seconds=worker_retention_seconds,
        storage_budget_bytes=postgres_storage_budget_bytes,
        accepted_notifier=accepted_notifier,
        result_notifier=result_notifier,
    )
    app[SANDBOX_TOKEN_KEY] = sandbox_bearer_token
    app[WORKER_TOKEN_KEY] = worker_bearer_token
    app[POLL_TIMEOUT_KEY] = worker_poll_timeout_seconds
    app[REQUEST_TIMEOUT_KEY] = request_timeout_seconds
    app[LEASE_SECONDS_KEY] = worker_lease_seconds
    app[ACCEPTED_NOTIFIER_KEY] = accepted_notifier
    app[RESULT_NOTIFIER_KEY] = result_notifier

    async def maintain_state(_app: web.Application):
        await _app[STATE_KEY].open()
        interval = max(0.01, maintenance_interval_seconds)
        task = asyncio.create_task(
            _model_relay_maintenance_loop(
                _app[STATE_KEY], interval, unavailable_callers
            )
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
    app.router.add_post(
        "/v1/relay/rollouts/{rollout_id}/resource-phase", update_resource_phase
    )
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
    state: PostgresRelayState,
    interval_seconds: float,
    unavailable_callers: Callable[
        [set[tuple[str, int]]], Awaitable[dict[tuple[str, int], str]]
    ]
    | None = None,
) -> None:
    while True:
        try:
            await state.maintain()
            if unavailable_callers is not None:
                candidates = await state.pending_caller_incarnations()
                if candidates:
                    await state.reconcile_unavailable_callers(
                        await unavailable_callers(candidates)
                    )
        except asyncio.CancelledError:
            raise
        except Exception:
            LOGGER.exception("model relay background maintenance failed")
        await asyncio.sleep(interval_seconds)


@web.middleware
async def _database_admission_middleware(request, handler):
    try:
        return await handler(request)
    except DatabaseAdmissionUnavailable:
        # These control operations either have not entered their transaction
        # or retry an identical fenced response. Renewal and unregister each
        # use one transaction, so admission failure cannot have changed a lease
        # or retired a registration. A model/tunnel HTTP request can already
        # have enqueued work before a later transaction fails; admission of that
        # later transaction is not proof that replaying the HTTP call is safe.
        if request.match_info.handler not in {
            worker_poll, worker_respond, worker_renew, unregister_rollout,
        }:
            raise
        return web.json_response(
            {"error": "relay database admission is temporarily unavailable",
             "error_code": "relay_database_busy", "retryable": True},
            status=503,
            headers={"Retry-After": "1", "X-UCloud-Retryable": "true"},
        )


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


async def update_resource_phase(request: web.Request) -> web.Response:
    _require_worker_token(request)
    payload = await _json_object(request)
    token = _registration_token_from_payload(payload)
    if set(payload) != {"registration_token", "update"}:
        raise web.HTTPBadRequest(
            text="resource phase requires registration_token and update"
        )
    state = _state(request)
    result = await state.update_resource_phase(
        str(request.match_info["rollout_id"]),
        registration_token=token,
        update=payload["update"],
    )
    return web.json_response({"ok": True, **result})


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
    # Result and delivery obligation already committed atomically. The durable
    # dispatcher owns wake retries independently of this inference-worker socket.
    return web.json_response(
        {
            "ok": True,
            "request_id": result.request.request_id,
            "duplicate": result.duplicate,
            "committed": True,
            "delivery_status": "pending"
            if result.request.delivery_pending
            else "released",
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
        expected_registration_token=registration_token,
    )
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


def _state(request: web.Request) -> PostgresRelayState:
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
    from .relay_phase import METADATA_KEY as phase_metadata_key

    if phase_metadata_key in metadata:
        raise web.HTTPBadRequest(
            text="resource phase metadata is reserved; use its update API"
        )
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
