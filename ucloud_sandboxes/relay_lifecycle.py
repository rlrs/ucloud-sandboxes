"""Canonical relay-to-gateway lifecycle delivery transport.

The relay owns durable obligations; gateway and workers own fenced lifecycle
execution and admission. This adapter encodes one async HTTP path, its bounded
responses, cancellation behavior and explicitly safe retry classifications.
"""

from __future__ import annotations

import asyncio
import io
import json
import random
import time
from typing import Any
from urllib.error import HTTPError
from urllib.parse import quote, urlparse

from opentelemetry.propagate import inject
from opentelemetry.trace import get_current_span

from .model_relay import (
    RelayCallerUnavailable,
    RelayRequest,
    _finish_before_cancellation,
)

_MAX_CONTROL_RESPONSE_BYTES = 1024 * 1024


class RelayLifecycleDispatcher:
    """Dispatch lifecycle HTTP asynchronously; workers own resource admission."""

    def __init__(self, gateway_url: str, bearer_token: str) -> None:
        self.gateway_url = gateway_url
        self.bearer_token = bearer_token
        self._closed = False
        self._active: set[asyncio.Task[str | None]] = set()
        self._wake_session = None

    async def notify(self, request: RelayRequest, *, action: str) -> str | None:
        if action not in {"park", "wake"}:
            raise ValueError("unsupported relay sandbox lifecycle action")
        if self._closed:
            raise RuntimeError("relay lifecycle dispatcher is closed")
        # Keep an accepted operation alive across caller cancellation, including
        # async backoff. HTTP attempts do not reserve executor threads.
        task = asyncio.create_task(self._notify(request, action=action))
        self._active.add(task)
        try:
            return await _finish_before_cancellation(task)
        finally:
            self._active.discard(task)

    async def _notify(self, request: RelayRequest, *, action: str) -> str | None:
        deadline = _relay_lifecycle_deadline(request)
        if self._closed:
            raise RuntimeError("relay lifecycle dispatcher is closed")
        if action == "park" and getattr(request, "completed_at", None) is not None:
            get_current_span().add_event(
                "relay.park.skipped", {"reason": "response_committed"}
            )
            return None
        if deadline <= time.monotonic():
            raise TimeoutError(f"relay {action} deadline exceeded")
        from aiohttp import ClientSession, TCPConnector

        if self._wake_session is None:
            # HTTP waits own no executor slots. Workers defer warm retention
            # and own checkpoint/restore admission against local pressure.
            self._wake_session = ClientSession(
                connector=TCPConnector(limit=0), trust_env=False
            )
        try:
            return await _post_lifecycle_attempt(
                self._wake_session,
                self.gateway_url,
                self.bearer_token,
                request,
                action=action,
                attempt=0,
                deadline=deadline,
            )
        except _RelayLifecycleRetry as retry:
            # PostgreSQL owns retry time and releases the durable claim;
            # transport never creates a second sleeping retry controller.
            from .model_relay import RelayLifecycleDeferred

            raise RelayLifecycleDeferred(
                retry.delay_seconds, transport_epoch=retry.transport_epoch
            ) from retry

    async def close(self) -> None:
        self._closed = True
        # Finish already-dispatched operations before closing their transport.
        await asyncio.gather(*tuple(self._active), return_exceptions=True)
        if self._wake_session is not None:
            await self._wake_session.close()


class _RelayLifecycleRetry(Exception):
    """An identified, side-effect-safe retry; the response socket is closed."""

    def __init__(
        self, delay_seconds: float, *, transport_epoch: str | None = None
    ) -> None:
        super().__init__("relay lifecycle retry pending")
        self.delay_seconds = delay_seconds
        self.transport_epoch = transport_epoch


def _relay_lifecycle_deadline(relay_request: RelayRequest) -> float:
    budget = 600.0
    expires_at = getattr(relay_request, "expires_at", None)
    if expires_at is not None:
        budget = max(0.0, min(budget, expires_at - time.time()))
    return time.monotonic() + budget


async def _post_lifecycle_attempt(
    session: Any,
    gateway_url: str,
    bearer_token: str | None,
    relay_request: RelayRequest,
    *,
    action: str,
    attempt: int,
    deadline: float,
) -> str | None:
    """One bounded HTTP attempt, without reserving a blocking worker thread."""
    from aiohttp import ClientTimeout

    if action not in {"wake", "park"}:
        raise ValueError("unsupported relay sandbox lifecycle action")
    if relay_request.sandbox_id is None:
        return None
    if relay_request.sandbox_generation is None:
        raise ValueError("relay sandbox lifecycle binding has no generation")
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("relay wake deadline exceeded")
    base = str(gateway_url).strip().rstrip("/")
    parsed = urlparse(base)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("gateway URL is invalid")
    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    if bearer_token is not None:
        if not bearer_token.strip():
            raise ValueError("gateway bearer token cannot be empty")
        headers["Authorization"] = "Bearer " + bearer_token.strip()
    inject(headers)
    payload = {
        "generation": relay_request.sandbox_generation,
        "operation_id": f"relay-{action}:{relay_request.request_id}",
        "rollout_id": relay_request.rollout_id,
        "request_id": relay_request.request_id,
        "request_created_at": relay_request.created_at,
        "durable_lifecycle": True,
    }
    phase = getattr(relay_request, "resource_phase", None)
    if action == "park" and phase is not None:
        payload["resource_phase"] = phase
    url = f"{base}/v1/sandboxes/{quote(relay_request.sandbox_id, safe='')}/{action}"
    async with session.post(
        url,
        json=payload,
        headers=headers,
        allow_redirects=False,
        timeout=ClientTimeout(total=remaining),
    ) as response:
        body = bytearray()
        async for chunk in response.content.iter_chunked(65536):
            body.extend(chunk)
            if len(body) > _MAX_CONTROL_RESPONSE_BYTES:
                # An oversized failure must not become a retryable JSON error.
                break
        if not 200 <= response.status < 300:
            _raise_relay_lifecycle_http_error(
                HTTPError(
                    url,
                    response.status,
                    response.reason,
                    response.headers,
                    io.BytesIO(body),
                ),
                action=action,
                attempt=attempt,
                deadline=deadline,
            )
        if len(body) > _MAX_CONTROL_RESPONSE_BYTES:
            raise ValueError("gateway lifecycle response exceeds 1 MiB")
        decoded = json.loads(body) if body else {}
        if not isinstance(decoded, dict):
            raise ValueError("gateway lifecycle response must be a JSON object")
        return (
            response.headers.get("X-UCloud-Sandbox-Transport-Epoch", "").strip() or None
        )


def _raise_relay_lifecycle_http_error(
    exc: HTTPError,
    *,
    action: str,
    attempt: int,
    deadline: float,
) -> None:
    # HTTPError owns the response socket even though open() raised.
    # Close every failure, including exhausted retries and 5xx errors.
    try:
        body = exc.read(_MAX_CONTROL_RESPONSE_BYTES + 1)
        failure = (
            json.loads(body)
            if body and len(body) <= _MAX_CONTROL_RESPONSE_BYTES
            else {}
        )
    except (ValueError, OSError):
        failure = {}
    finally:
        exc.close()
    if (
        action == "park"
        and exc.code == 409
        and isinstance(failure, dict)
        and failure.get("error_code") == "park_deferred"
        and failure.get("retryable") is True
    ):
        try:
            delay = float(failure["retry_after_seconds"])
        except (KeyError, TypeError, ValueError):
            delay = 1.0
        epoch = exc.headers.get("X-UCloud-Sandbox-Transport-Epoch", "").strip() or None
        raise _RelayLifecycleRetry(
            max(0.05, min(30.0, delay)), transport_epoch=epoch
        ) from exc
    permanent = exc.code in {404, 410} or (
        exc.code == 409
        and isinstance(failure, dict)
        and failure.get("retryable") is False
    )
    if action == "wake" and permanent:
        raise RelayCallerUnavailable(exc.code) from exc
    # Only retry positively identified admission failures here. An
    # unclassified 5xx still reaches the worker's existing retry path.
    capacity_pending = (
        action == "wake"
        and exc.code in {429, 503}
        and isinstance(failure, dict)
        and failure.get("retryable") is True
    )
    if isinstance(failure, dict) and failure.get("error_code"):
        exc.msg = f"{exc.msg} ({str(failure['error_code'])[:160]})"
    if capacity_pending:
        try:
            retry_after = float(exc.headers.get("Retry-After", "1"))
        except (TypeError, ValueError):
            retry_after = 1.0
        delay = max(1.0, min(5.0, retry_after)) + random.uniform(0, 0.25)
        if attempt >= 600 or time.monotonic() + delay >= deadline:
            raise exc
        get_current_span().add_event(
            "relay.wake.capacity_retry",
            {
                "gateway.lifecycle.status_code": exc.code,
                "gateway.lifecycle.error_code": str(failure.get("error_code", "")),
                "retry.attempt": attempt + 1,
                "retry.delay_seconds": delay,
            },
        )
        raise _RelayLifecycleRetry(delay) from exc
    # Another lifecycle request can win the fence between enqueue and
    # this explicit park, and a concurrent status/log read can briefly
    # hold the same activity fence. The bounded idempotent retry
    # observes the stable result without giving transient reads a
    # separate failure policy.
    if (
        permanent
        or exc.code != 409
        or attempt >= 100
        or (action == "wake" and time.monotonic() + 0.05 >= deadline)
    ):
        raise exc
    raise _RelayLifecycleRetry(0.05) from exc
