"""One server-owned execution budget shared by every stage of an image build."""

from __future__ import annotations

import math
import time
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Iterator


DEFAULT_BUILD_EXECUTION_TIMEOUT_SECONDS = 1800.0
_EXECUTION_DEADLINE: ContextVar[float | None] = ContextVar(
    "image_build_execution_deadline", default=None
)


class ImageBuildTimeoutError(RuntimeError):
    """The independent server execution budget expired, not a client wait."""


def _positive_seconds(value: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("build execution timeout must be finite and positive")
    if not math.isfinite(value) or value <= 0:
        raise ValueError("build execution timeout must be finite and positive")
    return float(value)


@contextmanager
def build_execution_deadline(timeout_seconds: float) -> Iterator[None]:
    """Start a budget; nested work cannot extend its parent's deadline.

    The deadline is local to this execution context. Blocking operations must
    consume the remaining budget rather than starting a fresh per-step timeout.
    It does not cancel work merely because a waiting HTTP client disconnects.
    """
    deadline = time.monotonic() + _positive_seconds(timeout_seconds)
    previous = _EXECUTION_DEADLINE.get()
    token = _EXECUTION_DEADLINE.set(
        deadline if previous is None else min(previous, deadline)
    )
    try:
        yield
    finally:
        _EXECUTION_DEADLINE.reset(token)


@contextmanager
def without_build_execution_deadline() -> Iterator[None]:
    """Allow finalizers to use their own finite cleanup timeouts after expiry."""
    token = _EXECUTION_DEADLINE.set(None)
    try:
        yield
    finally:
        _EXECUTION_DEADLINE.reset(token)


def remaining_build_execution_seconds(limit: float | None = None) -> float | None:
    """Cap an operation timeout by the build budget, raising once it expires."""
    maximum = None if limit is None else _positive_seconds(limit)
    deadline = _EXECUTION_DEADLINE.get()
    if deadline is None:
        return maximum
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise ImageBuildTimeoutError("image build exceeded its server execution deadline")
    return remaining if maximum is None else min(remaining, maximum)
