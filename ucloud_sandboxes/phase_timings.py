"""Request-scoped phase timings without passing a timer through every layer.

A caller opens ``recording()`` around one synchronous request. Code below it
marks phases with ``phase(name)``; outside a recording, ``phase`` is a no-op.
Repeated phases accumulate, and phases may nest, so values are not additive.
Work handed to other threads is not attributed unless it records its own.
"""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
import time
from typing import Iterator

_ACTIVE: ContextVar[dict[str, int] | None] = ContextVar(
    "ucloud_phase_timings", default=None
)


@contextmanager
def recording() -> Iterator[dict[str, int]]:
    phases: dict[str, int] = {}
    token = _ACTIVE.set(phases)
    try:
        yield phases
    finally:
        _ACTIVE.reset(token)


@contextmanager
def phase(name: str) -> Iterator[None]:
    phases = _ACTIVE.get()
    if phases is None:
        yield
        return
    started = time.monotonic()
    try:
        yield
    finally:
        key = f"{name}_ms"
        elapsed = max(0, int((time.monotonic() - started) * 1000))
        phases[key] = phases.get(key, 0) + elapsed
