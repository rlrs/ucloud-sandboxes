from __future__ import annotations

from contextlib import contextmanager
import threading
import time
from typing import Iterator

from .telemetry import Telemetry


DEFAULT_MAX_CONCURRENT_PUBLICATIONS = 4


class PublicationGate:
    """One concurrency and queue-observability contract for snapshot backends."""

    def __init__(self, limit: int) -> None:
        if limit < 1:
            raise ValueError("publication concurrency must be positive")
        self._limit = limit
        self._semaphore = threading.BoundedSemaphore(limit)
        self._lock = threading.Lock()
        self._active = 0
        self._waiting = 0
        self._wait_ms_total = 0
        self._wait_ms_max = 0

    @contextmanager
    def acquire(self, telemetry: Telemetry) -> Iterator[int]:
        started = time.monotonic()
        acquired = False
        active_counted = False
        with self._lock:
            self._waiting += 1
            waiting = self._waiting
        try:
            with telemetry.span(
                "snapshot.queue_wait",
                attributes={
                    "snapshot.publication.limit": self._limit,
                    "snapshot.publication.waiting": waiting,
                },
            ) as span:
                self._semaphore.acquire()
                acquired = True
                wait_ms = max(0, int((time.monotonic() - started) * 1000))
                span.set_attribute("snapshot.queue.wait_ms", wait_ms)
            with self._lock:
                self._waiting -= 1
                self._active += 1
                active_counted = True
                self._wait_ms_total += wait_ms
                self._wait_ms_max = max(self._wait_ms_max, wait_ms)
            yield wait_ms
        finally:
            with self._lock:
                if active_counted:
                    self._active -= 1
                else:
                    self._waiting -= 1
            if acquired:
                self._semaphore.release()

    def metrics(self) -> dict[str, int]:
        with self._lock:
            return {
                "snapshot_publication_limit": self._limit,
                "snapshot_publication_active": self._active,
                "snapshot_publication_waiting": self._waiting,
                "snapshot_publication_queue_wait_ms_total": self._wait_ms_total,
                "snapshot_publication_queue_wait_ms_max": self._wait_ms_max,
            }
