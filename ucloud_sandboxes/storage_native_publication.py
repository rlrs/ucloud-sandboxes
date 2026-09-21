from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
import threading
import time
from typing import Callable, Iterator

from .telemetry import Telemetry


DEFAULT_MAX_CONCURRENT_PUBLICATIONS = 4


def local_layer_data_bytes(path: Path) -> int:
    """Estimate export work without counting sparse virtual-address holes.

    Sparse OverlayBD uppers retain the volume's virtual EOF when sealed.
    Publication streams mapped data, so st_size can overstate that work by
    orders of magnitude. Allocation is a scheduling estimate, never a digest,
    quota, or authority check; dense export still computes the exact output.
    """
    stat = path.stat()
    blocks = getattr(stat, "st_blocks", None)
    return stat.st_size if blocks is None else min(stat.st_size, blocks * 512)


def snapshot_chain_needs_compaction(
    layer_sizes: tuple[int, ...], *, max_layers: int, max_delta_bytes: int,
) -> bool:
    # The oldest layer is the base, including the result of the last flatten.
    # Counting it makes a base larger than the byte threshold trigger another
    # full rewrite after every tiny delta. Bound accumulated deltas instead;
    # the independent layer-depth trigger still bounds lookup work.
    return len(layer_sizes) > max_layers or sum(layer_sizes[1:]) > max_delta_bytes


def snapshot_compaction_start(
    layer_sizes: tuple[int, ...], *, max_layers: int, max_delta_bytes: int,
    reusable_base: bool, origin_changed: bool = False,
) -> int | None:
    """Return the first layer to merge, or None when append alone suffices.

    Depth-only maintenance can retain a dominant immutable base and merge its
    deltas. This avoids at least half the input bytes while returning two layers.
    Delta growth still forces a full merge so obsolete base data is eventually
    reclaimed. A changed blob origin must copy every referenced layer.
    """
    if origin_changed:
        return 0
    if not snapshot_chain_needs_compaction(
        layer_sizes, max_layers=max_layers, max_delta_bytes=max_delta_bytes,
    ):
        return None
    delta_bytes = sum(layer_sizes[1:])
    if (
        reusable_base and max_layers >= 2 and len(layer_sizes) >= 3
        and delta_bytes <= max_delta_bytes and layer_sizes[0] > delta_bytes
    ):
        return 1
    return 0


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
    def acquire(self, telemetry: Telemetry, check_current: Callable[[], None] | None = None) -> Iterator[int]:
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
                if check_current is None:
                    self._semaphore.acquire()
                else:
                    check_current()
                    while not self._semaphore.acquire(timeout=0.25):
                        check_current()
                acquired = True
                if check_current is not None:
                    check_current()
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
