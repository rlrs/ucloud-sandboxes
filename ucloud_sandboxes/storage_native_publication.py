from __future__ import annotations

from contextlib import contextmanager
from collections import OrderedDict
from pathlib import Path
import stat
import threading
import time
from typing import Callable, Generic, Iterator, Protocol, TypeVar

from .telemetry import Telemetry


DEFAULT_MAX_CONCURRENT_PUBLICATIONS = 4


class _Layer(Protocol):
    digest: str
    size: int


LayerT = TypeVar("LayerT", bound=_Layer)


def local_layer_identity(path: Path) -> tuple:
    """Metadata identity of an immutable sealed input, without reading its data."""
    info = path.stat(follow_symlinks=False)
    if not stat.S_ISREG(info.st_mode):
        raise ValueError("publication input must be a regular file")
    return (str(path), info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


class CompletedLayerUploads(Generic[LayerT]):
    """Reuse confirmed blobs across failed publications, never partial uploads.

    Entries are scoped to one publisher/backend and bounded only for memory use.
    Remote existence is checked on every hit because unreferenced blobs can be
    collected. This is an optimization, not checkpoint authority or a lease.
    """

    def __init__(self, capacity: int = 1024) -> None:
        self._capacity = capacity
        self._entries: OrderedDict[tuple, LayerT] = OrderedDict()
        self._lock = threading.Lock()
        self._reused = self._reused_bytes = 0

    def publish(
        self, *, identity: Callable[[], tuple], upload: Callable[[], LayerT],
        exists: Callable[[LayerT], bool], check_current: Callable[[], None] | None,
    ) -> tuple[LayerT, int]:
        if check_current is not None:
            check_current()
        key = identity()
        with self._lock:
            cached = self._entries.get(key)
        if cached is not None and exists(cached):
            if check_current is not None:
                check_current()
            if identity() != key:
                raise ValueError("sealed publication inputs changed during reuse")
            with self._lock:
                if key in self._entries:
                    self._entries.move_to_end(key)
                self._reused += 1
                self._reused_bytes += cached.size
            return cached, 0
        with self._lock:
            self._entries.pop(key, None)
        layer = upload()
        if identity() != key:
            raise ValueError("sealed publication inputs changed during upload")
        # Save before the caller's next ownership check: a completed blob remains
        # reusable even if the snapshot manifest is never committed.
        with self._lock:
            self._entries[key] = layer
            self._entries.move_to_end(key)
            while len(self._entries) > self._capacity:
                self._entries.popitem(last=False)
        return layer, layer.size

    def metrics(self) -> dict[str, int]:
        with self._lock:
            return {"snapshot_reused_layers": self._reused,
                    "snapshot_reused_layer_bytes": self._reused_bytes}

    def completed_dense_sources(self, paths: tuple[Path, ...]) -> dict[str, Path]:
        """Return logical local equivalents of successfully exported dense blobs."""
        result = {}
        for path in paths:
            try:
                key = ("dense", local_layer_identity(path))
            except OSError:
                continue
            with self._lock:
                layer = self._entries.get(key)
            if layer is not None:
                result[layer.digest] = path
        return result


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
