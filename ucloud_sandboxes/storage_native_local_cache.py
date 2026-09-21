"""Disposable local equivalents of published blobs; remote descriptors stay authoritative."""
from collections import OrderedDict
import logging
import os
from pathlib import Path
import shutil
import threading
from uuid import uuid4

from .storage_native_publication import local_layer_data_bytes


LOGGER = logging.getLogger(__name__)


class PublishedLocalCache:
    def __init__(self, root: Path, *, capacity_bytes: int) -> None:
        self.root = root
        self.capacity_bytes = capacity_bytes
        self._lock = threading.Lock()
        self._entries: OrderedDict[tuple[str, str], tuple[Path, int]] = OrderedDict()
        self._bytes = self._hits = self._misses = self._evictions = 0
        try:
            root.mkdir(mode=0o700, parents=True, exist_ok=True)
            if root.is_symlink():
                raise OSError("local published cache root cannot be a symlink")
            # Cache metadata is process-local. Mount-specific pins live under
            # their volume and survive cleanup/restart via the ordinary journal.
            for path in root.glob("layer-*.commit"):
                path.unlink(missing_ok=True)
        except OSError:
            self.capacity_bytes = 0
            LOGGER.warning("published local cache unavailable; using remote layers", exc_info=True)

    def _has_headroom(self) -> bool:
        usage = shutil.disk_usage(self.root)
        return usage.free > min(1024**3, usage.total // 20)

    def _evict(self) -> None:
        _, (path, size) = self._entries.popitem(last=False)
        path.unlink(missing_ok=True)
        self._bytes -= size
        self._evictions += 1

    def maintain(self) -> None:
        try:
            with self._lock:
                while self._entries and (self._bytes > self.capacity_bytes or not self._has_headroom()):
                    self._evict()
        except OSError:
            LOGGER.warning("published local cache maintenance deferred", exc_info=True)

    def remember(self, origin: str, digest: str, source: Path) -> None:
        """Retain a sealed input only after its completed export is confirmed."""
        try:
            size = local_layer_data_bytes(source)
            if self.capacity_bytes <= 0 or size > self.capacity_bytes:
                return
            with self._lock:
                key = (origin, digest)
                if key in self._entries:
                    self._entries.move_to_end(key)
                    return
                while self._entries and (self._bytes + size > self.capacity_bytes or not self._has_headroom()):
                    self._evict()
                if not self._has_headroom():
                    return
                target = self.root / f"layer-{uuid4().hex}.commit"
                os.link(source, target)  # same filesystem, no data copy or rehash
                self._entries[key] = (target, size)
                self._bytes += size
        except OSError:
            # An unavailable optimization cannot fail a durable publication.
            LOGGER.warning("published local cache retention skipped", exc_info=True)

    def pin(self, origin: str, digest: str, volume_root: Path, *, mount_revision: int = 0) -> Path | None:
        """Pin before opening a device so concurrent LRU eviction is harmless."""
        try:
            with self._lock:
                entry = self._entries.get((origin, digest))
                if entry is None or not self._has_headroom():
                    self._misses += 1
                    return None
                target = volume_root / f"published-local-{mount_revision}-{uuid4().hex}.commit"
                os.link(entry[0], target)
                self._entries.move_to_end((origin, digest))
                self._hits += 1
                return target
        except OSError:
            return None  # fall back to the ordinary remote descriptor

    def metrics(self) -> dict[str, int]:
        with self._lock:
            return {"published_local_cache_bytes": self._bytes,
                    "published_local_cache_entries": len(self._entries),
                    "published_local_cache_hits": self._hits,
                    "published_local_cache_misses": self._misses,
                    "published_local_cache_evictions": self._evictions}

    def paths(self) -> tuple[Path, ...]:
        with self._lock:
            return tuple(path for path, _ in self._entries.values())
