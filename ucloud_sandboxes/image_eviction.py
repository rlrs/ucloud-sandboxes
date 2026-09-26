"""Keep recently used images on a worker; evict the least recently used under disk pressure.

A worker's Docker data root is a cache of images. Deleting a sandbox releases
its rootfs cache entry but keeps the pulled image, so repeated rollouts of a
task start without a pull. Without eviction the store fills and every later
pull fails with ENOSPC, which hangs creates while Docker retries.

This policy grants no lifecycle authority: the store removes an image only
under its exclusive digest lock, after re-checking that no registration
references it.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from pathlib import Path
from typing import Callable, Iterable, Protocol

_LOG = logging.getLogger(__name__)

# Start evicting at 85% of the image filesystem and stop below 70%, so one
# sweep frees room for several pulls instead of running before every pull.
HIGH_WATERMARK = 0.85
LOW_WATERMARK = 0.70
# An image pulled or released this recently may be about to back a create
# whose registration does not exist yet.
RECENT_USE_GRACE_SECONDS = 600.0


class EvictableImageStore(Protocol):
    root: Path

    def list_image_ids(self) -> tuple[str, ...]: ...

    def image_tag_times(self, image_ids: Iterable[str]) -> dict[str, float]: ...

    def evict_image(
        self, image_id: str, *, is_referenced: Callable[[str], bool]
    ) -> bool: ...


class ImageCacheEvictor:
    def __init__(
        self,
        store: EvictableImageStore,
        *,
        is_referenced: Callable[[str], bool],
        high_watermark: float = HIGH_WATERMARK,
        low_watermark: float = LOW_WATERMARK,
        grace_seconds: float = RECENT_USE_GRACE_SECONDS,
        clock: Callable[[], float] = time.time,
        statvfs: Callable[[Path], os.statvfs_result] = os.statvfs,
    ) -> None:
        if not 0 < low_watermark < high_watermark <= 1:
            raise ValueError("image eviction watermarks must satisfy 0 < low < high <= 1")
        self.store = store
        self.is_referenced = is_referenced
        self.high_watermark = high_watermark
        self.low_watermark = low_watermark
        self.grace_seconds = grace_seconds
        self.clock = clock
        self.statvfs = statvfs
        # Callbacks run after a sweep that removed images, for example to stop
        # advertising them in heartbeats.
        self.after_eviction: list[Callable[[tuple[str, ...]], None]] = []
        self._last_used: dict[str, float] = {}
        self._guard = threading.Lock()
        self._sweep = threading.Lock()
        self.evicted = 0

    def note_used(self, image_id: str) -> None:
        """Record that an image was just in use (for example, its sandbox was deleted)."""

        with self._guard:
            self._last_used[image_id] = self.clock()

    def usage(self) -> float:
        info = self.statvfs(self.store.root)
        if info.f_blocks <= 0:
            return 0.0
        return (info.f_blocks - info.f_bavail) / info.f_blocks

    def evict_if_needed(self) -> tuple[str, ...]:
        """Evict unreferenced images, least recently used first, if above the high mark."""

        if self.usage() < self.high_watermark:
            return ()
        # One sweep at a time; a concurrent caller relies on the running one.
        if not self._sweep.acquire(blocking=False):
            return ()
        try:
            return self._sweep_locked()
        finally:
            self._sweep.release()

    def _sweep_locked(self) -> tuple[str, ...]:
        image_ids = self.store.list_image_ids()
        tag_times = self.store.image_tag_times(image_ids)
        now = self.clock()
        with self._guard:
            last_used = {
                image_id: max(self._last_used.get(image_id, 0.0), tag_times.get(image_id, 0.0))
                for image_id in image_ids
            }
            self._last_used = {
                image_id: seen for image_id, seen in self._last_used.items()
                if image_id in last_used
            }
        evicted: list[str] = []
        for image_id in sorted(image_ids, key=lambda item: (last_used[item], item)):
            if self.usage() < self.low_watermark:
                break
            if now - last_used[image_id] < self.grace_seconds:
                continue
            try:
                if self.store.evict_image(image_id, is_referenced=self.is_referenced):
                    evicted.append(image_id)
            except Exception as exc:  # One bad image must not stop the sweep.
                _LOG.warning("could not evict cached image %s: %s", image_id, exc)
        if evicted:
            self.evicted += len(evicted)
            _LOG.info(
                "evicted %d cached image(s); image store at %.0f%%",
                len(evicted), 100 * self.usage(),
            )
            for callback in self.after_eviction:
                try:
                    callback(tuple(evicted))
                except Exception as exc:
                    _LOG.warning("image eviction follow-up failed: %s", exc)
        elif self.usage() >= self.high_watermark:
            _LOG.warning(
                "image store at %.0f%% and no image is evictable "
                "(all referenced, busy or used in the last %.0f s)",
                100 * self.usage(), self.grace_seconds,
            )
        return tuple(evicted)
