from __future__ import annotations

from dataclasses import dataclass
import time
from typing import Any, Callable, Iterable, Mapping

from .singleflight_cache import GenerationFencedSingleFlightCache


ImageInventoryRecord = dict[str, Any]
INCOMPLETE_IMAGE_INVENTORY_TTL_SECONDS = 0.5


@dataclass(frozen=True)
class ImageInventorySnapshot:
    """One raw cross-node inventory observation and its completeness proof."""

    records: tuple[ImageInventoryRecord, ...]
    complete: bool
    unobserved_references: frozenset[str] = frozenset()

    @classmethod
    def from_records(
        cls,
        records: Iterable[Mapping[str, Any]],
        *,
        complete: bool,
        unobserved_references: Iterable[str] = (),
    ) -> "ImageInventorySnapshot":
        return cls(
            records=tuple(dict(record) for record in records),
            complete=complete,
            unobserved_references=frozenset(unobserved_references),
        )


ImageInventoryLoader = Callable[[], ImageInventorySnapshot]


class ImageInventoryCache:
    """Keep one short-lived, generation-fenced image inventory snapshot.

    Exactly one caller performs a cold load. Other callers wait without holding
    the state lock, then recheck the cache. Invalidation never waits for network
    I/O: it advances the generation and clears the snapshot. A load that began
    before that invalidation may still serve its initiating request, but it can
    no longer publish stale records for later callers.
    """

    def __init__(
        self,
        *,
        ttl_seconds: float,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        resolved_ttl = float(ttl_seconds)
        self._cache = GenerationFencedSingleFlightCache[ImageInventorySnapshot](
            ttl_seconds=resolved_ttl,
            clock=clock,
            ttl_seconds_for_value=lambda snapshot: (
                resolved_ttl
                if snapshot.complete
                else min(resolved_ttl, INCOMPLETE_IMAGE_INVENTORY_TTL_SECONDS)
            ),
        )

    def get_or_load(self, loader: ImageInventoryLoader) -> ImageInventorySnapshot:
        """Return a fresh snapshot, joining any load already in progress."""

        snapshot = self._cache.get_or_load(lambda: _copy_snapshot(loader()))
        return _copy_snapshot(snapshot)

    def invalidate(self) -> None:
        """Fence any in-flight load and clear the published snapshot."""

        self._cache.invalidate()


def _copy_snapshot(snapshot: ImageInventorySnapshot) -> ImageInventorySnapshot:
    return ImageInventorySnapshot.from_records(
        snapshot.records,
        complete=snapshot.complete,
        unobserved_references=snapshot.unobserved_references,
    )
