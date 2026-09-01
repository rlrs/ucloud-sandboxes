from __future__ import annotations

import math
from threading import Event, Lock
import time
from typing import Callable, Generic, TypeVar, cast


_ValueT = TypeVar("_ValueT")


class GenerationFencedSingleFlightCache(Generic[_ValueT]):
    """Cache one expiring value while coalescing concurrent cold loads.

    Invalidation never waits for the loader. It advances a generation fence and
    clears the published value, preventing an older in-flight load from
    repopulating the cache. Loader failures are not cached and always wake
    waiters so one of them, or the next caller, can retry.
    """

    def __init__(
        self,
        *,
        ttl_seconds: float,
        clock: Callable[[], float] = time.monotonic,
        ttl_seconds_for_value: Callable[[_ValueT], float] | None = None,
    ) -> None:
        self._ttl_seconds = _valid_ttl_seconds(ttl_seconds)
        self._ttl_seconds_for_value = ttl_seconds_for_value
        self._clock = clock
        self._lock = Lock()
        self._cached = False
        self._value: _ValueT | None = None
        self._loaded_at = 0.0
        self._loaded_ttl_seconds = self._ttl_seconds
        self._generation = 0
        self._loading: Event | None = None

    def get_or_load(self, loader: Callable[[], _ValueT]) -> _ValueT:
        """Return a fresh value, joining any load already in progress."""

        while True:
            with self._lock:
                if self._is_fresh_locked():
                    return cast(_ValueT, self._value)
                loading = self._loading
                if loading is None:
                    loading = Event()
                    self._loading = loading
                    generation = self._generation
                    break
            loading.wait()

        try:
            value = loader()
            loaded_at = self._clock()
            loaded_ttl_seconds = (
                self._ttl_seconds
                if self._ttl_seconds_for_value is None
                else _valid_ttl_seconds(self._ttl_seconds_for_value(value))
            )
        except BaseException:
            self._finish_load(loading)
            raise

        with self._lock:
            if self._generation == generation:
                self._value = value
                self._loaded_at = loaded_at
                self._loaded_ttl_seconds = loaded_ttl_seconds
                self._cached = True
            self._complete_loading_locked(loading)
        return value

    def invalidate(self) -> None:
        """Fence any in-flight load and clear the published value."""

        with self._lock:
            self._generation += 1
            self._cached = False
            self._value = None
            self._loaded_at = 0.0
            self._loaded_ttl_seconds = self._ttl_seconds
            loading = self._loading
            self._loading = None
        if loading is not None:
            loading.set()

    def _is_fresh_locked(self) -> bool:
        if not self._cached:
            return False
        age = self._clock() - self._loaded_at
        return 0.0 <= age <= self._loaded_ttl_seconds

    def _finish_load(self, loading: Event) -> None:
        with self._lock:
            self._complete_loading_locked(loading)

    def _complete_loading_locked(self, loading: Event) -> None:
        if self._loading is loading:
            self._loading = None
        loading.set()


def _valid_ttl_seconds(value: float) -> float:
    resolved = float(value)
    if not math.isfinite(resolved) or resolved < 0:
        raise ValueError("cache TTL must be non-negative and finite")
    return resolved
