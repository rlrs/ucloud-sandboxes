"""A reclaim-aware grace period for relay parks; no lifecycle authority here."""

from collections import OrderedDict, deque
from contextlib import contextmanager
import threading
import time

from .background_io import PressureSampler


class WarmParkDeferred(RuntimeError):
    def __init__(self, seconds):
        super().__init__("relay park deferred for warm retention")
        self.seconds = seconds


class WarmParkPolicy:
    def __init__(self, pressure=None, *, max_delay=None, demand_bytes=lambda: 0):
        self.pressure = pressure or PressureSampler().sample
        self.max_delay = max_delay
        self.demand_bytes = demand_bytes
        self._lock = threading.Lock()
        self._pending = {}
        self._waiting_since = OrderedDict()
        self._responses = deque(maxlen=64)

    def _budget(self, memory_bytes=0):
        p = self.pressure()
        incoming = self.demand_bytes()
        if incoming and incoming >= p.memory_available_bytes:
            return 0.0
        headroom = max(0.0, min(1.0, (p.memory_fraction - 0.1) / 0.25))
        # Linux PSI avg10 is a percentage, not a fraction. A 1% stall
        # must not disable retention and trigger a checkpoint/restore storm.
        headroom *= max(0.0, min(1.0, 1.0 - p.memory_stall / 100.0))
        if p.memory_available_bytes:
            spare = max(0, p.memory_available_bytes - incoming)
            headroom *= spare / p.memory_available_bytes
            # A large retained sandbox yields sooner when its memory could
            # materially increase headroom. MemAvailable already accounts for
            # resident warm sandboxes; do not charge their footprint twice.
            if memory_bytes:
                headroom *= min(1.0, spare / (4 * memory_bytes))
        with self._lock:
            recent = sorted(self._responses)
        if self.max_delay is None:
            # A model wait has no useful fixed expiry while its resident pages
            # fit. Checkpointing it merely duplicates memory into storage and
            # creates the I/O/reclaim that then blocks its wake. As headroom
            # shrinks, older waits yield first; queued demand/drain still wins.
            expected = recent[-1] if recent else 30.0
            # Strong reclaim must shorten retention even when MemAvailable
            # includes reclaimable (but expensive-to-write) guest page cache.
            headroom *= max(0.0, 1.0 - p.memory_stall / 10.0)
            if headroom >= 1.0:
                return float("inf")
            return max(0.05, expected) * headroom / max(0.001, 1.0 - headroom)
        expected = (
            recent[min(len(recent) - 1, int(len(recent) * 0.90))]
            if recent
            else self.max_delay
        )
        return headroom * min(self.max_delay, max(0.05, expected))

    @contextmanager
    def defer(self, key, *, memory_bytes=0, blocking=True):
        with self._lock:
            entry = self._pending.get(key)
            if entry is None:
                started = self._waiting_since.setdefault(key, time.monotonic())
                # Prediction history is disposable; lifecycle fences are durable
                # elsewhere. Bound metadata without limiting accepted parks.
                while len(self._waiting_since) > 4096:
                    self._waiting_since.popitem(last=False)
                entry = [threading.Event(), started, 0]
                self._pending[key] = entry
            entry[2] += 1
        event, started, _ = entry
        try:
            # No sandbox lock, execution slot or storage reservation is held.
            while not event.is_set():
                remaining = self._budget(memory_bytes) - (time.monotonic() - started)
                if remaining <= 0:
                    break
                if not blocking:
                    # Recheck pressure/demand even if the resource-based lease
                    # has no expiry. Never put Infinity into the HTTP response.
                    raise WarmParkDeferred(
                        min(3.0, remaining) if self.max_delay is None else remaining
                    )
                event.wait(min(0.05, remaining))
            yield event
        finally:
            with self._lock:
                entry[2] -= 1
                if not entry[2]:
                    self._pending.pop(key, None)

    def wake(self, key):
        with self._lock:
            started = self._waiting_since.pop(key, None)
            if started is not None:
                # Include responses arriving after parking as well, otherwise
                # sampling only cancelled parks shrinks the grace period forever.
                self._responses.append(time.monotonic() - started)
            entry = self._pending.get(key)
            if entry is not None:
                entry[0].set()
