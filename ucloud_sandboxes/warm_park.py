"""A reclaim-aware grace period for relay parks; no lifecycle authority here."""

from collections import OrderedDict, deque
from contextlib import contextmanager
import threading
import time

from .background_io import PressureSampler


class WarmParkPolicy:
    def __init__(self, pressure=None, *, max_delay=15.0, demand_bytes=lambda: 0):
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
        headroom *= max(0.0, 1.0 - p.memory_stall)
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
        expected = (
            recent[min(len(recent) - 1, int(len(recent) * 0.90))]
            if recent
            else self.max_delay
        )
        return headroom * min(self.max_delay, max(0.05, expected))

    @contextmanager
    def defer(self, key, *, memory_bytes=0):
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
