"""Cooperative background I/O pacing from Linux pressure observations."""

from dataclasses import dataclass
from pathlib import Path
import threading
import time

from .resource_evidence import (
    MemoryBackingCapacity,
    read_memory_pressure,
    sample_memory_backing,
)


@dataclass(frozen=True)
class Pressure:
    memory_fraction: float = 0.0
    memory_stall: float = 100.0
    io_stall: float = 0.0
    memory_available_bytes: int = 0
    memory_backing: MemoryBackingCapacity | None = None


class PressureSampler:
    def __init__(self, root=Path("/proc"), *, memory_backing_root: Path | None = None):
        self.root = root
        self.memory_backing_root = memory_backing_root
        self._lock = threading.Lock()
        self._at = 0.0
        self._value = Pressure()

    def sample(self):
        with self._lock:
            now = time.monotonic()
            if now - self._at < 0.1:
                return self._value
            evidence = read_memory_pressure(Path(self.root))
            memory = evidence.memory
            total, available = memory.get("MemTotal"), memory.get("MemAvailable")
            # Unknown evidence is explicit in the canonical sample. This policy
            # adapter conservatively disables warm retention, without delaying
            # reclaim or treating an unreadable PSI file as observed zero.
            self._value = Pressure(
                available / total if total and available is not None else 0.0,
                evidence.memory_psi.get("some", 100.0),
                evidence.io_psi.get("some", 0.0),
                available * 1024 if available is not None else 0,
                sample_memory_backing(
                    self.memory_backing_root, proc_root=Path(self.root)
                ),
            )
            self._at = now
            return self._value


class BackgroundPacer:
    """Reduce maintenance duty cycle smoothly; never stop its progress.

    Pacing runs between stream chunks and the stream's inactivity clock resets
    after consumption. The separate control response must use progress-aware
    waiting too, rather than timing out a deliberately paced export.
    """

    def __init__(
        self,
        pressure,
        *,
        foreground=lambda: False,
        clock=time.monotonic,
        sleep=time.sleep,
    ):
        self.pressure, self.clock, self.sleep = pressure, clock, sleep
        self.foreground = foreground
        self.previous = clock()
        self.wait_seconds = 0.0

    def pace(self):
        now = self.clock()
        working = max(0.0, now - self.previous)
        stall = max(0.0, min(90.0, self.pressure().io_stall)) / 100
        stall = max(stall, 0.5 if self.foreground() else 0.0)
        delay = min(0.1, working * stall / max(0.1, 1 - stall))
        if delay:
            self.sleep(delay)
            self.wait_seconds += delay
        self.previous = self.clock()
