"""Cooperative background I/O pacing from Linux pressure observations."""

from dataclasses import dataclass
from pathlib import Path
import threading
import time


@dataclass(frozen=True)
class Pressure:
    memory_fraction: float = 0.0
    memory_stall: float = 100.0
    io_stall: float = 0.0
    memory_available_bytes: int = 0


class PressureSampler:
    def __init__(self, root=Path("/proc")):
        self.root = root
        self._lock = threading.Lock()
        self._at = 0.0
        self._value = Pressure()

    def sample(self):
        with self._lock:
            now = time.monotonic()
            if now - self._at < 0.1:
                return self._value
            try:
                memory = {
                    k: int(v.split()[0])
                    for line in (self.root / "meminfo").read_text().splitlines()
                    for k, v in [line.split(":", 1)]
                }

                def stall(kind):
                    fields = (
                        (self.root / "pressure" / kind)
                        .read_text()
                        .splitlines()[0]
                        .split()
                    )
                    return float(dict(item.split("=") for item in fields[1:])["avg10"])

                self._value = Pressure(
                    memory["MemAvailable"] / memory["MemTotal"],
                    stall("memory"),
                    stall("io"),
                    memory["MemAvailable"] * 1024,
                )
            except (OSError, ValueError, KeyError, ZeroDivisionError):
                self._value = (
                    Pressure()
                )  # Missing memory evidence never delays reclaim.
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
