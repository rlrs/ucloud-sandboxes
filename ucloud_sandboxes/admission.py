"""FIFO resource admission: wait before allocating, without retry races."""

from collections import deque
from threading import Condition
import time


class FairCapacity:
    def __init__(self, capacity: int) -> None:
        if capacity < 1:
            raise ValueError("capacity must be positive")
        self.capacity = capacity
        self._available = capacity
        self._condition = Condition()
        self._waiters: deque[object] = deque()

    def acquire(
        self, blocking: bool = True, timeout: float | None = None, *, weight: int = 1
    ) -> bool:
        if not 0 < weight <= self.capacity:
            raise ValueError("request weight exceeds capacity")
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._condition:
            if not self._waiters and self._available >= weight:
                self._available -= weight
                return True
            if not blocking:
                return False
            ticket = object()
            self._waiters.append(ticket)
            try:
                while True:
                    if self._waiters[0] is ticket and self._available >= weight:
                        self._available -= weight
                        self._waiters.popleft()
                        self._condition.notify_all()
                        return True
                    remaining = (
                        None if deadline is None else deadline - time.monotonic()
                    )
                    if remaining is not None and remaining <= 0:
                        return False
                    self._condition.wait(remaining)
            finally:
                if ticket in self._waiters:
                    self._waiters.remove(ticket)
                    self._condition.notify_all()

    def release(self, *, weight: int = 1) -> None:
        with self._condition:
            if weight <= 0 or self._available + weight > self.capacity:
                raise ValueError("capacity released without a reservation")
            self._available += weight
            self._condition.notify_all()
