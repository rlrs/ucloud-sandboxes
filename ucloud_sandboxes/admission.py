"""FIFO resource admission: wait before allocating, without retry races."""

from collections import deque
from dataclasses import dataclass, field
from threading import Condition, Event
import time


@dataclass(eq=False)
class _Waiter:
    weight: int
    ready: Event = field(default_factory=Event)
    granted: bool = False


class FairCapacity:
    def __init__(self, capacity: int) -> None:
        if capacity < 1:
            raise ValueError("capacity must be positive")
        self.capacity = capacity
        self._available = capacity
        self._condition = Condition()
        self._waiters: deque[_Waiter] = deque()

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
            ticket = _Waiter(weight)
            self._waiters.append(ticket)
        try:
            remaining = None if deadline is None else max(0, deadline - time.monotonic())
            ticket.ready.wait(remaining)
            with self._condition:
                # A grant racing the timeout owns the capacity until this
                # caller accepts or returns it; never leak or double-grant it.
                if ticket.granted:
                    return True
                self._waiters.remove(ticket)
                self._grant_waiters()
                return False
        except BaseException:
            with self._condition:
                if ticket.granted:
                    self._available += ticket.weight
                elif ticket in self._waiters:
                    self._waiters.remove(ticket)
                self._grant_waiters()
            raise

    def _grant_waiters(self) -> None:
        # Called under the queue lock. Reserve before notifying, so new callers
        # cannot steal a released slot. Only eligible FIFO heads are awakened;
        # a burst no longer broadcasts every release to all waiting threads.
        while self._waiters and self._waiters[0].weight <= self._available:
            ticket = self._waiters.popleft()
            self._available -= ticket.weight
            ticket.granted = True
            ticket.ready.set()

    def release(self, *, weight: int = 1) -> None:
        with self._condition:
            if weight <= 0 or self._available + weight > self.capacity:
                raise ValueError("capacity released without a reservation")
            self._available += weight
            self._grant_waiters()

    @property
    def waiting(self) -> int:
        with self._condition:
            return len(self._waiters)
