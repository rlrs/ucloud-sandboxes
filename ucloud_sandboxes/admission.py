"""FIFO resource admission: wait before allocating, without retry races."""

from collections import deque
from dataclasses import dataclass, field
from threading import Condition, Event, get_ident
import time


@dataclass(eq=False)
class _Waiter:
    weight: int
    owner: object = None
    ready: Event = field(default_factory=Event)
    granted: bool = False
    cancelled: bool = False


class FairCapacity:
    def __init__(self, capacity: int) -> None:
        if capacity < 1:
            raise ValueError("capacity must be positive")
        self.capacity = capacity
        self._available = capacity
        self._condition = Condition()
        self._waiters: deque[_Waiter] = deque()

    def acquire(
        self, blocking: bool = True, timeout: float | None = None, *, weight: int = 1,
        owner: object = None,
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
            ticket = _Waiter(weight, owner=owner)
            self._waiters.append(ticket)
        try:
            remaining = None if deadline is None else max(0, deadline - time.monotonic())
            ticket.ready.wait(remaining)
            with self._condition:
                # A grant racing the timeout owns the capacity until this
                # caller accepts or returns it; never leak or double-grant it.
                if ticket.granted:
                    return True
                if not ticket.cancelled:
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

    def cancel_waiters(self, owner: object = None) -> int:
        """Wake queued owners without revoking already-granted capacity."""
        with self._condition:
            selected = [ticket for ticket in self._waiters if owner is None or ticket.owner == owner]
            for ticket in selected:
                self._waiters.remove(ticket)
                ticket.cancelled = True
                ticket.ready.set()
            self._grant_waiters()
            return len(selected)

    @property
    def waiting(self) -> int:
        with self._condition:
            return len(self._waiters)


class FairRLock:
    """Reentrant FIFO mutex built on the shared admission queue.

    Returning capacity grants the oldest waiter before a new caller can acquire
    it. Reentrancy preserves nested reservation helpers without self-deadlock.
    """

    def __init__(self) -> None:
        self._capacity = FairCapacity(1)
        self._owner: int | None = None
        self._depth = 0

    def acquire(self, blocking: bool = True, timeout: float | None = None) -> bool:
        owner = get_ident()
        if self._owner == owner:
            self._depth += 1
            return True
        if not self._capacity.acquire(blocking=blocking, timeout=timeout):
            return False
        self._owner = owner
        self._depth = 1
        return True

    def release(self) -> None:
        if self._owner != get_ident():
            raise RuntimeError("cannot release an unowned reservation lock")
        self._depth -= 1
        if not self._depth:
            self._owner = None
            self._capacity.release()

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, *_exc) -> None:
        self.release()
