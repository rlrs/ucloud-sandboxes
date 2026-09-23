"""Reusable SQLite connection leases; bursts queue instead of opening more files.

SQLite can retain a closed connection's Unix descriptor while another connection
holds a POSIX lock on the same database. Bounding only idle retention therefore
does not bound descriptors. Reserve a lease before opening, and keep connections
until shutdown. Transactions never survive return to the pool.
"""

from collections import deque
from contextlib import contextmanager
import sqlite3
from threading import Condition, RLock


class SqliteConnectionPool:
    def __init__(self, capacity=16):
        if capacity < 1:
            raise ValueError("SQLite connection capacity must be positive")
        self.capacity = capacity
        self._lock = RLock()
        self._condition = Condition(self._lock)
        self._idle = []
        self._opened = 0
        self._closed = False
        self._waiters = deque()

    @contextmanager
    def connection(self, connect):
        connection = None
        reusable = False
        with self._condition:
            # Each FIFO waiter has its own condition on the same mutex. Waking
            # every HTTP thread on every short query creates quadratic scheduler
            # work and can starve the threads returning useful connections.
            ticket = Condition(self._lock)
            self._waiters.append(ticket)
            try:
                ticket.wait_for(
                    lambda: self._closed or (
                        self._waiters[0] is ticket
                        and (self._idle or self._opened < self.capacity)
                    )
                )
                if self._closed:
                    raise sqlite3.DatabaseError("SQLite connection pool is closed")
                if self._idle:
                    connection = self._idle.pop()
                else:
                    self._opened += 1
            finally:
                self._waiters.remove(ticket)
                self._wake_next()
        try:
            if connection is None:
                connection = connect()
            yield connection
            if connection.in_transaction:
                connection.rollback()
            reusable = True
        finally:
            with self._condition:
                if reusable and not self._closed:
                    self._idle.append(connection)
                    connection = None
                else:
                    self._opened -= 1
                self._wake_next()
            if connection is not None:
                connection.close()

    def close(self):
        with self._condition:
            self._closed = True
            idle, self._idle = self._idle, []
            self._opened -= len(idle)
            for ticket in self._waiters:
                ticket.notify()
        for connection in idle:
            connection.close()

    def _wake_next(self):
        # Caller holds the shared mutex. Admission wakes the next head again if
        # more capacity remains, so independent available leases still progress.
        if self._waiters and (self._closed or self._idle or self._opened < self.capacity):
            self._waiters[0].notify()
