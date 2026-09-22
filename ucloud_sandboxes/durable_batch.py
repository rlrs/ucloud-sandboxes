"""Group local SQLite writes without acknowledging uncommitted operations."""

from contextlib import contextmanager
from collections import deque
from dataclasses import dataclass, field
import threading
import time


@dataclass
class _Batch:
    deadline: float
    done: threading.Event = field(default_factory=threading.Event)
    error: BaseException | None = None
    operations: int = 0


class _Transaction:
    def __init__(self, connection):
        self.connection = connection
        self.explicit = False
        self.committed = False

    def execute(self, statement, parameters=()):
        command = statement.strip().rstrip(";").upper()
        if command in {"BEGIN", "BEGIN IMMEDIATE", "BEGIN TRANSACTION"}:
            if self.explicit:
                raise RuntimeError("nested journal transaction")
            self.explicit = True
            return self.connection.execute("SELECT 1 WHERE 0")
        if command.startswith(("COMMIT", "END", "ROLLBACK", "SAVEPOINT", "RELEASE")):
            raise ValueError("use journal commit()/rollback(), not transaction SQL")
        return self.connection.execute(statement, parameters)

    def executemany(self, statement, parameters):
        return self.connection.executemany(statement, parameters)

    def commit(self):
        self.committed = True

    def rollback(self):
        self.connection.execute("ROLLBACK TO operation")
        self.explicit = True
        self.committed = False


class DurableSqliteBatch:
    """One writer, per-operation savepoints, one FULL commit per short batch.

    The batch size and delay bound transaction work, never request admission.
    A failed operation rolls back only its savepoint. Commit failure fails every
    waiter; no acknowledged state is served from this connection before commit.
    """

    def __init__(self, connect, validate, *, delay_seconds=0.001, max_operations=64):
        if delay_seconds < 0 or max_operations < 1:
            raise ValueError("invalid journal batch window")
        self.connect, self.validate = connect, validate
        self.delay = delay_seconds
        self.max_operations = max_operations
        self._condition = threading.Condition()
        # The flusher and writers wait for different events. Waking every
        # writer when a closed batch needs flushing makes them wake each other
        # repeatedly, competing with the one thread that can commit it.
        self._flush_condition = threading.Condition(self._condition)
        self._writers_lock = threading.Lock()
        self._writers = deque()
        self._batch = None
        self._connection = None
        self._thread = None
        self.commits = 0
        self.operations = 0
        self._stats_guard = threading.Lock()
        self._stats = {
            key: 0.0
            for key in (
                "queue_wait_ms",
                "transaction_ms",
                "commit_ms",
                "failed_batches",
            )
        }

    def metrics(self):
        with self._stats_guard:
            return {
                "journal_batch_" + key: value
                for key, value in {
                    **self._stats,
                    "commits": self.commits,
                    "operations": self.operations,
                }.items()
            }

    def _observe(self, key, value):
        with self._stats_guard:
            self._stats[key] += value

    def _abort(self, batch, exc):
        batch.error = exc
        self._observe("failed_batches", 1)
        try:
            self._connection.close()  # rolls back any uncommitted savepoints
        finally:
            self._connection = None
            self._batch = None
            batch.done.set()
            self._condition.notify_all()
            self._flush_condition.notify()

    @contextmanager
    def _writer_turn(self):
        # Wake only the next queued writer. Letting every waiting request race
        # for each new batch creates a convoy and lets newcomers starve older
        # requests. Release the turn before waiting for durable commit, so
        # multiple operations can still share the same transaction.
        ready = threading.Event()
        with self._writers_lock:
            self._writers.append(ready)
            if len(self._writers) == 1:
                ready.set()
        try:
            ready.wait()
            yield
        finally:
            with self._writers_lock:
                first = self._writers[0] is ready
                self._writers.remove(ready)
                if first and self._writers:
                    self._writers[0].set()

    @contextmanager
    def transaction(self):
        self.validate()
        failure = None
        queued = time.monotonic()
        with self._writer_turn(), self._condition:
            while self._batch is not None and (
                self._batch.operations >= self.max_operations
                or time.monotonic() >= self._batch.deadline
            ):
                # Closed batches no longer accept writers. Yield the lock to
                # the flusher instead of letting arrivals postpone it forever.
                self._flush_condition.notify()
                self._condition.wait()
            self._observe("queue_wait_ms", (time.monotonic() - queued) * 1000)
            self.validate()
            if self._connection is None:
                self._connection = self.connect()
            if self._batch is None:
                self._connection.execute("BEGIN IMMEDIATE")
                self._batch = _Batch(time.monotonic() + self.delay)
            batch = self._batch
            connection = self._connection
            started = time.monotonic()
            try:
                connection.execute("SAVEPOINT operation")
                view = _Transaction(connection)
                try:
                    yield view
                    if view.explicit and not view.committed:
                        connection.execute("ROLLBACK TO operation")
                except BaseException as exc:
                    failure = exc
                    connection.execute("ROLLBACK TO operation")
                connection.execute("RELEASE operation")
                batch.operations += 1
                if batch.operations >= self.max_operations:
                    batch.deadline = 0
                if self._thread is None:
                    thread = threading.Thread(
                        target=self._flush, name="storage-journal-commit", daemon=True
                    )
                    thread.start()
                    self._thread = thread
                self._flush_condition.notify()
            except BaseException as exc:
                self._abort(batch, exc)
            finally:
                self._observe("transaction_ms", (time.monotonic() - started) * 1000)
        batch.done.wait()
        if batch.error is not None:
            raise batch.error
        self.validate()
        if failure is not None:
            raise failure

    def _flush(self):
        with self._condition:
            while True:
                if self._batch is None:
                    if not self._flush_condition.wait_for(
                        lambda: self._batch is not None, timeout=1
                    ):
                        if self._connection is not None:
                            self._connection.close()
                        self._connection = None
                        self._thread = None
                        return
                batch = self._batch
                remaining = batch.deadline - time.monotonic()
                if remaining > 0:
                    self._flush_condition.wait(remaining)
                    continue
                started = time.monotonic()
                try:
                    self.validate()
                    self._connection.commit()
                    self.validate()
                    with self._stats_guard:
                        self.commits += 1
                        self.operations += batch.operations
                except BaseException as exc:
                    self._abort(batch, exc)
                finally:
                    self._observe("commit_ms", (time.monotonic() - started) * 1000)
                    self._batch = None
                    batch.done.set()
                    self._condition.notify_all()
                if self._connection is None:
                    self._thread = None
                    return
