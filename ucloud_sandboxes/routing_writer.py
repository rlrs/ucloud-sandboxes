"""Run frequent routing commits outside the gateway's HTTP interpreter."""

from collections import deque
from concurrent.futures import Future, ProcessPoolExecutor
import multiprocessing
import os
from pathlib import Path
import sqlite3
from threading import Condition, Thread
import time


_METHODS = frozenset({
    'confirm_sandbox_wake', 'upsert_exec',
    'upsert_program_request_transition_with_change',
    'reconcile_sandboxes_for_node', 'allocate_sandbox_create_with_pending',
    'upsert_sandbox', 'confirm_sandbox_observation', 'reserve_sandbox_wakes',
})
_store = None
_identity = None


def _check(path, identity):
    info = Path(path).stat()
    if (info.st_dev, info.st_ino) != identity:
        raise sqlite3.DatabaseError('routing database file was replaced')


def _initialize(path, identity):
    from .routing import RoutingStore
    global _store, _identity
    _check(path, identity)
    _store = RoutingStore(Path(path))
    _identity = identity
    _check(path, identity)
    # This process serializes commands itself. A batching timer here would
    # delay every command without allowing another command to join its commit.
    _store._write_batches.delay = 0


def _dispatch(method, args, kwargs):
    _check(_store.path, _identity)
    if method == 'ready':
        return None
    if method not in _METHODS or kwargs.get('_connection') is not None:
        raise ValueError('unsupported routing writer command')
    result = getattr(_store, method)(*args, **kwargs)
    _check(_store.path, _identity)
    return result


def _dispatch_many(commands):
    # Each operation still owns its original FULL transaction and fences.
    # Batch only the IPC envelope, never acknowledge a commit speculatively.
    results = []
    for method, args, kwargs in commands:
        try:
            results.append((True, _dispatch(method, args, kwargs)))
        except Exception as exc:
            results.append((False, exc))
    return results


class RoutingWriteProcess:
    """Keep reads local and move independently fenced writes to a child.

    SQL, generation checks, savepoints, and FULL commits are unchanged. A
    response is returned only after the child acknowledges its commit. IPC
    failure is ambiguous and is never automatically replayed. The caller's
    existing durable readback/idempotency protocol handles that ambiguity.
    """

    def __init__(self, store):
        self._store = store
        info = store.path.stat()
        self._identity = (info.st_dev, info.st_ino)
        self._pid = os.getpid()
        self._guard = Condition()
        self._pending = deque()
        self._dispatcher = None
        self._closed = False
        self._failed = False
        self._executor = ProcessPoolExecutor(
            max_workers=1, mp_context=multiprocessing.get_context('spawn'),
            initializer=_initialize, initargs=(str(store.path), self._identity),
        )
        try:
            self._executor.submit(_dispatch, 'ready', (), {}).result()
            self._dispatcher = Thread(target=self._drain, name='routing-write-dispatch', daemon=True)
            self._dispatcher.start()
        except BaseException:
            self.close()
            raise

    def __getattr__(self, name):
        return getattr(self._store, name)

    def _write(self, method, *args, **kwargs):
        if os.getpid() != self._pid:
            raise sqlite3.DatabaseError('reopen routing writer after fork')
        _check(self._store.path, self._identity)
        with self._guard:
            if self._closed:
                raise RuntimeError('routing writer is closed')
            if self._failed:
                raise sqlite3.DatabaseError('routing writer unavailable; commit outcome is unknown')
            future = Future()
            self._pending.append((future, method, args, kwargs))
            self._guard.notify()
        result = future.result()
        _check(self._store.path, self._identity)
        return result

    def _drain(self):
        while True:
            with self._guard:
                self._guard.wait_for(lambda: self._pending or self._closed)
                if not self._pending:
                    return
                # Coalesce arrivals for at most 1 ms when idle. A backlog is
                # sent immediately. The envelope size bounds IPC work; all
                # callers remain queued and no admission limit is introduced.
                if len(self._pending) == 1 and not self._closed:
                    deadline = time.monotonic() + .001
                    while len(self._pending) < 32 and not self._closed:
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            break
                        self._guard.wait(remaining)
                batch = [self._pending.popleft() for _ in range(min(32, len(self._pending)))]
            try:
                results = self._executor.submit(
                    _dispatch_many, [(method, args, kwargs) for _, method, args, kwargs in batch],
                ).result()
                if len(results) != len(batch):
                    raise RuntimeError('invalid routing writer acknowledgment')
            except BaseException:
                # Some commands may already have committed. Fail every
                # unacknowledged caller and never replay an ambiguous batch.
                with self._guard:
                    self._failed = True
                    batch.extend(self._pending)
                    self._pending.clear()
                for future, *_ in batch:
                    future.set_exception(sqlite3.DatabaseError(
                        'routing writer unavailable; commit outcome is unknown',
                    ))
                return
            for (future, *_), (ok, result) in zip(batch, results):
                if ok:
                    future.set_result(result)
                else:
                    future.set_exception(result)

    def health_error(self):
        return 'routing writer unavailable' if self._failed or self._closed else ''

    def confirm_sandbox_wake(self, *args, **kwargs):
        return self._write('confirm_sandbox_wake', *args, **kwargs)

    def upsert_exec(self, *args, **kwargs):
        return self._write('upsert_exec', *args, **kwargs)

    def upsert_program_request_transition_with_change(self, *args, **kwargs):
        return self._write('upsert_program_request_transition_with_change', *args, **kwargs)

    def allocate_sandbox_create_with_pending(self, *args, **kwargs):
        return self._write('allocate_sandbox_create_with_pending', *args, **kwargs)

    def upsert_sandbox(self, *args, **kwargs):
        return self._write('upsert_sandbox', *args, **kwargs)

    def confirm_sandbox_observation(self,*args,**kwargs):
        return self._write('confirm_sandbox_observation',*args,**kwargs)

    def reserve_sandbox_wakes(self, requests):
        return self._write('reserve_sandbox_wakes', list(requests))

    def reserve_sandbox_wake(self, route, *, pending_id):
        return self.reserve_sandbox_wakes([(route, pending_id)])[route.sandbox_id]

    def reconcile_sandboxes_for_node(self, node_url, observations, *, reported_sandbox_ids, **kwargs):
        # Inventory reconciliation owns SQLite's write lock while projecting
        # every observation. Running that loop in the busy HTTP interpreter
        # also blocks the isolated lifecycle writer on the same database.
        return self._write(
            'reconcile_sandboxes_for_node', node_url, tuple(observations),
            reported_sandbox_ids=tuple(reported_sandbox_ids), **kwargs,
        )

    def close(self):
        with self._guard:
            self._closed = True
            self._guard.notify_all()
        if self._dispatcher is not None and self._dispatcher.ident is not None:
            self._dispatcher.join()
        self._executor.shutdown(wait=True)
