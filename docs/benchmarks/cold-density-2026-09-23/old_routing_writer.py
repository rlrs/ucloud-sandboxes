"""Run frequent routing commits outside the gateway's HTTP interpreter."""

from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
import multiprocessing
import os
from pathlib import Path
import sqlite3
from threading import Lock


_METHODS = frozenset({
    'confirm_sandbox_wake', 'upsert_exec',
    'upsert_program_request_transition_with_change',
})
_store = None
_identity = None


def _check(path, identity):
    info = Path(path).stat()
    if (info.st_dev, info.st_ino) != identity:
        raise sqlite3.DatabaseError('routing database file was replaced')


def _initialize(path, identity):
    from ucloud_sandboxes.routing import RoutingStore
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


class RoutingWriteProcess:
    """Keep reads local and move three independently fenced writes to a child.

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
        self._guard = Lock()
        self._closed = False
        self._failed = False
        self._executor = ProcessPoolExecutor(
            max_workers=1, mp_context=multiprocessing.get_context('spawn'),
            initializer=_initialize, initargs=(str(store.path), self._identity),
        )
        try:
            self._executor.submit(_dispatch, 'ready', (), {}).result()
        except BaseException:
            self.close()
            raise

    def __getattr__(self, name):
        return getattr(self._store, name)

    def _write(self, method, *args, **kwargs):
        if os.getpid() != self._pid:
            raise sqlite3.DatabaseError('reopen routing writer after fork')
        _check(self._store.path, self._identity)
        try:
            with self._guard:
                if self._closed:
                    raise RuntimeError('routing writer is closed')
                future = self._executor.submit(_dispatch, method, args, kwargs)
            result = future.result()
        except BrokenProcessPool:
            self._failed = True
            raise sqlite3.DatabaseError('routing writer unavailable; commit outcome is unknown') from None
        _check(self._store.path, self._identity)
        return result

    def health_error(self):
        return 'routing writer unavailable' if self._failed or self._closed else ''

    def confirm_sandbox_wake(self, *args, **kwargs):
        return self._write('confirm_sandbox_wake', *args, **kwargs)

    def upsert_exec(self, *args, **kwargs):
        return self._write('upsert_exec', *args, **kwargs)

    def upsert_program_request_transition_with_change(self, *args, **kwargs):
        return self._write('upsert_program_request_transition_with_change', *args, **kwargs)

    def close(self):
        with self._guard:
            self._closed = True
        self._executor.shutdown(wait=True)
