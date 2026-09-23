"""Isolate bulk fleet rendering from latency-sensitive gateway threads."""

import multiprocessing
from pathlib import Path
from threading import Lock


def _serve(connection, control_path, routing_path, ttl):
    # Spawn, never fork a multithreaded gateway or inherit SQLite connections.
    from .control_plane import _sandbox_list_bytes
    from .control_state import ControlStateStore
    from .routing import RoutingStore

    try:
        control = ControlStateStore(Path(control_path))
        routing = RoutingStore(Path(routing_path))
        while connection.recv_bytes() == b'read':
            try:
                payload = _sandbox_list_bytes(control, routing, ttl)
            except Exception:
                # No partial/stale snapshot and no database contents in errors.
                connection.send_bytes(b'error')
            else:
                connection.send_bytes(b'ok' + payload)
    except (EOFError, OSError):
        pass
    finally:
        connection.close()


class FleetSnapshotReader:
    """One fresh read per call; concurrent HTTP calls coalesce above this layer.

    An anonymous pipe carries only fixed commands and JSON bytes. The child has
    no network listener, request token, lifecycle authority, or response cache.
    A failed read is retried in a fresh process, never served from stale data.
    """

    def __init__(self, control_path, routing_path, ttl):
        self._args = (str(control_path), str(routing_path), ttl)
        self._guard = Lock()
        self._process = None
        self._connection = None
        self._closed = False

    def _start(self):
        context = multiprocessing.get_context('spawn')
        parent, child = context.Pipe()
        process = context.Process(target=_serve, args=(child, *self._args),
                                  name='gateway-fleet-reader', daemon=True)
        try:
            process.start()
        except BaseException:
            parent.close()
            child.close()
            raise
        child.close()
        self._connection, self._process = parent, process

    def _reset(self):
        if self._connection is not None:
            self._connection.close()
            self._connection = None
        if self._process is not None:
            process, self._process = self._process, None
            process.join(timeout=.2)
            if process.is_alive():
                process.terminate()
                process.join(timeout=2)
            process.close()

    def read(self):
        with self._guard:
            if self._closed:
                raise RuntimeError('fleet snapshot reader is closed')
            for attempt in range(2):
                try:
                    if self._process is None:
                        self._start()
                    self._connection.send_bytes(b'read')
                    if not self._connection.poll(30):
                        raise TimeoutError('fleet snapshot read timed out')
                    result = self._connection.recv_bytes()
                except (EOFError, OSError, TimeoutError):
                    self._reset()
                    if attempt:
                        raise RuntimeError('fleet snapshot reader unavailable') from None
                    continue
                if not result.startswith(b'ok'):
                    raise ValueError('fleet snapshot state is unreadable')
                return result[2:]

    def close(self):
        with self._guard:
            self._closed = True
            if self._connection is not None:
                try:
                    self._connection.send_bytes(b'close')
                except (OSError, EOFError):
                    pass
            self._reset()
