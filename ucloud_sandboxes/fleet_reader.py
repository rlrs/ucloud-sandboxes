"""Isolate bulk fleet rendering from latency-sensitive gateway threads."""

import multiprocessing
import json
from pathlib import Path
from threading import Lock

MAX_STATUS_IDS = 256


def status_ids(values):
    """Bound and canonicalize exact IDs; every SQL value remains a parameter."""
    if (not isinstance(values, (list, tuple)) or len(values) > MAX_STATUS_IDS
            or any(not isinstance(value, str) or not value or len(value) > 512
                   or "\0" in value for value in values)):
        raise ValueError(f"status filtering requires at most {MAX_STATUS_IDS} nonempty IDs")
    result = tuple(sorted(set(values)))
    if len(json.dumps(result).encode('utf-8')) + len(b'status:') > 256 * 1024:
        raise ValueError("encoded status filter exceeds 256 KiB")
    return result


class FleetResponseRenderer:
    """Reuse encoding work, never database observations.

    Inputs are fresh, privately owned database rows. Compare every
    route field and heartbeat, including clock-driven freshness, before reuse.
    Removed rows leave the cache immediately. The byte budget bounds retained
    encodings only; it never limits the response or fleet size.
    """

    def __init__(self, max_cached_bytes=8 * 1024 * 1024, *, status_only=False):
        self.max_cached_bytes = max_cached_bytes
        self.status_only = status_only
        self._nodes = {}
        self._routes = {}
        self._epoch = 0

    def render(self, rows, heartbeats, ttl):
        from .control_plane import _route_only_sandbox_record
        from .models import utc_now
        from .routing import _sandbox_route_from_row

        now = utc_now()
        nodes = {}
        for node_id, heartbeat in heartbeats.items():
            fresh = heartbeat.is_fresh(now, ttl)
            previous = self._nodes.get(node_id)
            if previous is not None and previous[:2] == (heartbeat, fresh):
                nodes[node_id] = previous
            else:
                self._epoch += 1
                nodes[node_id] = (heartbeat, fresh, self._epoch)
        retained = {}
        encoded = []
        size = 0
        for row in rows:
            node = nodes.get(row["node_id"])
            epoch = node[2] if node is not None else None
            cached = self._routes.get(row["sandbox_id"])
            if cached is not None and cached[0] == row and cached[1] == epoch:
                body = cached[2]
            else:
                projected = row
                if self.status_only:
                    # Validate the same lifecycle/generation/portability fields
                    # without loading or decoding omitted user specifications.
                    projected = dict(row, resources_json='{"vcpu":0,"memory_mb":0,"disk_mb":0}',
                                     spec_json=json.dumps({"id": row["sandbox_id"]}))
                route = _sandbox_route_from_row(projected)
                record = _route_only_sandbox_record(
                    route, node[0] if node is not None else None,
                    heartbeat_ttl_seconds=ttl,
                )
                if self.status_only:
                    record = {key: record[key] for key in (
                        "id", "spec", "state", "cached_state", "node",
                        "created_at", "updated_at",
                    )}
                    record["generation"] = route.generation
                body = json.dumps(record, separators=(",", ":")).encode("utf-8")
            encoded.append(body)
            if size + len(body) <= self.max_cached_bytes:
                retained[row["sandbox_id"]] = (row, epoch, body)
                size += len(body)
        self._nodes, self._routes = nodes, retained
        if self.status_only:
            return (b'{"sandboxes":[' + b','.join(encoded)
                    + b'],"cached":true,"refresh_supported":false,"view":"status"}')
        return b'{"sandboxes":[' + b','.join(encoded) + b'],"cached":true,"refresh_supported":true}'


def _identities(paths):
    return tuple((info.st_dev, info.st_ino) for info in (Path(p).stat() for p in paths))


def _serve(connection, control_path, routing_path, ttl, identities):
    # Spawn, never fork a multithreaded gateway or inherit SQLite connections.
    from .control_plane import _sandbox_list_bytes
    from .control_state import ControlStateStore
    from .routing import open_routing_store

    try:
        paths = (control_path, routing_path)
        if _identities(paths) != identities:
            connection.send_bytes(b'error')
            return
        control = ControlStateStore(Path(control_path))
        routing = open_routing_store(Path(routing_path))
        renderer = FleetResponseRenderer()
        status_renderer = FleetResponseRenderer(status_only=True)
        while True:
            command = connection.recv_bytes(256 * 1024)
            if command == b'close':
                break
            try:
                if _identities(paths) != identities:
                    raise ValueError('fleet state files changed')
                if command == b'read':
                    payload = _sandbox_list_bytes(control, routing, ttl, renderer=renderer)
                elif command.startswith(b'status:'):
                    ids = status_ids(json.loads(command[7:]))
                    payload = _sandbox_list_bytes(control, routing, ttl,
                        renderer=status_renderer, status_only=True, sandbox_ids=ids)
                else:
                    raise ValueError('unknown fleet read command')
                if _identities(paths) != identities:
                    raise ValueError('fleet state files changed')
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

    An anonymous pipe carries read commands, bounded ID filters and JSON bytes. The child has
    no network listener, request token, lifecycle authority, or response cache.
    A failed read is retried in a fresh process, never served from stale data.
    """

    def __init__(self, control_path, routing_path, ttl):
        paths = (str(control_path), str(routing_path))
        self._args = (*paths, ttl, _identities(paths))
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
        return self._read(b'read')

    def read_status(self, sandbox_ids=()):
        command = b'status:' + json.dumps(status_ids(sandbox_ids)).encode('utf-8')
        return self._read(command)

    def _read(self, command):
        with self._guard:
            if self._closed:
                raise RuntimeError('fleet snapshot reader is closed')
            for attempt in range(2):
                try:
                    if self._process is None:
                        self._start()
                    self._connection.send_bytes(command)
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
