from __future__ import annotations

from contextlib import contextmanager
from collections import OrderedDict
from dataclasses import replace
import json
import os
from pathlib import Path
import sqlite3
import stat
from threading import Lock
import time
from typing import Any, Iterable, Iterator
import weakref

from .bootstrap import VmBootstrapRecord
from .models import NODE_RUNTIME_METRIC_DEFAULTS, NodeHeartbeat
from .registry import (
    HeartbeatReceiptResult,
    _assert_heartbeat_binding,
    heartbeat_from_dict,
    heartbeat_to_dict,
    normalize_idle_since,
)


_APPLICATION_ID = 0x55435331  # UCS1
_SCHEMA_VERSION = 1
_ERROR = "control state is unreadable"
# These bound retained decoding work, not fleet size or admission. SQLite is
# still read on every lookup; only identical validated payloads can be reused.
_HEARTBEAT_CACHE_ENTRIES = 64
_HEARTBEAT_CACHE_BYTES = 16 * 1024**2
# Controller-owned metadata uses the existing extensible heartbeat labels so
# workers and the durable heartbeat schema remain wire-compatible.
QUARANTINE_REASON = "ucloud-sandboxes/controller-quarantine"
QUARANTINE_EPOCH = "ucloud-sandboxes/controller-quarantine-epoch"
_QUARANTINE_KEYS = {QUARANTINE_REASON, QUARANTINE_EPOCH}


def _placement_heartbeat(heartbeat: NodeHeartbeat) -> NodeHeartbeat:
    return (
        replace(heartbeat, admission_open=False)
        if heartbeat.labels.get(QUARANTINE_REASON)
        else heartbeat
    )


def _controller_labels(heartbeat: NodeHeartbeat, previous: NodeHeartbeat | None):
    labels = {k: v for k, v in heartbeat.labels.items() if k not in _QUARANTINE_KEYS}
    if previous is not None:
        labels.update(
            {k: v for k, v in previous.labels.items() if k in _QUARANTINE_KEYS}
        )
    return replace(heartbeat, labels=labels)


_TABLE_SQL = """CREATE TABLE control_records (
    namespace TEXT NOT NULL CHECK (namespace IN ('heartbeat', 'bootstrap')),
    record_id TEXT NOT NULL CHECK (length(record_id) > 0),
    payload TEXT NOT NULL,
    PRIMARY KEY (namespace, record_id)
) STRICT, WITHOUT ROWID"""


def _close_control_connections(connections, guard):
    with guard:
        while connections:
            connections.pop().close()


class ControlStateStore:
    """The gateway/autoscaler authority for heartbeats and VM bootstrap state."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self._heartbeat_cache: OrderedDict[str, tuple[str, NodeHeartbeat]] = OrderedDict()
        self._heartbeat_cache_bytes = 0
        self._heartbeat_cache_lock = Lock()
        self._connections: list[sqlite3.Connection] = []
        self._connections_guard = Lock()
        self._connection_pid = os.getpid()
        self._connection_identity: tuple[int, int] | None = None
        self._connection_finalizer = weakref.finalize(
            self, _close_control_connections, self._connections, self._connections_guard,
        )
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._prepare_file()
        connection = self._connect()
        try:
            if self._enable_wal(connection) != ("wal",):
                raise sqlite3.DatabaseError("control state requires WAL")
        except BaseException as exc:
            _reraise(exc)
        finally:
            connection.close()
        with self._transaction(write=True) as connection:
            identity = (
                connection.execute("PRAGMA application_id").fetchone()[0],
                connection.execute("PRAGMA user_version").fetchone()[0],
            )
            objects = dict(
                connection.execute(
                    "SELECT name, sql FROM sqlite_schema "
                    "WHERE type IN ('table', 'index', 'view', 'trigger') "
                    "AND name NOT LIKE 'sqlite_%'"
                )
            )
            if identity == (0, 0) and not objects:
                connection.execute(_TABLE_SQL)
                connection.execute(f"PRAGMA application_id = {_APPLICATION_ID}")
                connection.execute(f"PRAGMA user_version = {_SCHEMA_VERSION}")
                identity = (_APPLICATION_ID, _SCHEMA_VERSION)
                objects = {"control_records": _TABLE_SQL}
            if identity != (_APPLICATION_ID, _SCHEMA_VERSION) or objects != {
                "control_records": _TABLE_SQL
            }:
                raise ValueError("unsupported control state schema")
        self._secure_files()

    def load_heartbeats(self) -> dict[str, NodeHeartbeat]:
        with self._transaction(write=False) as connection:
            return {
                k: _placement_heartbeat(v)
                for k, v in self._load_heartbeats(connection).items()
            }

    def get_heartbeat(
        self, job_id: str, *, include_inventory: bool = True,
    ) -> NodeHeartbeat | None:
        """Load one heartbeat without decoding every node inventory."""

        if not job_id:
            return None
        # A single indexed SELECT is already a SQLite snapshot. Explicit
        # BEGIN/COMMIT and repeated WAL permission stats add several GIL
        # handoffs to every routed request without strengthening this read.
        with self._connection() as connection:
            row = connection.execute(
                "SELECT payload FROM control_records "
                "WHERE namespace = 'heartbeat' AND record_id = ?",
                (job_id,),
            ).fetchone()
            if row is None:
                return None
            return _placement_heartbeat(self._read_heartbeat(
                job_id, row[0], include_inventory=include_inventory,
            ))

    def quarantine_node(self, job_id: str, reason: str) -> NodeHeartbeat | None:
        """Close placement durably without discarding authenticated inventory."""
        with self._transaction(write=True) as connection:
            current = self._load_heartbeats(connection).get(job_id)
            if current is None:
                return None
            labels = dict(current.labels)
            labels.setdefault(QUARANTINE_EPOCH, current.node_epoch)
            labels[QUARANTINE_REASON] = reason
            stored, payload = _encode_heartbeat(replace(current, labels=labels))
            self._upsert(connection, "heartbeat", job_id, payload)
            return _placement_heartbeat(stored)

    def recover_quarantined_node(self, heartbeat: NodeHeartbeat) -> bool:
        """Commit verified continuity only if no newer boot/revision intervened."""
        with self._transaction(write=True) as connection:
            current = self._load_heartbeats(connection).get(heartbeat.job_id)
            if current is None or (
                (current.node_id, current.node_epoch, current.deployment_id)
                != (heartbeat.node_id, heartbeat.node_epoch, heartbeat.deployment_id)
                or current.activity_epoch > heartbeat.activity_epoch
                or current.freshness_at > heartbeat.freshness_at
            ):
                return False
            labels = {
                k: v for k, v in heartbeat.labels.items() if k not in _QUARANTINE_KEYS
            }
            stored, payload = _encode_heartbeat(
                replace(
                    heartbeat,
                    labels=labels,
                    retired_node_epochs=current.retired_node_epochs,
                )
            )
            self._upsert(connection, "heartbeat", stored.job_id, payload)
            return True

    def upsert_heartbeat(self, heartbeat: NodeHeartbeat) -> None:
        with self._transaction(write=True) as connection:
            heartbeats = self._load_heartbeats(connection)
            _assert_heartbeat_binding(heartbeats, heartbeat)
            stored, payload = _encode_heartbeat(
                normalize_idle_since(
                    _controller_labels(heartbeat, heartbeats.get(heartbeat.job_id)),
                    previous=heartbeats.get(heartbeat.job_id),
                )
            )
            self._upsert(connection, "heartbeat", stored.job_id, payload)

    def receive_heartbeat(self, heartbeat: NodeHeartbeat) -> HeartbeatReceiptResult:
        if heartbeat.received_at is None:
            raise ValueError("received heartbeat requires a gateway receipt timestamp")
        with self._transaction(write=True) as connection:
            heartbeats = self._load_heartbeats(connection)
            previous = heartbeats.get(heartbeat.job_id)
            _assert_heartbeat_binding(heartbeats, heartbeat)
            if (
                previous is not None
                and previous.received_at is not None
                and heartbeat.received_at < previous.received_at
            ):
                return HeartbeatReceiptResult(previous, previous, False)
            retired_epochs = set(previous.retired_node_epochs if previous else ())
            if previous is not None:
                if heartbeat.node_epoch != previous.node_epoch:
                    if (
                        not heartbeat.node_epoch
                        or heartbeat.node_epoch in retired_epochs
                    ):
                        return HeartbeatReceiptResult(previous, previous, False)
                    # Activity counters are scoped to one node boot. A fresh
                    # boot may legitimately restart its durable/transient
                    # revision at the same or a lower value, so comparing it
                    # with the retired boot's counter would permanently fence
                    # a healthy restarted guest. Epoch retirement, rather
                    # than a cross-epoch counter comparison, prevents an old
                    # boot from returning after the new one is accepted.
                    if previous.node_epoch:
                        retired_epochs.add(previous.node_epoch)
                elif heartbeat.activity_epoch < previous.activity_epoch:
                    return HeartbeatReceiptResult(previous, previous, False)
            stored, payload = _encode_heartbeat(
                normalize_idle_since(
                    replace(
                        _controller_labels(heartbeat, previous),
                        retired_node_epochs=tuple(sorted(retired_epochs)),
                    ),
                    previous=previous,
                )
            )
            self._upsert(connection, "heartbeat", stored.job_id, payload)
            return HeartbeatReceiptResult(_placement_heartbeat(stored), previous, True)

    def remove_heartbeats(
        self,
        job_ids: Iterable[str],
    ) -> dict[str, NodeHeartbeat]:
        target_ids = {str(job_id) for job_id in job_ids if str(job_id)}
        if not target_ids:
            return {}
        with self._transaction(write=True) as connection:
            heartbeats = self._load_heartbeats(connection)
            removed = {
                job_id: heartbeats[job_id]
                for job_id in sorted(target_ids)
                if job_id in heartbeats
            }
            connection.executemany(
                "DELETE FROM control_records "
                "WHERE namespace = 'heartbeat' AND record_id = ?",
                ((job_id,) for job_id in removed),
            )
            return removed

    def load_bootstrap_records(self) -> dict[str, VmBootstrapRecord]:
        with self._transaction(write=False) as connection:
            result = {}
            for job_id, payload in self._records(connection, "bootstrap"):
                try:
                    record = VmBootstrapRecord.from_dict(json.loads(payload))
                except (TypeError, ValueError, json.JSONDecodeError) as exc:
                    raise ValueError("invalid bootstrap control-state record") from exc
                if record.job_id != job_id or _json(record.to_dict()) != payload:
                    raise ValueError("invalid bootstrap control-state record")
                result[job_id] = record
            return result

    def save_bootstrap_records(
        self,
        records: dict[str, VmBootstrapRecord],
    ) -> None:
        encoded = []
        for job_id, record in records.items():
            if not isinstance(record, VmBootstrapRecord):
                raise ValueError("bootstrap state values must be bootstrap records")
            parsed = VmBootstrapRecord.from_dict(record.to_dict())
            if parsed.job_id != job_id:
                raise ValueError("bootstrap state key does not match its record")
            encoded.append((job_id, _json(parsed.to_dict())))
        with self._transaction(write=True) as connection:
            connection.execute(
                "DELETE FROM control_records WHERE namespace = 'bootstrap'"
            )
            connection.executemany(
                "INSERT INTO control_records (namespace, record_id, payload) "
                "VALUES (?, ?, ?)",
                (("bootstrap", job_id, payload) for job_id, payload in encoded),
            )

    @staticmethod
    def _records(connection, namespace):
        return connection.execute(
            "SELECT record_id, payload FROM control_records "
            "WHERE namespace = ? ORDER BY record_id",
            (namespace,),
        )

    def _load_heartbeats(self, connection) -> dict[str, NodeHeartbeat]:
        result = {}
        for job_id, payload in self._records(connection, "heartbeat"):
            heartbeat = self._read_heartbeat(job_id, payload)
            _assert_heartbeat_binding(result, heartbeat)
            result[job_id] = heartbeat
        return result

    def _read_heartbeat(
        self, job_id: str, payload: str, *, include_inventory: bool = True,
    ) -> NodeHeartbeat:
        with self._heartbeat_cache_lock:
            cached = self._heartbeat_cache.get(job_id)
            if cached is not None and cached[0] == payload:
                heartbeat = cached[1]
                self._heartbeat_cache.move_to_end(job_id)
            else:
                # Validate before caching, including canonical encoding and job
                # identity. External writers, quarantine and reboot fences are
                # visible immediately because the key includes the actual row.
                heartbeat = self._decode_heartbeat(job_id, payload)
                if cached is not None:
                    self._heartbeat_cache_bytes -= len(cached[0])
                    del self._heartbeat_cache[job_id]
                # Canonical payloads use JSON's ASCII escaping, so character
                # length is also their encoded byte length.
                if len(payload) <= _HEARTBEAT_CACHE_BYTES:
                    self._heartbeat_cache[job_id] = (payload, heartbeat)
                    self._heartbeat_cache_bytes += len(payload)
                    while (
                        len(self._heartbeat_cache) > _HEARTBEAT_CACHE_ENTRIES
                        or self._heartbeat_cache_bytes > _HEARTBEAT_CACHE_BYTES
                    ):
                        _, (evicted, _) = self._heartbeat_cache.popitem(last=False)
                        self._heartbeat_cache_bytes -= len(evicted)
        # Frozen dataclasses still contain mutable labels/snapshot descriptors.
        # Never expose cached dictionaries to a caller. Immutable scalar and
        # resource fields can be shared without revalidating the inventory.
        return replace(
            heartbeat,
            labels=dict(heartbeat.labels),
            inventory=tuple(
                replace(
                    entry,
                    storage_snapshot=_copy_json_value(entry.storage_snapshot),
                    storage_dependency=_copy_json_value(entry.storage_dependency),
                )
                for entry in heartbeat.inventory
            ) if include_inventory else (),
            # A header-only read must never be interpreted as proof that the
            # worker has no sandboxes. Validation above still covers the full
            # durable row, including inventory and canonical encoding.
            inventory_complete=heartbeat.inventory_complete if include_inventory else False,
        )

    @staticmethod
    def _decode_heartbeat(job_id: str, payload: str) -> NodeHeartbeat:
        try:
            raw = json.loads(payload)
        except (TypeError, json.JSONDecodeError) as exc:
            raise ValueError("invalid heartbeat control-state record") from exc
        heartbeat = heartbeat_from_dict(raw) if isinstance(raw, dict) else None
        if (
            heartbeat is None
            or heartbeat.job_id != job_id
            or not _heartbeat_payload_is_canonical(raw, heartbeat, payload)
        ):
            raise ValueError("invalid heartbeat control-state record")
        return heartbeat

    @staticmethod
    def _upsert(connection, namespace, record_id, payload) -> None:
        connection.execute(
            "INSERT INTO control_records (namespace, record_id, payload) "
            "VALUES (?, ?, ?) ON CONFLICT(namespace, record_id) "
            "DO UPDATE SET payload = excluded.payload",
            (namespace, record_id, payload),
        )

    def _connect(self) -> sqlite3.Connection:
        try:
            connection = sqlite3.connect(
                self.path, timeout=30, isolation_level=None, check_same_thread=False,
            )
            connection.execute("PRAGMA busy_timeout = 30000")
            connection.execute("PRAGMA synchronous = FULL")
            self._secure_files()
            return connection
        except sqlite3.Error as exc:
            raise ValueError(_ERROR) from exc

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        connection = None
        reusable = False
        try:
            # Filesystem calls release the GIL and can block. They must not
            # hold up another reader returning its connection to the pool.
            if os.getpid() != self._connection_pid:
                raise sqlite3.DatabaseError("reopen control state after fork")
            info = self.path.stat()
            identity = (info.st_dev, info.st_ino)
            with self._connections_guard:
                if self._connection_identity is not None and identity != self._connection_identity:
                    raise sqlite3.DatabaseError("control state database file was replaced")
                self._connection_identity = identity
                if self._connections:
                    connection = self._connections.pop()
            if stat.S_IMODE(info.st_mode) != 0o600:
                os.chmod(self.path, 0o600)
            if connection is None:
                connection = self._connect()
            yield connection
            if connection.in_transaction:
                connection.rollback()
            reusable = True
        except BaseException as exc:
            _reraise(exc)
        finally:
            if connection is not None:
                with self._connections_guard:
                    # Bound idle retention, never concurrent readers. Every
                    # caller owns its connection until its snapshot is closed.
                    if reusable and len(self._connections) < 16:
                        self._connections.append(connection)
                        connection = None
                if connection is not None:
                    connection.close()

    @contextmanager
    def _transaction(self, *, write: bool) -> Iterator[sqlite3.Connection]:
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE" if write else "BEGIN")
                self._secure_files()
                yield connection
                connection.commit()
            except BaseException:
                if connection.in_transaction:
                    connection.rollback()
                raise

    def _prepare_file(self) -> None:
        flags = os.O_RDWR | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        descriptor = None
        try:
            descriptor = os.open(self.path, flags | os.O_CREAT, 0o600)
            os.fchmod(descriptor, 0o600)
        except OSError as exc:
            raise ValueError(_ERROR) from exc
        finally:
            if descriptor is not None:
                os.close(descriptor)

    def _secure_files(self) -> None:
        for suffix in ("", "-wal", "-shm"):
            path = Path(f"{self.path}{suffix}")
            try:
                if stat.S_IMODE(path.lstat().st_mode) != 0o600:
                    os.chmod(path, 0o600, follow_symlinks=False)
            except FileNotFoundError:
                pass

    @staticmethod
    def _enable_wal(connection: sqlite3.Connection) -> tuple[Any, ...] | None:
        deadline = time.monotonic() + 30
        while True:
            try:
                return connection.execute("PRAGMA journal_mode = WAL").fetchone()
            except sqlite3.OperationalError as exc:
                if (
                    not any(word in str(exc).lower() for word in ("busy", "locked"))
                    or time.monotonic() >= deadline
                ):
                    raise
                time.sleep(0.01)


def _copy_json_value(value: Any) -> Any:
    """Detach containers from validated JSON; scalar leaves are immutable.

    These values came from json.loads, so they cannot contain cycles or custom
    Python objects that require deepcopy's memoization/reconstruction machinery.
    """
    if isinstance(value, dict):
        return {key: _copy_json_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_copy_json_value(item) for item in value]
    return value


def _heartbeat_payload_is_canonical(
    raw: dict[str, Any],
    heartbeat: NodeHeartbeat,
    payload: str,
) -> bool:
    encoded = heartbeat_to_dict(heartbeat)
    if _json(encoded) == payload:
        return True

    # Accept only the exact canonical legacy representation of additive
    # metrics with their default values. Unknown fields and noncanonical
    # encodings still fail closed; receive rewrites using the current schema.
    runtime_metrics = encoded.get("runtime_metrics")
    raw_metrics = raw.get("runtime_metrics")
    if not isinstance(runtime_metrics, dict) or not isinstance(raw_metrics, dict):
        return False
    legacy = dict(encoded)
    legacy_metrics = dict(runtime_metrics)
    for name, default in NODE_RUNTIME_METRIC_DEFAULTS.items():
        if name not in raw_metrics and legacy_metrics.get(name) == default:
            legacy_metrics.pop(name)
    legacy["runtime_metrics"] = legacy_metrics
    return raw == legacy and _json(raw) == payload


def _encode_heartbeat(heartbeat: NodeHeartbeat) -> tuple[NodeHeartbeat, str]:
    parsed = heartbeat_from_dict(heartbeat_to_dict(heartbeat))
    if parsed is None:
        raise ValueError("heartbeat does not match the current schema")
    return parsed, _json(heartbeat_to_dict(parsed))


def _json(payload: object) -> str:
    return json.dumps(payload, allow_nan=False, separators=(",", ":"), sort_keys=True)


def _reraise(exc: BaseException) -> None:
    if isinstance(exc, (OSError, sqlite3.Error)):
        raise ValueError(_ERROR) from exc
    raise exc
