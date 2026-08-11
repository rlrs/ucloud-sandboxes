from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace
import json
import os
from pathlib import Path
import sqlite3
import time
from typing import Any, Iterable, Iterator

from .bootstrap import VmBootstrapRecord
from .models import NodeHeartbeat
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
_TABLE_SQL = """CREATE TABLE control_records (
    namespace TEXT NOT NULL CHECK (namespace IN ('heartbeat', 'bootstrap')),
    record_id TEXT NOT NULL CHECK (length(record_id) > 0),
    payload TEXT NOT NULL,
    PRIMARY KEY (namespace, record_id)
) STRICT, WITHOUT ROWID"""


class ControlStateStore:
    """The gateway/autoscaler authority for heartbeats and VM bootstrap state."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
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
            return self._load_heartbeats(connection)

    def upsert_heartbeat(self, heartbeat: NodeHeartbeat) -> None:
        with self._transaction(write=True) as connection:
            heartbeats = self._load_heartbeats(connection)
            _assert_heartbeat_binding(heartbeats, heartbeat)
            stored, payload = _encode_heartbeat(
                normalize_idle_since(
                    heartbeat,
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
                        or heartbeat.activity_epoch <= previous.activity_epoch
                    ):
                        return HeartbeatReceiptResult(previous, previous, False)
                    if previous.node_epoch:
                        retired_epochs.add(previous.node_epoch)
                elif heartbeat.activity_epoch < previous.activity_epoch:
                    return HeartbeatReceiptResult(previous, previous, False)
            stored, payload = _encode_heartbeat(
                normalize_idle_since(
                    replace(
                        heartbeat,
                        retired_node_epochs=tuple(sorted(retired_epochs)),
                    ),
                    previous=previous,
                )
            )
            self._upsert(connection, "heartbeat", stored.job_id, payload)
            return HeartbeatReceiptResult(stored, previous, True)

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

    @classmethod
    def _load_heartbeats(cls, connection) -> dict[str, NodeHeartbeat]:
        result = {}
        for job_id, payload in cls._records(connection, "heartbeat"):
            try:
                raw = json.loads(payload)
            except (TypeError, json.JSONDecodeError) as exc:
                raise ValueError("invalid heartbeat control-state record") from exc
            heartbeat = heartbeat_from_dict(raw) if isinstance(raw, dict) else None
            if (
                heartbeat is None
                or heartbeat.job_id != job_id
                or _json(heartbeat_to_dict(heartbeat)) != payload
            ):
                raise ValueError("invalid heartbeat control-state record")
            _assert_heartbeat_binding(result, heartbeat)
            result[job_id] = heartbeat
        return result

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
            connection = sqlite3.connect(self.path, timeout=30, isolation_level=None)
            connection.execute("PRAGMA busy_timeout = 30000")
            connection.execute("PRAGMA synchronous = FULL")
            return connection
        except sqlite3.Error as exc:
            raise ValueError(_ERROR) from exc

    @contextmanager
    def _transaction(self, *, write: bool) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE" if write else "BEGIN")
            self._secure_files()
            yield connection
            connection.commit()
        except BaseException as exc:
            if connection.in_transaction:
                connection.rollback()
            _reraise(exc)
        finally:
            connection.close()

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
