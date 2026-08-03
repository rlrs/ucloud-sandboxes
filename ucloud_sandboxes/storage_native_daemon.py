from __future__ import annotations

from contextlib import closing, suppress
from dataclasses import asdict, dataclass, replace
from enum import Enum
import errno
import hashlib
import json
import os
from pathlib import Path
import re
import socket
import socketserver
import sqlite3
import stat
import struct
import subprocess
import shutil
import tempfile
import threading
import time
from typing import Any, Protocol

from .storage_native import AgentEnvUblkClient, StorageNativeDevice
from .storage_native_registry import (
    PublishedStorageLayer,
    StorageSnapshotPublication,
)


_SAFE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,239}\Z")
_PROTOCOL_SCHEMA = 1
_PROTOCOL_MAX_BYTES = 1024 * 1024
_ACTIVE_CAPACITY_STATES = {
    "creating",
    "acquiring",
    "mounted",
    "sealing",
    "sealed",
    "releasing",
    "released",
    "publishing",
    "deleting",
    "error",
}


class StorageNativeNodeError(RuntimeError):
    pass


class StorageNativeConflictError(StorageNativeNodeError):
    pass


class StorageNativePendingOperation(StorageNativeConflictError):
    pass


class StorageNativeTerminalError(StorageNativeNodeError):
    pass


class StorageVolumeState(str, Enum):
    CREATING = "creating"
    IMPORTING = "importing"
    ACQUIRING = "acquiring"
    MOUNTED = "mounted"
    SEALING = "sealing"
    SEALED = "sealed"
    RELEASING = "releasing"
    RELEASED = "released"
    PUBLISHING = "publishing"
    PUBLISHED = "published"
    DELETING = "deleting"
    DELETED = "deleted"
    ERROR = "error"


@dataclass(frozen=True)
class StorageNativeNodeConfig:
    journal_path: Path
    runtime_root: Path
    mount_root: Path
    hard_capacity_bytes: int
    upper_mode: str = "hybridLogStructured"
    command_timeout_seconds: float = 120.0
    max_concurrent_operations: int = 8
    device_pool_enabled: bool = False
    device_pool_low_watermark: int = 2
    device_pool_high_watermark: int = 16

    def __post_init__(self) -> None:
        for label, path in (
            ("journal_path", self.journal_path),
            ("runtime_root", self.runtime_root),
            ("mount_root", self.mount_root),
        ):
            if not path.is_absolute():
                raise ValueError(f"{label} must be absolute")
        if self.hard_capacity_bytes <= 0:
            raise ValueError("hard_capacity_bytes must be positive")
        if self.upper_mode not in {
            "sparse",
            "logStructured",
            "hybridLogStructured",
        }:
            raise ValueError("unsupported overlaybd upper mode")
        if self.command_timeout_seconds <= 0:
            raise ValueError("command timeout must be positive")
        if self.max_concurrent_operations <= 0:
            raise ValueError("max_concurrent_operations must be positive")
        if self.device_pool_low_watermark < 0:
            raise ValueError("device pool low watermark must be non-negative")
        if self.device_pool_high_watermark <= 0:
            raise ValueError("device pool high watermark must be positive")
        if self.device_pool_low_watermark > self.device_pool_high_watermark:
            raise ValueError("device pool low watermark cannot exceed high watermark")


@dataclass(frozen=True)
class StorageVolumeRecord:
    volume_id: str
    sandbox_id: str
    sandbox_generation: int
    revision: int
    state: StorageVolumeState
    operation_id: str
    virtual_size: int
    runtime_dir: str
    mount_path: str
    source_image_config: str
    device_id: int | None = None
    device_path: str = ""
    runtime_image_config: str = ""
    sealed_layer_path: str = ""
    sealed_layer_bytes: int = 0
    sealed_layer_paths: tuple[str, ...] = ()
    cached_layer_paths: tuple[str, ...] = ()
    published_manifest_digest: str = ""
    published_tag: str = ""
    published_repository: str = ""
    published_repo_blob_url: str = ""
    published_layers: tuple[dict[str, Any], ...] = ()
    accounting_id: int = 0
    error: str = ""
    updated_ns: int = 0

    def __post_init__(self) -> None:
        for label, value in (
            ("volume_id", self.volume_id),
            ("sandbox_id", self.sandbox_id),
            ("operation_id", self.operation_id),
        ):
            if not _SAFE_ID.fullmatch(value):
                raise ValueError(f"{label} is invalid")
        if self.sandbox_generation < 0 or self.revision < 0:
            raise ValueError("storage-native generations and revisions must be non-negative")
        if self.virtual_size <= 0:
            raise ValueError("storage-native virtual size must be positive")
        for label, raw in (
            ("runtime_dir", self.runtime_dir),
            ("mount_path", self.mount_path),
            ("source_image_config", self.source_image_config),
        ):
            if not Path(raw).is_absolute():
                raise ValueError(f"{label} must be absolute")
        if self.device_id is not None and self.device_id < 0:
            raise ValueError("device_id must be non-negative")
        for raw in (
            self.device_path,
            self.runtime_image_config,
            self.sealed_layer_path,
        ):
            if raw and not Path(raw).is_absolute():
                raise ValueError("record paths must be absolute")
        if self.sealed_layer_bytes < 0:
            raise ValueError("sealed_layer_bytes must be non-negative")
        if self.accounting_id < 0:
            raise ValueError("accounting_id must be non-negative")
        if any(
            not Path(path).is_absolute()
            for path in (*self.sealed_layer_paths, *self.cached_layer_paths)
        ):
            raise ValueError("sealed layer paths must be absolute")
        if self.published_manifest_digest and not _is_sha256_digest(
            self.published_manifest_digest
        ):
            raise ValueError("published manifest digest is invalid")
        for layer in self.published_layers:
            if (
                not isinstance(layer, dict)
                or not _is_sha256_digest(str(layer.get("digest") or ""))
                or not isinstance(layer.get("size"), int)
                or isinstance(layer.get("size"), bool)
                or int(layer["size"]) <= 0
            ):
                raise ValueError("published layer descriptor is invalid")

    def to_json(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["state"] = self.state.value
        payload["sealed_layer_paths"] = list(self.sealed_layer_paths)
        payload["cached_layer_paths"] = list(self.cached_layer_paths)
        payload["published_layers"] = list(self.published_layers)
        return payload

    @classmethod
    def from_json(cls, raw: dict[str, Any]) -> "StorageVolumeRecord":
        payload = dict(raw)
        payload["state"] = StorageVolumeState(str(payload["state"]))
        payload["sealed_layer_paths"] = tuple(
            payload.get("sealed_layer_paths", ())
        )
        payload["published_layers"] = tuple(payload.get("published_layers", ()))
        payload["cached_layer_paths"] = tuple(
            payload.get("cached_layer_paths", ())
        )
        return cls(**payload)


@dataclass(frozen=True)
class OperationReplay:
    result: dict[str, Any]


class StorageBlockBackend(Protocol):
    def create_runtime_device(
        self,
        *,
        source_image_config: Path,
        global_config: Path,
        runtime_dir: Path,
        virtual_size: int,
        upper_mode: str,
    ) -> StorageNativeDevice: ...

    def restack_snapshot(
        self,
        device_id: int,
        output_layer_path: Path,
    ) -> Any: ...

    def delete(self, device_id: int) -> None: ...

    def release(self, device_id: int) -> None: ...

    def export_dense_layer(
        self,
        *,
        source_layer_path: Path,
        stream_socket_path: Path,
    ) -> Any: ...


class StorageSnapshotPublisher(Protocol):
    def publish(
        self,
        *,
        exporter: Any,
        source_layer_paths: tuple[Path, ...],
        virtual_size: int,
        existing_layers: tuple[PublishedStorageLayer, ...] = (),
    ) -> StorageSnapshotPublication: ...

    def verify(
        self,
        publication: StorageSnapshotPublication,
    ) -> StorageSnapshotPublication: ...


class StorageHostOperations(Protocol):
    def format_xfs(self, device: Path) -> None: ...

    def mount(self, device: Path, target: Path) -> None: ...

    def sync(self, target: Path) -> None: ...

    def freeze(self, target: Path) -> None: ...

    def unfreeze(self, target: Path) -> None: ...

    def unmount(self, target: Path) -> None: ...

    def detach(self, target: Path) -> None: ...

    def is_mounted(self, target: Path) -> bool: ...

    def ublk_device_ids(self) -> set[int]: ...


class LinuxStorageHostOperations:
    def __init__(self, *, timeout_seconds: float = 120.0) -> None:
        self.timeout_seconds = timeout_seconds

    def format_xfs(self, device: Path) -> None:
        self._run(
            "mkfs.xfs",
            "-f",
            "-m",
            "reflink=1",
            "-n",
            "ftype=1",
            str(device),
        )

    def mount(self, device: Path, target: Path) -> None:
        self._run("mount", "-o", "noatime", str(device), str(target))

    def sync(self, target: Path) -> None:
        self._run("sync", "-f", str(target))

    def freeze(self, target: Path) -> None:
        self._run("fsfreeze", "--freeze", str(target))

    def unfreeze(self, target: Path) -> None:
        self._run("fsfreeze", "--unfreeze", str(target))

    def unmount(self, target: Path) -> None:
        self._run("umount", str(target))

    def detach(self, target: Path) -> None:
        self._run("umount", "--lazy", str(target))

    @staticmethod
    def is_mounted(target: Path) -> bool:
        # os.path.ismount() stats the target and returns false when a dead
        # block backend makes the mount root itself return EIO. Mountinfo is
        # the kernel authority and remains readable in exactly that case.
        expected = os.fsencode(str(target))
        try:
            lines = Path("/proc/self/mountinfo").read_bytes().splitlines()
        except OSError:
            return False
        return any(
            len(fields) > 4 and fields[4] == expected
            for fields in (line.split() for line in lines)
        )

    @staticmethod
    def ublk_device_ids() -> set[int]:
        result: set[int] = set()
        root = Path("/sys/class/ublk-char")
        if not root.is_dir():
            return result
        for entry in root.glob("ublkc*"):
            suffix = entry.name.removeprefix("ublkc")
            if suffix.isdigit():
                result.add(int(suffix))
        return result

    def _run(self, *argv: str) -> None:
        result = subprocess.run(
            argv,
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=self.timeout_seconds,
        )
        if result.returncode != 0:
            detail = result.stderr.strip() or result.stdout.strip()
            raise StorageNativeNodeError(
                f"{argv[0]} failed with {result.returncode}: {detail}"
            )


class StorageNativeJournal:
    _SCHEMA = """
        CREATE TABLE IF NOT EXISTS volumes (
            volume_id TEXT PRIMARY KEY,
            sandbox_id TEXT NOT NULL,
            sandbox_generation INTEGER NOT NULL,
            revision INTEGER NOT NULL,
            state TEXT NOT NULL,
            virtual_size INTEGER NOT NULL,
            record_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS operations (
            operation_id TEXT PRIMARY KEY,
            kind TEXT NOT NULL,
            request_sha256 TEXT NOT NULL,
            volume_id TEXT NOT NULL,
            status TEXT NOT NULL,
            result_json TEXT NOT NULL DEFAULT '',
            error TEXT NOT NULL DEFAULT ''
        );
    """

    def __init__(self, path: Path) -> None:
        if not path.is_absolute():
            raise ValueError("storage-native journal path must be absolute")
        self.path = path
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        mode = self.path.parent.stat().st_mode
        if mode & 0o022:
            raise StorageNativeNodeError(
                "storage-native journal parent cannot be group/world writable"
            )
        with closing(self._connect()) as connection:
            connection.executescript(self._SCHEMA)

    def reserve_create(
        self,
        *,
        request: dict[str, Any],
        record: StorageVolumeRecord,
        hard_capacity_bytes: int,
    ) -> StorageVolumeRecord | OperationReplay:
        request_sha256 = _request_sha256(request)
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            replay = self._operation_replay(
                connection,
                operation_id=record.operation_id,
                kind="CreateVolume",
                request_sha256=request_sha256,
            )
            if replay is not None:
                return replay
            existing = connection.execute(
                "SELECT 1 FROM volumes WHERE volume_id = ?",
                (record.volume_id,),
            ).fetchone()
            if existing is not None:
                raise StorageNativeConflictError("volume_id already exists")
            reserved = connection.execute(
                (
                    "SELECT COALESCE(SUM(virtual_size), 0) FROM volumes "
                    f"WHERE state IN ({','.join('?' for _ in _ACTIVE_CAPACITY_STATES)})"
                ),
                tuple(sorted(_ACTIVE_CAPACITY_STATES)),
            ).fetchone()[0]
            if int(reserved) + record.virtual_size > hard_capacity_bytes:
                raise StorageNativeConflictError(
                    "storage-native hard capacity is exhausted"
                )
            self._insert_operation(
                connection,
                record.operation_id,
                "CreateVolume",
                request_sha256,
                record.volume_id,
            )
            self._upsert_record(connection, record)
            connection.commit()
        return record

    def reserve_import(
        self,
        *,
        request: dict[str, Any],
        record: StorageVolumeRecord,
    ) -> StorageVolumeRecord | OperationReplay:
        request_sha256 = _request_sha256(request)
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            replay = self._operation_replay(
                connection,
                operation_id=record.operation_id,
                kind="AcquireSnapshot",
                request_sha256=request_sha256,
            )
            if replay is not None:
                return replay
            existing_row = connection.execute(
                "SELECT record_json FROM volumes WHERE volume_id = ?",
                (record.volume_id,),
            ).fetchone()
            if existing_row is not None:
                existing = StorageVolumeRecord.from_json(
                    json.loads(existing_row[0])
                )
                if (
                    existing.state != StorageVolumeState.DELETED
                    or existing.sandbox_id != record.sandbox_id
                    or existing.sandbox_generation
                    != record.sandbox_generation
                ):
                    raise StorageNativeConflictError("volume_id already exists")
                record = replace(
                    record,
                    revision=existing.revision + 1,
                )
            self._insert_operation(
                connection,
                record.operation_id,
                "AcquireSnapshot",
                request_sha256,
                record.volume_id,
            )
            self._upsert_record(connection, record)
            connection.commit()
        return record

    def begin_transition(
        self,
        *,
        request: dict[str, Any],
        operation_id: str,
        kind: str,
        volume_id: str,
        sandbox_id: str,
        sandbox_generation: int,
        expected_revision: int,
        allowed_states: set[StorageVolumeState],
        next_state: StorageVolumeState,
        reserve_capacity: bool = False,
        hard_capacity_bytes: int = 0,
    ) -> StorageVolumeRecord | OperationReplay:
        request_sha256 = _request_sha256(request)
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            replay = self._operation_replay(
                connection,
                operation_id=operation_id,
                kind=kind,
                request_sha256=request_sha256,
            )
            if replay is not None:
                return replay
            record = self._load(connection, volume_id)
            self._fence(
                record,
                sandbox_id=sandbox_id,
                sandbox_generation=sandbox_generation,
                expected_revision=expected_revision,
                allowed_states=allowed_states,
            )
            if reserve_capacity:
                if hard_capacity_bytes <= 0:
                    raise ValueError("hard capacity is required for reservation")
                reserved = connection.execute(
                    (
                        "SELECT COALESCE(SUM(virtual_size), 0) FROM volumes "
                        f"WHERE state IN ({','.join('?' for _ in _ACTIVE_CAPACITY_STATES)})"
                    ),
                    tuple(sorted(_ACTIVE_CAPACITY_STATES)),
                ).fetchone()[0]
                if int(reserved) + record.virtual_size > hard_capacity_bytes:
                    raise StorageNativeConflictError(
                        "storage-native hard capacity is exhausted"
                    )
            pending = replace(
                record,
                revision=record.revision + 1,
                state=next_state,
                operation_id=operation_id,
                error="",
                updated_ns=time.time_ns(),
            )
            self._insert_operation(
                connection,
                operation_id,
                kind,
                request_sha256,
                volume_id,
            )
            self._upsert_record(connection, pending)
            connection.commit()
        return pending

    def update_pending(
        self,
        record: StorageVolumeRecord,
    ) -> None:
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            current = self._load(connection, record.volume_id)
            if (
                current.revision != record.revision
                or current.operation_id != record.operation_id
            ):
                raise StorageNativeConflictError(
                    "storage-native pending operation lost its fence"
                )
            self._upsert_record(connection, record)
            connection.commit()

    def finish(
        self,
        record: StorageVolumeRecord,
        result: dict[str, Any],
    ) -> None:
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            current = self._load(connection, record.volume_id)
            if (
                current.revision != record.revision
                or current.operation_id != record.operation_id
            ):
                raise StorageNativeConflictError(
                    "storage-native operation lost its completion fence"
                )
            updated = replace(record, updated_ns=time.time_ns())
            self._upsert_record(connection, updated)
            changed = connection.execute(
                (
                    "UPDATE operations SET status = 'completed', result_json = ? "
                    "WHERE operation_id = ? AND status = 'pending'"
                ),
                (_canonical_json(result), record.operation_id),
            ).rowcount
            if changed != 1:
                raise StorageNativeConflictError(
                    "storage-native operation is not pending"
                )
            connection.commit()

    def fail(self, record: StorageVolumeRecord, error: str) -> None:
        terminal = replace(
            record,
            state=StorageVolumeState.ERROR,
            error=error[:4096],
            updated_ns=time.time_ns(),
        )
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            current = self._load(connection, record.volume_id)
            if (
                current.revision != record.revision
                or current.operation_id != record.operation_id
            ):
                raise StorageNativeConflictError(
                    "storage-native operation lost its failure fence"
                )
            self._upsert_record(connection, terminal)
            connection.execute(
                (
                    "UPDATE operations SET status = 'failed', error = ? "
                    "WHERE operation_id = ? AND status = 'pending'"
                ),
                (terminal.error, record.operation_id),
            )
            connection.commit()

    def fail_transition(
        self,
        record: StorageVolumeRecord,
        *,
        fallback_state: StorageVolumeState,
        error: str,
    ) -> StorageVolumeRecord:
        recovered = replace(
            record,
            state=fallback_state,
            error=error[:4096],
            updated_ns=time.time_ns(),
        )
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            current = self._load(connection, record.volume_id)
            if (
                current.revision != record.revision
                or current.operation_id != record.operation_id
            ):
                raise StorageNativeConflictError(
                    "storage-native operation lost its failure fence"
                )
            self._upsert_record(connection, recovered)
            changed = connection.execute(
                (
                    "UPDATE operations SET status = 'failed', error = ? "
                    "WHERE operation_id = ? AND status = 'pending'"
                ),
                (recovered.error, record.operation_id),
            ).rowcount
            if changed != 1:
                raise StorageNativeConflictError(
                    "storage-native operation is not pending"
                )
            connection.commit()
        return recovered

    def load(self, volume_id: str) -> StorageVolumeRecord | None:
        with closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT record_json FROM volumes WHERE volume_id = ?",
                (volume_id,),
            ).fetchone()
        if row is None:
            return None
        return StorageVolumeRecord.from_json(json.loads(row[0]))

    def list(self) -> tuple[StorageVolumeRecord, ...]:
        with closing(self._connect()) as connection:
            rows = connection.execute(
                "SELECT record_json FROM volumes ORDER BY volume_id"
            ).fetchall()
        return tuple(StorageVolumeRecord.from_json(json.loads(row[0])) for row in rows)

    def mark_reconcile_error(
        self,
        record: StorageVolumeRecord,
        error: str,
    ) -> StorageVolumeRecord:
        updated = replace(
            record,
            revision=record.revision + 1,
            state=StorageVolumeState.ERROR,
            operation_id=f"reconcile:{record.revision + 1}",
            error=error[:4096],
            updated_ns=time.time_ns(),
        )
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            current = self._load(connection, record.volume_id)
            if current.revision != record.revision:
                raise StorageNativeConflictError("record changed during reconciliation")
            self._upsert_record(connection, updated)
            if record.operation_id:
                connection.execute(
                    (
                        "UPDATE operations SET status = 'failed', error = ? "
                        "WHERE operation_id = ? AND status = 'pending'"
                    ),
                    (updated.error, record.operation_id),
                )
            connection.commit()
        return updated

    def _operation_replay(
        self,
        connection: sqlite3.Connection,
        *,
        operation_id: str,
        kind: str,
        request_sha256: str,
    ) -> OperationReplay | None:
        row = connection.execute(
            (
                "SELECT kind, request_sha256, status, result_json, error "
                "FROM operations WHERE operation_id = ?"
            ),
            (operation_id,),
        ).fetchone()
        if row is None:
            return None
        if row[0] != kind or row[1] != request_sha256:
            raise StorageNativeConflictError(
                "operation_id was reused for a different request"
            )
        if row[2] == "completed":
            return OperationReplay(json.loads(row[3]))
        if row[2] == "failed":
            raise StorageNativeTerminalError(row[4] or "storage operation failed")
        raise StorageNativePendingOperation(
            "operation is pending reconciliation; it will not be replayed blindly"
        )

    @staticmethod
    def _insert_operation(
        connection: sqlite3.Connection,
        operation_id: str,
        kind: str,
        request_sha256: str,
        volume_id: str,
    ) -> None:
        connection.execute(
            (
                "INSERT INTO operations "
                "(operation_id, kind, request_sha256, volume_id, status) "
                "VALUES (?, ?, ?, ?, 'pending')"
            ),
            (operation_id, kind, request_sha256, volume_id),
        )

    @staticmethod
    def _upsert_record(
        connection: sqlite3.Connection,
        record: StorageVolumeRecord,
    ) -> None:
        connection.execute(
            """
            INSERT INTO volumes (
                volume_id, sandbox_id, sandbox_generation, revision,
                state, virtual_size, record_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(volume_id) DO UPDATE SET
                sandbox_id = excluded.sandbox_id,
                sandbox_generation = excluded.sandbox_generation,
                revision = excluded.revision,
                state = excluded.state,
                virtual_size = excluded.virtual_size,
                record_json = excluded.record_json
            """,
            (
                record.volume_id,
                record.sandbox_id,
                record.sandbox_generation,
                record.revision,
                record.state.value,
                record.virtual_size,
                _canonical_json(record.to_json()),
            ),
        )

    @staticmethod
    def _load(
        connection: sqlite3.Connection,
        volume_id: str,
    ) -> StorageVolumeRecord:
        row = connection.execute(
            "SELECT record_json FROM volumes WHERE volume_id = ?",
            (volume_id,),
        ).fetchone()
        if row is None:
            raise StorageNativeConflictError("storage-native volume does not exist")
        return StorageVolumeRecord.from_json(json.loads(row[0]))

    @staticmethod
    def _fence(
        record: StorageVolumeRecord,
        *,
        sandbox_id: str,
        sandbox_generation: int,
        expected_revision: int,
        allowed_states: set[StorageVolumeState],
    ) -> None:
        if (
            record.sandbox_id != sandbox_id
            or record.sandbox_generation != sandbox_generation
        ):
            raise StorageNativeConflictError(
                "storage-native volume belongs to another sandbox incarnation"
            )
        if record.revision != expected_revision:
            raise StorageNativeConflictError(
                f"stale storage revision {expected_revision}; "
                f"current revision is {record.revision}"
            )
        if record.state not in allowed_states:
            raise StorageNativeConflictError(
                f"storage volume is {record.state.value}, not an allowed state"
            )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.path,
            timeout=30,
            isolation_level=None,
        )
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA synchronous = FULL")
        connection.execute("PRAGMA foreign_keys = ON")
        return connection


class StorageNativeNodeService:
    def __init__(
        self,
        config: StorageNativeNodeConfig,
        *,
        backend: StorageBlockBackend,
        global_config_path: Path,
        host: StorageHostOperations | None = None,
        publisher: StorageSnapshotPublisher | None = None,
    ) -> None:
        if not global_config_path.is_absolute():
            raise ValueError("global_config_path must be absolute")
        self.config = config
        self.backend = backend
        self.global_config_path = global_config_path
        self.host = host or LinuxStorageHostOperations(
            timeout_seconds=config.command_timeout_seconds
        )
        self.publisher = publisher
        self.journal = StorageNativeJournal(config.journal_path)
        self._pool_metrics_lock = threading.Lock()
        self._pool_acquires = 0
        self._pool_reused_acquires = 0
        self._pool_new_acquires = 0
        self._pool_releases = 0
        self._pool_discards = 0
        self._released_device_ids: set[int] = set()
        self._ensure_roots()

    def metrics(self) -> dict[str, Any]:
        records = self.journal.list()
        reserved = sum(
            record.virtual_size
            for record in records
            if record.state.value in _ACTIVE_CAPACITY_STATES
        )
        cache_bytes = 0
        for record in records:
            for raw_path in record.cached_layer_paths:
                try:
                    cache_bytes += Path(raw_path).stat().st_size
                except OSError:
                    continue
        live_device_ids = self.host.ublk_device_ids()
        active_device_ids = {
            record.device_id
            for record in records
            if record.device_id is not None
        }
        idle_device_ids = (
            live_device_ids - active_device_ids
            if self.config.device_pool_enabled
            else set()
        )
        with self._pool_metrics_lock:
            pool_metrics = {
                "device_pool_acquires": self._pool_acquires,
                "device_pool_reused_acquires": self._pool_reused_acquires,
                "device_pool_new_acquires": self._pool_new_acquires,
                "device_pool_releases": self._pool_releases,
                "device_pool_discards": self._pool_discards,
            }
        return {
            "cache_bytes": cache_bytes,
            "device_pool_enabled": self.config.device_pool_enabled,
            "device_pool_low_watermark": (
                self.config.device_pool_low_watermark
            ),
            "device_pool_high_watermark": (
                self.config.device_pool_high_watermark
            ),
            "device_pool_idle_devices": len(idle_device_ids),
            "ublk_active_devices": len(active_device_ids & live_device_ids),
            "ublk_live_devices": len(live_device_ids),
            "error_volumes": sum(
                record.state == StorageVolumeState.ERROR for record in records
            ),
            "hard_capacity_bytes": self.config.hard_capacity_bytes,
            "hard_reserved_bytes": reserved,
            "published_volumes": sum(
                record.state == StorageVolumeState.PUBLISHED for record in records
            ),
            "volume_count": len(records),
            **pool_metrics,
        }

    @classmethod
    def from_agentenv(
        cls,
        config: StorageNativeNodeConfig,
        *,
        backend_socket: Path,
        global_config_path: Path,
    ) -> "StorageNativeNodeService":
        return cls(
            config,
            backend=AgentEnvUblkClient(backend_socket),
            global_config_path=global_config_path,
        )

    def create_volume(
        self,
        *,
        sandbox_id: str,
        sandbox_generation: int,
        volume_id: str,
        operation_id: str,
        virtual_size: int,
        accounting_id: int = 0,
    ) -> dict[str, Any]:
        request = {
            "kind": "CreateVolume",
            "operation_id": operation_id,
            "sandbox_generation": sandbox_generation,
            "sandbox_id": sandbox_id,
            "virtual_size": virtual_size,
            "volume_id": volume_id,
            "accounting_id": accounting_id,
        }
        volume_root = self._volume_root(volume_id)
        record = StorageVolumeRecord(
            volume_id=volume_id,
            sandbox_id=sandbox_id,
            sandbox_generation=sandbox_generation,
            revision=1,
            state=StorageVolumeState.CREATING,
            operation_id=operation_id,
            virtual_size=virtual_size,
            runtime_dir=str(volume_root / "runtime"),
            mount_path=str(self.config.mount_root / volume_id),
            source_image_config=str(volume_root / "source.json"),
            accounting_id=accounting_id,
            updated_ns=time.time_ns(),
        )

        reserved = self.journal.reserve_create(
            request=request,
            record=record,
            hard_capacity_bytes=self.config.hard_capacity_bytes,
        )
        if isinstance(reserved, OperationReplay):
            return reserved.result
        try:
            volume_root.mkdir(mode=0o700, parents=True, exist_ok=False)
            runtime_dir = Path(record.runtime_dir)
            mount_path = Path(record.mount_path)
            mount_path.mkdir(mode=0o700)
            source = Path(record.source_image_config)
            _atomic_write_json(
                source,
                {"lowers": [], "resultFile": "", "upper": {}},
            )
            device = self._acquire_runtime_device(
                source_image_config=source,
                runtime_dir=runtime_dir,
                virtual_size=virtual_size,
            )
            if device.virtual_size != virtual_size:
                raise StorageNativeTerminalError(
                    "block backend changed the requested virtual size"
                )
            record = replace(
                record,
                device_id=device.device_id,
                device_path=str(device.device_path),
                runtime_image_config=str(device.image_config_path),
                updated_ns=time.time_ns(),
            )
            self.journal.update_pending(record)
            self.host.format_xfs(device.device_path)
            self.host.mount(device.device_path, mount_path)
            record = replace(
                record,
                state=StorageVolumeState.MOUNTED,
                updated_ns=time.time_ns(),
            )
            result = self._record_result(record)
            self.journal.finish(record, result)
            return result
        except BaseException as exc:
            self._best_effort_release(record)
            self.journal.fail(record, f"{type(exc).__name__}: {exc}")
            raise

    def acquire_snapshot(
        self,
        *,
        sandbox_id: str,
        sandbox_generation: int,
        volume_id: str,
        operation_id: str,
        publication_raw: dict[str, Any],
        accounting_id: int = 0,
    ) -> dict[str, Any]:
        if self.publisher is None:
            raise StorageNativeConflictError(
                "durable snapshot acquisition is not configured"
            )
        publication = StorageSnapshotPublication.from_dict(publication_raw)
        publication = self.publisher.verify(publication)
        request = {
            "accounting_id": accounting_id,
            "kind": "AcquireSnapshot",
            "operation_id": operation_id,
            "publication": publication.to_dict(),
            "sandbox_generation": sandbox_generation,
            "sandbox_id": sandbox_id,
            "volume_id": volume_id,
        }
        volume_root = self._volume_root(volume_id)
        record = StorageVolumeRecord(
            volume_id=volume_id,
            sandbox_id=sandbox_id,
            sandbox_generation=sandbox_generation,
            revision=1,
            state=StorageVolumeState.IMPORTING,
            operation_id=operation_id,
            virtual_size=publication.virtual_size,
            runtime_dir=str(volume_root / "runtime"),
            mount_path=str(self.config.mount_root / volume_id),
            source_image_config=str(volume_root / "source.json"),
            sealed_layer_bytes=sum(
                layer.size for layer in publication.layers
            ),
            published_manifest_digest=publication.manifest_digest,
            published_tag=publication.tag,
            published_repository=publication.repository,
            published_repo_blob_url=publication.repo_blob_url,
            published_layers=tuple(
                layer.to_dict() for layer in publication.layers
            ),
            accounting_id=accounting_id,
            updated_ns=time.time_ns(),
        )
        reserved = self.journal.reserve_import(request=request, record=record)
        if isinstance(reserved, OperationReplay):
            return reserved.result
        record = reserved
        try:
            volume_root.mkdir(mode=0o700, parents=True, exist_ok=False)
            Path(record.mount_path).mkdir(mode=0o700)
            _atomic_write_json(
                Path(record.source_image_config),
                {
                    "repoBlobUrl": publication.repo_blob_url,
                    "lowers": [
                        layer.to_dict() for layer in publication.layers
                    ],
                    "resultFile": "",
                    "upper": {},
                },
            )
            record = replace(
                record,
                state=StorageVolumeState.PUBLISHED,
                updated_ns=time.time_ns(),
            )
            result = {
                **self._record_result(record),
                "publication": publication.to_dict(),
            }
            self.journal.finish(record, result)
            return result
        except BaseException as exc:
            if volume_root.exists() and volume_root.is_dir():
                shutil.rmtree(volume_root, ignore_errors=True)
            mount_path = Path(record.mount_path)
            if mount_path.exists():
                with suppress(OSError):
                    mount_path.rmdir()
            self.journal.fail(record, f"{type(exc).__name__}: {exc}")
            raise

    def freeze_and_seal(
        self,
        *,
        sandbox_id: str,
        sandbox_generation: int,
        volume_id: str,
        operation_id: str,
        expected_revision: int,
    ) -> dict[str, Any]:
        request = {
            "expected_revision": expected_revision,
            "kind": "FreezeAndSeal",
            "operation_id": operation_id,
            "sandbox_generation": sandbox_generation,
            "sandbox_id": sandbox_id,
            "volume_id": volume_id,
        }
        pending = self.journal.begin_transition(
            request=request,
            operation_id=operation_id,
            kind="FreezeAndSeal",
            volume_id=volume_id,
            sandbox_id=sandbox_id,
            sandbox_generation=sandbox_generation,
            expected_revision=expected_revision,
            allowed_states={StorageVolumeState.MOUNTED},
            next_state=StorageVolumeState.SEALING,
        )
        if isinstance(pending, OperationReplay):
            return pending.result
        if pending.device_id is None:
            self.journal.fail(pending, "mounted volume has no block device")
            raise StorageNativeTerminalError("mounted volume has no block device")
        mount_path = Path(pending.mount_path)
        layer_path = Path(pending.runtime_dir) / "layers" / (
            f"revision-{pending.revision}.commit"
        )
        layer_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        frozen = False
        try:
            self.host.sync(mount_path)
            self.host.freeze(mount_path)
            frozen = True
            descriptor = self.backend.restack_snapshot(
                pending.device_id,
                layer_path,
            )
            metadata = layer_path.stat()
            if descriptor is not None and descriptor.size != metadata.st_size:
                raise StorageNativeTerminalError(
                    "sealed layer descriptor size does not match the file"
                )
            record = replace(
                pending,
                state=StorageVolumeState.SEALED,
                sealed_layer_path=str(layer_path),
                sealed_layer_bytes=metadata.st_size,
                sealed_layer_paths=(
                    *pending.sealed_layer_paths,
                    str(layer_path),
                ),
                updated_ns=time.time_ns(),
            )
            self.host.unfreeze(mount_path)
            frozen = False
            result = self._record_result(record)
            if descriptor is not None:
                result["layer"] = asdict(descriptor)
            self.journal.finish(record, result)
            return result
        except BaseException as exc:
            if frozen:
                try:
                    self.host.unfreeze(mount_path)
                except Exception:
                    pass
                frozen = False
            self.journal.fail(pending, f"{type(exc).__name__}: {exc}")
            raise

    def mount_snapshot_cow(
        self,
        *,
        sandbox_id: str,
        sandbox_generation: int,
        volume_id: str,
        operation_id: str,
        expected_revision: int,
    ) -> dict[str, Any]:
        request = {
            "expected_revision": expected_revision,
            "kind": "MountSnapshotCow",
            "operation_id": operation_id,
            "sandbox_generation": sandbox_generation,
            "sandbox_id": sandbox_id,
            "volume_id": volume_id,
        }
        pending = self.journal.begin_transition(
            request=request,
            operation_id=operation_id,
            kind="MountSnapshotCow",
            volume_id=volume_id,
            sandbox_id=sandbox_id,
            sandbox_generation=sandbox_generation,
            expected_revision=expected_revision,
            allowed_states={
                StorageVolumeState.RELEASED,
                StorageVolumeState.PUBLISHED,
            },
            next_state=StorageVolumeState.ACQUIRING,
            reserve_capacity=True,
            hard_capacity_bytes=self.config.hard_capacity_bytes,
        )
        if isinstance(pending, OperationReplay):
            return pending.result
        if not pending.sealed_layer_paths and not pending.published_layers:
            self.journal.fail(pending, "released volume has no sealed layers")
            raise StorageNativeTerminalError(
                "released volume has no sealed layers"
            )
        volume_root = self._volume_root(volume_id)
        runtime_dir = volume_root / f"runtime-{pending.revision}"
        source = volume_root / f"source-{pending.revision}.json"
        pending = replace(
            pending,
            runtime_dir=str(runtime_dir),
            source_image_config=str(source),
            updated_ns=time.time_ns(),
        )
        self.journal.update_pending(pending)
        try:
            source_config = {
                "lowers": [
                    *(dict(layer) for layer in pending.published_layers),
                    *(
                        {"file": path}
                        for path in pending.sealed_layer_paths
                    ),
                ],
                "resultFile": "",
                "upper": {},
            }
            if pending.published_layers:
                source_config["repoBlobUrl"] = pending.published_repo_blob_url
            _atomic_write_json(source, source_config)
            device = self._acquire_runtime_device(
                source_image_config=source,
                runtime_dir=runtime_dir,
                virtual_size=pending.virtual_size,
            )
            if device.virtual_size != pending.virtual_size:
                raise StorageNativeTerminalError(
                    "block backend changed the requested virtual size"
                )
            pending = replace(
                pending,
                device_id=device.device_id,
                device_path=str(device.device_path),
                runtime_image_config=str(device.image_config_path),
                updated_ns=time.time_ns(),
            )
            self.journal.update_pending(pending)
            self.host.mount(device.device_path, Path(pending.mount_path))
            record = replace(
                pending,
                state=StorageVolumeState.MOUNTED,
                updated_ns=time.time_ns(),
            )
            result = self._record_result(record)
            self.journal.finish(record, result)
            return result
        except BaseException as exc:
            self._best_effort_release(pending)
            self.journal.fail(pending, f"{type(exc).__name__}: {exc}")
            raise

    def discard_mounted_cow(
        self,
        *,
        sandbox_id: str,
        sandbox_generation: int,
        volume_id: str,
        operation_id: str,
        expected_revision: int,
    ) -> dict[str, Any]:
        """Drop an uncommitted writable upper and restore its parent authority."""

        request = {
            "expected_revision": expected_revision,
            "kind": "DiscardMountedCow",
            "operation_id": operation_id,
            "sandbox_generation": sandbox_generation,
            "sandbox_id": sandbox_id,
            "volume_id": volume_id,
        }
        pending = self.journal.begin_transition(
            request=request,
            operation_id=operation_id,
            kind="DiscardMountedCow",
            volume_id=volume_id,
            sandbox_id=sandbox_id,
            sandbox_generation=sandbox_generation,
            expected_revision=expected_revision,
            allowed_states={StorageVolumeState.MOUNTED},
            next_state=StorageVolumeState.RELEASING,
        )
        if isinstance(pending, OperationReplay):
            return pending.result
        if not pending.published_layers and not pending.sealed_layer_paths:
            self.journal.fail(
                pending,
                "mounted volume has no snapshot parent authority",
            )
            raise StorageNativeTerminalError(
                "mounted volume has no snapshot parent authority"
            )
        try:
            mount_path = Path(pending.mount_path)
            if self.host.is_mounted(mount_path):
                self.host.unmount(mount_path)
            if pending.device_id is not None:
                self._release_backend_device(pending.device_id)
            record = replace(
                pending,
                state=(
                    StorageVolumeState.PUBLISHED
                    if pending.published_layers
                    else StorageVolumeState.RELEASED
                ),
                device_id=None,
                device_path="",
                runtime_image_config="",
                updated_ns=time.time_ns(),
            )
            result = self._record_result(record)
            self.journal.finish(record, result)
            return result
        except BaseException as exc:
            self.journal.fail(pending, f"{type(exc).__name__}: {exc}")
            raise

    def release_runtime(
        self,
        *,
        sandbox_id: str,
        sandbox_generation: int,
        volume_id: str,
        operation_id: str,
        expected_revision: int,
    ) -> dict[str, Any]:
        request = {
            "expected_revision": expected_revision,
            "kind": "ReleaseRuntime",
            "operation_id": operation_id,
            "sandbox_generation": sandbox_generation,
            "sandbox_id": sandbox_id,
            "volume_id": volume_id,
        }
        pending = self.journal.begin_transition(
            request=request,
            operation_id=operation_id,
            kind="ReleaseRuntime",
            volume_id=volume_id,
            sandbox_id=sandbox_id,
            sandbox_generation=sandbox_generation,
            expected_revision=expected_revision,
            allowed_states={StorageVolumeState.SEALED},
            next_state=StorageVolumeState.RELEASING,
        )
        if isinstance(pending, OperationReplay):
            return pending.result
        try:
            mount_path = Path(pending.mount_path)
            if self.host.is_mounted(mount_path):
                self.host.unmount(mount_path)
            if pending.device_id is not None:
                self._release_backend_device(pending.device_id)
            record = replace(
                pending,
                state=StorageVolumeState.RELEASED,
                device_id=None,
                device_path="",
                runtime_image_config="",
                updated_ns=time.time_ns(),
            )
            result = self._record_result(record)
            self.journal.finish(record, result)
            return result
        except BaseException as exc:
            self.journal.fail(pending, f"{type(exc).__name__}: {exc}")
            raise

    def publish_snapshot(
        self,
        *,
        sandbox_id: str,
        sandbox_generation: int,
        volume_id: str,
        operation_id: str,
        expected_revision: int,
    ) -> dict[str, Any]:
        if self.publisher is None:
            raise StorageNativeConflictError(
                "durable snapshot publication is not configured"
            )
        request = {
            "expected_revision": expected_revision,
            "kind": "PublishSnapshot",
            "operation_id": operation_id,
            "sandbox_generation": sandbox_generation,
            "sandbox_id": sandbox_id,
            "volume_id": volume_id,
        }
        pending = self.journal.begin_transition(
            request=request,
            operation_id=operation_id,
            kind="PublishSnapshot",
            volume_id=volume_id,
            sandbox_id=sandbox_id,
            sandbox_generation=sandbox_generation,
            expected_revision=expected_revision,
            allowed_states={StorageVolumeState.RELEASED},
            next_state=StorageVolumeState.PUBLISHING,
        )
        if isinstance(pending, OperationReplay):
            return pending.result
        if not pending.sealed_layer_paths:
            self.journal.fail_transition(
                pending,
                fallback_state=StorageVolumeState.RELEASED,
                error="released volume has no unpublished sealed layer",
            )
            raise StorageNativeConflictError(
                "released volume has no unpublished sealed layer"
            )
        local_paths = tuple(Path(path) for path in pending.sealed_layer_paths)
        existing_layers = tuple(
            PublishedStorageLayer(
                digest=str(layer["digest"]),
                size=int(layer["size"]),
            )
            for layer in pending.published_layers
        )
        try:
            publication = self.publisher.publish(
                exporter=self.backend,
                source_layer_paths=local_paths,
                virtual_size=pending.virtual_size,
                existing_layers=existing_layers,
            )
            record = replace(
                pending,
                state=StorageVolumeState.PUBLISHED,
                sealed_layer_path="",
                sealed_layer_paths=(),
                cached_layer_paths=(
                    *pending.cached_layer_paths,
                    *(str(path) for path in local_paths),
                ),
                sealed_layer_bytes=sum(
                    layer.size for layer in publication.layers
                ),
                published_manifest_digest=publication.manifest_digest,
                published_tag=publication.tag,
                published_repository=publication.repository,
                published_repo_blob_url=publication.repo_blob_url,
                published_layers=tuple(
                    layer.to_dict() for layer in publication.layers
                ),
                updated_ns=time.time_ns(),
            )
            result = {
                **self._record_result(record),
                "publication": publication.to_dict(),
            }
            self.journal.finish(record, result)
            self._remove_local_layers(local_paths)
            return result
        except BaseException as exc:
            self.journal.fail_transition(
                pending,
                fallback_state=StorageVolumeState.RELEASED,
                error=f"{type(exc).__name__}: {exc}",
            )
            raise

    def delete_volume(
        self,
        *,
        sandbox_id: str,
        sandbox_generation: int,
        volume_id: str,
        operation_id: str,
        expected_revision: int,
    ) -> dict[str, Any]:
        request = {
            "expected_revision": expected_revision,
            "kind": "DeleteVolume",
            "operation_id": operation_id,
            "sandbox_generation": sandbox_generation,
            "sandbox_id": sandbox_id,
            "volume_id": volume_id,
        }
        pending = self.journal.begin_transition(
            request=request,
            operation_id=operation_id,
            kind="DeleteVolume",
            volume_id=volume_id,
            sandbox_id=sandbox_id,
            sandbox_generation=sandbox_generation,
            expected_revision=expected_revision,
            allowed_states={
                StorageVolumeState.MOUNTED,
                StorageVolumeState.SEALED,
                StorageVolumeState.RELEASED,
                StorageVolumeState.PUBLISHED,
                StorageVolumeState.ERROR,
            },
            next_state=StorageVolumeState.DELETING,
        )
        if isinstance(pending, OperationReplay):
            return pending.result
        try:
            self._best_effort_release(pending)
            volume_root = self._volume_root(volume_id)
            if volume_root.exists():
                if volume_root.is_symlink() or not volume_root.is_dir():
                    raise StorageNativeTerminalError(
                        "volume root is not a real directory"
                    )
                shutil.rmtree(volume_root)
            mount_path = Path(pending.mount_path)
            if mount_path.exists():
                mount_path.rmdir()
            record = replace(
                pending,
                state=StorageVolumeState.DELETED,
                device_id=None,
                device_path="",
                runtime_image_config="",
                sealed_layer_path="",
                sealed_layer_bytes=0,
                sealed_layer_paths=(),
                cached_layer_paths=(),
                published_manifest_digest="",
                published_tag="",
                published_repository="",
                published_repo_blob_url="",
                published_layers=(),
                updated_ns=time.time_ns(),
            )
            result = self._record_result(record)
            self.journal.finish(record, result)
            return result
        except BaseException as exc:
            self.journal.fail(pending, f"{type(exc).__name__}: {exc}")
            raise

    def reconcile(self) -> dict[str, Any]:
        records = self.journal.list()
        live_devices = self.host.ublk_device_ids()
        owned_devices = {
            record.device_id
            for record in records
            if record.device_id is not None
        }
        # With pooling enabled, unjournaled live devices are daemon-owned idle
        # placeholders.  The backend, not this service, owns their high-water
        # cleanup.  In non-pooled compatibility mode they remain true orphans.
        orphan_devices = (
            []
            if self.config.device_pool_enabled
            else sorted(live_devices - owned_devices)
        )
        deleted_orphans: list[int] = []
        for device_id in orphan_devices:
            self.backend.delete(device_id)
            deleted_orphans.append(device_id)

        errors: list[dict[str, Any]] = []
        for record in records:
            if record.state in {
                StorageVolumeState.MOUNTED,
                StorageVolumeState.ACQUIRING,
                StorageVolumeState.SEALING,
                StorageVolumeState.SEALED,
            } and (
                record.device_id is None or record.device_id not in live_devices
            ):
                updated = self.journal.mark_reconcile_error(
                    record,
                    "journaled live block device is missing",
                )
                errors.append(self._record_result(updated))
                continue
            if record.state == StorageVolumeState.CREATING:
                self._best_effort_release(record)
                updated = self.journal.mark_reconcile_error(
                    record,
                    "create was interrupted before the volume became authoritative",
                )
                errors.append(self._record_result(updated))
            elif record.state == StorageVolumeState.IMPORTING:
                volume_root = self._volume_root(record.volume_id)
                if volume_root.exists() and volume_root.is_dir():
                    shutil.rmtree(volume_root, ignore_errors=True)
                mount_path = Path(record.mount_path)
                if mount_path.exists():
                    with suppress(OSError):
                        mount_path.rmdir()
                updated = self.journal.mark_reconcile_error(
                    record,
                    "snapshot import was interrupted before registration",
                )
                errors.append(self._record_result(updated))
            elif record.state == StorageVolumeState.ACQUIRING:
                self._best_effort_release(record)
                updated = self.journal.mark_reconcile_error(
                    record,
                    "snapshot acquire was interrupted before mount became authoritative",
                )
                errors.append(self._record_result(updated))
            elif record.state == StorageVolumeState.SEALING:
                updated = self.journal.mark_reconcile_error(
                    record,
                    "seal was interrupted and cannot be replayed safely",
                )
                errors.append(self._record_result(updated))
            elif record.state == StorageVolumeState.RELEASING:
                self._best_effort_release(record)
                updated = replace(
                    record,
                    state=StorageVolumeState.RELEASED,
                    device_id=None,
                    device_path="",
                    runtime_image_config="",
                    updated_ns=time.time_ns(),
                )
                result = self._record_result(updated)
                self.journal.finish(updated, result)
            elif record.state == StorageVolumeState.PUBLISHING:
                updated = self.journal.fail_transition(
                    record,
                    fallback_state=StorageVolumeState.RELEASED,
                    error=(
                        "snapshot publication was interrupted; retry with a "
                        "new operation id"
                    ),
                )
                errors.append(self._record_result(updated))
            elif record.state == StorageVolumeState.PUBLISHED:
                self._remove_local_layers(
                    tuple(Path(path) for path in record.cached_layer_paths)
                )
            elif record.state == StorageVolumeState.DELETING:
                self._best_effort_release(record)
                updated = self.journal.mark_reconcile_error(
                    record,
                    "delete was interrupted and requires an idempotent retry",
                )
                errors.append(self._record_result(updated))

        return {
            "deleted_orphan_device_ids": deleted_orphans,
            "terminal_records": errors,
            "volume_count": len(records),
        }

    def _best_effort_release(self, record: StorageVolumeRecord) -> None:
        mount_path = Path(record.mount_path)
        mounted = True
        safe_to_pool = True
        try:
            mounted = self.host.is_mounted(mount_path)
            if mounted:
                self.host.unmount(mount_path)
                mounted = False
        except Exception:
            safe_to_pool = False
            # A missing or failed ublk backend can leave the kernel mount
            # present but unreadable. Destructive recovery must detach that
            # stale mount without reading it so DeleteVolume can still reclaim
            # the hard reservation and remove the mountpoint.
            if mounted:
                try:
                    self.host.detach(mount_path)
                except Exception:
                    pass
        if record.device_id is not None:
            try:
                if not safe_to_pool:
                    # A lazy-detached or otherwise questionable mount must
                    # never be rebound to another sandbox through the pool.
                    self._discard_backend_device(record.device_id)
                else:
                    self._release_backend_device(record.device_id)
            except Exception:
                pass

    def _acquire_runtime_device(
        self,
        *,
        source_image_config: Path,
        runtime_dir: Path,
        virtual_size: int,
    ) -> StorageNativeDevice:
        idle_before = (
            self.host.ublk_device_ids()
            - {
                record.device_id
                for record in self.journal.list()
                if record.device_id is not None
            }
            if self.config.device_pool_enabled
            else set()
        )
        device = self.backend.create_runtime_device(
            source_image_config=source_image_config,
            global_config=self.global_config_path,
            runtime_dir=runtime_dir,
            virtual_size=virtual_size,
            upper_mode=self.config.upper_mode,
        )
        if self.config.device_pool_enabled:
            with self._pool_metrics_lock:
                reused = (
                    device.device_id in idle_before
                    or device.device_id in self._released_device_ids
                )
                self._released_device_ids.discard(device.device_id)
                self._pool_acquires += 1
                if reused:
                    self._pool_reused_acquires += 1
                else:
                    self._pool_new_acquires += 1
        return device

    def _release_backend_device(self, device_id: int) -> None:
        if not self.config.device_pool_enabled:
            self.backend.delete(device_id)
            return
        self.backend.release(device_id)
        with self._pool_metrics_lock:
            self._pool_releases += 1
            self._released_device_ids.add(device_id)

    def _discard_backend_device(self, device_id: int) -> None:
        self.backend.delete(device_id)
        if self.config.device_pool_enabled:
            with self._pool_metrics_lock:
                self._pool_discards += 1
                self._released_device_ids.discard(device_id)

    @staticmethod
    def _remove_local_layers(paths: tuple[Path, ...]) -> None:
        for path in paths:
            try:
                path.unlink(missing_ok=True)
            except OSError:
                pass

    def _ensure_roots(self) -> None:
        for root in (self.config.runtime_root, self.config.mount_root):
            root.mkdir(mode=0o700, parents=True, exist_ok=True)
            if root.is_symlink() or not root.is_dir():
                raise StorageNativeNodeError(
                    f"storage-native root is not a real directory: {root}"
                )
            if root.stat().st_mode & 0o022:
                raise StorageNativeNodeError(
                    f"storage-native root cannot be group/world writable: {root}"
                )
        if (
            self.config.runtime_root.stat().st_dev
            != self.config.mount_root.stat().st_dev
        ):
            raise StorageNativeNodeError(
                "runtime and mount roots must be on one filesystem for atomic restack"
            )

    def _volume_root(self, volume_id: str) -> Path:
        if not _SAFE_ID.fullmatch(volume_id):
            raise ValueError("volume_id is invalid")
        result = self.config.runtime_root / volume_id
        if result.parent != self.config.runtime_root:
            raise ValueError("volume path escaped runtime root")
        return result

    @staticmethod
    def _record_result(record: StorageVolumeRecord) -> dict[str, Any]:
        return {"record": record.to_json()}


class StorageNativeNodeClient:
    def __init__(
        self,
        socket_path: Path,
        *,
        timeout_seconds: float = 120.0,
    ) -> None:
        if not socket_path.is_absolute():
            raise ValueError("storage-native service socket must be absolute")
        if timeout_seconds <= 0:
            raise ValueError("storage-native service timeout must be positive")
        self.socket_path = socket_path
        self.timeout_seconds = timeout_seconds

    def wait_ready(self, *, timeout_seconds: float = 30.0) -> dict[str, Any]:
        if timeout_seconds <= 0:
            raise ValueError("storage-native readiness timeout must be positive")
        deadline = time.monotonic() + timeout_seconds
        last_error: BaseException | None = None
        while time.monotonic() < deadline:
            try:
                features = self.get_features()
                metrics = self.get_metrics()
                return {"features": features, "metrics": metrics}
            except (OSError, StorageNativeNodeError) as exc:
                last_error = exc
                time.sleep(0.05)
        reason = f": {last_error}" if last_error is not None else ""
        raise StorageNativeNodeError(
            f"storage-native service did not become ready{reason}"
        )

    def get_features(self) -> dict[str, Any]:
        return self._call({"operation": "GetFeatures"})

    def get_metrics(self) -> dict[str, Any]:
        return self._call({"operation": "GetMetrics"})

    def create_volume(
        self,
        *,
        sandbox_id: str,
        sandbox_generation: int,
        volume_id: str,
        operation_id: str,
        virtual_size: int,
        accounting_id: int = 0,
    ) -> dict[str, Any]:
        return self._call(
            {
                "operation": "CreateVolume",
                "operation_id": operation_id,
                "sandbox_generation": sandbox_generation,
                "sandbox_id": sandbox_id,
                "virtual_size": virtual_size,
                "volume_id": volume_id,
                "accounting_id": accounting_id,
            }
        )

    def publish_snapshot(
        self,
        *,
        sandbox_id: str,
        sandbox_generation: int,
        volume_id: str,
        operation_id: str,
        expected_revision: int,
    ) -> dict[str, Any]:
        return self._call(
            {
                "expected_revision": expected_revision,
                "operation": "PublishSnapshot",
                "operation_id": operation_id,
                "sandbox_generation": sandbox_generation,
                "sandbox_id": sandbox_id,
                "volume_id": volume_id,
            }
        )

    def acquire_snapshot(
        self,
        *,
        sandbox_id: str,
        sandbox_generation: int,
        volume_id: str,
        operation_id: str,
        publication: dict[str, Any],
        accounting_id: int = 0,
    ) -> dict[str, Any]:
        return self._call(
            {
                "accounting_id": accounting_id,
                "operation": "AcquireSnapshot",
                "operation_id": operation_id,
                "publication": publication,
                "sandbox_generation": sandbox_generation,
                "sandbox_id": sandbox_id,
                "volume_id": volume_id,
            }
        )

    def mount_snapshot_cow(
        self,
        *,
        sandbox_id: str,
        sandbox_generation: int,
        volume_id: str,
        operation_id: str,
        expected_revision: int,
    ) -> dict[str, Any]:
        return self._call(
            {
                "expected_revision": expected_revision,
                "operation": "MountSnapshotCow",
                "operation_id": operation_id,
                "sandbox_generation": sandbox_generation,
                "sandbox_id": sandbox_id,
                "volume_id": volume_id,
            }
        )

    def freeze_and_seal(
        self,
        *,
        sandbox_id: str,
        sandbox_generation: int,
        volume_id: str,
        operation_id: str,
        expected_revision: int,
    ) -> dict[str, Any]:
        return self._call(
            {
                "expected_revision": expected_revision,
                "operation": "FreezeAndSeal",
                "operation_id": operation_id,
                "sandbox_generation": sandbox_generation,
                "sandbox_id": sandbox_id,
                "volume_id": volume_id,
            }
        )

    def discard_mounted_cow(
        self,
        *,
        sandbox_id: str,
        sandbox_generation: int,
        volume_id: str,
        operation_id: str,
        expected_revision: int,
    ) -> dict[str, Any]:
        return self._call(
            {
                "expected_revision": expected_revision,
                "operation": "DiscardMountedCow",
                "operation_id": operation_id,
                "sandbox_generation": sandbox_generation,
                "sandbox_id": sandbox_id,
                "volume_id": volume_id,
            }
        )

    def release_runtime(
        self,
        *,
        sandbox_id: str,
        sandbox_generation: int,
        volume_id: str,
        operation_id: str,
        expected_revision: int,
    ) -> dict[str, Any]:
        return self._call(
            {
                "expected_revision": expected_revision,
                "operation": "ReleaseRuntime",
                "operation_id": operation_id,
                "sandbox_generation": sandbox_generation,
                "sandbox_id": sandbox_id,
                "volume_id": volume_id,
            }
        )

    def delete_volume(
        self,
        *,
        sandbox_id: str,
        sandbox_generation: int,
        volume_id: str,
        operation_id: str,
        expected_revision: int,
    ) -> dict[str, Any]:
        return self._call(
            {
                "expected_revision": expected_revision,
                "operation": "DeleteVolume",
                "operation_id": operation_id,
                "sandbox_generation": sandbox_generation,
                "sandbox_id": sandbox_id,
                "volume_id": volume_id,
            }
        )

    def get_volume(self, volume_id: str) -> dict[str, Any]:
        return self._call(
            {
                "operation": "GetVolume",
                "volume_id": volume_id,
            }
        )

    def reconcile(self) -> dict[str, Any]:
        return self._call({"operation": "Reconcile"})

    def list_volumes(self) -> dict[str, Any]:
        return self._call({"operation": "ListVolumes"})

    def _call(self, request: dict[str, Any]) -> dict[str, Any]:
        payload = _canonical_json(
            {"schema": _PROTOCOL_SCHEMA, **request}
        ).encode("ascii")
        if len(payload) > _PROTOCOL_MAX_BYTES:
            raise ValueError("storage-native service request is too large")
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
            connection.settimeout(self.timeout_seconds)
            connection.connect(str(self.socket_path))
            connection.sendall(struct.pack(">I", len(payload)) + payload)
            size = struct.unpack(">I", _recv_exact(connection, 4))[0]
            if size > _PROTOCOL_MAX_BYTES:
                raise StorageNativeNodeError(
                    "storage-native service response is too large"
                )
            raw = json.loads(_recv_exact(connection, size).decode("utf-8"))
        if not isinstance(raw, dict):
            raise StorageNativeNodeError("storage-native response must be an object")
        status = raw.get("status")
        if status == "ok":
            result = raw.get("result")
            if not isinstance(result, dict):
                raise StorageNativeNodeError(
                    "storage-native response result must be an object"
                )
            return result
        message = str(raw.get("message") or "storage-native operation failed")
        if status == "conflict":
            raise StorageNativeConflictError(message)
        if status == "pending":
            raise StorageNativePendingOperation(message)
        if status == "terminal":
            raise StorageNativeTerminalError(message)
        raise StorageNativeNodeError(message)


class _StorageNativeRequestHandler(socketserver.BaseRequestHandler):
    server: "_StorageNativeUnixServer"

    def handle(self) -> None:
        try:
            self.server.require_allowed_peer(self.request)
            size = struct.unpack(">I", _recv_exact(self.request, 4))[0]
            if size > _PROTOCOL_MAX_BYTES:
                raise ValueError("storage-native request is too large")
            request = json.loads(
                _recv_exact(self.request, size).decode("utf-8")
            )
            if not isinstance(request, dict):
                raise ValueError("storage-native request must be an object")
            if request.get("operation") in {"GetFeatures", "GetMetrics"}:
                response = {
                    "status": "ok",
                    "result": self.server.dispatch(request),
                }
            else:
                self.server.operation_waiting()
                with self.server.operation_slots:
                    self.server.operation_started()
                    try:
                        response = {
                            "status": "ok",
                            "result": self.server.dispatch(request),
                        }
                    finally:
                        self.server.operation_finished()
        except StorageNativePendingOperation as exc:
            response = {"status": "pending", "message": str(exc)}
        except StorageNativeConflictError as exc:
            response = {"status": "conflict", "message": str(exc)}
        except StorageNativeTerminalError as exc:
            response = {"status": "terminal", "message": str(exc)}
        except Exception as exc:
            response = {
                "status": "error",
                "message": f"{type(exc).__name__}: {exc}",
            }
        payload = _canonical_json(response).encode("ascii")
        self.request.sendall(struct.pack(">I", len(payload)) + payload)


class _StorageNativeUnixServer(
    socketserver.ThreadingMixIn,
    socketserver.UnixStreamServer,
):
    daemon_threads = True
    # The default socketserver backlog is only five. Production permits eight
    # simultaneous storage operations plus metrics/reconcile traffic, so a
    # burst could otherwise fail connect() before reaching the explicit
    # operation semaphore.
    request_queue_size = 128

    def __init__(
        self,
        socket_path: Path,
        service: StorageNativeNodeService,
        *,
        require_root_peer: bool,
    ) -> None:
        self.service = service
        self.require_root_peer_enabled = require_root_peer
        self.operation_slots = threading.BoundedSemaphore(
            service.config.max_concurrent_operations
        )
        self._metrics_lock = threading.Lock()
        self._active_operations = 0
        self._waiting_operations = 0
        super().__init__(str(socket_path), _StorageNativeRequestHandler)

    def operation_waiting(self) -> None:
        with self._metrics_lock:
            self._waiting_operations += 1

    def operation_started(self) -> None:
        with self._metrics_lock:
            self._waiting_operations -= 1
            self._active_operations += 1

    def operation_finished(self) -> None:
        with self._metrics_lock:
            self._active_operations -= 1

    def metrics(self) -> dict[str, Any]:
        with self._metrics_lock:
            active = self._active_operations
            waiting = self._waiting_operations
        return {
            **self.service.metrics(),
            "active_operations": active,
            "waiting_operations": waiting,
            "max_concurrent_operations": (
                self.service.config.max_concurrent_operations
            ),
        }

    def require_allowed_peer(self, connection: socket.socket) -> None:
        if not self.require_root_peer_enabled:
            return
        peer_credential = getattr(socket, "SO_PEERCRED", None)
        if peer_credential is None:
            raise PermissionError("SO_PEERCRED is required for root peer fencing")
        raw = connection.getsockopt(socket.SOL_SOCKET, peer_credential, 12)
        _pid, uid, _gid = struct.unpack("3i", raw)
        if uid != 0:
            raise PermissionError("storage-native service requires a root peer")

    def dispatch(self, request: dict[str, Any]) -> dict[str, Any]:
        if request.get("schema") != _PROTOCOL_SCHEMA:
            raise ValueError("unsupported storage-native protocol schema")
        operation = request.get("operation")
        if operation == "GetFeatures":
            return {
                "protocol_schema": _PROTOCOL_SCHEMA,
                "storage_schema": "ucloud-storage-native-v1",
                "upper_mode": self.service.config.upper_mode,
                "durable_publication": self.service.publisher is not None,
            }
        if operation == "GetMetrics":
            return self.metrics()
        if operation == "CreateVolume":
            return self.service.create_volume(
                sandbox_id=_string_field(request, "sandbox_id"),
                sandbox_generation=_nonnegative_int_field(
                    request,
                    "sandbox_generation",
                ),
                volume_id=_string_field(request, "volume_id"),
                operation_id=_string_field(request, "operation_id"),
                virtual_size=_positive_int_field(request, "virtual_size"),
                accounting_id=_nonnegative_int_field(
                    request,
                    "accounting_id",
                ),
            )
        if operation == "AcquireSnapshot":
            publication = request.get("publication")
            if not isinstance(publication, dict):
                raise ValueError("publication must be an object")
            return self.service.acquire_snapshot(
                sandbox_id=_string_field(request, "sandbox_id"),
                sandbox_generation=_nonnegative_int_field(
                    request,
                    "sandbox_generation",
                ),
                volume_id=_string_field(request, "volume_id"),
                operation_id=_string_field(request, "operation_id"),
                publication_raw=publication,
                accounting_id=_nonnegative_int_field(
                    request,
                    "accounting_id",
                ),
            )
        if operation in {
            "DeleteVolume",
            "DiscardMountedCow",
            "FreezeAndSeal",
            "MountSnapshotCow",
            "PublishSnapshot",
            "ReleaseRuntime",
        }:
            arguments = {
                "sandbox_id": _string_field(request, "sandbox_id"),
                "sandbox_generation": _nonnegative_int_field(
                    request,
                    "sandbox_generation",
                ),
                "volume_id": _string_field(request, "volume_id"),
                "operation_id": _string_field(request, "operation_id"),
                "expected_revision": _nonnegative_int_field(
                    request,
                    "expected_revision",
                ),
            }
            if operation == "DeleteVolume":
                return self.service.delete_volume(**arguments)
            if operation == "DiscardMountedCow":
                return self.service.discard_mounted_cow(**arguments)
            if operation == "FreezeAndSeal":
                return self.service.freeze_and_seal(**arguments)
            if operation == "MountSnapshotCow":
                return self.service.mount_snapshot_cow(**arguments)
            if operation == "PublishSnapshot":
                return self.service.publish_snapshot(**arguments)
            return self.service.release_runtime(**arguments)
        if operation == "GetVolume":
            record = self.service.journal.load(
                _string_field(request, "volume_id")
            )
            if record is None:
                raise StorageNativeConflictError(
                    "storage-native volume does not exist"
                )
            return self.service._record_result(record)
        if operation == "ListVolumes":
            return {
                "records": [
                    record.to_json()
                    for record in self.service.journal.list()
                ]
            }
        if operation == "Reconcile":
            return self.service.reconcile()
        raise ValueError("unknown storage-native operation")


class StorageNativeNodeServer:
    def __init__(
        self,
        socket_path: Path,
        service: StorageNativeNodeService,
        *,
        require_root_peer: bool = True,
    ) -> None:
        if not socket_path.is_absolute():
            raise ValueError("storage-native service socket must be absolute")
        self.socket_path = socket_path
        self.service = service
        self.require_root_peer = require_root_peer
        self._server: _StorageNativeUnixServer | None = None

    def serve_forever(self) -> None:
        self._prepare_socket()
        server = _StorageNativeUnixServer(
            self.socket_path,
            self.service,
            require_root_peer=self.require_root_peer,
        )
        self._server = server
        os.chmod(self.socket_path, 0o600)
        try:
            server.serve_forever()
        finally:
            server.server_close()
            self._server = None
            self._remove_owned_socket()

    def shutdown(self) -> None:
        server = self._server
        if server is not None:
            server.shutdown()

    def _prepare_socket(self) -> None:
        parent = self.socket_path.parent
        parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        if parent.is_symlink() or not parent.is_dir() or parent.stat().st_mode & 0o022:
            raise StorageNativeNodeError(
                "storage-native socket parent must be a private real directory"
            )
        try:
            metadata = self.socket_path.lstat()
        except FileNotFoundError:
            return
        if not stat.S_ISSOCK(metadata.st_mode) or metadata.st_uid != os.geteuid():
            raise StorageNativeNodeError(
                "refusing to replace an unowned or non-socket service path"
            )
        probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            probe.connect(str(self.socket_path))
        except OSError as exc:
            if exc.errno != errno.ECONNREFUSED:
                raise
        else:
            raise StorageNativeNodeError(
                "storage-native service socket is already active"
            )
        finally:
            probe.close()
        self.socket_path.unlink()

    def _remove_owned_socket(self) -> None:
        try:
            metadata = self.socket_path.lstat()
        except FileNotFoundError:
            return
        if stat.S_ISSOCK(metadata.st_mode) and metadata.st_uid == os.geteuid():
            self.socket_path.unlink()


def _canonical_json(payload: Any) -> str:
    return json.dumps(
        payload,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )


def _request_sha256(request: dict[str, Any]) -> str:
    return hashlib.sha256(_canonical_json(request).encode("ascii")).hexdigest()


def _is_sha256_digest(value: str) -> bool:
    return bool(re.fullmatch(r"sha256:[0-9a-f]{64}", value))


def _recv_exact(connection: socket.socket, size: int) -> bytes:
    result = bytearray()
    while len(result) < size:
        chunk = connection.recv(size - len(result))
        if not chunk:
            raise EOFError("storage-native peer closed the connection early")
        result.extend(chunk)
    return bytes(result)


def _string_field(request: dict[str, Any], name: str) -> str:
    value = request.get(name)
    if not isinstance(value, str) or not value or "\0" in value:
        raise ValueError(f"{name} must be a non-empty NUL-free string")
    return value


def _nonnegative_int_field(request: dict[str, Any], name: str) -> int:
    value = request.get(name)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{name} must be a non-negative integer")
    return value


def _positive_int_field(request: dict[str, Any], name: str) -> int:
    value = _nonnegative_int_field(request, name)
    if value == 0:
        raise ValueError(f"{name} must be positive")
    return value


def _atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    descriptor, raw_temp = tempfile.mkstemp(
        prefix=f".{path.name}.",
        dir=path.parent,
    )
    temp = Path(raw_temp)
    try:
        with os.fdopen(descriptor, "w", encoding="ascii") as handle:
            handle.write(_canonical_json(payload) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp, path)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        temp.unlink(missing_ok=True)
