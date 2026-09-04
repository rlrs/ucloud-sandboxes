from __future__ import annotations

from contextlib import closing, contextmanager, suppress
from dataclasses import asdict, dataclass, fields, replace
from enum import Enum
from functools import wraps
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
from typing import Any, Literal, Protocol

from opentelemetry.trace import SpanKind

from .storage_native import (
    StorageNativeDevice,
    StorageNativeDeviceOwner,
)
from .storage_native_registry import (
    PublishedStorageLayer,
    StorageSnapshotPublication,
)
from .telemetry import Telemetry


_SAFE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,239}\Z")
_PROTOCOL_SCHEMA = 4
_JOURNAL_APPLICATION_ID = 0x55435342
_JOURNAL_SCHEMA_VERSION = 2
_PROTOCOL_MAX_BYTES = 1024 * 1024
_OWNER_REQUEST_FIELDS = ("sandbox_generation", "sandbox_id", "volume_id")
_PROTOCOL_EXTRA_FIELDS = {
    **{
        operation: ()
        for operation in (
            "GetFeatures",
            "GetMetrics",
            "ListVolumes",
            "Reconcile",
        )
    },
    "GetVolume": ("volume_id",),
    "ListVolumesPage": ("after_volume_id",),
    "PrepareVolume": (*_OWNER_REQUEST_FIELDS, "operation_id", "virtual_size"),
    "PrepareImport": (*_OWNER_REQUEST_FIELDS, "operation_id", "publication"),
    **{
        operation: (*_OWNER_REQUEST_FIELDS, "operation_id")
        for operation in (
            "DiscardResume",
            "EnsureMounted",
            "EnsurePublished",
            "EnsureReleased",
        )
    },
    "DeleteVolume": (
        *_OWNER_REQUEST_FIELDS,
        "expected_accounting_id",
        "expected_virtual_size",
        "operation_id",
    ),
}
_PROTOCOL_EXTRA_FIELDS = {
    operation: (*extra_fields, "trace_context")
    for operation, extra_fields in _PROTOCOL_EXTRA_FIELDS.items()
}
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


class StorageNativeCapacityError(StorageNativeConflictError):
    """The node cannot allocate another live storage volume right now."""


class StorageNativePendingOperation(StorageNativeConflictError):
    pass


class _StorageNativeBackendReleasePending(StorageNativeNodeError):
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
class StorageVolumeOwner:
    volume_id: str
    sandbox_id: str
    sandbox_generation: int

    def __post_init__(self) -> None:
        for label, value in (
            ("volume_id", self.volume_id),
            ("sandbox_id", self.sandbox_id),
        ):
            if not _SAFE_ID.fullmatch(value):
                raise ValueError(f"{label} is invalid")
        if (
            isinstance(self.sandbox_generation, bool)
            or not isinstance(self.sandbox_generation, int)
            or self.sandbox_generation < 0
        ):
            raise ValueError("sandbox_generation must be a non-negative integer")

    def request_fields(self) -> dict[str, Any]:
        return {
            "sandbox_generation": self.sandbox_generation,
            "sandbox_id": self.sandbox_id,
            "volume_id": self.volume_id,
        }


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
    max_ublk_devices: int = 0

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
        if self.max_ublk_devices < 0:
            raise ValueError("maximum ublk devices must be non-negative")
        if (
            self.max_ublk_devices > 0
            and self.device_pool_high_watermark > self.max_ublk_devices
        ):
            raise ValueError("device pool high watermark exceeds maximum ublk devices")


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
    device_owner_id: str
    device_id: int | None = None
    device_path: str = ""
    runtime_image_config: str = ""
    sealed_layer_bytes: int = 0
    sealed_layer_paths: tuple[str, ...] = ()
    cached_layer_paths: tuple[str, ...] = ()
    published_manifest_digest: str = ""
    published_tag: str = ""
    published_repository: str = ""
    published_repo_blob_url: str = ""
    published_backend: str = ""
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
            raise ValueError(
                "storage-native generations and revisions must be non-negative"
            )
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
        if self.device_owner_id and not _SAFE_ID.fullmatch(self.device_owner_id):
            raise ValueError("device_owner_id is invalid")
        if self.device_id is not None and not self.device_owner_id:
            raise ValueError("journaled devices require an owner identity")
        for raw in (
            self.device_path,
            self.runtime_image_config,
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
        if self.published_backend not in {"", "registry", "s3"}:
            raise ValueError("published backend is invalid")
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

    @property
    def owner(self) -> StorageVolumeOwner:
        return StorageVolumeOwner(
            volume_id=self.volume_id,
            sandbox_id=self.sandbox_id,
            sandbox_generation=self.sandbox_generation,
        )

    def publication(self) -> StorageSnapshotPublication:
        if self.state != StorageVolumeState.PUBLISHED:
            raise StorageNativeConflictError("storage-native volume is not published")
        return self.dependency_publication()

    def dependency_publication(self) -> StorageSnapshotPublication:
        """Describe remote layers even while a writable COW volume is mounted."""
        if not self.published_layers:
            raise StorageNativeConflictError(
                "storage-native volume has no remote layers"
            )
        return StorageSnapshotPublication(
            manifest_digest=self.published_manifest_digest,
            tag=self.published_tag,
            repository=self.published_repository,
            repo_blob_url=self.published_repo_blob_url,
            virtual_size=self.virtual_size,
            layers=tuple(
                PublishedStorageLayer.from_dict(layer)
                for layer in self.published_layers
            ),
            backend=(
                self.published_backend
                or (
                    "s3"
                    if self.published_repo_blob_url.startswith("s3://")
                    else "registry"
                )
            ),
        )

    @classmethod
    def from_json(cls, raw: dict[str, Any]) -> "StorageVolumeRecord":
        expected = {field.name for field in fields(cls)}
        if set(raw) == expected - {"published_backend"}:
            raw = {
                **raw,
                "published_backend": (
                    "s3"
                    if str(raw.get("published_repo_blob_url") or "").startswith("s3://")
                    else ("registry" if raw.get("published_manifest_digest") else "")
                ),
            }
        if set(raw) != expected:
            raise ValueError("storage-native volume record has an invalid schema")
        payload = dict(raw)
        payload["state"] = StorageVolumeState(str(payload["state"]))
        payload["sealed_layer_paths"] = tuple(payload.get("sealed_layer_paths", ()))
        payload["published_layers"] = tuple(payload.get("published_layers", ()))
        payload["cached_layer_paths"] = tuple(payload.get("cached_layer_paths", ()))
        record = cls(**payload)
        if record.accounting_id <= 0:
            raise ValueError(
                "journaled storage volumes require a positive accounting ID"
            )
        return record


@dataclass(frozen=True)
class OperationReplay:
    record: StorageVolumeRecord


class StorageBlockBackend(Protocol):
    def create_runtime_device(
        self,
        *,
        source_image_config: Path,
        global_config: Path,
        runtime_dir: Path,
        virtual_size: int,
        upper_mode: str,
        owner_id: str,
    ) -> StorageNativeDevice: ...

    def list_runtime_device_owners(
        self,
    ) -> tuple[StorageNativeDeviceOwner, ...]: ...

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

    def export_compacted_image(
        self,
        *,
        source_image_config: Path,
        global_config: Path,
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
        existing_repo_blob_url: str = "",
        global_config_path: Path | None = None,
    ) -> StorageSnapshotPublication: ...

    def verify(
        self,
        publication: StorageSnapshotPublication,
    ) -> StorageSnapshotPublication: ...

    def metrics(self) -> dict[str, int]: ...


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
    _SCHEMA = f"""
        BEGIN IMMEDIATE;
        CREATE TABLE volumes (
            volume_id TEXT PRIMARY KEY,
            state TEXT NOT NULL,
            virtual_size INTEGER NOT NULL,
            accounting_id INTEGER NOT NULL UNIQUE CHECK(accounting_id > 0),
            record_json TEXT NOT NULL
        );
        CREATE TABLE operations (
            operation_id TEXT PRIMARY KEY,
            kind TEXT NOT NULL,
            request_sha256 TEXT NOT NULL,
            volume_id TEXT NOT NULL,
            status TEXT NOT NULL,
            error TEXT NOT NULL DEFAULT ''
        );
        CREATE TABLE counters (
            name TEXT PRIMARY KEY,
            next_value INTEGER NOT NULL CHECK(next_value > 0)
        );
        INSERT INTO counters (name, next_value)
        VALUES ('accounting_id', 200000);
        PRAGMA application_id = {_JOURNAL_APPLICATION_ID};
        PRAGMA user_version = {_JOURNAL_SCHEMA_VERSION};
        COMMIT;
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
            application_id = int(
                connection.execute("PRAGMA application_id").fetchone()[0]
            )
            schema_version = int(
                connection.execute("PRAGMA user_version").fetchone()[0]
            )
            if application_id == 0 and schema_version == 0:
                existing_tables = connection.execute(
                    (
                        "SELECT name FROM sqlite_master "
                        "WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
                    )
                ).fetchall()
                if existing_tables:
                    raise StorageNativeNodeError(
                        "storage-native journal schema is incompatible"
                    )
                try:
                    connection.executescript(self._SCHEMA)
                except sqlite3.DatabaseError as exc:
                    connection.rollback()
                    application_id = int(
                        connection.execute("PRAGMA application_id").fetchone()[0]
                    )
                    schema_version = int(
                        connection.execute("PRAGMA user_version").fetchone()[0]
                    )
                    if (
                        application_id != _JOURNAL_APPLICATION_ID
                        or schema_version != _JOURNAL_SCHEMA_VERSION
                    ):
                        raise StorageNativeNodeError(
                            "storage-native journal initialization failed"
                        ) from exc
            elif (
                application_id != _JOURNAL_APPLICATION_ID
                or schema_version != _JOURNAL_SCHEMA_VERSION
            ):
                raise StorageNativeNodeError(
                    "storage-native journal schema is incompatible"
                )
            self._require_schema(connection)
            self._require_data(connection)
            connection.execute(
                "CREATE INDEX IF NOT EXISTS volumes_live_inventory "
                "ON volumes(volume_id) WHERE state != 'deleted'"
            )

    @staticmethod
    def _require_schema(connection: sqlite3.Connection) -> None:
        expected = {
            "volumes": (
                "volume_id",
                "state",
                "virtual_size",
                "accounting_id",
                "record_json",
            ),
            "operations": (
                "operation_id",
                "kind",
                "request_sha256",
                "volume_id",
                "status",
                "error",
            ),
            "counters": ("name", "next_value"),
        }
        tables = {
            str(row[0])
            for row in connection.execute(
                (
                    "SELECT name FROM sqlite_master "
                    "WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
                )
            ).fetchall()
        }
        if tables != set(expected):
            raise StorageNativeNodeError(
                "storage-native journal schema is incompatible"
            )
        for table, columns in expected.items():
            actual = tuple(
                str(row[1])
                for row in connection.execute(f"PRAGMA table_info({table})").fetchall()
            )
            if actual != columns:
                raise StorageNativeNodeError(
                    "storage-native journal schema is incompatible"
                )

    @classmethod
    def _require_data(cls, connection: sqlite3.Connection) -> None:
        counters = connection.execute(
            "SELECT name, next_value FROM counters"
        ).fetchall()
        maximum = int(
            connection.execute(
                "SELECT COALESCE(MAX(accounting_id), 0) FROM volumes"
            ).fetchone()[0]
        )
        if (
            len(counters) != 1
            or counters[0][0] != "accounting_id"
            or int(counters[0][1]) < 200_000
            or int(counters[0][1]) <= maximum
        ):
            raise StorageNativeNodeError(
                "storage-native accounting ID state is invalid"
            )
        rows = connection.execute(
            "SELECT volume_id, state, virtual_size, accounting_id, record_json "
            "FROM volumes"
        ).fetchall()
        for row in rows:
            cls._decode_record_row(row)

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
                raise StorageNativeCapacityError(
                    "storage-native hard capacity is exhausted"
                )
            record = self._allocate_accounting_id(connection, record)
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
                "SELECT volume_id, state, virtual_size, accounting_id, record_json "
                "FROM volumes WHERE volume_id = ?",
                (record.volume_id,),
            ).fetchone()
            if existing_row is not None:
                existing = self._decode_record_row(existing_row)
                if (
                    existing.state != StorageVolumeState.DELETED
                    or existing.sandbox_id != record.sandbox_id
                    or existing.sandbox_generation != record.sandbox_generation
                    or existing.virtual_size != record.virtual_size
                ):
                    raise StorageNativeConflictError("volume_id already exists")
                record = replace(
                    record,
                    revision=existing.revision + 1,
                    accounting_id=existing.accounting_id,
                )
            else:
                record = self._allocate_accounting_id(connection, record)
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
                        f"WHERE state IN ({','.join('?' for _ in _ACTIVE_CAPACITY_STATES)}) "
                        "AND volume_id != ?"
                    ),
                    (*sorted(_ACTIVE_CAPACITY_STATES), record.volume_id),
                ).fetchone()[0]
                if int(reserved) + record.virtual_size > hard_capacity_bytes:
                    raise StorageNativeCapacityError(
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

    def finish(self, record: StorageVolumeRecord) -> None:
        self._complete_operation(record, record, status="completed")

    def fail(self, record: StorageVolumeRecord, error: str) -> None:
        terminal = replace(
            record,
            state=StorageVolumeState.ERROR,
            error=error[:4096],
            updated_ns=time.time_ns(),
        )
        self._complete_operation(
            record, terminal, status="failed", require_pending=False
        )

    def fail_transition(
        self,
        record: StorageVolumeRecord,
        *,
        failure_state: StorageVolumeState,
        error: str,
    ) -> StorageVolumeRecord:
        recovered = replace(
            record,
            state=failure_state,
            error=error[:4096],
            updated_ns=time.time_ns(),
        )
        self._complete_operation(record, recovered, status="failed")
        return recovered

    def _complete_operation(
        self,
        pending: StorageVolumeRecord,
        result: StorageVolumeRecord,
        *,
        status: Literal["completed", "failed"],
        require_pending: bool = True,
    ) -> None:
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            current = self._load(connection, pending.volume_id)
            if (
                current.revision != pending.revision
                or current.operation_id != pending.operation_id
            ):
                raise StorageNativeConflictError(
                    "storage-native operation lost its "
                    f"{'failure' if status == 'failed' else 'completion'} fence"
                )
            self._upsert_record(connection, result)
            if status == "failed":
                changed = connection.execute(
                    "UPDATE operations SET status = 'failed', error = ? "
                    "WHERE operation_id = ? AND status = 'pending'",
                    (result.error, pending.operation_id),
                ).rowcount
            else:
                changed = connection.execute(
                    "UPDATE operations SET status = 'completed' "
                    "WHERE operation_id = ? AND status = 'pending'",
                    (pending.operation_id,),
                ).rowcount
            if require_pending and changed != 1:
                raise StorageNativeConflictError(
                    "storage-native operation is not pending"
                )
            connection.commit()

    def load(self, volume_id: str) -> StorageVolumeRecord | None:
        with closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT volume_id, state, virtual_size, accounting_id, record_json "
                "FROM volumes WHERE volume_id = ?",
                (volume_id,),
            ).fetchone()
        if row is None:
            return None
        return self._decode_record_row(row)

    def list(self) -> tuple[StorageVolumeRecord, ...]:
        with closing(self._connect()) as connection:
            rows = connection.execute(
                "SELECT volume_id, state, virtual_size, accounting_id, record_json "
                "FROM volumes ORDER BY volume_id"
            ).fetchall()
        return tuple(self._decode_record_row(row) for row in rows)

    def list_live_page(
        self, after_volume_id: str, *, limit: int = 128
    ) -> tuple[StorageVolumeRecord, ...]:
        with closing(self._connect()) as connection:
            rows = connection.execute(
                "SELECT volume_id, state, virtual_size, accounting_id, record_json "
                "FROM volumes WHERE state != 'deleted' AND volume_id > ? "
                "ORDER BY volume_id LIMIT ?",
                (after_volume_id, limit),
            ).fetchall()
        return tuple(self._decode_record_row(row) for row in rows)

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
                "SELECT kind, request_sha256, status, error, volume_id "
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
            return OperationReplay(self._load(connection, str(row[4])))
        if row[2] == "failed":
            raise StorageNativeTerminalError(row[3] or "storage operation failed")
        raise StorageNativePendingOperation(
            "operation is pending reconciliation; it will not be replayed blindly"
        )

    @staticmethod
    def _allocate_accounting_id(
        connection: sqlite3.Connection,
        record: StorageVolumeRecord,
    ) -> StorageVolumeRecord:
        row = connection.execute(
            "SELECT next_value FROM counters WHERE name = 'accounting_id'"
        ).fetchone()
        if row is None:
            raise StorageNativeNodeError("accounting ID counter is absent")
        accounting_id = int(row[0])
        changed = connection.execute(
            (
                "UPDATE counters SET next_value = ? "
                "WHERE name = 'accounting_id' AND next_value = ?"
            ),
            (accounting_id + 1, accounting_id),
        ).rowcount
        if changed != 1:
            raise StorageNativeConflictError("accounting ID allocation lost its fence")
        return replace(record, accounting_id=accounting_id)

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
        if record.accounting_id <= 0:
            raise StorageNativeNodeError(
                "journaled storage volumes require a positive accounting ID"
            )
        connection.execute(
            """
            INSERT INTO volumes (
                volume_id, state, virtual_size, accounting_id, record_json
            ) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(volume_id) DO UPDATE SET
                state = excluded.state,
                virtual_size = excluded.virtual_size,
                accounting_id = excluded.accounting_id,
                record_json = excluded.record_json
            """,
            (
                record.volume_id,
                record.state.value,
                record.virtual_size,
                record.accounting_id,
                _canonical_json(record.to_json()),
            ),
        )

    @staticmethod
    def _decode_record_row(row: tuple[Any, ...]) -> StorageVolumeRecord:
        record = StorageVolumeRecord.from_json(json.loads(row[4]))
        if (
            record.volume_id != row[0]
            or record.state.value != row[1]
            or record.virtual_size != int(row[2])
            or record.accounting_id != int(row[3])
        ):
            raise StorageNativeNodeError(
                "storage-native volume columns are inconsistent"
            )
        return record

    @staticmethod
    def _load(
        connection: sqlite3.Connection,
        volume_id: str,
    ) -> StorageVolumeRecord:
        row = connection.execute(
            "SELECT volume_id, state, virtual_size, accounting_id, record_json "
            "FROM volumes WHERE volume_id = ?",
            (volume_id,),
        ).fetchone()
        if row is None:
            raise StorageNativeConflictError("storage-native volume does not exist")
        return StorageNativeJournal._decode_record_row(row)

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


class _StorageOperationGate:
    """Allow concurrent mutations, but give reconciliation a quiescent view."""

    def __init__(self) -> None:
        self._condition = threading.Condition()
        self._local = threading.local()
        self._active = 0
        self._maintenance = False
        self._waiting = 0

    @contextmanager
    def operation(self):
        if getattr(self._local, "depth", 0):
            self._local.depth += 1
            try:
                yield
            finally:
                self._local.depth -= 1
            return
        with self._condition:
            self._condition.wait_for(
                lambda: not self._maintenance and not self._waiting
            )
            self._active += 1
            self._local.depth = 1
        try:
            yield
        finally:
            with self._condition:
                self._local.depth = 0
                self._active -= 1
                self._condition.notify_all()

    @contextmanager
    def maintenance(self):
        with self._condition:
            self._waiting += 1
            try:
                self._condition.wait_for(
                    lambda: not self._maintenance and not self._active
                )
                self._maintenance = True
            finally:
                self._waiting -= 1
        try:
            yield
        finally:
            with self._condition:
                self._maintenance = False
                self._condition.notify_all()


def _storage_mutation(method):
    @wraps(method)
    def guarded(self, *args, **kwargs):
        with self._operations.operation():
            return method(self, *args, **kwargs)

    return guarded


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
        self._operations = _StorageOperationGate()
        self._pool_metrics_lock = threading.Lock()
        self._pool_acquires = 0
        self._pool_reused_acquires = 0
        self._pool_new_acquires = 0
        self._pool_releases = 0
        self._pool_discards = 0
        self._released_device_ids: set[int] = set()
        self._device_slot_guard = threading.Lock()
        self._pending_device_allocations = 0
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
            owner.device_id for owner in self._backend_ownership().values()
        }
        idle_device_ids = live_device_ids - active_device_ids
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
            "device_pool_low_watermark": (self.config.device_pool_low_watermark),
            "device_pool_high_watermark": (self.config.device_pool_high_watermark),
            "device_pool_idle_devices": len(idle_device_ids),
            "ublk_active_devices": len(active_device_ids & live_device_ids),
            "ublk_live_devices": len(live_device_ids),
            "ublk_max_devices": self.config.max_ublk_devices,
            "error_volumes": sum(
                record.state == StorageVolumeState.ERROR for record in records
            ),
            "hard_capacity_bytes": self.config.hard_capacity_bytes,
            "hard_reserved_bytes": reserved,
            "published_volumes": sum(
                record.state == StorageVolumeState.PUBLISHED for record in records
            ),
            "volume_count": len(records),
            **(self.publisher.metrics() if self.publisher is not None else {}),
            **pool_metrics,
        }

    @_storage_mutation
    def create_volume(
        self,
        *,
        sandbox_id: str,
        sandbox_generation: int,
        volume_id: str,
        operation_id: str,
        virtual_size: int,
    ) -> StorageVolumeRecord:
        request = {
            "kind": "CreateVolume",
            "operation_id": operation_id,
            "sandbox_generation": sandbox_generation,
            "sandbox_id": sandbox_id,
            "virtual_size": virtual_size,
            "volume_id": volume_id,
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
            device_owner_id=_device_owner_id(
                volume_id=volume_id,
                revision=1,
                operation_id=operation_id,
            ),
            updated_ns=time.time_ns(),
        )

        existing = self.journal.load(volume_id)
        slot = self._device_allocation_slot() if existing is None else suppress()
        with slot:
            reserved = self.journal.reserve_create(
                request=request,
                record=record,
                hard_capacity_bytes=self.config.hard_capacity_bytes,
            )
            if isinstance(reserved, OperationReplay):
                return reserved.record
            record = reserved
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
                    owner_id=record.device_owner_id,
                    reserved_slot=True,
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
                self.journal.finish(record)
                return record
            except BaseException as exc:
                self._best_effort_release(record)
                self.journal.fail(record, f"{type(exc).__name__}: {exc}")
                raise

    @_storage_mutation
    def acquire_snapshot(
        self,
        *,
        sandbox_id: str,
        sandbox_generation: int,
        volume_id: str,
        operation_id: str,
        publication_raw: dict[str, Any],
    ) -> StorageVolumeRecord:
        if self.publisher is None:
            raise StorageNativeConflictError(
                "durable snapshot acquisition is not configured"
            )
        publication = StorageSnapshotPublication.from_dict(publication_raw)
        publication = self.publisher.verify(publication)
        request = {
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
            device_owner_id="",
            sealed_layer_bytes=sum(layer.size for layer in publication.layers),
            published_manifest_digest=publication.manifest_digest,
            published_tag=publication.tag,
            published_repository=publication.repository,
            published_repo_blob_url=publication.repo_blob_url,
            published_backend=publication.backend,
            published_layers=tuple(layer.to_dict() for layer in publication.layers),
            updated_ns=time.time_ns(),
        )
        reserved = self.journal.reserve_import(request=request, record=record)
        if isinstance(reserved, OperationReplay):
            return reserved.record
        record = reserved
        try:
            volume_root.mkdir(mode=0o700, parents=True, exist_ok=False)
            Path(record.mount_path).mkdir(mode=0o700)
            _atomic_write_json(
                Path(record.source_image_config),
                {
                    "repoBlobUrl": publication.repo_blob_url,
                    "lowers": [layer.to_dict() for layer in publication.layers],
                    "resultFile": "",
                    "upper": {},
                },
            )
            record = replace(
                record,
                state=StorageVolumeState.PUBLISHED,
                updated_ns=time.time_ns(),
            )
            self.journal.finish(record)
            return record
        except BaseException as exc:
            if volume_root.exists() and volume_root.is_dir():
                shutil.rmtree(volume_root, ignore_errors=True)
            mount_path = Path(record.mount_path)
            if mount_path.exists():
                with suppress(OSError):
                    mount_path.rmdir()
            self.journal.fail(record, f"{type(exc).__name__}: {exc}")
            raise

    def _begin_transition(
        self,
        *,
        sandbox_id: str,
        sandbox_generation: int,
        volume_id: str,
        operation_id: str,
        expected_revision: int,
        kind: str,
        allowed_states: set[StorageVolumeState],
        next_state: StorageVolumeState,
        reserve_capacity: bool = False,
    ) -> StorageVolumeRecord | OperationReplay:
        request = {
            "expected_revision": expected_revision,
            "kind": kind,
            "operation_id": operation_id,
            "sandbox_generation": sandbox_generation,
            "sandbox_id": sandbox_id,
            "volume_id": volume_id,
        }
        return self.journal.begin_transition(
            request=request,
            operation_id=operation_id,
            kind=kind,
            volume_id=volume_id,
            sandbox_id=sandbox_id,
            sandbox_generation=sandbox_generation,
            expected_revision=expected_revision,
            allowed_states=allowed_states,
            next_state=next_state,
            reserve_capacity=reserve_capacity,
            hard_capacity_bytes=(
                self.config.hard_capacity_bytes if reserve_capacity else 0
            ),
        )

    @_storage_mutation
    def freeze_and_seal(
        self,
        *,
        sandbox_id: str,
        sandbox_generation: int,
        volume_id: str,
        operation_id: str,
        expected_revision: int,
    ) -> StorageVolumeRecord:
        pending = self._begin_transition(
            kind="FreezeAndSeal",
            operation_id=operation_id,
            volume_id=volume_id,
            sandbox_id=sandbox_id,
            sandbox_generation=sandbox_generation,
            expected_revision=expected_revision,
            allowed_states={StorageVolumeState.MOUNTED},
            next_state=StorageVolumeState.SEALING,
        )
        if isinstance(pending, OperationReplay):
            return pending.record
        if pending.device_id is None:
            self.journal.fail(pending, "mounted volume has no block device")
            raise StorageNativeTerminalError("mounted volume has no block device")
        mount_path = Path(pending.mount_path)
        layer_path = (
            Path(pending.runtime_dir)
            / "layers"
            / (f"revision-{pending.revision}.commit")
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
                sealed_layer_bytes=metadata.st_size,
                sealed_layer_paths=(
                    *pending.sealed_layer_paths,
                    str(layer_path),
                ),
                updated_ns=time.time_ns(),
            )
            self.host.unfreeze(mount_path)
            frozen = False
            self.journal.finish(record)
            return record
        except BaseException as exc:
            if frozen:
                try:
                    self.host.unfreeze(mount_path)
                except Exception:
                    pass
                frozen = False
            self.journal.fail(pending, f"{type(exc).__name__}: {exc}")
            raise

    @_storage_mutation
    def mount_snapshot_cow(
        self,
        *,
        sandbox_id: str,
        sandbox_generation: int,
        volume_id: str,
        operation_id: str,
        expected_revision: int,
    ) -> StorageVolumeRecord:
        with self._device_allocation_slot():
            return self._mount_snapshot_cow_with_reserved_device(
                sandbox_id=sandbox_id,
                sandbox_generation=sandbox_generation,
                volume_id=volume_id,
                operation_id=operation_id,
                expected_revision=expected_revision,
            )

    def _mount_snapshot_cow_with_reserved_device(
        self,
        *,
        sandbox_id: str,
        sandbox_generation: int,
        volume_id: str,
        operation_id: str,
        expected_revision: int,
    ) -> StorageVolumeRecord:
        pending = self._begin_transition(
            kind="MountSnapshotCow",
            operation_id=operation_id,
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
        )
        if isinstance(pending, OperationReplay):
            return pending.record
        if not pending.sealed_layer_paths and not pending.published_layers:
            self.journal.fail(pending, "released volume has no sealed layers")
            raise StorageNativeTerminalError("released volume has no sealed layers")
        volume_root = self._volume_root(volume_id)
        runtime_dir = volume_root / f"runtime-{pending.revision}"
        source = volume_root / f"source-{pending.revision}.json"
        pending = replace(
            pending,
            runtime_dir=str(runtime_dir),
            source_image_config=str(source),
            device_owner_id=_device_owner_id(
                volume_id=volume_id,
                revision=pending.revision,
                operation_id=operation_id,
            ),
            updated_ns=time.time_ns(),
        )
        self.journal.update_pending(pending)
        try:
            source_config = {
                "lowers": [
                    *(dict(layer) for layer in pending.published_layers),
                    *({"file": path} for path in pending.sealed_layer_paths),
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
                owner_id=pending.device_owner_id,
                reserved_slot=True,
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
            self.journal.finish(record)
            return record
        except BaseException as exc:
            self._best_effort_release(pending)
            self.journal.fail(pending, f"{type(exc).__name__}: {exc}")
            raise

    @_storage_mutation
    def discard_mounted_cow(
        self,
        *,
        sandbox_id: str,
        sandbox_generation: int,
        volume_id: str,
        operation_id: str,
        expected_revision: int,
    ) -> StorageVolumeRecord:
        """Drop an uncommitted writable upper and restore its parent authority."""

        pending = self._begin_transition(
            kind="DiscardMountedCow",
            operation_id=operation_id,
            volume_id=volume_id,
            sandbox_id=sandbox_id,
            sandbox_generation=sandbox_generation,
            expected_revision=expected_revision,
            allowed_states={StorageVolumeState.MOUNTED},
            next_state=StorageVolumeState.RELEASING,
        )
        if isinstance(pending, OperationReplay):
            return pending.record
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
                device_owner_id="",
                device_id=None,
                device_path="",
                runtime_image_config="",
                updated_ns=time.time_ns(),
            )
            self.journal.finish(record)
            return record
        except BaseException as exc:
            self.journal.fail(pending, f"{type(exc).__name__}: {exc}")
            raise

    @_storage_mutation
    def release_runtime(
        self,
        *,
        sandbox_id: str,
        sandbox_generation: int,
        volume_id: str,
        operation_id: str,
        expected_revision: int,
    ) -> StorageVolumeRecord:
        pending = self._begin_transition(
            kind="ReleaseRuntime",
            operation_id=operation_id,
            volume_id=volume_id,
            sandbox_id=sandbox_id,
            sandbox_generation=sandbox_generation,
            expected_revision=expected_revision,
            allowed_states={StorageVolumeState.SEALED},
            next_state=StorageVolumeState.RELEASING,
        )
        if isinstance(pending, OperationReplay):
            return pending.record
        try:
            mount_path = Path(pending.mount_path)
            if self.host.is_mounted(mount_path):
                self.host.unmount(mount_path)
            if pending.device_id is not None:
                self._release_backend_device(pending.device_id)
            record = replace(
                pending,
                state=StorageVolumeState.RELEASED,
                device_owner_id="",
                device_id=None,
                device_path="",
                runtime_image_config="",
                updated_ns=time.time_ns(),
            )
            self.journal.finish(record)
            return record
        except BaseException as exc:
            self.journal.fail(pending, f"{type(exc).__name__}: {exc}")
            raise

    @_storage_mutation
    def publish_snapshot(
        self,
        *,
        sandbox_id: str,
        sandbox_generation: int,
        volume_id: str,
        operation_id: str,
        expected_revision: int,
    ) -> StorageVolumeRecord:
        if self.publisher is None:
            raise StorageNativeConflictError(
                "durable snapshot publication is not configured"
            )
        pending = self._begin_transition(
            kind="PublishSnapshot",
            operation_id=operation_id,
            volume_id=volume_id,
            sandbox_id=sandbox_id,
            sandbox_generation=sandbox_generation,
            expected_revision=expected_revision,
            allowed_states={StorageVolumeState.RELEASED},
            next_state=StorageVolumeState.PUBLISHING,
        )
        if isinstance(pending, OperationReplay):
            return pending.record
        if not pending.sealed_layer_paths:
            self.journal.fail_transition(
                pending,
                failure_state=StorageVolumeState.RELEASED,
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
                existing_repo_blob_url=pending.published_repo_blob_url,
                global_config_path=self.global_config_path,
            )
            record = replace(
                pending,
                state=StorageVolumeState.PUBLISHED,
                sealed_layer_paths=(),
                cached_layer_paths=(
                    *pending.cached_layer_paths,
                    *(str(path) for path in local_paths),
                ),
                sealed_layer_bytes=sum(layer.size for layer in publication.layers),
                published_manifest_digest=publication.manifest_digest,
                published_tag=publication.tag,
                published_repository=publication.repository,
                published_repo_blob_url=publication.repo_blob_url,
                published_backend=publication.backend,
                published_layers=tuple(layer.to_dict() for layer in publication.layers),
                updated_ns=time.time_ns(),
            )
            self.journal.finish(record)
            self._remove_local_layers(local_paths)
            return record
        except BaseException as exc:
            self.journal.fail_transition(
                pending,
                failure_state=StorageVolumeState.RELEASED,
                error=f"{type(exc).__name__}: {exc}",
            )
            raise

    @_storage_mutation
    def delete_volume(
        self,
        *,
        sandbox_id: str,
        sandbox_generation: int,
        volume_id: str,
        operation_id: str,
        expected_revision: int,
    ) -> StorageVolumeRecord:
        pending = self._begin_transition(
            kind="DeleteVolume",
            operation_id=operation_id,
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
            return pending.record
        try:
            return self._complete_delete(pending)
        except _StorageNativeBackendReleasePending as exc:
            waiting = replace(
                pending,
                error=f"{type(exc).__name__}: {exc}"[:4096],
                updated_ns=time.time_ns(),
            )
            self.journal.update_pending(waiting)
            raise StorageNativePendingOperation(
                "delete is waiting for backend device release"
            ) from exc
        except BaseException as exc:
            current = self.journal.load(volume_id) or pending
            self.journal.fail(current, f"{type(exc).__name__}: {exc}")
            raise

    @_storage_mutation
    def converge_volume(
        self,
        owner: StorageVolumeOwner,
        *,
        action: str,
        operation_id: str,
        publication: StorageSnapshotPublication | None = None,
        virtual_size: int | None = None,
        expected_accounting_id: int | None = None,
    ) -> StorageVolumeRecord:
        if action not in {
            "delete",
            "discard",
            "import",
            "mount",
            "prepare",
            "publish",
            "release",
        }:
            raise ValueError("unknown storage-native convergence action")
        record = self.journal.load(owner.volume_id)
        if action == "prepare" and record is None:
            if virtual_size is None:
                raise ValueError("prepared storage requires a virtual size")
            record = self.create_volume(
                **owner.request_fields(),
                operation_id=_storage_operation_id(
                    owner,
                    operation_id,
                    "prepare-create",
                ),
                virtual_size=virtual_size,
            )
        if action == "import" and (
            record is None or record.state == StorageVolumeState.DELETED
        ):
            if publication is None:
                raise ValueError("imported storage requires a publication")
            record = self.acquire_snapshot(
                **owner.request_fields(),
                operation_id=_storage_operation_id(
                    owner,
                    operation_id,
                    "import-acquire",
                ),
                publication_raw=publication.to_dict(),
            )
        if record is None:
            raise StorageNativeConflictError("storage-native volume does not exist")
        self._require_owner(
            record,
            owner,
            accounting_id=expected_accounting_id,
            virtual_size=(
                publication.virtual_size if publication is not None else virtual_size
            ),
        )
        if (
            publication is not None
            and record.published_manifest_digest != publication.manifest_digest
        ):
            raise StorageNativeConflictError(
                "storage-native import owns another snapshot manifest"
            )
        if action == "delete":
            if record.state != StorageVolumeState.DELETED:
                record = self.delete_volume(
                    **owner.request_fields(),
                    operation_id=_storage_operation_id(owner, operation_id, "delete"),
                    expected_revision=record.revision,
                )
            return record
        if (
            action in {"release", "publish"}
            and record.state == StorageVolumeState.MOUNTED
        ):
            record = self.freeze_and_seal(
                **owner.request_fields(),
                operation_id=_storage_operation_id(owner, operation_id, "seal"),
                expected_revision=record.revision,
            )
        if record.state == StorageVolumeState.SEALED:
            record = self.release_runtime(
                **owner.request_fields(),
                operation_id=_storage_operation_id(owner, operation_id, "release"),
                expected_revision=record.revision,
            )
        if action in {"import", "mount", "prepare"} and record.state in {
            StorageVolumeState.RELEASED,
            StorageVolumeState.PUBLISHED,
        }:
            record = self.mount_snapshot_cow(
                **owner.request_fields(),
                operation_id=_storage_operation_id(owner, operation_id, "mount"),
                expected_revision=record.revision,
            )
        if action == "publish" and record.state == StorageVolumeState.RELEASED:
            record = self.publish_snapshot(
                **owner.request_fields(),
                operation_id=_storage_operation_id(owner, operation_id, "publish"),
                expected_revision=record.revision,
            )
        if action == "discard" and record.state == StorageVolumeState.MOUNTED:
            record = self.discard_mounted_cow(
                **owner.request_fields(),
                operation_id=_storage_operation_id(owner, operation_id, "discard"),
                expected_revision=record.revision,
            )
        expected_states = {
            "discard": {StorageVolumeState.RELEASED, StorageVolumeState.PUBLISHED},
            "import": {StorageVolumeState.MOUNTED},
            "mount": {StorageVolumeState.MOUNTED},
            "prepare": {StorageVolumeState.MOUNTED},
            "publish": {StorageVolumeState.PUBLISHED},
            "release": {StorageVolumeState.RELEASED, StorageVolumeState.PUBLISHED},
        }
        if record.state not in expected_states[action]:
            raise StorageNativeConflictError(
                f"storage-native volume is {record.state.value}; cannot {action}"
            )
        return record

    @staticmethod
    def _require_owner(
        record: StorageVolumeRecord,
        owner: StorageVolumeOwner,
        *,
        accounting_id: int | None = None,
        virtual_size: int | None = None,
    ) -> None:
        if (
            record.owner != owner
            or (accounting_id is not None and record.accounting_id != accounting_id)
            or (virtual_size is not None and record.virtual_size != virtual_size)
        ):
            raise StorageNativeConflictError(
                "storage-native volume belongs to another owner"
            )

    def reconcile(self) -> dict[str, Any]:
        with self._operations.maintenance():
            return self._reconcile_exclusive()

    def _reconcile_exclusive(self) -> dict[str, Any]:
        records = list(self.journal.list())
        live_devices = self.host.ublk_device_ids()
        backend_owners = self._backend_ownership()
        expected_owner_ids = {
            record.device_owner_id for record in records if record.device_owner_id
        }
        orphan_devices = sorted(
            owner.device_id
            for owner_id, owner in backend_owners.items()
            if owner_id not in expected_owner_ids
        )
        deleted_orphans: list[int] = []
        for device_id in orphan_devices:
            self._discard_backend_device(device_id)
            deleted_orphans.append(device_id)

        errors: list[dict[str, Any]] = []
        for index, original in enumerate(records):
            record = original
            owner = (
                backend_owners.get(record.device_owner_id)
                if record.device_owner_id
                else None
            )
            if record.device_id is None and owner is not None:
                record = replace(
                    record,
                    device_id=owner.device_id,
                    device_path=str(owner.device_path),
                    runtime_image_config=str(owner.image_config_path),
                    updated_ns=time.time_ns(),
                )
                self.journal.update_pending(record)
                records[index] = record
            owner_matches = (
                owner is not None
                and record.device_id == owner.device_id
                and record.device_id in live_devices
            )
            if (
                record.state
                in {
                    StorageVolumeState.MOUNTED,
                    StorageVolumeState.ACQUIRING,
                    StorageVolumeState.SEALING,
                    StorageVolumeState.SEALED,
                }
                and not owner_matches
            ):
                updated = self.journal.mark_reconcile_error(
                    record,
                    "journaled block device ownership is missing or mismatched",
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
                    device_owner_id="",
                    device_id=None,
                    device_path="",
                    runtime_image_config="",
                    updated_ns=time.time_ns(),
                )
                self.journal.finish(updated)
            elif record.state == StorageVolumeState.PUBLISHING:
                updated = self.journal.fail_transition(
                    record,
                    failure_state=StorageVolumeState.RELEASED,
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
                try:
                    self._complete_delete(record)
                except _StorageNativeBackendReleasePending as exc:
                    waiting = replace(
                        record,
                        error=f"{type(exc).__name__}: {exc}"[:4096],
                        updated_ns=time.time_ns(),
                    )
                    self.journal.update_pending(waiting)
                    errors.append(self._record_result(waiting))
                except BaseException as exc:
                    current = self.journal.load(record.volume_id) or record
                    updated = self.journal.mark_reconcile_error(
                        current,
                        f"delete reconciliation failed: {type(exc).__name__}: {exc}",
                    )
                    errors.append(self._record_result(updated))

        return {
            "deleted_orphan_device_ids": deleted_orphans,
            "terminal_records": errors,
            "volume_count": len(records),
        }

    def _complete_delete(self, record: StorageVolumeRecord) -> StorageVolumeRecord:
        self._best_effort_release(record, require_backend=True)
        released = replace(
            record,
            device_owner_id="",
            device_id=None,
            device_path="",
            runtime_image_config="",
            error="",
            updated_ns=time.time_ns(),
        )
        if released != record:
            # Persist the backend ownership transfer before local cleanup. A
            # crash after an acknowledged release must not retry the release
            # against a device that is already idle in the warm pool.
            self.journal.update_pending(released)
        volume_root = self._volume_root(record.volume_id)
        if volume_root.exists():
            if volume_root.is_symlink() or not volume_root.is_dir():
                raise StorageNativeTerminalError("volume root is not a real directory")
            shutil.rmtree(volume_root)
        mount_path = Path(record.mount_path)
        if mount_path.exists():
            mount_path.rmdir()
        deleted = replace(
            released,
            state=StorageVolumeState.DELETED,
            sealed_layer_bytes=0,
            sealed_layer_paths=(),
            cached_layer_paths=(),
            published_manifest_digest="",
            published_tag="",
            published_repository="",
            published_repo_blob_url="",
            published_backend="",
            published_layers=(),
            updated_ns=time.time_ns(),
        )
        self.journal.finish(deleted)
        return deleted

    def _best_effort_release(
        self,
        record: StorageVolumeRecord,
        *,
        require_backend: bool = False,
    ) -> None:
        mount_path = Path(record.mount_path)
        mounted = True
        safe_to_pool = True
        device_id = record.device_id
        if device_id is None and record.device_owner_id:
            try:
                owner = self._backend_ownership().get(record.device_owner_id)
            except Exception as exc:
                if require_backend:
                    raise _StorageNativeBackendReleasePending(
                        "cannot inspect backend device ownership"
                    ) from exc
                owner = None
            if owner is not None:
                device_id = owner.device_id
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
        if device_id is not None:
            if require_backend:
                try:
                    owner = self._backend_ownership().get(record.device_owner_id)
                    if owner is None:
                        return
                    if owner.device_id != device_id:
                        raise StorageNativeNodeError(
                            "backend owner identity points to another device"
                        )
                except Exception as exc:
                    raise _StorageNativeBackendReleasePending(
                        f"cannot inspect backend device {device_id}: {exc}"
                    ) from exc
            try:
                if not safe_to_pool:
                    # A lazy-detached or otherwise questionable mount must
                    # never be rebound to another sandbox through the pool.
                    self._discard_backend_device(device_id)
                else:
                    self._release_backend_device(device_id)
            except Exception as exc:
                if require_backend:
                    raise _StorageNativeBackendReleasePending(
                        f"cannot release backend device {device_id}: {exc}"
                    ) from exc

    def _acquire_runtime_device(
        self,
        *,
        source_image_config: Path,
        runtime_dir: Path,
        virtual_size: int,
        owner_id: str,
        reserved_slot: bool = False,
    ) -> StorageNativeDevice:
        idle_before = (
            self.host.ublk_device_ids()
            - {owner.device_id for owner in self._backend_ownership().values()}
            if self.config.device_pool_enabled
            else set()
        )
        with self._device_slot_guard:
            owners = self._backend_ownership()
            existing_owner = owners.get(owner_id)
            demand = len(owners) + self._pending_device_allocations
            if reserved_slot:
                demand -= 1
            if (
                self.config.max_ublk_devices > 0
                and existing_owner is None
                and demand >= self.config.max_ublk_devices
            ):
                raise StorageNativeCapacityError(
                    "storage-native ublk device capacity is exhausted"
                )
            device = self.backend.create_runtime_device(
                source_image_config=source_image_config,
                global_config=self.global_config_path,
                runtime_dir=runtime_dir,
                virtual_size=virtual_size,
                upper_mode=self.config.upper_mode,
                owner_id=owner_id,
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

    @contextmanager
    def _device_allocation_slot(self):
        """Fence the provider's hard ublk-device ceiling before journaling.

        Idle pooled devices are reusable and therefore do not consume an
        admission slot. Active backend owners plus allocations currently
        crossing the backend boundary are the authoritative device demand.
        """

        maximum = self.config.max_ublk_devices
        if maximum <= 0:
            yield
            return
        with self._device_slot_guard:
            active = len(self._backend_ownership())
            if active + self._pending_device_allocations >= maximum:
                raise StorageNativeCapacityError(
                    "storage-native ublk device capacity is exhausted"
                )
            self._pending_device_allocations += 1
        try:
            yield
        finally:
            with self._device_slot_guard:
                self._pending_device_allocations -= 1

    def _backend_ownership(self) -> dict[str, StorageNativeDeviceOwner]:
        owners = self.backend.list_runtime_device_owners()
        by_owner = {owner.owner_id: owner for owner in owners}
        if len(by_owner) != len(owners):
            raise StorageNativeNodeError(
                "block backend returned duplicate device owner identities"
            )
        if len({owner.device_id for owner in owners}) != len(owners):
            raise StorageNativeNodeError(
                "block backend returned duplicate owned device ids"
            )
        return by_owner

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
        telemetry: Telemetry | None = None,
    ) -> None:
        if not socket_path.is_absolute():
            raise ValueError("storage-native service socket must be absolute")
        if timeout_seconds <= 0:
            raise ValueError("storage-native service timeout must be positive")
        self.socket_path = socket_path
        self.timeout_seconds = timeout_seconds
        self.telemetry = telemetry or Telemetry.disabled("storage-native-client")

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

    def prepare_volume(
        self,
        owner: StorageVolumeOwner,
        *,
        operation_id: str,
        virtual_size: int,
    ) -> StorageVolumeRecord:
        return self._record_call(
            "PrepareVolume",
            owner,
            operation_id=operation_id,
            virtual_size=virtual_size,
        )

    def prepare_import(
        self,
        owner: StorageVolumeOwner,
        *,
        operation_id: str,
        publication: StorageSnapshotPublication,
    ) -> StorageVolumeRecord:
        return self._record_call(
            "PrepareImport",
            owner,
            operation_id=operation_id,
            publication=publication.to_dict(),
        )

    def ensure_mounted(
        self,
        owner: StorageVolumeOwner,
        *,
        operation_id: str,
    ) -> StorageVolumeRecord:
        return self._record_call("EnsureMounted", owner, operation_id=operation_id)

    def ensure_released(
        self,
        owner: StorageVolumeOwner,
        *,
        operation_id: str,
    ) -> StorageVolumeRecord:
        return self._record_call("EnsureReleased", owner, operation_id=operation_id)

    def ensure_published(
        self,
        owner: StorageVolumeOwner,
        *,
        operation_id: str,
    ) -> StorageVolumeRecord:
        return self._record_call("EnsurePublished", owner, operation_id=operation_id)

    def discard_resume(
        self,
        owner: StorageVolumeOwner,
        *,
        operation_id: str,
    ) -> StorageVolumeRecord:
        return self._record_call("DiscardResume", owner, operation_id=operation_id)

    def delete_volume(
        self,
        owner: StorageVolumeOwner,
        *,
        operation_id: str,
        expected_accounting_id: int | None = None,
        expected_virtual_size: int | None = None,
    ) -> StorageVolumeRecord:
        return self._record_call(
            "DeleteVolume",
            owner,
            operation_id=operation_id,
            expected_accounting_id=expected_accounting_id,
            expected_virtual_size=expected_virtual_size,
        )

    def get_volume(self, volume_id: str) -> StorageVolumeRecord:
        return self._decode_record(
            self._call(
                {
                    "operation": "GetVolume",
                    "volume_id": volume_id,
                }
            )
        )

    def list_volumes(self) -> tuple[StorageVolumeRecord, ...]:
        """Return live volumes in bounded pages; tombstones remain journaled."""
        inventory: list[StorageVolumeRecord] = []
        after = ""
        while True:
            result = self._call(
                {"operation": "ListVolumesPage", "after_volume_id": after}
            )
            records = result.get("records")
            next_after = result.get("next_after_volume_id")
            try:
                if not isinstance(records, list) or any(
                    not isinstance(raw, dict) for raw in records
                ):
                    raise ValueError("volume inventory entries must be objects")
                page = tuple(StorageVolumeRecord.from_json(raw) for raw in records)
                if not isinstance(next_after, str) or (
                    next_after
                    and (
                        not page
                        or next_after != page[-1].volume_id
                        or next_after <= after
                    )
                ):
                    raise ValueError("invalid volume inventory cursor")
            except (TypeError, ValueError) as exc:
                raise StorageNativeNodeError(
                    "storage-native service returned an invalid volume inventory"
                ) from exc
            inventory.extend(page)
            if not next_after:
                return tuple(inventory)
            after = next_after

    def _record_call(
        self,
        operation: str,
        owner: StorageVolumeOwner,
        **arguments: Any,
    ) -> StorageVolumeRecord:
        return self._decode_record(
            self._call(
                {
                    "operation": operation,
                    **owner.request_fields(),
                    **arguments,
                }
            )
        )

    @staticmethod
    def _decode_record(result: dict[str, Any]) -> StorageVolumeRecord:
        raw = result.get("record")
        if not isinstance(raw, dict):
            raise StorageNativeNodeError(
                "storage-native service returned an invalid volume record"
            )
        try:
            return StorageVolumeRecord.from_json(raw)
        except (TypeError, ValueError) as exc:
            raise StorageNativeNodeError(
                "storage-native service returned an invalid volume record"
            ) from exc

    def reconcile(self) -> dict[str, Any]:
        return self._call({"operation": "Reconcile"})

    def _call(self, request: dict[str, Any]) -> dict[str, Any]:
        operation = str(request.get("operation") or "unknown")
        with self.telemetry.span(
            f"storage.client.{operation}",
            kind=SpanKind.CLIENT,
            attributes={
                "rpc.system": "ucloud-storage-native",
                "rpc.method": operation,
            },
        ):
            return self._call_unobserved(request)

    def _call_unobserved(self, request: dict[str, Any]) -> dict[str, Any]:
        payload = _canonical_json(
            {
                "schema": _PROTOCOL_SCHEMA,
                **request,
                "trace_context": self.telemetry.current_trace_headers(),
            }
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
        if status == "capacity":
            raise StorageNativeCapacityError(message)
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
            request = json.loads(_recv_exact(self.request, size).decode("utf-8"))
            if not isinstance(request, dict):
                raise ValueError("storage-native request must be an object")
            response = self._observed_dispatch(request)
        except StorageNativePendingOperation as exc:
            response = {"status": "pending", "message": str(exc)}
        except StorageNativeCapacityError as exc:
            response = {"status": "capacity", "message": str(exc)}
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

    def _observed_dispatch(self, request: dict[str, Any]) -> dict[str, Any]:
        operation = str(request.get("operation") or "unknown")
        raw_context = request.get("trace_context")
        trace_context = (
            {str(key): str(value) for key, value in raw_context.items()}
            if isinstance(raw_context, dict)
            else {}
        )
        attributes: dict[str, Any] = {
            "rpc.system": "ucloud-storage-native",
            "rpc.method": operation,
        }
        for field, attribute in (
            ("sandbox_id", "sandbox.id"),
            ("volume_id", "storage.volume.id"),
        ):
            value = request.get(field)
            if isinstance(value, str):
                attributes[attribute] = value[:256]
        with self.server.telemetry.span(
            f"storage.server.{operation}",
            kind=SpanKind.SERVER,
            attributes=attributes,
            parent_context=self.server.telemetry.extracted_context(trace_context),
        ) as span:
            if operation in {"GetFeatures", "GetMetrics"}:
                return {"status": "ok", "result": self.server.dispatch(request)}
            waiting_started = time.monotonic()
            self.server.operation_waiting()
            with self.server.operation_slots:
                queue_wait = max(0.0, time.monotonic() - waiting_started)
                span.add_event(
                    "storage.queue.acquired",
                    {"storage.queue.wait_seconds": queue_wait},
                )
                self.server.operation_started()
                try:
                    return {"status": "ok", "result": self.server.dispatch(request)}
                finally:
                    self.server.operation_finished()


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
        telemetry: Telemetry,
    ) -> None:
        self.service = service
        self.require_root_peer_enabled = require_root_peer
        self.telemetry = telemetry
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
        if not isinstance(operation, str) or operation not in _PROTOCOL_EXTRA_FIELDS:
            raise ValueError("unknown storage-native operation")
        if set(request) != {"operation", "schema", *_PROTOCOL_EXTRA_FIELDS[operation]}:
            raise ValueError("storage-native request has an invalid schema")
        if operation == "GetFeatures":
            return {
                "protocol_schema": _PROTOCOL_SCHEMA,
                "storage_schema": "ucloud-storage-native-v1",
                "upper_mode": self.service.config.upper_mode,
                "durable_publication": self.service.publisher is not None,
            }
        if operation == "GetMetrics":
            return self.metrics()
        if operation == "PrepareVolume":
            return self.service._record_result(
                self.service.converge_volume(
                    _volume_owner_from_request(request),
                    action="prepare",
                    operation_id=_string_field(request, "operation_id"),
                    virtual_size=_positive_int_field(request, "virtual_size"),
                )
            )
        if operation == "PrepareImport":
            publication = request.get("publication")
            if not isinstance(publication, dict):
                raise ValueError("publication must be an object")
            return self.service._record_result(
                self.service.converge_volume(
                    _volume_owner_from_request(request),
                    action="import",
                    operation_id=_string_field(request, "operation_id"),
                    publication=StorageSnapshotPublication.from_dict(publication),
                )
            )
        if operation in {
            "DeleteVolume",
            "DiscardResume",
            "EnsureMounted",
            "EnsurePublished",
            "EnsureReleased",
        }:
            owner = _volume_owner_from_request(request)
            operation_id = _string_field(request, "operation_id")
            if operation == "DeleteVolume":
                record = self.service.converge_volume(
                    owner,
                    action="delete",
                    operation_id=operation_id,
                    expected_accounting_id=_optional_positive_int_field(
                        request,
                        "expected_accounting_id",
                    ),
                    virtual_size=_optional_positive_int_field(
                        request,
                        "expected_virtual_size",
                    ),
                )
            else:
                action = {
                    "DiscardResume": "discard",
                    "EnsureMounted": "mount",
                    "EnsurePublished": "publish",
                    "EnsureReleased": "release",
                }[operation]
                record = self.service.converge_volume(
                    owner,
                    action=action,
                    operation_id=operation_id,
                )
            return self.service._record_result(record)
        if operation == "GetVolume":
            record = self.service.journal.load(_string_field(request, "volume_id"))
            if record is None:
                raise StorageNativeConflictError("storage-native volume does not exist")
            return self.service._record_result(record)
        if operation == "ListVolumesPage":
            after = request.get("after_volume_id")
            if not isinstance(after, str):
                raise ValueError("after_volume_id must be a string")
            page = self.service.journal.list_live_page(after)
            records: list[dict[str, Any]] = []
            size = 0
            for record in page:
                raw = record.to_json()
                encoded_size = len(json.dumps(raw, ensure_ascii=True).encode("ascii"))
                if records and size + encoded_size > _PROTOCOL_MAX_BYTES // 2:
                    break
                records.append(raw)
                size += encoded_size
            return {
                "records": records,
                "next_after_volume_id": records[-1]["volume_id"] if records else "",
            }
        if operation == "ListVolumes":
            return {
                "records": [record.to_json() for record in self.service.journal.list()]
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
        telemetry: Telemetry | None = None,
    ) -> None:
        if not socket_path.is_absolute():
            raise ValueError("storage-native service socket must be absolute")
        self.socket_path = socket_path
        self.service = service
        self.require_root_peer = require_root_peer
        self.telemetry = telemetry or Telemetry.disabled("storage-native-server")
        self._server: _StorageNativeUnixServer | None = None

    def serve_forever(self) -> None:
        self._prepare_socket()
        server = _StorageNativeUnixServer(
            self.socket_path,
            self.service,
            require_root_peer=self.require_root_peer,
            telemetry=self.telemetry,
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


def _storage_operation_id(
    owner: StorageVolumeOwner,
    operation_id: str,
    step: str,
) -> str:
    identity = _canonical_json(
        {**owner.request_fields(), "operation_id": operation_id, "step": step}
    )
    return f"storage-{hashlib.sha256(identity.encode('ascii')).hexdigest()}"


def _device_owner_id(
    *,
    volume_id: str,
    revision: int,
    operation_id: str,
) -> str:
    identity = _canonical_json(
        {
            "operation_id": operation_id,
            "revision": revision,
            "volume_id": volume_id,
        }
    )
    return f"device:{hashlib.sha256(identity.encode('ascii')).hexdigest()}"


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


def _optional_positive_int_field(
    request: dict[str, Any],
    name: str,
) -> int | None:
    if request.get(name) is None:
        return None
    return _positive_int_field(request, name)


def _volume_owner_from_request(request: dict[str, Any]) -> StorageVolumeOwner:
    return StorageVolumeOwner(
        volume_id=_string_field(request, "volume_id"),
        sandbox_id=_string_field(request, "sandbox_id"),
        sandbox_generation=_nonnegative_int_field(request, "sandbox_generation"),
    )


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
