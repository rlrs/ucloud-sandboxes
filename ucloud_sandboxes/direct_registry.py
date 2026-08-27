from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, replace
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import time
from types import MappingProxyType
from typing import Any, Iterator, Mapping

from .direct_warden import DirectSandbox
from .sandbox import (
    NodeDrainState,
    OPERATION_ID_RE,
    SandboxSpec,
    sandbox_spec_fingerprint,
)


DIRECT_REGISTRATION_VERSION = 3
_ROOTFS_PHASES = {
    "rootfs_ready",
    "import_ready",
    "owned",
    "moving_out",
}
DIRECT_REGISTRATION_PHASES = _ROOTFS_PHASES | {
    "planned",
    "import_planned",
    "quota_ready",
    "importing",
    "deleting",
}
_DIGEST = re.compile(r"[0-9a-f]{64}\Z")
_CONTAINER_ID = re.compile(r"[0-9a-f]{64}\Z")
_DIRECT_REGISTRY_APPLICATION_ID = 0x55435247
_DIRECT_REGISTRY_SCHEMA_VERSION = 3
_DIRECT_REGISTRY_IDENTITY = (
    _DIRECT_REGISTRY_APPLICATION_ID,
    _DIRECT_REGISTRY_SCHEMA_VERSION,
)


class DirectRegistryError(RuntimeError):
    pass


class DirectRegistryConflictError(DirectRegistryError):
    pass


@dataclass(frozen=True)
class DirectSandboxRegistration:
    spec: SandboxSpec
    sandbox_generation: int
    operation_id: str
    runtime_compatibility_sha256: str
    phase: str
    revision: int
    created_ns: int
    updated_ns: int
    quota_project_id: int | None = None
    quota_total_mb: int | None = None
    quota_path: str = ""
    image_id: str = ""
    rootfs_sha256: str = ""
    container_id: str = ""
    bundle: str = ""
    memory_directory: str = ""
    migration_id: str = ""
    migration_sha256: str = ""
    version: int = DIRECT_REGISTRATION_VERSION

    def __post_init__(self) -> None:
        if self.version != DIRECT_REGISTRATION_VERSION:
            raise ValueError("unsupported direct registration version")
        self.spec.validate()
        if self.sandbox_generation <= 0:
            raise ValueError("sandbox generation must be positive")
        if not self.operation_id or not OPERATION_ID_RE.fullmatch(self.operation_id):
            raise ValueError("direct registration operation id is invalid")
        if not _DIGEST.fullmatch(self.runtime_compatibility_sha256):
            raise ValueError("direct registration runtime compatibility is invalid")
        if self.phase not in DIRECT_REGISTRATION_PHASES:
            raise ValueError("direct registration phase is invalid")
        if self.migration_id and not OPERATION_ID_RE.fullmatch(self.migration_id):
            raise ValueError("direct registration migration id is invalid")
        if self.migration_sha256 and not _DIGEST.fullmatch(self.migration_sha256):
            raise ValueError("direct registration migration digest is invalid")
        migration_phase = self.phase in {
            "import_planned",
            "importing",
            "import_ready",
            "moving_out",
        } or (
            self.phase in {"rootfs_ready", "owned", "deleting"}
            and bool(self.migration_id)
        )
        if bool(self.migration_id) != bool(
            self.migration_sha256
        ) or migration_phase != bool(self.migration_id):
            raise ValueError("direct registration migration ownership is invalid")
        if self.revision < 1 or self.created_ns < 1 or self.updated_ns < 1:
            raise ValueError("direct registration revision/timestamp is invalid")
        quota_parts = (
            self.quota_project_id is not None,
            self.quota_total_mb is not None,
            bool(self.quota_path),
        )
        quota_present = all(quota_parts)
        if any(quota_parts) != quota_present:
            raise ValueError("direct registration quota identity is incomplete")
        if quota_present:
            assert self.quota_project_id is not None
            assert self.quota_total_mb is not None
            if self.quota_project_id < 1 or self.quota_total_mb < 1:
                raise ValueError("direct registration quota bounds are invalid")
            if not Path(self.quota_path).is_absolute():
                raise ValueError("direct registration quota path must be absolute")
        rootfs_parts = tuple(
            bool(value)
            for value in (
                self.image_id,
                self.rootfs_sha256,
                self.container_id,
                self.bundle,
                self.memory_directory,
            )
        )
        rootfs_present = all(rootfs_parts)
        if any(rootfs_parts) != rootfs_present:
            raise ValueError("direct registration rootfs identity is incomplete")
        if rootfs_present:
            if not self.image_id.startswith("sha256:") or not _DIGEST.fullmatch(
                self.image_id[7:]
            ):
                raise ValueError("direct registration image id is invalid")
            if not _DIGEST.fullmatch(self.rootfs_sha256):
                raise ValueError("direct registration rootfs digest is invalid")
            if not _CONTAINER_ID.fullmatch(self.container_id):
                raise ValueError("direct registration container id is invalid")
            if not Path(self.bundle).is_absolute():
                raise ValueError("direct registration bundle must be absolute")
            if "/" in self.memory_directory or not self.memory_directory:
                raise ValueError("direct registration memory directory is invalid")
        if self.phase in {"planned", "import_planned"} and (
            quota_present or rootfs_present
        ):
            raise ValueError("planned direct registration owns external state")
        if self.phase in {"quota_ready", "importing"} and (
            not quota_present or rootfs_present
        ):
            raise ValueError("quota-ready direct registration is inconsistent")
        if self.phase in _ROOTFS_PHASES and (
            not quota_present or not rootfs_present
        ):
            raise ValueError("direct registration is missing owned resources")

    @property
    def sandbox_id(self) -> str:
        return self.spec.id

    @property
    def spec_sha256(self) -> str:
        return sandbox_spec_fingerprint(self.spec)

    @property
    def has_direct_sandbox(self) -> bool:
        """Whether this registration owns a materialized runsc sandbox."""

        return bool(self.container_id)

    def to_direct_sandbox(self) -> DirectSandbox:
        if not self.has_direct_sandbox:
            raise DirectRegistryError("registration has no direct sandbox yet")
        return DirectSandbox(
            sandbox_id=self.sandbox_id,
            sandbox_generation=self.sandbox_generation,
            container_id=self.container_id,
            spec_sha256=self.spec_sha256,
            rootfs_sha256=self.rootfs_sha256,
            bundle=Path(self.bundle),
            memory_directory=self.memory_directory,
        )

    def to_dict(self) -> dict[str, Any]:
        raw = vars(self).copy()
        raw["spec"] = self.spec.to_dict()
        return raw

    @classmethod
    def from_dict(cls, raw: object) -> DirectSandboxRegistration:
        if not isinstance(raw, dict):
            raise DirectRegistryError("direct registration must be an object")
        if (
            raw.get("version") != DIRECT_REGISTRATION_VERSION
            or set(raw) != set(cls.__dataclass_fields__)
            or not isinstance(raw["spec"], dict)
        ):
            raise DirectRegistryError("direct registration schema is invalid")
        integer_fields = (
            "created_ns",
            "revision",
            "sandbox_generation",
            "updated_ns",
            "version",
        )
        non_strings = {
            *integer_fields,
            "quota_project_id",
            "quota_total_mb",
            "spec",
        }
        if (
            any(type(raw[field]) is not str for field in set(raw) - non_strings)
            or any(type(raw[field]) is not int for field in integer_fields)
            or any(
                raw[field] is not None and type(raw[field]) is not int
                for field in ("quota_project_id", "quota_total_mb")
            )
        ):
            raise DirectRegistryError("direct registration schema is invalid")
        try:
            values = dict(raw)
            values["spec"] = SandboxSpec.from_dict(raw["spec"])
            return cls(**values)
        except (TypeError, ValueError) as exc:
            raise DirectRegistryError("direct registration is invalid") from exc


@dataclass(frozen=True)
class DirectRegistrySnapshot:
    """One coherent, indexed view of the durable direct registry."""

    records: tuple[DirectSandboxRegistration, ...]
    by_sandbox_id: Mapping[str, DirectSandboxRegistration]
    image_ids: frozenset[str]
    activity_revision: int

    def get(self, sandbox_id: str) -> DirectSandboxRegistration | None:
        return self.by_sandbox_id.get(sandbox_id)


class DirectSandboxRegistry:
    """SQLite-backed ownership bridge from admission through Warden create."""

    _SCHEMA = """
        CREATE TABLE registry_metadata (
            singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
            activity_revision INTEGER NOT NULL CHECK (activity_revision >= 0),
            runtime_compatibility_sha256 TEXT CHECK (
                runtime_compatibility_sha256 IS NULL OR (
                    length(runtime_compatibility_sha256) = 64 AND
                    runtime_compatibility_sha256 NOT GLOB '*[^0-9a-f]*'
                )
            ),
            drain_json TEXT NOT NULL CHECK (json_valid(drain_json))
        ) STRICT;
        CREATE TABLE registrations (
            sandbox_id TEXT PRIMARY KEY,
            image_id TEXT NOT NULL,
            record_json TEXT NOT NULL CHECK (json_valid(record_json))
        ) STRICT;
        CREATE TABLE generation_tombstones (
            sandbox_id TEXT PRIMARY KEY,
            generation INTEGER NOT NULL CHECK (generation > 0)
        ) STRICT;
        CREATE TABLE migration_tombstones (
            sandbox_id TEXT NOT NULL,
            migration_id TEXT NOT NULL,
            PRIMARY KEY (sandbox_id, migration_id)
        ) STRICT;
        CREATE INDEX registrations_image_id ON registrations (image_id);
        INSERT INTO registry_metadata VALUES (
            1,
            0,
            NULL,
            '{"admission_open":true,"drain_activity_epoch":0,"draining":false,"token":""}'
        );
    """

    def __init__(self, path: Path) -> None:
        if not path.is_absolute():
            raise ValueError("direct registry path must be absolute")
        self.path = path

    def bind_runtime_compatibility(
        self,
        expected_sha256: str,
    ) -> str:
        if not _DIGEST.fullmatch(expected_sha256):
            raise ValueError("runtime compatibility digest is invalid")
        with self._transaction(write=True) as connection:
            _activity, actual, _drain = self._metadata(connection)
            if actual is not None and actual != expected_sha256:
                raise DirectRegistryError(
                    "node state belongs to another runtime compatibility"
                )
            if any(
                self._decode(row).runtime_compatibility_sha256 != expected_sha256
                for row in connection.execute(
                    "SELECT sandbox_id, image_id, record_json FROM registrations"
                )
            ):
                raise DirectRegistryError(
                    "direct registry contains another runtime compatibility"
                )
            if (
                actual is None
                and connection.execute(
                    "UPDATE registry_metadata SET runtime_compatibility_sha256 = ? "
                    "WHERE singleton = 1 "
                    "AND runtime_compatibility_sha256 IS NULL",
                    (expected_sha256,),
                ).rowcount
                != 1
            ):
                raise DirectRegistryError("direct registry metadata changed")
            return actual or expected_sha256

    def load_drain(self) -> NodeDrainState:
        with self._transaction(write=False) as connection:
            return self._metadata(connection)[2]

    def save_drain(self, drain: NodeDrainState) -> None:
        encoded = _canonical_json(drain.to_dict())
        self._decode_drain(encoded)
        with self._transaction(write=True) as connection:
            if (
                connection.execute(
                    "UPDATE registry_metadata SET drain_json = ? WHERE singleton = 1",
                    (encoded,),
                ).rowcount
                != 1
            ):
                raise DirectRegistryError("direct registry metadata changed")

    def plan(
        self,
        *,
        spec: SandboxSpec,
        sandbox_generation: int,
        operation_id: str,
        runtime_compatibility_sha256: str,
    ) -> DirectSandboxRegistration:
        if sandbox_generation <= 0:
            raise ValueError("sandbox generation must be positive")
        now = time.time_ns()
        return self._plan(
            DirectSandboxRegistration(
                spec=spec,
                sandbox_generation=sandbox_generation,
                operation_id=operation_id,
                runtime_compatibility_sha256=runtime_compatibility_sha256,
                phase="planned",
                revision=1,
                created_ns=now,
                updated_ns=now,
            ),
            imported=False,
        )

    def plan_import(
        self,
        *,
        spec: SandboxSpec,
        sandbox_generation: int,
        operation_id: str,
        runtime_compatibility_sha256: str,
        migration_id: str,
        migration_sha256: str,
    ) -> DirectSandboxRegistration:
        if sandbox_generation <= 0:
            raise ValueError("sandbox generation must be positive")
        now = time.time_ns()
        return self._plan(
            DirectSandboxRegistration(
                spec=spec,
                sandbox_generation=sandbox_generation,
                operation_id=operation_id,
                runtime_compatibility_sha256=runtime_compatibility_sha256,
                phase="import_planned",
                revision=1,
                created_ns=now,
                updated_ns=now,
                migration_id=migration_id,
                migration_sha256=migration_sha256,
            ),
            imported=True,
        )

    def commit_quota(
        self,
        sandbox_id: str,
        *,
        expected_revision: int,
        project_id: int,
        total_mb: int,
        quota_path: Path,
    ) -> DirectSandboxRegistration:
        return self._transition(
            sandbox_id,
            expected_revision,
            "planned",
            "quota_ready",
            quota_project_id=project_id,
            quota_total_mb=total_mb,
            quota_path=str(quota_path),
        )

    def commit_import_quota(
        self,
        sandbox_id: str,
        *,
        expected_revision: int,
        project_id: int,
        total_mb: int,
        quota_path: Path,
    ) -> DirectSandboxRegistration:
        return self._transition(
            sandbox_id,
            expected_revision,
            "import_planned",
            "importing",
            quota_project_id=project_id,
            quota_total_mb=total_mb,
            quota_path=str(quota_path),
        )

    def abort_import_planned(
        self,
        sandbox_id: str,
        *,
        expected_revision: int,
        migration_id: str,
        migration_sha256: str,
        retire: bool = True,
    ) -> None:
        self._abort_plan(
            sandbox_id,
            expected_revision,
            "import_planned",
            "import plan abort lost its ownership fence",
            fence=(migration_id, migration_sha256),
            retire=retire,
        )

    def commit_rootfs(
        self,
        sandbox_id: str,
        *,
        expected_revision: int,
        image_id: str,
        sandbox: DirectSandbox,
    ) -> DirectSandboxRegistration:
        return self._commit_rootfs(
            sandbox_id, expected_revision, "quota_ready", image_id, sandbox
        )

    def commit_import_rootfs(
        self,
        sandbox_id: str,
        *,
        expected_revision: int,
        image_id: str,
        sandbox: DirectSandbox,
    ) -> DirectSandboxRegistration:
        return self._commit_rootfs(
            sandbox_id, expected_revision, "importing", image_id, sandbox
        )

    def commit_import_ready(
        self,
        sandbox_id: str,
        *,
        expected_revision: int,
        migration_id: str,
        migration_sha256: str,
    ) -> DirectSandboxRegistration:
        return self._transition(
            sandbox_id,
            expected_revision,
            "rootfs_ready",
            "import_ready",
            fence=(migration_id, migration_sha256),
            error="import readiness lost its ownership fence",
        )

    def activate_import(
        self,
        sandbox_id: str,
        *,
        expected_revision: int,
        migration_id: str,
        migration_sha256: str,
    ) -> DirectSandboxRegistration:
        return self._transition(
            sandbox_id,
            expected_revision,
            "import_ready",
            "owned",
            fence=(migration_id, migration_sha256),
            error="import activation lost its ownership fence",
        )

    def begin_move_out(
        self,
        sandbox_id: str,
        *,
        expected_revision: int,
        migration_id: str,
        migration_sha256: str,
    ) -> DirectSandboxRegistration:
        return self._transition(
            sandbox_id,
            expected_revision,
            "owned",
            "moving_out",
            error="move preparation lost its ownership fence",
            retire=True,
            migration_id=migration_id,
            migration_sha256=migration_sha256,
        )

    def abort_move_out(
        self,
        sandbox_id: str,
        *,
        expected_revision: int,
        migration_id: str,
        migration_sha256: str,
    ) -> DirectSandboxRegistration:
        return self._transition(
            sandbox_id,
            expected_revision,
            "moving_out",
            "owned",
            fence=(migration_id, migration_sha256),
            error="move abort lost its ownership fence",
            migration_id="",
            migration_sha256="",
        )

    def commit_owned(
        self,
        sandbox_id: str,
        *,
        expected_revision: int,
    ) -> DirectSandboxRegistration:
        return self._transition(sandbox_id, expected_revision, "rootfs_ready", "owned")

    def begin_delete(
        self,
        sandbox_id: str,
        *,
        expected_revision: int,
    ) -> DirectSandboxRegistration:
        return self._transition(
            sandbox_id,
            expected_revision,
            {"planned", "quota_ready", "rootfs_ready", "owned"},
            "deleting",
        )

    def begin_delete_moved(
        self,
        sandbox_id: str,
        *,
        expected_revision: int,
        migration_id: str,
        migration_sha256: str,
    ) -> DirectSandboxRegistration:
        return self._transition(
            sandbox_id,
            expected_revision,
            "moving_out",
            "deleting",
            fence=(migration_id, migration_sha256),
            error="move finalization lost its ownership fence",
        )

    def begin_delete_import(
        self,
        sandbox_id: str,
        *,
        expected_revision: int,
        migration_id: str,
        migration_sha256: str,
    ) -> DirectSandboxRegistration:
        return self._transition(
            sandbox_id,
            expected_revision,
            {"importing", "rootfs_ready", "import_ready"},
            "deleting",
            fence=(migration_id, migration_sha256),
            error="import abort lost its ownership fence",
        )

    def commit_deleted(
        self,
        sandbox_id: str,
        *,
        sandbox_generation: int,
        expected_revision: int,
    ) -> None:
        if sandbox_generation <= 0:
            raise ValueError("sandbox generation must be positive")
        with self._transaction(write=True) as connection:
            record = self._require(connection, sandbox_id)
            if (
                record.phase != "deleting"
                or record.revision != expected_revision
                or record.sandbox_generation != sandbox_generation
            ):
                raise DirectRegistryConflictError(
                    "direct deletion completion lost its ownership fence"
                )
            connection.execute(
                """
                INSERT INTO generation_tombstones VALUES (?, ?)
                ON CONFLICT (sandbox_id) DO UPDATE SET
                    generation = MAX(generation, excluded.generation)
                """,
                (sandbox_id, sandbox_generation),
            )
            if record.migration_id:
                self._retire(connection, sandbox_id, record.migration_id)
            if (
                connection.execute(
                    "DELETE FROM registrations WHERE sandbox_id = ?",
                    (sandbox_id,),
                ).rowcount
                != 1
            ):
                raise DirectRegistryError("direct registration disappeared")
            self._bump_activity(connection)

    def get(self, sandbox_id: str) -> DirectSandboxRegistration | None:
        with self._transaction(write=False) as connection:
            return self._get(connection, sandbox_id)

    def list(self) -> tuple[DirectSandboxRegistration, ...]:
        return self.snapshot().records

    def snapshot(self) -> DirectRegistrySnapshot:
        """Return records, indexes, roots, and revision from one durable read."""

        with self._transaction(write=False) as connection:
            activity_revision = self._metadata(connection)[0]
            records = tuple(
                self._decode(row)
                for row in connection.execute(
                    """
                    SELECT sandbox_id, image_id, record_json
                    FROM registrations ORDER BY sandbox_id
                    """
                )
            )
        if activity_revision < max((record.revision for record in records), default=0):
            raise DirectRegistryError("direct registry activity revision is invalid")
        by_id = {record.sandbox_id: record for record in records}
        return DirectRegistrySnapshot(
            records=records,
            by_sandbox_id=MappingProxyType(by_id),
            image_ids=frozenset(
                record.image_id for record in records if record.image_id
            ),
            activity_revision=activity_revision,
        )

    def references_image(self, image_id: str) -> bool:
        """Check one Docker image root from the durable image-id index."""

        with self._transaction(write=False) as connection:
            row = connection.execute(
                """
                SELECT sandbox_id, image_id, record_json FROM registrations
                WHERE image_id = ? LIMIT 1
                """,
                (image_id,),
            ).fetchone()
            if row is not None:
                self._decode(row)
            return row is not None

    def _plan(
        self,
        candidate: DirectSandboxRegistration,
        *,
        imported: bool,
    ) -> DirectSandboxRegistration:
        with self._transaction(write=True) as connection:
            _activity, compatibility, _drain = self._metadata(connection)
            if (
                compatibility is not None
                and candidate.runtime_compatibility_sha256 != compatibility
            ):
                raise DirectRegistryError(
                    "direct registration belongs to another runtime compatibility"
                )
            existing = self._get(connection, candidate.sandbox_id)
            if existing is not None:
                replay = (
                    existing.sandbox_generation == candidate.sandbox_generation
                    and existing.operation_id == candidate.operation_id
                    and existing.spec == candidate.spec
                    and existing.runtime_compatibility_sha256
                    == candidate.runtime_compatibility_sha256
                    and (
                        not imported
                        or (
                            existing.migration_id == candidate.migration_id
                            and existing.migration_sha256 == candidate.migration_sha256
                        )
                    )
                )
                if replay:
                    return existing
                raise DirectRegistryConflictError(
                    "sandbox already has another direct registration"
                )
            if imported:
                fenced = connection.execute(
                    """
                    SELECT 1 FROM migration_tombstones
                    WHERE sandbox_id = ? AND migration_id = ?
                    """,
                    (candidate.sandbox_id, candidate.migration_id),
                ).fetchone()
                error = "migration import is fenced by a tombstone"
            else:
                row = connection.execute(
                    """
                    SELECT generation FROM generation_tombstones
                    WHERE sandbox_id = ?
                    """,
                    (candidate.sandbox_id,),
                ).fetchone()
                fenced = row is not None and row[0] >= candidate.sandbox_generation
                error = "direct registration is fenced by a tombstone"
            if fenced:
                raise DirectRegistryConflictError(error)
            self._write(connection, candidate, insert=True)
            self._bump_activity(connection)
        return candidate

    def _commit_rootfs(
        self,
        sandbox_id: str,
        revision: int,
        expected_phase: str,
        image_id: str,
        sandbox: DirectSandbox,
    ) -> DirectSandboxRegistration:
        return self._transition(
            sandbox_id,
            revision,
            expected_phase,
            "rootfs_ready",
            image_id=image_id,
            rootfs_sha256=sandbox.rootfs_sha256,
            container_id=sandbox.container_id,
            bundle=str(sandbox.bundle),
            memory_directory=sandbox.memory_directory,
        )

    def _transition(
        self,
        sandbox_id: str,
        revision: int,
        expected: str | set[str],
        phase: str,
        *,
        fence: tuple[str, str] | None = None,
        error: str = "direct registration transition lost its ownership fence",
        retire: bool = False,
        **changes: Any,
    ) -> DirectSandboxRegistration:
        with self._transaction(write=True) as connection:
            record = self._require(connection, sandbox_id)
            phase_matches = (
                record.phase in expected
                if isinstance(expected, set)
                else record.phase == expected
            )
            if (
                record.revision != revision
                or not phase_matches
                or (
                    fence is not None
                    and (record.migration_id, record.migration_sha256) != fence
                )
            ):
                raise DirectRegistryConflictError(error)
            if retire and record.migration_id:
                self._retire(connection, sandbox_id, record.migration_id)
            updated = replace(
                record,
                phase=phase,
                revision=record.revision + 1,
                updated_ns=time.time_ns(),
                **changes,
            )
            self._write(connection, updated)
            self._bump_activity(connection)
        return updated

    def _abort_plan(
        self,
        sandbox_id: str,
        revision: int,
        phase: str,
        error: str,
        *,
        fence: tuple[str, str] | None = None,
        retire: bool = False,
    ) -> None:
        with self._transaction(write=True) as connection:
            record = self._require(connection, sandbox_id)
            if (
                record.phase != phase
                or record.revision != revision
                or (
                    fence is not None
                    and (record.migration_id, record.migration_sha256) != fence
                )
            ):
                raise DirectRegistryConflictError(error)
            if retire:
                assert fence is not None
                self._retire(connection, sandbox_id, fence[0])
            if (
                connection.execute(
                    "DELETE FROM registrations WHERE sandbox_id = ?",
                    (sandbox_id,),
                ).rowcount
                != 1
            ):
                raise DirectRegistryError("direct registration disappeared")
            self._bump_activity(connection)

    @staticmethod
    def _encode(record: DirectSandboxRegistration) -> str:
        return _canonical_json(record.to_dict())

    @classmethod
    def _decode(cls, row: object) -> DirectSandboxRegistration:
        if (
            not isinstance(row, tuple)
            or len(row) != 3
            or any(not isinstance(value, str) for value in row)
        ):
            raise DirectRegistryError("direct registration row is invalid")
        sandbox_id, image_id, encoded = row
        try:
            record = DirectSandboxRegistration.from_dict(json.loads(encoded))
        except (TypeError, json.JSONDecodeError) as exc:
            raise DirectRegistryError(
                "direct registration encoding is invalid"
            ) from exc
        if (record.sandbox_id, record.image_id) != (
            sandbox_id,
            image_id,
        ) or cls._encode(record) != encoded:
            raise DirectRegistryError("direct registration encoding is invalid")
        return record

    @classmethod
    def _get(
        cls,
        connection: sqlite3.Connection,
        sandbox_id: str,
    ) -> DirectSandboxRegistration | None:
        row = connection.execute(
            """
            SELECT sandbox_id, image_id, record_json FROM registrations
            WHERE sandbox_id = ?
            """,
            (sandbox_id,),
        ).fetchone()
        return cls._decode(row) if row else None

    @classmethod
    def _require(
        cls,
        connection: sqlite3.Connection,
        sandbox_id: str,
    ) -> DirectSandboxRegistration:
        record = cls._get(connection, sandbox_id)
        if record is None:
            raise DirectRegistryConflictError("direct registration is absent")
        return record

    @classmethod
    def _write(
        cls,
        connection: sqlite3.Connection,
        record: DirectSandboxRegistration,
        *,
        insert: bool = False,
    ) -> None:
        encoded = cls._encode(record)
        if insert:
            connection.execute(
                "INSERT INTO registrations VALUES (?, ?, ?)",
                (record.sandbox_id, record.image_id, encoded),
            )
        elif (
            connection.execute(
                """
            UPDATE registrations SET image_id = ?, record_json = ?
            WHERE sandbox_id = ?
            """,
                (record.image_id, encoded, record.sandbox_id),
            ).rowcount
            != 1
        ):
            raise DirectRegistryError("direct registration disappeared")

    @staticmethod
    def _retire(
        connection: sqlite3.Connection,
        sandbox_id: str,
        migration_id: str,
    ) -> None:
        connection.execute(
            "INSERT OR IGNORE INTO migration_tombstones VALUES (?, ?)",
            (sandbox_id, migration_id),
        )

    @classmethod
    def _metadata(
        cls,
        connection: sqlite3.Connection,
    ) -> tuple[int, str | None, NodeDrainState]:
        row = connection.execute(
            "SELECT activity_revision, runtime_compatibility_sha256, drain_json "
            "FROM registry_metadata WHERE singleton = 1"
        ).fetchone()
        if (
            row is None
            or type(row[0]) is not int
            or row[0] < 0
            or (
                row[1] is not None
                and (not isinstance(row[1], str) or not _DIGEST.fullmatch(row[1]))
            )
        ):
            raise DirectRegistryError("direct registry metadata is invalid")
        return row[0], row[1], cls._decode_drain(row[2])

    @staticmethod
    def _decode_drain(encoded: str) -> NodeDrainState:
        try:
            drain = NodeDrainState.from_dict(json.loads(encoded))
            if _canonical_json(drain.to_dict()) != encoded:
                raise ValueError("noncanonical metadata")
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise DirectRegistryError("direct registry metadata is invalid") from exc
        return drain

    @staticmethod
    def _bump_activity(connection: sqlite3.Connection) -> None:
        if (
            connection.execute(
                """
            UPDATE registry_metadata
            SET activity_revision = activity_revision + 1
            """
            ).rowcount
            != 1
        ):
            raise DirectRegistryError("direct registry metadata is invalid")

    @contextmanager
    def _transaction(
        self,
        *,
        write: bool,
    ) -> Iterator[sqlite3.Connection]:
        connection: sqlite3.Connection | None = None
        try:
            self._prepare_file()
            connection = sqlite3.connect(self.path, timeout=30.0, isolation_level=None)
            connection.execute("PRAGMA trusted_schema = OFF")
            connection.execute("PRAGMA synchronous = FULL")
            self._ensure_schema(connection)
            connection.execute("BEGIN IMMEDIATE" if write else "BEGIN")
            yield connection
            connection.commit()
        except BaseException as exc:
            if connection is not None:
                connection.rollback()
            if isinstance(exc, DirectRegistryError):
                raise
            if isinstance(exc, (OSError, sqlite3.DatabaseError)):
                raise DirectRegistryError("direct registry is unreadable") from exc
            raise
        finally:
            if connection is not None:
                connection.close()

    def _prepare_file(self) -> None:
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        parent = self.path.parent.lstat()
        if (
            not stat.S_ISDIR(parent.st_mode)
            or parent.st_uid != os.geteuid()
            or parent.st_mode & 0o022
        ):
            raise DirectRegistryError(
                "direct registry directory must be private and owned"
            )
        try:
            descriptor = os.open(
                self.path,
                os.O_RDWR
                | os.O_CREAT
                | os.O_EXCL
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0),
                0o600,
            )
        except FileExistsError:
            pass
        else:
            os.close(descriptor)
            directory = os.open(
                self.path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
            )
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        info = self.path.lstat()
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.geteuid()
            or info.st_mode & 0o077
        ):
            raise DirectRegistryError(
                "direct registry must be private, regular, and owned"
            )

    @classmethod
    def _ensure_schema(cls, connection: sqlite3.Connection) -> None:
        if cls._versions(connection) != _DIRECT_REGISTRY_IDENTITY:
            if cls._enable_wal(connection) != ("wal",):
                raise DirectRegistryError(
                    "direct registry cannot enable durable journaling"
                )
            connection.execute("BEGIN IMMEDIATE")
            has_schema = connection.execute(
                "SELECT 1 FROM sqlite_schema " "WHERE name NOT LIKE 'sqlite_%' LIMIT 1"
            ).fetchone()
            if cls._versions(connection) == (0, 0) and not has_schema:
                for statement in cls._SCHEMA.split(";"):
                    if statement.strip():
                        connection.execute(statement)
                connection.execute(
                    f"PRAGMA application_id = {_DIRECT_REGISTRY_APPLICATION_ID}"
                )
                connection.execute(
                    f"PRAGMA user_version = {_DIRECT_REGISTRY_SCHEMA_VERSION}"
                )
            cls._validate_schema(connection)
            connection.commit()
        else:
            cls._validate_schema(connection)

    @staticmethod
    def _enable_wal(connection: sqlite3.Connection) -> tuple[Any, ...] | None:
        deadline = time.monotonic() + 30.0
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

    @staticmethod
    def _versions(
        connection: sqlite3.Connection,
    ) -> tuple[int, int]:
        return (
            connection.execute("PRAGMA application_id").fetchone()[0],
            connection.execute("PRAGMA user_version").fetchone()[0],
        )

    @classmethod
    def _validate_schema(cls, connection: sqlite3.Connection) -> None:
        expected = {
            statement.split()[2]: statement
            for raw in cls._SCHEMA.split(";")
            if (statement := raw.strip()).startswith("CREATE ")
        }
        actual = dict(
            connection.execute(
                "SELECT name, sql FROM sqlite_schema "
                "WHERE type IN ('table', 'index') AND name NOT LIKE 'sqlite_%'"
            )
        )
        if (
            cls._versions(connection) != _DIRECT_REGISTRY_IDENTITY
            or connection.execute("PRAGMA journal_mode").fetchone() != ("wal",)
            or actual != expected
        ):
            raise DirectRegistryError("direct registry schema is invalid")
        cls._metadata(connection)


def _canonical_json(payload: object) -> str:
    return json.dumps(
        payload,
        separators=(",", ":"),
        sort_keys=True,
    )
