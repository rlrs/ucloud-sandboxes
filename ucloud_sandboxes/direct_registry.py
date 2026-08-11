from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, replace
import fcntl
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import tempfile
from threading import RLock
import time
from types import MappingProxyType
from typing import Any, Iterator, Mapping

from .direct_warden import DirectSandbox
from .sandbox import (
    OPERATION_ID_RE,
    SandboxSpec,
    sandbox_spec_fingerprint,
)


DIRECT_REGISTRY_VERSION = 3
DIRECT_REGISTRATION_VERSION = 2
DIRECT_REGISTRY_MAX_BYTES = 16 * 1024 * 1024
DIRECT_REGISTRY_MAX_INLINE_TOMBSTONES = 4096
DIRECT_REGISTRY_MAX_INLINE_MIGRATION_TOMBSTONES = 4096
DIRECT_REGISTRY_MAX_MIGRATION_TOMBSTONES_PER_SANDBOX = 256
DIRECT_REGISTRATION_PHASES = {
    "planned",
    "import_planned",
    "quota_ready",
    "importing",
    "rootfs_ready",
    "import_ready",
    "owned",
    "moving_out",
    "deleting",
}
_DIGEST = re.compile(r"[0-9a-f]{64}\Z")
_CONTAINER_ID = re.compile(r"[0-9a-f]{64}\Z")
_CACHE_UNINITIALIZED = object()


class DirectRegistryError(RuntimeError):
    pass


class DirectRegistryConflictError(DirectRegistryError):
    pass


@dataclass(frozen=True)
class DirectSandboxRegistration:
    spec: SandboxSpec
    sandbox_generation: int
    operation_id: str
    runtime_identity_sha256: str
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
        if not _DIGEST.fullmatch(self.runtime_identity_sha256):
            raise ValueError("direct registration runtime identity is invalid")
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
        if migration_phase and not self.migration_id:
            raise ValueError("migration registration has no migration id")
        if migration_phase and not self.migration_sha256:
            raise ValueError("migration registration has no snapshot digest")
        if not migration_phase and (self.migration_id or self.migration_sha256):
            raise ValueError("ordinary registration has migration ownership")
        if self.revision < 1 or self.created_ns < 1 or self.updated_ns < 1:
            raise ValueError("direct registration revision/timestamp is invalid")
        quota_values = (
            self.quota_project_id,
            self.quota_total_mb,
            self.quota_path,
        )
        quota_present = all(value not in {None, ""} for value in quota_values)
        if any(value not in {None, ""} for value in quota_values) != quota_present:
            raise ValueError("direct registration quota identity is incomplete")
        if quota_present:
            assert self.quota_project_id is not None
            assert self.quota_total_mb is not None
            if self.quota_project_id < 1 or self.quota_total_mb < 1:
                raise ValueError("direct registration quota bounds are invalid")
            if not Path(self.quota_path).is_absolute():
                raise ValueError("direct registration quota path must be absolute")
        rootfs_values = (
            self.image_id,
            self.rootfs_sha256,
            self.container_id,
            self.bundle,
            self.memory_directory,
        )
        rootfs_present = all(bool(value) for value in rootfs_values)
        if any(bool(value) for value in rootfs_values) != rootfs_present:
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
        if self.phase in {
            "rootfs_ready",
            "import_ready",
            "owned",
            "moving_out",
            "deleting",
        } and (not quota_present or not rootfs_present):
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

        return self.phase in {
            "rootfs_ready",
            "import_ready",
            "owned",
            "moving_out",
            "deleting",
        }

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
        return {
            "bundle": self.bundle,
            "container_id": self.container_id,
            "created_ns": self.created_ns,
            "image_id": self.image_id,
            "memory_directory": self.memory_directory,
            "migration_id": self.migration_id,
            "migration_sha256": self.migration_sha256,
            "operation_id": self.operation_id,
            "phase": self.phase,
            "quota_path": self.quota_path,
            "quota_project_id": self.quota_project_id,
            "quota_total_mb": self.quota_total_mb,
            "revision": self.revision,
            "rootfs_sha256": self.rootfs_sha256,
            "runtime_identity_sha256": self.runtime_identity_sha256,
            "sandbox_generation": self.sandbox_generation,
            "spec": self.spec.to_dict(),
            "updated_ns": self.updated_ns,
            "version": self.version,
        }

    @classmethod
    def from_dict(cls, raw: object) -> DirectSandboxRegistration:
        if not isinstance(raw, dict):
            raise DirectRegistryError("direct registration must be an object")
        expected = {
            "bundle",
            "container_id",
            "created_ns",
            "image_id",
            "memory_directory",
            "migration_id",
            "migration_sha256",
            "operation_id",
            "phase",
            "quota_path",
            "quota_project_id",
            "quota_total_mb",
            "revision",
            "rootfs_sha256",
            "runtime_identity_sha256",
            "sandbox_generation",
            "spec",
            "updated_ns",
            "version",
        }
        if (
            raw.get("version") != DIRECT_REGISTRATION_VERSION
            or set(raw) != expected
            or not isinstance(raw["spec"], dict)
        ):
            raise DirectRegistryError("direct registration schema is invalid")
        try:
            return cls(
                bundle=str(raw["bundle"]),
                container_id=str(raw["container_id"]),
                created_ns=int(raw["created_ns"]),
                image_id=str(raw["image_id"]),
                memory_directory=str(raw["memory_directory"]),
                migration_id=str(raw.get("migration_id") or ""),
                migration_sha256=str(raw.get("migration_sha256") or ""),
                operation_id=str(raw["operation_id"]),
                phase=str(raw["phase"]),
                quota_path=str(raw["quota_path"]),
                quota_project_id=(
                    int(raw["quota_project_id"])
                    if raw["quota_project_id"] is not None
                    else None
                ),
                quota_total_mb=(
                    int(raw["quota_total_mb"])
                    if raw["quota_total_mb"] is not None
                    else None
                ),
                revision=int(raw["revision"]),
                rootfs_sha256=str(raw["rootfs_sha256"]),
                runtime_identity_sha256=str(raw["runtime_identity_sha256"]),
                sandbox_generation=int(raw["sandbox_generation"]),
                spec=SandboxSpec.from_dict(raw["spec"]),
                updated_ns=int(raw["updated_ns"]),
                version=DIRECT_REGISTRATION_VERSION,
            )
        except (TypeError, ValueError) as exc:
            raise DirectRegistryError("direct registration is invalid") from exc


@dataclass(frozen=True)
class _DirectRegistryState:
    records: tuple[DirectSandboxRegistration, ...]
    tombstones: dict[str, int]
    migration_tombstones: dict[str, tuple[str, ...]]
    activity_revision: int
    version: int = DIRECT_REGISTRY_VERSION

    def to_dict(self) -> dict[str, Any]:
        return {
            "activity_revision": self.activity_revision,
            "records": [
                item.to_dict()
                for item in sorted(self.records, key=lambda value: value.sandbox_id)
            ],
            "migration_tombstones": {
                key: list(values)
                for key, values in sorted(self.migration_tombstones.items())
            },
            "tombstones": dict(sorted(self.tombstones.items())),
            "version": self.version,
        }


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
    """Crash-durable ownership bridge from admission through Warden create."""

    def __init__(
        self,
        path: Path,
        *,
        max_bytes: int = DIRECT_REGISTRY_MAX_BYTES,
        max_inline_tombstones: int = DIRECT_REGISTRY_MAX_INLINE_TOMBSTONES,
        max_inline_migration_tombstones: int = (
            DIRECT_REGISTRY_MAX_INLINE_MIGRATION_TOMBSTONES
        ),
    ) -> None:
        if not path.is_absolute():
            raise ValueError("direct registry path must be absolute")
        self.path = path
        self.lock_path = path.with_name(f".{path.name}.lock")
        self.tombstone_archive_path = path.with_name(f"{path.name}.tombstones.sqlite3")
        self._max_bytes = max(1, max_bytes)
        self._max_inline_tombstones = max(1, max_inline_tombstones)
        self._max_inline_migration_tombstones = max(
            1,
            max_inline_migration_tombstones,
        )
        self._cache_guard = RLock()
        self._cached_fingerprint: object | tuple[int, int, int, int, int] | None = (
            _CACHE_UNINITIALIZED
        )
        self._cached_state: _DirectRegistryState | None = None
        self._cached_snapshot: DirectRegistrySnapshot | None = None

    def plan(
        self,
        *,
        spec: SandboxSpec,
        sandbox_generation: int,
        operation_id: str,
        runtime_identity_sha256: str,
    ) -> DirectSandboxRegistration:
        if sandbox_generation <= 0:
            raise ValueError("sandbox generation must be positive")
        candidate = DirectSandboxRegistration(
            spec=spec,
            sandbox_generation=sandbox_generation,
            operation_id=operation_id,
            runtime_identity_sha256=runtime_identity_sha256,
            phase="planned",
            revision=1,
            created_ns=time.time_ns(),
            updated_ns=time.time_ns(),
        )
        with self._locked():
            state = self._load_unlocked()
            existing = self._find(state, spec.id)
            if existing is not None:
                if (
                    existing.sandbox_generation == sandbox_generation
                    and existing.operation_id == operation_id
                    and existing.spec == spec
                    and existing.runtime_identity_sha256 == runtime_identity_sha256
                ):
                    return existing
                raise DirectRegistryConflictError(
                    "sandbox already has another direct registration"
                )
            if (
                state.tombstones.get(spec.id, -1) >= sandbox_generation
                or self._archived_generation_unlocked(spec.id) >= sandbox_generation
            ):
                raise DirectRegistryConflictError(
                    "direct registration is fenced by a tombstone"
                )
            self._save_unlocked(replace(state, records=(*state.records, candidate)))
            return candidate

    def plan_import(
        self,
        *,
        spec: SandboxSpec,
        sandbox_generation: int,
        operation_id: str,
        runtime_identity_sha256: str,
        migration_id: str,
        migration_sha256: str,
    ) -> DirectSandboxRegistration:
        if sandbox_generation <= 0:
            raise ValueError("sandbox generation must be positive")
        candidate = DirectSandboxRegistration(
            spec=spec,
            sandbox_generation=sandbox_generation,
            operation_id=operation_id,
            runtime_identity_sha256=runtime_identity_sha256,
            phase="import_planned",
            revision=1,
            created_ns=time.time_ns(),
            updated_ns=time.time_ns(),
            migration_id=migration_id,
            migration_sha256=migration_sha256,
        )
        with self._locked():
            state = self._load_unlocked()
            existing = self._find(state, spec.id)
            if existing is not None:
                if (
                    existing.sandbox_generation == sandbox_generation
                    and existing.operation_id == operation_id
                    and existing.spec == spec
                    and existing.runtime_identity_sha256 == runtime_identity_sha256
                    and existing.migration_id == migration_id
                    and existing.migration_sha256 == migration_sha256
                ):
                    return existing
                raise DirectRegistryConflictError(
                    "sandbox already has another direct registration"
                )
            if migration_id in state.migration_tombstones.get(
                spec.id, ()
            ) or self._archived_migration_unlocked(spec.id, migration_id):
                raise DirectRegistryConflictError(
                    "migration import is fenced by a tombstone"
                )
            self._save_unlocked(replace(state, records=(*state.records, candidate)))
            return candidate

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
            expected_revision=expected_revision,
            expected_phase="planned",
            phase="quota_ready",
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
            expected_revision=expected_revision,
            expected_phase="import_planned",
            phase="importing",
            quota_project_id=project_id,
            quota_total_mb=total_mb,
            quota_path=str(quota_path),
        )

    def abort_planned(
        self,
        sandbox_id: str,
        *,
        expected_revision: int,
    ) -> None:
        """Forget a create that failed before acquiring any external resource."""
        with self._locked():
            state = self._load_unlocked()
            record = self._require(state, sandbox_id)
            if record.phase != "planned" or record.revision != expected_revision:
                raise DirectRegistryConflictError(
                    "direct plan abort lost its ownership fence"
                )
            self._save_unlocked(
                replace(
                    state,
                    records=tuple(
                        item for item in state.records if item.sandbox_id != sandbox_id
                    ),
                )
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
        with self._locked():
            state = self._load_unlocked()
            record = self._require(state, sandbox_id)
            if (
                record.phase != "import_planned"
                or record.revision != expected_revision
                or record.migration_id != migration_id
                or record.migration_sha256 != migration_sha256
            ):
                raise DirectRegistryConflictError(
                    "import plan abort lost its ownership fence"
                )
            migration_tombstones = dict(state.migration_tombstones)
            if retire:
                previous = migration_tombstones.get(sandbox_id, ())
                migration_tombstones[sandbox_id] = tuple(
                    dict.fromkeys((*previous, migration_id))
                )
            self._save_unlocked(
                replace(
                    state,
                    records=tuple(
                        item for item in state.records if item.sandbox_id != sandbox_id
                    ),
                    migration_tombstones=migration_tombstones,
                )
            )

    def commit_rootfs(
        self,
        sandbox_id: str,
        *,
        expected_revision: int,
        image_id: str,
        sandbox: DirectSandbox,
    ) -> DirectSandboxRegistration:
        return self._transition(
            sandbox_id,
            expected_revision=expected_revision,
            expected_phase="quota_ready",
            phase="rootfs_ready",
            image_id=image_id,
            rootfs_sha256=sandbox.rootfs_sha256,
            container_id=sandbox.container_id,
            bundle=str(sandbox.bundle),
            memory_directory=sandbox.memory_directory,
        )

    def begin_import(
        self,
        sandbox_id: str,
        *,
        expected_revision: int,
        migration_id: str,
        migration_sha256: str,
    ) -> DirectSandboxRegistration:
        return self._transition(
            sandbox_id,
            expected_revision=expected_revision,
            expected_phase="quota_ready",
            phase="importing",
            migration_id=migration_id,
            migration_sha256=migration_sha256,
        )

    def commit_import_rootfs(
        self,
        sandbox_id: str,
        *,
        expected_revision: int,
        image_id: str,
        sandbox: DirectSandbox,
    ) -> DirectSandboxRegistration:
        return self._transition(
            sandbox_id,
            expected_revision=expected_revision,
            expected_phase="importing",
            phase="rootfs_ready",
            image_id=image_id,
            rootfs_sha256=sandbox.rootfs_sha256,
            container_id=sandbox.container_id,
            bundle=str(sandbox.bundle),
            memory_directory=sandbox.memory_directory,
        )

    def commit_import_ready(
        self,
        sandbox_id: str,
        *,
        expected_revision: int,
        migration_id: str,
        migration_sha256: str,
    ) -> DirectSandboxRegistration:
        with self._locked():
            state = self._load_unlocked()
            record = self._require(state, sandbox_id)
            if (
                record.phase != "rootfs_ready"
                or record.revision != expected_revision
                or record.migration_id != migration_id
                or record.migration_sha256 != migration_sha256
            ):
                raise DirectRegistryConflictError(
                    "import readiness lost its ownership fence"
                )
            return self._replace_record_unlocked(
                state,
                record,
                phase="import_ready",
            )

    def activate_import(
        self,
        sandbox_id: str,
        *,
        expected_revision: int,
        migration_id: str,
        migration_sha256: str,
    ) -> DirectSandboxRegistration:
        with self._locked():
            state = self._load_unlocked()
            record = self._require(state, sandbox_id)
            if (
                record.phase != "import_ready"
                or record.revision != expected_revision
                or record.migration_id != migration_id
                or record.migration_sha256 != migration_sha256
            ):
                raise DirectRegistryConflictError(
                    "import activation lost its ownership fence"
                )
            return self._replace_record_unlocked(
                state,
                record,
                phase="owned",
            )

    def begin_move_out(
        self,
        sandbox_id: str,
        *,
        expected_revision: int,
        migration_id: str,
        migration_sha256: str,
    ) -> DirectSandboxRegistration:
        with self._locked():
            state = self._load_unlocked()
            record = self._require(state, sandbox_id)
            if record.phase != "owned" or record.revision != expected_revision:
                raise DirectRegistryConflictError(
                    "move preparation lost its ownership fence"
                )
            migration_tombstones = dict(state.migration_tombstones)
            if record.migration_id:
                previous = migration_tombstones.get(sandbox_id, ())
                migration_tombstones[sandbox_id] = tuple(
                    dict.fromkeys((*previous, record.migration_id))
                )
            updated = replace(
                record,
                phase="moving_out",
                revision=record.revision + 1,
                updated_ns=time.time_ns(),
                migration_id=migration_id,
                migration_sha256=migration_sha256,
            )
            self._save_unlocked(
                replace(
                    state,
                    records=tuple(
                        updated if item.sandbox_id == sandbox_id else item
                        for item in state.records
                    ),
                    migration_tombstones=migration_tombstones,
                )
            )
            return updated

    def abort_move_out(
        self,
        sandbox_id: str,
        *,
        expected_revision: int,
        migration_id: str,
        migration_sha256: str,
    ) -> DirectSandboxRegistration:
        with self._locked():
            state = self._load_unlocked()
            record = self._require(state, sandbox_id)
            if (
                record.phase != "moving_out"
                or record.revision != expected_revision
                or record.migration_id != migration_id
                or record.migration_sha256 != migration_sha256
            ):
                raise DirectRegistryConflictError("move abort lost its ownership fence")
            return self._replace_record_unlocked(
                state,
                record,
                phase="owned",
                migration_id="",
                migration_sha256="",
            )

    def commit_owned(
        self,
        sandbox_id: str,
        *,
        expected_revision: int,
    ) -> DirectSandboxRegistration:
        return self._transition(
            sandbox_id,
            expected_revision=expected_revision,
            expected_phase="rootfs_ready",
            phase="owned",
        )

    def begin_delete(
        self,
        sandbox_id: str,
        *,
        expected_revision: int,
    ) -> DirectSandboxRegistration:
        return self._transition(
            sandbox_id,
            expected_revision=expected_revision,
            expected_phase="owned",
            phase="deleting",
        )

    def begin_delete_moved(
        self,
        sandbox_id: str,
        *,
        expected_revision: int,
        migration_id: str,
        migration_sha256: str,
    ) -> DirectSandboxRegistration:
        with self._locked():
            state = self._load_unlocked()
            record = self._require(state, sandbox_id)
            if (
                record.phase != "moving_out"
                or record.revision != expected_revision
                or record.migration_id != migration_id
                or record.migration_sha256 != migration_sha256
            ):
                raise DirectRegistryConflictError(
                    "move finalization lost its ownership fence"
                )
            return self._replace_record_unlocked(
                state,
                record,
                phase="deleting",
            )

    def begin_delete_import(
        self,
        sandbox_id: str,
        *,
        expected_revision: int,
        migration_id: str,
        migration_sha256: str,
    ) -> DirectSandboxRegistration:
        with self._locked():
            state = self._load_unlocked()
            record = self._require(state, sandbox_id)
            if (
                record.phase not in {"importing", "rootfs_ready", "import_ready"}
                or record.revision != expected_revision
                or record.migration_id != migration_id
                or record.migration_sha256 != migration_sha256
            ):
                raise DirectRegistryConflictError(
                    "import abort lost its ownership fence"
                )
            return self._replace_record_unlocked(
                state,
                record,
                phase="deleting",
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
        with self._locked():
            state = self._load_unlocked()
            record = self._require(state, sandbox_id)
            if (
                record.phase != "deleting"
                or record.revision != expected_revision
                or record.sandbox_generation != sandbox_generation
            ):
                raise DirectRegistryConflictError(
                    "direct deletion completion lost its ownership fence"
                )
            tombstones = dict(state.tombstones)
            tombstones[sandbox_id] = max(
                sandbox_generation,
                tombstones.get(sandbox_id, -1),
                self._archived_generation_unlocked(sandbox_id),
            )
            migration_tombstones = dict(state.migration_tombstones)
            if record.migration_id:
                previous = migration_tombstones.get(sandbox_id, ())
                migration_tombstones[sandbox_id] = tuple(
                    dict.fromkeys((*previous, record.migration_id))
                )
            self._save_unlocked(
                replace(
                    state,
                    records=tuple(
                        item for item in state.records if item.sandbox_id != sandbox_id
                    ),
                    tombstones=tombstones,
                    migration_tombstones=migration_tombstones,
                )
            )

    def get(self, sandbox_id: str) -> DirectSandboxRegistration | None:
        with self._locked():
            return self._snapshot_unlocked().get(sandbox_id)

    def list(self) -> tuple[DirectSandboxRegistration, ...]:
        return self.snapshot().records

    def snapshot(self) -> DirectRegistrySnapshot:
        """Return records, indexes, roots, and revision from one durable read."""

        with self._locked():
            return self._snapshot_unlocked()

    def references_image(self, image_id: str) -> bool:
        """Recheck one Docker image root without reparsing unchanged JSON."""

        return image_id in self.snapshot().image_ids

    def _transition(
        self,
        sandbox_id: str,
        *,
        expected_revision: int,
        expected_phase: str,
        phase: str,
        **changes: Any,
    ) -> DirectSandboxRegistration:
        with self._locked():
            state = self._load_unlocked()
            record = self._require(state, sandbox_id)
            if record.revision != expected_revision or record.phase != expected_phase:
                raise DirectRegistryConflictError(
                    "direct registration transition lost its ownership fence"
                )
            updated = replace(
                record,
                phase=phase,
                revision=record.revision + 1,
                updated_ns=time.time_ns(),
                **changes,
            )
            self._save_unlocked(
                replace(
                    state,
                    records=tuple(
                        updated if item.sandbox_id == sandbox_id else item
                        for item in state.records
                    ),
                )
            )
            return updated

    def _replace_record_unlocked(
        self,
        state: _DirectRegistryState,
        record: DirectSandboxRegistration,
        **changes: Any,
    ) -> DirectSandboxRegistration:
        updated = replace(
            record,
            revision=record.revision + 1,
            updated_ns=time.time_ns(),
            **changes,
        )
        self._save_unlocked(
            replace(
                state,
                records=tuple(
                    updated if item.sandbox_id == record.sandbox_id else item
                    for item in state.records
                ),
            )
        )
        return updated

    @staticmethod
    def _find(
        state: _DirectRegistryState,
        sandbox_id: str,
    ) -> DirectSandboxRegistration | None:
        return next(
            (item for item in state.records if item.sandbox_id == sandbox_id),
            None,
        )

    @classmethod
    def _require(
        cls,
        state: _DirectRegistryState,
        sandbox_id: str,
    ) -> DirectSandboxRegistration:
        record = cls._find(state, sandbox_id)
        if record is None:
            raise DirectRegistryConflictError("direct registration is absent")
        return record

    @contextmanager
    def _locked(self) -> Iterator[None]:
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        parent = self.path.parent.lstat()
        if (
            not stat.S_ISDIR(parent.st_mode)
            or stat.S_ISLNK(parent.st_mode)
            or parent.st_uid != os.geteuid()
            or parent.st_mode & 0o022
        ):
            raise DirectRegistryError(
                "direct registry directory must be private and owned"
            )
        descriptor = os.open(
            self.lock_path,
            os.O_RDWR
            | os.O_CREAT
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        try:
            info = os.fstat(descriptor)
            if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077:
                raise DirectRegistryError("direct registry lock is invalid")
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            yield
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)

    def _load_unlocked(self) -> _DirectRegistryState:
        if not os.path.lexists(self.path):
            with self._cache_guard:
                if (
                    self._cached_fingerprint is None
                    and self._cached_state is not None
                ):
                    return self._cached_state
            state = _DirectRegistryState(
                records=(),
                tombstones={},
                migration_tombstones={},
                activity_revision=0,
            )
            self._remember_state_unlocked(state, None)
            return state
        try:
            info = self.path.lstat()
            if not stat.S_ISREG(info.st_mode) or stat.S_ISLNK(info.st_mode):
                raise DirectRegistryError("direct registry must be a regular file")
            if info.st_uid != os.geteuid() or info.st_mode & 0o077:
                raise DirectRegistryError("direct registry must be private and owned")
            fingerprint = self._fingerprint(info)
            with self._cache_guard:
                if (
                    self._cached_fingerprint == fingerprint
                    and self._cached_state is not None
                ):
                    return self._cached_state
            raw = self.path.read_bytes()
            if len(raw) > self._max_bytes:
                raise DirectRegistryError(
                    f"direct registry exceeds the {self._max_bytes} byte limit"
                )
            payload = json.loads(raw.decode("utf-8"))
        except DirectRegistryError:
            raise
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise DirectRegistryError("direct registry is unreadable") from exc
        payload_version = payload.get("version") if isinstance(payload, dict) else None
        expected_keys = {
            "activity_revision",
            "migration_tombstones",
            "records",
            "tombstones",
            "version",
        }
        if (
            not isinstance(payload, dict)
            or set(payload) != expected_keys
            or not isinstance(payload_version, int)
            or isinstance(payload_version, bool)
            or payload_version != DIRECT_REGISTRY_VERSION
            or not isinstance(payload["records"], list)
            or not isinstance(payload["tombstones"], dict)
            or not isinstance(payload["migration_tombstones"], dict)
            or not isinstance(payload["activity_revision"], int)
            or isinstance(payload["activity_revision"], bool)
            or payload["activity_revision"] < 0
        ):
            raise DirectRegistryError("direct registry schema is invalid")
        raw_migration_tombstones = payload["migration_tombstones"]
        if any(
            not isinstance(key, str)
            or not isinstance(values, list)
            or len(values) > 256
            or any(not isinstance(item, str) for item in values)
            for key, values in raw_migration_tombstones.items()
        ):
            raise DirectRegistryError(
                "direct registry migration tombstones are invalid"
            )
        try:
            records = tuple(
                DirectSandboxRegistration.from_dict(item) for item in payload["records"]
            )
            tombstones = {
                str(key): int(value) for key, value in payload["tombstones"].items()
            }
            migration_tombstones = {
                str(key): tuple(str(item) for item in values)
                for key, values in raw_migration_tombstones.items()
            }
        except (TypeError, ValueError) as exc:
            raise DirectRegistryError("direct registry content is invalid") from exc
        ids = [item.sandbox_id for item in records]
        if (
            len(ids) != len(set(ids))
            or any(value <= 0 for value in tombstones.values())
            or any(
                not OPERATION_ID_RE.fullmatch(migration_id)
                for values in migration_tombstones.values()
                for migration_id in values
            )
        ):
            raise DirectRegistryError("direct registry ownership is invalid")
        state = _DirectRegistryState(
            records=records,
            tombstones=tombstones,
            migration_tombstones=migration_tombstones,
            activity_revision=int(payload["activity_revision"]),
        )
        if state.activity_revision < max(
            (record.revision for record in records),
            default=0,
        ):
            raise DirectRegistryError("direct registry activity revision is invalid")
        self._remember_state_unlocked(state, fingerprint)
        return state

    @staticmethod
    def _fingerprint(info: os.stat_result) -> tuple[int, int, int, int, int]:
        return (
            info.st_dev,
            info.st_ino,
            info.st_size,
            info.st_mtime_ns,
            info.st_ctime_ns,
        )

    def _remember_state_unlocked(
        self,
        state: _DirectRegistryState,
        fingerprint: tuple[int, int, int, int, int] | None,
    ) -> None:
        records = tuple(sorted(state.records, key=lambda item: item.sandbox_id))
        by_sandbox_id = {record.sandbox_id: record for record in records}
        snapshot = DirectRegistrySnapshot(
            records=records,
            by_sandbox_id=MappingProxyType(by_sandbox_id),
            image_ids=frozenset(
                record.image_id for record in records if record.image_id
            ),
            activity_revision=state.activity_revision,
        )
        with self._cache_guard:
            self._cached_fingerprint = fingerprint
            self._cached_state = state
            self._cached_snapshot = snapshot

    def _snapshot_unlocked(self) -> DirectRegistrySnapshot:
        self._load_unlocked()
        with self._cache_guard:
            assert self._cached_snapshot is not None
            return self._cached_snapshot

    @contextmanager
    def _tombstone_archive_connection_unlocked(
        self,
        *,
        create: bool,
    ) -> Iterator[sqlite3.Connection | None]:
        archive_created = False
        if not os.path.lexists(self.tombstone_archive_path):
            if not create:
                yield None
                return
            try:
                descriptor = os.open(
                    self.tombstone_archive_path,
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
                archive_created = True

        try:
            info = self.tombstone_archive_path.lstat()
            if not stat.S_ISREG(info.st_mode) or stat.S_ISLNK(info.st_mode):
                raise DirectRegistryError(
                    "direct tombstone archive must be a regular file"
                )
            if info.st_uid != os.geteuid() or info.st_mode & 0o077:
                raise DirectRegistryError(
                    "direct tombstone archive must be private and owned"
                )
        except FileNotFoundError as exc:
            raise DirectRegistryError("direct tombstone archive disappeared") from exc

        connection: sqlite3.Connection | None = None
        try:
            connection = sqlite3.connect(
                self.tombstone_archive_path
                if create
                else f"{self.tombstone_archive_path.as_uri()}?mode=ro",
                timeout=5.0,
                uri=not create,
            )
            connection.execute("PRAGMA trusted_schema = OFF")
            if create:
                connection.execute("PRAGMA synchronous = FULL")
                connection.execute("PRAGMA journal_mode = DELETE")
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS ordinary_tombstones (
                        sandbox_id TEXT PRIMARY KEY,
                        generation INTEGER NOT NULL CHECK (generation > 0)
                    )
                    """
                )
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS migration_tombstones (
                        sandbox_id TEXT NOT NULL,
                        migration_id TEXT NOT NULL,
                        PRIMARY KEY (sandbox_id, migration_id)
                    )
                    """
                )
                connection.commit()
                if archive_created:
                    directory = os.open(
                        self.path.parent,
                        os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
                    )
                    try:
                        os.fsync(directory)
                    finally:
                        os.close(directory)
            yield connection
        except DirectRegistryError:
            raise
        except (OSError, sqlite3.DatabaseError) as exc:
            raise DirectRegistryError("direct tombstone archive is unreadable") from exc
        finally:
            if connection is not None:
                connection.close()

    def _archived_generation_unlocked(self, sandbox_id: str) -> int:
        with self._tombstone_archive_connection_unlocked(create=False) as connection:
            if connection is None:
                return -1
            row = connection.execute(
                """
                SELECT generation
                FROM ordinary_tombstones
                WHERE sandbox_id = ?
                """,
                (sandbox_id,),
            ).fetchone()
        if row is None:
            return -1
        generation = int(row[0])
        if generation <= 0:
            raise DirectRegistryError("direct tombstone archive is invalid")
        return generation

    def _archived_migration_unlocked(
        self,
        sandbox_id: str,
        migration_id: str,
    ) -> bool:
        with self._tombstone_archive_connection_unlocked(create=False) as connection:
            if connection is None:
                return False
            row = connection.execute(
                """
                SELECT 1
                FROM migration_tombstones
                WHERE sandbox_id = ? AND migration_id = ?
                """,
                (sandbox_id, migration_id),
            ).fetchone()
        return row is not None

    def _archive_tombstones_unlocked(
        self,
        ordinary: tuple[tuple[str, int], ...],
        migrations: tuple[tuple[str, str], ...],
    ) -> None:
        if not ordinary and not migrations:
            return
        with self._tombstone_archive_connection_unlocked(create=True) as connection:
            assert connection is not None
            with connection:
                connection.executemany(
                    """
                    INSERT INTO ordinary_tombstones (sandbox_id, generation)
                    VALUES (?, ?)
                    ON CONFLICT (sandbox_id) DO UPDATE SET
                        generation = MAX(generation, excluded.generation)
                    """,
                    ordinary,
                )
                connection.executemany(
                    """
                    INSERT OR IGNORE INTO migration_tombstones (
                        sandbox_id,
                        migration_id
                    ) VALUES (?, ?)
                    """,
                    migrations,
                )

    def _compact_state_unlocked(
        self,
        state: _DirectRegistryState,
    ) -> _DirectRegistryState:
        ordinary_items = tuple(sorted(state.tombstones.items()))
        ordinary_overflow = ordinary_items[: -self._max_inline_tombstones]
        inline_ordinary = ordinary_items[-self._max_inline_tombstones :]

        migration_overflow: list[tuple[str, str]] = []
        inline_migrations: list[tuple[str, str]] = []
        for sandbox_id, migration_ids in sorted(state.migration_tombstones.items()):
            split = max(
                0,
                len(migration_ids)
                - DIRECT_REGISTRY_MAX_MIGRATION_TOMBSTONES_PER_SANDBOX,
            )
            migration_overflow.extend(
                (sandbox_id, migration_id) for migration_id in migration_ids[:split]
            )
            inline_migrations.extend(
                (sandbox_id, migration_id) for migration_id in migration_ids[split:]
            )
        global_split = max(
            0,
            len(inline_migrations) - self._max_inline_migration_tombstones,
        )
        migration_overflow.extend(inline_migrations[:global_split])
        inline_migrations = inline_migrations[global_split:]

        compact_migrations: dict[str, list[str]] = {}
        for sandbox_id, migration_id in inline_migrations:
            compact_migrations.setdefault(sandbox_id, []).append(migration_id)

        self._archive_tombstones_unlocked(
            ordinary_overflow,
            tuple(migration_overflow),
        )
        return replace(
            state,
            tombstones=dict(inline_ordinary),
            migration_tombstones={
                sandbox_id: tuple(migration_ids)
                for sandbox_id, migration_ids in compact_migrations.items()
            },
        )

    def _save_unlocked(self, state: _DirectRegistryState) -> None:
        state = self._compact_state_unlocked(state)
        state = replace(state, activity_revision=state.activity_revision + 1)
        payload = (
            json.dumps(
                state.to_dict(),
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("ascii")
            + b"\n"
        )
        if len(payload) > self._max_bytes:
            raise DirectRegistryError(
                f"direct registry update exceeds the {self._max_bytes} byte limit"
            )
        descriptor, raw_path = tempfile.mkstemp(
            prefix=f".{self.path.name}.",
            suffix=".tmp",
            dir=self.path.parent,
        )
        temporary = Path(raw_path)
        try:
            os.fchmod(descriptor, 0o600)
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            temporary.replace(self.path)
            directory = os.open(
                self.path.parent,
                os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
            )
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
            self._remember_state_unlocked(state, self._fingerprint(self.path.lstat()))
        finally:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass
