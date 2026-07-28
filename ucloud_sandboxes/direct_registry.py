from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, replace
import fcntl
import json
import os
from pathlib import Path
import re
import stat
import tempfile
import time
from typing import Any, Iterator

from .direct_warden import DirectSandbox
from .sandbox import (
    OPERATION_ID_RE,
    SandboxSpec,
    sandbox_spec_fingerprint,
)


DIRECT_REGISTRY_VERSION = 1
DIRECT_REGISTRATION_VERSION = 1
DIRECT_REGISTRATION_PHASES = {
    "planned",
    "quota_ready",
    "rootfs_ready",
    "owned",
    "deleting",
}
_DIGEST = re.compile(r"[0-9a-f]{64}\Z")
_CONTAINER_ID = re.compile(r"[0-9a-f]{64}\Z")


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
    version: int = DIRECT_REGISTRATION_VERSION

    def __post_init__(self) -> None:
        if self.version != DIRECT_REGISTRATION_VERSION:
            raise ValueError("unsupported direct registration version")
        if self.spec.forkable:
            raise ValueError("fork is deferred from the direct runtime")
        self.spec.validate()
        if self.sandbox_generation < 0:
            raise ValueError("sandbox generation cannot be negative")
        if not self.operation_id or not OPERATION_ID_RE.fullmatch(self.operation_id):
            raise ValueError("direct registration operation id is invalid")
        if not _DIGEST.fullmatch(self.runtime_identity_sha256):
            raise ValueError("direct registration runtime identity is invalid")
        if self.phase not in DIRECT_REGISTRATION_PHASES:
            raise ValueError("direct registration phase is invalid")
        if self.revision < 1 or self.created_ns < 1 or self.updated_ns < 1:
            raise ValueError("direct registration revision/timestamp is invalid")
        quota_values = (
            self.quota_project_id,
            self.quota_total_mb,
            self.quota_path,
        )
        quota_present = all(
            value not in {None, ""}
            for value in quota_values
        )
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
        if self.phase == "planned" and (quota_present or rootfs_present):
            raise ValueError("planned direct registration owns external state")
        if self.phase == "quota_ready" and (not quota_present or rootfs_present):
            raise ValueError("quota-ready direct registration is inconsistent")
        if self.phase in {"rootfs_ready", "owned", "deleting"} and (
            not quota_present or not rootfs_present
        ):
            raise ValueError("direct registration is missing owned resources")

    @property
    def sandbox_id(self) -> str:
        return self.spec.id

    @property
    def spec_sha256(self) -> str:
        return sandbox_spec_fingerprint(self.spec)

    def to_direct_sandbox(self) -> DirectSandbox:
        if self.phase not in {"rootfs_ready", "owned", "deleting"}:
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
        if set(raw) != expected or not isinstance(raw["spec"], dict):
            raise DirectRegistryError("direct registration schema is invalid")
        try:
            return cls(
                bundle=str(raw["bundle"]),
                container_id=str(raw["container_id"]),
                created_ns=int(raw["created_ns"]),
                image_id=str(raw["image_id"]),
                memory_directory=str(raw["memory_directory"]),
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
                version=int(raw["version"]),
            )
        except (TypeError, ValueError) as exc:
            raise DirectRegistryError("direct registration is invalid") from exc


@dataclass(frozen=True)
class _DirectRegistryState:
    records: tuple[DirectSandboxRegistration, ...]
    tombstones: dict[str, int]
    version: int = DIRECT_REGISTRY_VERSION

    def to_dict(self) -> dict[str, Any]:
        return {
            "records": [
                item.to_dict()
                for item in sorted(self.records, key=lambda value: value.sandbox_id)
            ],
            "tombstones": dict(sorted(self.tombstones.items())),
            "version": self.version,
        }


class DirectSandboxRegistry:
    """Crash-durable ownership bridge from admission through Warden create."""

    def __init__(self, path: Path) -> None:
        if not path.is_absolute():
            raise ValueError("direct registry path must be absolute")
        self.path = path
        self.lock_path = path.with_name(f".{path.name}.lock")

    def plan(
        self,
        *,
        spec: SandboxSpec,
        sandbox_generation: int,
        operation_id: str,
        runtime_identity_sha256: str,
    ) -> DirectSandboxRegistration:
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
                    and existing.runtime_identity_sha256
                    == runtime_identity_sha256
                ):
                    return existing
                raise DirectRegistryConflictError(
                    "sandbox already has another direct registration"
                )
            if state.tombstones.get(spec.id, -1) >= sandbox_generation:
                raise DirectRegistryConflictError(
                    "direct registration is fenced by a tombstone"
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

    def commit_deleted(
        self,
        sandbox_id: str,
        *,
        sandbox_generation: int,
        expected_revision: int,
    ) -> None:
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
            )
            self._save_unlocked(
                replace(
                    state,
                    records=tuple(
                        item for item in state.records if item.sandbox_id != sandbox_id
                    ),
                    tombstones=tombstones,
                )
            )

    def get(self, sandbox_id: str) -> DirectSandboxRegistration | None:
        with self._locked():
            return self._find(self._load_unlocked(), sandbox_id)

    def list(self) -> tuple[DirectSandboxRegistration, ...]:
        with self._locked():
            return tuple(
                sorted(
                    self._load_unlocked().records,
                    key=lambda item: item.sandbox_id,
                )
            )

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
            if (
                record.revision != expected_revision
                or record.phase != expected_phase
            ):
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
            return _DirectRegistryState(records=(), tombstones={})
        try:
            info = self.path.lstat()
            if not stat.S_ISREG(info.st_mode) or stat.S_ISLNK(info.st_mode):
                raise DirectRegistryError("direct registry must be a regular file")
            if info.st_uid != os.geteuid() or info.st_mode & 0o077:
                raise DirectRegistryError("direct registry must be private and owned")
            raw = self.path.read_bytes()
            if len(raw) > 16 * 1024 * 1024:
                raise DirectRegistryError("direct registry is too large")
            payload = json.loads(raw.decode("utf-8"))
        except DirectRegistryError:
            raise
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise DirectRegistryError("direct registry is unreadable") from exc
        if (
            not isinstance(payload, dict)
            or set(payload) != {"records", "tombstones", "version"}
            or payload["version"] != DIRECT_REGISTRY_VERSION
            or not isinstance(payload["records"], list)
            or not isinstance(payload["tombstones"], dict)
        ):
            raise DirectRegistryError("direct registry schema is invalid")
        try:
            records = tuple(
                DirectSandboxRegistration.from_dict(item)
                for item in payload["records"]
            )
            tombstones = {
                str(key): int(value)
                for key, value in payload["tombstones"].items()
            }
        except (TypeError, ValueError) as exc:
            raise DirectRegistryError("direct registry content is invalid") from exc
        ids = [item.sandbox_id for item in records]
        if len(ids) != len(set(ids)) or any(value < 0 for value in tombstones.values()):
            raise DirectRegistryError("direct registry ownership is invalid")
        return _DirectRegistryState(records=records, tombstones=tombstones)

    def _save_unlocked(self, state: _DirectRegistryState) -> None:
        payload = (
            json.dumps(
                state.to_dict(),
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("ascii")
            + b"\n"
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
        finally:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass
