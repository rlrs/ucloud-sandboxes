from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, replace
import errno
from enum import Enum
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import tempfile
import time
from typing import Any, Callable, Iterator, Sequence


MIB = 1024 * 1024
HIBERNATION_SCHEMA_VERSION = 1
HIBERNATION_MANIFEST_VERSION = 1
HIBERNATION_ALLOCATOR_CHUNK_MB = 1024
HIBERNATION_FIXED_OVERHEAD_MB = 64
MAX_HIBERNATION_JSON_BYTES = 1024 * 1024

_SAFE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}\Z")
_SAFE_FILE_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\Z")
_DIGEST = re.compile(r"[0-9a-f]{64}\Z")
_COMMIT = re.compile(r"[0-9a-f]{40,64}\Z")
_RUNTIME_VALUE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.+-]{0,63}\Z")


class HibernationError(RuntimeError):
    pass


class HibernationConflictError(HibernationError):
    pass


class HibernationValidationError(HibernationError):
    pass


class HibernationCapacityError(HibernationError):
    pass


class HibernationQuotaError(HibernationError):
    pass


class HibernationState(str, Enum):
    RUNNING = "running"
    HIBERNATING = "hibernating"
    PARKED = "parked"
    RESTORING = "restoring"
    RECOVERY_REQUIRED = "recovery-required"


class HibernationAuthority(str, Enum):
    NONE = "none"
    LIVE = "live"
    PENDING = "pending"
    PARKED = "parked"
    CANDIDATE = "candidate"


class HibernationFileRole(str, Enum):
    MAIN_MEMORY = "main-memory"
    KERNEL_STATE = "kernel-state"
    ALLOCATOR_METADATA = "allocator-metadata"
    PRIVATE_PAGES = "private-pages"


class HibernationRecoveryAction(str, Enum):
    ADOPT_RUNNING = "adopt-running"
    RESUME_OR_RETRY_HIBERNATE = "resume-or-retry-hibernate"
    FINISH_PUBLISHED_GENERATION = "finish-published-generation"
    FINISH_PENDING_GENERATION = "finish-pending-generation"
    KEEP_PARKED = "keep-parked"
    RETRY_RESTORE = "retry-restore"
    VERIFY_CANDIDATE = "verify-candidate"
    ROLLBACK_TO_PARKED = "rollback-to-parked"
    QUARANTINE = "quarantine"
    OPERATOR_REQUIRED = "operator-required"


def _canonical_json(payload: object) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")


def _sha256_json(payload: object) -> str:
    return hashlib.sha256(_canonical_json(payload)).hexdigest()


def _require_exact_keys(
    label: str,
    raw: dict[str, Any],
    expected: set[str],
) -> None:
    actual = set(raw)
    if actual != expected:
        missing = sorted(expected - actual)
        extra = sorted(actual - expected)
        raise ValueError(
            f"{label} has invalid schema; missing={missing}, extra={extra}"
        )


def _validate_safe_id(label: str, value: object) -> str:
    if not isinstance(value, str) or not _SAFE_ID.fullmatch(value):
        raise ValueError(f"{label} must be a safe 1-128 character identifier")
    if value in {".", ".."}:
        raise ValueError(f"{label} cannot be '.' or '..'")
    return value


def _validate_digest(label: str, value: object) -> str:
    if not isinstance(value, str) or not _DIGEST.fullmatch(value):
        raise ValueError(f"{label} must be a lowercase SHA-256 digest")
    return value


def _validate_runtime_value(label: str, value: object) -> str:
    if not isinstance(value, str) or not _RUNTIME_VALUE.fullmatch(value):
        raise ValueError(f"{label} contains unsupported characters")
    return value


def _validate_nonnegative_int(label: str, value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{label} must be a non-negative integer")
    return value


def _validate_positive_int(label: str, value: object) -> int:
    value = _validate_nonnegative_int(label, value)
    if value == 0:
        raise ValueError(f"{label} must be positive")
    return value


@dataclass(frozen=True)
class HibernationRuntimeFingerprint:
    runsc_sha256: str
    runsc_commit: str
    platform: str
    architecture: str
    page_size: int
    cpu_features_sha256: str
    boot_config_sha256: str
    rootfs_sha256: str

    def __post_init__(self) -> None:
        _validate_digest("runsc_sha256", self.runsc_sha256)
        if not _COMMIT.fullmatch(self.runsc_commit):
            raise ValueError("runsc_commit must be 40-64 lowercase hex characters")
        for label, value in (
            ("platform", self.platform),
            ("architecture", self.architecture),
        ):
            _validate_runtime_value(label, value)
        _validate_positive_int("page_size", self.page_size)
        if self.page_size & (self.page_size - 1):
            raise ValueError("page_size must be a power of two")
        _validate_digest("cpu_features_sha256", self.cpu_features_sha256)
        _validate_digest("boot_config_sha256", self.boot_config_sha256)
        _validate_digest("rootfs_sha256", self.rootfs_sha256)

    @property
    def digest(self) -> str:
        return _sha256_json(self.to_dict())

    def to_dict(self) -> dict[str, Any]:
        return {
            "architecture": self.architecture,
            "boot_config_sha256": self.boot_config_sha256,
            "cpu_features_sha256": self.cpu_features_sha256,
            "page_size": self.page_size,
            "platform": self.platform,
            "rootfs_sha256": self.rootfs_sha256,
            "runsc_commit": self.runsc_commit,
            "runsc_sha256": self.runsc_sha256,
        }

    @classmethod
    def from_dict(cls, raw: object) -> "HibernationRuntimeFingerprint":
        if not isinstance(raw, dict):
            raise ValueError("runtime fingerprint must be a JSON object")
        _require_exact_keys(
            "runtime fingerprint",
            raw,
            {
                "architecture",
                "boot_config_sha256",
                "cpu_features_sha256",
                "page_size",
                "platform",
                "rootfs_sha256",
                "runsc_commit",
                "runsc_sha256",
            },
        )
        return cls(
            runsc_sha256=_validate_digest("runsc_sha256", raw["runsc_sha256"]),
            runsc_commit=str(raw["runsc_commit"]),
            platform=_validate_runtime_value("platform", raw["platform"]),
            architecture=_validate_runtime_value("architecture", raw["architecture"]),
            page_size=_validate_positive_int("page_size", raw["page_size"]),
            cpu_features_sha256=_validate_digest(
                "cpu_features_sha256", raw["cpu_features_sha256"]
            ),
            boot_config_sha256=_validate_digest(
                "boot_config_sha256", raw["boot_config_sha256"]
            ),
            rootfs_sha256=_validate_digest("rootfs_sha256", raw["rootfs_sha256"]),
        )


@dataclass(frozen=True)
class HibernationArtifactFile:
    name: str
    role: HibernationFileRole
    device: int
    inode: int
    logical_bytes: int
    allocated_bytes: int

    def __post_init__(self) -> None:
        if (
            not _SAFE_FILE_NAME.fullmatch(self.name)
            or self.name in {".", ".."}
            or "/" in self.name
        ):
            raise ValueError("artifact file name must be a safe basename")
        if not isinstance(self.role, HibernationFileRole):
            raise ValueError("artifact file role is invalid")
        _validate_nonnegative_int("artifact device", self.device)
        _validate_positive_int("artifact inode", self.inode)
        _validate_nonnegative_int("artifact logical_bytes", self.logical_bytes)
        _validate_nonnegative_int("artifact allocated_bytes", self.allocated_bytes)

    @classmethod
    def from_path(
        cls,
        path: Path,
        *,
        role: HibernationFileRole,
    ) -> "HibernationArtifactFile":
        info = path.lstat()
        if not stat.S_ISREG(info.st_mode) or stat.S_ISLNK(info.st_mode):
            raise ValueError(f"artifact file must be a regular file: {path.name}")
        return cls(
            name=path.name,
            role=role,
            device=info.st_dev,
            inode=info.st_ino,
            logical_bytes=info.st_size,
            allocated_bytes=info.st_blocks * 512,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "allocated_bytes": self.allocated_bytes,
            "device": self.device,
            "inode": self.inode,
            "logical_bytes": self.logical_bytes,
            "name": self.name,
            "role": self.role.value,
        }

    @classmethod
    def from_dict(cls, raw: object) -> "HibernationArtifactFile":
        if not isinstance(raw, dict):
            raise ValueError("artifact file must be a JSON object")
        _require_exact_keys(
            "artifact file",
            raw,
            {
                "allocated_bytes",
                "device",
                "inode",
                "logical_bytes",
                "name",
                "role",
            },
        )
        try:
            role = HibernationFileRole(raw["role"])
        except (TypeError, ValueError) as exc:
            raise ValueError("artifact file role is invalid") from exc
        return cls(
            name=str(raw["name"]),
            role=role,
            device=_validate_nonnegative_int("artifact device", raw["device"]),
            inode=_validate_positive_int("artifact inode", raw["inode"]),
            logical_bytes=_validate_nonnegative_int(
                "artifact logical_bytes", raw["logical_bytes"]
            ),
            allocated_bytes=_validate_nonnegative_int(
                "artifact allocated_bytes", raw["allocated_bytes"]
            ),
        )


@dataclass(frozen=True)
class HibernationManifest:
    sandbox_id: str
    sandbox_generation: int
    hibernation_generation: int
    operation_id: str
    spec_sha256: str
    container_id: str
    created_ns: int
    runtime: HibernationRuntimeFingerprint
    files: tuple[HibernationArtifactFile, ...]
    managed_process_sha256: str = ""
    version: int = HIBERNATION_MANIFEST_VERSION

    def __post_init__(self) -> None:
        if self.version != HIBERNATION_MANIFEST_VERSION:
            raise ValueError("unsupported hibernation manifest version")
        _validate_safe_id("sandbox_id", self.sandbox_id)
        _validate_positive_int("sandbox_generation", self.sandbox_generation)
        _validate_positive_int("hibernation_generation", self.hibernation_generation)
        _validate_safe_id("operation_id", self.operation_id)
        _validate_digest("spec_sha256", self.spec_sha256)
        if not re.fullmatch(r"[0-9a-f]{64}", self.container_id):
            raise ValueError("container_id must be a full lowercase Docker id")
        _validate_positive_int("created_ns", self.created_ns)
        if not isinstance(self.runtime, HibernationRuntimeFingerprint):
            raise ValueError("runtime fingerprint is invalid")
        if self.managed_process_sha256:
            _validate_digest(
                "managed_process_sha256", self.managed_process_sha256
            )
        if not self.files:
            raise ValueError("hibernation manifest must contain artifact files")
        names = [item.name for item in self.files]
        if len(names) != len(set(names)):
            raise ValueError("hibernation manifest contains duplicate file names")
        roles = [item.role for item in self.files]
        for required in (
            HibernationFileRole.MAIN_MEMORY,
            HibernationFileRole.KERNEL_STATE,
            HibernationFileRole.ALLOCATOR_METADATA,
        ):
            if roles.count(required) != 1:
                raise ValueError(
                    f"hibernation manifest must contain exactly one {required.value}"
                )

    def _unsigned_dict(self) -> dict[str, Any]:
        payload = {
            "container_id": self.container_id,
            "created_ns": self.created_ns,
            "files": [item.to_dict() for item in self.files],
            "hibernation_generation": self.hibernation_generation,
            "operation_id": self.operation_id,
            "runtime": self.runtime.to_dict(),
            "sandbox_generation": self.sandbox_generation,
            "sandbox_id": self.sandbox_id,
            "spec_sha256": self.spec_sha256,
            "version": self.version,
        }
        if self.managed_process_sha256:
            payload["managed_process_sha256"] = self.managed_process_sha256
        return payload

    @property
    def metadata_sha256(self) -> str:
        return _sha256_json(self._unsigned_dict())

    def to_dict(self) -> dict[str, Any]:
        payload = self._unsigned_dict()
        payload["metadata_sha256"] = self.metadata_sha256
        return payload

    @classmethod
    def from_dict(cls, raw: object) -> "HibernationManifest":
        if not isinstance(raw, dict):
            raise ValueError("hibernation manifest must be a JSON object")
        required_keys = {
                "container_id",
                "created_ns",
                "files",
                "hibernation_generation",
                "metadata_sha256",
                "operation_id",
                "runtime",
                "sandbox_generation",
                "sandbox_id",
                "spec_sha256",
                "version",
            }
        if frozenset(raw) not in {
            frozenset(required_keys),
            frozenset(required_keys | {"managed_process_sha256"}),
        }:
            raise ValueError("hibernation manifest has an invalid schema")
        files_raw = raw["files"]
        if not isinstance(files_raw, list):
            raise ValueError("hibernation manifest files must be a list")
        manifest = cls(
            version=_validate_positive_int("manifest version", raw["version"]),
            sandbox_id=_validate_safe_id("sandbox_id", raw["sandbox_id"]),
            sandbox_generation=_validate_nonnegative_int(
                "sandbox_generation", raw["sandbox_generation"]
            ),
            hibernation_generation=_validate_positive_int(
                "hibernation_generation", raw["hibernation_generation"]
            ),
            operation_id=_validate_safe_id("operation_id", raw["operation_id"]),
            spec_sha256=_validate_digest("spec_sha256", raw["spec_sha256"]),
            container_id=str(raw["container_id"]),
            created_ns=_validate_positive_int("created_ns", raw["created_ns"]),
            runtime=HibernationRuntimeFingerprint.from_dict(raw["runtime"]),
            files=tuple(HibernationArtifactFile.from_dict(item) for item in files_raw),
            managed_process_sha256=(
                _validate_digest(
                    "managed_process_sha256", raw["managed_process_sha256"]
                )
                if "managed_process_sha256" in raw
                else ""
            ),
        )
        supplied_digest = _validate_digest("metadata_sha256", raw["metadata_sha256"])
        if supplied_digest != manifest.metadata_sha256:
            raise ValueError("hibernation manifest metadata digest does not match")
        return manifest

    def validate_identity(
        self,
        *,
        sandbox_id: str,
        sandbox_generation: int,
        spec_sha256: str,
        runtime_sha256: str,
    ) -> None:
        expected = (
            _validate_safe_id("sandbox_id", sandbox_id),
            _validate_positive_int("sandbox_generation", sandbox_generation),
            _validate_digest("spec_sha256", spec_sha256),
            _validate_digest("runtime_sha256", runtime_sha256),
        )
        actual = (
            self.sandbox_id,
            self.sandbox_generation,
            self.spec_sha256,
            self.runtime.digest,
        )
        if actual != expected:
            raise HibernationValidationError(
                "hibernation artifact does not match the requested sandbox "
                "or runtime"
            )

    def validate_files(
        self,
        root: Path,
        *,
        require_stable_device: bool = True,
    ) -> None:
        root_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        root_flags |= getattr(os, "O_NOFOLLOW", 0)
        try:
            root_fd = os.open(root, root_flags)
        except OSError as exc:
            raise HibernationValidationError(
                "cannot safely open hibernation artifact directory"
            ) from exc
        try:
            for item in self.files:
                flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
                flags |= getattr(os, "O_NOFOLLOW", 0)
                try:
                    descriptor = os.open(item.name, flags, dir_fd=root_fd)
                except OSError as exc:
                    raise HibernationValidationError(
                        f"cannot safely open hibernation artifact file: {item.name}"
                    ) from exc
                try:
                    info = os.fstat(descriptor)
                    if (
                        not stat.S_ISREG(info.st_mode)
                        or (require_stable_device and info.st_dev != item.device)
                        or info.st_ino != item.inode
                        or info.st_size != item.logical_bytes
                    ):
                        raise HibernationValidationError(
                            f"hibernation artifact file identity changed: {item.name}"
                        )
                finally:
                    os.close(descriptor)
        finally:
            os.close(root_fd)


@dataclass(frozen=True)
class HibernationGenerationInventory:
    sandbox_id: str
    sandbox_generation: int
    hibernation_generation: int
    state: str
    metadata_sha256: str = ""


class HibernationArtifactStore:
    """Crash-durable local storage for single-owner hibernation generations.

    A generation directory is pending until both ``manifest.json`` and the
    small ``COMPLETE`` marker have been fsynced. Artifact files are opened by
    basename through a pre-opened directory and are never content-hashed.
    """

    MANIFEST_NAME = "manifest.json"
    COMPLETE_NAME = "COMPLETE"
    LOCK_NAME = ".store.lock"

    def __init__(
        self,
        root: Path,
        *,
        owner_uid: int | None = None,
        preserve_incarnation_roots: bool = False,
        require_stable_device: bool = True,
    ) -> None:
        if not root.is_absolute():
            raise ValueError("hibernation artifact root must be absolute")
        self.root = root
        self.owner_uid = os.geteuid() if owner_uid is None else int(owner_uid)
        self.preserve_incarnation_roots = bool(preserve_incarnation_roots)
        # A storage-native snapshot is authenticated by its manifest and
        # remounted through a newly allocated ublk device.  Its files retain
        # inode and size identity, but Linux st_dev is intentionally not stable
        # across those mounts.  Ordinary local artifact stores keep requiring
        # all three identity fields.
        self.require_stable_device = bool(require_stable_device)

    def prepare_generation(
        self,
        *,
        sandbox_id: str,
        sandbox_generation: int,
        hibernation_generation: int,
    ) -> Path:
        with self._locked():
            path = self.generation_path(
                sandbox_id=sandbox_id,
                sandbox_generation=sandbox_generation,
                hibernation_generation=hibernation_generation,
            )
            self._ensure_directory(path.parent, create=True)
            if os.path.lexists(path):
                self._ensure_directory(path, create=False)
                if os.path.lexists(path / self.COMPLETE_NAME):
                    raise HibernationConflictError(
                        "hibernation generation is already complete"
                    )
                return path
            path.mkdir(mode=0o700)
            self._fsync_directory(path.parent)
            self._ensure_directory(path, create=False)
            return path

    def generation_path(
        self,
        *,
        sandbox_id: str,
        sandbox_generation: int,
        hibernation_generation: int,
    ) -> Path:
        sandbox_id = _validate_safe_id("sandbox_id", sandbox_id)
        sandbox_generation = _validate_nonnegative_int(
            "sandbox_generation", sandbox_generation
        )
        hibernation_generation = _validate_positive_int(
            "hibernation_generation", hibernation_generation
        )
        incarnation = f"{sandbox_id}.sandbox-{sandbox_generation}"
        generation = f"hibernate-{hibernation_generation}"
        path = self.root / incarnation / generation
        if path.parent.parent != self.root:
            raise HibernationError("hibernation generation path escaped its root")
        return path

    def adopt_file(
        self,
        *,
        active_root: Path,
        active_name: str,
        generation: Path,
        artifact_name: str,
    ) -> Path:
        if not _SAFE_FILE_NAME.fullmatch(active_name) or active_name in {".", ".."}:
            raise ValueError("active file name must be a safe basename")
        if not _SAFE_FILE_NAME.fullmatch(artifact_name) or artifact_name in {".", ".."}:
            raise ValueError("artifact file name must be a safe basename")
        with self._locked():
            self._ensure_directory(active_root, create=False)
            self._require_generation_path(generation)
            if os.path.lexists(generation / self.COMPLETE_NAME):
                raise HibernationConflictError(
                    "cannot add files to a complete hibernation generation"
                )
            source_fd = self._open_directory(active_root)
            target_fd = self._open_directory(generation)
            try:
                source_info = os.stat(
                    active_name,
                    dir_fd=source_fd,
                    follow_symlinks=False,
                )
                if not stat.S_ISREG(source_info.st_mode):
                    raise HibernationError(
                        "active memory backing must be a regular file"
                    )
                try:
                    os.stat(
                        artifact_name,
                        dir_fd=target_fd,
                        follow_symlinks=False,
                    )
                except FileNotFoundError:
                    pass
                else:
                    raise HibernationConflictError(
                        "hibernation artifact target already exists"
                    )
                try:
                    os.rename(
                        active_name,
                        artifact_name,
                        src_dir_fd=source_fd,
                        dst_dir_fd=target_fd,
                    )
                except OSError as exc:
                    raise HibernationError(
                        "cannot move backing file into hibernation generation; "
                        "active and artifact roots must share a filesystem"
                    ) from exc
                os.fsync(source_fd)
                os.fsync(target_fd)
            finally:
                os.close(target_fd)
                os.close(source_fd)
            return generation / artifact_name

    def return_consumed_file(
        self,
        manifest: HibernationManifest,
        *,
        active_root: Path,
        file_name: str,
        artifact_name: str | None = None,
    ) -> Path:
        """Return a manifest-owned inode after a failed restore candidate.

        Restore is allowed to consume the main-memory name from a complete
        single-owner generation. This is the only mutation allowed after
        ``COMPLETE``: the source must still have the exact device, inode, and
        size recorded by that manifest, and the original generation name must
        be absent.
        """
        if not _SAFE_FILE_NAME.fullmatch(file_name) or file_name in {".", ".."}:
            raise ValueError("consumed artifact name must be a safe basename")
        artifact_name = file_name if artifact_name is None else artifact_name
        if (
            not _SAFE_FILE_NAME.fullmatch(artifact_name)
            or artifact_name in {".", ".."}
        ):
            raise ValueError("artifact target name must be a safe basename")
        expected = next(
            (item for item in manifest.files if item.name == artifact_name),
            None,
        )
        if expected is None:
            raise HibernationConflictError(
                "consumed file is not owned by the hibernation manifest"
            )
        with self._locked():
            generation = self.generation_path(
                sandbox_id=manifest.sandbox_id,
                sandbox_generation=manifest.sandbox_generation,
                hibernation_generation=manifest.hibernation_generation,
            )
            self._require_generation_path(generation)
            marker = self._read_json_at(
                generation,
                self.COMPLETE_NAME,
                "COMPLETE",
            )
            if marker.get("metadata_sha256") != manifest.metadata_sha256:
                raise HibernationConflictError(
                    "complete generation no longer matches the manifest"
                )
            self._ensure_directory(active_root, create=False)
            source_fd = self._open_directory(active_root)
            target_fd = self._open_directory(generation)
            try:
                source_info = os.stat(
                    file_name,
                    dir_fd=source_fd,
                    follow_symlinks=False,
                )
                if (
                    not stat.S_ISREG(source_info.st_mode)
                    or (
                        self.require_stable_device
                        and source_info.st_dev != expected.device
                    )
                    or source_info.st_ino != expected.inode
                    or source_info.st_size != expected.logical_bytes
                ):
                    raise HibernationConflictError(
                        "active consumed file identity does not match its manifest"
                    )
                try:
                    os.stat(
                        artifact_name,
                        dir_fd=target_fd,
                        follow_symlinks=False,
                    )
                except FileNotFoundError:
                    pass
                else:
                    raise HibernationConflictError(
                        "complete generation already contains the consumed file"
                    )
                os.rename(
                    file_name,
                    artifact_name,
                    src_dir_fd=source_fd,
                    dst_dir_fd=target_fd,
                )
                os.fsync(source_fd)
                os.fsync(target_fd)
            finally:
                os.close(target_fd)
                os.close(source_fd)
            return generation / artifact_name

    def publish_complete(self, manifest: HibernationManifest) -> HibernationManifest:
        with self._locked():
            generation = self.generation_path(
                sandbox_id=manifest.sandbox_id,
                sandbox_generation=manifest.sandbox_generation,
                hibernation_generation=manifest.hibernation_generation,
            )
            self._require_generation_path(generation)
            if os.path.lexists(generation / self.COMPLETE_NAME):
                existing = self.load_complete(
                    sandbox_id=manifest.sandbox_id,
                    sandbox_generation=manifest.sandbox_generation,
                    hibernation_generation=manifest.hibernation_generation,
                )
                if existing.metadata_sha256 != manifest.metadata_sha256:
                    raise HibernationConflictError(
                        "complete hibernation generation has another manifest"
                    )
                return existing

            manifest.validate_files(
                generation,
                require_stable_device=self.require_stable_device,
            )
            generation_fd = self._open_directory(generation)
            try:
                # The large memory file is not read, but every named artifact must
                # reach durable storage before COMPLETE is authoritative.
                for item in manifest.files:
                    descriptor = os.open(
                        item.name,
                        os.O_RDONLY
                        | getattr(os, "O_CLOEXEC", 0)
                        | getattr(os, "O_NOFOLLOW", 0),
                        dir_fd=generation_fd,
                    )
                    try:
                        os.fsync(descriptor)
                    finally:
                        os.close(descriptor)

                self._atomic_write_at(
                    generation,
                    self.MANIFEST_NAME,
                    _canonical_json(manifest.to_dict()) + b"\n",
                )
                marker = {
                    "hibernation_generation": manifest.hibernation_generation,
                    "metadata_sha256": manifest.metadata_sha256,
                    "sandbox_generation": manifest.sandbox_generation,
                    "sandbox_id": manifest.sandbox_id,
                    "version": HIBERNATION_MANIFEST_VERSION,
                }
                self._atomic_write_at(
                    generation,
                    self.COMPLETE_NAME,
                    _canonical_json(marker) + b"\n",
                )
                os.fsync(generation_fd)
            finally:
                os.close(generation_fd)
            return manifest

    def discard_pending(
        self,
        *,
        sandbox_id: str,
        sandbox_generation: int,
        hibernation_generation: int,
    ) -> None:
        """Remove an unpublished generation after its live backend resumed.

        A durable ``COMPLETE`` marker is an ownership boundary and is never
        removed by this rollback operation.
        """
        with self._locked():
            generation = self.generation_path(
                sandbox_id=sandbox_id,
                sandbox_generation=sandbox_generation,
                hibernation_generation=hibernation_generation,
            )
            if not os.path.lexists(generation):
                return
            self._require_generation_path(generation)
            if os.path.lexists(generation / self.COMPLETE_NAME):
                raise HibernationConflictError(
                    "cannot discard a complete hibernation generation"
                )
            generation_fd = self._open_directory(generation)
            try:
                for name in os.listdir(generation_fd):
                    if not _SAFE_FILE_NAME.fullmatch(name) or name in {".", ".."}:
                        raise HibernationError(
                            "pending generation contains an unsafe entry"
                        )
                    info = os.stat(name, dir_fd=generation_fd, follow_symlinks=False)
                    if not stat.S_ISREG(info.st_mode):
                        raise HibernationError(
                            "pending generation contains a non-regular entry"
                        )
                    os.unlink(name, dir_fd=generation_fd)
                os.fsync(generation_fd)
            finally:
                os.close(generation_fd)
            generation.rmdir()
            self._fsync_directory(generation.parent)
            self._remove_empty_incarnation(generation.parent)

    def load_complete(
        self,
        *,
        sandbox_id: str,
        sandbox_generation: int,
        hibernation_generation: int,
    ) -> HibernationManifest:
        manifest = self.load_published_metadata(
            sandbox_id=sandbox_id,
            sandbox_generation=sandbox_generation,
            hibernation_generation=hibernation_generation,
        )
        generation = self.generation_path(
            sandbox_id=sandbox_id,
            sandbox_generation=sandbox_generation,
            hibernation_generation=hibernation_generation,
        )
        manifest.validate_files(
            generation,
            require_stable_device=self.require_stable_device,
        )
        return manifest

    def load_published_metadata(
        self,
        *,
        sandbox_id: str,
        sandbox_generation: int,
        hibernation_generation: int,
    ) -> HibernationManifest:
        """Load and authenticate a published manifest without opening payloads.

        This is used only after restore has intentionally consumed the
        single-owner main-memory name from its complete generation.
        """
        generation = self.generation_path(
            sandbox_id=sandbox_id,
            sandbox_generation=sandbox_generation,
            hibernation_generation=hibernation_generation,
        )
        self._require_generation_path(generation)
        marker = self._read_json_at(generation, self.COMPLETE_NAME, "COMPLETE")
        _require_exact_keys(
            "hibernation COMPLETE marker",
            marker,
            {
                "hibernation_generation",
                "metadata_sha256",
                "sandbox_generation",
                "sandbox_id",
                "version",
            },
        )
        if marker != {
            "hibernation_generation": hibernation_generation,
            "metadata_sha256": _validate_digest(
                "metadata_sha256", marker["metadata_sha256"]
            ),
            "sandbox_generation": sandbox_generation,
            "sandbox_id": sandbox_id,
            "version": HIBERNATION_MANIFEST_VERSION,
        }:
            raise HibernationValidationError(
                "hibernation COMPLETE marker identity is invalid"
            )
        manifest = HibernationManifest.from_dict(
            self._read_json_at(
                generation,
                self.MANIFEST_NAME,
                "hibernation manifest",
            )
        )
        if (
            manifest.metadata_sha256 != marker["metadata_sha256"]
            or manifest.sandbox_id != sandbox_id
            or manifest.sandbox_generation != sandbox_generation
            or manifest.hibernation_generation != hibernation_generation
        ):
            raise HibernationValidationError(
                "hibernation COMPLETE marker does not match its manifest"
            )
        return manifest

    def delete_published(
        self,
        manifest: HibernationManifest,
        *,
        allow_consumed_main_memory: bool = False,
    ) -> None:
        """Delete one authenticated generation for an authorized sandbox delete."""
        with self._locked():
            generation = self.generation_path(
                sandbox_id=manifest.sandbox_id,
                sandbox_generation=manifest.sandbox_generation,
                hibernation_generation=manifest.hibernation_generation,
            )
            self._require_generation_path(generation)
            published = self.load_published_metadata(
                sandbox_id=manifest.sandbox_id,
                sandbox_generation=manifest.sandbox_generation,
                hibernation_generation=manifest.hibernation_generation,
            )
            if published.metadata_sha256 != manifest.metadata_sha256:
                raise HibernationConflictError(
                    "published generation changed before deletion"
                )
            expected = {item.name: item for item in manifest.files}
            allowed = set(expected) | {self.MANIFEST_NAME, self.COMPLETE_NAME}
            actual = set(os.listdir(generation))
            unexpected = actual - allowed
            if unexpected:
                raise HibernationError(
                    f"published generation contains unexpected entries: "
                    f"{sorted(unexpected)}"
                )
            missing = set(expected) - actual
            if allow_consumed_main_memory:
                missing -= {
                    item.name
                    for item in manifest.files
                    if item.role == HibernationFileRole.MAIN_MEMORY
                }
            if missing:
                raise HibernationValidationError(
                    f"published generation is missing files: {sorted(missing)}"
                )
            generation_fd = self._open_directory(generation)
            try:
                for name, item in expected.items():
                    if name not in actual:
                        continue
                    info = os.stat(name, dir_fd=generation_fd, follow_symlinks=False)
                    if (
                        not stat.S_ISREG(info.st_mode)
                        or (
                            self.require_stable_device
                            and info.st_dev != item.device
                        )
                        or info.st_ino != item.inode
                        or info.st_size != item.logical_bytes
                    ):
                        raise HibernationValidationError(
                            f"published artifact identity changed: {name}"
                        )
                # Removing COMPLETE first makes a crash fail closed as an
                # incomplete delete rather than resurrecting a partial image.
                os.unlink(self.COMPLETE_NAME, dir_fd=generation_fd)
                os.fsync(generation_fd)
                for name in sorted(actual - {self.COMPLETE_NAME}):
                    os.unlink(name, dir_fd=generation_fd)
                os.fsync(generation_fd)
            finally:
                os.close(generation_fd)
            generation.rmdir()
            self._fsync_directory(generation.parent)
            self._remove_empty_incarnation(generation.parent)

    def _remove_empty_incarnation(self, incarnation: Path) -> None:
        if self.preserve_incarnation_roots:
            return
        try:
            incarnation.rmdir()
        except OSError as exc:
            if exc.errno in {errno.ENOTEMPTY, errno.EEXIST}:
                return
            raise
        self._fsync_directory(self.root)

    def inventory(
        self,
        *,
        ignored_root_entries: Sequence[str] = (),
        ignored_incarnation_entries: Sequence[str] = (),
    ) -> tuple[HibernationGenerationInventory, ...]:
        if not os.path.lexists(self.root):
            return ()
        self._ensure_directory(self.root, create=False)
        ignored_root = frozenset(ignored_root_entries)
        ignored_incarnation = frozenset(ignored_incarnation_entries)
        inventory: list[HibernationGenerationInventory] = []
        for incarnation in sorted(self.root.iterdir(), key=lambda path: path.name):
            if incarnation.name == self.LOCK_NAME:
                continue
            if incarnation.name in ignored_root:
                self._ensure_private_owned_entry(
                    incarnation,
                    "ignored hibernation root entry",
                )
                continue
            match = re.fullmatch(
                r"([A-Za-z0-9][A-Za-z0-9_.:-]{0,127})\.sandbox-([0-9]+)",
                incarnation.name,
            )
            if match is None:
                raise HibernationError(
                    f"unexpected entry in hibernation root: {incarnation.name}"
                )
            self._ensure_directory(incarnation, create=False)
            sandbox_id = match.group(1)
            sandbox_generation = int(match.group(2))
            inventory.extend(
                self._inventory_incarnation_path(
                    incarnation,
                    sandbox_id=sandbox_id,
                    sandbox_generation=sandbox_generation,
                    ignored_entries=ignored_incarnation,
                )
            )
        return tuple(inventory)

    def inventory_incarnation(
        self,
        *,
        sandbox_id: str,
        sandbox_generation: int,
        ignored_entries: Sequence[str] = (),
    ) -> tuple[HibernationGenerationInventory, ...]:
        incarnation = self.generation_path(
            sandbox_id=sandbox_id,
            sandbox_generation=sandbox_generation,
            hibernation_generation=1,
        ).parent
        if not os.path.lexists(incarnation):
            return ()
        self._ensure_directory(incarnation, create=False)
        return tuple(
            self._inventory_incarnation_path(
                incarnation,
                sandbox_id=sandbox_id,
                sandbox_generation=sandbox_generation,
                ignored_entries=frozenset(ignored_entries),
            )
        )

    def _inventory_incarnation_path(
        self,
        incarnation: Path,
        *,
        sandbox_id: str,
        sandbox_generation: int,
        ignored_entries: frozenset[str],
    ) -> list[HibernationGenerationInventory]:
        inventory: list[HibernationGenerationInventory] = []
        for generation in sorted(incarnation.iterdir(), key=lambda path: path.name):
            if generation.name in ignored_entries:
                self._ensure_private_owned_entry(
                    generation,
                    "ignored hibernation incarnation entry",
                )
                continue
            generation_match = re.fullmatch(
                r"hibernate-([1-9][0-9]*)",
                generation.name,
            )
            if generation_match is None:
                raise HibernationError(
                    f"unexpected hibernation generation entry: {generation.name}"
                )
            self._ensure_directory(generation, create=False)
            hibernation_generation = int(generation_match.group(1))
            if os.path.lexists(generation / self.COMPLETE_NAME):
                manifest = self.load_published_metadata(
                    sandbox_id=sandbox_id,
                    sandbox_generation=sandbox_generation,
                    hibernation_generation=hibernation_generation,
                )
                state = "complete"
                digest = manifest.metadata_sha256
            else:
                state = "pending"
                digest = ""
            inventory.append(
                HibernationGenerationInventory(
                    sandbox_id=sandbox_id,
                    sandbox_generation=sandbox_generation,
                    hibernation_generation=hibernation_generation,
                    state=state,
                    metadata_sha256=digest,
                )
            )
        return inventory

    def _ensure_private_owned_entry(self, path: Path, label: str) -> None:
        try:
            info = path.lstat()
        except FileNotFoundError as exc:
            raise HibernationError(f"{label} disappeared") from exc
        if stat.S_ISLNK(info.st_mode) or not (
            stat.S_ISDIR(info.st_mode) or stat.S_ISREG(info.st_mode)
        ):
            raise HibernationError(f"{label} must be a real file or directory")
        # Explicitly ignored directories can be OCI/overlay roots whose
        # read/traverse bits must match the image. The private parent still
        # protects them from discovery; ownership and write access are the
        # integrity boundary. Ignored regular files remain strictly private.
        forbidden_mode = 0o022 if stat.S_ISDIR(info.st_mode) else 0o077
        if info.st_uid != self.owner_uid or info.st_mode & forbidden_mode:
            raise HibernationError(f"{label} must be private and owned")

    @contextmanager
    def _locked(self) -> Iterator[None]:
        self._ensure_directory(self.root, create=True)
        root_fd = self._open_directory(self.root)
        descriptor = -1
        try:
            descriptor = os.open(
                self.LOCK_NAME,
                os.O_RDWR
                | os.O_CREAT
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0),
                0o600,
                dir_fd=root_fd,
            )
            info = os.fstat(descriptor)
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_uid != self.owner_uid
                or info.st_mode & 0o077
            ):
                raise HibernationError(
                    "hibernation artifact lock must be a private owned regular file"
                )
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            yield
        finally:
            if descriptor >= 0:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
                os.close(descriptor)
            os.close(root_fd)

    def _require_generation_path(self, generation: Path) -> None:
        try:
            relative = generation.relative_to(self.root)
        except ValueError as exc:
            raise HibernationError("generation path escaped hibernation root") from exc
        if len(relative.parts) != 2:
            raise HibernationError("generation path has an invalid depth")
        self._ensure_directory(self.root, create=False)
        self._ensure_directory(generation.parent, create=False)
        self._ensure_directory(generation, create=False)

    def _ensure_directory(self, path: Path, *, create: bool) -> None:
        if create and not os.path.lexists(path):
            path.mkdir(mode=0o700)
            if path.parent != path:
                self._fsync_directory(path.parent)
        try:
            info = path.lstat()
        except FileNotFoundError as exc:
            raise HibernationError(
                f"hibernation directory does not exist: {path.name}"
            ) from exc
        if not stat.S_ISDIR(info.st_mode) or stat.S_ISLNK(info.st_mode):
            raise HibernationError("hibernation path must be a real directory")
        if info.st_uid != self.owner_uid:
            raise HibernationError("hibernation directory has an unexpected owner")
        if info.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
            raise HibernationError(
                "hibernation directory cannot be group/world writable"
            )

    @staticmethod
    def _open_directory(path: Path) -> int:
        return os.open(
            path,
            os.O_RDONLY
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0),
        )

    @classmethod
    def _fsync_directory(cls, path: Path) -> None:
        descriptor = cls._open_directory(path)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    @classmethod
    def _atomic_write_at(cls, root: Path, name: str, payload: bytes) -> None:
        descriptor, raw_path = tempfile.mkstemp(
            prefix=f".{name}.",
            suffix=".tmp",
            dir=root,
        )
        temporary = Path(raw_path)
        try:
            os.fchmod(descriptor, 0o600)
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, root / name)
            cls._fsync_directory(root)
        finally:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass

    @classmethod
    def _read_json_at(
        cls,
        root: Path,
        name: str,
        label: str,
    ) -> dict[str, Any]:
        root_fd = cls._open_directory(root)
        try:
            try:
                descriptor = os.open(
                    name,
                    os.O_RDONLY
                    | getattr(os, "O_CLOEXEC", 0)
                    | getattr(os, "O_NOFOLLOW", 0),
                    dir_fd=root_fd,
                )
            except FileNotFoundError as exc:
                raise HibernationValidationError(f"{label} is absent") from exc
            except OSError as exc:
                raise HibernationValidationError(
                    f"{label} cannot be safely opened"
                ) from exc
            try:
                info = os.fstat(descriptor)
                if not stat.S_ISREG(info.st_mode):
                    raise HibernationValidationError(
                        f"{label} must be a regular file"
                    )
                if info.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
                    raise HibernationValidationError(
                        f"{label} cannot be group/world writable"
                    )
                if info.st_size > MAX_HIBERNATION_JSON_BYTES:
                    raise HibernationValidationError(f"{label} is too large")
                payload = bytearray()
                while len(payload) <= MAX_HIBERNATION_JSON_BYTES:
                    block = os.read(
                        descriptor,
                        min(
                            64 * 1024,
                            MAX_HIBERNATION_JSON_BYTES + 1 - len(payload),
                        ),
                    )
                    if not block:
                        break
                    payload.extend(block)
                if len(payload) > MAX_HIBERNATION_JSON_BYTES:
                    raise HibernationValidationError(f"{label} is too large")
            finally:
                os.close(descriptor)
        finally:
            os.close(root_fd)
        try:
            raw = json.loads(bytes(payload).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise HibernationValidationError(f"{label} is invalid JSON") from exc
        if not isinstance(raw, dict):
            raise HibernationValidationError(f"{label} must be a JSON object")
        return raw


@dataclass(frozen=True)
class HibernationRecord:
    sandbox_id: str
    sandbox_generation: int
    hibernation_generation: int
    spec_sha256: str
    state: HibernationState
    authority: HibernationAuthority
    operation_kind: str
    operation_id: str
    revision: int
    updated_ns: int
    sentry_pid: int | None = None
    sentry_start_time_ticks: int | None = None
    candidate_pid: int | None = None
    candidate_start_time_ticks: int | None = None
    manifest_sha256: str = ""
    recovery_reason: str = ""
    version: int = HIBERNATION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.version != HIBERNATION_SCHEMA_VERSION:
            raise ValueError("unsupported hibernation state version")
        _validate_safe_id("sandbox_id", self.sandbox_id)
        _validate_positive_int("sandbox_generation", self.sandbox_generation)
        _validate_nonnegative_int("hibernation_generation", self.hibernation_generation)
        _validate_digest("spec_sha256", self.spec_sha256)
        if not isinstance(self.state, HibernationState):
            raise ValueError("hibernation state is invalid")
        if not isinstance(self.authority, HibernationAuthority):
            raise ValueError("hibernation authority is invalid")
        if self.operation_kind not in {
            "initialize",
            "hibernate",
            "restore",
            "reconcile",
        }:
            raise ValueError("hibernation operation_kind is invalid")
        _validate_safe_id("operation_id", self.operation_id)
        _validate_nonnegative_int("revision", self.revision)
        _validate_positive_int("updated_ns", self.updated_ns)
        for label, pid in (
            ("sentry_pid", self.sentry_pid),
            ("candidate_pid", self.candidate_pid),
        ):
            if pid is not None:
                _validate_positive_int(label, pid)
        for label, ticks in (
            ("sentry_start_time_ticks", self.sentry_start_time_ticks),
            ("candidate_start_time_ticks", self.candidate_start_time_ticks),
        ):
            if ticks is not None:
                _validate_positive_int(label, ticks)
        if (self.sentry_pid is None) != (self.sentry_start_time_ticks is None):
            raise ValueError(
                "sentry_pid and sentry_start_time_ticks must be set together"
            )
        if (self.candidate_pid is None) != (self.candidate_start_time_ticks is None):
            raise ValueError(
                "candidate_pid and candidate_start_time_ticks must be set together"
            )
        if self.manifest_sha256:
            _validate_digest("manifest_sha256", self.manifest_sha256)
        if len(self.recovery_reason) > 1024:
            raise ValueError("recovery_reason is too large")
        legal = {
            (HibernationState.RUNNING, HibernationAuthority.LIVE),
            (HibernationState.HIBERNATING, HibernationAuthority.LIVE),
            (HibernationState.HIBERNATING, HibernationAuthority.PENDING),
            (HibernationState.PARKED, HibernationAuthority.PARKED),
            (HibernationState.RESTORING, HibernationAuthority.PARKED),
            (HibernationState.RESTORING, HibernationAuthority.CANDIDATE),
            (HibernationState.RECOVERY_REQUIRED, HibernationAuthority.PENDING),
            (HibernationState.RECOVERY_REQUIRED, HibernationAuthority.PARKED),
            (HibernationState.RECOVERY_REQUIRED, HibernationAuthority.CANDIDATE),
            (HibernationState.RECOVERY_REQUIRED, HibernationAuthority.NONE),
        }
        if (self.state, self.authority) not in legal:
            raise ValueError(
                f"illegal hibernation state/authority pair: "
                f"{self.state.value}/{self.authority.value}"
            )
        if self.state == HibernationState.PARKED and not self.manifest_sha256:
            raise ValueError("parked hibernation state requires a manifest digest")
        if (
            self.authority == HibernationAuthority.CANDIDATE
            and self.candidate_pid is None
        ):
            raise ValueError("candidate authority requires a process identity")

    def to_dict(self) -> dict[str, Any]:
        return {
            "authority": self.authority.value,
            "candidate_pid": self.candidate_pid,
            "candidate_start_time_ticks": self.candidate_start_time_ticks,
            "hibernation_generation": self.hibernation_generation,
            "manifest_sha256": self.manifest_sha256,
            "operation_id": self.operation_id,
            "operation_kind": self.operation_kind,
            "recovery_reason": self.recovery_reason,
            "revision": self.revision,
            "sandbox_generation": self.sandbox_generation,
            "sandbox_id": self.sandbox_id,
            "sentry_pid": self.sentry_pid,
            "sentry_start_time_ticks": self.sentry_start_time_ticks,
            "spec_sha256": self.spec_sha256,
            "state": self.state.value,
            "updated_ns": self.updated_ns,
            "version": self.version,
        }

    @classmethod
    def from_dict(cls, raw: object) -> "HibernationRecord":
        if not isinstance(raw, dict):
            raise ValueError("hibernation record must be a JSON object")
        _require_exact_keys(
            "hibernation record",
            raw,
            {
                "authority",
                "candidate_pid",
                "candidate_start_time_ticks",
                "hibernation_generation",
                "manifest_sha256",
                "operation_id",
                "operation_kind",
                "recovery_reason",
                "revision",
                "sandbox_generation",
                "sandbox_id",
                "sentry_pid",
                "sentry_start_time_ticks",
                "spec_sha256",
                "state",
                "updated_ns",
                "version",
            },
        )
        try:
            state = HibernationState(raw["state"])
            authority = HibernationAuthority(raw["authority"])
        except (TypeError, ValueError) as exc:
            raise ValueError("hibernation state or authority is invalid") from exc
        sentry_pid = raw["sentry_pid"]
        sentry_start_time_ticks = raw["sentry_start_time_ticks"]
        candidate_pid = raw["candidate_pid"]
        candidate_start_time_ticks = raw["candidate_start_time_ticks"]
        return cls(
            version=_validate_positive_int("state version", raw["version"]),
            sandbox_id=_validate_safe_id("sandbox_id", raw["sandbox_id"]),
            sandbox_generation=_validate_nonnegative_int(
                "sandbox_generation", raw["sandbox_generation"]
            ),
            hibernation_generation=_validate_nonnegative_int(
                "hibernation_generation", raw["hibernation_generation"]
            ),
            spec_sha256=_validate_digest("spec_sha256", raw["spec_sha256"]),
            state=state,
            authority=authority,
            operation_kind=str(raw["operation_kind"]),
            operation_id=_validate_safe_id("operation_id", raw["operation_id"]),
            revision=_validate_nonnegative_int("revision", raw["revision"]),
            updated_ns=_validate_positive_int("updated_ns", raw["updated_ns"]),
            sentry_pid=(
                None
                if sentry_pid is None
                else _validate_positive_int("sentry_pid", sentry_pid)
            ),
            sentry_start_time_ticks=(
                None
                if sentry_start_time_ticks is None
                else _validate_positive_int(
                    "sentry_start_time_ticks", sentry_start_time_ticks
                )
            ),
            candidate_pid=(
                None
                if candidate_pid is None
                else _validate_positive_int("candidate_pid", candidate_pid)
            ),
            candidate_start_time_ticks=(
                None
                if candidate_start_time_ticks is None
                else _validate_positive_int(
                    "candidate_start_time_ticks",
                    candidate_start_time_ticks,
                )
            ),
            manifest_sha256=(
                ""
                if not raw["manifest_sha256"]
                else _validate_digest("manifest_sha256", raw["manifest_sha256"])
            ),
            recovery_reason=str(raw["recovery_reason"]),
        )


class HibernationJournal:
    """Durable compare-and-swap journal for one sandbox incarnation."""

    def __init__(self, path: Path) -> None:
        if not path.is_absolute():
            raise ValueError("hibernation journal path must be absolute")
        self.path = path
        self.lock_path = path.with_name(f".{path.name}.lock")

    @contextmanager
    def _locked(self) -> Iterator[None]:
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        parent_info = self.path.parent.lstat()
        if not stat.S_ISDIR(parent_info.st_mode) or stat.S_ISLNK(parent_info.st_mode):
            raise HibernationError(
                "hibernation journal parent must be a real directory"
            )
        if parent_info.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
            raise HibernationError(
                "hibernation journal parent cannot be group/world writable"
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
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_uid != os.geteuid()
                or info.st_mode & 0o077
            ):
                raise HibernationError(
                    "hibernation journal lock must be a private owned regular file"
                )
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            yield
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)

    def load(self) -> HibernationRecord | None:
        with self._locked():
            return self._load_unlocked()

    def load_snapshot(self) -> HibernationRecord | None:
        """Read one atomically committed revision without waiting for its writer.

        Journal updates are fsynced temporary files installed with ``os.replace``.
        A monitoring reader can therefore safely observe either the previous or
        next complete revision without joining the lifecycle operation fence.
        This must remain read-only: compare-and-swap decisions still use
        :meth:`load` or another journal method that holds ``_locked``.
        """

        return self._load_unlocked()

    def initialize_running(
        self,
        *,
        sandbox_id: str,
        sandbox_generation: int,
        spec_sha256: str,
        operation_id: str,
        sentry_pid: int | None,
        sentry_start_time_ticks: int | None,
    ) -> HibernationRecord:
        with self._locked():
            existing = self._load_unlocked()
            if existing is not None:
                expected = (sandbox_id, sandbox_generation, spec_sha256)
                actual = (
                    existing.sandbox_id,
                    existing.sandbox_generation,
                    existing.spec_sha256,
                )
                if actual != expected:
                    raise HibernationConflictError(
                        "hibernation journal belongs to another sandbox incarnation"
                    )
                return existing
            record = HibernationRecord(
                sandbox_id=sandbox_id,
                sandbox_generation=sandbox_generation,
                hibernation_generation=0,
                spec_sha256=spec_sha256,
                state=HibernationState.RUNNING,
                authority=HibernationAuthority.LIVE,
                operation_kind="initialize",
                operation_id=operation_id,
                revision=0,
                updated_ns=time.time_ns(),
                sentry_pid=sentry_pid,
                sentry_start_time_ticks=sentry_start_time_ticks,
            )
            self._save_unlocked(record)
            return record

    def initialize_parked(
        self,
        manifest: HibernationManifest,
    ) -> HibernationRecord:
        """Adopt a complete portable generation without creating a backend."""
        with self._locked():
            existing = self._load_unlocked()
            if existing is not None:
                expected = (
                    manifest.sandbox_id,
                    manifest.sandbox_generation,
                    manifest.hibernation_generation,
                    manifest.spec_sha256,
                    manifest.metadata_sha256,
                )
                actual = (
                    existing.sandbox_id,
                    existing.sandbox_generation,
                    existing.hibernation_generation,
                    existing.spec_sha256,
                    existing.manifest_sha256,
                )
                if (
                    actual == expected
                    and existing.state == HibernationState.PARKED
                    and existing.authority == HibernationAuthority.PARKED
                ):
                    return existing
                raise HibernationConflictError(
                    "hibernation journal already owns another lifecycle state"
                )
            record = HibernationRecord(
                sandbox_id=manifest.sandbox_id,
                sandbox_generation=manifest.sandbox_generation,
                hibernation_generation=manifest.hibernation_generation,
                spec_sha256=manifest.spec_sha256,
                state=HibernationState.PARKED,
                authority=HibernationAuthority.PARKED,
                operation_kind="hibernate",
                operation_id=manifest.operation_id,
                revision=0,
                updated_ns=time.time_ns(),
                manifest_sha256=manifest.metadata_sha256,
            )
            self._save_unlocked(record)
            return record

    def begin_hibernate(
        self,
        *,
        operation_id: str,
        expected_revision: int,
    ) -> HibernationRecord:
        with self._locked():
            record = self._require_unlocked()
            if (
                record.operation_kind == "hibernate"
                and record.operation_id == operation_id
                and record.state
                in {
                    HibernationState.HIBERNATING,
                    HibernationState.PARKED,
                    HibernationState.RECOVERY_REQUIRED,
                }
            ):
                return record
            self._require_revision(record, expected_revision)
            if (
                record.state != HibernationState.RUNNING
                or record.authority != HibernationAuthority.LIVE
            ):
                raise HibernationConflictError("sandbox is not running")
            return self._replace_unlocked(
                record,
                hibernation_generation=record.hibernation_generation + 1,
                state=HibernationState.HIBERNATING,
                authority=HibernationAuthority.LIVE,
                operation_kind="hibernate",
                operation_id=_validate_safe_id("operation_id", operation_id),
                candidate_pid=None,
                candidate_start_time_ticks=None,
                manifest_sha256="",
                recovery_reason="",
            )

    def mark_sentry_reaped(
        self,
        *,
        operation_id: str,
        expected_revision: int,
    ) -> HibernationRecord:
        with self._locked():
            record = self._require_operation_unlocked(
                "hibernate", operation_id, expected_revision
            )
            if (
                record.state == HibernationState.HIBERNATING
                and record.authority == HibernationAuthority.PENDING
            ):
                return record
            if (
                record.state != HibernationState.HIBERNATING
                or record.authority != HibernationAuthority.LIVE
            ):
                raise HibernationConflictError(
                    "sentry can only be reaped from live hibernating state"
                )
            return self._replace_unlocked(
                record,
                authority=HibernationAuthority.PENDING,
                sentry_pid=None,
                sentry_start_time_ticks=None,
            )

    def abort_hibernate(
        self,
        *,
        operation_id: str,
        expected_revision: int,
        sentry_pid: int | None,
        sentry_start_time_ticks: int | None,
    ) -> HibernationRecord:
        with self._locked():
            record = self._require_operation_unlocked(
                "hibernate", operation_id, expected_revision
            )
            if (
                record.state != HibernationState.HIBERNATING
                or record.authority != HibernationAuthority.LIVE
            ):
                raise HibernationConflictError(
                    "hibernate cannot be aborted after the sentry is reaped"
                )
            return self._replace_unlocked(
                record,
                state=HibernationState.RUNNING,
                authority=HibernationAuthority.LIVE,
                sentry_pid=sentry_pid,
                sentry_start_time_ticks=sentry_start_time_ticks,
            )

    def commit_parked(
        self,
        manifest: HibernationManifest,
        *,
        operation_id: str,
        expected_revision: int,
    ) -> HibernationRecord:
        with self._locked():
            record = self._require_operation_unlocked(
                "hibernate", operation_id, expected_revision
            )
            if (
                record.state == HibernationState.PARKED
                and record.manifest_sha256 == manifest.metadata_sha256
            ):
                return record
            if (
                record.state != HibernationState.HIBERNATING
                or record.authority != HibernationAuthority.PENDING
            ):
                raise HibernationConflictError(
                    "park can only commit after the sentry is reaped"
                )
            if (
                manifest.sandbox_id != record.sandbox_id
                or manifest.sandbox_generation != record.sandbox_generation
                or manifest.hibernation_generation != record.hibernation_generation
                or manifest.operation_id != record.operation_id
                or manifest.spec_sha256 != record.spec_sha256
            ):
                raise HibernationConflictError(
                    "hibernation manifest does not match the pending generation"
                )
            return self._replace_unlocked(
                record,
                state=HibernationState.PARKED,
                authority=HibernationAuthority.PARKED,
                manifest_sha256=manifest.metadata_sha256,
            )

    def begin_restore(
        self,
        *,
        operation_id: str,
        expected_revision: int,
    ) -> HibernationRecord:
        with self._locked():
            record = self._require_unlocked()
            if (
                record.operation_kind == "restore"
                and record.operation_id == operation_id
                and record.state
                in {
                    HibernationState.RESTORING,
                    HibernationState.RUNNING,
                    HibernationState.RECOVERY_REQUIRED,
                }
            ):
                return record
            self._require_revision(record, expected_revision)
            if (
                record.state != HibernationState.PARKED
                or record.authority != HibernationAuthority.PARKED
            ):
                raise HibernationConflictError("sandbox is not parked")
            return self._replace_unlocked(
                record,
                state=HibernationState.RESTORING,
                authority=HibernationAuthority.PARKED,
                operation_kind="restore",
                operation_id=_validate_safe_id("operation_id", operation_id),
                candidate_pid=None,
                candidate_start_time_ticks=None,
                recovery_reason="",
            )

    def mark_candidate_started(
        self,
        *,
        operation_id: str,
        expected_revision: int,
        candidate_pid: int,
        candidate_start_time_ticks: int,
    ) -> HibernationRecord:
        with self._locked():
            record = self._require_operation_unlocked(
                "restore", operation_id, expected_revision
            )
            if (
                record.state == HibernationState.RESTORING
                and record.authority == HibernationAuthority.CANDIDATE
                and record.candidate_pid == candidate_pid
                and record.candidate_start_time_ticks == candidate_start_time_ticks
            ):
                return record
            if (
                record.state != HibernationState.RESTORING
                or record.authority != HibernationAuthority.PARKED
            ):
                raise HibernationConflictError(
                    "restore candidate can only start from a parked authority"
                )
            return self._replace_unlocked(
                record,
                authority=HibernationAuthority.CANDIDATE,
                candidate_pid=_validate_positive_int("candidate_pid", candidate_pid),
                candidate_start_time_ticks=_validate_positive_int(
                    "candidate_start_time_ticks", candidate_start_time_ticks
                ),
            )

    def commit_running(
        self,
        *,
        operation_id: str,
        expected_revision: int,
        sentry_pid: int,
        sentry_start_time_ticks: int,
    ) -> HibernationRecord:
        with self._locked():
            record = self._require_operation_unlocked(
                "restore", operation_id, expected_revision
            )
            if (
                record.state == HibernationState.RUNNING
                and record.sentry_pid == sentry_pid
                and record.sentry_start_time_ticks == sentry_start_time_ticks
            ):
                return record
            if (
                record.state != HibernationState.RESTORING
                or record.authority != HibernationAuthority.CANDIDATE
            ):
                raise HibernationConflictError(
                    "running can only commit from a verified restore candidate"
                )
            return self._replace_unlocked(
                record,
                state=HibernationState.RUNNING,
                authority=HibernationAuthority.LIVE,
                sentry_pid=_validate_positive_int("sentry_pid", sentry_pid),
                sentry_start_time_ticks=_validate_positive_int(
                    "sentry_start_time_ticks", sentry_start_time_ticks
                ),
                candidate_pid=None,
                candidate_start_time_ticks=None,
                manifest_sha256="",
            )

    def rollback_restore(
        self,
        *,
        operation_id: str,
        expected_revision: int,
        candidate_reaped: bool = False,
    ) -> HibernationRecord:
        with self._locked():
            record = self._require_operation_unlocked(
                "restore", operation_id, expected_revision
            )
            if record.state != HibernationState.RESTORING:
                raise HibernationConflictError("sandbox is not restoring")
            if (
                record.authority == HibernationAuthority.CANDIDATE
                and not candidate_reaped
            ):
                raise HibernationConflictError(
                    "restore cannot roll back while the candidate may be alive"
                )
            return self._replace_unlocked(
                record,
                state=HibernationState.PARKED,
                authority=HibernationAuthority.PARKED,
                candidate_pid=None,
                candidate_start_time_ticks=None,
            )

    def quarantine(
        self,
        *,
        reason: str,
        expected_revision: int,
        live_process_confirmed_dead: bool = False,
    ) -> HibernationRecord:
        with self._locked():
            record = self._require_unlocked()
            self._require_revision(record, expected_revision)
            if (
                record.authority == HibernationAuthority.LIVE
                and not live_process_confirmed_dead
            ):
                raise HibernationConflictError(
                    "a live sandbox must be fenced before quarantine"
                )
            reason = str(reason).strip()
            if not reason:
                raise ValueError("recovery reason is required")
            changes: dict[str, Any] = {
                "state": HibernationState.RECOVERY_REQUIRED,
                "operation_kind": "reconcile",
                "recovery_reason": reason,
            }
            if record.authority == HibernationAuthority.LIVE:
                changes.update(
                    authority=HibernationAuthority.NONE,
                    sentry_pid=None,
                    sentry_start_time_ticks=None,
                )
            return self._replace_unlocked(
                record,
                **changes,
            )

    def _require_unlocked(self) -> HibernationRecord:
        record = self._load_unlocked()
        if record is None:
            raise HibernationConflictError("hibernation journal is not initialized")
        return record

    def _require_operation_unlocked(
        self,
        kind: str,
        operation_id: str,
        expected_revision: int,
    ) -> HibernationRecord:
        record = self._require_unlocked()
        if record.operation_kind != kind or record.operation_id != operation_id:
            raise HibernationConflictError(
                f"hibernation journal is owned by another {kind} operation"
            )
        self._require_revision(record, expected_revision)
        return record

    @staticmethod
    def _require_revision(record: HibernationRecord, expected_revision: int) -> None:
        _validate_nonnegative_int("expected_revision", expected_revision)
        if record.revision != expected_revision:
            raise HibernationConflictError(
                f"stale hibernation revision {expected_revision}; "
                f"current revision is {record.revision}"
            )

    def _replace_unlocked(
        self,
        record: HibernationRecord,
        **changes: Any,
    ) -> HibernationRecord:
        updated = replace(
            record,
            **changes,
            revision=record.revision + 1,
            updated_ns=time.time_ns(),
        )
        self._save_unlocked(updated)
        return updated

    def _load_unlocked(self) -> HibernationRecord | None:
        parent_fd = os.open(
            self.path.parent,
            os.O_RDONLY
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0),
        )
        try:
            descriptor = os.open(
                self.path.name,
                os.O_RDONLY
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=parent_fd,
            )
        except FileNotFoundError:
            os.close(parent_fd)
            return None
        except OSError as exc:
            os.close(parent_fd)
            raise HibernationError(
                "cannot safely open the hibernation journal"
            ) from exc
        try:
            info = os.fstat(descriptor)
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_uid != os.geteuid()
                or info.st_mode & (stat.S_IWGRP | stat.S_IWOTH)
            ):
                raise HibernationError(
                    "hibernation journal must be an owned private regular file"
                )
            if info.st_size > MAX_HIBERNATION_JSON_BYTES:
                raise HibernationError("hibernation journal is too large")
            payload = bytearray()
            while len(payload) <= MAX_HIBERNATION_JSON_BYTES:
                block = os.read(
                    descriptor,
                    min(
                        64 * 1024,
                        MAX_HIBERNATION_JSON_BYTES + 1 - len(payload),
                    ),
                )
                if not block:
                    break
                payload.extend(block)
            if len(payload) > MAX_HIBERNATION_JSON_BYTES:
                raise HibernationError("hibernation journal is too large")
            raw = json.loads(bytes(payload).decode("utf-8"))
            return HibernationRecord.from_dict(raw)
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
            raise HibernationError("hibernation journal is invalid") from exc
        finally:
            os.close(descriptor)
            os.close(parent_fd)

    def _save_unlocked(self, record: HibernationRecord) -> None:
        descriptor, raw_path = tempfile.mkstemp(
            prefix=f".{self.path.name}.",
            suffix=".tmp",
            dir=self.path.parent,
        )
        temporary = Path(raw_path)
        try:
            os.fchmod(descriptor, 0o600)
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(_canonical_json(record.to_dict()) + b"\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self.path)
            directory_fd = os.open(
                self.path.parent,
                os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
            )
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        finally:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass


def classify_hibernation_recovery(
    record: HibernationRecord,
    *,
    sentry_alive: bool,
    candidate_alive: bool,
    complete_manifest: bool,
) -> HibernationRecoveryAction:
    if record.state == HibernationState.RECOVERY_REQUIRED:
        return HibernationRecoveryAction.OPERATOR_REQUIRED
    if record.state == HibernationState.RUNNING:
        return (
            HibernationRecoveryAction.ADOPT_RUNNING
            if sentry_alive
            else HibernationRecoveryAction.QUARANTINE
        )
    if record.state == HibernationState.HIBERNATING:
        if record.authority == HibernationAuthority.LIVE and sentry_alive:
            return (
                HibernationRecoveryAction.FINISH_PUBLISHED_GENERATION
                if complete_manifest
                else HibernationRecoveryAction.RESUME_OR_RETRY_HIBERNATE
            )
        return HibernationRecoveryAction.FINISH_PENDING_GENERATION
    if record.state == HibernationState.PARKED:
        return (
            HibernationRecoveryAction.KEEP_PARKED
            if complete_manifest
            else HibernationRecoveryAction.QUARANTINE
        )
    if record.state == HibernationState.RESTORING:
        if record.authority == HibernationAuthority.CANDIDATE:
            if candidate_alive:
                return HibernationRecoveryAction.VERIFY_CANDIDATE
            return (
                HibernationRecoveryAction.ROLLBACK_TO_PARKED
                if complete_manifest
                else HibernationRecoveryAction.QUARANTINE
            )
        if candidate_alive:
            return HibernationRecoveryAction.VERIFY_CANDIDATE
        return (
            HibernationRecoveryAction.RETRY_RESTORE
            if complete_manifest
            else HibernationRecoveryAction.QUARANTINE
        )
    return HibernationRecoveryAction.QUARANTINE


@dataclass(frozen=True)
class HibernationReconcileResult:
    action: HibernationRecoveryAction
    record: HibernationRecord
    changed: bool
    detail: str = ""


class HibernationReconciler:
    """Reconcile journal, process identity, and the local COMPLETE marker."""

    def __init__(
        self,
        journal: HibernationJournal,
        artifacts: HibernationArtifactStore,
        *,
        runtime_sha256: str,
        proc_root: Path = Path("/proc"),
        candidate_identity_resolver: (
            Callable[[HibernationRecord], tuple[int, int] | None] | None
        ) = None,
    ) -> None:
        self.journal = journal
        self.artifacts = artifacts
        self.runtime_sha256 = _validate_digest("runtime_sha256", runtime_sha256)
        self.proc_root = proc_root
        self.candidate_identity_resolver = candidate_identity_resolver

    def reconcile(self) -> HibernationReconcileResult:
        record = self.journal.load()
        if record is None:
            raise HibernationConflictError("hibernation journal is not initialized")
        original_revision = record.revision
        sentry_alive = hibernation_process_identity_matches(
            record.sentry_pid,
            record.sentry_start_time_ticks,
            proc_root=self.proc_root,
        )
        if (
            record.state == HibernationState.RESTORING
            and record.authority == HibernationAuthority.PARKED
            and self.candidate_identity_resolver is not None
        ):
            try:
                resolved_candidate = self.candidate_identity_resolver(record)
            except Exception as exc:
                raise HibernationError(
                    "could not determine whether restore already started"
                ) from exc
            if resolved_candidate is not None:
                if (
                    not isinstance(resolved_candidate, tuple)
                    or len(resolved_candidate) != 2
                ):
                    raise HibernationError(
                        "restore candidate resolver returned an invalid identity"
                    )
                candidate_pid = _validate_positive_int(
                    "resolved candidate_pid",
                    resolved_candidate[0],
                )
                candidate_start_time_ticks = _validate_positive_int(
                    "resolved candidate_start_time_ticks",
                    resolved_candidate[1],
                )
                if hibernation_process_identity_matches(
                    candidate_pid,
                    candidate_start_time_ticks,
                    proc_root=self.proc_root,
                ):
                    # This closes the crash window between a successful runtime
                    # restore and publishing the candidate identity. Once this
                    # CAS is durable, reconciliation must verify/adopt or reap
                    # this process; it can no longer retry restore blindly.
                    record = self.journal.mark_candidate_started(
                        operation_id=record.operation_id,
                        expected_revision=record.revision,
                        candidate_pid=candidate_pid,
                        candidate_start_time_ticks=candidate_start_time_ticks,
                    )
        candidate_alive = hibernation_process_identity_matches(
            record.candidate_pid,
            record.candidate_start_time_ticks,
            proc_root=self.proc_root,
        )
        manifest: HibernationManifest | None = None
        artifact_error = ""
        if record.hibernation_generation > 0:
            generation = self.artifacts.generation_path(
                sandbox_id=record.sandbox_id,
                sandbox_generation=record.sandbox_generation,
                hibernation_generation=record.hibernation_generation,
            )
            if os.path.lexists(generation / self.artifacts.COMPLETE_NAME):
                try:
                    loader = (
                        self.artifacts.load_published_metadata
                        if record.state == HibernationState.RESTORING
                        else self.artifacts.load_complete
                    )
                    manifest = loader(
                        sandbox_id=record.sandbox_id,
                        sandbox_generation=record.sandbox_generation,
                        hibernation_generation=record.hibernation_generation,
                    )
                    manifest.validate_identity(
                        sandbox_id=record.sandbox_id,
                        sandbox_generation=record.sandbox_generation,
                        spec_sha256=record.spec_sha256,
                        runtime_sha256=self.runtime_sha256,
                    )
                    if manifest.hibernation_generation != record.hibernation_generation:
                        raise HibernationValidationError(
                            "complete artifact does not match the journal generation"
                        )
                    if (
                        record.state == HibernationState.HIBERNATING
                        and manifest.operation_id != record.operation_id
                    ):
                        raise HibernationValidationError(
                            "complete artifact does not match the pending operation"
                        )
                    if (
                        record.state
                        in {
                            HibernationState.PARKED,
                            HibernationState.RESTORING,
                        }
                        and manifest.metadata_sha256 != record.manifest_sha256
                    ):
                        raise HibernationValidationError(
                            "complete artifact does not match the authoritative "
                            "journal digest"
                        )
                except (HibernationError, ValueError) as exc:
                    manifest = None
                    artifact_error = str(exc)

        action = classify_hibernation_recovery(
            record,
            sentry_alive=sentry_alive,
            candidate_alive=candidate_alive,
            complete_manifest=manifest is not None,
        )

        if artifact_error and not sentry_alive and not candidate_alive:
            record = self.journal.quarantine(
                reason=f"complete artifact is invalid: {artifact_error}",
                expected_revision=record.revision,
                live_process_confirmed_dead=(
                    record.authority == HibernationAuthority.LIVE
                ),
            )
            return HibernationReconcileResult(
                action=HibernationRecoveryAction.QUARANTINE,
                record=record,
                changed=True,
                detail=artifact_error,
            )

        if action == HibernationRecoveryAction.FINISH_PENDING_GENERATION:
            if record.authority == HibernationAuthority.LIVE and not sentry_alive:
                record = self.journal.mark_sentry_reaped(
                    operation_id=record.operation_id,
                    expected_revision=record.revision,
                )
            if manifest is not None:
                record = self.journal.commit_parked(
                    manifest,
                    operation_id=record.operation_id,
                    expected_revision=record.revision,
                )
                action = HibernationRecoveryAction.KEEP_PARKED
        elif action == HibernationRecoveryAction.ROLLBACK_TO_PARKED:
            record = self.journal.rollback_restore(
                operation_id=record.operation_id,
                expected_revision=record.revision,
                candidate_reaped=True,
            )
            action = HibernationRecoveryAction.KEEP_PARKED
        elif action == HibernationRecoveryAction.QUARANTINE:
            record = self.journal.quarantine(
                reason="reconciliation could not prove an authoritative runtime",
                expected_revision=record.revision,
                live_process_confirmed_dead=(
                    record.authority == HibernationAuthority.LIVE and not sentry_alive
                ),
            )
        return HibernationReconcileResult(
            action=action,
            record=record,
            changed=record.revision != original_revision,
            detail=artifact_error,
        )


class HibernationJournalStore:
    """Strict inventory and startup reconciliation for node-local journals."""

    _JOURNAL_PATTERN = re.compile(
        r"([A-Za-z0-9][A-Za-z0-9_.:-]{0,127})\.sandbox-([0-9]+)\.json\Z"
    )

    def __init__(self, root: Path) -> None:
        if not root.is_absolute():
            raise ValueError("hibernation journal root must be absolute")
        self.root = root

    def journal(
        self,
        *,
        sandbox_id: str,
        sandbox_generation: int,
    ) -> HibernationJournal:
        sandbox_id = _validate_safe_id("sandbox_id", sandbox_id)
        sandbox_generation = _validate_nonnegative_int(
            "sandbox_generation",
            sandbox_generation,
        )
        return HibernationJournal(
            self.root / f"{sandbox_id}.sandbox-{sandbox_generation}.json"
        )

    def inventory(self) -> tuple[HibernationRecord, ...]:
        if not os.path.lexists(self.root):
            return ()
        self._require_root()
        records: list[HibernationRecord] = []
        for path in sorted(self.root.iterdir(), key=lambda item: item.name):
            if path.name.startswith(".") and path.name.endswith(".json.lock"):
                self._require_private_regular_file(path, "hibernation journal lock")
                journal_name = path.name[1 : -len(".lock")]
                if (
                    self._JOURNAL_PATTERN.fullmatch(journal_name) is None
                    or not (self.root / journal_name).is_file()
                ):
                    raise HibernationError(
                        "hibernation journal lock has no owning journal"
                    )
                continue
            match = self._JOURNAL_PATTERN.fullmatch(path.name)
            if match is None:
                raise HibernationError(
                    f"unexpected entry in hibernation journal root: {path.name}"
                )
            self._require_private_regular_file(path, "hibernation journal")
            record = HibernationJournal(path).load()
            if record is None:
                raise HibernationError("inventoried hibernation journal disappeared")
            if record.sandbox_id != match.group(1) or record.sandbox_generation != int(
                match.group(2)
            ):
                raise HibernationConflictError(
                    "hibernation journal filename does not match its incarnation"
                )
            records.append(record)
        return tuple(records)

    def reconcile_all(
        self,
        artifacts: HibernationArtifactStore,
        *,
        runtime_sha256: str,
        proc_root: Path = Path("/proc"),
        candidate_identity_resolver: (
            Callable[[HibernationRecord], tuple[int, int] | None] | None
        ) = None,
    ) -> tuple[HibernationReconcileResult, ...]:
        records = self.inventory()
        by_incarnation = {
            (record.sandbox_id, record.sandbox_generation): record for record in records
        }
        for artifact in artifacts.inventory():
            identity = (artifact.sandbox_id, artifact.sandbox_generation)
            record = by_incarnation.get(identity)
            if record is None:
                raise HibernationConflictError(
                    "hibernation artifact exists without an owning journal"
                )
            if artifact.hibernation_generation > record.hibernation_generation:
                raise HibernationConflictError(
                    "hibernation artifact generation is ahead of its journal"
                )
        return tuple(
            HibernationReconciler(
                self.journal(
                    sandbox_id=record.sandbox_id,
                    sandbox_generation=record.sandbox_generation,
                ),
                artifacts,
                runtime_sha256=runtime_sha256,
                proc_root=proc_root,
                candidate_identity_resolver=candidate_identity_resolver,
            ).reconcile()
            for record in records
        )

    def remove(
        self,
        *,
        sandbox_id: str,
        sandbox_generation: int,
        expected_revision: int,
        processes_confirmed_dead: bool,
    ) -> None:
        if not processes_confirmed_dead:
            raise HibernationConflictError(
                "cannot remove a journal while a backend may still be alive"
            )
        journal = self.journal(
            sandbox_id=sandbox_id,
            sandbox_generation=sandbox_generation,
        )
        with journal._locked():
            record = journal._require_unlocked()
            journal._require_revision(record, expected_revision)
            journal.path.unlink()
            try:
                journal.lock_path.unlink()
            except FileNotFoundError:
                pass
            directory_fd = os.open(
                self.root,
                os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
            )
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)

    def _require_root(self) -> None:
        try:
            info = self.root.lstat()
        except FileNotFoundError as exc:
            raise HibernationError("hibernation journal root does not exist") from exc
        if not stat.S_ISDIR(info.st_mode) or stat.S_ISLNK(info.st_mode):
            raise HibernationError("hibernation journal root must be a real directory")
        if info.st_uid != os.geteuid():
            raise HibernationError("hibernation journal root has an unexpected owner")
        if info.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
            raise HibernationError(
                "hibernation journal root cannot be group/world writable"
            )

    @staticmethod
    def _require_private_regular_file(path: Path, label: str) -> None:
        info = path.lstat()
        if (
            not stat.S_ISREG(info.st_mode)
            or stat.S_ISLNK(info.st_mode)
            or info.st_uid != os.geteuid()
            or info.st_mode & (stat.S_IWGRP | stat.S_IWOTH)
        ):
            raise HibernationError(f"{label} must be an owned private regular file")


def linux_process_start_time_ticks(
    pid: int,
    *,
    proc_root: Path = Path("/proc"),
) -> int:
    pid = _validate_positive_int("pid", pid)
    try:
        raw = (proc_root / str(pid) / "stat").read_text(encoding="ascii")
    except (OSError, UnicodeDecodeError) as exc:
        raise ProcessLookupError(pid) from exc
    # comm is parenthesized and may contain spaces or ')', so fields after it
    # are located from the final closing parenthesis. starttime is proc stat
    # field 22; the suffix begins at field 3.
    closing = raw.rfind(")")
    if closing < 2 or closing + 2 >= len(raw):
        raise ValueError("process stat has an invalid format")
    fields = raw[closing + 2 :].split()
    try:
        start_time_ticks = int(fields[19])
    except (IndexError, ValueError) as exc:
        raise ValueError("process stat is missing starttime") from exc
    return _validate_positive_int("process start_time_ticks", start_time_ticks)


def hibernation_process_identity_matches(
    pid: int | None,
    start_time_ticks: int | None,
    *,
    proc_root: Path = Path("/proc"),
) -> bool:
    if pid is None or start_time_ticks is None:
        return False
    try:
        actual = linux_process_start_time_ticks(pid, proc_root=proc_root)
    except (ProcessLookupError, ValueError):
        return False
    return actual == start_time_ticks


def hibernation_memory_backing_reservation_mb(
    memory_mb: int,
    *,
    allocator_chunk_mb: int = HIBERNATION_ALLOCATOR_CHUNK_MB,
) -> int:
    memory_mb = _validate_positive_int("memory_mb", memory_mb)
    allocator_chunk_mb = _validate_positive_int(
        "allocator_chunk_mb", allocator_chunk_mb
    )
    return (memory_mb // allocator_chunk_mb + 1) * allocator_chunk_mb


def hibernation_disk_reservation_mb(
    *,
    memory_mb: int,
    writable_disk_mb: int,
    allocator_chunk_mb: int = HIBERNATION_ALLOCATOR_CHUNK_MB,
    private_pages_mb: int | None = None,
    fixed_overhead_mb: int = HIBERNATION_FIXED_OVERHEAD_MB,
) -> int:
    memory_mb = _validate_positive_int("memory_mb", memory_mb)
    writable_disk_mb = _validate_positive_int("writable_disk_mb", writable_disk_mb)
    # Until every private MemoryFile is externalized or a smaller universal
    # limit is proven, its ordinary page image may consume another full guest
    # memory bound. Admission must use this conservative value.
    if private_pages_mb is None:
        private_pages_mb = memory_mb
    private_pages_mb = _validate_nonnegative_int("private_pages_mb", private_pages_mb)
    fixed_overhead_mb = _validate_nonnegative_int(
        "fixed_overhead_mb", fixed_overhead_mb
    )
    return (
        writable_disk_mb
        + hibernation_memory_backing_reservation_mb(
            memory_mb,
            allocator_chunk_mb=allocator_chunk_mb,
        )
        + private_pages_mb
        + fixed_overhead_mb
    )


def manifests_total_allocated_bytes(
    manifests: Sequence[HibernationManifest],
) -> int:
    return sum(
        item.allocated_bytes for manifest in manifests for item in manifest.files
    )


@dataclass(frozen=True)
class HibernationDiskReservation:
    sandbox_id: str
    sandbox_generation: int
    project_id: int
    memory_mb: int
    writable_disk_mb: int
    memory_backing_mb: int
    private_pages_mb: int
    fixed_overhead_mb: int
    created_ns: int

    def __post_init__(self) -> None:
        _validate_safe_id("sandbox_id", self.sandbox_id)
        _validate_positive_int("sandbox_generation", self.sandbox_generation)
        _validate_positive_int("project_id", self.project_id)
        _validate_positive_int("memory_mb", self.memory_mb)
        _validate_positive_int("writable_disk_mb", self.writable_disk_mb)
        _validate_positive_int("memory_backing_mb", self.memory_backing_mb)
        _validate_nonnegative_int("private_pages_mb", self.private_pages_mb)
        _validate_nonnegative_int("fixed_overhead_mb", self.fixed_overhead_mb)
        _validate_positive_int("created_ns", self.created_ns)
        expected_backing = hibernation_memory_backing_reservation_mb(self.memory_mb)
        if self.memory_backing_mb != expected_backing:
            raise ValueError(
                "memory_backing_mb does not match the allocator reservation"
            )

    @property
    def hibernation_quota_mb(self) -> int:
        return self.memory_backing_mb + self.private_pages_mb + self.fixed_overhead_mb

    @property
    def total_mb(self) -> int:
        return self.writable_disk_mb + self.hibernation_quota_mb

    def to_dict(self) -> dict[str, Any]:
        return {
            "created_ns": self.created_ns,
            "fixed_overhead_mb": self.fixed_overhead_mb,
            "memory_backing_mb": self.memory_backing_mb,
            "memory_mb": self.memory_mb,
            "private_pages_mb": self.private_pages_mb,
            "project_id": self.project_id,
            "sandbox_generation": self.sandbox_generation,
            "sandbox_id": self.sandbox_id,
            "writable_disk_mb": self.writable_disk_mb,
        }

    @classmethod
    def from_dict(cls, raw: object) -> "HibernationDiskReservation":
        if not isinstance(raw, dict):
            raise ValueError("hibernation disk reservation must be a JSON object")
        _require_exact_keys(
            "hibernation disk reservation",
            raw,
            {
                "created_ns",
                "fixed_overhead_mb",
                "memory_backing_mb",
                "memory_mb",
                "private_pages_mb",
                "project_id",
                "sandbox_generation",
                "sandbox_id",
                "writable_disk_mb",
            },
        )
        return cls(
            sandbox_id=_validate_safe_id("sandbox_id", raw["sandbox_id"]),
            sandbox_generation=_validate_nonnegative_int(
                "sandbox_generation", raw["sandbox_generation"]
            ),
            project_id=_validate_positive_int("project_id", raw["project_id"]),
            memory_mb=_validate_positive_int("memory_mb", raw["memory_mb"]),
            writable_disk_mb=_validate_positive_int(
                "writable_disk_mb", raw["writable_disk_mb"]
            ),
            memory_backing_mb=_validate_positive_int(
                "memory_backing_mb", raw["memory_backing_mb"]
            ),
            private_pages_mb=_validate_nonnegative_int(
                "private_pages_mb", raw["private_pages_mb"]
            ),
            fixed_overhead_mb=_validate_nonnegative_int(
                "fixed_overhead_mb", raw["fixed_overhead_mb"]
            ),
            created_ns=_validate_positive_int("created_ns", raw["created_ns"]),
        )


@dataclass(frozen=True)
class HibernationDiskInventory:
    capacity_mb: int
    safety_headroom_mb: int
    reserved_mb: int
    available_mb: int
    reservations: tuple[HibernationDiskReservation, ...]


@dataclass(frozen=True)
class _HibernationDiskLedgerState:
    capacity_mb: int
    safety_headroom_mb: int
    next_project_id: int
    reservations: tuple[HibernationDiskReservation, ...]
    tombstones: dict[str, int]
    version: int = 1

    def __post_init__(self) -> None:
        if self.version != 1:
            raise ValueError("unsupported hibernation disk ledger version")
        _validate_positive_int("capacity_mb", self.capacity_mb)
        _validate_nonnegative_int("safety_headroom_mb", self.safety_headroom_mb)
        if self.safety_headroom_mb >= self.capacity_mb:
            raise ValueError("safety_headroom_mb must be smaller than capacity_mb")
        _validate_positive_int("next_project_id", self.next_project_id)
        identities: set[tuple[str, int]] = set()
        project_ids: set[int] = set()
        sandbox_ids: set[str] = set()
        for reservation in self.reservations:
            if not isinstance(reservation, HibernationDiskReservation):
                raise ValueError("hibernation disk reservation is invalid")
            identity = (
                reservation.sandbox_id,
                reservation.sandbox_generation,
            )
            if identity in identities or reservation.sandbox_id in sandbox_ids:
                raise ValueError("hibernation disk ledger has duplicate reservations")
            if reservation.project_id in project_ids:
                raise ValueError("hibernation disk ledger has duplicate project IDs")
            identities.add(identity)
            sandbox_ids.add(reservation.sandbox_id)
            project_ids.add(reservation.project_id)
        for sandbox_id, generation in self.tombstones.items():
            _validate_safe_id("tombstone sandbox_id", sandbox_id)
            _validate_nonnegative_int("tombstone generation", generation)
        if project_ids and self.next_project_id <= max(project_ids):
            raise ValueError("next_project_id does not fence allocated project IDs")
        if sum(item.total_mb for item in self.reservations) > (
            self.capacity_mb - self.safety_headroom_mb
        ):
            raise ValueError("hibernation disk ledger exceeds usable capacity")

    def to_dict(self) -> dict[str, Any]:
        return {
            "capacity_mb": self.capacity_mb,
            "next_project_id": self.next_project_id,
            "reservations": [
                item.to_dict()
                for item in sorted(
                    self.reservations,
                    key=lambda value: (value.sandbox_id, value.sandbox_generation),
                )
            ],
            "safety_headroom_mb": self.safety_headroom_mb,
            "tombstones": dict(sorted(self.tombstones.items())),
            "version": self.version,
        }

    @classmethod
    def from_dict(cls, raw: object) -> "_HibernationDiskLedgerState":
        if not isinstance(raw, dict):
            raise ValueError("hibernation disk ledger must be a JSON object")
        _require_exact_keys(
            "hibernation disk ledger",
            raw,
            {
                "capacity_mb",
                "next_project_id",
                "reservations",
                "safety_headroom_mb",
                "tombstones",
                "version",
            },
        )
        reservations = raw["reservations"]
        tombstones = raw["tombstones"]
        if not isinstance(reservations, list):
            raise ValueError("hibernation disk reservations must be a list")
        if not isinstance(tombstones, dict):
            raise ValueError("hibernation disk tombstones must be a JSON object")
        return cls(
            version=_validate_positive_int("ledger version", raw["version"]),
            capacity_mb=_validate_positive_int("capacity_mb", raw["capacity_mb"]),
            safety_headroom_mb=_validate_nonnegative_int(
                "safety_headroom_mb", raw["safety_headroom_mb"]
            ),
            next_project_id=_validate_positive_int(
                "next_project_id", raw["next_project_id"]
            ),
            reservations=tuple(
                HibernationDiskReservation.from_dict(item) for item in reservations
            ),
            tombstones={
                _validate_safe_id(
                    "tombstone sandbox_id", sandbox_id
                ): _validate_nonnegative_int("tombstone generation", generation)
                for sandbox_id, generation in tombstones.items()
            },
        )


class HibernationDiskLedger:
    """Crash-durable logical disk admission for parkable sandbox incarnations.

    Sparse allocation never changes the charge. Each live incarnation owns one
    monotonically allocated XFS project ID for its hibernation tree, while the
    reservation also includes the separately enforced Docker writable quota.
    """

    def __init__(
        self,
        path: Path,
        *,
        capacity_mb: int,
        safety_headroom_mb: int,
        first_project_id: int = 200_000,
    ) -> None:
        if not path.is_absolute():
            raise ValueError("hibernation disk ledger path must be absolute")
        self.path = path
        self.lock_path = path.with_name(f".{path.name}.lock")
        self.capacity_mb = _validate_positive_int("capacity_mb", capacity_mb)
        self.safety_headroom_mb = _validate_nonnegative_int(
            "safety_headroom_mb", safety_headroom_mb
        )
        if self.safety_headroom_mb >= self.capacity_mb:
            raise ValueError("safety_headroom_mb must be smaller than capacity_mb")
        self.first_project_id = _validate_positive_int(
            "first_project_id", first_project_id
        )

    def reserve(
        self,
        *,
        sandbox_id: str,
        sandbox_generation: int,
        memory_mb: int,
        writable_disk_mb: int,
        private_pages_mb: int | None = None,
        fixed_overhead_mb: int = HIBERNATION_FIXED_OVERHEAD_MB,
        allow_released_generation: bool = False,
    ) -> HibernationDiskReservation:
        sandbox_id = _validate_safe_id("sandbox_id", sandbox_id)
        sandbox_generation = _validate_nonnegative_int(
            "sandbox_generation", sandbox_generation
        )
        memory_mb = _validate_positive_int("memory_mb", memory_mb)
        writable_disk_mb = _validate_positive_int("writable_disk_mb", writable_disk_mb)
        if private_pages_mb is None:
            private_pages_mb = memory_mb
        private_pages_mb = _validate_nonnegative_int(
            "private_pages_mb", private_pages_mb
        )
        fixed_overhead_mb = _validate_nonnegative_int(
            "fixed_overhead_mb", fixed_overhead_mb
        )
        with self._locked():
            state = self._load_unlocked()
            for existing in state.reservations:
                if existing.sandbox_id != sandbox_id:
                    continue
                if existing.sandbox_generation != sandbox_generation:
                    raise HibernationConflictError(
                        "sandbox already has a disk reservation for another "
                        "generation"
                    )
                expected = (
                    memory_mb,
                    writable_disk_mb,
                    private_pages_mb,
                    fixed_overhead_mb,
                )
                actual = (
                    existing.memory_mb,
                    existing.writable_disk_mb,
                    existing.private_pages_mb,
                    existing.fixed_overhead_mb,
                )
                if actual != expected:
                    raise HibernationConflictError(
                        "replayed disk reservation has different resource bounds"
                    )
                return existing
            if (
                state.tombstones.get(sandbox_id, -1) >= sandbox_generation
                and not allow_released_generation
            ):
                raise HibernationConflictError(
                    "sandbox disk reservation generation is fenced by a tombstone"
                )
            reservation = HibernationDiskReservation(
                sandbox_id=sandbox_id,
                sandbox_generation=sandbox_generation,
                project_id=state.next_project_id,
                memory_mb=memory_mb,
                writable_disk_mb=writable_disk_mb,
                memory_backing_mb=hibernation_memory_backing_reservation_mb(memory_mb),
                private_pages_mb=private_pages_mb,
                fixed_overhead_mb=fixed_overhead_mb,
                created_ns=time.time_ns(),
            )
            reserved_mb = sum(item.total_mb for item in state.reservations)
            usable_mb = state.capacity_mb - state.safety_headroom_mb
            if reserved_mb + reservation.total_mb > usable_mb:
                raise HibernationCapacityError(
                    "hibernation disk reservation exceeds fail-closed node "
                    f"capacity: requested={reservation.total_mb}MB, "
                    f"reserved={reserved_mb}MB, usable={usable_mb}MB"
                )
            updated = replace(
                state,
                next_project_id=state.next_project_id + 1,
                reservations=(*state.reservations, reservation),
            )
            self._save_unlocked(updated)
            return reservation

    def release(
        self,
        *,
        sandbox_id: str,
        sandbox_generation: int,
    ) -> HibernationDiskReservation | None:
        sandbox_id = _validate_safe_id("sandbox_id", sandbox_id)
        sandbox_generation = _validate_nonnegative_int(
            "sandbox_generation", sandbox_generation
        )
        with self._locked():
            state = self._load_unlocked()
            matching = [
                item for item in state.reservations if item.sandbox_id == sandbox_id
            ]
            if not matching:
                if state.tombstones.get(sandbox_id, -1) >= sandbox_generation:
                    return None
                raise HibernationConflictError(
                    "sandbox disk reservation does not exist"
                )
            reservation = matching[0]
            if reservation.sandbox_generation != sandbox_generation:
                raise HibernationConflictError(
                    "cannot release another sandbox generation's disk reservation"
                )
            tombstones = dict(state.tombstones)
            tombstones[sandbox_id] = max(
                sandbox_generation,
                tombstones.get(sandbox_id, -1),
            )
            self._save_unlocked(
                replace(
                    state,
                    reservations=tuple(
                        item for item in state.reservations if item != reservation
                    ),
                    tombstones=tombstones,
                )
            )
            return reservation

    def inventory(self) -> HibernationDiskInventory:
        with self._locked():
            state = self._load_unlocked()
        reserved_mb = sum(item.total_mb for item in state.reservations)
        usable_mb = state.capacity_mb - state.safety_headroom_mb
        return HibernationDiskInventory(
            capacity_mb=state.capacity_mb,
            safety_headroom_mb=state.safety_headroom_mb,
            reserved_mb=reserved_mb,
            available_mb=usable_mb - reserved_mb,
            reservations=tuple(
                sorted(
                    state.reservations,
                    key=lambda item: (item.sandbox_id, item.sandbox_generation),
                )
            ),
        )

    def require_quota_consistency(
        self,
        *,
        expected_incarnations: Sequence[tuple[str, int]],
        project_hard_limits_mb: dict[int, int],
        path_project_ids: dict[tuple[str, int], int],
    ) -> HibernationDiskInventory:
        inventory = self.inventory()
        expected = {
            (
                _validate_safe_id("sandbox_id", sandbox_id),
                _validate_positive_int("sandbox_generation", sandbox_generation),
            )
            for sandbox_id, sandbox_generation in expected_incarnations
        }
        actual = {
            (item.sandbox_id, item.sandbox_generation)
            for item in inventory.reservations
        }
        if actual != expected:
            raise HibernationQuotaError(
                "disk reservation inventory does not match live sandbox " "incarnations"
            )
        if set(path_project_ids) != actual:
            raise HibernationQuotaError(
                "hibernation quota paths do not exactly match reservations"
            )
        expected_projects = {item.project_id for item in inventory.reservations}
        if set(project_hard_limits_mb) != expected_projects:
            raise HibernationQuotaError(
                "XFS project quota inventory does not exactly match reservations"
            )
        for item in inventory.reservations:
            identity = (item.sandbox_id, item.sandbox_generation)
            if path_project_ids[identity] != item.project_id:
                raise HibernationQuotaError(
                    "hibernation path has the wrong XFS project ID"
                )
            if project_hard_limits_mb[item.project_id] != item.hibernation_quota_mb:
                raise HibernationQuotaError(
                    "hibernation XFS project hard limit does not match reservation"
                )
        return inventory

    @contextmanager
    def _locked(self) -> Iterator[None]:
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        parent = self.path.parent.lstat()
        if not stat.S_ISDIR(parent.st_mode) or stat.S_ISLNK(parent.st_mode):
            raise HibernationError("disk ledger parent must be a real directory")
        if parent.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
            raise HibernationError("disk ledger parent cannot be group/world writable")
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
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_uid != os.geteuid()
                or info.st_mode & 0o077
            ):
                raise HibernationError(
                    "disk ledger lock must be a private owned regular file"
                )
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            yield
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)

    def _load_unlocked(self) -> _HibernationDiskLedgerState:
        if not os.path.lexists(self.path):
            return _HibernationDiskLedgerState(
                capacity_mb=self.capacity_mb,
                safety_headroom_mb=self.safety_headroom_mb,
                next_project_id=self.first_project_id,
                reservations=(),
                tombstones={},
            )
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(self.path, flags)
        try:
            info = os.fstat(descriptor)
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_uid != os.geteuid()
                or info.st_mode & 0o077
                or info.st_size > MAX_HIBERNATION_JSON_BYTES
            ):
                raise HibernationQuotaError(
                    "disk ledger must be a small private owned regular file"
                )
            payload = bytearray()
            while len(payload) <= MAX_HIBERNATION_JSON_BYTES:
                block = os.read(
                    descriptor,
                    min(
                        64 * 1024,
                        MAX_HIBERNATION_JSON_BYTES + 1 - len(payload),
                    ),
                )
                if not block:
                    break
                payload.extend(block)
        finally:
            os.close(descriptor)
        if len(payload) > MAX_HIBERNATION_JSON_BYTES:
            raise HibernationQuotaError("disk ledger is too large")
        try:
            raw = json.loads(bytes(payload).decode("utf-8"))
            state = _HibernationDiskLedgerState.from_dict(raw)
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
            raise HibernationQuotaError("disk ledger is invalid") from exc
        if (
            state.capacity_mb != self.capacity_mb
            or state.safety_headroom_mb != self.safety_headroom_mb
        ):
            raise HibernationQuotaError(
                "configured disk capacity differs from the durable ledger"
            )
        return state

    def _save_unlocked(self, state: _HibernationDiskLedgerState) -> None:
        payload = _canonical_json(state.to_dict()) + b"\n"
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
            os.replace(temporary, self.path)
            HibernationArtifactStore._fsync_directory(self.path.parent)
        finally:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass
