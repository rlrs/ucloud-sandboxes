from __future__ import annotations

from dataclasses import dataclass
import hashlib
import ipaddress
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import subprocess
import tarfile
import tempfile
import time
from typing import Any
from uuid import uuid4

from .direct_network import NETWORK_CIDR
from .direct_registry import DirectSandboxRegistration
from .hibernation import (
    HibernationArtifactFile,
    HibernationArtifactStore,
    HibernationCompatibilityError,
    HibernationFileRole,
    HibernationManifest,
    HibernationRuntimeFingerprint,
)
from .runtime_identity import NodeRuntimeIdentity
from .sandbox import SandboxSpec, sandbox_spec_fingerprint
from .storage_native_registry import StorageSnapshotPublication


DIRECT_MIGRATION_SCHEMA = "direct-parked-migration-v2"
STORAGE_NATIVE_MIGRATION_SCHEMA = "storage-native-v1"
DIRECT_MIGRATION_METADATA = "ucloud-migration.json"
MIGRATION_CONNECTION_POLICY_DISCONNECT = "disconnect"
MIGRATION_CONNECTION_POLICY_NONE = "none"
_DIGEST_LENGTH = 64


class DirectMigrationError(RuntimeError):
    pass


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")


@dataclass(frozen=True)
class PortableArtifactFile:
    name: str
    role: HibernationFileRole
    logical_bytes: int
    allocated_bytes: int

    def __post_init__(self) -> None:
        if (
            not self.name
            or self.name in {".", ".."}
            or "/" in self.name
            or PurePosixPath(self.name).name != self.name
        ):
            raise ValueError("portable artifact name must be a safe basename")
        if not isinstance(self.role, HibernationFileRole):
            raise ValueError("portable artifact role is invalid")
        if self.logical_bytes < 0 or self.allocated_bytes < 0:
            raise ValueError("portable artifact sizes cannot be negative")

    def to_dict(self) -> dict[str, Any]:
        return {
            "allocated_bytes": self.allocated_bytes,
            "logical_bytes": self.logical_bytes,
            "name": self.name,
            "role": self.role.value,
        }

    @classmethod
    def from_dict(cls, raw: object) -> "PortableArtifactFile":
        if not isinstance(raw, dict) or set(raw) != {
            "allocated_bytes",
            "logical_bytes",
            "name",
            "role",
        }:
            raise ValueError("portable artifact file has an invalid schema")
        return cls(
            name=str(raw["name"]),
            role=HibernationFileRole(str(raw["role"])),
            logical_bytes=int(raw["logical_bytes"]),
            allocated_bytes=int(raw["allocated_bytes"]),
        )


@dataclass(frozen=True)
class DirectMigrationManifest:
    spec: SandboxSpec
    sandbox_generation: int
    create_operation_id: str
    runtime_identity: NodeRuntimeIdentity
    hibernation_generation: int
    park_operation_id: str
    captured_ns: int
    runtime: HibernationRuntimeFingerprint
    source_manifest_sha256: str
    source_guest_ip: str | None
    connection_policy: str
    files: tuple[PortableArtifactFile, ...]
    schema: str = DIRECT_MIGRATION_SCHEMA

    def __post_init__(self) -> None:
        if self.schema != DIRECT_MIGRATION_SCHEMA:
            raise ValueError("unsupported direct migration schema")
        self.spec.validate()
        if self.sandbox_generation < 0 or self.hibernation_generation < 1:
            raise ValueError("direct migration generations are invalid")
        if not self.create_operation_id or not self.park_operation_id:
            raise ValueError("direct migration operation identities are required")
        if self.captured_ns < 1:
            raise ValueError("direct migration capture timestamp is invalid")
        if (
            len(self.source_manifest_sha256) != _DIGEST_LENGTH
            or any(
                character not in "0123456789abcdef"
                for character in self.source_manifest_sha256
            )
        ):
            raise ValueError("source manifest digest is invalid")
        expected_connection_policy = (
            MIGRATION_CONNECTION_POLICY_NONE
            if self.spec.network == "none"
            else MIGRATION_CONNECTION_POLICY_DISCONNECT
        )
        if self.connection_policy != expected_connection_policy:
            raise ValueError(
                "direct migration connection policy does not match sandbox networking"
            )
        if self.spec.network == "none":
            if self.source_guest_ip is not None:
                raise ValueError(
                    "network=none migration cannot carry a source guest IP"
                )
        else:
            if self.source_guest_ip is None:
                raise ValueError(
                    "networked migration requires its source guest IP"
                )
            try:
                source_guest_ip = ipaddress.ip_address(self.source_guest_ip)
            except ValueError as exc:
                raise ValueError("source guest IP is invalid") from exc
            if not isinstance(source_guest_ip, ipaddress.IPv4Address):
                raise ValueError("source guest IP must be IPv4")
            if source_guest_ip not in NETWORK_CIDR:
                raise ValueError("source guest IP is outside the direct network")
        if not self.files or len({item.name for item in self.files}) != len(self.files):
            raise ValueError("direct migration artifact inventory is invalid")
        roles = [item.role for item in self.files]
        for required in (
            HibernationFileRole.MAIN_MEMORY,
            HibernationFileRole.KERNEL_STATE,
            HibernationFileRole.ALLOCATOR_METADATA,
        ):
            if roles.count(required) != 1:
                raise ValueError(
                    f"direct migration requires exactly one {required.value} file"
                )

    @property
    def sandbox_id(self) -> str:
        return self.spec.id

    @property
    def spec_sha256(self) -> str:
        return sandbox_spec_fingerprint(self.spec)

    def to_dict(self) -> dict[str, Any]:
        return {
            "captured_ns": self.captured_ns,
            "create_operation_id": self.create_operation_id,
            "files": [item.to_dict() for item in self.files],
            "hibernation_generation": self.hibernation_generation,
            "park_operation_id": self.park_operation_id,
            "connection_policy": self.connection_policy,
            "runtime": self.runtime.to_dict(),
            "runtime_identity": self.runtime_identity.to_dict(),
            "sandbox_generation": self.sandbox_generation,
            "schema": self.schema,
            "source_manifest_sha256": self.source_manifest_sha256,
            "source_guest_ip": self.source_guest_ip,
            "spec": self.spec.to_dict(),
            "spec_sha256": self.spec_sha256,
        }

    @classmethod
    def from_dict(cls, raw: object) -> "DirectMigrationManifest":
        if not isinstance(raw, dict) or set(raw) != {
            "captured_ns",
            "create_operation_id",
            "files",
            "hibernation_generation",
            "park_operation_id",
            "connection_policy",
            "runtime",
            "runtime_identity",
            "sandbox_generation",
            "schema",
            "source_manifest_sha256",
            "source_guest_ip",
            "spec",
            "spec_sha256",
        }:
            raise ValueError("direct migration manifest has an invalid schema")
        files = raw["files"]
        if not isinstance(files, list):
            raise ValueError("direct migration files must be a list")
        manifest = cls(
            spec=SandboxSpec.from_dict(raw["spec"]),
            sandbox_generation=int(raw["sandbox_generation"]),
            create_operation_id=str(raw["create_operation_id"]),
            runtime_identity=NodeRuntimeIdentity.from_dict(raw["runtime_identity"]),
            hibernation_generation=int(raw["hibernation_generation"]),
            park_operation_id=str(raw["park_operation_id"]),
            connection_policy=str(raw["connection_policy"]),
            captured_ns=int(raw["captured_ns"]),
            runtime=HibernationRuntimeFingerprint.from_dict(raw["runtime"]),
            source_manifest_sha256=str(raw["source_manifest_sha256"]),
            source_guest_ip=(
                str(raw["source_guest_ip"])
                if raw["source_guest_ip"] is not None
                else None
            ),
            files=tuple(PortableArtifactFile.from_dict(item) for item in files),
            schema=str(raw["schema"]),
        )
        if str(raw["spec_sha256"]) != manifest.spec_sha256:
            raise ValueError("direct migration spec digest does not match")
        return manifest

    @classmethod
    def from_local(
        cls,
        registration: DirectSandboxRegistration,
        manifest: HibernationManifest,
        *,
        runtime_identity: NodeRuntimeIdentity,
        source_guest_ip: str | None,
    ) -> "DirectMigrationManifest":
        manifest.require_compatible(
            sandbox_id=registration.sandbox_id,
            sandbox_generation=registration.sandbox_generation,
            spec_sha256=registration.spec_sha256,
            runtime_sha256=manifest.runtime.digest,
        )
        expected_container_id = hashlib.sha256(
            (
                f"{registration.sandbox_id}:"
                f"{registration.sandbox_generation}"
            ).encode("utf-8")
        ).hexdigest()
        if (
            registration.phase != "owned"
            or registration.runtime_identity_sha256 != runtime_identity.digest
            or NodeRuntimeIdentity.from_fingerprint(manifest.runtime)
            != runtime_identity
            or registration.rootfs_sha256 != manifest.runtime.rootfs_sha256
            or registration.container_id != manifest.container_id
            or registration.container_id != expected_container_id
        ):
            raise DirectMigrationError(
                "source registration is not owned by this runtime identity"
            )
        return cls(
            spec=registration.spec,
            sandbox_generation=registration.sandbox_generation,
            create_operation_id=registration.operation_id,
            runtime_identity=runtime_identity,
            hibernation_generation=manifest.hibernation_generation,
            park_operation_id=manifest.operation_id,
            captured_ns=manifest.created_ns,
            runtime=manifest.runtime,
            source_manifest_sha256=manifest.metadata_sha256,
            source_guest_ip=source_guest_ip,
            connection_policy=(
                MIGRATION_CONNECTION_POLICY_NONE
                if registration.spec.network == "none"
                else MIGRATION_CONNECTION_POLICY_DISCONNECT
            ),
            files=tuple(
                PortableArtifactFile(
                    name=item.name,
                    role=item.role,
                    logical_bytes=item.logical_bytes,
                    allocated_bytes=item.allocated_bytes,
                )
                for item in manifest.files
            ),
        )


@dataclass(frozen=True)
class StorageNativeMigration:
    """Portable runtime metadata fenced to one durable block publication."""

    manifest: DirectMigrationManifest
    publication: StorageSnapshotPublication
    schema: str = STORAGE_NATIVE_MIGRATION_SCHEMA

    def __post_init__(self) -> None:
        if self.schema != STORAGE_NATIVE_MIGRATION_SCHEMA:
            raise ValueError("unsupported storage-native migration schema")
        if self.publication.virtual_size <= 0:
            raise ValueError("storage-native migration has no virtual size")

    @property
    def sha256(self) -> str:
        return hashlib.sha256(_canonical_json(self.to_dict())).hexdigest()

    def to_dict(self) -> dict[str, Any]:
        return {
            "manifest": self.manifest.to_dict(),
            "publication": self.publication.to_dict(),
            "schema": self.schema,
        }

    @classmethod
    def from_dict(cls, raw: object) -> "StorageNativeMigration":
        if not isinstance(raw, dict) or set(raw) != {
            "manifest",
            "publication",
            "schema",
        }:
            raise ValueError("storage-native migration has an invalid schema")
        return cls(
            manifest=DirectMigrationManifest.from_dict(raw["manifest"]),
            publication=StorageSnapshotPublication.from_dict(raw["publication"]),
            schema=str(raw["schema"]),
        )


class StorageNativeMigrationStore:
    """Crash-durable small metadata records for prepared/staged migrations."""

    def __init__(self, root: Path) -> None:
        if not root.is_absolute():
            raise ValueError("storage-native migration root must be absolute")
        self.root = root

    def save(
        self,
        migration_id: str,
        migration: StorageNativeMigration,
    ) -> StorageNativeMigration:
        target = self._path(migration_id)
        target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        payload = _canonical_json(migration.to_dict()) + b"\n"
        if target.exists():
            existing = self.load(migration_id)
            if existing.sha256 != migration.sha256:
                raise DirectMigrationError(
                    "migration metadata already has another snapshot identity"
                )
            return existing
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{target.name}.",
            dir=target.parent,
        )
        temporary = Path(temporary_name)
        try:
            os.fchmod(descriptor, 0o600)
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            temporary.replace(target)
            self._fsync_directory(target.parent)
        except Exception:
            try:
                os.close(descriptor)
            except OSError:
                pass
            temporary.unlink(missing_ok=True)
            raise
        return migration

    def load(self, migration_id: str) -> StorageNativeMigration:
        path = self._path(migration_id)
        try:
            info = path.lstat()
            if not stat.S_ISREG(info.st_mode) or stat.S_ISLNK(info.st_mode):
                raise DirectMigrationError(
                    "storage-native migration metadata is not a regular file"
                )
            payload = path.read_bytes()
            if len(payload) > 1024 * 1024:
                raise DirectMigrationError(
                    "storage-native migration metadata is too large"
                )
            return StorageNativeMigration.from_dict(
                json.loads(payload.decode("ascii"))
            )
        except DirectMigrationError:
            raise
        except (
            FileNotFoundError,
            OSError,
            UnicodeDecodeError,
            json.JSONDecodeError,
            TypeError,
            ValueError,
        ) as exc:
            raise DirectMigrationError(
                "storage-native migration metadata is unavailable"
            ) from exc

    def discard(self, migration_id: str) -> None:
        path = self._path(migration_id)
        path.unlink(missing_ok=True)
        if path.parent.exists():
            self._fsync_directory(path.parent)

    def rebind_mounted_snapshot(
        self,
        migration: StorageNativeMigration,
        *,
        expected_runtime_identity: NodeRuntimeIdentity,
        expected_runtime: HibernationRuntimeFingerprint,
        artifact_store: HibernationArtifactStore,
        writable_incarnation: Path,
    ) -> HibernationManifest:
        """Replace source-local file identities after mounting a remote snapshot."""

        portable = migration.manifest
        if (
            portable.runtime_identity != expected_runtime_identity
            or portable.runtime != expected_runtime
        ):
            raise HibernationCompatibilityError(
                "storage-native snapshot belongs to an incompatible runtime"
            )
        expected_incarnation = (
            f"{portable.sandbox_id}.sandbox-{portable.sandbox_generation}"
        )
        if (
            not writable_incarnation.is_absolute()
            or writable_incarnation.name != expected_incarnation
            or writable_incarnation.parent != artifact_store.root
        ):
            raise DirectMigrationError(
                "storage-native destination is not the sandbox quota incarnation"
            )
        generation = writable_incarnation / (
            f"hibernate-{portable.hibernation_generation}"
        )
        local_files = tuple(
            HibernationArtifactFile.from_path(
                generation / item.name,
                role=item.role,
            )
            for item in portable.files
        )
        for expected, actual in zip(portable.files, local_files):
            if actual.logical_bytes != expected.logical_bytes:
                raise DirectMigrationError(
                    f"migrated artifact size changed: {actual.name}"
                )
        local_manifest = HibernationManifest(
            sandbox_id=portable.sandbox_id,
            sandbox_generation=portable.sandbox_generation,
            hibernation_generation=portable.hibernation_generation,
            operation_id=portable.park_operation_id,
            spec_sha256=portable.spec_sha256,
            container_id=hashlib.sha256(
                (
                    f"{portable.sandbox_id}:"
                    f"{portable.sandbox_generation}"
                ).encode("utf-8")
            ).hexdigest(),
            created_ns=portable.captured_ns,
            runtime=portable.runtime,
            files=local_files,
        )
        published = artifact_store.load_published_metadata(
            sandbox_id=portable.sandbox_id,
            sandbox_generation=portable.sandbox_generation,
            hibernation_generation=portable.hibernation_generation,
        )
        if published.metadata_sha256 == local_manifest.metadata_sha256:
            local_manifest.validate_files(generation)
            return local_manifest
        if published.metadata_sha256 != portable.source_manifest_sha256:
            raise DirectMigrationError(
                "mounted snapshot has another hibernation manifest identity"
            )
        # COMPLETE is removed first so a crash cannot advertise a manifest
        # whose source-local inode/device identities no longer authenticate.
        (generation / artifact_store.COMPLETE_NAME).unlink()
        (generation / artifact_store.MANIFEST_NAME).unlink()
        self._fsync_directory(generation)
        return artifact_store.publish_complete(local_manifest)

    def _path(self, migration_id: str) -> Path:
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}", migration_id):
            raise ValueError("migration id contains unsupported characters")
        return self.root / f"{migration_id}.json"

    @staticmethod
    def _fsync_directory(path: Path) -> None:
        descriptor = os.open(
            path,
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_DIRECTORY", 0),
        )
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


@dataclass(frozen=True)
class DirectMigrationArchive:
    path: Path
    sha256: str
    physical_bytes: int
    elapsed_ms: float
    manifest: DirectMigrationManifest


class DirectMigrationArchiveStore:
    """Portable, sparse-aware transfer format for one parked sandbox.

    The local hibernation manifest deliberately authenticates inode/device
    identity and therefore cannot cross a node boundary. This archive carries
    only portable identities. Import reconstructs and publishes a new local
    manifest after the destination files acquire their new inode identities.
    """

    def __init__(
        self,
        *,
        tar_binary: str = "tar",
        compress: bool = True,
    ) -> None:
        self.tar_binary = tar_binary
        self.compress = compress
        self._gnu_tar: bool | None = None

    def export(
        self,
        *,
        registration: DirectSandboxRegistration,
        local_manifest: HibernationManifest,
        runtime_identity: NodeRuntimeIdentity,
        writable_incarnation: Path,
        archive_path: Path,
        source_guest_ip: str | None = None,
    ) -> DirectMigrationArchive:
        started = time.monotonic()
        if not writable_incarnation.is_absolute() or not archive_path.is_absolute():
            raise ValueError("migration source and archive paths must be absolute")
        if os.path.lexists(archive_path):
            raise DirectMigrationError("migration archive target already exists")
        portable = DirectMigrationManifest.from_local(
            registration,
            local_manifest,
            runtime_identity=runtime_identity,
            source_guest_ip=source_guest_ip,
        )
        generation_name = f"hibernate-{portable.hibernation_generation}"
        generation = writable_incarnation / generation_name
        upper = writable_incarnation / "upper"
        if not upper.is_dir() or upper.is_symlink():
            raise DirectMigrationError("parked sandbox upper directory is unavailable")
        local_manifest.validate_files(generation)

        archive_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(
            prefix=".ucloud-migration-meta.",
            dir=archive_path.parent,
        ) as raw_metadata:
            metadata_root = Path(raw_metadata)
            metadata_path = metadata_root / DIRECT_MIGRATION_METADATA
            metadata_path.write_bytes(
                json.dumps(
                    portable.to_dict(),
                    ensure_ascii=True,
                    separators=(",", ":"),
                    sort_keys=True,
                ).encode("ascii")
                + b"\n"
            )
            os.chmod(metadata_path, 0o600)
            members = ["upper"]
            members.extend(
                f"{generation_name}/{item.name}" for item in portable.files
            )
            command = [self.tar_binary]
            if self._is_gnu_tar():
                command.extend(
                    [
                        "--sparse",
                        "--xattrs",
                        "--acls",
                        "--numeric-owner",
                    ]
                )
                if self.compress:
                    # Level 1 is intentional: migration is latency-sensitive,
                    # and private-network transfer is much slower than either
                    # node's local disk. Checkpoint pages commonly compress
                    # extremely well even at the fastest gzip level.
                    command.append("--use-compress-program=gzip -1")
            elif self.compress:
                command.append("-z")
            command.extend(
                [
                    "-cf",
                    str(archive_path),
                    "-C",
                    str(metadata_root),
                    DIRECT_MIGRATION_METADATA,
                    "-C",
                    str(writable_incarnation),
                    *members,
                ]
            )
            result = subprocess.run(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=self._tar_environment(),
                check=False,
                timeout=3600,
            )
            if result.returncode != 0:
                try:
                    archive_path.unlink()
                except FileNotFoundError:
                    pass
                raise DirectMigrationError(
                    "could not create migration archive: "
                    + result.stderr.decode("utf-8", errors="replace")
                )
        self._fsync_file(archive_path)
        archive_sha256 = self._sha256_file(archive_path)
        info = archive_path.lstat()
        if not stat.S_ISREG(info.st_mode) or stat.S_ISLNK(info.st_mode):
            raise DirectMigrationError("migration archive is not a regular file")
        return DirectMigrationArchive(
            path=archive_path,
            sha256=archive_sha256,
            physical_bytes=info.st_blocks * 512,
            elapsed_ms=(time.monotonic() - started) * 1000,
            manifest=portable,
        )

    def read_manifest(
        self,
        archive_path: Path,
        *,
        expected_sha256: str,
    ) -> DirectMigrationManifest:
        """Read the authenticated first member without streaming checkpoint data."""
        self._require_digest(expected_sha256)
        if self._sha256_file(archive_path) != expected_sha256:
            raise DirectMigrationError("migration archive digest does not match")
        try:
            with tarfile.open(archive_path, mode="r:*") as archive:
                metadata = archive.next()
                if (
                    metadata is None
                    or metadata.name != DIRECT_MIGRATION_METADATA
                    or not metadata.isreg()
                ):
                    # Archives produced before metadata-first v2 remain
                    # readable, but take the deliberately slower strict path.
                    return self.inspect(
                        archive_path,
                        expected_sha256=expected_sha256,
                    )
                handle = archive.extractfile(metadata)
                if handle is None:
                    raise DirectMigrationError("migration metadata cannot be read")
                payload = handle.read(1024 * 1024 + 1)
                if len(payload) > 1024 * 1024:
                    raise DirectMigrationError("migration metadata is too large")
                return DirectMigrationManifest.from_dict(
                    json.loads(payload.decode("ascii"))
                )
        except DirectMigrationError:
            raise
        except (OSError, tarfile.TarError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise DirectMigrationError("migration archive is unreadable") from exc
        except (TypeError, ValueError) as exc:
            raise DirectMigrationError("migration metadata is invalid") from exc

    def inspect(
        self,
        archive_path: Path,
        *,
        expected_sha256: str | None = None,
    ) -> DirectMigrationManifest:
        if expected_sha256 is not None:
            self._require_digest(expected_sha256)
            if self._sha256_file(archive_path) != expected_sha256:
                raise DirectMigrationError("migration archive digest does not match")
        try:
            with tarfile.open(archive_path, mode="r:*") as archive:
                members = archive.getmembers()
                metadata_members = [
                    item for item in members if item.name == DIRECT_MIGRATION_METADATA
                ]
                if len(metadata_members) != 1 or not metadata_members[0].isreg():
                    raise DirectMigrationError(
                        "migration archive has no unique regular metadata member"
                    )
                handle = archive.extractfile(metadata_members[0])
                if handle is None:
                    raise DirectMigrationError("migration metadata cannot be read")
                payload = handle.read(1024 * 1024 + 1)
                if len(payload) > 1024 * 1024:
                    raise DirectMigrationError("migration metadata is too large")
                manifest = DirectMigrationManifest.from_dict(
                    json.loads(payload.decode("ascii"))
                )
                self._validate_members(members, manifest)
                return manifest
        except DirectMigrationError:
            raise
        except (OSError, tarfile.TarError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise DirectMigrationError("migration archive is unreadable") from exc
        except (TypeError, ValueError) as exc:
            raise DirectMigrationError("migration metadata is invalid") from exc

    def import_archive(
        self,
        archive_path: Path,
        *,
        expected_sha256: str,
        expected_runtime_identity: NodeRuntimeIdentity,
        expected_runtime: HibernationRuntimeFingerprint,
        artifact_store: HibernationArtifactStore,
        writable_incarnation: Path,
        portable_manifest: DirectMigrationManifest | None = None,
    ) -> tuple[DirectMigrationManifest, HibernationManifest]:
        """Verify, stage, and publish a destination-local parked generation."""
        portable = portable_manifest or self.inspect(
            archive_path,
            expected_sha256=expected_sha256,
        )
        if (
            portable.runtime_identity != expected_runtime_identity
            or portable.runtime != expected_runtime
        ):
            raise HibernationCompatibilityError(
                "migration archive belongs to an incompatible runtime"
            )
        expected_incarnation = (
            f"{portable.sandbox_id}.sandbox-{portable.sandbox_generation}"
        )
        if (
            not writable_incarnation.is_absolute()
            or writable_incarnation.name != expected_incarnation
            or writable_incarnation.parent != artifact_store.root
        ):
            raise DirectMigrationError(
                "migration destination is not the sandbox quota incarnation"
            )
        writable_incarnation.mkdir(mode=0o700, parents=False, exist_ok=True)
        existing = tuple(writable_incarnation.iterdir())
        if existing:
            raise DirectMigrationError(
                "migration destination must be empty before import"
            )

        staging = writable_incarnation / f".migration-{uuid4().hex}.pending"
        staging.mkdir(mode=0o700)
        promoted: list[Path] = []
        try:
            command = [self.tar_binary]
            if self._is_gnu_tar():
                command.extend(
                    [
                        "--xattrs",
                        "--acls",
                        "--numeric-owner",
                        "--same-owner",
                        "--same-permissions",
                    ]
                )
            generation_name = f"hibernate-{portable.hibernation_generation}"
            selected_members = [
                DIRECT_MIGRATION_METADATA,
                "upper",
                *(
                    f"{generation_name}/{item.name}"
                    for item in portable.files
                ),
            ]
            command.extend(
                [
                    "-xf",
                    str(archive_path),
                    "-C",
                    str(staging),
                    "--",
                    *selected_members,
                ]
            )
            result = subprocess.run(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=self._tar_environment(),
                check=False,
                timeout=3600,
            )
            if result.returncode != 0:
                raise DirectMigrationError(
                    "could not extract migration archive: "
                    + result.stderr.decode("utf-8", errors="replace")
                )
            metadata = staging / DIRECT_MIGRATION_METADATA
            metadata.unlink()
            if {item.name for item in staging.iterdir()} != {
                generation_name,
                "upper",
            }:
                raise DirectMigrationError(
                    "migration archive staged unexpected top-level members"
                )
            generation_members = {
                item.name
                for item in (staging / generation_name).iterdir()
            }
            if generation_members != {item.name for item in portable.files}:
                raise DirectMigrationError(
                    "migration archive staged an invalid artifact inventory"
                )
            for name in (generation_name, "upper"):
                source = staging / name
                target = writable_incarnation / name
                if not source.exists() or os.path.lexists(target):
                    raise DirectMigrationError(
                        f"migration archive did not stage a unique {name}"
                    )
                source.replace(target)
                promoted.append(target)
            staging.rmdir()
            self._fsync_directory(writable_incarnation)

            generation = writable_incarnation / generation_name
            local_files = tuple(
                HibernationArtifactFile.from_path(
                    generation / item.name,
                    role=item.role,
                )
                for item in portable.files
            )
            for expected, actual in zip(portable.files, local_files):
                if actual.logical_bytes != expected.logical_bytes:
                    raise DirectMigrationError(
                        f"migrated artifact size changed: {actual.name}"
                    )
            local_manifest = HibernationManifest(
                sandbox_id=portable.sandbox_id,
                sandbox_generation=portable.sandbox_generation,
                hibernation_generation=portable.hibernation_generation,
                operation_id=portable.park_operation_id,
                spec_sha256=portable.spec_sha256,
                container_id=hashlib.sha256(
                    (
                        f"{portable.sandbox_id}:"
                        f"{portable.sandbox_generation}"
                    ).encode("utf-8")
                ).hexdigest(),
                created_ns=portable.captured_ns,
                runtime=portable.runtime,
                files=local_files,
            )
            artifact_store.publish_complete(local_manifest)
            return portable, local_manifest
        except Exception:
            if staging.exists():
                shutil.rmtree(staging)
            for path in reversed(promoted):
                if path.is_dir() and not path.is_symlink():
                    shutil.rmtree(path)
                else:
                    try:
                        path.unlink()
                    except FileNotFoundError:
                        pass
            self._fsync_directory(writable_incarnation)
            raise

    def _is_gnu_tar(self) -> bool:
        if self._gnu_tar is None:
            try:
                result = subprocess.run(
                    (self.tar_binary, "--version"),
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    check=False,
                    timeout=5,
                )
            except (OSError, subprocess.TimeoutExpired):
                self._gnu_tar = False
            else:
                self._gnu_tar = (
                    result.returncode == 0 and b"GNU tar" in result.stdout
                )
        return self._gnu_tar

    @staticmethod
    def _tar_environment() -> dict[str, str]:
        environment = dict(os.environ)
        # macOS bsdtar otherwise synthesizes AppleDouble ``._*`` members that
        # are neither sandbox state nor accepted by the strict inventory.
        environment["COPYFILE_DISABLE"] = "1"
        return environment

    @staticmethod
    def _validate_members(
        members: list[tarfile.TarInfo],
        manifest: DirectMigrationManifest,
    ) -> None:
        generation = f"hibernate-{manifest.hibernation_generation}"
        artifact_names = {
            f"{generation}/{item.name}" for item in manifest.files
        }
        seen_artifacts: set[str] = set()
        symlink_paths: set[PurePosixPath] = set()
        metadata_count = 0
        for member in members:
            name = member.name.removeprefix("./")
            path = PurePosixPath(name)
            if (
                path.is_absolute()
                or ".." in path.parts
                or not path.parts
                or "\\" in name
            ):
                raise DirectMigrationError(
                    "migration archive contains an unsafe path"
                )
            if name == DIRECT_MIGRATION_METADATA:
                metadata_count += 1
                continue
            if name in artifact_names:
                if not member.isreg():
                    raise DirectMigrationError(
                        "migration artifact payload must be a regular file"
                    )
                seen_artifacts.add(name)
                continue
            if name == generation and member.isdir():
                continue
            if path.parts[0] == "upper":
                if member.issym():
                    link = PurePosixPath(member.linkname)
                    if link.is_absolute():
                        raise DirectMigrationError(
                            "migration archive contains an unsafe link"
                        )
                    symlink_paths.add(path)
                elif member.islnk():
                    link = PurePosixPath(member.linkname)
                    if (
                        link.is_absolute()
                        or ".." in link.parts
                        or not link.parts
                        or link.parts[0] != "upper"
                    ):
                        raise DirectMigrationError(
                            "migration archive contains an unsafe hard link"
                        )
                continue
            raise DirectMigrationError(
                f"migration archive contains an unexpected member: {name}"
            )
        for member in members:
            path = PurePosixPath(member.name.removeprefix("./"))
            if any(parent in symlink_paths for parent in path.parents):
                raise DirectMigrationError(
                    "migration archive writes through an archived symlink"
                )
        if metadata_count != 1 or seen_artifacts != artifact_names:
            raise DirectMigrationError(
                "migration archive artifact inventory is incomplete"
            )

    @staticmethod
    def _require_digest(value: str) -> None:
        if len(value) != _DIGEST_LENGTH or any(
            character not in "0123456789abcdef" for character in value
        ):
            raise ValueError("migration archive digest is invalid")

    @staticmethod
    def _sha256_file(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb", buffering=1024 * 1024) as handle:
            while block := handle.read(1024 * 1024):
                digest.update(block)
        return digest.hexdigest()

    @staticmethod
    def _fsync_file(path: Path) -> None:
        descriptor = os.open(
            path,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0),
        )
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        DirectMigrationArchiveStore._fsync_directory(path.parent)

    @staticmethod
    def _fsync_directory(path: Path) -> None:
        descriptor = os.open(
            path,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
        )
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
