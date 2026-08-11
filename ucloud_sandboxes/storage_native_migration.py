from __future__ import annotations

from dataclasses import dataclass
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
import stat
import tempfile
from typing import Any

from .direct_network import NETWORK_CIDR
from .direct_registry import DirectSandboxRegistration
from .hibernation import (
    HibernationArtifactFile,
    HibernationArtifactStore,
    HibernationValidationError,
    HibernationManifest,
    HibernationRuntimeFingerprint,
    LocalHibernationArtifactFile,
    _validate_artifact_inventory,
    _validate_digest,
    _validate_digest_or_empty,
    _validate_positive_int,
    _validate_safe_id,
)
from .sandbox import SandboxSpec, sandbox_spec_fingerprint
from .storage_native_registry import StorageSnapshotPublication


STORAGE_NATIVE_RUNTIME_SCHEMA = "storage-native-runtime-v2"
STORAGE_NATIVE_MIGRATION_SCHEMA = "storage-native-v1"
MIGRATION_CONNECTION_POLICY_DISCONNECT = "disconnect"
MIGRATION_CONNECTION_POLICY_NONE = "none"


class StorageNativeMigrationError(RuntimeError):
    pass


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")


@dataclass(frozen=True)
class StorageNativeSandboxManifest:
    spec: SandboxSpec
    sandbox_generation: int
    create_operation_id: str
    hibernation_generation: int
    park_operation_id: str
    captured_ns: int
    runtime: HibernationRuntimeFingerprint
    source_manifest_sha256: str
    source_guest_ip: str | None
    connection_policy: str
    files: tuple[HibernationArtifactFile, ...]
    managed_process_sha256: str = ""
    schema: str = STORAGE_NATIVE_RUNTIME_SCHEMA

    def __post_init__(self) -> None:
        if self.schema != STORAGE_NATIVE_RUNTIME_SCHEMA:
            raise ValueError("unsupported storage-native runtime schema")
        self.spec.validate()
        _validate_positive_int("sandbox generation", self.sandbox_generation)
        _validate_positive_int("hibernation generation", self.hibernation_generation)
        _validate_safe_id("create operation", self.create_operation_id)
        _validate_safe_id("park operation", self.park_operation_id)
        _validate_positive_int("capture timestamp", self.captured_ns)
        _validate_digest("source manifest digest", self.source_manifest_sha256)
        _validate_digest_or_empty("managed-process digest", self.managed_process_sha256)
        expected_connection_policy = (
            MIGRATION_CONNECTION_POLICY_NONE
            if self.spec.network == "none"
            else MIGRATION_CONNECTION_POLICY_DISCONNECT
        )
        if self.connection_policy != expected_connection_policy:
            raise ValueError(
                "direct migration connection policy does not match sandbox networking"
            )
        if self.source_guest_ip is not None and not isinstance(
            self.source_guest_ip, str
        ):
            raise ValueError("source guest IP is invalid")
        if self.spec.network == "none":
            if self.source_guest_ip is not None:
                raise ValueError(
                    "network=none migration cannot carry a source guest IP"
                )
        else:
            if self.source_guest_ip is None:
                raise ValueError("networked migration requires its source guest IP")
            try:
                source_guest_ip = ipaddress.ip_address(self.source_guest_ip)
            except ValueError as exc:
                raise ValueError("source guest IP is invalid") from exc
            if not isinstance(source_guest_ip, ipaddress.IPv4Address):
                raise ValueError("source guest IP must be IPv4")
            if source_guest_ip not in NETWORK_CIDR:
                raise ValueError("source guest IP is outside the direct network")
        if any(not isinstance(item, HibernationArtifactFile) for item in self.files):
            raise ValueError("direct migration artifact metadata is invalid")
        _validate_artifact_inventory("direct migration", self.files)

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
            "managed_process_sha256": self.managed_process_sha256,
            "park_operation_id": self.park_operation_id,
            "connection_policy": self.connection_policy,
            "runtime": self.runtime.to_dict(),
            "sandbox_generation": self.sandbox_generation,
            "schema": self.schema,
            "source_manifest_sha256": self.source_manifest_sha256,
            "source_guest_ip": self.source_guest_ip,
            "spec": self.spec.to_dict(),
            "spec_sha256": self.spec_sha256,
        }

    @classmethod
    def from_dict(cls, raw: object) -> "StorageNativeSandboxManifest":
        required_keys = {
            "captured_ns",
            "create_operation_id",
            "files",
            "hibernation_generation",
            "managed_process_sha256",
            "park_operation_id",
            "connection_policy",
            "runtime",
            "sandbox_generation",
            "schema",
            "source_manifest_sha256",
            "source_guest_ip",
            "spec",
            "spec_sha256",
        }
        if not isinstance(raw, dict) or set(raw) != required_keys:
            raise ValueError("direct migration manifest has an invalid schema")
        files = raw["files"]
        if not isinstance(files, list):
            raise ValueError("direct migration files must be a list")
        manifest = cls(
            spec=SandboxSpec.from_dict(raw["spec"]),
            sandbox_generation=raw["sandbox_generation"],
            create_operation_id=raw["create_operation_id"],
            hibernation_generation=raw["hibernation_generation"],
            park_operation_id=raw["park_operation_id"],
            connection_policy=raw["connection_policy"],
            captured_ns=raw["captured_ns"],
            runtime=HibernationRuntimeFingerprint.from_dict(raw["runtime"]),
            source_manifest_sha256=raw["source_manifest_sha256"],
            source_guest_ip=raw["source_guest_ip"],
            files=tuple(HibernationArtifactFile.from_dict(item) for item in files),
            managed_process_sha256=raw["managed_process_sha256"],
            schema=raw["schema"],
        )
        if raw["spec_sha256"] != manifest.spec_sha256:
            raise ValueError("direct migration spec digest does not match")
        return manifest

    @classmethod
    def from_local(
        cls,
        registration: DirectSandboxRegistration,
        manifest: HibernationManifest,
        *,
        source_guest_ip: str | None,
    ) -> "StorageNativeSandboxManifest":
        manifest.validate_identity(
            sandbox_id=registration.sandbox_id,
            sandbox_generation=registration.sandbox_generation,
            spec_sha256=registration.spec_sha256,
            runtime_sha256=manifest.runtime.digest,
        )
        expected_container_id = hashlib.sha256(
            (f"{registration.sandbox_id}:" f"{registration.sandbox_generation}").encode(
                "utf-8"
            )
        ).hexdigest()
        if (
            registration.phase != "owned"
            or registration.runtime_compatibility_sha256
            != manifest.runtime.node_compatibility_sha256
            or registration.rootfs_sha256 != manifest.runtime.rootfs_sha256
            or registration.container_id != manifest.container_id
            or registration.container_id != expected_container_id
        ):
            raise StorageNativeMigrationError(
                "source registration has another runtime compatibility"
            )
        return cls(
            spec=registration.spec,
            sandbox_generation=registration.sandbox_generation,
            create_operation_id=registration.operation_id,
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
            files=tuple(item.artifact for item in manifest.files),
            managed_process_sha256=manifest.managed_process_sha256,
        )


@dataclass(frozen=True)
class StorageNativeMigration:
    """Portable runtime metadata fenced to one durable block publication."""

    manifest: StorageNativeSandboxManifest
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
            manifest=StorageNativeSandboxManifest.from_dict(raw["manifest"]),
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
                raise StorageNativeMigrationError(
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
                raise StorageNativeMigrationError(
                    "storage-native migration metadata is not a regular file"
                )
            payload = path.read_bytes()
            if len(payload) > 1024 * 1024:
                raise StorageNativeMigrationError(
                    "storage-native migration metadata is too large"
                )
            return StorageNativeMigration.from_dict(json.loads(payload.decode("ascii")))
        except StorageNativeMigrationError:
            raise
        except (
            FileNotFoundError,
            OSError,
            UnicodeDecodeError,
            json.JSONDecodeError,
            TypeError,
            ValueError,
        ) as exc:
            raise StorageNativeMigrationError(
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
        expected_runtime: HibernationRuntimeFingerprint,
        artifact_store: HibernationArtifactStore,
        writable_incarnation: Path,
    ) -> HibernationManifest:
        """Replace source-local file identities after mounting a remote snapshot."""

        portable = migration.manifest
        if portable.runtime != expected_runtime:
            raise HibernationValidationError(
                "storage-native snapshot does not match the required runtime"
            )
        expected_incarnation = (
            f"{portable.sandbox_id}.sandbox-{portable.sandbox_generation}"
        )
        if (
            not writable_incarnation.is_absolute()
            or writable_incarnation.name != expected_incarnation
            or writable_incarnation.parent != artifact_store.root
        ):
            raise StorageNativeMigrationError(
                "storage-native destination is not the sandbox quota incarnation"
            )
        generation = writable_incarnation / (
            f"hibernate-{portable.hibernation_generation}"
        )
        local_files = tuple(
            LocalHibernationArtifactFile.from_path(
                generation / item.name,
                role=item.role,
            )
            for item in portable.files
        )
        for expected, actual in zip(portable.files, local_files):
            if actual.artifact.logical_bytes != expected.logical_bytes:
                raise StorageNativeMigrationError(
                    f"migrated artifact size changed: {actual.artifact.name}"
                )
        local_manifest = HibernationManifest(
            sandbox_id=portable.sandbox_id,
            sandbox_generation=portable.sandbox_generation,
            hibernation_generation=portable.hibernation_generation,
            operation_id=portable.park_operation_id,
            spec_sha256=portable.spec_sha256,
            container_id=hashlib.sha256(
                (f"{portable.sandbox_id}:" f"{portable.sandbox_generation}").encode(
                    "utf-8"
                )
            ).hexdigest(),
            created_ns=portable.captured_ns,
            runtime=portable.runtime,
            files=local_files,
            managed_process_sha256=portable.managed_process_sha256,
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
            raise StorageNativeMigrationError(
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
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_DIRECTORY", 0),
        )
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
