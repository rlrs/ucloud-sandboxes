from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, replace
from functools import lru_cache
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import time
from threading import Lock
import weakref
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
_DIRECT_REGISTRY_SCHEMA_VERSION = 9
_DIRECT_REGISTRY_IDENTITY = (
    _DIRECT_REGISTRY_APPLICATION_ID,
    _DIRECT_REGISTRY_SCHEMA_VERSION,
)
# The directory walk and create probe repeat at most this often. Every use
# still compares the file's own identity, owner and mode.
_FILE_RECHECK_SECONDS = 1.0
# Schema stamp and metadata row in one statement, optionally with the read
# itself, so one SQLite snapshot answers all three.
_STAMPED_SELECT = (
    "SELECT schema_version, application_id, user_version, journal_mode, "
    "m.activity_revision, m.runtime_compatibility_sha256, m.drain_json{columns} "
    "FROM pragma_schema_version, pragma_application_id, pragma_user_version, "
    "pragma_journal_mode JOIN registry_metadata AS m ON m.singleton = 1{joins}"
)
_STAMPED_METADATA = _STAMPED_SELECT.format(columns="", joins="")


class DirectRegistryError(RuntimeError):
    pass


class DirectRegistryConflictError(DirectRegistryError):
    pass


class DirectRegistryCapacityUnavailable(DirectRegistryConflictError):
    """No disk claim was granted; the caller may wait for physical capacity."""


@dataclass(frozen=True)
class DiskClaim:
    """A registration's current physical promise, in MiB.

    ``workspace_mb`` is the workspace grant plus its local sealed layers;
    ``memory_mb`` is the memory allocation's project limit. Specs remain
    maximums; these follow what the sandbox has demonstrated.
    """

    workspace_mb: int
    memory_mb: int

    def __post_init__(self) -> None:
        for value in (self.workspace_mb, self.memory_mb):
            if type(value) is not int or value < 0:
                raise ValueError("disk claim components must be non-negative integers")

    @property
    def total_mb(self) -> int:
        return self.workspace_mb + self.memory_mb


# One registration's current claim. Fixed rows carry reserved_mb (their
# lifetime claim) less any published workspace; dynamic rows carry a
# workspace claim, dropped while published, plus a memory claim.
_ROW_CLAIM_MB = (
    "d.reserved_mb + d.memory_mb + CASE WHEN COALESCE(w.released_mb, 0) > 0 "
    "THEN CASE WHEN d.dynamic = 1 THEN 0 ELSE -w.released_mb END "
    "ELSE d.workspace_mb END"
)
_CLAIM_JOIN = (
    "registration_disk AS d LEFT JOIN workspace_capacity AS w "
    "ON w.sandbox_id = d.sandbox_id AND w.sandbox_generation = d.sandbox_generation"
)


@dataclass(frozen=True)
class ReflinkOverlapClaim:
    sandbox_id: str
    sandbox_generation: int
    hibernation_generation: int
    allocated_bytes: int
    manifest_sha256: str


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
    workspace_directory: str = ""
    memory_allocation_id: str = ""
    migration_id: str = ""
    migration_sha256: str = ""
    version: int = DIRECT_REGISTRATION_VERSION

    def __post_init__(self) -> None:
        if self.version not in {3, 4}:
            raise ValueError("unsupported direct registration version")
        if self.version == 3 and (
            self.workspace_directory or self.memory_allocation_id
        ):
            raise ValueError("legacy registration cannot contain split backing")
        incarnation = f"{self.spec.id}.sandbox-{self.sandbox_generation}"
        if self.version == 4 and (
            self.workspace_directory != f"workspace-{incarnation}"
            or self.memory_allocation_id != incarnation
        ):
            raise ValueError("split registration has invalid component identities")
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
        if self.phase in _ROOTFS_PHASES and (not quota_present or not rootfs_present):
            raise ValueError("direct registration is missing owned resources")

    @property
    def sandbox_id(self) -> str:
        return self.spec.id

    @property
    def spec_sha256(self) -> str:
        return sandbox_spec_fingerprint(self.spec)

    @property
    def workspace_volume_id(self) -> str:
        """Canonical storage identity, including pre-materialization records."""
        return self.workspace_directory or self.memory_directory or (
            f"{self.sandbox_id}.sandbox-{self.sandbox_generation}"
        )

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
            workspace_directory=self.workspace_directory,
            memory=self.memory_reference,
        )

    @property
    def memory_reference(self):
        from .checkpoint_components import MemoryBackingRef

        if not self.memory_allocation_id:
            return None
        return MemoryBackingRef(
            self.memory_allocation_id,
            (self.spec.requested_resources().disk_mb - self.spec.disk_mb) * 1024**2,
        )

    def to_dict(self) -> dict[str, Any]:
        raw = vars(self).copy()
        raw["spec"] = self.spec.to_dict()
        if self.version == 3:
            raw.pop("workspace_directory")
            raw.pop("memory_allocation_id")
        return raw

    @classmethod
    def from_dict(cls, raw: object) -> DirectSandboxRegistration:
        if not isinstance(raw, dict):
            raise DirectRegistryError("direct registration must be an object")
        expected = set(cls.__dataclass_fields__)
        if raw.get("version") == 3:
            expected -= {"workspace_directory", "memory_allocation_id"}
        if (
            raw.get("version") not in {3, 4}
            or set(raw) != expected
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
class ManagedGrowthIntent:
    """Host forecast for the supervisor's sole primary process per incarnation.

    This does not grant execution authority. Active survives an ambiguous launch;
    safe means an authoritative model wait (or completed checkpoint) was observed.
    """

    sandbox_id: str
    generation: int
    job_id: str
    launch_sha256: str
    memory_bytes: int
    phase: str
    request_id: str


@dataclass(frozen=True)
class DirectRegistrySnapshot:
    """One coherent, indexed view of the durable direct registry."""

    records: tuple[DirectSandboxRegistration, ...]
    by_sandbox_id: Mapping[str, DirectSandboxRegistration]
    image_ids: frozenset[str]
    activity_revision: int

    def get(self, sandbox_id: str) -> DirectSandboxRegistration | None:
        return self.by_sandbox_id.get(sandbox_id)


@dataclass
class _RegistryConnection:
    connection: sqlite3.Connection
    schema_stamp: tuple[Any, ...] | None = None


def _close_idle_registry_connections(idle, guard):
    with guard:
        entries, idle[:] = list(idle), []
    for entry in entries:
        entry.connection.close()


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
        CREATE TABLE relay_wake_fences (
            sandbox_id TEXT NOT NULL,
            generation INTEGER NOT NULL CHECK (generation > 0),
            request_id TEXT NOT NULL,
            PRIMARY KEY (sandbox_id, generation, request_id)
        ) STRICT;
        CREATE TABLE managed_growth (
            sandbox_id TEXT PRIMARY KEY,
            generation INTEGER NOT NULL CHECK (generation > 0),
            job_id TEXT NOT NULL,
            launch_sha256 TEXT NOT NULL,
            memory_bytes INTEGER NOT NULL CHECK (memory_bytes > 0),
            phase TEXT NOT NULL CHECK (phase IN ('queued','active','safe','parked','terminal')),
            request_id TEXT NOT NULL
        ) STRICT;
        CREATE TABLE reflink_overlaps (
            sandbox_id TEXT NOT NULL,
            sandbox_generation INTEGER NOT NULL CHECK (sandbox_generation > 0),
            hibernation_generation INTEGER NOT NULL CHECK (hibernation_generation > 0),
            allocated_bytes INTEGER NOT NULL CHECK (allocated_bytes >= 0),
            manifest_sha256 TEXT NOT NULL CHECK (
                length(manifest_sha256) = 64 AND
                manifest_sha256 NOT GLOB '*[^0-9a-f]*'
            ),
            PRIMARY KEY (sandbox_id, sandbox_generation, hibernation_generation)
        ) STRICT;
        CREATE TABLE workspace_capacity (
            sandbox_id TEXT PRIMARY KEY,
            sandbox_generation INTEGER NOT NULL CHECK (sandbox_generation > 0),
            mount_epoch INTEGER NOT NULL CHECK (mount_epoch >= 0),
            released_mb INTEGER NOT NULL CHECK (released_mb >= 0)
        ) STRICT;
        CREATE TABLE registration_disk (
            sandbox_id TEXT PRIMARY KEY,
            sandbox_generation INTEGER NOT NULL CHECK (sandbox_generation > 0),
            reserved_mb INTEGER NOT NULL CHECK (reserved_mb >= 0),
            workspace_mb INTEGER NOT NULL CHECK (workspace_mb >= 0),
            memory_mb INTEGER NOT NULL CHECK (memory_mb >= 0),
            dynamic INTEGER NOT NULL CHECK (dynamic IN (0, 1))
        ) STRICT;
        INSERT INTO registry_metadata VALUES (
            1,
            0,
            NULL,
            '{"admission_open":true,"drain_activity_epoch":0,"draining":false,"token":""}'
        );
    """

    def __init__(self, path: Path, *, hard_disk_capacity_mb: int = 0) -> None:
        if not path.is_absolute():
            raise ValueError("direct registry path must be absolute")
        self.path = path
        if hard_disk_capacity_mb < 0:
            raise ValueError("disk capacity cannot be negative")
        self.hard_disk_capacity_mb = hard_disk_capacity_mb
        self._connections: list[_RegistryConnection] = []
        self._connections_guard = Lock()
        # In-process writers queue here rather than in SQLite's busy handler,
        # which polls with sleeps of up to 100 ms and admits in no order.
        self._writer_turn = Lock()
        # Validated records keyed by their exact stored encoding. Identical
        # text decodes to an identical record, so snapshots decode only rows
        # that changed instead of the whole inventory on every loop.
        self._decoded: dict[str, tuple[str, str, DirectSandboxRegistration]] = {}
        self._file_identity: tuple[int, int] | None = None
        self._file_checked_at = float("-inf")
        self._connection_pid = os.getpid()
        self._connection_finalizer = weakref.finalize(
            self,
            _close_idle_registry_connections,
            self._connections,
            self._connections_guard,
        )

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

    @staticmethod
    def _validate_overlap_identity(sandbox_id, sandbox_generation, hibernation_generation, manifest_sha256):
        if (not isinstance(sandbox_id, str) or not sandbox_id
                or type(sandbox_generation) is not int or sandbox_generation <= 0
                or type(hibernation_generation) is not int or hibernation_generation <= 0
                or not isinstance(manifest_sha256, str) or not _DIGEST.fullmatch(manifest_sha256)):
            raise ValueError("invalid reflink overlap identity")

    @classmethod
    def _reserved_disk_bytes(cls, connection):
        # Plans and both component allocators share this transaction authority.
        # Allocator metrics are observations, never an independent free budget.
        # registration_disk mirrors each registration's claim in the same
        # transaction, so this runs under the writer lock without decoding JSON.
        # A published, unmounted workspace no longer occupies local disk; the
        # storage daemon stopped charging it. Every mount re-reserves it first.
        reserved_mb = connection.execute(
            f"SELECT COALESCE(SUM({_ROW_CLAIM_MB}),0) FROM {_CLAIM_JOIN}"
        ).fetchone()[0]
        overlap = connection.execute(
            "SELECT COALESCE(SUM(allocated_bytes),0) FROM reflink_overlaps"
        ).fetchone()[0]
        return reserved_mb * 1024**2 + overlap

    def workspace_mount_epoch(self, sandbox_id: str, sandbox_generation: int) -> int:
        """Fence for a publication: capture before publishing, release with it."""
        with self._transaction(write=False) as connection:
            row = connection.execute(
                "SELECT sandbox_generation,mount_epoch FROM workspace_capacity WHERE sandbox_id=?",
                (sandbox_id,),
            ).fetchone()
            return row[1] if row is not None and row[0] == sandbox_generation else 0

    def release_published_workspace(self, sandbox_id: str, sandbox_generation: int, *,
                                    workspace_mb: int, expected_mount_epoch: int) -> bool:
        """Stop charging a workspace the storage daemon has published.

        The caller proves PUBLISHED after capturing ``expected_mount_epoch``.
        Any mount since then re-reserved and advanced the epoch; a late
        publisher must not un-charge that live workspace, so it is refused.
        """
        if type(workspace_mb) is not int or workspace_mb <= 0:
            raise ValueError("invalid released workspace size")
        with self._transaction(write=True) as connection:
            owner = self._require(connection, sandbox_id)
            if owner.sandbox_generation != sandbox_generation or owner.phase != "owned":
                raise DirectRegistryConflictError("workspace release lost incarnation ownership")
            row = connection.execute(
                "SELECT sandbox_generation,mount_epoch FROM workspace_capacity WHERE sandbox_id=?",
                (sandbox_id,),
            ).fetchone()
            epoch = row[1] if row is not None and row[0] == sandbox_generation else 0
            if epoch != expected_mount_epoch:
                return False
            claim = connection.execute(
                "SELECT dynamic, workspace_mb FROM registration_disk WHERE sandbox_id=?",
                (sandbox_id,),
            ).fetchone()
            if claim is not None and claim[0]:
                # The remount re-reserves exactly the grant it will mount.
                workspace_mb = max(1, claim[1])
            connection.execute(
                "INSERT OR REPLACE INTO workspace_capacity VALUES (?,?,?,?)",
                (sandbox_id, sandbox_generation, epoch, workspace_mb),
            )
            self._bump_activity(connection)
            return True

    def reserve_workspace_for_mount(self, sandbox_id: str, sandbox_generation: int) -> None:
        """Re-charge a released workspace before any mount; always fence publishers.

        Refusal is retryable: the sandbox stays parked and published, so the
        wake can be placed on another worker.
        """
        with self._transaction(write=True) as connection:
            owner = self._require(connection, sandbox_id)
            if owner.sandbox_generation != sandbox_generation:
                raise DirectRegistryConflictError("workspace mount lost incarnation ownership")
            row = connection.execute(
                "SELECT sandbox_generation,mount_epoch,released_mb FROM workspace_capacity WHERE sandbox_id=?",
                (sandbox_id,),
            ).fetchone()
            epoch, released = (row[1], row[2]) if row is not None and row[0] == sandbox_generation else (0, 0)
            if released and self.hard_disk_capacity_mb and (
                self._reserved_disk_bytes(connection) + released * 1024**2
                > self.hard_disk_capacity_mb * 1024**2
            ):
                raise DirectRegistryCapacityUnavailable(
                    "workspace remount physical disk capacity exhausted"
                )
            connection.execute(
                "INSERT OR REPLACE INTO workspace_capacity VALUES (?,?,?,0)",
                (sandbox_id, sandbox_generation, epoch + 1),
            )
            self._bump_activity(connection)

    def disk_claim(self, sandbox_id: str, sandbox_generation: int) -> DiskClaim | None:
        """The current dynamic claim, or None for a fixed (legacy) claim."""
        with self._transaction(write=False) as connection:
            row = connection.execute(
                "SELECT workspace_mb, memory_mb, dynamic FROM registration_disk "
                "WHERE sandbox_id=? AND sandbox_generation=?",
                (sandbox_id, sandbox_generation),
            ).fetchone()
        return DiskClaim(row[0], row[1]) if row is not None and row[2] else None

    def disk_claims_mb(self) -> dict[tuple[str, int], int]:
        """Every registration's current charge, as heartbeat accounting uses it."""
        with self._transaction(write=False) as connection:
            return {
                (row[0], row[1]): max(0, row[2])
                for row in connection.execute(
                    f"SELECT d.sandbox_id, d.sandbox_generation, {_ROW_CLAIM_MB} FROM {_CLAIM_JOIN}"
                )
            }

    def update_disk_claim(
        self,
        sandbox_id: str,
        sandbox_generation: int,
        *,
        workspace_mb: int | None = None,
        memory_mb: int | None = None,
        require_capacity: bool = False,
    ) -> DiskClaim | None:
        """Move a dynamic claim. Fixed claims are left unchanged (returns None).

        ``require_capacity`` admits an increase that will create physical
        bytes (grant growth, park capture space); refusal is retryable and
        changes nothing. Without it the update records bytes that already
        exist, such as a sealed layer or a committed checkpoint, and always
        succeeds: the node then refuses new work until relief frees space.
        """
        for value in (workspace_mb, memory_mb):
            if value is not None and (type(value) is not int or value < 0):
                raise ValueError("disk claim components must be non-negative integers")
        with self._transaction(write=True) as connection:
            row = connection.execute(
                "SELECT d.workspace_mb, d.memory_mb, d.dynamic, COALESCE(w.released_mb, 0) "
                "FROM registration_disk AS d LEFT JOIN workspace_capacity AS w "
                "ON w.sandbox_id = d.sandbox_id AND w.sandbox_generation = d.sandbox_generation "
                "WHERE d.sandbox_id=? AND d.sandbox_generation=?",
                (sandbox_id, sandbox_generation),
            ).fetchone()
            if row is None:
                raise DirectRegistryConflictError("disk claim lost incarnation ownership")
            if not row[2]:
                return None
            old = DiskClaim(row[0], row[1])
            new = DiskClaim(old.workspace_mb if workspace_mb is None else workspace_mb,
                            old.memory_mb if memory_mb is None else memory_mb)
            if new == old:
                return new
            published = row[3] > 0
            delta_mb = (new.memory_mb - old.memory_mb) + (
                0 if published else new.workspace_mb - old.workspace_mb
            )
            if require_capacity and delta_mb > 0 and (
                not self.hard_disk_capacity_mb
                or self._reserved_disk_bytes(connection) + delta_mb * 1024**2
                > self.hard_disk_capacity_mb * 1024**2
            ):
                raise DirectRegistryCapacityUnavailable("physical disk capacity exhausted")
            connection.execute(
                "UPDATE registration_disk SET workspace_mb=?, memory_mb=? "
                "WHERE sandbox_id=? AND sandbox_generation=?",
                (new.workspace_mb, new.memory_mb, sandbox_id, sandbox_generation),
            )
            if published:
                connection.execute(
                    "UPDATE workspace_capacity SET released_mb=? "
                    "WHERE sandbox_id=? AND sandbox_generation=? AND released_mb > 0",
                    (max(1, new.workspace_mb), sandbox_id, sandbox_generation),
                )
            self._bump_activity(connection)
            return new

    def reserve_reflink_overlap(self, sandbox_id: str, sandbox_generation: int,
                               hibernation_generation: int, allocated_bytes: int, *,
                               manifest_sha256: str) -> None:
        """Persist exact source overlap before the caller raises a project quota.

        The Warden authenticates the source and owns PARKED lifecycle authority.
        This ledger only grants physical capacity, atomically with new creates.
        Ambiguous caller failure retains the claim for exact-owner recovery.
        """
        self._validate_overlap_identity(sandbox_id, sandbox_generation, hibernation_generation, manifest_sha256)
        if type(allocated_bytes) is not int or not 0 <= allocated_bytes <= 2**63 - 1:
            raise ValueError("invalid reflink overlap bytes")
        with self._transaction(write=True) as connection:
            record = self._require(connection, sandbox_id)
            if record.sandbox_generation != sandbox_generation or record.phase != "owned":
                raise DirectRegistryConflictError("reflink overlap lost incarnation ownership")
            identity = (sandbox_id, sandbox_generation, hibernation_generation)
            existing = connection.execute(
                "SELECT allocated_bytes,manifest_sha256 FROM reflink_overlaps "
                "WHERE sandbox_id=? AND sandbox_generation=? AND hibernation_generation=?", identity
            ).fetchone()
            if existing is not None:
                if existing != (allocated_bytes, manifest_sha256):
                    raise DirectRegistryConflictError("reflink overlap source changed")
                return
            if (not self.hard_disk_capacity_mb or self._reserved_disk_bytes(connection) + allocated_bytes
                    > self.hard_disk_capacity_mb * 1024**2):
                raise DirectRegistryCapacityUnavailable("reflink overlap physical disk capacity exhausted")
            connection.execute("INSERT INTO reflink_overlaps VALUES (?,?,?,?,?)",
                               (*identity, allocated_bytes, manifest_sha256))
            self._bump_activity(connection)

    def release_reflink_overlap(self, sandbox_id: str, sandbox_generation: int,
                               hibernation_generation: int, *, manifest_sha256: str) -> None:
        """Release only after source/candidate cleanup and quota reconciliation.

        Lifecycle cleanup owns that proof. The digest fence prevents stale
        retries from releasing a different source, including across restart.
        """
        self._validate_overlap_identity(sandbox_id, sandbox_generation, hibernation_generation, manifest_sha256)
        with self._transaction(write=True) as connection:
            identity = (sandbox_id, sandbox_generation, hibernation_generation)
            row = connection.execute(
                "SELECT manifest_sha256 FROM reflink_overlaps "
                "WHERE sandbox_id=? AND sandbox_generation=? AND hibernation_generation=?", identity
            ).fetchone()
            if row is None:
                return
            if row[0] != manifest_sha256:
                raise DirectRegistryConflictError("reflink overlap release lost source ownership")
            connection.execute(
                "DELETE FROM reflink_overlaps WHERE sandbox_id=? AND sandbox_generation=? AND hibernation_generation=?",
                identity)
            self._bump_activity(connection)

    def list_reflink_overlaps(self, sandbox_id: str | None = None,
                             sandbox_generation: int | None = None) -> tuple[ReflinkOverlapClaim, ...]:
        if (sandbox_id is None) != (sandbox_generation is None):
            raise ValueError("reflink overlap owner is incomplete")
        where = "" if sandbox_id is None else " WHERE sandbox_id=? AND sandbox_generation=?"
        args = () if sandbox_id is None else (sandbox_id, sandbox_generation)
        with self._transaction(write=False) as connection:
            return tuple(ReflinkOverlapClaim(*row) for row in connection.execute(
                "SELECT sandbox_id,sandbox_generation,hibernation_generation,allocated_bytes,manifest_sha256 "
                "FROM reflink_overlaps" + where + " ORDER BY sandbox_id,sandbox_generation,hibernation_generation",
                args))

    def reflink_overlap_bytes(self) -> int:
        with self._transaction(write=False) as connection:
            return connection.execute("SELECT COALESCE(SUM(allocated_bytes),0) FROM reflink_overlaps").fetchone()[0]

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
        split_memory_backing: bool = False,
        initial_claim: DiskClaim | None = None,
    ) -> DirectSandboxRegistration:
        if sandbox_generation <= 0:
            raise ValueError("sandbox generation must be positive")
        if initial_claim is not None and not split_memory_backing:
            raise ValueError("dynamic disk claims require split memory backing")
        now = time.time_ns()
        return self._plan(
            DirectSandboxRegistration(
                spec=spec,
                sandbox_generation=sandbox_generation,
                operation_id=operation_id,
                runtime_compatibility_sha256=runtime_compatibility_sha256,
                phase="planned",
                version=4 if split_memory_backing else 3,
                workspace_directory=f"workspace-{spec.id}.sandbox-{sandbox_generation}"
                if split_memory_backing
                else "",
                memory_allocation_id=f"{spec.id}.sandbox-{sandbox_generation}"
                if split_memory_backing
                else "",
                revision=1,
                created_ns=now,
                updated_ns=now,
            ),
            imported=False,
            initial_claim=initial_claim,
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
        split_memory_backing: bool = False,
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
                version=4 if split_memory_backing else 3,
                workspace_directory=f"workspace-{spec.id}.sandbox-{sandbox_generation}"
                if split_memory_backing
                else "",
                memory_allocation_id=f"{spec.id}.sandbox-{sandbox_generation}"
                if split_memory_backing
                else "",
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
        expected_generation: int | None = None,
    ) -> DirectSandboxRegistration:
        return self._transition(
            sandbox_id,
            expected_revision,
            {"planned", "quota_ready", "rootfs_ready", "owned"},
            "deleting",
            expected_generation=expected_generation,
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
            if connection.execute(
                "SELECT 1 FROM reflink_overlaps WHERE sandbox_id=? AND sandbox_generation=? LIMIT 1",
                (sandbox_id, sandbox_generation),
            ).fetchone() is not None:
                raise DirectRegistryConflictError("deletion retains unreconciled reflink overlap")
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
            connection.execute("DELETE FROM workspace_capacity WHERE sandbox_id=?", (sandbox_id,))
            connection.execute("DELETE FROM managed_growth WHERE sandbox_id=? AND generation=?",
                               (sandbox_id, sandbox_generation))
            connection.execute(
                "DELETE FROM relay_wake_fences WHERE sandbox_id=? AND generation=?",
                (sandbox_id, sandbox_generation),
            )
            connection.execute(
                "DELETE FROM registration_disk WHERE sandbox_id = ?", (sandbox_id,)
            )
            if (
                connection.execute(
                    "DELETE FROM registrations WHERE sandbox_id = ?",
                    (sandbox_id,),
                ).rowcount
                != 1
            ):
                raise DirectRegistryError("direct registration disappeared")
            self._bump_activity(connection)

    def relay_wake_fence(
        self, sandbox_id: str, generation: int, request_id: str, *, record: bool = False
    ) -> bool:
        """A committed wake intent permanently supersedes this request's park.

        Caller holds the runtime lifecycle lock. Persist before attempting wake:
        an uncertain/overloaded restore must not allow an older park to win later.
        """
        if generation < 1 or not OPERATION_ID_RE.fullmatch(request_id):
            raise ValueError("invalid relay lifecycle identity")
        # Fences are insert-only for a generation, so an existing fence is
        # already durable. A warm wake usually committed it with its growth
        # admission; do not queue behind create commits for the writer lock.
        present = self._relay_wake_fence(sandbox_id, generation, request_id, record=False)
        if present or not record:
            return present
        return self._relay_wake_fence(sandbox_id, generation, request_id, record=True)

    def _relay_wake_fence(self, sandbox_id, generation, request_id, *, record):
        with self._transaction(write=record) as connection:
            owner = self._require(connection, sandbox_id)
            if owner.sandbox_generation != generation:
                raise DirectRegistryConflictError("relay lifecycle generation changed")
            if record:
                connection.execute(
                    "INSERT OR IGNORE INTO relay_wake_fences VALUES (?,?,?)",
                    (sandbox_id, generation, request_id),
                )
                return True
            return (
                connection.execute(
                    "SELECT 1 FROM relay_wake_fences WHERE sandbox_id=? AND generation=? AND request_id=?",
                    (sandbox_id, generation, request_id),
                ).fetchone()
                is not None
            )

    def growth_intents(self) -> tuple[ManagedGrowthIntent, ...]:
        with self._transaction(write=False) as connection:
            return tuple(ManagedGrowthIntent(*row) for row in connection.execute(
                "SELECT * FROM managed_growth ORDER BY sandbox_id"))

    def growth_intent(self, sandbox_id, generation, *, action, job_id="", launch_sha256="", request_id=""):
        """Mutate a forecast under the same durable incarnation/wake fences.

        The guest supervisor accepts only one primary job/spec for its entire
        generation. A different launch cannot replace an ambiguous first launch.
        Imported generations initially have unknown job identity; their existing
        lifecycle authority still fences wait and continuation observations.
        """
        if action not in {"launch", "bind", "activate", "wait", "park", "terminal"}:
            raise ValueError("invalid growth action")
        with self._transaction(write=True) as connection:
            owner = self._require(connection, sandbox_id)
            if owner.sandbox_generation != generation or owner.phase != "owned":
                raise DirectRegistryConflictError("growth intent lost incarnation ownership")
            row = connection.execute("SELECT * FROM managed_growth WHERE sandbox_id=?", (sandbox_id,)).fetchone()
            intent = ManagedGrowthIntent(*row) if row else None
            if intent is not None and intent.generation != generation:
                raise DirectRegistryConflictError("growth intent has stale generation")
            if action in {"launch", "bind"}:
                if not job_id or not _DIGEST.fullmatch(launch_sha256):
                    raise ValueError("invalid managed launch identity")
                if intent is not None:
                    if not intent.job_id:
                        if action == "launch":
                            return intent  # Imported primary: only supervisor can bind it.
                        intent = replace(intent, job_id=job_id, launch_sha256=launch_sha256)
                        connection.execute("INSERT OR REPLACE INTO managed_growth VALUES (?,?,?,?,?,?,?)", tuple(vars(intent).values()))
                        return intent
                    if (intent.job_id, intent.launch_sha256) != (job_id, launch_sha256):
                        raise DirectRegistryConflictError("sandbox generation already owns another primary process")
                    return intent
                intent = ManagedGrowthIntent(sandbox_id, generation, job_id, launch_sha256,
                    int(owner.spec.memory_mb * 1024**2), "queued", "")
            else:
                if action == "terminal":
                    if intent is None or not job_id or (intent.job_id and intent.job_id != job_id):
                        return intent
                    # An imported primary has no local launch identity yet.
                    # The supervisor's authoritative terminal response still
                    # proves this generation's sole primary cannot grow again.
                    intent = replace(intent, phase="terminal")
                elif action == "park":
                    if intent is None or intent.phase == "terminal":
                        return intent
                    intent = replace(intent, phase="parked")
                elif action == "wait":
                    if not request_id or connection.execute(
                        "SELECT 1 FROM relay_wake_fences WHERE sandbox_id=? AND generation=? AND request_id=?",
                        (sandbox_id, generation, request_id)).fetchone():
                        raise DirectRegistryConflictError("growth wait was superseded by wake")
                    if intent is None:
                        intent = ManagedGrowthIntent(sandbox_id, generation, "", "",
                            int(owner.spec.memory_mb * 1024**2), "safe", request_id)
                    elif intent.phase in {"active", "safe", "parked"}:
                        intent = replace(intent, phase="parked" if intent.phase == "parked" else "safe", request_id=request_id)
                elif action == "activate":
                    if intent is None:
                        intent = ManagedGrowthIntent(sandbox_id, generation, "", "",
                            int(owner.spec.memory_mb * 1024**2), "active", request_id)
                    elif intent.phase in {"queued", "parked"} or (
                        intent.phase == "safe" and (not intent.request_id or intent.request_id == request_id)):
                        intent = replace(intent, phase="active", request_id=request_id)
            if action == "activate" and request_id:
                # Admission and revocation of this safe wait are one commit.
                # Before this commit a queued continuation remains reclaimable;
                # after it an old park cannot erase the admitted growth claim.
                connection.execute("INSERT OR IGNORE INTO relay_wake_fences VALUES (?,?,?)",
                                   (sandbox_id, generation, request_id))
            encoded = tuple(vars(intent).values())
            if row != encoded:
                connection.execute("INSERT OR REPLACE INTO managed_growth VALUES (?,?,?,?,?,?,?)", encoded)
            return intent

    def get(self, sandbox_id: str) -> DirectSandboxRegistration | None:
        read = self._stamped_read(
            ", r.sandbox_id, r.image_id, r.record_json",
            " LEFT JOIN registrations AS r ON r.sandbox_id = ?",
            (sandbox_id,),
        )
        if read is None:
            with self._transaction(write=False) as connection:
                return self._get(connection, sandbox_id)
        row = read[1]
        return self._decode_cached(row) if row[0] is not None else None

    def list(self) -> tuple[DirectSandboxRegistration, ...]:
        return self.snapshot().records

    def activity_revision(self) -> int:
        """Read the durable clock without materializing the node inventory.

        Lifecycle responses need only this clock. Registration reads and full
        heartbeat snapshots still validate their records independently.
        """

        read = self._stamped_read()
        if read is None:
            with self._transaction(write=False) as connection:
                return self._metadata(connection)[0]
        return read[0][0]

    def snapshot(self) -> DirectRegistrySnapshot:
        """Return records, indexes, roots, and revision from one durable read."""

        with self._transaction(write=False) as connection:
            activity_revision = self._metadata(connection)[0]
            rows = connection.execute(
                """
                SELECT sandbox_id, image_id, record_json
                FROM registrations ORDER BY sandbox_id
                """
            ).fetchall()
        previous = self._decoded
        decoded: dict[str, tuple[str, str, DirectSandboxRegistration]] = {}
        for row in rows:
            record = self._decode_cached(row, previous)
            decoded[record.sandbox_id] = (row[1], row[2], record)
        # Rebuilding from the live inventory drops deleted owners' entries.
        self._decoded = decoded
        records = tuple(entry[2] for entry in decoded.values())
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
                self._decode_cached(row)
            return row is not None

    def _plan(
        self,
        candidate: DirectSandboxRegistration,
        *,
        imported: bool,
        initial_claim: DiskClaim | None = None,
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
            claim_mb = (
                initial_claim.total_mb
                if initial_claim is not None
                else candidate.spec.requested_resources().disk_mb
            )
            if self.hard_disk_capacity_mb:
                if (
                    self._reserved_disk_bytes(connection)
                    + claim_mb * 1024**2
                    > self.hard_disk_capacity_mb * 1024**2
                ):
                    raise DirectRegistryCapacityUnavailable(
                        "combined workspace and memory backing capacity exhausted"
                    )
            self._write(connection, candidate, insert=True)
            if initial_claim is not None:
                connection.execute(
                    "UPDATE registration_disk SET reserved_mb=0, workspace_mb=?, memory_mb=?, "
                    "dynamic=1 WHERE sandbox_id=?",
                    (initial_claim.workspace_mb, initial_claim.memory_mb, candidate.sandbox_id),
                )
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
        expected_generation: int | None = None,
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
                or (
                    expected_generation is not None
                    and record.sandbox_generation != expected_generation
                )
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
            connection.execute(
                "DELETE FROM registration_disk WHERE sandbox_id = ?", (sandbox_id,)
            )
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

    def _decode_cached(self, row: object, cache=None) -> DirectSandboxRegistration:
        """Decode a row, reusing the validated record for identical stored text.

        Identical text decodes to an identical frozen record, so the stored
        encoding is the whole cache key; external writers only cause misses.
        """
        cache = self._decoded if cache is None else cache
        cached = cache.get(row[0]) if isinstance(row, tuple) and len(row) == 3 else None
        if cached is not None and cached[:2] == row[1:]:
            return cached[2]
        record = self._decode(row)
        cache[record.sandbox_id] = (row[1], row[2], record)
        return record

    def _get(
        self,
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
        return self._decode_cached(row) if row else None

    def _require(
        self,
        connection: sqlite3.Connection,
        sandbox_id: str,
    ) -> DirectSandboxRegistration:
        record = self._get(connection, sandbox_id)
        if record is None:
            raise DirectRegistryConflictError("direct registration is absent")
        return record

    def _write(
        self,
        connection: sqlite3.Connection,
        record: DirectSandboxRegistration,
        *,
        insert: bool = False,
    ) -> None:
        encoded = self._encode(record)
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
        self._write_disk_claim(connection, record)

    @staticmethod
    def _write_disk_claim(
        connection: sqlite3.Connection, record: DirectSandboxRegistration
    ) -> None:
        reserved_mb = (
            record.quota_total_mb
            if record.quota_total_mb is not None
            else record.spec.requested_resources().disk_mb
        )
        # Dynamic claims are maintained explicitly by update_disk_claim.
        connection.execute(
            "INSERT INTO registration_disk VALUES (?, ?, ?, 0, 0, 0) "
            "ON CONFLICT (sandbox_id) DO UPDATE SET "
            "sandbox_generation=excluded.sandbox_generation, reserved_mb=excluded.reserved_mb "
            "WHERE registration_disk.dynamic = 0",
            (record.sandbox_id, record.sandbox_generation, reserved_mb),
        )

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
        return cls._checked_metadata(
            connection.execute(
                "SELECT activity_revision, runtime_compatibility_sha256, drain_json "
                "FROM registry_metadata WHERE singleton = 1"
            ).fetchone()
        )

    @classmethod
    def _checked_metadata(
        cls,
        row: tuple[Any, ...] | None,
    ) -> tuple[int, str | None, NodeDrainState]:
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
    @lru_cache(maxsize=8)
    def _decode_drain(encoded: str) -> NodeDrainState:
        # Every transaction checks this row; it changes only on drain moves.
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

    def _check_file(self) -> None:
        """Validate the registry file before lending a connection.

        Replacement, owner and mode changes of the file itself are caught on
        every use. The directory walk and create probe cost several syscalls
        and two raised exceptions, so they repeat at most once a second.
        """
        now = time.monotonic()
        if self._file_identity is not None and now - self._file_checked_at < _FILE_RECHECK_SECONDS:
            try:
                info = os.lstat(self.path)
            except OSError:
                info = None
            if (
                info is not None
                and stat.S_ISREG(info.st_mode)
                and info.st_uid == os.geteuid()
                and not info.st_mode & 0o077
                and (info.st_dev, info.st_ino) == self._file_identity
            ):
                return
        self._prepare_file()
        info = self.path.lstat()
        identity = (info.st_dev, info.st_ino)
        with self._connections_guard:
            if self._file_identity is not None and self._file_identity != identity:
                raise DirectRegistryError(
                    "direct registry file was replaced; reopen it"
                )
            self._file_identity = identity
        self._file_checked_at = now

    @contextmanager
    def _borrow(self) -> Iterator[_RegistryConnection]:
        """Lend one pooled connection with a validated file and schema."""
        entry: _RegistryConnection | None = None
        reusable = False
        try:
            if os.getpid() != self._connection_pid:
                raise DirectRegistryError("reopen direct registry after fork")
            self._check_file()
            with self._connections_guard:
                if self._connections:
                    entry = self._connections.pop()
            if entry is None:
                entry = _RegistryConnection(
                    sqlite3.connect(
                        self.path,
                        timeout=30.0,
                        isolation_level=None,
                        check_same_thread=False,
                    )
                )
                entry.connection.execute("PRAGMA trusted_schema = OFF")
                entry.connection.execute("PRAGMA synchronous = FULL")
            # A retained connection caches SQLite's parsed schema/statements.
            # Its schema cookie and durable identity are checked on every use;
            # changed DDL goes through full validation before any row access.
            if entry.schema_stamp is None:
                self._ensure_schema(entry.connection)
                entry.schema_stamp = self._schema_stamp(entry.connection)
            yield entry
            reusable = True
        except BaseException as exc:
            if entry is not None:
                entry.connection.rollback()
            if isinstance(exc, DirectRegistryError):
                raise
            if isinstance(exc, (OSError, sqlite3.DatabaseError)):
                raise DirectRegistryError("direct registry is unreadable") from exc
            raise
        finally:
            if entry is not None:
                # This only bounds idle handles, never admitted operations.
                with self._connections_guard:
                    if reusable and len(self._connections) < 16:
                        self._connections.append(entry)
                        entry = None
                if entry is not None:
                    entry.connection.close()

    def _stamped_read(
        self,
        columns: str = "",
        joins: str = "",
        parameters: tuple[Any, ...] = (),
    ) -> tuple[tuple[int, str | None, NodeDrainState], tuple[Any, ...]] | None:
        """Answer a read with one autocommit statement, or None to fall back.

        The statement carries the schema stamp and metadata row, so the checks
        and the read see one snapshot. A changed stamp or an unreadable
        metadata row returns None; the caller's validating transaction then
        reports it. Returns the checked metadata and the extra columns.
        """
        with self._borrow() as entry:
            try:
                rows = entry.connection.execute(
                    _STAMPED_SELECT.format(columns=columns, joins=joins), parameters
                ).fetchall()
            except sqlite3.OperationalError:
                return None
            stamp = entry.schema_stamp
        if len(rows) != 1 or tuple(rows[0][:4]) != stamp:
            return None
        return self._checked_metadata(rows[0][4:7]), tuple(rows[0][7:])

    @contextmanager
    def _transaction(
        self,
        *,
        write: bool,
    ) -> Iterator[sqlite3.Connection]:
        with self._borrow() as entry:
            connection = entry.connection
            writing = False
            try:
                if write:
                    self._writer_turn.acquire()
                    writing = True
                connection.execute("BEGIN IMMEDIATE" if write else "BEGIN")
                try:
                    row = connection.execute(_STAMPED_METADATA).fetchone()
                except sqlite3.OperationalError:
                    row = None  # Changed DDL: full validation reports it.
                if row is None or tuple(row[:4]) != entry.schema_stamp:
                    self._validate_schema(connection)
                    entry.schema_stamp = self._schema_stamp(connection)
                else:
                    self._checked_metadata(row[4:7])
                yield connection
                connection.commit()
            except BaseException:
                # Roll back before the next in-process writer may BEGIN.
                connection.rollback()
                raise
            finally:
                if writing:
                    self._writer_turn.release()

    @staticmethod
    def _schema_stamp(connection: sqlite3.Connection) -> tuple[Any, ...]:
        return tuple(
            connection.execute(
                "SELECT schema_version, application_id, user_version, journal_mode "
                "FROM pragma_schema_version, pragma_application_id, "
                "pragma_user_version, pragma_journal_mode"
            ).fetchone()
        )

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
                "SELECT 1 FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' LIMIT 1"
            ).fetchone()
            version = cls._versions(connection)
            if version in {(_DIRECT_REGISTRY_APPLICATION_ID, old) for old in (3, 4, 5, 6, 7, 8)}:
                cls._validate_schema(connection, legacy_version=version[1])
                if version[1] == 8:
                    # Rebuilt below with dynamic-claim columns. Existing
                    # registrations keep their fixed lifetime claim.
                    connection.execute("DROP TABLE registration_disk")
                missing = {"registration_disk"}
                if version[1] < 7:
                    missing.add("workspace_capacity")
                if version[1] < 6:
                    missing.add("reflink_overlaps")
                if version[1] < 5:
                    missing.add("managed_growth")
                if version[1] == 3:
                    missing.add("relay_wake_fences")
                for raw in cls._SCHEMA.split(";"):
                    statement = raw.strip()
                    if statement.startswith("CREATE TABLE ") and statement.split()[2] in missing:
                        connection.execute(statement)
                for row in connection.execute(
                    "SELECT sandbox_id, image_id, record_json FROM registrations"
                ).fetchall():
                    cls._write_disk_claim(connection, cls._decode(row))
                connection.execute(f"PRAGMA user_version = {_DIRECT_REGISTRY_SCHEMA_VERSION}")
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
    def _validate_schema(
        cls, connection: sqlite3.Connection, *, legacy_version: int | None = None
    ) -> None:
        expected = {
            statement.split()[2]: statement
            for raw in cls._SCHEMA.split(";")
            if (statement := raw.strip()).startswith("CREATE ")
        }
        if legacy_version == 8:
            expected["registration_disk"] = (
                "CREATE TABLE registration_disk (\n"
                "            sandbox_id TEXT PRIMARY KEY,\n"
                "            sandbox_generation INTEGER NOT NULL CHECK (sandbox_generation > 0),\n"
                "            reserved_mb INTEGER NOT NULL CHECK (reserved_mb >= 0)\n"
                "        ) STRICT"
            )
        elif legacy_version is not None:
            expected.pop("registration_disk")
        if legacy_version is not None and legacy_version < 7:
            expected.pop("workspace_capacity")
            if legacy_version < 6:
                expected.pop("reflink_overlaps")
            if legacy_version < 5:
                expected.pop("managed_growth")
            if legacy_version == 3:
                expected.pop("relay_wake_fences")
        actual = dict(
            connection.execute(
                "SELECT name, sql FROM sqlite_schema "
                "WHERE type IN ('table', 'index', 'view', 'trigger') AND name NOT LIKE 'sqlite_%'"
            )
        )
        if (
            cls._versions(connection)
            != (
                (_DIRECT_REGISTRY_APPLICATION_ID, legacy_version)
                if legacy_version is not None
                else _DIRECT_REGISTRY_IDENTITY
            )
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
