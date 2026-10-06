from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, replace
import fcntl
from functools import cached_property, lru_cache
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import itertools
import time
from threading import Event, Lock
import weakref
from types import MappingProxyType
from typing import Any, Iterator, Mapping

from . import phase_timings
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
# The directory walk and create probe repeat at most this often. Every
# connection use still compares the file's own identity, owner and mode.
# Owner reads, which use no connection, revalidate at most this often.
_FILE_RECHECK_SECONDS = 1.0
# Writers one owner transaction (group commit) serves at most; bounds how
# long its first writer waits for the shared COMMIT.
_GROUP_COMMIT_MAX = 64
# Schema stamp, metadata row and this connection's data version in one
# statement, so one SQLite snapshot answers all of them. A connection's data
# version moves on every other connection's commit (and WAL truncation), never its own.
_STAMPED_METADATA = (
    "SELECT schema_version, application_id, user_version, journal_mode, "
    "m.activity_revision, m.runtime_compatibility_sha256, m.drain_json, data_version "
    "FROM pragma_schema_version, pragma_application_id, pragma_user_version, "
    "pragma_journal_mode, pragma_data_version JOIN registry_metadata AS m ON m.singleton = 1"
)
# The owner instance holds an exclusive flock on this sidecar for its life;
# the kernel drops it with the process.
_OWNER_SUFFIX = ".owner"
# Idle handles kept for reuse. Request threads borrow concurrently for reads
# the owner's index does not serve; a smaller pool reopens them under load.
_IDLE_CONNECTIONS = 64


class DirectRegistryError(RuntimeError):
    pass


class DirectRegistryConflictError(DirectRegistryError):
    pass


class DirectRegistrationOwnedError(DirectRegistryConflictError):
    """Another incarnation owns this id; nothing was registered."""


class DirectRegistryCapacityUnavailable(DirectRegistryConflictError):
    """No disk claim was granted; the caller may wait for physical capacity."""


class ManagedPrimaryOwnedError(DirectRegistryConflictError):
    """This generation's sole primary belongs to another launch identity."""

    def __init__(self, job_id: str):
        super().__init__("sandbox generation already owns another primary process")
        self.job_id = job_id


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

    # The derived values below are memoized outside the fields: records are
    # immutable and the owner's index keeps them across heartbeats, each of
    # which derives both several times per record.
    @property
    def spec_sha256(self) -> str:
        if (digest := self.__dict__.get("_spec_sha256")) is None:
            digest = self.__dict__["_spec_sha256"] = sandbox_spec_fingerprint(self.spec)
        return digest

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
        if (sandbox := self.__dict__.get("_direct_sandbox")) is not None:
            return sandbox
        if not self.has_direct_sandbox:
            raise DirectRegistryError("registration has no direct sandbox yet")
        sandbox = self.__dict__["_direct_sandbox"] = DirectSandbox(
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
        return sandbox

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
        raw = {name: getattr(self, name) for name in self.__dataclass_fields__}
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


@dataclass(frozen=True)
class _RegistryIndex:
    """The owner's registrations exactly as committed at ``revision``.

    ``rows`` maps a sandbox ID to its stored image ID, stored encoding and
    validated record, and is never mutated once published. ``data_version``
    is the owner connection's; equal values prove no other connection has
    committed since this state was read or written through.
    """

    revision: int
    data_version: int
    rows: Mapping[str, tuple[str, str, DirectSandboxRegistration]]

    @cached_property
    def snapshot(self) -> DirectRegistrySnapshot:
        records = tuple(self.rows[key][2] for key in sorted(self.rows))
        if self.revision < max((record.revision for record in records), default=0):
            raise DirectRegistryError("direct registry activity revision is invalid")
        return DirectRegistrySnapshot(
            records=records,
            by_sandbox_id=MappingProxyType({record.sandbox_id: record for record in records}),
            image_ids=frozenset(record.image_id for record in records if record.image_id),
            activity_revision=self.revision,
        )

    def applied(self, staged: Mapping[str, Any], revision: int) -> _RegistryIndex:
        """``staged`` maps each written ID to its new row, or None if deleted."""
        rows = {**self.rows, **staged}
        return _RegistryIndex(revision, self.data_version,
                              {key: row for key, row in rows.items() if row is not None})


class _GroupCommit:
    """One open owner transaction that queued writers share, and its outcome."""

    def __init__(self, connection: sqlite3.Connection, revision: int) -> None:
        self.connection, self.revision, self.members = connection, revision, 0
        self.done, self.error = Event(), None


@dataclass
class _RegistryConnection:
    connection: sqlite3.Connection
    schema_stamp: tuple[Any, ...] | None = None


def _close_idle_registry_connections(idle, guard):
    with guard:
        entries, idle[:] = list(idle), []
    for entry in entries:
        entry.connection.close()


def _release_owner_lock(held: list[int]) -> None:
    while held:
        os.close(held.pop())


class DirectSandboxRegistry:
    """SQLite-backed ownership bridge from admission through Warden create.

    ``owner=True`` is the node agent's instance. It holds the file's exclusive
    owner lock, so a second live owner in any process is refused, and serves
    registration reads from an in-memory index with SQLite as the journal.
    Other instances, such as readers in other processes, read SQLite.

    ``cached_reads=True`` is a foreign instance (the node agent while
    runtime/noded owns the file): it writes through SQLite's cross-process
    locking and serves reads from an index revalidated like the owner's, by
    activity revision and its own reader connection's data version, at most
    once a second, and before the next read after each of its own writes.
    """

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

    def __init__(self, path: Path, *, hard_disk_capacity_mb: int = 0, owner: bool = False,
                 cached_reads: bool = False) -> None:
        if not path.is_absolute():
            raise ValueError("direct registry path must be absolute")
        self.path = path
        if hard_disk_capacity_mb < 0:
            raise ValueError("disk capacity cannot be negative")
        self.hard_disk_capacity_mb = hard_disk_capacity_mb
        self._connections: list[_RegistryConnection] = []
        self._connections_guard = Lock()
        self._validated_stamp: tuple[Any, ...] | None = None
        # In-process writers queue here rather than in SQLite's busy handler,
        # which polls with sleeps of up to 100 ms and admits in no order.
        # In owner mode it also guards the owner connection and the index.
        self._writer_turn = Lock()
        self._file_identity: tuple[int, int] | None = None
        self._file_checked_at = float("-inf")
        self._connection_pid = os.getpid()
        self._connection_finalizer = weakref.finalize(
            self,
            _close_idle_registry_connections,
            self._connections,
            self._connections_guard,
        )
        self._owner_lock: list[int] = []
        self._owner_entry: _RegistryConnection | None = None
        self._index: _RegistryIndex | None = None
        self._index_checked_at = float("-inf")
        self._claims: tuple[_RegistryIndex, dict[tuple[str, int], int]] | None = None
        # The open owner transaction's changes, applied after its COMMIT.
        self._staged: dict[str, Any] | None = None
        self._bumps = 0
        # The open group commit, and writers queued for the turn to join it.
        self._group: _GroupCommit | None = None
        self._turn_waiters = 0
        self._turn_waiters_guard = Lock()
        # Foreign index (cached_reads): its own reader connection, whose data
        # version is comparable between refreshes, and refreshes in sequence
        # order. An index read at sequence n covers every commit before n; an
        # own write commits before taking _stale_seq, so later reads refresh.
        self._cached_reads = bool(cached_reads) and not owner
        self._reader_entry: _RegistryConnection | None = None
        self._reader_guard = Lock()
        self._sequence = itertools.count(1)
        self._stale_seq = 0
        # (index, sequence its read began at, monotonic check time), replaced whole.
        self._foreign: tuple[_RegistryIndex, int, float] | None = None
        if owner:
            self._own()

    @property
    def is_owner(self) -> bool:
        return bool(self._owner_lock)

    def _own(self) -> None:
        """Take the exclusive owner lock, then build the index from SQLite."""
        self._prepare_file()
        descriptor = os.open(
            self.path.with_name(self.path.name + _OWNER_SUFFIX),
            os.O_RDWR | os.O_CREAT | os.O_CLOEXEC | os.O_NOFOLLOW,
            0o600,
        )
        self._owner_lock.append(descriptor)
        weakref.finalize(self, _release_owner_lock, self._owner_lock)
        try:
            info = os.fstat(descriptor)
            if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
                    or info.st_mode & 0o077):
                raise DirectRegistryError("direct registry owner lock must be private and owned")
            try:
                # Per open file description: a second instance in this process
                # conflicts too. Fork children never use it (see _owned_index).
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise DirectRegistryError("direct registry has another live owner") from None
            self._owned_index()
        except BaseException:
            self.close()
            raise

    def close(self) -> None:
        """Release ownership and idle connections; stragglers then use SQLite."""
        with self._writer_turn:
            if self._group is not None:
                self._abandon_group(DirectRegistryError("direct registry was closed"))
            entry, self._owner_entry = self._owner_entry, None
            self._index = self._claims = None
            _release_owner_lock(self._owner_lock)
        if entry is not None:
            entry.connection.close()
        with self._reader_guard:
            reader, self._reader_entry, self._foreign = self._reader_entry, None, None
        if reader is not None:
            reader.connection.close()
        _close_idle_registry_connections(self._connections, self._connections_guard)

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
        """Every registration's current charge, as heartbeat accounting uses it.

        Every write that changes a charge bumps the activity revision, so the
        owner keeps the charges read at its index's revision until it moves.
        """
        index = self._cached_index()
        if index is not None and self._claims is not None and self._claims[0] is index:
            return dict(self._claims[1])
        with self._transaction(write=False) as connection:
            claims = {
                (row[0], row[1]): max(0, row[2])
                for row in connection.execute(
                    f"SELECT d.sandbox_id, d.sandbox_generation, {_ROW_CLAIM_MB} FROM {_CLAIM_JOIN}"
                )
            }
            if index is not None and self._metadata(connection)[0] == index.revision:
                self._claims = (index, dict(claims))
        return claims

    def update_disk_claim(
        self,
        sandbox_id: str,
        sandbox_generation: int,
        *,
        workspace_mb: int | None = None,
        memory_mb: int | None = None,
        require_capacity: bool = False,
        adopt: bool = False,
    ) -> DiskClaim | None:
        """Move a dynamic claim. Fixed claims are left unchanged (returns None).

        ``adopt`` first converts a split registration's fixed claim (imports,
        upgraded registrations) into the equal dynamic claim: the workspace
        ceiling plus the formula memory claim. The total is unchanged.

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
                adopted = self._adopt_dynamic_claim(connection, sandbox_id) if adopt else None
                if adopted is None:
                    return None
                row = (adopted.workspace_mb, adopted.memory_mb, 1, row[3])
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

    def _adopt_dynamic_claim(self, connection, sandbox_id: str) -> DiskClaim | None:
        record = self._get(connection, sandbox_id)
        if record is None or record.memory_reference is None or record.spec.disk_mb is None:
            return None
        reserved = connection.execute(
            "SELECT reserved_mb FROM registration_disk WHERE sandbox_id=?", (sandbox_id,)
        ).fetchone()[0]
        claim = DiskClaim(record.spec.disk_mb, max(0, reserved - record.spec.disk_mb))
        # A published workspace's release stays released; its released_mb
        # already equals the adopted workspace ceiling.
        connection.execute(
            "UPDATE registration_disk SET reserved_mb=0, workspace_mb=?, memory_mb=?, dynamic=1 "
            "WHERE sandbox_id=?",
            (claim.workspace_mb, claim.memory_mb, sandbox_id),
        )
        return claim

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
        quota: tuple[int, int, Path] | None = None,
    ) -> DirectSandboxRegistration:
        # From ``planned``, ``quota`` (project ID, MiB, path) rides on this
        # commit: its prepare is owner-keyed, so a crash before here replays it.
        # Without it, this advances ``quota_ready`` from an earlier release.
        fields = {} if quota is None else {
            "quota_project_id": quota[0], "quota_total_mb": quota[1], "quota_path": str(quota[2])}
        return self._commit_rootfs(sandbox_id, expected_revision,
                                   "quota_ready" if quota is None else "planned",
                                   image_id, sandbox, **fields)

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
        # Not fsynced: ``owned`` is derivable. The Warden fsyncs its journal
        # after runsc start and before this, and recovery advances a journaled
        # rootfs_ready registration exactly as an owned one, to the same
        # revision. Every later FULL commit, and every checkpoint, syncs the
        # WAL prefix holding this commit, so an OS crash loses at most a suffix
        # of these commits, never a FULL one that depended on ``owned``.
        return self._transition(sandbox_id, expected_revision, "rootfs_ready", "owned",
                                durable=False)

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
            self._delete(connection, sandbox_id)
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
        generation. A different launch cannot replace an ambiguous first launch,
        but may replace a still-queued one: activation commits before dispatch,
        so a queued launch never reached the supervisor. Imported generations
        initially have unknown job identity; their existing lifecycle authority
        still fences wait and continuation observations.
        """
        with self._transaction(write=True) as connection:
            return self._growth_intent(connection, sandbox_id, generation, action=action,
                                       job_id=job_id, launch_sha256=launch_sha256, request_id=request_id)

    def growth_intent_batch(self, operations):
        """(key, action, request_id) transitions as one commit, in order.

        Each item is a result or the exception it raised alone (a SAVEPOINT
        undoes just that item). Node-local waits and resumes arrive tens per
        second per worker; one commit each queued them on the one writer.
        """
        results = []
        with self._transaction(write=True) as connection:
            for (sandbox_id, generation), action, request_id in operations:
                connection.execute("SAVEPOINT growth")
                try:
                    results.append(self._growth_intent(connection, sandbox_id, generation,
                                                       action=action, request_id=request_id))
                except (DirectRegistryError, ValueError) as exc:
                    connection.execute("ROLLBACK TO growth")
                    results.append(exc)
                connection.execute("RELEASE growth")
        return results

    def _growth_intent(self, connection, sandbox_id, generation, *, action, job_id="", launch_sha256="",
                       request_id=""):
        if action not in {"launch", "bind", "activate", "wait", "park", "terminal"}:
            raise ValueError("invalid growth action")
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
                if (intent.job_id, intent.launch_sha256) == (job_id, launch_sha256):
                    return intent
                if action == "bind" or intent.phase != "queued":
                    raise ManagedPrimaryOwnedError(intent.job_id)
                # An admission timeout left this launch queued and the SDK's
                # next start chose a fresh job id. Nothing reached the
                # supervisor; the old caller's activation now fails its fence.
                intent = replace(intent, job_id=job_id, launch_sha256=launch_sha256)
            else:
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
                # A queued launch has no primary to capture. Parking it
                # would let a later wake activate growth never dispatched.
                if intent is None or intent.phase in {"terminal", "queued"}:
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
                elif intent.phase == "queued":
                    # Only this launch's own admission charges it. A wake or
                    # a replaced launch's late admission leaves it queued.
                    if (intent.job_id, intent.launch_sha256) == (job_id, launch_sha256):
                        intent = replace(intent, phase="active", request_id=request_id)
                elif intent.phase == "parked" or (
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

    def _view(self, *, fresh: bool = False) -> _RegistryIndex:
        """The owner's or the foreign index; any other instance reads and
        validates every row. ``fresh`` proves the index against the file now."""
        index = self._cached_index(fresh=fresh)
        if index is not None:
            return index
        with self._transaction(write=False) as connection:
            return self._read_index(connection, self._metadata(connection)[0], 0)

    def get(self, sandbox_id: str, *, fresh: bool = False) -> DirectSandboxRegistration | None:
        """``fresh`` also sees another process's commits from just now."""
        entry = self._view(fresh=fresh).rows.get(sandbox_id)
        return None if entry is None else entry[2]

    def list(self) -> tuple[DirectSandboxRegistration, ...]:
        return self.snapshot().records

    def activity_revision(self) -> int:
        """Read the durable clock without materializing the node inventory."""
        index = self._cached_index()
        if index is not None:
            return index.revision
        with self._transaction(write=False) as connection:
            return self._metadata(connection)[0]

    def snapshot(self) -> DirectRegistrySnapshot:
        """Return records, indexes, roots, and revision from one durable read."""
        return self._view().snapshot

    def references_image(self, image_id: str, *, fresh: bool = False) -> bool:
        return image_id in self._view(fresh=fresh).snapshot.image_ids

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
                raise DirectRegistrationOwnedError("sandbox already has another direct registration")
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
        **quota: Any,
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
            **quota,
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
        durable: bool = True,
        **changes: Any,
    ) -> DirectSandboxRegistration:
        with self._transaction(write=True, durable=durable) as connection:
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
            self._delete(connection, sandbox_id)
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

    def _read_index(self, connection, revision: int, data_version: int,
                    previous: _RegistryIndex | None = None) -> _RegistryIndex:
        """Read every registration, decoding only rows whose stored text changed
        since ``previous`` (by default the owner's index).

        Identical text decodes to an identical frozen record, so the stored
        encoding is the whole cache key.
        """
        cache = previous if previous is not None else self._index
        previous = cache.rows if cache is not None else {}
        rows = {}
        for row in connection.execute(
            "SELECT sandbox_id, image_id, record_json FROM registrations"
        ):
            cached = previous.get(row[0])
            rows[row[0]] = (cached if cached is not None and cached[:2] == row[1:]
                            else (row[1], row[2], self._decode(row)))
        index = _RegistryIndex(revision, data_version, rows)
        index.snapshot  # Validates the clock against every record.
        return index

    def _in_owner_transaction(self, connection: sqlite3.Connection) -> bool:
        # Only the turn holder sets _staged, and only on the owner connection.
        entry = self._owner_entry
        return self._staged is not None and entry is not None and connection is entry.connection

    def _get(
        self,
        connection: sqlite3.Connection,
        sandbox_id: str,
    ) -> DirectSandboxRegistration | None:
        if self._in_owner_transaction(connection):
            # The opening statement proved the index equals this snapshot.
            assert self._index is not None and self._staged is not None
            entry = (self._staged[sandbox_id] if sandbox_id in self._staged
                     else self._index.rows.get(sandbox_id))
            return None if entry is None else entry[2]
        row = connection.execute(
            """
            SELECT sandbox_id, image_id, record_json FROM registrations
            WHERE sandbox_id = ?
            """,
            (sandbox_id,),
        ).fetchone()
        return self._decode(row) if row else None

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
        if self._in_owner_transaction(connection):
            self._staged[record.sandbox_id] = (record.image_id, encoded, record)

    def _delete(self, connection: sqlite3.Connection, sandbox_id: str) -> None:
        connection.execute("DELETE FROM registration_disk WHERE sandbox_id = ?", (sandbox_id,))
        if connection.execute(
            "DELETE FROM registrations WHERE sandbox_id = ?", (sandbox_id,)
        ).rowcount != 1:
            raise DirectRegistryError("direct registration disappeared")
        if self._in_owner_transaction(connection):
            self._staged[sandbox_id] = None

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

    def _bump_activity(self, connection: sqlite3.Connection) -> None:
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
        if self._in_owner_transaction(connection):
            self._bumps += 1

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

    def _connect(self) -> _RegistryConnection:
        """Open a FULL-synchronous connection to the checked file and schema.

        Every use compares the live schema stamp and validates any change
        before a row is read, so a new connection may start from the last
        validated stamp.
        """
        connection = sqlite3.connect(
            self.path, timeout=30.0, isolation_level=None, check_same_thread=False
        )
        try:
            connection.execute("PRAGMA trusted_schema = OFF")
            connection.execute("PRAGMA synchronous = FULL")
            if self._validated_stamp is None:
                self._ensure_schema(connection)
                self._validated_stamp = self._schema_stamp(connection)
        except BaseException:
            connection.close()
            raise
        return _RegistryConnection(connection, self._validated_stamp)

    @contextmanager
    def _readable(self) -> Iterator[None]:
        if os.getpid() != self._connection_pid:
            raise DirectRegistryError("reopen direct registry after fork")
        try:
            yield
        except (OSError, sqlite3.DatabaseError) as exc:
            raise DirectRegistryError("direct registry is unreadable") from exc

    @contextmanager
    def _borrow(self) -> Iterator[_RegistryConnection]:
        """Lend one pooled connection with a validated file and schema."""
        entry: _RegistryConnection | None = None
        reusable = False
        try:
            with self._readable():
                self._check_file()
                with self._connections_guard:
                    if self._connections:
                        entry = self._connections.pop()
                if entry is None:
                    entry = self._connect()
                yield entry
            reusable = True
        except BaseException:
            if entry is not None:
                entry.connection.rollback()
            raise
        finally:
            if entry is not None:
                # This only bounds idle handles, never admitted operations.
                with self._connections_guard:
                    if reusable and len(self._connections) < _IDLE_CONNECTIONS:
                        self._connections.append(entry)
                        entry = None
                if entry is not None:
                    entry.connection.close()

    def _checked_stamp(self, entry: _RegistryConnection) -> tuple[int, int]:
        """Check the open transaction's schema and metadata; return its
        activity revision and the connection's data version."""
        connection = entry.connection
        try:
            row = connection.execute(_STAMPED_METADATA).fetchone()
        except sqlite3.OperationalError:
            row = None  # Changed DDL: full validation reports it.
        if row is None or tuple(row[:4]) != entry.schema_stamp:
            self._validate_schema(connection)
            entry.schema_stamp = self._validated_stamp = self._schema_stamp(connection)
            row = connection.execute(_STAMPED_METADATA).fetchone()
        return self._checked_metadata(row[4:7])[0], row[7]

    @contextmanager
    def _transaction(self, *, write: bool, durable: bool = True) -> Iterator[sqlite3.Connection]:
        """One validated transaction; ``durable=False`` commits without fsync.

        synchronous=NORMAL in WAL mode keeps the commit atomic and loses it only
        to an OS crash, never to process death. Only commit_owned uses it, and
        the owner's writes are always durable: writers queued behind one share
        its COMMIT (group commit). In a 1,024-rollout burst 25-42 writers queued
        for the turn while each held it for its own fsync, about 1.2 s a create.
        Each writer runs under a SAVEPOINT of the shared transaction, so a failure
        undoes only its own changes; it returns once the COMMIT holding them does.
        """
        if not write:
            with self._borrow() as entry, self._validated(entry, write=False):
                yield entry.connection
        elif self._owner_lock:
            with self._readable():
                self._check_file()  # Its syscalls release the GIL: not under the turn.
            with phase_timings.phase("registry_turn"):
                with self._turn_waiters_guard:
                    self._turn_waiters += 1
                try:
                    self._writer_turn.acquire()
                finally:
                    with self._turn_waiters_guard:
                        self._turn_waiters -= 1
            group = failure = None
            try:
                group = self._group or self._begin_group()
                with self._readable():
                    group.connection.execute("SAVEPOINT member")
                staged, bumps = dict(self._staged), self._bumps
                try:
                    yield group.connection
                except BaseException as exc:
                    failure = exc
                    with self._readable():
                        group.connection.execute("ROLLBACK TO member")
                        group.connection.execute("RELEASE member")
                    self._staged, self._bumps = staged, bumps
                else:
                    with self._readable():
                        group.connection.execute("RELEASE member")
                    group.members += 1
                if not self._turn_waiters or group.members >= _GROUP_COMMIT_MAX:
                    with phase_timings.phase("registry_sync"):
                        self._commit_group()
            except BaseException as exc:
                if group is not None and self._group is group:
                    self._abandon_group(exc)
                failure = failure or exc
            finally:
                self._writer_turn.release()
            if failure is not None:
                raise failure
            with phase_timings.phase("registry_sync"):
                group.done.wait()
            if group.error is not None:
                raise DirectRegistryError("direct registry commit failed") from group.error
        else:
            try:
                with self._borrow() as entry, self._writer_turn:
                    with self._validated(entry, write=True, durable=durable):
                        yield entry.connection
            finally:
                # After the COMMIT (or an uncertain one): later reads refresh.
                self._stale_seq = next(self._sequence)

    @contextmanager
    def _validated(
        self, entry: _RegistryConnection, *, write: bool, durable: bool = True
    ) -> Iterator[tuple[int, int]]:
        """A transaction that first checks its schema and metadata stamp."""
        connection = entry.connection
        if not durable:
            connection.execute("PRAGMA synchronous = NORMAL")
        try:
            connection.execute("BEGIN IMMEDIATE" if write else "BEGIN")
            try:
                yield self._checked_stamp(entry)
                with phase_timings.phase("registry_sync"):
                    connection.commit()
            except BaseException:
                # Roll back before the next in-process writer may BEGIN.
                connection.rollback()
                raise
        finally:
            if not durable:
                # A failure here discards the connection, never pools it.
                connection.execute("PRAGMA synchronous = FULL")

    def _begin_group(self) -> _GroupCommit:
        """BEGIN the owner's shared write transaction; the caller holds the turn.

        The opening statement proves that no other connection committed since
        the index was read, or the index is rebuilt from this snapshot first.
        """
        with self._readable():
            if not self._owner_lock:
                raise DirectRegistryError("direct registry ownership was released")
            if self._owner_entry is None:
                self._owner_entry = self._connect()
            entry = self._owner_entry
            connection = entry.connection
            try:
                connection.execute("BEGIN IMMEDIATE")
                revision, data_version = self._checked_stamp(entry)
                index = self._index
                if index is None or (index.revision, index.data_version) != (revision, data_version):
                    self._index = self._read_index(connection, revision, data_version)
            except BaseException:
                self._drop_owner_connection()
                raise
        self._index_checked_at = time.monotonic()
        self._staged, self._bumps = {}, 0
        self._group = _GroupCommit(connection, revision)
        return self._group

    def _commit_group(self) -> None:
        """COMMIT the open group; its changes reach the index after COMMIT returns."""
        group, self._group = self._group, None
        staged, bumps, self._staged = self._staged, self._bumps, None
        try:
            with self._readable():
                group.connection.commit()
        except BaseException as exc:
            # An uncertain COMMIT drops index and connection.
            group.error = exc
            self._drop_owner_connection()
            group.done.set()
            raise
        if staged or bumps:
            self._index = self._index.applied(staged, group.revision + bumps)
        group.done.set()

    def _abandon_group(self, error: BaseException) -> None:
        group, self._group, self._staged = self._group, None, None
        group.error = error
        self._drop_owner_connection()
        group.done.set()

    def _drop_owner_connection(self) -> None:
        entry, self._owner_entry, self._index = self._owner_entry, None, None
        if entry is not None:
            entry.connection.close()  # Closing rolls back any open transaction.

    def _cached_index(self, *, fresh: bool = False) -> _RegistryIndex | None:
        if self._owner_lock:
            return self._owned_index(fresh=fresh)
        return self._foreign_index(fresh=fresh) if self._cached_reads else None

    def _foreign_index(self, *, fresh: bool = False) -> _RegistryIndex:
        """The foreign index, revalidated at most once a second, and before
        any read that follows an own write or asks to be ``fresh``.

        While another reader refreshes, a read that needs no newer state serves
        the current index rather than wait, as the owner's reads do.
        """
        if os.getpid() != self._connection_pid:
            raise DirectRegistryError("reopen direct registry after fork")
        required = next(self._sequence) if fresh else self._stale_seq
        view = self._foreign
        current = view is not None and view[1] > required
        if current and time.monotonic() - view[2] < _FILE_RECHECK_SECONDS:
            return view[0]
        if not self._reader_guard.acquire(blocking=not current):
            return view[0]
        try:
            view = self._foreign
            if view is not None and view[1] > required and (
                    fresh or time.monotonic() - view[2] < _FILE_RECHECK_SECONDS):
                return view[0]  # A refresh that began after ``required``.
            return self._refresh_foreign_index()
        finally:
            self._reader_guard.release()

    def _refresh_foreign_index(self) -> _RegistryIndex:
        """Prove the foreign index against the file; the caller holds the reader guard."""
        started = next(self._sequence)
        with self._readable():
            self._check_file()
            if self._reader_entry is None:
                self._reader_entry = self._connect()
            entry = self._reader_entry
            try:
                with self._validated(entry, write=False) as (revision, data_version):
                    index = self._foreign[0] if self._foreign is not None else None
                    if index is None or (index.revision, index.data_version) != (revision, data_version):
                        index = self._read_index(entry.connection, revision, data_version, previous=index)
            except BaseException:
                self._reader_entry = None
                entry.connection.close()
                raise
        self._foreign = (index, started, time.monotonic())
        return index

    def _owned_index(self, *, fresh: bool = False) -> _RegistryIndex | None:
        """The owner's index, revalidated at most once a second; else None.

        A writer holding the turn validates at its BEGIN: reads then serve the
        current index rather than wait. ``fresh`` waits for the turn instead.
        """
        if not self._owner_lock:
            return None
        if os.getpid() != self._connection_pid:
            raise DirectRegistryError("reopen direct registry after fork")
        index = self._index
        if not fresh and index is not None and time.monotonic() - self._index_checked_at < _FILE_RECHECK_SECONDS:
            return index
        with self._readable():
            self._check_file()
        if not self._writer_turn.acquire(blocking=fresh or index is None):
            return index
        try:
            if not self._owner_lock:
                return None
            if self._group is not None:
                return self._index  # Validated at the group's BEGIN.
            self._refresh_owned_index()
            return self._index
        finally:
            self._writer_turn.release()

    def _refresh_owned_index(self) -> None:
        """Prove the index against the file; the caller holds the turn, no group is open.

        The opening statement proves that no other connection committed since
        the index was read, or the index is rebuilt from this snapshot first.
        """
        with self._readable():
            if not self._owner_lock:
                raise DirectRegistryError("direct registry ownership was released")
            if self._owner_entry is None:
                self._owner_entry = self._connect()
            entry = self._owner_entry
            try:
                with self._validated(entry, write=False) as (revision, data_version):
                    index = self._index
                    if index is None or (index.revision, index.data_version) != (revision, data_version):
                        self._index = self._read_index(entry.connection, revision, data_version)
                    self._index_checked_at = time.monotonic()
            except BaseException:
                if entry.connection.in_transaction:  # A ROLLBACK that failed open.
                    self._drop_owner_connection()
                raise

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
