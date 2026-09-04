from __future__ import annotations

from collections import defaultdict, OrderedDict
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from enum import Enum
import json
import os
from pathlib import Path
import sqlite3
from threading import RLock
from typing import Any, Iterable, Iterator
from uuid import uuid4

from .capabilities import STORAGE_NATIVE_CAPABILITY
from .models import (
    ResourceQuantity,
    SandboxDemand,
    SandboxInventoryEntry,
    SandboxPlacementRequest,
    parse_iso_datetime,
    utc_now,
)
from .managed_process import ManagedProcessRecord
from .sandbox import OPERATION_ID_RE
from .storage_native_registry import StorageSnapshotPublication


_ROUTE_LOCKS_GUARD = RLock()
_ROUTE_LOCKS: defaultdict[Path, RLock] = defaultdict(RLock)
_EXEC_ROUTE_CACHES: defaultdict[Path, OrderedDict[str, ExecRoute]] = defaultdict(
    OrderedDict
)
_EXEC_ROUTE_CACHE_SANDBOX_INDEXES: defaultdict[Path, dict[str, set[str]]] = defaultdict(
    dict
)
PENDING_DEMAND_TTL_SECONDS = 300
MAX_PREPARED_CAPACITY_COUNT = 100
EXEC_ROUTE_CACHE_MAX_ENTRIES = 65_536
PROGRAM_TERMINAL_RETENTION_SECONDS = 7 * 24 * 60 * 60
ROUTING_SCHEMA_VERSION = 3
SANDBOX_WORKER_STATES = ("attached", "detaching", "detached")
PROGRAM_REQUEST_STATES = (
    "model_wait",
    "ready_to_wake",
    "waking",
    "acting",
    "terminal",
)
_PROGRAM_STATE_RANK = {
    state: index for index, state in enumerate(PROGRAM_REQUEST_STATES)
}


class SandboxRouteConflictError(RuntimeError):
    pass


def _validate_sandbox_route_identity(route: "SandboxRoute") -> None:
    if (
        not route.sandbox_id
        or not route.node_id
        or not route.job_id
        or not route.node_url
    ):
        raise ValueError("sandbox route requires exact sandbox and node identity")
    if route.spec.get("id") != route.sandbox_id:
        raise ValueError("sandbox route spec must contain its exact id")
    if not route.state.strip():
        raise ValueError("sandbox route state is required")
    if not route.resources.is_valid:
        raise ValueError("sandbox route resources are invalid")
    if route.generation < 1:
        raise ValueError("sandbox route generation must be positive")
    if not OPERATION_ID_RE.fullmatch(route.create_operation_id):
        raise ValueError("sandbox route create_operation_id is invalid")
    if route.delete_operation_id and not OPERATION_ID_RE.fullmatch(
        route.delete_operation_id
    ):
        raise ValueError("sandbox route delete_operation_id is invalid")
    if len(route.spec_hash) != 64 or any(
        character not in "0123456789abcdef" for character in route.spec_hash
    ):
        raise ValueError("sandbox route spec_hash must be a lowercase SHA-256 digest")
    if route.activity_epoch < 0:
        raise ValueError("sandbox route activity_epoch must be non-negative")
    if route.worker_state not in SANDBOX_WORKER_STATES:
        raise ValueError(
            "sandbox route worker_state must be one of: "
            + ", ".join(SANDBOX_WORKER_STATES)
        )
    if route.worker_state != "attached" and not is_portable_parked_route(route):
        raise ValueError("a detaching or detached route must be a fully published park")


@dataclass(frozen=True)
class SandboxRoute:
    sandbox_id: str
    node_id: str
    job_id: str
    node_url: str
    resources: ResourceQuantity
    spec: dict[str, Any]
    state: str
    generation: int
    create_operation_id: str
    spec_hash: str
    delete_operation_id: str = ""
    node_epoch: str = ""
    activity_epoch: int = 0
    worker_state: str = "attached"
    storage_schema: str = ""
    snapshot_manifest_digest: str = ""
    snapshot_repository: str = ""
    snapshot_tag: str = ""
    storage_snapshot: dict[str, Any] = field(default_factory=dict)
    created_at: str = ""
    updated_at: str = ""

    def __post_init__(self) -> None:
        _validate_sandbox_route_identity(self)

    def to_dict(self) -> dict[str, Any]:
        return {
            "sandbox_id": self.sandbox_id,
            "node_id": self.node_id,
            "job_id": self.job_id,
            "node_url": self.node_url,
            "resources": self.resources.to_dict(),
            "spec": dict(self.spec),
            "state": self.state,
            "generation": self.generation,
            "create_operation_id": self.create_operation_id,
            "spec_hash": self.spec_hash,
            "delete_operation_id": self.delete_operation_id,
            "node_epoch": self.node_epoch,
            "activity_epoch": self.activity_epoch,
            "worker_state": self.worker_state,
            "snapshot_manifest_digest": self.snapshot_manifest_digest,
            "snapshot_repository": self.snapshot_repository,
            "snapshot_tag": self.snapshot_tag,
            "storage_snapshot": dict(self.storage_snapshot),
            "storage_schema": self.storage_schema,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


@dataclass(frozen=True)
class SandboxRouteAllocation:
    sandbox_id: str
    node_id: str
    job_id: str
    node_url: str
    resources: ResourceQuantity
    spec: dict[str, Any]
    node_epoch: str = ""
    activity_epoch: int = 0

    def __post_init__(self) -> None:
        if self.spec.get("id") != self.sandbox_id:
            raise ValueError("sandbox allocation spec must contain its exact id")
        if not self.node_id or not self.job_id or not self.node_url:
            raise ValueError("sandbox allocation requires exact node identity")
        if not self.resources.is_valid:
            raise ValueError("sandbox allocation resources are invalid")


def is_portable_parked_route(route: SandboxRoute) -> bool:
    """Return whether a route is recoverable without its former node.

    A storage-native park is portable only after both its opaque descriptor and
    content-addressed Registry publication have reached the gateway. Merely
    observing the ``parked`` state is insufficient because publication may
    still be in progress.
    """

    return bool(
        (route.state or "unknown").lower() == "parked"
        and route.storage_schema == STORAGE_NATIVE_CAPABILITY
        and route.storage_snapshot
        and route.snapshot_manifest_digest
        and route.snapshot_repository
        and route.snapshot_tag
    )


class SandboxOwnerLossDisposition(str, Enum):
    RECOVER_DETACHED = "recover_detached"
    TERMINAL_REPLACE = "terminal_replace"
    TERMINAL_DELETE = "terminal_delete"


def sandbox_owner_loss_disposition(
    route: SandboxRoute,
) -> SandboxOwnerLossDisposition:
    """Classify every route exactly once when its worker owner is lost."""

    if route.delete_operation_id:
        return SandboxOwnerLossDisposition.TERMINAL_DELETE
    if is_portable_parked_route(route):
        return SandboxOwnerLossDisposition.RECOVER_DETACHED
    return SandboxOwnerLossDisposition.TERMINAL_REPLACE


def route_with_inventory_snapshot(
    route: SandboxRoute,
    item: SandboxInventoryEntry,
) -> SandboxRoute:
    """Validate and attach a worker's complete portable snapshot descriptor."""

    if (
        item.sandbox_id != route.sandbox_id
        or item.generation != route.generation
        or item.operation_id != route.create_operation_id
        or item.spec_hash != route.spec_hash
    ):
        raise ValueError("inventory snapshot does not own its sandbox route")
    if item.state.strip().lower() != "parked":
        raise ValueError("only a parked inventory entry can publish a snapshot")
    if item.storage_schema != STORAGE_NATIVE_CAPABILITY:
        raise ValueError("inventory snapshot has an unknown storage schema")

    # Keep the heavy descriptor parser out of routing module import startup and
    # avoid a module cycle through the sandbox/storage lifecycle modules.
    from .storage_native_migration import StorageNativeMigration

    snapshot = StorageNativeMigration.from_dict(item.storage_snapshot)
    if (
        snapshot.manifest.sandbox_id != route.sandbox_id
        or snapshot.manifest.sandbox_generation != route.generation
        or snapshot.manifest.create_operation_id != route.create_operation_id
        or snapshot.manifest.spec_sha256 != route.spec_hash
        or snapshot.publication.manifest_digest != item.snapshot_manifest_digest
        or snapshot.publication.repository != item.snapshot_repository
        or snapshot.publication.tag != item.snapshot_tag
    ):
        raise ValueError("inventory snapshot descriptor does not match its route")
    return replace(
        route,
        storage_schema=item.storage_schema,
        snapshot_manifest_digest=item.snapshot_manifest_digest,
        snapshot_repository=item.snapshot_repository,
        snapshot_tag=item.snapshot_tag,
        storage_snapshot=snapshot.to_dict(),
    )


def is_worker_detachable_parked_route(route: SandboxRoute) -> bool:
    """Return whether drain can publish and remove this worker incarnation."""

    return bool(
        route.worker_state in {"attached", "detaching"}
        and (route.state or "unknown").lower() == "parked"
        and not route.delete_operation_id
    )


@dataclass(frozen=True)
class ProgramRequestState:
    """Current scheduler projection for one relay request.

    Product ownership remains in the relay journal and sandbox route. This
    projection is durable so autoscaling and shadow placement never need to
    reconstruct current program phases by scanning metric history.
    """

    request_id: str
    rollout_id: str
    sandbox_id: str
    sandbox_generation: int
    state: str
    resources: ResourceQuantity
    accepted_at: str = ""
    parked_at: str = ""
    response_ready_at: str = ""
    wake_started_at: str = ""
    wake_completed_at: str = ""
    updated_at: str = ""
    last_error: str = ""

    @property
    def is_terminal(self) -> bool:
        return self.state == "terminal"

    def to_dict(self) -> dict[str, Any]:
        return {
            "request_id": self.request_id,
            "rollout_id": self.rollout_id,
            "sandbox_id": self.sandbox_id,
            "sandbox_generation": self.sandbox_generation,
            "state": self.state,
            "resources": self.resources.to_dict(),
            "accepted_at": self.accepted_at,
            "parked_at": self.parked_at,
            "response_ready_at": self.response_ready_at,
            "wake_started_at": self.wake_started_at,
            "wake_completed_at": self.wake_completed_at,
            "updated_at": self.updated_at,
            "last_error": self.last_error,
        }


@dataclass(frozen=True)
class ExecRoute:
    session_id: str
    sandbox_id: str
    node_id: str
    job_id: str
    node_url: str
    created_at: str = ""
    updated_at: str = ""

    def __post_init__(self) -> None:
        identity = (
            self.session_id,
            self.sandbox_id,
            self.node_id,
            self.job_id,
            self.node_url,
        )
        if any(
            not isinstance(value, str) or not value or value != value.strip()
            for value in identity
        ):
            raise ValueError("exec route requires exact session and worker identity")

    def to_dict(self) -> dict[str, Any]:
        return {
            "session_id": self.session_id,
            "sandbox_id": self.sandbox_id,
            "node_id": self.node_id,
            "job_id": self.job_id,
            "node_url": self.node_url,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


@dataclass(frozen=True)
class SandboxMigration:
    migration_id: str
    sandbox_id: str
    phase: str
    source_node_id: str
    source_job_id: str
    source_node_url: str
    destination_node_id: str
    destination_job_id: str
    destination_node_url: str
    generation: int
    create_operation_id: str
    spec_hash: str
    storage_schema: str = ""
    snapshot_sha256: str = ""
    storage_snapshot: dict[str, Any] = field(default_factory=dict)
    source_fenced: bool = False
    created_at: str = ""
    updated_at: str = ""
    error: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "snapshot_sha256": self.snapshot_sha256,
            "storage_schema": self.storage_schema,
            "storage_snapshot": self.storage_snapshot,
            "source_fenced": self.source_fenced,
            "create_operation_id": self.create_operation_id,
            "created_at": self.created_at,
            "destination_job_id": self.destination_job_id,
            "destination_node_id": self.destination_node_id,
            "destination_node_url": self.destination_node_url,
            "error": self.error,
            "generation": self.generation,
            "migration_id": self.migration_id,
            "phase": self.phase,
            "sandbox_id": self.sandbox_id,
            "source_job_id": self.source_job_id,
            "source_node_id": self.source_node_id,
            "source_node_url": self.source_node_url,
            "spec_hash": self.spec_hash,
            "updated_at": self.updated_at,
        }


class _ExpiringDemand:
    created_at: str
    updated_at: str

    def is_expired(
        self,
        now: datetime,
        *,
        ttl_seconds: int = PENDING_DEMAND_TTL_SECONDS,
    ) -> bool:
        reference = parse_iso_datetime(self.updated_at) or parse_iso_datetime(
            self.created_at
        )
        if reference is None:
            return False
        return reference + timedelta(seconds=max(1, ttl_seconds)) <= now

    def expires_at(
        self,
        *,
        ttl_seconds: int = PENDING_DEMAND_TTL_SECONDS,
    ) -> str:
        reference = parse_iso_datetime(self.updated_at) or parse_iso_datetime(
            self.created_at
        )
        if reference is None:
            return ""
        return (reference + timedelta(seconds=max(1, ttl_seconds))).isoformat()


@dataclass(frozen=True)
class PendingSandboxDemand(_ExpiringDemand):
    sandbox_id: str
    resources: ResourceQuantity
    created_at: str
    updated_at: str
    attempts: int = 1
    generation: int = 0
    operation_id: str = ""
    spec_hash: str = ""
    failure_reason: str = ""

    @property
    def is_capacity_demand(self) -> bool:
        """Whether another schedulable node can resolve this pending request.

        Image and registry-reference failures happen after placement selected a
        node with sufficient hard resources.  Treating those failures as
        capacity demand creates a positive feedback loop: a pull failure asks
        for a VM, the VM clears the signal, and the retried pull asks for
        another VM.  Keep the durable record for retry identity and operator
        diagnostics, but do not let it scale the fleet.
        """

        reason = self.failure_reason.strip().lower()
        return not (
            reason.startswith("image_pull_http_")
            or reason == "registry_lease_unavailable"
            or reason == "wake_snapshot_publication_pending"
        )

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "sandbox_id": self.sandbox_id,
            "resources": self.resources.to_dict(),
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "attempts": self.attempts,
            "generation": self.generation,
            "operation_id": self.operation_id,
            "spec_hash": self.spec_hash,
            "failure_reason": self.failure_reason,
            "capacity_demand": self.is_capacity_demand,
        }
        expires_at = self.expires_at()
        if expires_at:
            payload["expires_at"] = expires_at
        return payload


@dataclass(frozen=True)
class PendingImageBuildDemand(_ExpiringDemand):
    image_id: str
    tag: str
    created_at: str
    updated_at: str
    attempts: int = 1

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "image_id": self.image_id,
            "tag": self.tag,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "attempts": self.attempts,
        }
        expires_at = self.expires_at()
        if expires_at:
            payload["expires_at"] = expires_at
        return payload


@dataclass(frozen=True)
class PendingImageWarmup:
    warmup_id: str
    image: str
    resources: ResourceQuantity
    count: int
    created_at: str
    updated_at: str
    expires_at: str
    image_id: str = ""
    warmed_node_ids: tuple[str, ...] = ()
    attempts: int = 1

    @property
    def total_resources(self) -> ResourceQuantity:
        return self.resources.scaled(cpu=self.count, memory=self.count, disk=self.count)

    def is_expired(self, now: datetime) -> bool:
        expires_at = parse_iso_datetime(self.expires_at)
        return expires_at is not None and expires_at <= now

    def to_dict(self) -> dict[str, Any]:
        return {
            "warmup_id": self.warmup_id,
            "image": self.image,
            "image_id": self.image_id,
            "resources": self.resources.to_dict(),
            "count": self.count,
            "total_resources": self.total_resources.to_dict(),
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "expires_at": self.expires_at,
            "warmed_node_ids": list(self.warmed_node_ids),
            "attempts": self.attempts,
        }


@dataclass(frozen=True)
class PreparedCapacityDemand:
    prepare_id: str
    resources: ResourceQuantity
    count: int
    created_at: str
    updated_at: str
    expires_at: str
    image: str = ""

    def __post_init__(self) -> None:
        if isinstance(self.count, bool) or not isinstance(self.count, int):
            raise ValueError("prepared capacity count must be an integer")
        if not 1 <= self.count <= MAX_PREPARED_CAPACITY_COUNT:
            raise ValueError(
                "prepared capacity count must be between 1 and "
                f"{MAX_PREPARED_CAPACITY_COUNT}"
            )

    @property
    def total_resources(self) -> ResourceQuantity:
        return self.resources.scaled(cpu=self.count, memory=self.count, disk=self.count)

    def is_expired(self, now: datetime) -> bool:
        expires_at = parse_iso_datetime(self.expires_at)
        return expires_at is not None and expires_at <= now

    def to_dict(self) -> dict[str, Any]:
        return {
            "prepare_id": self.prepare_id,
            "resources": self.resources.to_dict(),
            "count": self.count,
            "total_resources": self.total_resources.to_dict(),
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "expires_at": self.expires_at,
            "image": self.image,
        }


@dataclass(frozen=True)
class PreparedBuilderDemand:
    prepare_id: str
    count: int
    created_at: str
    updated_at: str
    expires_at: str

    def is_expired(self, now: datetime) -> bool:
        expires_at = parse_iso_datetime(self.expires_at)
        return expires_at is not None and expires_at <= now

    def to_dict(self) -> dict[str, Any]:
        return {
            "prepare_id": self.prepare_id,
            "count": self.count,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "expires_at": self.expires_at,
        }


@dataclass(frozen=True)
class RoutingState:
    sandboxes: dict[str, SandboxRoute]
    exec_sessions: dict[str, ExecRoute]
    pending: dict[str, PendingSandboxDemand]
    image_builds: dict[str, PendingImageBuildDemand]
    prepared: dict[str, PreparedCapacityDemand] = field(default_factory=dict)
    prepared_builders: dict[str, PreparedBuilderDemand] = field(default_factory=dict)
    image_warmups: dict[str, PendingImageWarmup] = field(default_factory=dict)


class RoutingStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = _route_lock(path)
        self._exec_route_cache = _exec_route_cache(path)
        self._exec_route_cache_sandbox_index = _exec_route_cache_sandbox_index(path)
        with self._lock:
            self._ensure_db()

    def load(self) -> RoutingState:
        with self._lock:
            with self._transaction() as conn:
                self._prune_expired_unlocked(conn, utc_now())
                return self._load_unlocked(conn)

    def load_metrics(self) -> tuple[RoutingState, int]:
        """Load dashboard state without materializing every exec session."""

        with self._connect() as conn:
            exec_session_count = int(
                conn.execute("SELECT COUNT(*) FROM exec_sessions").fetchone()[0]
            )
            return (
                self._load_unlocked(conn, include_exec_sessions=False),
                exec_session_count,
            )

    def get_sandbox(self, sandbox_id: str) -> SandboxRoute | None:
        with self._lock:
            with self._connect() as conn:
                return self._get_sandbox_unlocked(conn, sandbox_id)

    def get_sandbox_readonly(self, sandbox_id: str) -> SandboxRoute | None:
        with self._connect() as conn:
            return self._get_sandbox_unlocked(conn, sandbox_id)

    def get_managed_process(
        self,
        sandbox_id: str,
        job_id: str = "",
        *,
        sandbox_generation: int | None = None,
    ) -> ManagedProcessRecord | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT sandbox_generation, record_json FROM managed_processes
                WHERE sandbox_id = ?
                """,
                (sandbox_id,),
            ).fetchone()
        if row is None:
            return None
        if (
            sandbox_generation is not None
            and int(row["sandbox_generation"]) != sandbox_generation
        ):
            return None
        try:
            record = ManagedProcessRecord.from_dict(json.loads(row["record_json"]))
        except (TypeError, ValueError, json.JSONDecodeError):
            return None
        if job_id and record.job_id != job_id:
            return None
        return record

    def upsert_managed_process(
        self,
        route: SandboxRoute,
        record: ManagedProcessRecord,
    ) -> ManagedProcessRecord:
        if (
            record.sandbox_id != route.sandbox_id
            or record.sandbox_generation != route.generation
        ):
            raise SandboxRouteConflictError(
                "managed process does not belong to the routed sandbox generation"
            )
        record.validate()
        with self._lock:
            with self._transaction() as conn:
                current_route = self._get_sandbox_unlocked(conn, route.sandbox_id)
                if (
                    current_route is None
                    or current_route.generation != route.generation
                ):
                    raise SandboxRouteConflictError(
                        "managed process route generation is no longer current"
                    )
                existing_row = conn.execute(
                    "SELECT record_json FROM managed_processes WHERE sandbox_id = ?",
                    (route.sandbox_id,),
                ).fetchone()
                if existing_row is not None:
                    existing = ManagedProcessRecord.from_dict(
                        json.loads(existing_row["record_json"])
                    )
                    if existing.sandbox_generation > record.sandbox_generation:
                        raise SandboxRouteConflictError(
                            "managed process generation is fenced by newer state"
                        )
                    if existing.sandbox_generation == record.sandbox_generation and (
                        existing.job_id != record.job_id
                        or existing.spec_sha256 != record.spec_sha256
                    ):
                        raise SandboxRouteConflictError(
                            "sandbox generation already owns another managed process"
                        )
                    if existing.sandbox_generation == record.sandbox_generation and (
                        existing.sequence > record.sequence
                        or (existing.terminal and not record.terminal)
                    ):
                        return existing
                conn.execute(
                    """
                    INSERT INTO managed_processes (
                        sandbox_id, sandbox_generation, job_id,
                        spec_sha256, record_json, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(sandbox_id) DO UPDATE SET
                        sandbox_generation = excluded.sandbox_generation,
                        job_id = excluded.job_id,
                        spec_sha256 = excluded.spec_sha256,
                        record_json = excluded.record_json,
                        updated_at = excluded.updated_at
                    """,
                    (
                        record.sandbox_id,
                        record.sandbox_generation,
                        record.job_id,
                        record.spec_sha256,
                        _object_json(record.to_dict()),
                        record.updated_at or utc_now().isoformat(),
                    ),
                )
        return record

    def storage_snapshot_dependencies_readonly(
        self, *, require_complete: bool = False
    ) -> list[dict[str, Any]]:
        """GC roots, including live remote layers after restore authority expires."""
        with self._connect() as conn:
            conn.execute("BEGIN")
            if (
                require_complete
                and conn.execute(
                    """
                SELECT 1 FROM sandboxes s
                LEFT JOIN sandbox_storage_dependencies d
                  ON s.sandbox_id = d.sandbox_id AND s.generation = d.generation
                WHERE d.sandbox_id IS NULL LIMIT 1
                """
                ).fetchone()
                is not None
            ):
                raise ValueError(
                    "cannot GC before every sandbox reports its storage dependencies; "
                    "wait for current worker heartbeats or republish its parked snapshot"
                )
            rows = conn.execute(
                """
                SELECT d.storage_snapshot_json
                FROM sandbox_storage_dependencies d JOIN sandboxes s
                  ON s.sandbox_id = d.sandbox_id AND s.generation = d.generation
                WHERE d.storage_snapshot_json != '{}'
                UNION
                SELECT storage_snapshot_json FROM sandboxes
                  WHERE storage_snapshot_json != '{}'
                UNION
                SELECT storage_snapshot_json FROM sandbox_migrations
                  WHERE phase != 'complete' AND storage_snapshot_json != '{}'
                """
            ).fetchall()
        return [_object(json.loads(row[0])) for row in rows]

    def sandbox_routes_readonly(self) -> list[SandboxRoute]:
        with self._connect() as conn:
            return [
                route
                for route in (
                    _sandbox_route_from_row(row)
                    for row in conn.execute(
                        """
                        SELECT sandbox_id, node_id, job_id, node_url,
                               resources_json, spec_json, state, generation,
                               create_operation_id, spec_hash, delete_operation_id,
                               node_epoch, activity_epoch, worker_state,
                               storage_schema,
                               snapshot_manifest_digest, snapshot_repository,
                               snapshot_tag, storage_snapshot_json,
                               created_at, updated_at
                        FROM sandboxes
                        ORDER BY sandbox_id
                        """
                    )
                )
                if route is not None
            ]

    def sandbox_routes_matching_node_identity(
        self,
        *,
        node_id: str,
        job_id: str,
        node_url: str,
    ) -> list[SandboxRoute]:
        cleaned_node_url = node_url.strip().rstrip("/")
        node_url_with_slash = f"{cleaned_node_url}/" if cleaned_node_url else ""
        with self._connect() as conn:
            return [
                route
                for route in (
                    _sandbox_route_from_row(row)
                    for row in conn.execute(
                        """
                        SELECT sandbox_id, node_id, job_id, node_url,
                               resources_json, spec_json, state, generation,
                               create_operation_id, spec_hash, delete_operation_id,
                               node_epoch, activity_epoch, worker_state,
                               storage_schema,
                               snapshot_manifest_digest, snapshot_repository,
                               snapshot_tag, storage_snapshot_json,
                               created_at, updated_at
                        FROM sandboxes
                        WHERE node_id = ? OR job_id = ?
                           OR node_url IN (?, ?)
                        ORDER BY sandbox_id
                        """,
                        (
                            node_id.strip(),
                            job_id.strip(),
                            cleaned_node_url,
                            node_url_with_slash,
                        ),
                    )
                )
                if route is not None
            ]

    def upsert_program_request_transition_with_change(
        self,
        route: SandboxRoute,
        *,
        request_id: str,
        rollout_id: str,
        state: str,
        transition_at: str | None = None,
        accepted_at: str | None = None,
        parked_at: str | None = None,
        last_error: str = "",
        clear_error: bool = False,
    ) -> tuple[ProgramRequestState, bool]:
        request_id = request_id.strip()
        rollout_id = rollout_id.strip()
        if not request_id or not rollout_id:
            raise ValueError("program request and rollout ids are required")
        if state not in _PROGRAM_STATE_RANK:
            raise ValueError(f"unsupported program request state: {state}")
        transition_at = transition_at or utc_now().isoformat()
        with self._lock:
            with self._transaction() as conn:
                current_route = self._get_sandbox_unlocked(conn, route.sandbox_id)
                if (
                    current_route is None
                    or current_route.generation != route.generation
                ):
                    raise SandboxRouteConflictError(
                        "program transition does not own the current sandbox generation"
                    )
                existing_row = conn.execute(
                    """
                    SELECT request_id, rollout_id, sandbox_id,
                           sandbox_generation, state, resources_json,
                           accepted_at, parked_at, response_ready_at,
                           wake_started_at, wake_completed_at, updated_at,
                           last_error
                    FROM program_requests
                    WHERE request_id = ?
                    """,
                    (request_id,),
                ).fetchone()
                existing = (
                    _program_request_from_row(existing_row)
                    if existing_row is not None
                    else None
                )
                if existing is not None and (
                    existing.rollout_id != rollout_id
                    or existing.sandbox_id != route.sandbox_id
                    or existing.sandbox_generation != route.generation
                ):
                    raise SandboxRouteConflictError(
                        "program request id belongs to another rollout or sandbox"
                    )
                effective_state = state
                if (
                    existing is not None
                    and _PROGRAM_STATE_RANK[existing.state] > _PROGRAM_STATE_RANK[state]
                ):
                    effective_state = existing.state
                timestamps = {
                    "accepted_at": existing.accepted_at if existing else "",
                    "parked_at": existing.parked_at if existing else "",
                    "response_ready_at": (
                        existing.response_ready_at if existing else ""
                    ),
                    "wake_started_at": existing.wake_started_at if existing else "",
                    "wake_completed_at": (
                        existing.wake_completed_at if existing else ""
                    ),
                }
                if accepted_at and not timestamps["accepted_at"]:
                    timestamps["accepted_at"] = accepted_at
                if not timestamps["accepted_at"]:
                    timestamps["accepted_at"] = transition_at
                if parked_at and not timestamps["parked_at"]:
                    timestamps["parked_at"] = parked_at
                transition_field = {
                    "ready_to_wake": "response_ready_at",
                    "waking": "wake_started_at",
                    "acting": "wake_completed_at",
                }.get(state)
                if transition_field and not timestamps[transition_field]:
                    timestamps[transition_field] = transition_at
                advanced = bool(
                    existing is not None
                    and _PROGRAM_STATE_RANK[effective_state]
                    > _PROGRAM_STATE_RANK[existing.state]
                )
                error = (
                    last_error
                    if last_error
                    else (
                        ""
                        if clear_error or advanced
                        else (existing.last_error if existing else "")
                    )
                )
                if existing is not None and (
                    existing.state == effective_state
                    and existing.resources == route.resources
                    and existing.accepted_at == timestamps["accepted_at"]
                    and existing.parked_at == timestamps["parked_at"]
                    and existing.response_ready_at == timestamps["response_ready_at"]
                    and existing.wake_started_at == timestamps["wake_started_at"]
                    and existing.wake_completed_at == timestamps["wake_completed_at"]
                    and existing.last_error == error
                ):
                    return existing, False
                conn.execute(
                    """
                    INSERT INTO program_requests (
                        request_id, rollout_id, sandbox_id, sandbox_generation,
                        state, resources_json, accepted_at, parked_at,
                        response_ready_at, wake_started_at, wake_completed_at,
                        updated_at, last_error
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(request_id) DO UPDATE SET
                        state = excluded.state,
                        resources_json = excluded.resources_json,
                        accepted_at = excluded.accepted_at,
                        parked_at = excluded.parked_at,
                        response_ready_at = excluded.response_ready_at,
                        wake_started_at = excluded.wake_started_at,
                        wake_completed_at = excluded.wake_completed_at,
                        updated_at = excluded.updated_at,
                        last_error = excluded.last_error
                    """,
                    (
                        request_id,
                        rollout_id,
                        route.sandbox_id,
                        route.generation,
                        effective_state,
                        _resources_json(route.resources),
                        timestamps["accepted_at"],
                        timestamps["parked_at"],
                        timestamps["response_ready_at"],
                        timestamps["wake_started_at"],
                        timestamps["wake_completed_at"],
                        transition_at,
                        error,
                    ),
                )
                row = conn.execute(
                    """
                    SELECT request_id, rollout_id, sandbox_id,
                           sandbox_generation, state, resources_json,
                           accepted_at, parked_at, response_ready_at,
                           wake_started_at, wake_completed_at, updated_at,
                           last_error
                    FROM program_requests
                    WHERE request_id = ?
                    """,
                    (request_id,),
                ).fetchone()
                assert row is not None
                return _program_request_from_row(row), True

    def program_requests_readonly(
        self,
        *,
        include_terminal: bool = False,
        limit: int = 100_000,
    ) -> list[ProgramRequestState]:
        clauses = "" if include_terminal else "WHERE state != 'terminal'"
        bounded_limit = max(1, min(1_000_000, int(limit)))
        with self._connect() as conn:
            return [
                _program_request_from_row(row)
                for row in conn.execute(
                    f"""
                    SELECT request_id, rollout_id, sandbox_id,
                           sandbox_generation, state, resources_json,
                           accepted_at, parked_at, response_ready_at,
                           wake_started_at, wake_completed_at, updated_at,
                           last_error
                    FROM program_requests
                    {clauses}
                    ORDER BY updated_at DESC, request_id
                    LIMIT ?
                    """,
                    (bounded_limit,),
                )
            ]

    def set_sandbox_state_if_current(
        self,
        route: SandboxRoute,
        *,
        expected_states: Iterable[str],
        state: str,
        node_epoch: str | None = None,
        activity_epoch: int | None = None,
        storage_schema: str | None = None,
        snapshot_manifest_digest: str | None = None,
        snapshot_repository: str | None = None,
        snapshot_tag: str | None = None,
        storage_snapshot: dict[str, Any] | None = None,
    ) -> SandboxRoute | None:
        """Change only the state of the exact routed sandbox incarnation.

        A supplied node/activity pair is a worker lifecycle proof. It must
        strictly postdate the caller's route and can never move the durable
        route behind a heartbeat that has already been reconciled.
        """

        expected = {str(item or "unknown").strip().lower() for item in expected_states}
        cleaned_state = str(state).strip()
        if not expected or not cleaned_state:
            raise ValueError("expected and destination sandbox states are required")
        if (node_epoch is None) != (activity_epoch is None):
            raise ValueError("node_epoch and activity_epoch must be supplied together")
        lifecycle_node_epoch: str | None = None
        lifecycle_activity_epoch: int | None = None
        if node_epoch is not None:
            lifecycle_node_epoch = node_epoch.strip()
            if not lifecycle_node_epoch:
                raise ValueError("lifecycle node_epoch must be nonempty")
            if isinstance(activity_epoch, bool) or not isinstance(activity_epoch, int):
                raise ValueError("lifecycle activity_epoch must be an integer")
            if activity_epoch < 0:
                raise ValueError("lifecycle activity_epoch must be non-negative")
            lifecycle_activity_epoch = activity_epoch
        with self._lock:
            with self._transaction() as conn:
                current = self._get_sandbox_unlocked(conn, route.sandbox_id)
                if (
                    current is None
                    or (current.state or "unknown").lower() not in expected
                    or not _same_sandbox_route_incarnation(current, route)
                    or current.worker_state != route.worker_state
                    or bool(current.delete_operation_id)
                    or (
                        lifecycle_node_epoch is not None
                        and (
                            lifecycle_activity_epoch is None
                            or lifecycle_activity_epoch <= route.activity_epoch
                            or bool(
                                route.node_epoch
                                and route.node_epoch != lifecycle_node_epoch
                            )
                            or bool(
                                current.node_epoch
                                and current.node_epoch != lifecycle_node_epoch
                            )
                            or current.activity_epoch > lifecycle_activity_epoch
                        )
                    )
                ):
                    return None
                stored = replace(
                    current,
                    state=cleaned_state,
                    node_epoch=(
                        current.node_epoch
                        if lifecycle_node_epoch is None
                        else lifecycle_node_epoch
                    ),
                    activity_epoch=(
                        current.activity_epoch
                        if lifecycle_activity_epoch is None
                        else lifecycle_activity_epoch
                    ),
                    storage_schema=(
                        current.storage_schema
                        if storage_schema is None
                        else storage_schema
                    ),
                    snapshot_manifest_digest=(
                        current.snapshot_manifest_digest
                        if snapshot_manifest_digest is None
                        else snapshot_manifest_digest
                    ),
                    snapshot_repository=(
                        current.snapshot_repository
                        if snapshot_repository is None
                        else snapshot_repository
                    ),
                    snapshot_tag=(
                        current.snapshot_tag if snapshot_tag is None else snapshot_tag
                    ),
                    storage_snapshot=(
                        current.storage_snapshot
                        if storage_snapshot is None
                        else dict(storage_snapshot)
                    ),
                    updated_at=utc_now().isoformat(),
                )
                self._write_sandbox(conn, stored)
            return stored

    def begin_sandbox_detach(
        self,
        route: SandboxRoute,
    ) -> SandboxRoute | None:
        """Fence a portable park before removing its worker-local incarnation."""

        with self._lock:
            with self._transaction() as conn:
                current = self._get_sandbox_unlocked(conn, route.sandbox_id)
                if current is None or not _same_sandbox_route_incarnation(
                    current, route
                ):
                    return None
                if current.worker_state in {"detaching", "detached"}:
                    return current
                if (
                    current.worker_state != "attached"
                    or not is_portable_parked_route(current)
                    or bool(current.delete_operation_id)
                ):
                    return None
                stored = replace(
                    current,
                    worker_state="detaching",
                    updated_at=utc_now().isoformat(),
                )
                self._write_sandbox(conn, stored)
                conn.execute(
                    "DELETE FROM exec_sessions WHERE sandbox_id = ?",
                    (stored.sandbox_id,),
                )
            self._drop_cached_exec_routes_for_sandbox_unlocked(route.sandbox_id)
            return stored

    def complete_sandbox_detach(
        self,
        route: SandboxRoute,
    ) -> SandboxRoute | None:
        """Commit that a published park no longer occupies its last worker."""

        with self._lock:
            with self._transaction() as conn:
                current = self._get_sandbox_unlocked(conn, route.sandbox_id)
                if current is None or not _same_sandbox_route_incarnation(
                    current, route
                ):
                    return None
                if current.worker_state == "detached":
                    return current
                if (
                    current.worker_state != "detaching"
                    or not is_portable_parked_route(current)
                    or bool(current.delete_operation_id)
                ):
                    return None
                stored = self._detach_owner_lost_route_unlocked(
                    conn,
                    current,
                    updated_at=utc_now().isoformat(),
                )
            return stored

    def upsert_sandbox(
        self,
        route: SandboxRoute,
        *,
        allow_node_epoch_adoption: bool = True,
    ) -> SandboxRoute:
        with self._lock:
            now = utc_now().isoformat()
            with self._transaction() as conn:
                existing = self._get_sandbox_unlocked(conn, route.sandbox_id)
                if existing is not None and not _route_update_is_current(
                    existing,
                    route,
                    allow_node_epoch_adoption=allow_node_epoch_adoption,
                ):
                    return existing
                activity_epoch = route.activity_epoch
                if existing is not None and route.node_epoch == existing.node_epoch:
                    activity_epoch = max(activity_epoch, existing.activity_epoch)
                stored = replace(
                    route,
                    activity_epoch=max(0, activity_epoch),
                    created_at=(
                        route.created_at
                        or (existing.created_at if existing is not None else now)
                    ),
                    updated_at=now,
                )
                self._write_sandbox(conn, stored)
                conn.execute(
                    "DELETE FROM pending WHERE sandbox_id = ?", (route.sandbox_id,)
                )
            return stored

    def _claim_prepared_capacity_unlocked(
        self,
        conn: sqlite3.Connection,
        route: SandboxRoute,
    ) -> PreparedCapacityDemand | None:
        route_image = str(route.spec.get("image") or "").strip()
        now = utc_now()
        matching = [
            item
            for item in (
                _prepared_from_row(row)
                for row in conn.execute(
                    """
                    SELECT prepare_id, resources_json, count, created_at,
                           updated_at, expires_at, image
                    FROM prepared_capacity
                    """
                )
            )
            if not item.is_expired(now)
            and item.resources == route.resources
            and (not item.image or item.image == route_image)
        ]
        if not matching:
            return None
        selected = min(
            matching,
            key=lambda item: (
                0 if item.image == route_image and item.image else 1,
                item.created_at,
                item.prepare_id,
            ),
        )
        if selected.count <= 1:
            conn.execute(
                "DELETE FROM prepared_capacity WHERE prepare_id = ?",
                (selected.prepare_id,),
            )
        else:
            self._write_prepared(
                conn,
                PreparedCapacityDemand(
                    prepare_id=selected.prepare_id,
                    resources=selected.resources,
                    count=selected.count - 1,
                    created_at=selected.created_at,
                    updated_at=now.isoformat(),
                    expires_at=selected.expires_at,
                    image=selected.image,
                ),
            )
        return selected

    def allocate_sandbox_create_with_pending(
        self,
        allocation: SandboxRouteAllocation,
        *,
        spec_hash: str,
        create_operation_id: str | None = None,
    ) -> tuple[SandboxRoute, PendingSandboxDemand | None]:
        """Allocate a route and atomically return the demand it consumed."""

        operation_id = (
            f"create-{uuid4().hex}"
            if create_operation_id is None
            else create_operation_id.strip()
        )
        if not operation_id or not spec_hash.strip():
            raise ValueError("create operation id and spec hash are required")
        with self._lock:
            now = utc_now().isoformat()
            with self._transaction() as conn:
                pending = self._get_pending_unlocked(conn, allocation.sandbox_id)
                existing = self._get_sandbox_unlocked(conn, allocation.sandbox_id)
                if existing is not None:
                    if (existing.spec_hash and existing.spec_hash != spec_hash) or (
                        existing.spec
                        and allocation.spec
                        and existing.spec != allocation.spec
                    ):
                        raise SandboxRouteConflictError(
                            f"sandbox route already exists with a different spec: "
                            f"{allocation.sandbox_id}"
                        )
                    return existing, pending
                row = conn.execute(
                    "SELECT generation FROM sandbox_generation_hwm WHERE sandbox_id = ?",
                    (allocation.sandbox_id,),
                ).fetchone()
                high_water = int(row["generation"]) if row is not None else 0
                generation = high_water + 1
                stored = SandboxRoute(
                    sandbox_id=allocation.sandbox_id,
                    node_id=allocation.node_id,
                    job_id=allocation.job_id,
                    node_url=allocation.node_url,
                    resources=allocation.resources,
                    spec=dict(allocation.spec),
                    state="creating",
                    generation=generation,
                    create_operation_id=operation_id,
                    spec_hash=spec_hash.strip(),
                    node_epoch=allocation.node_epoch,
                    activity_epoch=max(0, allocation.activity_epoch),
                    created_at=now,
                    updated_at=now,
                )
                self._write_sandbox(conn, stored)
                conn.execute(
                    "DELETE FROM pending WHERE sandbox_id = ?",
                    (allocation.sandbox_id,),
                )
                self._claim_prepared_capacity_unlocked(conn, stored)
            return stored, pending

    def prepare_sandbox_delete(self, sandbox_id: str) -> SandboxRoute | None:
        """Persist and reuse one delete operation for the current generation."""

        with self._lock:
            with self._transaction() as conn:
                existing = self._get_sandbox_unlocked(conn, sandbox_id)
                if existing is None:
                    return None
                if existing.delete_operation_id:
                    stored = existing
                else:
                    stored = SandboxRoute(
                        **{
                            **existing.__dict__,
                            "delete_operation_id": f"delete-{uuid4().hex}",
                            "updated_at": utc_now().isoformat(),
                        }
                    )
                    self._write_sandbox(conn, stored)
                self._terminalize_program_requests_unlocked(
                    conn,
                    sandbox_id,
                    generation=stored.generation,
                    last_error="sandbox deletion requested",
                )
            return stored

    def begin_sandbox_migration(
        self,
        source: SandboxRoute,
        *,
        migration_id: str,
        destination_node_id: str,
        destination_job_id: str,
        destination_node_url: str,
    ) -> SandboxMigration:
        cleaned_id = migration_id.strip()
        cleaned_destination_url = destination_node_url.strip().rstrip("/")
        if (
            not cleaned_id
            or not destination_node_id.strip()
            or not destination_job_id.strip()
            or not cleaned_destination_url
        ):
            raise ValueError("migration and destination identities are required")
        with self._lock:
            now = utc_now().isoformat()
            with self._transaction() as conn:
                existing_migration = self._get_sandbox_migration_unlocked(
                    conn,
                    cleaned_id,
                )
                if existing_migration is not None:
                    return existing_migration
                active = conn.execute(
                    """
                    SELECT migration_id FROM sandbox_migrations
                    WHERE sandbox_id = ? AND phase != 'complete'
                    """,
                    (source.sandbox_id,),
                ).fetchone()
                if active is not None:
                    raise SandboxRouteConflictError(
                        "sandbox already has an active migration"
                    )
                current = self._get_sandbox_unlocked(conn, source.sandbox_id)
                if (
                    current is None
                    or current.generation != source.generation
                    or current.create_operation_id != source.create_operation_id
                    or current.spec_hash != source.spec_hash
                    or current.node_id != source.node_id
                    or current.job_id != source.job_id
                    or current.node_url.rstrip("/") != source.node_url.rstrip("/")
                    or current.worker_state != source.worker_state
                    or bool(current.delete_operation_id)
                ):
                    raise SandboxRouteConflictError(
                        "sandbox route changed before migration began"
                    )
                migration = SandboxMigration(
                    migration_id=cleaned_id,
                    sandbox_id=source.sandbox_id,
                    phase="planned",
                    source_node_id=source.node_id,
                    source_job_id=source.job_id,
                    source_node_url=source.node_url.rstrip("/"),
                    destination_node_id=destination_node_id.strip(),
                    destination_job_id=destination_job_id.strip(),
                    destination_node_url=cleaned_destination_url,
                    generation=source.generation,
                    create_operation_id=source.create_operation_id,
                    spec_hash=source.spec_hash,
                    created_at=now,
                    updated_at=now,
                )
                self._write_sandbox_migration(conn, migration)
            return migration

    def get_sandbox_migration(
        self,
        migration_id: str,
    ) -> SandboxMigration | None:
        with self._connect() as conn:
            return self._get_sandbox_migration_unlocked(
                conn,
                migration_id.strip(),
            )

    def sandbox_migrations(
        self,
        *,
        active_only: bool = False,
    ) -> list[SandboxMigration]:
        where = "WHERE phase != 'complete'" if active_only else ""
        with self._connect() as conn:
            return [
                _sandbox_migration_from_row(row)
                for row in conn.execute(
                    f"""
                    SELECT migration_id, sandbox_id, phase,
                           source_node_id, source_job_id, source_node_url,
                           destination_node_id, destination_job_id,
                           destination_node_url, generation,
                           create_operation_id, spec_hash,
                           storage_schema, snapshot_sha256,
                           storage_snapshot_json, source_fenced,
                           created_at, updated_at, error
                    FROM sandbox_migrations
                    {where}
                    ORDER BY created_at, migration_id
                    """
                )
            ]

    def terminalize_orphaned_sandbox_migrations(
        self,
        *,
        max_count: int,
    ) -> list[SandboxMigration]:
        """Bound stale active journals that no longer have a canonical route."""

        if max_count < 0:
            raise ValueError(
                "orphaned migration reconciliation limit cannot be negative"
            )
        if max_count == 0:
            return []
        now = utc_now().isoformat()
        terminalized: list[SandboxMigration] = []
        with self._lock:
            with self._transaction() as conn:
                rows = conn.execute(
                    """
                    SELECT migration_id, sandbox_id, phase,
                           source_node_id, source_job_id, source_node_url,
                           destination_node_id, destination_job_id,
                           destination_node_url, generation,
                           create_operation_id, spec_hash,
                           storage_schema, snapshot_sha256,
                           storage_snapshot_json, source_fenced,
                           created_at, updated_at, error
                    FROM sandbox_migrations AS migration
                    WHERE migration.phase != 'complete'
                      AND NOT EXISTS (
                          SELECT 1 FROM sandboxes AS sandbox
                          WHERE sandbox.sandbox_id = migration.sandbox_id
                      )
                    ORDER BY migration.created_at, migration.migration_id
                    LIMIT ?
                    """,
                    (max_count,),
                ).fetchall()
                for row in rows:
                    migration = _sandbox_migration_from_row(row)
                    error = migration.error or "sandbox route is absent"
                    updated = conn.execute(
                        """
                        UPDATE sandbox_migrations
                        SET phase = 'complete', updated_at = ?, error = ?
                        WHERE migration_id = ? AND phase != 'complete'
                          AND NOT EXISTS (
                              SELECT 1 FROM sandboxes AS sandbox
                              WHERE sandbox.sandbox_id = sandbox_migrations.sandbox_id
                          )
                        """,
                        (now, error, migration.migration_id),
                    )
                    if updated.rowcount:
                        terminalized.append(
                            replace(
                                migration,
                                phase="complete",
                                updated_at=now,
                                error=error,
                            )
                        )
        return terminalized

    def advance_sandbox_migration(
        self,
        migration_id: str,
        *,
        expected_phases: Iterable[str],
        phase: str,
        storage_schema: str | None = None,
        snapshot_sha256: str | None = None,
        storage_snapshot: dict[str, Any] | None = None,
        source_fenced: bool | None = None,
        error: str | None = None,
    ) -> SandboxMigration | None:
        expected = set(expected_phases)
        if not expected or not phase.strip():
            raise ValueError("migration phases are required")
        with self._lock:
            with self._transaction() as conn:
                existing = self._get_sandbox_migration_unlocked(
                    conn,
                    migration_id.strip(),
                )
                if existing is None or existing.phase not in expected:
                    return None
                stored = replace(
                    existing,
                    phase=phase.strip(),
                    storage_schema=(
                        storage_schema
                        if storage_schema is not None
                        else existing.storage_schema
                    ),
                    snapshot_sha256=(
                        snapshot_sha256
                        if snapshot_sha256 is not None
                        else existing.snapshot_sha256
                    ),
                    storage_snapshot=(
                        dict(storage_snapshot)
                        if storage_snapshot is not None
                        else existing.storage_snapshot
                    ),
                    source_fenced=(
                        bool(source_fenced)
                        if source_fenced is not None
                        else existing.source_fenced
                    ),
                    error=error if error is not None else existing.error,
                    updated_at=utc_now().isoformat(),
                )
                self._write_sandbox_migration(conn, stored)
            return stored

    def complete_sandbox_migration(
        self,
        migration_id: str,
        *,
        wake_destination: bool = False,
    ) -> SandboxMigration | None:
        """Complete activation and optionally reserve the destination for wake.

        Wake relocation must not expose a parked destination after dropping the
        migration reservation. Updating the route and journal in one SQLite
        transaction prevents an unrelated create from consuming the CPU/RAM in
        that gap.
        """

        with self._lock:
            with self._transaction() as conn:
                migration = self._get_sandbox_migration_unlocked(
                    conn,
                    migration_id.strip(),
                )
                if migration is None:
                    return None
                current = self._get_sandbox_unlocked(conn, migration.sandbox_id)
                if migration.phase == "complete":
                    if (
                        wake_destination
                        and current is not None
                        and current.node_id == migration.destination_node_id
                        and current.job_id == migration.destination_job_id
                        and (current.state or "unknown").lower() == "parked"
                    ):
                        current = replace(
                            current,
                            state="waking",
                            updated_at=utc_now().isoformat(),
                        )
                        self._write_sandbox(conn, current)
                    return migration
                if migration.phase != "activated" or current is None:
                    return None
                if (
                    current.generation != migration.generation
                    or current.create_operation_id != migration.create_operation_id
                    or current.spec_hash != migration.spec_hash
                    or current.node_id != migration.destination_node_id
                    or current.job_id != migration.destination_job_id
                    or current.node_url.rstrip("/")
                    != migration.destination_node_url.rstrip("/")
                    or bool(current.delete_operation_id)
                ):
                    return None
                now = utc_now().isoformat()
                if wake_destination:
                    current = replace(current, state="waking", updated_at=now)
                    self._write_sandbox(conn, current)
                completed = replace(
                    migration,
                    phase="complete",
                    updated_at=now,
                    error="",
                )
                self._write_sandbox_migration(conn, completed)
            return completed

    def route_sandbox_migration(
        self,
        migration_id: str,
    ) -> tuple[SandboxMigration, SandboxRoute] | None:
        """Atomically commit destination routing and the migration journal."""

        with self._lock:
            with self._transaction() as conn:
                migration = self._get_sandbox_migration_unlocked(
                    conn,
                    migration_id.strip(),
                )
                if migration is None:
                    return None
                current = self._get_sandbox_unlocked(conn, migration.sandbox_id)
                if migration.phase == "routed":
                    if (
                        current is not None
                        and current.node_id == migration.destination_node_id
                        and current.node_url.rstrip("/")
                        == migration.destination_node_url.rstrip("/")
                    ):
                        return migration, current
                    return None
                if migration.phase != "staged" or current is None:
                    return None
                if (
                    current.generation != migration.generation
                    or current.create_operation_id != migration.create_operation_id
                    or current.spec_hash != migration.spec_hash
                    or current.node_id != migration.source_node_id
                    or current.job_id != migration.source_job_id
                    or current.node_url.rstrip("/")
                    != migration.source_node_url.rstrip("/")
                    or bool(current.delete_operation_id)
                ):
                    return None
                snapshot_manifest_digest = ""
                snapshot_repository = ""
                snapshot_tag = ""
                if migration.storage_schema:
                    publication = migration.storage_snapshot.get("publication")
                    if not isinstance(publication, dict):
                        return None
                    snapshot_manifest_digest = str(
                        publication.get("manifest_digest") or ""
                    )
                    if not snapshot_manifest_digest:
                        return None
                    snapshot_repository = str(publication.get("repository") or "")
                    snapshot_tag = str(publication.get("tag") or "")
                    if not snapshot_repository or not snapshot_tag:
                        return None
                now = utc_now().isoformat()
                route = replace(
                    current,
                    node_id=migration.destination_node_id,
                    job_id=migration.destination_job_id,
                    node_url=migration.destination_node_url,
                    state="parked",
                    node_epoch="",
                    activity_epoch=0,
                    worker_state="attached",
                    storage_schema=migration.storage_schema,
                    snapshot_manifest_digest=snapshot_manifest_digest,
                    snapshot_repository=snapshot_repository,
                    snapshot_tag=snapshot_tag,
                    storage_snapshot=(
                        dict(migration.storage_snapshot)
                        if migration.storage_schema
                        else {}
                    ),
                    updated_at=now,
                )
                self._write_sandbox(conn, route)
                conn.execute(
                    "DELETE FROM exec_sessions WHERE sandbox_id = ?",
                    (migration.sandbox_id,),
                )
                migration = replace(
                    migration,
                    phase="routed",
                    updated_at=now,
                    error="",
                )
                self._write_sandbox_migration(conn, migration)
            self._drop_cached_exec_routes_for_sandbox_unlocked(migration.sandbox_id)
            return migration, route

    def delete_sandbox_if_current(
        self,
        sandbox_id: str,
        *,
        generation: int,
        create_operation_id: str = "",
        delete_operation_id: str = "",
    ) -> SandboxRoute | None:
        """Delete only the exact route incarnation observed by the caller."""

        with self._lock:
            with self._transaction() as conn:
                existing = self._get_sandbox_unlocked(conn, sandbox_id)
                if existing is None or existing.generation != generation:
                    return None
                if (
                    create_operation_id
                    and existing.create_operation_id != create_operation_id
                ):
                    return None
                if (
                    delete_operation_id
                    and existing.delete_operation_id != delete_operation_id
                ):
                    return None
                self._delete_sandbox_unlocked(conn, existing)
            self._drop_cached_exec_routes_for_sandbox_unlocked(sandbox_id)
            return existing

    def reconcile_sandboxes_for_node(
        self,
        node_url: str,
        observations: Iterable[SandboxInventoryEntry],
        *,
        node_id: str,
        job_id: str,
        reported_sandbox_ids: Iterable[str],
        observed_at: str,
        node_epoch: str = "",
        activity_epoch: int = 0,
        inventory_complete: bool = True,
        allow_node_epoch_adoption: bool = True,
    ) -> tuple[list[SandboxRoute], list[SandboxRoute]]:
        """Reconcile inventory and return removed routes and stale snapshots.

        The caller owns external Registry leases, so SQLite reconciliation
        reports the exact durable references that became obsolete instead of
        silently leaking them.
        """
        node_url = node_url.strip()
        node_id = node_id.strip()
        job_id = job_id.strip()
        if not node_url or not node_id or not job_id:
            raise ValueError("sandbox inventory requires exact node identity")
        observed = tuple(observations)
        # Malformed or delayed reports are not proof of absence.
        reported_ids = {
            sandbox_id
            for raw_sandbox_id in reported_sandbox_ids
            if (sandbox_id := str(raw_sandbox_id).strip())
        }
        reported_ids.update(item.sandbox_id for item in observed)
        observed_at_dt = parse_iso_datetime(observed_at)
        with self._lock:
            removed_sandbox_ids: list[str] = []
            removed_routes: list[SandboxRoute] = []
            stale_snapshot_routes: list[SandboxRoute] = []
            with self._transaction() as conn:
                for item in observed:
                    existing = self._get_sandbox_unlocked(conn, item.sandbox_id)
                    if existing is None:
                        continue
                    observed_state = item.route_state
                    # Heartbeats are sampled independently of synchronous
                    # lifecycle requests. Registration, transition, recovery,
                    # unavailable, and unrecognised samples still prove
                    # presence through ``reported_ids``, but none may become
                    # durable gateway routing state. The next stable
                    # RUNNING/PARKED observation remains authoritative.
                    if observed_state is None:
                        continue
                    parked = observed_state == "parked"
                    published_snapshot = bool(parked and item.storage_snapshot)
                    validated_snapshot: SandboxRoute | None = None
                    if published_snapshot:
                        try:
                            validated_snapshot = route_with_inventory_snapshot(
                                existing,
                                item,
                            )
                        except ValueError:
                            # Presence and stable lifecycle state remain useful,
                            # but malformed publication metadata can never grant
                            # portable authority.
                            published_snapshot = False
                    candidate = replace(
                        existing,
                        node_id=node_id,
                        job_id=job_id,
                        node_url=node_url,
                        resources=(
                            item.resources
                            if item.resources != ResourceQuantity()
                            else existing.resources
                        ),
                        state=observed_state,
                        generation=item.generation,
                        create_operation_id=item.operation_id,
                        spec_hash=item.spec_hash,
                        node_epoch=node_epoch,
                        # Activity counters are scoped to a node epoch.  Do not
                        # carry the old epoch's high water into a proven restart.
                        activity_epoch=max(0, activity_epoch),
                        storage_schema=(
                            validated_snapshot.storage_schema
                            if published_snapshot
                            else (existing.storage_schema if parked else "")
                        ),
                        snapshot_manifest_digest=(
                            validated_snapshot.snapshot_manifest_digest
                            if published_snapshot
                            else (existing.snapshot_manifest_digest if parked else "")
                        ),
                        snapshot_repository=(
                            validated_snapshot.snapshot_repository
                            if published_snapshot
                            else (existing.snapshot_repository if parked else "")
                        ),
                        snapshot_tag=(
                            validated_snapshot.snapshot_tag
                            if published_snapshot
                            else (existing.snapshot_tag if parked else "")
                        ),
                        storage_snapshot=(
                            dict(validated_snapshot.storage_snapshot)
                            if published_snapshot
                            else (dict(existing.storage_snapshot) if parked else {})
                        ),
                        updated_at=observed_at,
                    )
                    if candidate.generation != existing.generation:
                        continue
                    if not _route_update_is_current(
                        existing,
                        candidate,
                        allow_node_epoch_adoption=allow_node_epoch_adoption,
                    ):
                        continue
                    if existing.snapshot_manifest_digest and (
                        existing.snapshot_manifest_digest
                        != candidate.snapshot_manifest_digest
                        or existing.snapshot_repository != candidate.snapshot_repository
                        or existing.snapshot_tag != candidate.snapshot_tag
                    ):
                        stale_snapshot_routes.append(existing)
                    self._write_sandbox(conn, candidate)
                    if item.storage_dependency is not None:
                        # This metadata conveys liveness only, never permission
                        # to restore a running sandbox from an old checkpoint.
                        dependency = item.storage_dependency
                        if dependency:
                            dependency = StorageSnapshotPublication.from_dict(
                                dependency
                            ).to_dict()
                        conn.execute(
                            """
                            INSERT INTO sandbox_storage_dependencies VALUES (?, ?, ?)
                            ON CONFLICT(sandbox_id) DO UPDATE SET
                                generation = excluded.generation,
                                storage_snapshot_json = excluded.storage_snapshot_json
                            WHERE excluded.storage_snapshot_json != '{}'
                            """,
                            (
                                candidate.sandbox_id,
                                candidate.generation,
                                _object_json({"publication": dependency})
                                if dependency
                                else "{}",
                            ),
                        )

                    conn.execute(
                        "DELETE FROM pending WHERE sandbox_id = ?",
                        (candidate.sandbox_id,),
                    )

                current_routes = self._sandbox_routes_for_node_url_unlocked(
                    conn,
                    node_url,
                )
                for route in current_routes:
                    sandbox_id = route.sandbox_id
                    if sandbox_id in reported_ids:
                        continue
                    if not inventory_complete:
                        continue
                    replaced_boot = bool(
                        route.node_epoch
                        and node_epoch
                        and route.node_epoch != node_epoch
                    )
                    if replaced_boot and not allow_node_epoch_adoption:
                        # Refresh polling carries an already accepted heartbeat
                        # fence. It may prove state only for that exact boot; a
                        # delayed response from a retired boot cannot delete the
                        # replacement boot's inventory.
                        continue
                    if (route.state or "unknown").lower() in {
                        "creating",
                        "unknown",
                    } and not replaced_boot:
                        # An empty inventory does not distinguish "create never
                        # arrived" from "create is still in progress" with the
                        # current node protocol. Preserve the reservation until a
                        # later generation-aware reconciliation can prove absence.
                        # A newly accepted boot epoch is that proof: the former
                        # guest process namespace no longer exists.
                        continue
                    if route.node_epoch and not node_epoch:
                        # Do not let an unversioned/legacy observation erase a
                        # route that is already fenced to a known guest boot.
                        continue
                    if not replaced_boot and route.activity_epoch > max(
                        0, activity_epoch
                    ):
                        continue
                    route_updated_at = parse_iso_datetime(
                        route.updated_at
                    ) or parse_iso_datetime(route.created_at)
                    if not (
                        observed_at_dt is None
                        or route_updated_at is None
                        or route_updated_at <= observed_at_dt
                    ):
                        continue
                    if (
                        sandbox_owner_loss_disposition(route)
                        is SandboxOwnerLossDisposition.RECOVER_DETACHED
                    ):
                        self._detach_owner_lost_route_unlocked(
                            conn,
                            route,
                            updated_at=observed_at,
                        )
                        removed_sandbox_ids.append(sandbox_id)
                        continue
                    if not self._delete_sandbox_unlocked(conn, route):
                        continue
                    removed_routes.append(route)
                    removed_sandbox_ids.append(sandbox_id)
            for sandbox_id in removed_sandbox_ids:
                self._drop_cached_exec_routes_for_sandbox_unlocked(sandbox_id)
            return removed_routes, stale_snapshot_routes

    def delete_sandbox(self, sandbox_id: str) -> None:
        with self._lock:
            with self._transaction() as conn:
                self._delete_sandbox_unlocked(conn, sandbox_id)
            self._drop_cached_exec_routes_for_sandbox_unlocked(sandbox_id)

    def delete_sandboxes_for_jobs(self, job_ids: Iterable[str]) -> list[SandboxRoute]:
        return self.delete_sandboxes_for_jobs_with_error(job_ids)

    def delete_sandboxes_for_jobs_with_error(
        self,
        job_ids: Iterable[str],
        *,
        terminal_error: str = "",
    ) -> list[SandboxRoute]:
        """Forget non-portable sandboxes owned by terminated VM jobs.

        A fully published storage-native parked route is intentionally retained:
        it is content-addressed remote state and can be adopted by another node
        after complete source-node loss.  Every live exec session still belongs
        to the lost process incarnation and is removed.
        """

        target_ids = tuple(sorted({str(job_id) for job_id in job_ids if str(job_id)}))
        if not target_ids:
            return []
        with self._lock:
            removed: list[SandboxRoute] = []
            preserved: list[SandboxRoute] = []
            with self._transaction() as conn:
                for job_id in target_ids:
                    rows = conn.execute(
                        """
                        SELECT sandbox_id, node_id, job_id, node_url,
                               resources_json, spec_json, state, generation,
                               create_operation_id, spec_hash, delete_operation_id,
                               node_epoch, activity_epoch, worker_state,
                               storage_schema,
                               snapshot_manifest_digest, snapshot_repository,
                               snapshot_tag, storage_snapshot_json,
                               created_at, updated_at
                        FROM sandboxes
                        WHERE job_id = ?
                        ORDER BY sandbox_id
                        """,
                        (job_id,),
                    ).fetchall()
                    for row in rows:
                        route = _sandbox_route_from_row(row)
                        if route is not None:
                            if (
                                sandbox_owner_loss_disposition(route)
                                is SandboxOwnerLossDisposition.RECOVER_DETACHED
                            ):
                                preserved.append(route)
                            else:
                                removed.append(route)
                for route in preserved:
                    self._detach_owner_lost_route_unlocked(
                        conn,
                        route,
                        updated_at=utc_now().isoformat(),
                    )
                for route in removed:
                    self._delete_sandbox_unlocked(
                        conn,
                        route,
                        terminal_error=terminal_error,
                    )
            if not removed and not preserved:
                return []
            for route in (*removed, *preserved):
                self._drop_cached_exec_routes_for_sandbox_unlocked(route.sandbox_id)
            return removed

    def delete_stale_sandboxes(
        self,
        *,
        active_job_ids: Iterable[str],
        active_node_ids: Iterable[str] = (),
        older_than: datetime,
    ) -> list[SandboxRoute]:
        keep_jobs = {str(job_id) for job_id in active_job_ids if str(job_id)}
        keep_nodes = {str(node_id) for node_id in active_node_ids if str(node_id)}
        with self._lock:
            removed: list[SandboxRoute] = []
            preserved: list[SandboxRoute] = []
            with self._transaction() as conn:
                rows = conn.execute(
                    """
                    SELECT sandbox_id, node_id, job_id, node_url,
                           resources_json, spec_json, state, generation,
                           create_operation_id, spec_hash, delete_operation_id,
                           node_epoch, activity_epoch, worker_state,
                           storage_schema,
                           snapshot_manifest_digest, snapshot_repository,
                           snapshot_tag, storage_snapshot_json,
                           created_at, updated_at
                    FROM sandboxes
                    ORDER BY sandbox_id
                    """
                ).fetchall()
                for row in rows:
                    route = _sandbox_route_from_row(row)
                    if route is None:
                        continue
                    if route.job_id in keep_jobs or route.node_id in keep_nodes:
                        continue
                    reference = parse_iso_datetime(
                        route.updated_at
                    ) or parse_iso_datetime(route.created_at)
                    if reference is None or reference > older_than:
                        continue
                    if (
                        sandbox_owner_loss_disposition(route)
                        is SandboxOwnerLossDisposition.RECOVER_DETACHED
                    ):
                        preserved.append(route)
                    else:
                        removed.append(route)
                detached_at = utc_now().isoformat()
                for route in preserved:
                    self._detach_owner_lost_route_unlocked(
                        conn,
                        route,
                        updated_at=detached_at,
                    )
                for route in removed:
                    self._delete_sandbox_unlocked(conn, route)
            if not removed and not preserved:
                return []
            for route in (*removed, *preserved):
                self._drop_cached_exec_routes_for_sandbox_unlocked(route.sandbox_id)
            return removed

    def _detach_owner_lost_route_unlocked(
        self,
        conn: sqlite3.Connection,
        route: SandboxRoute,
        *,
        updated_at: str,
    ) -> SandboxRoute:
        """Project a lost worker owner without changing caller policy."""

        detached = replace(
            route,
            worker_state="detached",
            node_epoch="",
            activity_epoch=0,
            updated_at=updated_at,
        )
        self._write_sandbox(conn, detached)
        conn.execute(
            "DELETE FROM exec_sessions WHERE sandbox_id = ?",
            (route.sandbox_id,),
        )
        return detached

    def _delete_sandbox_unlocked(
        self,
        conn: sqlite3.Connection,
        route: SandboxRoute | str,
        *,
        terminal_error: str = "",
    ) -> bool:
        sandbox_id = route.sandbox_id if isinstance(route, SandboxRoute) else route
        if isinstance(route, SandboxRoute):
            removed = conn.execute(
                """
                DELETE FROM sandboxes
                WHERE sandbox_id = ? AND generation = ?
                  AND create_operation_id = ? AND spec_hash = ?
                """,
                (
                    sandbox_id,
                    route.generation,
                    route.create_operation_id,
                    route.spec_hash,
                ),
            ).rowcount
            if not removed:
                return False
        else:
            removed = conn.execute(
                "DELETE FROM sandboxes WHERE sandbox_id = ?", (sandbox_id,)
            ).rowcount
        conn.execute(
            "DELETE FROM sandbox_storage_dependencies WHERE sandbox_id = ?",
            (sandbox_id,),
        )
        conn.execute("DELETE FROM pending WHERE sandbox_id = ?", (sandbox_id,))
        conn.execute("DELETE FROM exec_sessions WHERE sandbox_id = ?", (sandbox_id,))
        conn.execute(
            """
            UPDATE sandbox_migrations
            SET phase = 'complete',
                updated_at = ?,
                error = CASE
                    WHEN error = '' THEN 'sandbox deleted before migration completed'
                    ELSE error
                END
            WHERE sandbox_id = ? AND phase != 'complete'
            """,
            (utc_now().isoformat(), sandbox_id),
        )
        self._terminalize_program_requests_unlocked(
            conn,
            sandbox_id,
            generation=route.generation if isinstance(route, SandboxRoute) else None,
            last_error=terminal_error,
        )
        return bool(removed)

    @staticmethod
    def _terminalize_program_requests_unlocked(
        conn: sqlite3.Connection,
        sandbox_id: str,
        *,
        generation: int | None = None,
        last_error: str = "",
    ) -> int:
        """Retain a bounded terminal projection when its sandbox disappears."""

        generation_clause = ""
        parameters: list[object] = [
            utc_now().isoformat(),
            last_error,
            last_error,
            sandbox_id,
        ]
        if generation is not None:
            generation_clause = " AND sandbox_generation = ?"
            parameters.append(generation)
        return conn.execute(
            f"""
            UPDATE program_requests
            SET state = 'terminal', updated_at = ?,
                last_error = CASE WHEN ? != '' THEN ? ELSE last_error END
            WHERE sandbox_id = ? AND state != 'terminal'
            {generation_clause}
            """,
            parameters,
        ).rowcount

    def get_exec(self, session_id: str) -> ExecRoute | None:
        with self._lock:
            cached = self._exec_route_cache.pop(session_id, None)
            if cached is not None:
                self._exec_route_cache[session_id] = cached
                return cached
            with self._connect() as conn:
                route = self._get_exec_unlocked(conn, session_id)
            if route is not None:
                self._cache_exec_route_unlocked(route)
            return route

    def get_pending(self, sandbox_id: str) -> PendingSandboxDemand | None:
        with self._lock:
            with self._connect() as conn:
                return self._get_pending_unlocked(conn, sandbox_id)

    def upsert_exec(self, route: ExecRoute) -> None:
        with self._lock:
            now = utc_now().isoformat()
            with self._transaction() as conn:
                existing = self._get_exec_unlocked(conn, route.session_id)
                if existing is not None and (
                    existing.sandbox_id != route.sandbox_id
                    or existing.node_id != route.node_id
                    or existing.job_id != route.job_id
                    or existing.node_url != route.node_url
                ):
                    raise SandboxRouteConflictError(
                        "exec session id belongs to another sandbox or worker"
                    )
                stored = ExecRoute(
                    session_id=route.session_id,
                    sandbox_id=route.sandbox_id,
                    node_id=route.node_id,
                    job_id=route.job_id,
                    node_url=route.node_url,
                    created_at=route.created_at
                    or (existing.created_at if existing else now),
                    updated_at=now,
                )
                self._write_exec(conn, stored)
            self._cache_exec_route_unlocked(stored)

    def delete_exec(self, session_id: str) -> ExecRoute | None:
        with self._lock:
            with self._transaction() as conn:
                existing = self._get_exec_unlocked(conn, session_id)
                conn.execute(
                    "DELETE FROM exec_sessions WHERE session_id = ?",
                    (session_id,),
                )
            cached = self._exec_route_cache.pop(session_id, None)
            if cached is not None:
                self._remove_exec_route_from_sandbox_index_unlocked(cached)
            return existing or cached

    def _cache_exec_route_unlocked(self, route: ExecRoute) -> None:
        previous = self._exec_route_cache.pop(route.session_id, None)
        if previous is not None:
            self._remove_exec_route_from_sandbox_index_unlocked(previous)
        self._exec_route_cache[route.session_id] = route
        self._exec_route_cache_sandbox_index.setdefault(
            route.sandbox_id,
            set(),
        ).add(route.session_id)
        while len(self._exec_route_cache) > EXEC_ROUTE_CACHE_MAX_ENTRIES:
            _, evicted = self._exec_route_cache.popitem(last=False)
            self._remove_exec_route_from_sandbox_index_unlocked(evicted)

    def _remove_exec_route_from_sandbox_index_unlocked(
        self,
        route: ExecRoute,
    ) -> None:
        session_ids = self._exec_route_cache_sandbox_index.get(route.sandbox_id)
        if session_ids is None:
            return
        session_ids.discard(route.session_id)
        if not session_ids:
            del self._exec_route_cache_sandbox_index[route.sandbox_id]

    def _drop_cached_exec_routes_for_sandbox_unlocked(
        self,
        sandbox_id: str,
    ) -> None:
        session_ids = self._exec_route_cache_sandbox_index.pop(sandbox_id, set())
        for session_id in session_ids:
            self._exec_route_cache.pop(session_id, None)

    def upsert_pending(
        self,
        sandbox_id: str,
        resources: ResourceQuantity,
        *,
        generation: int = 0,
        operation_id: str = "",
        spec_hash: str = "",
        failure_reason: str = "",
    ) -> None:
        with self._lock:
            now = utc_now()
            with self._transaction() as conn:
                self._upsert_pending_unlocked(
                    conn,
                    sandbox_id,
                    resources,
                    now=now,
                    generation=generation,
                    operation_id=operation_id,
                    spec_hash=spec_hash,
                    failure_reason=failure_reason,
                )

    def upsert_pending_with_demand(
        self,
        sandbox_id: str,
        resources: ResourceQuantity,
        *,
        generation: int = 0,
        operation_id: str = "",
        spec_hash: str = "",
        failure_reason: str = "",
    ) -> tuple[PendingSandboxDemand, SandboxDemand]:
        """Persist pending demand and return aggregate demand in one transaction."""

        with self._lock:
            now_datetime = utc_now()
            with self._transaction() as conn:
                stored = self._upsert_pending_unlocked(
                    conn,
                    sandbox_id,
                    resources,
                    now=now_datetime,
                    generation=generation,
                    operation_id=operation_id,
                    spec_hash=spec_hash,
                    failure_reason=failure_reason,
                )
                self._prune_expired_unlocked(conn, now_datetime)
                return stored, self._sandbox_demand_unlocked(conn, now_datetime)

    def clear_pending(self, sandbox_id: str) -> None:
        with self._lock:
            with self._transaction() as conn:
                conn.execute("DELETE FROM pending WHERE sandbox_id = ?", (sandbox_id,))

    def consume_pending_demand(
        self,
        items: Iterable[PendingSandboxDemand] | None = None,
    ) -> list[PendingSandboxDemand]:
        now = utc_now()
        with self._lock:
            pending = self._active_pending_unlocked(now)
            if not pending:
                return []
            targets = list(items) if items is not None else list(pending.values())
            if not targets:
                return []
            consumed: list[PendingSandboxDemand] = []
            with self._transaction() as conn:
                for item in targets:
                    cursor = conn.execute(
                        """
                        DELETE FROM pending
                        WHERE sandbox_id = ?
                          AND updated_at = ?
                          AND attempts = ?
                        """,
                        (
                            item.sandbox_id,
                            item.updated_at,
                            item.attempts,
                        ),
                    )
                    if cursor.rowcount:
                        consumed.append(item)
            if not consumed:
                return []
            return consumed

    def pending_sandboxes(self) -> list[PendingSandboxDemand]:
        now = utc_now()
        with self._lock:
            return list(self._active_pending_unlocked(now).values())

    def upsert_pending_image_build(self, image_id: str, tag: str) -> None:
        with self._lock:
            now = utc_now().isoformat()
            with self._transaction() as conn:
                existing = self._get_image_build_unlocked(conn, image_id)
                stored = PendingImageBuildDemand(
                    image_id=image_id,
                    tag=tag,
                    created_at=existing.created_at if existing else now,
                    updated_at=now,
                    attempts=(existing.attempts + 1) if existing else 1,
                )
                self._write_image_build(conn, stored)

    def clear_pending_image_build(self, image_id: str) -> None:
        with self._lock:
            with self._transaction() as conn:
                conn.execute("DELETE FROM image_builds WHERE image_id = ?", (image_id,))

    def consume_pending_image_builds(
        self,
        items: Iterable[PendingImageBuildDemand] | None = None,
    ) -> list[PendingImageBuildDemand]:
        now = utc_now()
        with self._lock:
            image_builds = self._active_image_builds_unlocked(now)
            if not image_builds:
                return []
            targets = list(items) if items is not None else list(image_builds.values())
            if not targets:
                return []
            consumed: list[PendingImageBuildDemand] = []
            with self._transaction() as conn:
                for item in targets:
                    cursor = conn.execute(
                        """
                        DELETE FROM image_builds
                        WHERE image_id = ?
                          AND tag = ?
                          AND updated_at = ?
                          AND attempts = ?
                        """,
                        (
                            item.image_id,
                            item.tag,
                            item.updated_at,
                            item.attempts,
                        ),
                    )
                    if cursor.rowcount:
                        consumed.append(item)
            if not consumed:
                return []
            return consumed

    def upsert_image_warmup(
        self,
        warmup_id: str,
        image: str,
        resources: ResourceQuantity,
        *,
        count: int,
        ttl_seconds: int,
        image_id: str = "",
    ) -> PendingImageWarmup:
        cleaned_warmup_id = warmup_id.strip()
        cleaned_image = image.strip()
        if not cleaned_warmup_id:
            raise ValueError("warmup id is required.")
        if not cleaned_image:
            raise ValueError("image is required.")
        with self._lock:
            now = utc_now()
            with self._transaction() as conn:
                existing = self._get_image_warmup_unlocked(conn, cleaned_warmup_id)
                preserve_warmed_nodes = (
                    existing.warmed_node_ids
                    if existing is not None
                    and existing.image == cleaned_image
                    and existing.image_id == image_id.strip()
                    else ()
                )
                stored = PendingImageWarmup(
                    warmup_id=cleaned_warmup_id,
                    image=cleaned_image,
                    image_id=image_id.strip(),
                    resources=resources,
                    count=max(1, count),
                    created_at=existing.created_at if existing else now.isoformat(),
                    updated_at=now.isoformat(),
                    expires_at=(
                        now + timedelta(seconds=max(1, ttl_seconds))
                    ).isoformat(),
                    warmed_node_ids=tuple(dict.fromkeys(preserve_warmed_nodes)),
                    attempts=(existing.attempts + 1) if existing else 1,
                )
                self._write_image_warmup(conn, stored)
            return stored

    def image_warmups(self) -> list[PendingImageWarmup]:
        now = utc_now()
        with self._lock:
            return list(self._active_image_warmups_unlocked(now).values())

    def mark_image_warmup_node(
        self,
        warmup_id: str,
        node_id: str,
        *,
        expected_image: str = "",
        expected_image_id: str = "",
    ) -> PendingImageWarmup | None:
        cleaned_warmup_id = warmup_id.strip()
        cleaned_node_id = node_id.strip()
        if not cleaned_warmup_id or not cleaned_node_id:
            return None
        with self._lock:
            with self._transaction() as conn:
                existing = self._get_image_warmup_unlocked(conn, cleaned_warmup_id)
                if existing is None:
                    return None
                if expected_image and existing.image != expected_image.strip():
                    return None
                if expected_image_id and existing.image_id != expected_image_id.strip():
                    return None
                now = utc_now().isoformat()
                stored = replace(
                    existing,
                    updated_at=now,
                    warmed_node_ids=tuple(
                        dict.fromkeys((*existing.warmed_node_ids, cleaned_node_id))
                    ),
                )
                self._write_image_warmup(conn, stored)
            return stored

    def delete_image_warmup(self, warmup_id: str) -> PendingImageWarmup | None:
        cleaned_warmup_id = warmup_id.strip()
        if not cleaned_warmup_id:
            return None
        with self._lock:
            with self._transaction() as conn:
                existing = self._get_image_warmup_unlocked(conn, cleaned_warmup_id)
                conn.execute(
                    "DELETE FROM image_warmups WHERE warmup_id = ?",
                    (cleaned_warmup_id,),
                )
            return existing

    def upsert_prepared_capacity(
        self,
        prepare_id: str,
        resources: ResourceQuantity,
        *,
        count: int,
        ttl_seconds: int,
        image: str = "",
    ) -> PreparedCapacityDemand:
        if isinstance(count, bool) or not isinstance(count, int):
            raise ValueError("prepared capacity count must be an integer")
        if not 1 <= count <= MAX_PREPARED_CAPACITY_COUNT:
            raise ValueError(
                "prepared capacity count must be between 1 and "
                f"{MAX_PREPARED_CAPACITY_COUNT}"
            )
        with self._lock:
            now = utc_now()
            with self._transaction() as conn:
                existing = self._get_prepared_unlocked(conn, prepare_id)
                stored = PreparedCapacityDemand(
                    prepare_id=prepare_id,
                    resources=resources,
                    count=count,
                    created_at=existing.created_at if existing else now.isoformat(),
                    updated_at=now.isoformat(),
                    expires_at=(
                        now + timedelta(seconds=max(1, ttl_seconds))
                    ).isoformat(),
                    image=image.strip(),
                )
                self._write_prepared(conn, stored)
            return stored

    def delete_prepared_capacity(
        self, prepare_id: str
    ) -> PreparedCapacityDemand | None:
        with self._lock:
            with self._transaction() as conn:
                existing = self._get_prepared_unlocked(conn, prepare_id)
                conn.execute(
                    "DELETE FROM prepared_capacity WHERE prepare_id = ?",
                    (prepare_id,),
                )
                conn.execute(
                    "DELETE FROM image_warmups WHERE warmup_id = ?",
                    (prepare_id,),
                )
            return existing

    def prepared_capacity(self) -> list[PreparedCapacityDemand]:
        now = utc_now()
        with self._lock:
            prepared = self._active_prepared_unlocked(now)
            return list(prepared.values())

    def upsert_prepared_builder(
        self,
        prepare_id: str,
        *,
        count: int,
        ttl_seconds: int,
    ) -> PreparedBuilderDemand:
        with self._lock:
            now = utc_now()
            with self._transaction() as conn:
                existing = self._get_prepared_builder_unlocked(conn, prepare_id)
                stored = PreparedBuilderDemand(
                    prepare_id=prepare_id,
                    count=max(1, count),
                    created_at=existing.created_at if existing else now.isoformat(),
                    updated_at=now.isoformat(),
                    expires_at=(
                        now + timedelta(seconds=max(1, ttl_seconds))
                    ).isoformat(),
                )
                self._write_prepared_builder(conn, stored)
            return stored

    def delete_prepared_builder(self, prepare_id: str) -> PreparedBuilderDemand | None:
        with self._lock:
            with self._transaction() as conn:
                existing = self._get_prepared_builder_unlocked(conn, prepare_id)
                conn.execute(
                    "DELETE FROM prepared_builders WHERE prepare_id = ?",
                    (prepare_id,),
                )
            return existing

    def prepared_builders(self) -> list[PreparedBuilderDemand]:
        now = utc_now()
        with self._lock:
            prepared_builders = self._active_prepared_builders_unlocked(now)
            return list(prepared_builders.values())

    def consume_prepared_builders(
        self,
        items: Iterable[PreparedBuilderDemand] | None = None,
    ) -> list[PreparedBuilderDemand]:
        now = utc_now()
        with self._lock:
            prepared_builders = self._active_prepared_builders_unlocked(now)
            if not prepared_builders:
                return []
            targets = (
                list(items) if items is not None else list(prepared_builders.values())
            )
            if not targets:
                return []
            consumed: list[PreparedBuilderDemand] = []
            with self._transaction() as conn:
                for item in targets:
                    cursor = conn.execute(
                        """
                        DELETE FROM prepared_builders
                        WHERE prepare_id = ?
                          AND count = ?
                          AND updated_at = ?
                          AND expires_at = ?
                        """,
                        (
                            item.prepare_id,
                            item.count,
                            item.updated_at,
                            item.expires_at,
                        ),
                    )
                    if cursor.rowcount:
                        consumed.append(item)
            if not consumed:
                return []
            return consumed

    def prepared_builder_count(self) -> int:
        now = utc_now()
        with self._lock:
            return sum(
                item.count
                for item in self._active_prepared_builders_unlocked(now).values()
            )

    def pending_image_build_count(self) -> int:
        now = utc_now()
        with self._lock:
            return len(self._active_image_builds_unlocked(now))

    def pending_demand(self) -> SandboxDemand:
        with self._lock:
            now = utc_now()
            with self._transaction() as conn:
                self._prune_expired_unlocked(conn, now)
                return self._sandbox_demand_unlocked(conn, now)

    def _active_pending_unlocked(
        self,
        now: datetime,
    ) -> dict[str, PendingSandboxDemand]:
        with self._transaction() as conn:
            items = {
                item.sandbox_id: item
                for item in (
                    _pending_from_row(row)
                    for row in conn.execute(
                        """
                        SELECT sandbox_id, resources_json, created_at, updated_at,
                               attempts, generation, operation_id, spec_hash,
                               failure_reason
                        FROM pending
                        ORDER BY sandbox_id
                        """
                    )
                )
                if item is not None
            }
            expired = [
                sandbox_id for sandbox_id, item in items.items() if item.is_expired(now)
            ]
            conn.executemany(
                "DELETE FROM pending WHERE sandbox_id = ?",
                ((sandbox_id,) for sandbox_id in expired),
            )
        return {
            sandbox_id: item
            for sandbox_id, item in items.items()
            if sandbox_id not in set(expired)
        }

    def _active_image_builds_unlocked(
        self,
        now: datetime,
    ) -> dict[str, PendingImageBuildDemand]:
        with self._transaction() as conn:
            items = {
                item.image_id: item
                for item in (
                    _image_build_from_row(row)
                    for row in conn.execute(
                        """
                        SELECT image_id, tag, created_at, updated_at, attempts
                        FROM image_builds
                        ORDER BY image_id
                        """
                    )
                )
                if item is not None
            }
            expired = [
                image_id for image_id, item in items.items() if item.is_expired(now)
            ]
            conn.executemany(
                "DELETE FROM image_builds WHERE image_id = ?",
                ((image_id,) for image_id in expired),
            )
        return {
            image_id: item
            for image_id, item in items.items()
            if image_id not in set(expired)
        }

    def _active_prepared_unlocked(
        self,
        now: datetime,
    ) -> dict[str, PreparedCapacityDemand]:
        with self._transaction() as conn:
            conn.execute(
                "DELETE FROM prepared_capacity WHERE expires_at <= ?",
                (now.isoformat(),),
            )
            rows = conn.execute(
                """
                SELECT prepare_id, resources_json, count, created_at,
                       updated_at, expires_at, image
                FROM prepared_capacity
                ORDER BY prepare_id
                """
            )
            return {
                item.prepare_id: item
                for item in (_prepared_from_row(row) for row in rows)
                if item is not None
            }

    def _active_prepared_builders_unlocked(
        self,
        now: datetime,
    ) -> dict[str, PreparedBuilderDemand]:
        with self._transaction() as conn:
            conn.execute(
                "DELETE FROM prepared_builders WHERE expires_at <= ?",
                (now.isoformat(),),
            )
            rows = conn.execute(
                """
                SELECT prepare_id, count, created_at, updated_at, expires_at
                FROM prepared_builders
                ORDER BY prepare_id
                """
            )
            return {
                item.prepare_id: item
                for item in (_prepared_builder_from_row(row) for row in rows)
                if item is not None
            }

    def _active_image_warmups_unlocked(
        self,
        now: datetime,
    ) -> dict[str, PendingImageWarmup]:
        with self._transaction() as conn:
            conn.execute(
                "DELETE FROM image_warmups WHERE expires_at <= ?",
                (now.isoformat(),),
            )
            rows = conn.execute(
                """
                SELECT warmup_id, image, image_id, resources_json, count,
                       created_at, updated_at, expires_at,
                       warmed_node_ids_json, attempts
                FROM image_warmups
                ORDER BY warmup_id
                """
            )
            return {
                item.warmup_id: item
                for item in (_image_warmup_from_row(row) for row in rows)
                if item is not None
            }

    def _prune_expired_unlocked(
        self,
        conn: sqlite3.Connection,
        now: datetime,
    ) -> None:
        pending_cutoff = (
            now - timedelta(seconds=PENDING_DEMAND_TTL_SECONDS)
        ).isoformat()
        conn.execute(
            """
            DELETE FROM pending
            WHERE COALESCE(NULLIF(updated_at, ''), created_at) != ''
              AND julianday(COALESCE(NULLIF(updated_at, ''), created_at)) IS NOT NULL
              AND julianday(COALESCE(NULLIF(updated_at, ''), created_at))
                  <= julianday(?)
            """,
            (pending_cutoff,),
        )
        conn.execute(
            """
            DELETE FROM image_builds
            WHERE COALESCE(NULLIF(updated_at, ''), created_at) != ''
              AND julianday(COALESCE(NULLIF(updated_at, ''), created_at)) IS NOT NULL
              AND julianday(COALESCE(NULLIF(updated_at, ''), created_at))
                  <= julianday(?)
            """,
            (pending_cutoff,),
        )
        timestamp = now.isoformat()
        conn.execute(
            "DELETE FROM prepared_capacity WHERE expires_at <= ?",
            (timestamp,),
        )
        conn.execute(
            "DELETE FROM prepared_builders WHERE expires_at <= ?",
            (timestamp,),
        )
        conn.execute(
            "DELETE FROM image_warmups WHERE expires_at <= ?",
            (timestamp,),
        )
        terminal_cutoff = (
            now - timedelta(seconds=PROGRAM_TERMINAL_RETENTION_SECONDS)
        ).isoformat()
        conn.execute(
            """
            DELETE FROM program_requests
            WHERE state = 'terminal' AND updated_at <= ?
            """,
            (terminal_cutoff,),
        )

    def _sandbox_demand_unlocked(
        self,
        conn: sqlite3.Connection,
        now: datetime,
    ) -> SandboxDemand:
        pending = [
            item
            for item in (
                _pending_from_row(row)
                for row in conn.execute(
                    """
                    SELECT sandbox_id, resources_json, created_at, updated_at,
                           attempts, generation, operation_id, spec_hash,
                           failure_reason
                    FROM pending
                    """
                )
            )
            if item is not None and not item.is_expired(now)
        ]
        prepared = [
            item
            for item in (
                _prepared_from_row(row)
                for row in conn.execute(
                    """
                    SELECT prepare_id, resources_json, count, created_at,
                           updated_at, expires_at, image
                    FROM prepared_capacity
                    """
                )
            )
            if item is not None and not item.is_expired(now)
        ]
        pending_resources = ResourceQuantity()
        suppressed_pending_resources = ResourceQuantity()
        pending_count = 0
        suppressed_pending_count = 0
        placement_requests: list[SandboxPlacementRequest] = []
        prepared_placement_requests: list[SandboxPlacementRequest] = []
        oldest_pending_seconds = 0
        for item in pending:
            if not item.is_capacity_demand:
                suppressed_pending_resources = (
                    suppressed_pending_resources + item.resources
                )
                suppressed_pending_count += 1
                continue
            pending_resources = pending_resources + item.resources
            pending_count += 1
            excluded_job_ids: tuple[str, ...] = ()
            for prefix in ("__migration__:", "__wake__:"):
                if item.sandbox_id.startswith(prefix):
                    route = self._get_sandbox_unlocked(
                        conn,
                        item.sandbox_id[len(prefix) :],
                    )
                    if route is not None and route.job_id:
                        excluded_job_ids = (route.job_id,)
                    break
            placement_requests.append(
                SandboxPlacementRequest(
                    resources=item.resources,
                    excluded_job_ids=excluded_job_ids,
                )
            )
            created_at = parse_iso_datetime(item.created_at)
            if created_at is not None:
                oldest_pending_seconds = max(
                    oldest_pending_seconds,
                    int((now - created_at).total_seconds()),
                )
        for item in prepared:
            prepared_placement_requests.append(
                SandboxPlacementRequest(
                    resources=item.resources,
                    count=item.count,
                )
            )
            created_at = parse_iso_datetime(item.created_at)
            if created_at is not None:
                oldest_pending_seconds = max(
                    oldest_pending_seconds,
                    int((now - created_at).total_seconds()),
                )
        return SandboxDemand(
            pending_resources=pending_resources,
            suppressed_pending_resources=suppressed_pending_resources,
            pending_count=pending_count,
            suppressed_pending_count=suppressed_pending_count,
            oldest_pending_seconds=max(0, oldest_pending_seconds),
            placement_requests=tuple(placement_requests),
            prepared_placement_requests=tuple(prepared_placement_requests),
        )

    def _ensure_db(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        has_state = self.path.exists() and self.path.stat().st_size > 0
        if has_state and not _is_sqlite_file(self.path):
            raise sqlite3.DatabaseError(
                f"routing state is not a SQLite database: {self.path}"
            )
        with self._connect() as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=30000")
            schema_version = int(conn.execute("PRAGMA user_version").fetchone()[0])
            if has_state and schema_version not in {2, ROUTING_SCHEMA_VERSION}:
                raise sqlite3.DatabaseError(
                    "unsupported routing schema version "
                    f"{schema_version}; expected {ROUTING_SCHEMA_VERSION}"
                )
            if has_state and schema_version == 2:
                conn.execute(
                    """
                    ALTER TABLE sandboxes ADD COLUMN worker_state TEXT NOT NULL
                    DEFAULT 'attached' CHECK (
                        worker_state IN ('attached', 'detaching', 'detached')
                    )
                    """
                )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS sandboxes (
                    sandbox_id TEXT PRIMARY KEY CHECK (length(trim(sandbox_id)) > 0),
                    node_id TEXT NOT NULL CHECK (length(trim(node_id)) > 0),
                    job_id TEXT NOT NULL CHECK (length(trim(job_id)) > 0),
                    node_url TEXT NOT NULL CHECK (length(trim(node_url)) > 0),
                    resources_json TEXT NOT NULL,
                    spec_json TEXT NOT NULL,
                    state TEXT NOT NULL CHECK (length(trim(state)) > 0),
                    generation INTEGER NOT NULL CHECK (generation > 0),
                    create_operation_id TEXT NOT NULL
                        CHECK (length(create_operation_id) BETWEEN 1 AND 128),
                    spec_hash TEXT NOT NULL CHECK (
                        length(spec_hash) = 64
                        AND spec_hash NOT GLOB '*[^0-9a-f]*'
                    ),
                    delete_operation_id TEXT NOT NULL DEFAULT '',
                    node_epoch TEXT NOT NULL DEFAULT '',
                    activity_epoch INTEGER NOT NULL DEFAULT 0
                        CHECK (activity_epoch >= 0),
                    worker_state TEXT NOT NULL DEFAULT 'attached' CHECK (
                        worker_state IN ('attached', 'detaching', 'detached')
                    ),
                    storage_schema TEXT NOT NULL DEFAULT '',
                    snapshot_manifest_digest TEXT NOT NULL DEFAULT '',
                    snapshot_repository TEXT NOT NULL DEFAULT '',
                    snapshot_tag TEXT NOT NULL DEFAULT '',
                    storage_snapshot_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL CHECK (length(trim(created_at)) > 0),
                    updated_at TEXT NOT NULL CHECK (length(trim(updated_at)) > 0)
                ) STRICT
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS sandbox_storage_dependencies (
                    sandbox_id TEXT PRIMARY KEY,
                    generation INTEGER NOT NULL,
                    storage_snapshot_json TEXT NOT NULL
                ) STRICT
                """
            )
            # Upgrade existing parked routes before a wake can clear their
            # restore metadata. Dependency ownership lasts until route deletion
            # or replacement by a newer, complete layer publication.
            conn.execute(
                """
                INSERT OR IGNORE INTO sandbox_storage_dependencies
                SELECT sandbox_id, generation, storage_snapshot_json
                FROM sandboxes WHERE storage_snapshot_json != '{}'
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS sandbox_generation_hwm (
                    sandbox_id TEXT PRIMARY KEY,
                    generation INTEGER NOT NULL CHECK (generation > 0)
                ) STRICT
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS sandboxes_node_id
                ON sandboxes(node_id)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS sandboxes_job_id
                ON sandboxes(job_id)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS sandboxes_node_url
                ON sandboxes(node_url)
                """
            )
            conn.execute(
                """
                INSERT INTO sandbox_generation_hwm (sandbox_id, generation)
                SELECT sandbox_id, MAX(generation) FROM sandboxes GROUP BY sandbox_id
                ON CONFLICT(sandbox_id) DO UPDATE SET generation =
                    MAX(sandbox_generation_hwm.generation, excluded.generation)
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS exec_sessions (
                    session_id TEXT PRIMARY KEY,
                    sandbox_id TEXT NOT NULL,
                    node_id TEXT NOT NULL,
                    job_id TEXT NOT NULL,
                    node_url TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS managed_processes (
                    sandbox_id TEXT PRIMARY KEY,
                    sandbox_generation INTEGER NOT NULL,
                    job_id TEXT NOT NULL,
                    spec_sha256 TEXT NOT NULL,
                    record_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS managed_process_identity
                ON managed_processes (
                    sandbox_id, sandbox_generation, job_id, spec_sha256
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS sandbox_migrations (
                    migration_id TEXT PRIMARY KEY,
                    sandbox_id TEXT NOT NULL,
                    phase TEXT NOT NULL,
                    source_node_id TEXT NOT NULL,
                    source_job_id TEXT NOT NULL,
                    source_node_url TEXT NOT NULL,
                    destination_node_id TEXT NOT NULL,
                    destination_job_id TEXT NOT NULL,
                    destination_node_url TEXT NOT NULL,
                    generation INTEGER NOT NULL,
                    create_operation_id TEXT NOT NULL,
                    spec_hash TEXT NOT NULL,
                    storage_schema TEXT NOT NULL DEFAULT '',
                    snapshot_sha256 TEXT NOT NULL DEFAULT '',
                    storage_snapshot_json TEXT NOT NULL DEFAULT '{}',
                    source_fenced INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    error TEXT NOT NULL DEFAULT ''
                )
                """
            )
            conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS
                    sandbox_migrations_active_sandbox
                ON sandbox_migrations (sandbox_id)
                WHERE phase != 'complete'
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS pending (
                    sandbox_id TEXT PRIMARY KEY,
                    resources_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    attempts INTEGER NOT NULL,
                    generation INTEGER NOT NULL DEFAULT 0,
                    operation_id TEXT NOT NULL DEFAULT '',
                    spec_hash TEXT NOT NULL DEFAULT '',
                    failure_reason TEXT NOT NULL DEFAULT ''
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS image_builds (
                    image_id TEXT PRIMARY KEY,
                    tag TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    attempts INTEGER NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS prepared_capacity (
                    prepare_id TEXT PRIMARY KEY,
                    resources_json TEXT NOT NULL,
                    count INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    image TEXT NOT NULL DEFAULT ''
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS prepared_builders (
                    prepare_id TEXT PRIMARY KEY,
                    count INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS image_warmups (
                    warmup_id TEXT PRIMARY KEY,
                    image TEXT NOT NULL,
                    image_id TEXT NOT NULL DEFAULT '',
                    resources_json TEXT NOT NULL,
                    count INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    warmed_node_ids_json TEXT NOT NULL DEFAULT '[]',
                    attempts INTEGER NOT NULL DEFAULT 1
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS program_requests (
                    request_id TEXT PRIMARY KEY,
                    rollout_id TEXT NOT NULL,
                    sandbox_id TEXT NOT NULL,
                    sandbox_generation INTEGER NOT NULL,
                    state TEXT NOT NULL,
                    resources_json TEXT NOT NULL,
                    accepted_at TEXT NOT NULL DEFAULT '',
                    parked_at TEXT NOT NULL DEFAULT '',
                    response_ready_at TEXT NOT NULL DEFAULT '',
                    wake_started_at TEXT NOT NULL DEFAULT '',
                    wake_completed_at TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL,
                    last_error TEXT NOT NULL DEFAULT ''
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS program_requests_state_updated
                ON program_requests(state, updated_at)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS program_requests_rollout
                ON program_requests(rollout_id, updated_at)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS program_requests_sandbox
                ON program_requests(sandbox_id, sandbox_generation)
                """
            )
            conn.execute(f"PRAGMA user_version={ROUTING_SCHEMA_VERSION}")
            conn.commit()

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.path, timeout=30)
        _chmod_sqlite_state_files(self.path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        try:
            yield conn
        finally:
            conn.close()
            _chmod_sqlite_state_files(self.path)

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                yield conn
            except BaseException:
                conn.rollback()
                raise
            else:
                conn.commit()

    def _load_unlocked(
        self,
        conn: sqlite3.Connection,
        *,
        include_exec_sessions: bool = True,
    ) -> RoutingState:
        return RoutingState(
            sandboxes={
                route.sandbox_id: route
                for route in (
                    _sandbox_route_from_row(row)
                    for row in conn.execute(
                        """
                        SELECT sandbox_id, node_id, job_id, node_url,
                               resources_json, spec_json, state, generation,
                               create_operation_id, spec_hash, delete_operation_id,
                               node_epoch, activity_epoch, worker_state,
                               storage_schema,
                               snapshot_manifest_digest, snapshot_repository,
                               snapshot_tag, storage_snapshot_json,
                               created_at, updated_at
                        FROM sandboxes
                        ORDER BY sandbox_id
                        """
                    )
                )
                if route is not None
            },
            exec_sessions=(
                {
                    route.session_id: route
                    for route in (
                        _exec_route_from_row(row)
                        for row in conn.execute(
                            """
                            SELECT session_id, sandbox_id, node_id, job_id, node_url,
                                   created_at, updated_at
                            FROM exec_sessions
                            ORDER BY session_id
                            """
                        )
                    )
                    if route is not None
                }
                if include_exec_sessions
                else {}
            ),
            pending={
                item.sandbox_id: item
                for item in (
                    _pending_from_row(row)
                    for row in conn.execute(
                        """
                        SELECT sandbox_id, resources_json, created_at, updated_at,
                               attempts, generation, operation_id, spec_hash,
                               failure_reason
                        FROM pending
                        ORDER BY sandbox_id
                        """
                    )
                )
                if item is not None
            },
            image_builds={
                item.image_id: item
                for item in (
                    _image_build_from_row(row)
                    for row in conn.execute(
                        """
                        SELECT image_id, tag, created_at, updated_at, attempts
                        FROM image_builds
                        ORDER BY image_id
                        """
                    )
                )
                if item is not None
            },
            prepared={
                item.prepare_id: item
                for item in (
                    _prepared_from_row(row)
                    for row in conn.execute(
                        """
                        SELECT prepare_id, resources_json, count, created_at,
                               updated_at, expires_at, image
                        FROM prepared_capacity
                        ORDER BY prepare_id
                        """
                    )
                )
                if item is not None
            },
            prepared_builders={
                item.prepare_id: item
                for item in (
                    _prepared_builder_from_row(row)
                    for row in conn.execute(
                        """
                        SELECT prepare_id, count, created_at, updated_at,
                               expires_at
                        FROM prepared_builders
                        ORDER BY prepare_id
                        """
                    )
                )
                if item is not None
            },
            image_warmups={
                item.warmup_id: item
                for item in (
                    _image_warmup_from_row(row)
                    for row in conn.execute(
                        """
                        SELECT warmup_id, image, image_id, resources_json, count,
                               created_at, updated_at, expires_at,
                               warmed_node_ids_json, attempts
                        FROM image_warmups
                        ORDER BY warmup_id
                        """
                    )
                )
                if item is not None
            },
        )

    def _sandbox_routes_for_node_url_unlocked(
        self,
        conn: sqlite3.Connection,
        node_url: str,
    ) -> list[SandboxRoute]:
        return [
            route
            for route in (
                _sandbox_route_from_row(row)
                for row in conn.execute(
                    """
                    SELECT sandbox_id, node_id, job_id, node_url,
                           resources_json, spec_json, state, generation,
                           create_operation_id, spec_hash, delete_operation_id,
                           node_epoch, activity_epoch, worker_state,
                           storage_schema,
                           snapshot_manifest_digest, snapshot_repository,
                           snapshot_tag, storage_snapshot_json,
                           created_at, updated_at
                    FROM sandboxes
                    WHERE node_url = ?
                    ORDER BY sandbox_id
                    """,
                    (node_url,),
                )
            )
            if route is not None
        ]

    def _get_sandbox_unlocked(
        self,
        conn: sqlite3.Connection,
        sandbox_id: str,
    ) -> SandboxRoute | None:
        row = conn.execute(
            """
            SELECT sandbox_id, node_id, job_id, node_url, resources_json, spec_json, state,
                   generation, create_operation_id, spec_hash, delete_operation_id,
                   node_epoch, activity_epoch, worker_state, storage_schema,
                   snapshot_manifest_digest, snapshot_repository,
                   snapshot_tag, storage_snapshot_json, created_at, updated_at
            FROM sandboxes
            WHERE sandbox_id = ?
            """,
            (sandbox_id,),
        ).fetchone()
        return _sandbox_route_from_row(row) if row is not None else None

    def _get_exec_unlocked(
        self,
        conn: sqlite3.Connection,
        session_id: str,
    ) -> ExecRoute | None:
        row = conn.execute(
            """
            SELECT session_id, sandbox_id, node_id, job_id, node_url,
                   created_at, updated_at
            FROM exec_sessions
            WHERE session_id = ?
            """,
            (session_id,),
        ).fetchone()
        return _exec_route_from_row(row) if row is not None else None

    def _get_sandbox_migration_unlocked(
        self,
        conn: sqlite3.Connection,
        migration_id: str,
    ) -> SandboxMigration | None:
        row = conn.execute(
            """
            SELECT migration_id, sandbox_id, phase,
                   source_node_id, source_job_id, source_node_url,
                   destination_node_id, destination_job_id,
                   destination_node_url, generation, create_operation_id,
                   spec_hash, storage_schema, snapshot_sha256,
                   storage_snapshot_json,
                   source_fenced,
                   created_at, updated_at, error
            FROM sandbox_migrations
            WHERE migration_id = ?
            """,
            (migration_id,),
        ).fetchone()
        return _sandbox_migration_from_row(row) if row is not None else None

    def _get_pending_unlocked(
        self,
        conn: sqlite3.Connection,
        sandbox_id: str,
    ) -> PendingSandboxDemand | None:
        row = conn.execute(
            """
            SELECT sandbox_id, resources_json, created_at, updated_at, attempts,
                   generation, operation_id, spec_hash, failure_reason
            FROM pending
            WHERE sandbox_id = ?
            """,
            (sandbox_id,),
        ).fetchone()
        return _pending_from_row(row) if row is not None else None

    def _upsert_pending_unlocked(
        self,
        conn: sqlite3.Connection,
        sandbox_id: str,
        resources: ResourceQuantity,
        *,
        now: datetime,
        generation: int,
        operation_id: str,
        spec_hash: str,
        failure_reason: str,
    ) -> PendingSandboxDemand:
        timestamp = now.isoformat()
        existing = self._get_pending_unlocked(conn, sandbox_id)
        same_incarnation = bool(
            existing is not None
            and existing.generation == max(0, generation)
            and existing.operation_id == operation_id
            and existing.spec_hash == spec_hash
        )
        stored = PendingSandboxDemand(
            sandbox_id=sandbox_id,
            resources=resources,
            created_at=(
                existing.created_at if same_incarnation and existing else timestamp
            ),
            updated_at=timestamp,
            attempts=(existing.attempts + 1) if same_incarnation and existing else 1,
            generation=max(0, generation),
            operation_id=operation_id.strip(),
            spec_hash=spec_hash.strip(),
            failure_reason=failure_reason.strip(),
        )
        self._write_pending(conn, stored)
        return stored

    def _get_image_build_unlocked(
        self,
        conn: sqlite3.Connection,
        image_id: str,
    ) -> PendingImageBuildDemand | None:
        row = conn.execute(
            """
            SELECT image_id, tag, created_at, updated_at, attempts
            FROM image_builds
            WHERE image_id = ?
            """,
            (image_id,),
        ).fetchone()
        return _image_build_from_row(row) if row is not None else None

    def _get_prepared_unlocked(
        self,
        conn: sqlite3.Connection,
        prepare_id: str,
    ) -> PreparedCapacityDemand | None:
        row = conn.execute(
            """
            SELECT prepare_id, resources_json, count, created_at,
                   updated_at, expires_at, image
            FROM prepared_capacity
            WHERE prepare_id = ?
            """,
            (prepare_id,),
        ).fetchone()
        return _prepared_from_row(row) if row is not None else None

    def _get_prepared_builder_unlocked(
        self,
        conn: sqlite3.Connection,
        prepare_id: str,
    ) -> PreparedBuilderDemand | None:
        row = conn.execute(
            """
            SELECT prepare_id, count, created_at, updated_at, expires_at
            FROM prepared_builders
            WHERE prepare_id = ?
            """,
            (prepare_id,),
        ).fetchone()
        return _prepared_builder_from_row(row) if row is not None else None

    def _get_image_warmup_unlocked(
        self,
        conn: sqlite3.Connection,
        warmup_id: str,
    ) -> PendingImageWarmup | None:
        row = conn.execute(
            """
            SELECT warmup_id, image, image_id, resources_json, count,
                   created_at, updated_at, expires_at,
                   warmed_node_ids_json, attempts
            FROM image_warmups
            WHERE warmup_id = ?
            """,
            (warmup_id,),
        ).fetchone()
        return _image_warmup_from_row(row) if row is not None else None

    def _write_sandbox(self, conn: sqlite3.Connection, route: SandboxRoute) -> None:
        conn.execute(
            """
            INSERT INTO sandboxes (
                sandbox_id, node_id, job_id, node_url, resources_json, spec_json, state,
                generation, create_operation_id, spec_hash, delete_operation_id,
                node_epoch, activity_epoch, worker_state, storage_schema,
                snapshot_manifest_digest, snapshot_repository, snapshot_tag,
                storage_snapshot_json, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(sandbox_id) DO UPDATE SET
                node_id = excluded.node_id,
                job_id = excluded.job_id,
                node_url = excluded.node_url,
                resources_json = excluded.resources_json,
                spec_json = excluded.spec_json,
                state = excluded.state,
                generation = excluded.generation,
                create_operation_id = excluded.create_operation_id,
                spec_hash = excluded.spec_hash,
                delete_operation_id = excluded.delete_operation_id,
                node_epoch = excluded.node_epoch,
                activity_epoch = excluded.activity_epoch,
                worker_state = excluded.worker_state,
                storage_schema = excluded.storage_schema,
                snapshot_manifest_digest = excluded.snapshot_manifest_digest,
                snapshot_repository = excluded.snapshot_repository,
                snapshot_tag = excluded.snapshot_tag,
                storage_snapshot_json = excluded.storage_snapshot_json,
                created_at = excluded.created_at,
                updated_at = excluded.updated_at
            """,
            (
                route.sandbox_id,
                route.node_id,
                route.job_id,
                route.node_url,
                _resources_json(route.resources),
                _object_json(route.spec),
                route.state,
                route.generation,
                route.create_operation_id,
                route.spec_hash,
                route.delete_operation_id,
                route.node_epoch,
                max(0, route.activity_epoch),
                route.worker_state,
                route.storage_schema,
                route.snapshot_manifest_digest,
                route.snapshot_repository,
                route.snapshot_tag,
                _object_json(route.storage_snapshot),
                route.created_at,
                route.updated_at,
            ),
        )
        conn.execute(
            "DELETE FROM sandbox_storage_dependencies WHERE sandbox_id = ? AND generation != ?",
            (route.sandbox_id, route.generation),
        )
        if route.storage_snapshot:
            conn.execute(
                """
                INSERT INTO sandbox_storage_dependencies VALUES (?, ?, ?)
                ON CONFLICT(sandbox_id) DO UPDATE SET
                    generation = excluded.generation,
                    storage_snapshot_json = excluded.storage_snapshot_json
                """,
                (
                    route.sandbox_id,
                    route.generation,
                    _object_json(route.storage_snapshot),
                ),
            )
        conn.execute(
            """
            INSERT INTO sandbox_generation_hwm (sandbox_id, generation)
            VALUES (?, ?)
            ON CONFLICT(sandbox_id) DO UPDATE SET generation =
                MAX(sandbox_generation_hwm.generation, excluded.generation)
            """,
            (route.sandbox_id, route.generation),
        )

    def _write_exec(self, conn: sqlite3.Connection, route: ExecRoute) -> None:
        conn.execute(
            """
            INSERT INTO exec_sessions (
                session_id, sandbox_id, node_id, job_id, node_url,
                created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(session_id) DO UPDATE SET
                sandbox_id = excluded.sandbox_id,
                node_id = excluded.node_id,
                job_id = excluded.job_id,
                node_url = excluded.node_url,
                created_at = excluded.created_at,
                updated_at = excluded.updated_at
            """,
            (
                route.session_id,
                route.sandbox_id,
                route.node_id,
                route.job_id,
                route.node_url,
                route.created_at,
                route.updated_at,
            ),
        )

    def _write_sandbox_migration(
        self,
        conn: sqlite3.Connection,
        migration: SandboxMigration,
    ) -> None:
        conn.execute(
            """
            INSERT INTO sandbox_migrations (
                migration_id, sandbox_id, phase,
                source_node_id, source_job_id, source_node_url,
                destination_node_id, destination_job_id,
                destination_node_url, generation, create_operation_id,
                spec_hash, storage_schema, snapshot_sha256,
                storage_snapshot_json,
                source_fenced,
                created_at, updated_at, error
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(migration_id) DO UPDATE SET
                sandbox_id = excluded.sandbox_id,
                phase = excluded.phase,
                source_node_id = excluded.source_node_id,
                source_job_id = excluded.source_job_id,
                source_node_url = excluded.source_node_url,
                destination_node_id = excluded.destination_node_id,
                destination_job_id = excluded.destination_job_id,
                destination_node_url = excluded.destination_node_url,
                generation = excluded.generation,
                create_operation_id = excluded.create_operation_id,
                spec_hash = excluded.spec_hash,
                storage_schema = excluded.storage_schema,
                snapshot_sha256 = excluded.snapshot_sha256,
                storage_snapshot_json = excluded.storage_snapshot_json,
                source_fenced = excluded.source_fenced,
                created_at = excluded.created_at,
                updated_at = excluded.updated_at,
                error = excluded.error
            """,
            (
                migration.migration_id,
                migration.sandbox_id,
                migration.phase,
                migration.source_node_id,
                migration.source_job_id,
                migration.source_node_url,
                migration.destination_node_id,
                migration.destination_job_id,
                migration.destination_node_url,
                migration.generation,
                migration.create_operation_id,
                migration.spec_hash,
                migration.storage_schema,
                migration.snapshot_sha256,
                json.dumps(
                    migration.storage_snapshot,
                    ensure_ascii=True,
                    separators=(",", ":"),
                    sort_keys=True,
                ),
                int(migration.source_fenced),
                migration.created_at,
                migration.updated_at,
                migration.error,
            ),
        )

    def _write_pending(
        self,
        conn: sqlite3.Connection,
        item: PendingSandboxDemand,
    ) -> None:
        conn.execute(
            """
            INSERT INTO pending (
                sandbox_id, resources_json, created_at, updated_at, attempts,
                generation, operation_id, spec_hash, failure_reason
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(sandbox_id) DO UPDATE SET
                resources_json = excluded.resources_json,
                created_at = excluded.created_at,
                updated_at = excluded.updated_at,
                attempts = excluded.attempts,
                generation = excluded.generation,
                operation_id = excluded.operation_id,
                spec_hash = excluded.spec_hash,
                failure_reason = excluded.failure_reason
            """,
            (
                item.sandbox_id,
                _resources_json(item.resources),
                item.created_at,
                item.updated_at,
                item.attempts,
                item.generation,
                item.operation_id,
                item.spec_hash,
                item.failure_reason,
            ),
        )

    def _write_image_build(
        self,
        conn: sqlite3.Connection,
        item: PendingImageBuildDemand,
    ) -> None:
        conn.execute(
            """
            INSERT INTO image_builds (
                image_id, tag, created_at, updated_at, attempts
            )
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(image_id) DO UPDATE SET
                tag = excluded.tag,
                created_at = excluded.created_at,
                updated_at = excluded.updated_at,
                attempts = excluded.attempts
            """,
            (
                item.image_id,
                item.tag,
                item.created_at,
                item.updated_at,
                item.attempts,
            ),
        )

    def _write_prepared(
        self,
        conn: sqlite3.Connection,
        item: PreparedCapacityDemand,
    ) -> None:
        conn.execute(
            """
            INSERT INTO prepared_capacity (
                prepare_id, resources_json, count, created_at, updated_at,
                expires_at, image
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(prepare_id) DO UPDATE SET
                resources_json = excluded.resources_json,
                count = excluded.count,
                created_at = excluded.created_at,
                updated_at = excluded.updated_at,
                expires_at = excluded.expires_at,
                image = excluded.image
            """,
            (
                item.prepare_id,
                _resources_json(item.resources),
                item.count,
                item.created_at,
                item.updated_at,
                item.expires_at,
                item.image,
            ),
        )

    def _write_prepared_builder(
        self,
        conn: sqlite3.Connection,
        item: PreparedBuilderDemand,
    ) -> None:
        conn.execute(
            """
            INSERT INTO prepared_builders (
                prepare_id, count, created_at, updated_at, expires_at
            )
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(prepare_id) DO UPDATE SET
                count = excluded.count,
                created_at = excluded.created_at,
                updated_at = excluded.updated_at,
                expires_at = excluded.expires_at
            """,
            (
                item.prepare_id,
                item.count,
                item.created_at,
                item.updated_at,
                item.expires_at,
            ),
        )

    def _write_image_warmup(
        self,
        conn: sqlite3.Connection,
        item: PendingImageWarmup,
    ) -> None:
        conn.execute(
            """
            INSERT INTO image_warmups (
                warmup_id, image, image_id, resources_json, count, created_at,
                updated_at, expires_at, warmed_node_ids_json, attempts
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(warmup_id) DO UPDATE SET
                image = excluded.image,
                image_id = excluded.image_id,
                resources_json = excluded.resources_json,
                count = excluded.count,
                created_at = excluded.created_at,
                updated_at = excluded.updated_at,
                expires_at = excluded.expires_at,
                warmed_node_ids_json = excluded.warmed_node_ids_json,
                attempts = excluded.attempts
            """,
            (
                item.warmup_id,
                item.image,
                item.image_id,
                _resources_json(item.resources),
                item.count,
                item.created_at,
                item.updated_at,
                item.expires_at,
                json.dumps(list(item.warmed_node_ids), sort_keys=True),
                item.attempts,
            ),
        )


def _chmod_sqlite_state_files(path: Path) -> None:
    for candidate in (path, Path(f"{path}-wal"), Path(f"{path}-shm")):
        try:
            os.chmod(candidate, 0o600)
        except FileNotFoundError:
            continue


def _string(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None


def _nonnegative_int(value: object) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError, OverflowError):
        return 0


def _route_update_is_current(
    existing: SandboxRoute,
    candidate: SandboxRoute,
    *,
    allow_node_epoch_adoption: bool = True,
) -> bool:
    if not _same_sandbox_route_incarnation(existing, candidate):
        return False
    # Exact incarnation identity on the same assigned node proves that the
    # route belongs to the new guest boot. Activity counters are boot-scoped
    # and cannot be compared across that boundary, so permit epoch adoption.
    if candidate.node_epoch and candidate.node_epoch != existing.node_epoch:
        return allow_node_epoch_adoption
    if (
        existing.node_epoch == candidate.node_epoch
        and candidate.activity_epoch < existing.activity_epoch
    ):
        return False
    return True


def _same_sandbox_route_incarnation(
    existing: SandboxRoute,
    candidate: SandboxRoute,
) -> bool:
    return bool(
        existing.sandbox_id == candidate.sandbox_id
        and existing.generation == candidate.generation
        and existing.create_operation_id == candidate.create_operation_id
        and existing.spec_hash == candidate.spec_hash
        and existing.node_id == candidate.node_id
        and existing.job_id == candidate.job_id
        and _normalized_route_node_url(existing.node_url)
        == _normalized_route_node_url(candidate.node_url)
    )


def _normalized_route_node_url(node_url: str) -> str:
    return node_url.strip().rstrip("/")


def _object(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def sandbox_demand_from_routing_state(
    state: RoutingState,
    *,
    now: datetime | None = None,
) -> SandboxDemand:
    if now is None:
        now = utc_now()
    pending_total = ResourceQuantity()
    suppressed_pending_total = ResourceQuantity()
    pending_count = 0
    suppressed_pending_count = 0
    placement_requests: list[SandboxPlacementRequest] = []
    prepared_placement_requests: list[SandboxPlacementRequest] = []
    oldest_pending_seconds = 0
    for item in state.pending.values():
        if item.is_expired(now):
            continue
        if not item.is_capacity_demand:
            suppressed_pending_total = suppressed_pending_total + item.resources
            suppressed_pending_count += 1
            continue
        pending_total = pending_total + item.resources
        pending_count += 1
        excluded_job_ids: tuple[str, ...] = ()
        for prefix in ("__migration__:", "__wake__:"):
            if item.sandbox_id.startswith(prefix):
                route = state.sandboxes.get(item.sandbox_id[len(prefix) :])
                if route is not None and route.job_id:
                    excluded_job_ids = (route.job_id,)
                break
        placement_requests.append(
            SandboxPlacementRequest(
                resources=item.resources,
                excluded_job_ids=excluded_job_ids,
            )
        )
        created_at = parse_iso_datetime(item.created_at)
        if created_at is not None:
            oldest_pending_seconds = max(
                oldest_pending_seconds,
                int((now - created_at).total_seconds()),
            )
    for item in state.prepared.values():
        if item.is_expired(now):
            continue
        prepared_placement_requests.append(
            SandboxPlacementRequest(
                resources=item.resources,
                count=item.count,
            )
        )
        created_at = parse_iso_datetime(item.created_at)
        if created_at is not None:
            oldest_pending_seconds = max(
                oldest_pending_seconds,
                int((now - created_at).total_seconds()),
            )
    return SandboxDemand(
        pending_resources=pending_total,
        suppressed_pending_resources=suppressed_pending_total,
        pending_count=pending_count,
        suppressed_pending_count=suppressed_pending_count,
        oldest_pending_seconds=max(0, oldest_pending_seconds),
        placement_requests=tuple(placement_requests),
        prepared_placement_requests=tuple(prepared_placement_requests),
    )


def _route_lock(path: Path) -> RLock:
    with _ROUTE_LOCKS_GUARD:
        return _ROUTE_LOCKS[path.resolve()]


def _exec_route_cache(path: Path) -> OrderedDict[str, ExecRoute]:
    with _ROUTE_LOCKS_GUARD:
        return _EXEC_ROUTE_CACHES[path.resolve()]


def _exec_route_cache_sandbox_index(path: Path) -> dict[str, set[str]]:
    with _ROUTE_LOCKS_GUARD:
        return _EXEC_ROUTE_CACHE_SANDBOX_INDEXES[path.resolve()]


def _is_sqlite_file(path: Path) -> bool:
    try:
        with path.open("rb") as file:
            header = file.read(16)
    except OSError:
        return False
    return header == b"SQLite format 3\x00"


def _resources_json(resources: ResourceQuantity) -> str:
    normalized = ResourceQuantity.from_dict(resources.to_dict())
    return json.dumps(normalized.to_dict(), sort_keys=True, separators=(",", ":"))


def _resources_from_json(raw: object) -> ResourceQuantity:
    if not isinstance(raw, str):
        return ResourceQuantity()
    try:
        return ResourceQuantity.from_dict(json.loads(raw))
    except json.JSONDecodeError:
        return ResourceQuantity()


def _object_json(raw: dict[str, Any]) -> str:
    return json.dumps(raw, sort_keys=True, separators=(",", ":"))


def _sandbox_route_from_row(row: sqlite3.Row) -> SandboxRoute:
    try:
        values = dict(row)
        values["resources"] = ResourceQuantity.from_dict(
            json.loads(values.pop("resources_json"))
        )
        values["spec"] = json.loads(values.pop("spec_json"))
        values["storage_snapshot"] = json.loads(values.pop("storage_snapshot_json"))
        if not isinstance(values["spec"], dict) or not isinstance(
            values["storage_snapshot"], dict
        ):
            raise ValueError("route JSON fields must be objects")
        return SandboxRoute(**values)
    except (
        AttributeError,
        json.JSONDecodeError,
        KeyError,
        TypeError,
        ValueError,
    ) as exc:
        raise sqlite3.DatabaseError("invalid sandbox route row") from exc


def _program_request_from_row(row: sqlite3.Row) -> ProgramRequestState:
    state = str(row["state"])
    if state not in _PROGRAM_STATE_RANK:
        state = "terminal"
    return ProgramRequestState(
        request_id=str(row["request_id"]),
        rollout_id=str(row["rollout_id"]),
        sandbox_id=str(row["sandbox_id"]),
        sandbox_generation=max(0, int(row["sandbox_generation"])),
        state=state,
        resources=_resources_from_json(row["resources_json"]),
        accepted_at=str(row["accepted_at"] or ""),
        parked_at=str(row["parked_at"] or ""),
        response_ready_at=str(row["response_ready_at"] or ""),
        wake_started_at=str(row["wake_started_at"] or ""),
        wake_completed_at=str(row["wake_completed_at"] or ""),
        updated_at=str(row["updated_at"] or ""),
        last_error=str(row["last_error"] or ""),
    )


def _exec_route_from_row(row: sqlite3.Row) -> ExecRoute:
    return ExecRoute(
        session_id=str(row["session_id"]),
        sandbox_id=str(row["sandbox_id"]),
        node_id=str(row["node_id"]),
        job_id=str(row["job_id"]),
        node_url=str(row["node_url"]),
        created_at=str(row["created_at"]),
        updated_at=str(row["updated_at"]),
    )


def _sandbox_migration_from_row(row: sqlite3.Row) -> SandboxMigration:
    return SandboxMigration(
        migration_id=str(row["migration_id"]),
        sandbox_id=str(row["sandbox_id"]),
        phase=str(row["phase"]),
        source_node_id=str(row["source_node_id"]),
        source_job_id=str(row["source_job_id"]),
        source_node_url=str(row["source_node_url"]),
        destination_node_id=str(row["destination_node_id"]),
        destination_job_id=str(row["destination_job_id"]),
        destination_node_url=str(row["destination_node_url"]),
        generation=max(0, int(row["generation"])),
        create_operation_id=str(row["create_operation_id"]),
        spec_hash=str(row["spec_hash"]),
        storage_schema=str(row["storage_schema"]),
        snapshot_sha256=str(row["snapshot_sha256"]),
        storage_snapshot=_object(json.loads(str(row["storage_snapshot_json"]))),
        source_fenced=bool(row["source_fenced"]),
        created_at=str(row["created_at"]),
        updated_at=str(row["updated_at"]),
        error=str(row["error"]),
    )


def _pending_from_row(row: sqlite3.Row) -> PendingSandboxDemand:
    return PendingSandboxDemand(
        sandbox_id=str(row["sandbox_id"]),
        resources=_resources_from_json(row["resources_json"]),
        created_at=str(row["created_at"]),
        updated_at=str(row["updated_at"]),
        attempts=max(1, int(row["attempts"])),
        generation=max(0, int(row["generation"])),
        operation_id=str(row["operation_id"]),
        spec_hash=str(row["spec_hash"]),
        failure_reason=str(row["failure_reason"]),
    )


def _image_build_from_row(row: sqlite3.Row) -> PendingImageBuildDemand:
    return PendingImageBuildDemand(
        image_id=str(row["image_id"]),
        tag=str(row["tag"]),
        created_at=str(row["created_at"]),
        updated_at=str(row["updated_at"]),
        attempts=max(1, int(row["attempts"])),
    )


def _prepared_from_row(row: sqlite3.Row) -> PreparedCapacityDemand:
    return PreparedCapacityDemand(
        prepare_id=str(row["prepare_id"]),
        resources=_resources_from_json(row["resources_json"]),
        count=int(row["count"]),
        created_at=str(row["created_at"]),
        updated_at=str(row["updated_at"]),
        expires_at=str(row["expires_at"]),
        image=str(row["image"] or ""),
    )


def _prepared_builder_from_row(row: sqlite3.Row) -> PreparedBuilderDemand:
    return PreparedBuilderDemand(
        prepare_id=str(row["prepare_id"]),
        count=max(1, int(row["count"])),
        created_at=str(row["created_at"]),
        updated_at=str(row["updated_at"]),
        expires_at=str(row["expires_at"]),
    )


def _image_warmup_from_row(row: sqlite3.Row) -> PendingImageWarmup:
    try:
        warmed_raw = json.loads(str(row["warmed_node_ids_json"] or "[]"))
    except json.JSONDecodeError:
        warmed_raw = []
    warmed_node_ids = (
        tuple(str(item) for item in warmed_raw if str(item))
        if isinstance(warmed_raw, list)
        else ()
    )
    return PendingImageWarmup(
        warmup_id=str(row["warmup_id"]),
        image=str(row["image"]),
        image_id=str(row["image_id"] or ""),
        resources=_resources_from_json(row["resources_json"]),
        count=max(1, int(row["count"])),
        created_at=str(row["created_at"]),
        updated_at=str(row["updated_at"]),
        expires_at=str(row["expires_at"]),
        warmed_node_ids=tuple(dict.fromkeys(warmed_node_ids)),
        attempts=max(1, int(row["attempts"])),
    )


def _oldest_seconds(timestamps: list[str]) -> int:
    now = utc_now()
    oldest_pending_seconds = 0
    for timestamp in timestamps:
        created_at = parse_iso_datetime(timestamp)
        if created_at is not None:
            oldest_pending_seconds = max(
                oldest_pending_seconds,
                int((now - created_at).total_seconds()),
            )
    return max(0, oldest_pending_seconds)
