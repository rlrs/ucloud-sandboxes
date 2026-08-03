from __future__ import annotations

from collections import OrderedDict
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import sqlite3
from threading import RLock
from typing import Any, Iterable, Iterator
from uuid import uuid4

from .models import (
    ResourceQuantity,
    SandboxDemand,
    SandboxPlacementRequest,
    parse_iso_datetime,
    utc_now,
)
from .managed_process import ManagedProcessRecord


_ROUTE_LOCKS_GUARD = RLock()
_ROUTE_LOCKS: dict[Path, RLock] = {}
_EXEC_ROUTE_CACHES: dict[Path, OrderedDict[str, ExecRoute]] = {}
_EXEC_ROUTE_CACHE_SANDBOX_INDEXES: dict[Path, dict[str, set[str]]] = {}
PENDING_DEMAND_TTL_SECONDS = 300
EXEC_ROUTE_CACHE_MAX_ENTRIES = 65_536
PROGRAM_TERMINAL_RETENTION_SECONDS = 7 * 24 * 60 * 60
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


@dataclass(frozen=True)
class SandboxRoute:
    sandbox_id: str
    node_id: str
    job_id: str
    node_url: str
    resources: ResourceQuantity = ResourceQuantity()
    spec: dict[str, Any] = field(default_factory=dict)
    state: str = "unknown"
    generation: int = 0
    create_operation_id: str = ""
    spec_hash: str = ""
    delete_operation_id: str = ""
    node_epoch: str = ""
    activity_epoch: int = 0
    storage_schema: str = ""
    snapshot_manifest_digest: str = ""
    snapshot_repository: str = ""
    snapshot_tag: str = ""
    storage_snapshot: dict[str, Any] = field(default_factory=dict)
    created_at: str = ""
    updated_at: str = ""

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "SandboxRoute | None":
        sandbox_id = _string(raw.get("sandbox_id") or raw.get("sandboxId"))
        node_url = _string(raw.get("node_url") or raw.get("nodeUrl"))
        if not sandbox_id or not node_url:
            return None
        return cls(
            sandbox_id=sandbox_id,
            node_id=_string(raw.get("node_id") or raw.get("nodeId")) or "",
            job_id=_string(raw.get("job_id") or raw.get("jobId")) or "",
            node_url=node_url,
            resources=ResourceQuantity.from_dict(raw.get("resources")),
            spec=_object(raw.get("spec")),
            state=_string(raw.get("state")) or "unknown",
            generation=_nonnegative_int(raw.get("generation")),
            create_operation_id=_string(
                raw.get("create_operation_id") or raw.get("createOperationId")
            )
            or "",
            spec_hash=_string(raw.get("spec_hash") or raw.get("specHash")) or "",
            delete_operation_id=_string(
                raw.get("delete_operation_id") or raw.get("deleteOperationId")
            )
            or "",
            node_epoch=_string(raw.get("node_epoch") or raw.get("nodeEpoch")) or "",
            activity_epoch=_nonnegative_int(
                raw.get("activity_epoch") or raw.get("activityEpoch")
            ),
            storage_schema=_string(
                raw.get("storage_schema") or raw.get("storageSchema")
            )
            or "",
            snapshot_manifest_digest=_string(
                raw.get("snapshot_manifest_digest")
                or raw.get("snapshotManifestDigest")
            )
            or "",
            snapshot_repository=_string(
                raw.get("snapshot_repository") or raw.get("snapshotRepository")
            )
            or "",
            snapshot_tag=_string(
                raw.get("snapshot_tag") or raw.get("snapshotTag")
            )
            or "",
            storage_snapshot=_object(
                raw.get("storage_snapshot") or raw.get("storageSnapshot")
            ),
            created_at=_string(raw.get("created_at") or raw.get("createdAt")) or "",
            updated_at=_string(raw.get("updated_at") or raw.get("updatedAt")) or "",
        )

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
            "snapshot_manifest_digest": self.snapshot_manifest_digest,
            "snapshot_repository": self.snapshot_repository,
            "snapshot_tag": self.snapshot_tag,
            "storage_snapshot": dict(self.storage_snapshot),
            "storage_schema": self.storage_schema,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


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

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "ExecRoute | None":
        session_id = _string(raw.get("session_id") or raw.get("sessionId"))
        node_url = _string(raw.get("node_url") or raw.get("nodeUrl"))
        if not session_id or not node_url:
            return None
        return cls(
            session_id=session_id,
            sandbox_id=_string(raw.get("sandbox_id") or raw.get("sandboxId")) or "",
            node_id=_string(raw.get("node_id") or raw.get("nodeId")) or "",
            job_id=_string(raw.get("job_id") or raw.get("jobId")) or "",
            node_url=node_url,
            created_at=_string(raw.get("created_at") or raw.get("createdAt")) or "",
            updated_at=_string(raw.get("updated_at") or raw.get("updatedAt")) or "",
        )

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
    archive_sha256: str = ""
    archive_token: str = ""
    storage_schema: str = ""
    snapshot_sha256: str = ""
    storage_snapshot: dict[str, Any] = field(default_factory=dict)
    source_fenced: bool = False
    created_at: str = ""
    updated_at: str = ""
    error: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "archive_sha256": self.archive_sha256,
            "archive_token": self.archive_token,
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


@dataclass(frozen=True)
class PendingSandboxDemand:
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
        )

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "PendingSandboxDemand | None":
        sandbox_id = _string(raw.get("sandbox_id") or raw.get("sandboxId"))
        if not sandbox_id:
            return None
        return cls(
            sandbox_id=sandbox_id,
            resources=ResourceQuantity.from_dict(raw.get("resources")),
            created_at=_string(raw.get("created_at") or raw.get("createdAt")) or "",
            updated_at=_string(raw.get("updated_at") or raw.get("updatedAt")) or "",
            attempts=max(1, int(raw.get("attempts") or 1)),
            generation=_nonnegative_int(raw.get("generation")),
            operation_id=_string(
                raw.get("operation_id") or raw.get("operationId")
            )
            or "",
            spec_hash=_string(raw.get("spec_hash") or raw.get("specHash")) or "",
            failure_reason=_string(
                raw.get("failure_reason") or raw.get("failureReason")
            )
            or "",
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
class PendingImageBuildDemand:
    image_id: str
    tag: str
    created_at: str
    updated_at: str
    attempts: int = 1

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "PendingImageBuildDemand | None":
        image_id = _string(raw.get("image_id") or raw.get("imageId"))
        tag = _string(raw.get("tag"))
        if not image_id:
            return None
        return cls(
            image_id=image_id,
            tag=tag or "",
            created_at=_string(raw.get("created_at") or raw.get("createdAt")) or "",
            updated_at=_string(raw.get("updated_at") or raw.get("updatedAt")) or "",
            attempts=max(1, int(raw.get("attempts") or 1)),
        )

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

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "PendingImageWarmup | None":
        warmup_id = _string(raw.get("warmup_id") or raw.get("warmupId") or raw.get("id"))
        image = _string(raw.get("image"))
        if not warmup_id or not image:
            return None
        raw_warmed = raw.get("warmed_node_ids") or raw.get("warmedNodeIds") or ()
        warmed_node_ids = (
            tuple(str(item) for item in raw_warmed if str(item))
            if isinstance(raw_warmed, list)
            else ()
        )
        return cls(
            warmup_id=warmup_id,
            image=image,
            image_id=_string(raw.get("image_id") or raw.get("imageId")) or "",
            resources=ResourceQuantity.from_dict(raw.get("resources")),
            count=max(1, int(raw.get("count") or 1)),
            created_at=_string(raw.get("created_at") or raw.get("createdAt")) or "",
            updated_at=_string(raw.get("updated_at") or raw.get("updatedAt")) or "",
            expires_at=_string(raw.get("expires_at") or raw.get("expiresAt")) or "",
            warmed_node_ids=tuple(dict.fromkeys(warmed_node_ids)),
            attempts=max(1, int(raw.get("attempts") or 1)),
        )

    @property
    def total_resources(self) -> ResourceQuantity:
        return ResourceQuantity(
            vcpu=self.resources.vcpu * self.count,
            memory_mb=self.resources.memory_mb * self.count,
            disk_mb=self.resources.disk_mb * self.count,
        )

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

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "PreparedCapacityDemand | None":
        prepare_id = _string(
            raw.get("prepare_id") or raw.get("prepareId") or raw.get("id")
        )
        if not prepare_id:
            return None
        return cls(
            prepare_id=prepare_id,
            resources=ResourceQuantity.from_dict(raw.get("resources")),
            count=max(1, int(raw.get("count") or 1)),
            created_at=_string(raw.get("created_at") or raw.get("createdAt")) or "",
            updated_at=_string(raw.get("updated_at") or raw.get("updatedAt")) or "",
            expires_at=_string(raw.get("expires_at") or raw.get("expiresAt")) or "",
            image=_string(raw.get("image")) or "",
        )

    @property
    def total_resources(self) -> ResourceQuantity:
        return ResourceQuantity(
            vcpu=self.resources.vcpu * self.count,
            memory_mb=self.resources.memory_mb * self.count,
            disk_mb=self.resources.disk_mb * self.count,
        )

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

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "PreparedBuilderDemand | None":
        prepare_id = _string(
            raw.get("prepare_id") or raw.get("prepareId") or raw.get("id")
        )
        if not prepare_id:
            return None
        return cls(
            prepare_id=prepare_id,
            count=max(1, int(raw.get("count") or 1)),
            created_at=_string(raw.get("created_at") or raw.get("createdAt")) or "",
            updated_at=_string(raw.get("updated_at") or raw.get("updatedAt")) or "",
            expires_at=_string(raw.get("expires_at") or raw.get("expiresAt")) or "",
        )

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

    def save(self, state: RoutingState) -> None:
        with self._lock:
            with self._transaction() as conn:
                conn.execute("DELETE FROM sandboxes")
                conn.execute("DELETE FROM exec_sessions")
                conn.execute("DELETE FROM pending")
                conn.execute("DELETE FROM image_builds")
                conn.execute("DELETE FROM prepared_capacity")
                conn.execute("DELETE FROM prepared_builders")
                conn.execute("DELETE FROM image_warmups")
                for route in state.sandboxes.values():
                    self._write_sandbox(conn, route)
                for route in state.exec_sessions.values():
                    self._write_exec(conn, route)
                for item in state.pending.values():
                    self._write_pending(conn, item)
                for item in state.image_builds.values():
                    self._write_image_build(conn, item)
                for item in state.prepared.values():
                    self._write_prepared(conn, item)
                for item in state.prepared_builders.values():
                    self._write_prepared_builder(conn, item)
                for item in state.image_warmups.values():
                    self._write_image_warmup(conn, item)
            self._exec_route_cache.clear()
            self._exec_route_cache_sandbox_index.clear()
            for route in state.exec_sessions.values():
                self._cache_exec_route_unlocked(route)

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
                if current_route is None or current_route.generation != route.generation:
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
                    if (
                        existing.sandbox_generation == record.sandbox_generation
                        and (
                            existing.job_id != record.job_id
                            or existing.spec_sha256 != record.spec_sha256
                        )
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
                               node_epoch, activity_epoch, storage_schema,
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

    def upsert_program_request_transition(
        self,
        route: SandboxRoute,
        *,
        request_id: str,
        rollout_id: str,
        state: str,
        transition_at: str | None = None,
        accepted_at: str | None = None,
        last_error: str = "",
    ) -> ProgramRequestState:
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
                    and _PROGRAM_STATE_RANK[existing.state]
                    > _PROGRAM_STATE_RANK[state]
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
                transition_field = {
                    "model_wait": "parked_at",
                    "ready_to_wake": "response_ready_at",
                    "waking": "wake_started_at",
                    "acting": "wake_completed_at",
                }.get(state)
                if transition_field and not timestamps[transition_field]:
                    timestamps[transition_field] = transition_at
                error = last_error or (existing.last_error if existing else "")
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
                return _program_request_from_row(row)

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
        storage_schema: str | None = None,
        snapshot_manifest_digest: str | None = None,
        snapshot_repository: str | None = None,
        snapshot_tag: str | None = None,
        storage_snapshot: dict[str, Any] | None = None,
    ) -> SandboxRoute | None:
        """Change only the state of the exact routed sandbox incarnation."""

        expected = {
            str(item or "unknown").strip().lower() for item in expected_states
        }
        cleaned_state = str(state).strip()
        if not expected or not cleaned_state:
            raise ValueError("expected and destination sandbox states are required")
        with self._lock:
            with self._transaction() as conn:
                current = self._get_sandbox_unlocked(conn, route.sandbox_id)
                if (
                    current is None
                    or (current.state or "unknown").lower() not in expected
                    or current.generation != route.generation
                    or current.create_operation_id != route.create_operation_id
                    or current.spec_hash != route.spec_hash
                    or current.node_id != route.node_id
                    or current.job_id != route.job_id
                    or current.node_url.rstrip("/") != route.node_url.rstrip("/")
                    or bool(current.delete_operation_id)
                ):
                    return None
                stored = replace(
                    current,
                    state=cleaned_state,
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
                        current.snapshot_tag
                        if snapshot_tag is None
                        else snapshot_tag
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

    def upsert_sandbox(self, route: SandboxRoute) -> SandboxRoute:
        with self._lock:
            now = utc_now().isoformat()
            with self._transaction() as conn:
                existing = self._get_sandbox_unlocked(conn, route.sandbox_id)
                if existing is not None and not _route_update_is_current(existing, route):
                    return existing
                adopts_node_epoch = bool(
                    existing is not None
                    and route.node_epoch
                    and route.node_epoch != existing.node_epoch
                )
                stored = SandboxRoute(
                    sandbox_id=route.sandbox_id,
                    node_id=route.node_id,
                    job_id=route.job_id,
                    node_url=route.node_url,
                    resources=route.resources,
                    spec=dict(route.spec)
                    or (dict(existing.spec) if existing is not None else {}),
                    state=route.state
                    if route.state != "unknown" or existing is None
                    else existing.state,
                    generation=route.generation
                    if route.generation > 0 or existing is None
                    else existing.generation,
                    create_operation_id=route.create_operation_id
                    or (existing.create_operation_id if existing is not None else ""),
                    spec_hash=route.spec_hash
                    or (existing.spec_hash if existing is not None else ""),
                    delete_operation_id=route.delete_operation_id
                    or (existing.delete_operation_id if existing is not None else ""),
                    node_epoch=route.node_epoch
                    or (existing.node_epoch if existing is not None else ""),
                    activity_epoch=(
                        max(0, route.activity_epoch)
                        if adopts_node_epoch
                        else max(
                            route.activity_epoch,
                            existing.activity_epoch if existing is not None else 0,
                        )
                    ),
                    storage_schema=route.storage_schema
                    or (existing.storage_schema if existing is not None else ""),
                    snapshot_manifest_digest=route.snapshot_manifest_digest
                    or (
                        existing.snapshot_manifest_digest
                        if existing is not None
                        else ""
                    ),
                    snapshot_repository=route.snapshot_repository
                    or (
                        existing.snapshot_repository
                        if existing is not None
                        else ""
                    ),
                    snapshot_tag=route.snapshot_tag
                    or (existing.snapshot_tag if existing is not None else ""),
                    storage_snapshot=dict(route.storage_snapshot)
                    or (
                        dict(existing.storage_snapshot)
                        if existing is not None
                        else {}
                    ),
                    created_at=route.created_at
                    or (existing.created_at if existing else now),
                    updated_at=now,
                )
                self._write_sandbox(conn, stored)
                conn.execute(
                    "DELETE FROM pending WHERE sandbox_id = ?", (route.sandbox_id,)
                )
            return stored

    def finalize_sandbox_create(self, route: SandboxRoute) -> SandboxRoute | None:
        """Finalize an exact create reservation unless DELETE already won.

        A long-running create/fork may complete after a concurrent delete has
        marked or removed its route.  This compare-and-swap prevents the late
        success response from clearing that delete intent or resurrecting the
        deleted route.
        """

        with self._lock:
            now = utc_now().isoformat()
            with self._transaction() as conn:
                existing = self._get_sandbox_unlocked(conn, route.sandbox_id)
                if (
                    existing is None
                    or existing.generation != route.generation
                    or existing.create_operation_id != route.create_operation_id
                    or existing.spec_hash != route.spec_hash
                    or existing.node_id != route.node_id
                    or existing.node_url != route.node_url
                    or bool(existing.delete_operation_id)
                ):
                    return None
                stored = replace(
                    route,
                    delete_operation_id="",
                    node_epoch=existing.node_epoch,
                    activity_epoch=max(
                        existing.activity_epoch,
                        route.activity_epoch,
                    ),
                    created_at=existing.created_at or route.created_at,
                    updated_at=now,
                )
                self._write_sandbox(conn, stored)
                conn.execute(
                    "DELETE FROM pending WHERE sandbox_id = ?",
                    (route.sandbox_id,),
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

    def allocate_sandbox_create(
        self,
        route: SandboxRoute,
        *,
        spec_hash: str,
        create_operation_id: str | None = None,
    ) -> SandboxRoute:
        """Persist a new route incarnation before its node create is dispatched."""

        stored, _pending = self.allocate_sandbox_create_with_pending(
            route,
            spec_hash=spec_hash,
            create_operation_id=create_operation_id,
        )
        return stored

    def allocate_sandbox_create_with_pending(
        self,
        route: SandboxRoute,
        *,
        spec_hash: str,
        create_operation_id: str | None = None,
    ) -> tuple[SandboxRoute, PendingSandboxDemand | None]:
        """Allocate a route and atomically return the demand it consumed."""

        operation_id = (create_operation_id or f"create-{uuid4().hex}").strip()
        if not operation_id or not spec_hash.strip():
            raise ValueError("create operation id and spec hash are required")
        with self._lock:
            now = utc_now().isoformat()
            with self._transaction() as conn:
                pending = self._get_pending_unlocked(conn, route.sandbox_id)
                existing = self._get_sandbox_unlocked(conn, route.sandbox_id)
                if existing is not None:
                    if (
                        (existing.spec_hash and existing.spec_hash != spec_hash)
                        or (
                            existing.spec
                            and route.spec
                            and existing.spec != route.spec
                        )
                    ):
                        raise SandboxRouteConflictError(
                            f"sandbox route already exists with a different spec: "
                            f"{route.sandbox_id}"
                        )
                    return existing, pending
                row = conn.execute(
                    "SELECT generation FROM sandbox_generation_hwm WHERE sandbox_id = ?",
                    (route.sandbox_id,),
                ).fetchone()
                high_water = int(row["generation"]) if row is not None else 0
                generation = high_water + 1
                stored = SandboxRoute(
                    sandbox_id=route.sandbox_id,
                    node_id=route.node_id,
                    job_id=route.job_id,
                    node_url=route.node_url,
                    resources=route.resources,
                    spec=dict(route.spec),
                    state="creating",
                    generation=generation,
                    create_operation_id=operation_id,
                    spec_hash=spec_hash.strip(),
                    node_epoch=route.node_epoch,
                    activity_epoch=max(0, route.activity_epoch),
                    created_at=route.created_at or now,
                    updated_at=now,
                )
                self._write_sandbox(conn, stored)
                conn.execute(
                    "DELETE FROM pending WHERE sandbox_id = ?", (route.sandbox_id,)
                )
                self._claim_prepared_capacity_unlocked(conn, stored)
            return stored, pending

    def allocate_sandbox_creates(
        self,
        requests: Iterable[tuple[SandboxRoute, str, str]],
    ) -> tuple[tuple[SandboxRoute, bool], ...]:
        """Persist a set of create reservations in one SQLite transaction.

        The boolean paired with each route is true only when this call created
        that reservation.  Existing routes are accepted solely as exact
        idempotent replays; any conflict aborts the entire batch.
        """

        requested = tuple(requests)
        if not requested:
            raise ValueError("at least one sandbox create reservation is required")
        sandbox_ids = [route.sandbox_id for route, _hash, _operation in requested]
        if len(set(sandbox_ids)) != len(sandbox_ids):
            raise ValueError("sandbox create reservation ids must be unique")
        for _route, spec_hash, operation_id in requested:
            if not operation_id.strip() or not spec_hash.strip():
                raise ValueError("create operation id and spec hash are required")

        with self._lock:
            now = utc_now().isoformat()
            allocated: list[tuple[SandboxRoute, bool]] = []
            with self._transaction() as conn:
                for route, spec_hash, operation_id in requested:
                    normalized_hash = spec_hash.strip()
                    normalized_operation = operation_id.strip()
                    existing = self._get_sandbox_unlocked(conn, route.sandbox_id)
                    if existing is not None:
                        if (
                            existing.spec_hash != normalized_hash
                            or existing.create_operation_id != normalized_operation
                            or (existing.spec and route.spec and existing.spec != route.spec)
                        ):
                            raise SandboxRouteConflictError(
                                "sandbox route already exists with a different "
                                f"operation: {route.sandbox_id}"
                            )
                        allocated.append((existing, False))
                        continue

                    row = conn.execute(
                        "SELECT generation FROM sandbox_generation_hwm "
                        "WHERE sandbox_id = ?",
                        (route.sandbox_id,),
                    ).fetchone()
                    high_water = int(row["generation"]) if row is not None else 0
                    stored = SandboxRoute(
                        sandbox_id=route.sandbox_id,
                        node_id=route.node_id,
                        job_id=route.job_id,
                        node_url=route.node_url,
                        resources=route.resources,
                        spec=dict(route.spec),
                        state="creating",
                        generation=high_water + 1,
                        create_operation_id=normalized_operation,
                        spec_hash=normalized_hash,
                        node_epoch=route.node_epoch,
                        activity_epoch=max(0, route.activity_epoch),
                        created_at=route.created_at or now,
                        updated_at=now,
                    )
                    self._write_sandbox(conn, stored)
                    conn.execute(
                        "DELETE FROM pending WHERE sandbox_id = ?",
                        (route.sandbox_id,),
                    )
                    self._claim_prepared_capacity_unlocked(conn, stored)
                    allocated.append((stored, True))
            return tuple(allocated)

    def prepare_sandbox_delete(self, sandbox_id: str) -> SandboxRoute | None:
        """Persist and reuse one delete operation for the current generation."""

        with self._lock:
            with self._transaction() as conn:
                existing = self._get_sandbox_unlocked(conn, sandbox_id)
                if existing is None:
                    return None
                if existing.delete_operation_id:
                    return existing
                stored = SandboxRoute(
                    **{
                        **existing.__dict__,
                        "delete_operation_id": f"delete-{uuid4().hex}",
                        "updated_at": utc_now().isoformat(),
                    }
                )
                self._write_sandbox(conn, stored)
            return stored

    def move_sandbox_if_current(
        self,
        source: SandboxRoute,
        *,
        destination_node_id: str,
        destination_job_id: str,
        destination_node_url: str,
        destination_node_epoch: str = "",
        destination_activity_epoch: int = 0,
    ) -> SandboxRoute | None:
        """Atomically move one exact sandbox incarnation to another node.

        The sandbox generation and create identity do not change during a
        parked migration. A stale coordinator therefore cannot redirect a
        replacement incarnation or move a route that has already gone
        elsewhere.
        """

        cleaned_url = destination_node_url.strip().rstrip("/")
        if (
            not destination_node_id.strip()
            or not destination_job_id.strip()
            or not cleaned_url
        ):
            raise ValueError("destination node identity is required")
        with self._lock:
            now = utc_now().isoformat()
            with self._transaction() as conn:
                existing = self._get_sandbox_unlocked(conn, source.sandbox_id)
                if (
                    existing is None
                    or existing.generation != source.generation
                    or existing.create_operation_id != source.create_operation_id
                    or existing.spec_hash != source.spec_hash
                    or existing.node_id != source.node_id
                    or existing.job_id != source.job_id
                    or existing.node_url.rstrip("/") != source.node_url.rstrip("/")
                    or bool(existing.delete_operation_id)
                ):
                    return None
                stored = replace(
                    existing,
                    node_id=destination_node_id.strip(),
                    job_id=destination_job_id.strip(),
                    node_url=cleaned_url,
                    state="parked",
                    node_epoch=destination_node_epoch.strip(),
                    activity_epoch=max(0, destination_activity_epoch),
                    updated_at=now,
                )
                self._write_sandbox(conn, stored)
                conn.execute(
                    "DELETE FROM exec_sessions WHERE sandbox_id = ?",
                    (source.sandbox_id,),
                )
            self._drop_cached_exec_routes_for_sandbox_unlocked(source.sandbox_id)
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
                           create_operation_id, spec_hash, archive_sha256,
                           archive_token, storage_schema, snapshot_sha256,
                           storage_snapshot_json, source_fenced,
                           created_at, updated_at, error
                    FROM sandbox_migrations
                    {where}
                    ORDER BY created_at, migration_id
                    """
                )
            ]

    def advance_sandbox_migration(
        self,
        migration_id: str,
        *,
        expected_phases: Iterable[str],
        phase: str,
        archive_sha256: str | None = None,
        archive_token: str | None = None,
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
                    archive_sha256=(
                        archive_sha256
                        if archive_sha256 is not None
                        else existing.archive_sha256
                    ),
                    archive_token=(
                        archive_token
                        if archive_token is not None
                        else existing.archive_token
                    ),
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
                    or current.create_operation_id
                    != migration.create_operation_id
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
                    snapshot_repository = str(
                        publication.get("repository") or ""
                    )
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
            self._drop_cached_exec_routes_for_sandbox_unlocked(
                migration.sandbox_id
            )
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
                conn.execute("DELETE FROM sandboxes WHERE sandbox_id = ?", (sandbox_id,))
                conn.execute("DELETE FROM pending WHERE sandbox_id = ?", (sandbox_id,))
                conn.execute(
                    "DELETE FROM exec_sessions WHERE sandbox_id = ?", (sandbox_id,)
                )
                self._terminalize_program_requests_unlocked(
                    conn,
                    sandbox_id,
                    generation=generation,
                )
            self._drop_cached_exec_routes_for_sandbox_unlocked(sandbox_id)
            return existing

    def reconcile_sandboxes_for_node(
        self,
        node_url: str,
        routes: list[SandboxRoute],
        *,
        observed_at: str,
        node_epoch: str = "",
        activity_epoch: int = 0,
        inventory_complete: bool = True,
    ) -> None:
        node_url = node_url.strip()
        if not node_url:
            return
        # Include rejected/stale observations in this set.  A malformed or
        # delayed report is not evidence of absence, so it must conservatively
        # protect the corresponding route from this reconciliation pass.
        reported_ids = {route.sandbox_id for route in routes}
        observed_at_dt = parse_iso_datetime(observed_at)
        with self._lock:
            removed_sandbox_ids: list[str] = []
            with self._transaction() as conn:
                # BEGIN IMMEDIATE precedes every read in this method.  A second
                # RoutingStore (or process) therefore cannot install a newer
                # incarnation between validation and the writes/deletes below.
                for route in routes:
                    candidate_node_url = route.node_url.strip() or node_url
                    if candidate_node_url != node_url:
                        continue
                    if route.generation > 0 and (
                        not route.create_operation_id or not route.spec_hash
                    ):
                        continue
                    existing = self._get_sandbox_unlocked(conn, route.sandbox_id)
                    observed = SandboxRoute(
                        sandbox_id=route.sandbox_id,
                        node_id=route.node_id,
                        job_id=route.job_id,
                        node_url=candidate_node_url,
                        resources=(
                            route.resources
                            if route.resources != ResourceQuantity() or existing is None
                            else existing.resources
                        ),
                        spec=dict(route.spec)
                        or (dict(existing.spec) if existing is not None else {}),
                        state=(
                            route.state
                            if route.state != "unknown" or existing is None
                            else existing.state
                        ),
                        generation=route.generation,
                        create_operation_id=route.create_operation_id,
                        spec_hash=route.spec_hash,
                        delete_operation_id=(
                            existing.delete_operation_id if existing is not None else ""
                        ),
                        node_epoch=route.node_epoch or node_epoch,
                        # Activity counters are scoped to a node epoch.  Do not
                        # carry the old epoch's high water into a proven restart.
                        activity_epoch=max(route.activity_epoch, activity_epoch),
                        storage_schema=route.storage_schema
                        or (
                            existing.storage_schema
                            if existing is not None
                            else ""
                        ),
                        snapshot_manifest_digest=(
                            route.snapshot_manifest_digest
                            or (
                                existing.snapshot_manifest_digest
                                if existing is not None
                                and route.state.lower() == "parked"
                                else ""
                            )
                        ),
                        snapshot_repository=(
                            route.snapshot_repository
                            or (
                                existing.snapshot_repository
                                if existing is not None
                                and route.state.lower() == "parked"
                                else ""
                            )
                        ),
                        snapshot_tag=(
                            route.snapshot_tag
                            or (
                                existing.snapshot_tag
                                if existing is not None
                                and route.state.lower() == "parked"
                                else ""
                            )
                        ),
                        storage_snapshot=(
                            dict(route.storage_snapshot)
                            or (
                                dict(existing.storage_snapshot)
                                if existing is not None
                                and route.state.lower() == "parked"
                                else {}
                            )
                        ),
                        created_at=route.created_at
                        or (existing.created_at if existing else observed_at),
                        updated_at=observed_at,
                    )
                    if existing is not None and not _route_update_is_current(
                        existing, observed
                    ):
                        continue
                    self._write_sandbox(conn, observed)
                    conn.execute(
                        "DELETE FROM pending WHERE sandbox_id = ?",
                        (observed.sandbox_id,),
                    )

                current = self._load_unlocked(conn)
                for sandbox_id, route in current.sandboxes.items():
                    if route.node_url != node_url or sandbox_id in reported_ids:
                        continue
                    if not inventory_complete:
                        continue
                    if (route.state or "unknown").lower() in {"creating", "unknown"}:
                        # An empty inventory does not distinguish "create never
                        # arrived" from "create is still in progress" with the
                        # current node protocol. Preserve the reservation until a
                        # later generation-aware reconciliation can prove absence.
                        continue
                    if route.node_epoch and route.node_epoch != node_epoch:
                        # Epoch identifiers are opaque. An observation from a
                        # different incarnation cannot order or delete this route.
                        continue
                    if route.activity_epoch > max(0, activity_epoch):
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
                    # Keep the identity predicate even though BEGIN IMMEDIATE
                    # already excludes concurrent writers.  It documents and
                    # enforces that dependent cleanup happens only after the
                    # exact incarnation selected above was removed.
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
                        continue
                    conn.execute(
                        "DELETE FROM pending WHERE sandbox_id = ?", (sandbox_id,)
                    )
                    conn.execute(
                        "DELETE FROM exec_sessions WHERE sandbox_id = ?", (sandbox_id,)
                    )
                    self._terminalize_program_requests_unlocked(
                        conn,
                        sandbox_id,
                        generation=route.generation,
                    )
                    removed_sandbox_ids.append(sandbox_id)
            for sandbox_id in removed_sandbox_ids:
                self._drop_cached_exec_routes_for_sandbox_unlocked(sandbox_id)

    def delete_sandbox(self, sandbox_id: str) -> None:
        with self._lock:
            with self._transaction() as conn:
                conn.execute(
                    "DELETE FROM sandboxes WHERE sandbox_id = ?", (sandbox_id,)
                )
                conn.execute("DELETE FROM pending WHERE sandbox_id = ?", (sandbox_id,))
                conn.execute(
                    "DELETE FROM exec_sessions WHERE sandbox_id = ?", (sandbox_id,)
                )
                self._terminalize_program_requests_unlocked(conn, sandbox_id)
            self._drop_cached_exec_routes_for_sandbox_unlocked(sandbox_id)

    def delete_sandboxes_for_jobs(self, job_ids: Iterable[str]) -> list[SandboxRoute]:
        target_ids = tuple(sorted({str(job_id) for job_id in job_ids if str(job_id)}))
        if not target_ids:
            return []
        with self._lock:
            removed: list[SandboxRoute] = []
            with self._transaction() as conn:
                for job_id in target_ids:
                    rows = conn.execute(
                        """
                        SELECT sandbox_id, node_id, job_id, node_url,
                               resources_json, spec_json, state, generation,
                               create_operation_id, spec_hash, delete_operation_id,
                               node_epoch, activity_epoch, storage_schema,
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
                            removed.append(route)
                for route in removed:
                    conn.execute(
                        "DELETE FROM sandboxes WHERE sandbox_id = ?",
                        (route.sandbox_id,),
                    )
                    conn.execute(
                        "DELETE FROM pending WHERE sandbox_id = ?",
                        (route.sandbox_id,),
                    )
                    conn.execute(
                        "DELETE FROM exec_sessions WHERE sandbox_id = ?",
                        (route.sandbox_id,),
                    )
                    self._terminalize_program_requests_unlocked(
                        conn,
                        route.sandbox_id,
                        generation=route.generation,
                    )
            if not removed:
                return []
            for route in removed:
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
            with self._transaction() as conn:
                rows = conn.execute(
                    """
                    SELECT sandbox_id, node_id, job_id, node_url,
                           resources_json, spec_json, state, generation,
                           create_operation_id, spec_hash, delete_operation_id,
                           node_epoch, activity_epoch, storage_schema,
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
                    removed.append(route)
                for route in removed:
                    conn.execute(
                        "DELETE FROM sandboxes WHERE sandbox_id = ?",
                        (route.sandbox_id,),
                    )
                    conn.execute(
                        "DELETE FROM pending WHERE sandbox_id = ?",
                        (route.sandbox_id,),
                    )
                    conn.execute(
                        "DELETE FROM exec_sessions WHERE sandbox_id = ?",
                        (route.sandbox_id,),
                    )
                    self._terminalize_program_requests_unlocked(
                        conn,
                        route.sandbox_id,
                        generation=route.generation,
                    )
            if not removed:
                return []
            for route in removed:
                self._drop_cached_exec_routes_for_sandbox_unlocked(route.sandbox_id)
            return removed

    @staticmethod
    def _terminalize_program_requests_unlocked(
        conn: sqlite3.Connection,
        sandbox_id: str,
        *,
        generation: int | None = None,
    ) -> int:
        """Retain a bounded terminal projection when its sandbox disappears."""

        generation_clause = ""
        parameters: list[object] = [utc_now().isoformat(), sandbox_id]
        if generation is not None:
            generation_clause = " AND sandbox_generation = ?"
            parameters.append(generation)
        return conn.execute(
            f"""
            UPDATE program_requests
            SET state = 'terminal', updated_at = ?
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
                    expires_at=(now + timedelta(seconds=max(1, ttl_seconds))).isoformat(),
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
        with self._lock:
            now = utc_now()
            with self._transaction() as conn:
                existing = self._get_prepared_unlocked(conn, prepare_id)
                stored = PreparedCapacityDemand(
                    prepare_id=prepare_id,
                    resources=resources,
                    count=max(1, count),
                    created_at=existing.created_at if existing else now.isoformat(),
                    updated_at=now.isoformat(),
                    expires_at=(now + timedelta(seconds=max(1, ttl_seconds))).isoformat(),
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

    def consume_prepared_capacity(
        self,
        items: Iterable[PreparedCapacityDemand] | None = None,
    ) -> list[PreparedCapacityDemand]:
        now = utc_now()
        with self._lock:
            prepared = self._active_prepared_unlocked(now)
            if not prepared:
                return []
            targets = list(items) if items is not None else list(prepared.values())
            if not targets:
                return []
            consumed: list[PreparedCapacityDemand] = []
            with self._transaction() as conn:
                for item in targets:
                    cursor = conn.execute(
                        """
                        DELETE FROM prepared_capacity
                        WHERE prepare_id = ?
                          AND count = ?
                          AND updated_at = ?
                          AND expires_at = ?
                          AND image = ?
                        """,
                        (
                            item.prepare_id,
                            item.count,
                            item.updated_at,
                            item.expires_at,
                            item.image,
                        ),
                    )
                    if cursor.rowcount:
                        consumed.append(item)
            if not consumed:
                return []
            return consumed

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
                    expires_at=(now + timedelta(seconds=max(1, ttl_seconds))).isoformat(),
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

    def oldest_pending_image_build_seconds(self) -> int:
        now = utc_now()
        with self._lock:
            timestamps = [
                item.created_at
                for item in self._active_image_builds_unlocked(now).values()
            ]
        return _oldest_seconds(timestamps)

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
                sandbox_id
                for sandbox_id, item in items.items()
                if item.is_expired(now)
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
        prepared_resources = ResourceQuantity()
        placement_requests: list[SandboxPlacementRequest] = []
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
            prepared_resources = prepared_resources + item.total_resources
            placement_requests.extend(
                SandboxPlacementRequest(resources=item.resources)
                for _ in range(item.count)
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
            prepared_resources=prepared_resources,
            oldest_pending_seconds=max(0, oldest_pending_seconds),
            placement_requests=tuple(placement_requests),
        )

    def _ensure_db(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if (
            self.path.exists()
            and self.path.stat().st_size > 0
            and not _is_sqlite_file(self.path)
        ):
            backup = self.path.with_name(f"{self.path.name}.legacy-{uuid4().hex}")
            self.path.replace(backup)
        with self._connect() as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=30000")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS sandboxes (
                    sandbox_id TEXT PRIMARY KEY,
                    node_id TEXT NOT NULL,
                    job_id TEXT NOT NULL,
                    node_url TEXT NOT NULL,
                    resources_json TEXT NOT NULL,
                    spec_json TEXT NOT NULL DEFAULT '{}',
                    state TEXT NOT NULL DEFAULT 'unknown',
                    generation INTEGER NOT NULL DEFAULT 0,
                    create_operation_id TEXT NOT NULL DEFAULT '',
                    spec_hash TEXT NOT NULL DEFAULT '',
                    delete_operation_id TEXT NOT NULL DEFAULT '',
                    node_epoch TEXT NOT NULL DEFAULT '',
                    activity_epoch INTEGER NOT NULL DEFAULT 0,
                    storage_schema TEXT NOT NULL DEFAULT '',
                    snapshot_manifest_digest TEXT NOT NULL DEFAULT '',
                    snapshot_repository TEXT NOT NULL DEFAULT '',
                    snapshot_tag TEXT NOT NULL DEFAULT '',
                    storage_snapshot_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            self._ensure_column(
                conn, "sandboxes", "spec_json", "TEXT NOT NULL DEFAULT '{}'"
            )
            self._ensure_column(
                conn, "sandboxes", "state", "TEXT NOT NULL DEFAULT 'unknown'"
            )
            for column, definition in (
                ("generation", "INTEGER NOT NULL DEFAULT 0"),
                ("create_operation_id", "TEXT NOT NULL DEFAULT ''"),
                ("spec_hash", "TEXT NOT NULL DEFAULT ''"),
                ("delete_operation_id", "TEXT NOT NULL DEFAULT ''"),
                ("node_epoch", "TEXT NOT NULL DEFAULT ''"),
                ("activity_epoch", "INTEGER NOT NULL DEFAULT 0"),
                ("storage_schema", "TEXT NOT NULL DEFAULT ''"),
                ("snapshot_manifest_digest", "TEXT NOT NULL DEFAULT ''"),
                ("snapshot_repository", "TEXT NOT NULL DEFAULT ''"),
                ("snapshot_tag", "TEXT NOT NULL DEFAULT ''"),
                ("storage_snapshot_json", "TEXT NOT NULL DEFAULT '{}'"),
            ):
                self._ensure_column(conn, "sandboxes", column, definition)
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS sandbox_generation_hwm (
                    sandbox_id TEXT PRIMARY KEY,
                    generation INTEGER NOT NULL
                )
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
                    archive_sha256 TEXT NOT NULL DEFAULT '',
                    archive_token TEXT NOT NULL DEFAULT '',
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
            for column, definition in (
                ("storage_schema", "TEXT NOT NULL DEFAULT ''"),
                ("snapshot_sha256", "TEXT NOT NULL DEFAULT ''"),
                ("storage_snapshot_json", "TEXT NOT NULL DEFAULT '{}'"),
                ("source_fenced", "INTEGER NOT NULL DEFAULT 0"),
            ):
                self._ensure_column(
                    conn,
                    "sandbox_migrations",
                    column,
                    definition,
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
            for column, definition in (
                ("generation", "INTEGER NOT NULL DEFAULT 0"),
                ("operation_id", "TEXT NOT NULL DEFAULT ''"),
                ("spec_hash", "TEXT NOT NULL DEFAULT ''"),
                ("failure_reason", "TEXT NOT NULL DEFAULT ''"),
            ):
                self._ensure_column(conn, "pending", column, definition)
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
            self._ensure_column(
                conn, "prepared_capacity", "image", "TEXT NOT NULL DEFAULT ''"
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
            conn.commit()

    @staticmethod
    def _ensure_column(
        conn: sqlite3.Connection,
        table: str,
        column: str,
        definition: str,
    ) -> None:
        existing = {
            str(row["name"]) for row in conn.execute(f"PRAGMA table_info({table})")
        }
        if column not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

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
                               node_epoch, activity_epoch, storage_schema,
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
    def _get_sandbox_unlocked(
        self,
        conn: sqlite3.Connection,
        sandbox_id: str,
    ) -> SandboxRoute | None:
        row = conn.execute(
            """
            SELECT sandbox_id, node_id, job_id, node_url, resources_json, spec_json, state,
                   generation, create_operation_id, spec_hash, delete_operation_id,
                   node_epoch, activity_epoch, storage_schema,
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
                   spec_hash, archive_sha256, archive_token,
                   storage_schema, snapshot_sha256, storage_snapshot_json,
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
                node_epoch, activity_epoch, storage_schema,
                snapshot_manifest_digest, snapshot_repository, snapshot_tag,
                storage_snapshot_json, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                max(0, route.generation),
                route.create_operation_id,
                route.spec_hash,
                route.delete_operation_id,
                route.node_epoch,
                max(0, route.activity_epoch),
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
            """
            INSERT INTO sandbox_generation_hwm (sandbox_id, generation)
            VALUES (?, ?)
            ON CONFLICT(sandbox_id) DO UPDATE SET generation =
                MAX(sandbox_generation_hwm.generation, excluded.generation)
            """,
            (route.sandbox_id, max(0, route.generation)),
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
                spec_hash, archive_sha256, archive_token,
                storage_schema, snapshot_sha256, storage_snapshot_json,
                source_fenced,
                created_at, updated_at, error
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                archive_sha256 = excluded.archive_sha256,
                archive_token = excluded.archive_token,
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
                migration.archive_sha256,
                migration.archive_token,
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


def _route_update_is_current(existing: SandboxRoute, candidate: SandboxRoute) -> bool:
    if candidate.generation < existing.generation:
        return False
    if candidate.generation > existing.generation:
        return True
    if existing.generation > 0:
        if not existing.create_operation_id or not candidate.create_operation_id:
            return False
        if existing.create_operation_id != candidate.create_operation_id:
            return False
        if not existing.spec_hash or not candidate.spec_hash:
            return False
        if existing.spec_hash != candidate.spec_hash:
            return False
        if existing.node_url and candidate.node_url != existing.node_url:
            return False
        # Exact incarnation identity on the same assigned node proves that the
        # sandbox survived a node-agent restart. Epoch counters cannot be
        # compared across that boundary, so permit adoption of the new epoch.
        if candidate.node_epoch and candidate.node_epoch != existing.node_epoch:
            return True
        if (
            existing.node_epoch == candidate.node_epoch
            and candidate.activity_epoch < existing.activity_epoch
        ):
            return False
    return True


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
    prepared_total = ResourceQuantity()
    placement_requests: list[SandboxPlacementRequest] = []
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
        prepared_total = prepared_total + item.total_resources
        placement_requests.extend(
            SandboxPlacementRequest(resources=item.resources)
            for _ in range(item.count)
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
        prepared_resources=prepared_total,
        oldest_pending_seconds=max(0, oldest_pending_seconds),
        placement_requests=tuple(placement_requests),
    )


def _route_lock(path: Path) -> RLock:
    key = path.resolve()
    with _ROUTE_LOCKS_GUARD:
        lock = _ROUTE_LOCKS.get(key)
        if lock is None:
            lock = RLock()
            _ROUTE_LOCKS[key] = lock
        return lock


def _exec_route_cache(path: Path) -> OrderedDict[str, ExecRoute]:
    key = path.resolve()
    with _ROUTE_LOCKS_GUARD:
        cache = _EXEC_ROUTE_CACHES.get(key)
        if cache is None:
            cache = OrderedDict()
            _EXEC_ROUTE_CACHES[key] = cache
        return cache


def _exec_route_cache_sandbox_index(path: Path) -> dict[str, set[str]]:
    key = path.resolve()
    with _ROUTE_LOCKS_GUARD:
        index = _EXEC_ROUTE_CACHE_SANDBOX_INDEXES.get(key)
        if index is None:
            index = {}
            _EXEC_ROUTE_CACHE_SANDBOX_INDEXES[key] = index
        return index


def _is_sqlite_file(path: Path) -> bool:
    try:
        with path.open("rb") as file:
            header = file.read(16)
    except OSError:
        return False
    return header == b"SQLite format 3\x00"


def _resources_json(resources: ResourceQuantity) -> str:
    return json.dumps(resources.to_dict(), sort_keys=True, separators=(",", ":"))


def _resources_from_json(raw: object) -> ResourceQuantity:
    if not isinstance(raw, str):
        return ResourceQuantity()
    try:
        return ResourceQuantity.from_dict(json.loads(raw))
    except json.JSONDecodeError:
        return ResourceQuantity()


def _object_json(raw: dict[str, Any]) -> str:
    return json.dumps(raw, sort_keys=True, separators=(",", ":"))


def _object_from_json(raw: object) -> dict[str, Any]:
    if not isinstance(raw, str):
        return {}
    try:
        decoded = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return dict(decoded) if isinstance(decoded, dict) else {}


def _sandbox_route_from_row(row: sqlite3.Row) -> SandboxRoute:
    return SandboxRoute(
        sandbox_id=str(row["sandbox_id"]),
        node_id=str(row["node_id"]),
        job_id=str(row["job_id"]),
        node_url=str(row["node_url"]),
        resources=_resources_from_json(row["resources_json"]),
        spec=_object_from_json(row["spec_json"]),
        state=str(row["state"] or "unknown"),
        generation=_nonnegative_int(row["generation"]),
        create_operation_id=str(row["create_operation_id"] or ""),
        spec_hash=str(row["spec_hash"] or ""),
        delete_operation_id=str(row["delete_operation_id"] or ""),
        node_epoch=str(row["node_epoch"] or ""),
        activity_epoch=_nonnegative_int(row["activity_epoch"]),
        storage_schema=str(row["storage_schema"] or ""),
        snapshot_manifest_digest=str(row["snapshot_manifest_digest"] or ""),
        snapshot_repository=str(row["snapshot_repository"] or ""),
        snapshot_tag=str(row["snapshot_tag"] or ""),
        storage_snapshot=_object_from_json(row["storage_snapshot_json"]),
        created_at=str(row["created_at"]),
        updated_at=str(row["updated_at"]),
    )


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
        archive_sha256=str(row["archive_sha256"]),
        archive_token=str(row["archive_token"]),
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
        count=max(1, int(row["count"])),
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
