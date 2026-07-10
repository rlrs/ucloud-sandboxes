from __future__ import annotations

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

from .models import ResourceQuantity, SandboxDemand, parse_iso_datetime, utc_now


_ROUTE_LOCKS_GUARD = RLock()
_ROUTE_LOCKS: dict[Path, RLock] = {}
PENDING_DEMAND_TTL_SECONDS = 300


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
            "created_at": self.created_at,
            "updated_at": self.updated_at,
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
        with self._lock:
            self._ensure_db()

    def load(self) -> RoutingState:
        with self._lock:
            with self._transaction() as conn:
                self._prune_expired_unlocked(conn, utc_now())
                return self._load_unlocked(conn)

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

    def get_sandbox(self, sandbox_id: str) -> SandboxRoute | None:
        with self._lock:
            with self._connect() as conn:
                return self._get_sandbox_unlocked(conn, sandbox_id)

    def get_sandbox_readonly(self, sandbox_id: str) -> SandboxRoute | None:
        with self._connect() as conn:
            return self._get_sandbox_unlocked(conn, sandbox_id)

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
                               node_epoch, activity_epoch, created_at, updated_at
                        FROM sandboxes
                        ORDER BY sandbox_id
                        """
                    )
                )
                if route is not None
            ]

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
                    created_at=route.created_at
                    or (existing.created_at if existing else now),
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

    def allocate_sandbox_create(
        self,
        route: SandboxRoute,
        *,
        spec_hash: str,
        create_operation_id: str | None = None,
    ) -> SandboxRoute:
        """Persist a new route incarnation before its node create is dispatched."""

        operation_id = (create_operation_id or f"create-{uuid4().hex}").strip()
        if not operation_id or not spec_hash.strip():
            raise ValueError("create operation id and spec hash are required")
        with self._lock:
            now = utc_now().isoformat()
            with self._transaction() as conn:
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
                    return existing
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
            return stored

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
                               node_epoch, activity_epoch, created_at, updated_at
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
            if not removed:
                return []
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
                           node_epoch, activity_epoch, created_at, updated_at
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
            if not removed:
                return []
            return removed

    def get_exec(self, session_id: str) -> ExecRoute | None:
        with self._lock:
            with self._connect() as conn:
                return self._get_exec_unlocked(conn, session_id)

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
            now = utc_now().isoformat()
            with self._transaction() as conn:
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
                        existing.created_at if same_incarnation and existing else now
                    ),
                    updated_at=now,
                    attempts=(existing.attempts + 1) if same_incarnation and existing else 1,
                    generation=max(0, generation),
                    operation_id=operation_id.strip(),
                    spec_hash=spec_hash.strip(),
                    failure_reason=failure_reason.strip(),
                )
                self._write_pending(conn, stored)

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
            pending = list(self._active_pending_unlocked(now).values())
            prepared = list(self._active_prepared_unlocked(now).values())
        pending_total = ResourceQuantity()
        prepared_total = ResourceQuantity()
        oldest_pending_seconds = 0
        now = utc_now()
        for item in pending:
            pending_total = pending_total + item.resources
            created_at = parse_iso_datetime(item.created_at)
            if created_at is not None:
                oldest_pending_seconds = max(
                    oldest_pending_seconds,
                    int((now - created_at).total_seconds()),
                )
        for item in prepared:
            prepared_total = prepared_total + item.total_resources
            created_at = parse_iso_datetime(item.created_at)
            if created_at is not None:
                oldest_pending_seconds = max(
                    oldest_pending_seconds,
                    int((now - created_at).total_seconds()),
                )
        return SandboxDemand(
            pending_resources=pending_total,
            prepared_resources=prepared_total,
            oldest_pending_seconds=max(0, oldest_pending_seconds),
        )

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
        state = self._load_unlocked(conn)
        conn.executemany(
            "DELETE FROM pending WHERE sandbox_id = ?",
            (
                (sandbox_id,)
                for sandbox_id, item in state.pending.items()
                if item.is_expired(now)
            ),
        )
        conn.executemany(
            "DELETE FROM image_builds WHERE image_id = ?",
            (
                (image_id,)
                for image_id, item in state.image_builds.items()
                if item.is_expired(now)
            ),
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

    def _load_unlocked(self, conn: sqlite3.Connection) -> RoutingState:
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
                               node_epoch, activity_epoch, created_at, updated_at
                        FROM sandboxes
                        ORDER BY sandbox_id
                        """
                    )
                )
                if route is not None
            },
            exec_sessions={
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
            },
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
                   node_epoch, activity_epoch, created_at, updated_at
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
                node_epoch, activity_epoch, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
    prepared_total = ResourceQuantity()
    oldest_pending_seconds = 0
    for item in state.pending.values():
        if item.is_expired(now):
            continue
        pending_total = pending_total + item.resources
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
        created_at = parse_iso_datetime(item.created_at)
        if created_at is not None:
            oldest_pending_seconds = max(
                oldest_pending_seconds,
                int((now - created_at).total_seconds()),
            )
    return SandboxDemand(
        pending_resources=pending_total,
        prepared_resources=prepared_total,
        oldest_pending_seconds=max(0, oldest_pending_seconds),
    )


def _route_lock(path: Path) -> RLock:
    key = path.resolve()
    with _ROUTE_LOCKS_GUARD:
        lock = _ROUTE_LOCKS.get(key)
        if lock is None:
            lock = RLock()
            _ROUTE_LOCKS[key] = lock
        return lock


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
        created_at=str(row["created_at"]),
        updated_at=str(row["updated_at"]),
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
