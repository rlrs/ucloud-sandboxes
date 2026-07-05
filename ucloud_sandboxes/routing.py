from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import json
from pathlib import Path
import sqlite3
from threading import RLock
from typing import Any, Iterator
from uuid import uuid4

from .models import ResourceQuantity, SandboxDemand, parse_iso_datetime, utc_now


_ROUTE_LOCKS_GUARD = RLock()
_ROUTE_LOCKS: dict[Path, RLock] = {}
PENDING_DEMAND_TTL_SECONDS = 300


@dataclass(frozen=True)
class SandboxRoute:
    sandbox_id: str
    node_id: str
    job_id: str
    node_url: str
    resources: ResourceQuantity = ResourceQuantity()
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
        )

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "sandbox_id": self.sandbox_id,
            "resources": self.resources.to_dict(),
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


class RoutingStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = _route_lock(path)
        with self._lock:
            self._ensure_db()
            with self._connect() as conn:
                self._state = self._load_unlocked(conn)

    def load(self) -> RoutingState:
        with self._lock:
            self._refresh_unlocked()
            now = utc_now()
            self._active_pending_unlocked(now)
            self._active_image_builds_unlocked(now)
            self._active_prepared_unlocked(now)
            self._active_prepared_builders_unlocked(now)
            return _copy_state(self._state)

    def save(self, state: RoutingState) -> None:
        with self._lock:
            with self._transaction() as conn:
                conn.execute("DELETE FROM sandboxes")
                conn.execute("DELETE FROM exec_sessions")
                conn.execute("DELETE FROM pending")
                conn.execute("DELETE FROM image_builds")
                conn.execute("DELETE FROM prepared_capacity")
                conn.execute("DELETE FROM prepared_builders")
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
            self._state = _copy_state(state)

    def _save_unlocked(self, state: RoutingState) -> None:
        with self._transaction() as conn:
            conn.execute("DELETE FROM sandboxes")
            conn.execute("DELETE FROM exec_sessions")
            conn.execute("DELETE FROM pending")
            conn.execute("DELETE FROM image_builds")
            conn.execute("DELETE FROM prepared_capacity")
            conn.execute("DELETE FROM prepared_builders")
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
        self._state = _copy_state(state)

    def get_sandbox(self, sandbox_id: str) -> SandboxRoute | None:
        with self._lock:
            with self._connect() as conn:
                route = self._get_sandbox_unlocked(conn, sandbox_id)
            sandboxes = dict(self._state.sandboxes)
            if route is None:
                sandboxes.pop(sandbox_id, None)
            else:
                sandboxes[sandbox_id] = route
            self._state = RoutingState(
                sandboxes=sandboxes,
                exec_sessions=dict(self._state.exec_sessions),
                pending=dict(self._state.pending),
                image_builds=dict(self._state.image_builds),
                prepared=dict(self._state.prepared),
                prepared_builders=dict(self._state.prepared_builders),
            )
            return route

    def upsert_sandbox(self, route: SandboxRoute) -> None:
        with self._lock:
            now = utc_now().isoformat()
            with self._transaction() as conn:
                existing = self._get_sandbox_unlocked(conn, route.sandbox_id)
                stored = SandboxRoute(
                    sandbox_id=route.sandbox_id,
                    node_id=route.node_id,
                    job_id=route.job_id,
                    node_url=route.node_url,
                    resources=route.resources,
                    created_at=route.created_at
                    or (existing.created_at if existing else now),
                    updated_at=now,
                )
                self._write_sandbox(conn, stored)
                conn.execute(
                    "DELETE FROM pending WHERE sandbox_id = ?", (route.sandbox_id,)
                )
            sandboxes = dict(self._state.sandboxes)
            pending = dict(self._state.pending)
            sandboxes[route.sandbox_id] = stored
            pending.pop(route.sandbox_id, None)
            self._state = RoutingState(
                sandboxes=sandboxes,
                exec_sessions=dict(self._state.exec_sessions),
                pending=pending,
                image_builds=dict(self._state.image_builds),
                prepared=dict(self._state.prepared),
                prepared_builders=dict(self._state.prepared_builders),
            )

    def reconcile_sandboxes_for_node(
        self,
        node_url: str,
        routes: list[SandboxRoute],
        *,
        observed_at: str,
    ) -> None:
        node_url = node_url.strip()
        if not node_url:
            return
        observed_ids = {route.sandbox_id for route in routes}
        observed_at_dt = parse_iso_datetime(observed_at)
        with self._lock:
            self._refresh_unlocked()
            stored_routes: list[SandboxRoute] = []
            for route in routes:
                existing = self._state.sandboxes.get(route.sandbox_id)
                stored_routes.append(
                    SandboxRoute(
                        sandbox_id=route.sandbox_id,
                        node_id=route.node_id,
                        job_id=route.job_id,
                        node_url=route.node_url,
                        resources=route.resources,
                        created_at=route.created_at
                        or (existing.created_at if existing else observed_at),
                        updated_at=observed_at,
                    )
                )
            stale_ids: list[str] = []
            for sandbox_id, route in self._state.sandboxes.items():
                if route.node_url != node_url or sandbox_id in observed_ids:
                    continue
                route_updated_at = parse_iso_datetime(
                    route.updated_at
                ) or parse_iso_datetime(route.created_at)
                if (
                    observed_at_dt is None
                    or route_updated_at is None
                    or route_updated_at <= observed_at_dt
                ):
                    stale_ids.append(sandbox_id)
            with self._transaction() as conn:
                for route in stored_routes:
                    self._write_sandbox(conn, route)
                    conn.execute(
                        "DELETE FROM pending WHERE sandbox_id = ?", (route.sandbox_id,)
                    )
                for sandbox_id in stale_ids:
                    conn.execute(
                        "DELETE FROM sandboxes WHERE sandbox_id = ?", (sandbox_id,)
                    )
                    conn.execute(
                        "DELETE FROM pending WHERE sandbox_id = ?", (sandbox_id,)
                    )
                    conn.execute(
                        "DELETE FROM exec_sessions WHERE sandbox_id = ?", (sandbox_id,)
                    )
            sandboxes = dict(self._state.sandboxes)
            pending = dict(self._state.pending)
            exec_sessions = dict(self._state.exec_sessions)
            for route in stored_routes:
                sandboxes[route.sandbox_id] = route
                pending.pop(route.sandbox_id, None)
            for sandbox_id in stale_ids:
                sandboxes.pop(sandbox_id, None)
                pending.pop(sandbox_id, None)
            if stale_ids:
                stale_id_set = set(stale_ids)
                exec_sessions = {
                    session_id: route
                    for session_id, route in exec_sessions.items()
                    if route.sandbox_id not in stale_id_set
                }
            self._state = RoutingState(
                sandboxes=sandboxes,
                exec_sessions=exec_sessions,
                pending=pending,
                image_builds=dict(self._state.image_builds),
                prepared=dict(self._state.prepared),
                prepared_builders=dict(self._state.prepared_builders),
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
            sandboxes = dict(self._state.sandboxes)
            pending = dict(self._state.pending)
            exec_sessions = {
                session_id: route
                for session_id, route in self._state.exec_sessions.items()
                if route.sandbox_id != sandbox_id
            }
            sandboxes.pop(sandbox_id, None)
            pending.pop(sandbox_id, None)
            self._state = RoutingState(
                sandboxes=sandboxes,
                exec_sessions=exec_sessions,
                pending=pending,
                image_builds=dict(self._state.image_builds),
                prepared=dict(self._state.prepared),
                prepared_builders=dict(self._state.prepared_builders),
            )

    def get_exec(self, session_id: str) -> ExecRoute | None:
        with self._lock:
            with self._connect() as conn:
                route = self._get_exec_unlocked(conn, session_id)
            exec_sessions = dict(self._state.exec_sessions)
            if route is None:
                exec_sessions.pop(session_id, None)
            else:
                exec_sessions[session_id] = route
            self._state = RoutingState(
                sandboxes=dict(self._state.sandboxes),
                exec_sessions=exec_sessions,
                pending=dict(self._state.pending),
                image_builds=dict(self._state.image_builds),
                prepared=dict(self._state.prepared),
                prepared_builders=dict(self._state.prepared_builders),
            )
            return route

    def get_pending(self, sandbox_id: str) -> PendingSandboxDemand | None:
        with self._lock:
            with self._connect() as conn:
                pending = self._get_pending_unlocked(conn, sandbox_id)
            pending_items = dict(self._state.pending)
            if pending is None:
                pending_items.pop(sandbox_id, None)
            else:
                pending_items[sandbox_id] = pending
            self._state = RoutingState(
                sandboxes=dict(self._state.sandboxes),
                exec_sessions=dict(self._state.exec_sessions),
                pending=pending_items,
                image_builds=dict(self._state.image_builds),
                prepared=dict(self._state.prepared),
                prepared_builders=dict(self._state.prepared_builders),
            )
            return pending

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
            exec_sessions = dict(self._state.exec_sessions)
            exec_sessions[route.session_id] = stored
            self._state = RoutingState(
                sandboxes=dict(self._state.sandboxes),
                exec_sessions=exec_sessions,
                pending=dict(self._state.pending),
                image_builds=dict(self._state.image_builds),
                prepared=dict(self._state.prepared),
                prepared_builders=dict(self._state.prepared_builders),
            )

    def upsert_pending(self, sandbox_id: str, resources: ResourceQuantity) -> None:
        with self._lock:
            now = utc_now().isoformat()
            with self._transaction() as conn:
                existing = self._get_pending_unlocked(conn, sandbox_id)
                stored = PendingSandboxDemand(
                    sandbox_id=sandbox_id,
                    resources=resources,
                    created_at=existing.created_at if existing else now,
                    updated_at=now,
                    attempts=(existing.attempts + 1) if existing else 1,
                )
                self._write_pending(conn, stored)
            pending = dict(self._state.pending)
            pending[sandbox_id] = stored
            self._state = RoutingState(
                sandboxes=dict(self._state.sandboxes),
                exec_sessions=dict(self._state.exec_sessions),
                pending=pending,
                image_builds=dict(self._state.image_builds),
                prepared=dict(self._state.prepared),
                prepared_builders=dict(self._state.prepared_builders),
            )

    def clear_pending(self, sandbox_id: str) -> None:
        with self._lock:
            with self._transaction() as conn:
                conn.execute("DELETE FROM pending WHERE sandbox_id = ?", (sandbox_id,))
            pending = dict(self._state.pending)
            pending.pop(sandbox_id, None)
            self._state = RoutingState(
                sandboxes=dict(self._state.sandboxes),
                exec_sessions=dict(self._state.exec_sessions),
                pending=pending,
                image_builds=dict(self._state.image_builds),
                prepared=dict(self._state.prepared),
                prepared_builders=dict(self._state.prepared_builders),
            )

    def consume_pending_demand(self) -> list[PendingSandboxDemand]:
        now = utc_now()
        with self._lock:
            self._refresh_unlocked()
            pending = self._active_pending_unlocked(now)
            if not pending:
                return []
            with self._transaction() as conn:
                conn.execute("DELETE FROM pending")
            self._state = RoutingState(
                sandboxes=dict(self._state.sandboxes),
                exec_sessions=dict(self._state.exec_sessions),
                pending={},
                image_builds=dict(self._state.image_builds),
                prepared=dict(self._state.prepared),
                prepared_builders=dict(self._state.prepared_builders),
            )
            return list(pending.values())

    def pending_sandboxes(self) -> list[PendingSandboxDemand]:
        now = utc_now()
        with self._lock:
            self._refresh_unlocked()
            return list(self._active_pending_unlocked(now).values())

    def upsert_pending_image_build(self, image_id: str, tag: str) -> None:
        with self._lock:
            self._refresh_unlocked()
            existing = self._state.image_builds.get(image_id)
            now = utc_now().isoformat()
            stored = PendingImageBuildDemand(
                image_id=image_id,
                tag=tag,
                created_at=existing.created_at if existing else now,
                updated_at=now,
                attempts=(existing.attempts + 1) if existing else 1,
            )
            with self._transaction() as conn:
                self._write_image_build(conn, stored)
            image_builds = dict(self._state.image_builds)
            image_builds[image_id] = stored
            self._state = RoutingState(
                sandboxes=dict(self._state.sandboxes),
                exec_sessions=dict(self._state.exec_sessions),
                pending=dict(self._state.pending),
                image_builds=image_builds,
                prepared=dict(self._state.prepared),
                prepared_builders=dict(self._state.prepared_builders),
            )

    def clear_pending_image_build(self, image_id: str) -> None:
        with self._lock:
            self._refresh_unlocked()
            with self._transaction() as conn:
                conn.execute("DELETE FROM image_builds WHERE image_id = ?", (image_id,))
            image_builds = dict(self._state.image_builds)
            image_builds.pop(image_id, None)
            self._state = RoutingState(
                sandboxes=dict(self._state.sandboxes),
                exec_sessions=dict(self._state.exec_sessions),
                pending=dict(self._state.pending),
                image_builds=image_builds,
                prepared=dict(self._state.prepared),
                prepared_builders=dict(self._state.prepared_builders),
            )

    def consume_pending_image_builds(self) -> list[PendingImageBuildDemand]:
        now = utc_now()
        with self._lock:
            self._refresh_unlocked()
            image_builds = self._active_image_builds_unlocked(now)
            if not image_builds:
                return []
            with self._transaction() as conn:
                conn.execute("DELETE FROM image_builds")
            self._state = RoutingState(
                sandboxes=dict(self._state.sandboxes),
                exec_sessions=dict(self._state.exec_sessions),
                pending=dict(self._state.pending),
                image_builds={},
                prepared=dict(self._state.prepared),
                prepared_builders=dict(self._state.prepared_builders),
            )
            return list(image_builds.values())

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
            self._refresh_unlocked()
            existing = self._state.prepared.get(prepare_id)
            now = utc_now()
            stored = PreparedCapacityDemand(
                prepare_id=prepare_id,
                resources=resources,
                count=max(1, count),
                created_at=existing.created_at if existing else now.isoformat(),
                updated_at=now.isoformat(),
                expires_at=(now + timedelta(seconds=max(1, ttl_seconds))).isoformat(),
                image=image.strip(),
            )
            with self._transaction() as conn:
                self._write_prepared(conn, stored)
            prepared = dict(self._state.prepared)
            prepared[prepare_id] = stored
            self._state = RoutingState(
                sandboxes=dict(self._state.sandboxes),
                exec_sessions=dict(self._state.exec_sessions),
                pending=dict(self._state.pending),
                image_builds=dict(self._state.image_builds),
                prepared=prepared,
                prepared_builders=dict(self._state.prepared_builders),
            )
            return stored

    def delete_prepared_capacity(
        self, prepare_id: str
    ) -> PreparedCapacityDemand | None:
        with self._lock:
            self._refresh_unlocked()
            existing = self._state.prepared.get(prepare_id)
            with self._transaction() as conn:
                conn.execute(
                    "DELETE FROM prepared_capacity WHERE prepare_id = ?",
                    (prepare_id,),
                )
            prepared = dict(self._state.prepared)
            prepared.pop(prepare_id, None)
            self._state = RoutingState(
                sandboxes=dict(self._state.sandboxes),
                exec_sessions=dict(self._state.exec_sessions),
                pending=dict(self._state.pending),
                image_builds=dict(self._state.image_builds),
                prepared=prepared,
                prepared_builders=dict(self._state.prepared_builders),
            )
            return existing

    def prepared_capacity(self) -> list[PreparedCapacityDemand]:
        now = utc_now()
        with self._lock:
            self._refresh_unlocked()
            prepared = self._active_prepared_unlocked(now)
            return list(prepared.values())

    def consume_prepared_capacity(self) -> list[PreparedCapacityDemand]:
        now = utc_now()
        with self._lock:
            self._refresh_unlocked()
            prepared = self._active_prepared_unlocked(now)
            if not prepared:
                return []
            with self._transaction() as conn:
                conn.execute("DELETE FROM prepared_capacity")
            self._state = RoutingState(
                sandboxes=dict(self._state.sandboxes),
                exec_sessions=dict(self._state.exec_sessions),
                pending=dict(self._state.pending),
                image_builds=dict(self._state.image_builds),
                prepared={},
                prepared_builders=dict(self._state.prepared_builders),
            )
            return list(prepared.values())

    def upsert_prepared_builder(
        self,
        prepare_id: str,
        *,
        count: int,
        ttl_seconds: int,
    ) -> PreparedBuilderDemand:
        with self._lock:
            self._refresh_unlocked()
            existing = self._state.prepared_builders.get(prepare_id)
            now = utc_now()
            stored = PreparedBuilderDemand(
                prepare_id=prepare_id,
                count=max(1, count),
                created_at=existing.created_at if existing else now.isoformat(),
                updated_at=now.isoformat(),
                expires_at=(now + timedelta(seconds=max(1, ttl_seconds))).isoformat(),
            )
            with self._transaction() as conn:
                self._write_prepared_builder(conn, stored)
            prepared_builders = dict(self._state.prepared_builders)
            prepared_builders[prepare_id] = stored
            self._state = RoutingState(
                sandboxes=dict(self._state.sandboxes),
                exec_sessions=dict(self._state.exec_sessions),
                pending=dict(self._state.pending),
                image_builds=dict(self._state.image_builds),
                prepared=dict(self._state.prepared),
                prepared_builders=prepared_builders,
            )
            return stored

    def delete_prepared_builder(self, prepare_id: str) -> PreparedBuilderDemand | None:
        with self._lock:
            self._refresh_unlocked()
            existing = self._state.prepared_builders.get(prepare_id)
            with self._transaction() as conn:
                conn.execute(
                    "DELETE FROM prepared_builders WHERE prepare_id = ?",
                    (prepare_id,),
                )
            prepared_builders = dict(self._state.prepared_builders)
            prepared_builders.pop(prepare_id, None)
            self._state = RoutingState(
                sandboxes=dict(self._state.sandboxes),
                exec_sessions=dict(self._state.exec_sessions),
                pending=dict(self._state.pending),
                image_builds=dict(self._state.image_builds),
                prepared=dict(self._state.prepared),
                prepared_builders=prepared_builders,
            )
            return existing

    def prepared_builders(self) -> list[PreparedBuilderDemand]:
        now = utc_now()
        with self._lock:
            self._refresh_unlocked()
            prepared_builders = self._active_prepared_builders_unlocked(now)
            return list(prepared_builders.values())

    def consume_prepared_builders(self) -> list[PreparedBuilderDemand]:
        now = utc_now()
        with self._lock:
            self._refresh_unlocked()
            prepared_builders = self._active_prepared_builders_unlocked(now)
            if not prepared_builders:
                return []
            with self._transaction() as conn:
                conn.execute("DELETE FROM prepared_builders")
            self._state = RoutingState(
                sandboxes=dict(self._state.sandboxes),
                exec_sessions=dict(self._state.exec_sessions),
                pending=dict(self._state.pending),
                image_builds=dict(self._state.image_builds),
                prepared=dict(self._state.prepared),
                prepared_builders={},
            )
            return list(prepared_builders.values())

    def prepared_builder_count(self) -> int:
        now = utc_now()
        with self._lock:
            self._refresh_unlocked()
            return sum(
                item.count
                for item in self._active_prepared_builders_unlocked(now).values()
            )

    def pending_image_build_count(self) -> int:
        now = utc_now()
        with self._lock:
            self._refresh_unlocked()
            return len(self._active_image_builds_unlocked(now))

    def oldest_pending_image_build_seconds(self) -> int:
        now = utc_now()
        with self._lock:
            self._refresh_unlocked()
            timestamps = [
                item.created_at
                for item in self._active_image_builds_unlocked(now).values()
            ]
        return _oldest_seconds(timestamps)

    def pending_demand(self) -> SandboxDemand:
        with self._lock:
            self._refresh_unlocked()
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
        expired = [
            sandbox_id
            for sandbox_id, item in self._state.pending.items()
            if item.is_expired(now)
        ]
        if not expired:
            return dict(self._state.pending)
        with self._transaction() as conn:
            for sandbox_id in expired:
                conn.execute(
                    "DELETE FROM pending WHERE sandbox_id = ?",
                    (sandbox_id,),
                )
        expired_ids = set(expired)
        pending = {
            sandbox_id: item
            for sandbox_id, item in self._state.pending.items()
            if sandbox_id not in expired_ids
        }
        self._state = RoutingState(
            sandboxes=dict(self._state.sandboxes),
            exec_sessions=dict(self._state.exec_sessions),
            pending=pending,
            image_builds=dict(self._state.image_builds),
            prepared=dict(self._state.prepared),
            prepared_builders=dict(self._state.prepared_builders),
        )
        return dict(pending)

    def _active_image_builds_unlocked(
        self,
        now: datetime,
    ) -> dict[str, PendingImageBuildDemand]:
        expired = [
            image_id
            for image_id, item in self._state.image_builds.items()
            if item.is_expired(now)
        ]
        if not expired:
            return dict(self._state.image_builds)
        with self._transaction() as conn:
            for image_id in expired:
                conn.execute(
                    "DELETE FROM image_builds WHERE image_id = ?",
                    (image_id,),
                )
        expired_ids = set(expired)
        image_builds = {
            image_id: item
            for image_id, item in self._state.image_builds.items()
            if image_id not in expired_ids
        }
        self._state = RoutingState(
            sandboxes=dict(self._state.sandboxes),
            exec_sessions=dict(self._state.exec_sessions),
            pending=dict(self._state.pending),
            image_builds=image_builds,
            prepared=dict(self._state.prepared),
            prepared_builders=dict(self._state.prepared_builders),
        )
        return dict(image_builds)

    def _active_prepared_unlocked(
        self,
        now: datetime,
    ) -> dict[str, PreparedCapacityDemand]:
        expired = [
            prepare_id
            for prepare_id, item in self._state.prepared.items()
            if item.is_expired(now)
        ]
        if not expired:
            return dict(self._state.prepared)
        with self._transaction() as conn:
            for prepare_id in expired:
                conn.execute(
                    "DELETE FROM prepared_capacity WHERE prepare_id = ?",
                    (prepare_id,),
                )
        expired_ids = set(expired)
        prepared = {
            prepare_id: item
            for prepare_id, item in self._state.prepared.items()
            if prepare_id not in expired_ids
        }
        self._state = RoutingState(
            sandboxes=dict(self._state.sandboxes),
            exec_sessions=dict(self._state.exec_sessions),
            pending=dict(self._state.pending),
            image_builds=dict(self._state.image_builds),
            prepared=prepared,
            prepared_builders=dict(self._state.prepared_builders),
        )
        return dict(prepared)

    def _active_prepared_builders_unlocked(
        self,
        now: datetime,
    ) -> dict[str, PreparedBuilderDemand]:
        expired = [
            prepare_id
            for prepare_id, item in self._state.prepared_builders.items()
            if item.is_expired(now)
        ]
        if not expired:
            return dict(self._state.prepared_builders)
        with self._transaction() as conn:
            for prepare_id in expired:
                conn.execute(
                    "DELETE FROM prepared_builders WHERE prepare_id = ?",
                    (prepare_id,),
                )
        expired_ids = set(expired)
        prepared_builders = {
            prepare_id: item
            for prepare_id, item in self._state.prepared_builders.items()
            if prepare_id not in expired_ids
        }
        self._state = RoutingState(
            sandboxes=dict(self._state.sandboxes),
            exec_sessions=dict(self._state.exec_sessions),
            pending=dict(self._state.pending),
            image_builds=dict(self._state.image_builds),
            prepared=dict(self._state.prepared),
            prepared_builders=prepared_builders,
        )
        return dict(prepared_builders)

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
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
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
                    attempts INTEGER NOT NULL
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
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        try:
            yield conn
        finally:
            conn.close()

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
                               resources_json, created_at, updated_at
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
                               attempts
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
        )

    def _refresh_unlocked(self) -> None:
        with self._connect() as conn:
            self._state = self._load_unlocked(conn)

    def _get_sandbox_unlocked(
        self,
        conn: sqlite3.Connection,
        sandbox_id: str,
    ) -> SandboxRoute | None:
        row = conn.execute(
            """
            SELECT sandbox_id, node_id, job_id, node_url, resources_json,
                   created_at, updated_at
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
            SELECT sandbox_id, resources_json, created_at, updated_at, attempts
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

    def _write_sandbox(self, conn: sqlite3.Connection, route: SandboxRoute) -> None:
        conn.execute(
            """
            INSERT INTO sandboxes (
                sandbox_id, node_id, job_id, node_url, resources_json,
                created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(sandbox_id) DO UPDATE SET
                node_id = excluded.node_id,
                job_id = excluded.job_id,
                node_url = excluded.node_url,
                resources_json = excluded.resources_json,
                created_at = excluded.created_at,
                updated_at = excluded.updated_at
            """,
            (
                route.sandbox_id,
                route.node_id,
                route.job_id,
                route.node_url,
                _resources_json(route.resources),
                route.created_at,
                route.updated_at,
            ),
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
                sandbox_id, resources_json, created_at, updated_at, attempts
            )
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(sandbox_id) DO UPDATE SET
                resources_json = excluded.resources_json,
                created_at = excluded.created_at,
                updated_at = excluded.updated_at,
                attempts = excluded.attempts
            """,
            (
                item.sandbox_id,
                _resources_json(item.resources),
                item.created_at,
                item.updated_at,
                item.attempts,
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


def _string(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None


def _copy_state(state: RoutingState) -> RoutingState:
    return RoutingState(
        sandboxes=dict(state.sandboxes),
        exec_sessions=dict(state.exec_sessions),
        pending=dict(state.pending),
        image_builds=dict(state.image_builds),
        prepared=dict(state.prepared),
        prepared_builders=dict(state.prepared_builders),
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


def _sandbox_route_from_row(row: sqlite3.Row) -> SandboxRoute:
    return SandboxRoute(
        sandbox_id=str(row["sandbox_id"]),
        node_id=str(row["node_id"]),
        job_id=str(row["job_id"]),
        node_url=str(row["node_url"]),
        resources=_resources_from_json(row["resources_json"]),
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
