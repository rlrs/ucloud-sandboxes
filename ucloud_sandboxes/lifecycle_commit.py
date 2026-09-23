"""Gateway lifecycle authority, independent of HTTP request/response handling.

A worker receipt is evidence, not authority: its boot/activity fence must match
our accepted worker view before a route CAS can commit it. Snapshot protection
precedes that CAS; compensation reads the durable route after uncertain commits.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any, Callable, Mapping

from .models import NodeHeartbeat, SandboxInventoryEntry
from .routing import (
    ProgramRequestState,
    RoutingStore,
    SandboxRoute,
    route_with_inventory_snapshot,
)
from .storage_native_migration import StorageNativeMigration


class InvalidLifecycleReceipt(ValueError):
    """Worker evidence cannot authorize a lifecycle transition."""


class LifecycleRouteChanged(RuntimeError):
    """The request no longer owns its route; a new request may re-resolve it."""


@dataclass(frozen=True)
class LifecycleFence:
    node_epoch: str
    activity_epoch: int

    @classmethod
    def validate(
        cls,
        route: SandboxRoute,
        payload: Mapping[str, Any],
        heartbeat: NodeHeartbeat | None,
    ) -> LifecycleFence:
        node_epoch = payload.get("node_epoch")
        activity_epoch = payload.get("activity_epoch")
        if not isinstance(node_epoch, str) or not node_epoch.strip():
            raise ValueError("node_epoch is required")
        node_epoch = node_epoch.strip()
        if isinstance(activity_epoch, bool) or not isinstance(activity_epoch, int):
            raise ValueError("activity_epoch must be an integer")
        if activity_epoch < 0:
            raise ValueError("activity_epoch must be non-negative")
        if (
            heartbeat is None
            or heartbeat.node_id != route.node_id
            or (heartbeat.node_url or "").rstrip("/") != route.node_url.rstrip("/")
            or heartbeat.node_epoch != node_epoch
        ):
            raise ValueError("node epoch does not match the routed worker")
        if route.node_epoch and route.node_epoch != node_epoch:
            raise ValueError("node epoch does not match the sandbox route")
        if activity_epoch <= route.activity_epoch:
            raise ValueError("activity_epoch does not postdate the sandbox route")
        if activity_epoch < heartbeat.activity_epoch:
            raise ValueError("activity_epoch predates the accepted worker heartbeat")
        return cls(node_epoch, activity_epoch)


@dataclass(frozen=True)
class LifecycleCommit:
    route: SandboxRoute
    program_transition: tuple[ProgramRequestState | None, bool] | None = None


@dataclass(frozen=True)
class SnapshotReferences:
    """Durable reference operations; release must retain keep_route's closure."""

    protect: Callable[[SandboxRoute], None]
    release: Callable[..., None]


class LifecycleCommitter:
    def __init__(
        self,
        routes: RoutingStore,
        *,
        heartbeat: Callable[[str], NodeHeartbeat | None],
        snapshots: SnapshotReferences,
    ):
        self.routes = routes
        self.heartbeat = heartbeat
        self.snapshots = snapshots

    def _fence(self, route, payload):
        try:
            return LifecycleFence.validate(route, payload, self.heartbeat(route.job_id))
        except ValueError as exc:
            raise InvalidLifecycleReceipt(
                f"invalid node lifecycle response: {exc}"
            ) from exc

    def _compensate(self, route, candidate=None):
        # If readback fails, release nothing: the commit may have succeeded.
        current = self.routes.get_sandbox_readonly(route.sandbox_id)
        if candidate is not None:
            self.snapshots.release(candidate, keep_route=current)
        self.snapshots.release(route, keep_route=current)

    def park(self, route: SandboxRoute, payload: Mapping[str, Any]) -> LifecycleCommit:
        sandbox = payload.get("sandbox", {})
        if not isinstance(sandbox, dict):
            raise InvalidLifecycleReceipt("node returned an unstable wait state")
        state = str(sandbox.get("state", "parked")).lower()
        if state not in {"parked", "running"}:
            raise InvalidLifecycleReceipt("node returned an unstable wait state")
        fence = self._fence(route, payload)
        candidate = None
        has_snapshot = bool(
            payload.get("storage_schema") or payload.get("snapshot_manifest_digest")
        )
        if state != "parked" and has_snapshot:
            raise InvalidLifecycleReceipt(
                "a resident wait cannot advertise a portable checkpoint"
            )
        if has_snapshot:
            try:
                candidate = route_with_snapshot_payload(route, payload)
            except ValueError as exc:
                raise InvalidLifecycleReceipt(
                    "node returned invalid durable park metadata"
                ) from exc
            try:
                self.snapshots.protect(candidate)
            except BaseException:
                current = self.routes.get_sandbox_readonly(route.sandbox_id)
                self.snapshots.release(candidate, keep_route=current)
                raise
        metadata = (
            {
                "storage_schema": candidate.storage_schema,
                "snapshot_manifest_digest": candidate.snapshot_manifest_digest,
                "snapshot_repository": candidate.snapshot_repository,
                "snapshot_tag": candidate.snapshot_tag,
                "storage_snapshot": dict(candidate.storage_snapshot),
            }
            if candidate is not None
            else {
                "storage_schema": None if state == "parked" else "",
                "snapshot_manifest_digest": None if state == "parked" else "",
                "snapshot_repository": None if state == "parked" else "",
                "snapshot_tag": None if state == "parked" else "",
                "storage_snapshot": None if state == "parked" else {},
            }
        )
        try:
            updated = self.routes.set_sandbox_state_if_current(
                route,
                expected_states={"running", "waking", "parked"},
                state=state,
                node_epoch=fence.node_epoch,
                activity_epoch=fence.activity_epoch,
                **metadata,
            )
        except BaseException:
            self._compensate(route, candidate)
            raise
        if updated is None:
            self._compensate(route, candidate)
            raise LifecycleRouteChanged(
                "sandbox route changed while committing lifecycle state"
            )
        self.snapshots.release(route, keep_route=updated)
        return LifecycleCommit(updated)

    def wake(
        self,
        route: SandboxRoute,
        payload: Mapping[str, Any],
        *,
        program_transition: dict[str, Any] | None = None,
    ) -> LifecycleCommit:
        fence = self._fence(route, payload)
        try:
            updated, program = self.routes.confirm_sandbox_wake(
                route,
                node_epoch=fence.node_epoch,
                activity_epoch=fence.activity_epoch,
                program_transition=program_transition,
            )
        except BaseException:
            self._compensate(route)
            raise
        if updated is None:
            self._compensate(route)
            raise LifecycleRouteChanged("sandbox route changed while committing wake")
        self.snapshots.release(route, keep_route=updated)
        return LifecycleCommit(updated, program)


def route_with_snapshot_payload(
    route: SandboxRoute,
    payload: Mapping[str, Any],
    *,
    observation: SandboxInventoryEntry | None = None,
) -> SandboxRoute:
    """Canonical complete-publication validation for lifecycle and inventory."""
    snapshot = StorageNativeMigration.from_dict(payload.get("storage_snapshot"))
    if snapshot.sha256 != str(payload.get("snapshot_sha256") or ""):
        raise ValueError("worker snapshot digest does not match its descriptor")
    base = observation or SandboxInventoryEntry(
        sandbox_id=route.sandbox_id,
        generation=route.generation,
        operation_id=route.create_operation_id,
        spec_hash=route.spec_hash,
        state="parked",
        resources=route.resources,
    )
    item = replace(
        base,
        storage_schema=str(payload.get("storage_schema") or ""),
        snapshot_manifest_digest=str(payload.get("snapshot_manifest_digest") or ""),
        snapshot_repository=str(payload.get("snapshot_repository") or ""),
        snapshot_tag=str(payload.get("snapshot_tag") or ""),
        storage_snapshot=snapshot.to_dict(),
    )
    return route_with_inventory_snapshot(route, item)
