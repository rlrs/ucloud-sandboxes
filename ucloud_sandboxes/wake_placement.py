"""Wake placement use case, independent of HTTP request/response handling.

The routing store remains lifecycle authority. Capacity and migration decisions
run under the existing shared placement reservation; worker RPCs and migration
execution run after releasing it. Deferred work retains the existing demand or
migration reservation rather than adding a second queue.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from threading import RLock
import time
from typing import Any, Callable, ContextManager, Sequence
from uuid import uuid4

from .models import NodeHeartbeat, ResourceQuantity, utc_now
from .routing import (
    RoutingStore,
    SandboxMigration,
    SandboxRoute,
    is_portable_parked_route,
    wake_pending_demand_id,
)
from .wake_admission import PlacementOccupant, WakeAdmission


@dataclass(frozen=True)
class WakePlaced:
    route: SandboxRoute
    owner_changed: bool = False


@dataclass(frozen=True)
class WakeUnavailable:
    message: str
    error_code: str = ""
    retry_after: int = 5
    pending_resources: ResourceQuantity | None = None
    migration: SandboxMigration | None = None
    missing_sandbox_id: str = ""
    details: dict[str, Any] = field(default_factory=dict)


class WakePlacementStopped(Exception):
    def __init__(self, outcome: WakeUnavailable):
        super().__init__(outcome.message)
        self.outcome = outcome


class WakeSnapshotPublicationRequired(Exception):
    def __init__(self, route: SandboxRoute, pending_resources: ResourceQuantity):
        self.route, self.pending_resources = route, pending_resources


class WakeCapacityRefreshPending(Exception):
    pass


class WakeCapacityRefreshRequired(Exception):
    def __init__(self, route: SandboxRoute):
        self.route = route


@dataclass(frozen=True)
class WakePlacementPorts:
    """Existing capacity, worker and migration operations; none write HTTP."""

    reservation: Callable[[], ContextManager]
    owner: Callable[[str], NodeHeartbeat | None]
    occupants: Callable[[NodeHeartbeat], Sequence[PlacementOccupant]]
    destination: Callable[..., NodeHeartbeat | None]
    reserve_local: Callable[[SandboxRoute], SandboxRoute | None]
    finish_detach: Callable[[SandboxRoute], tuple[SandboxRoute | None, str]]
    advance_migration: Callable[..., SandboxMigration]
    refresh_capacity: Callable[[SandboxRoute], bool]
    publish: Callable[[SandboxRoute], dict[str, Any] | None]
    decode_publication: Callable[[SandboxRoute, dict[str, Any]], SandboxRoute | None]
    observe_owner: Callable[[NodeHeartbeat | None, Sequence[PlacementOccupant]], None]
    observe_consolidation: Callable[[SandboxRoute, SandboxMigration | None], None]
    atomic: Callable[[Callable[[], Any]], Any] | None = None


class WakePlacement:
    def __init__(
        self, routes: RoutingStore, admission: WakeAdmission, ports: WakePlacementPorts
    ):
        self.routes, self.admission, self.ports = routes, admission, ports

    def _atomic(self, operation):
        if self.ports.atomic is not None:
            return self.ports.atomic(operation)
        with self.ports.reservation():
            return operation()

    @staticmethod
    def changed() -> WakePlacementStopped:
        return WakePlacementStopped(
            WakeUnavailable("sandbox route changed during wake admission")
        )

    def current(self, requested: SandboxRoute) -> SandboxRoute:
        current = self.routes.get_sandbox_readonly(requested.sandbox_id)
        if current is None:
            raise WakePlacementStopped(
                WakeUnavailable(
                    "sandbox route not found", missing_sandbox_id=requested.sandbox_id
                )
            )
        if (
            not self.admission.same_incarnation(current, requested)
            or current.delete_operation_id
        ):
            raise self.changed()
        return current

    def place(self, requested: SandboxRoute) -> WakePlaced | WakeUnavailable:
        try:
            current = self.current(requested)
            if current.state != "parked":
                return WakePlaced(current)
            try:
                try:
                    return self._placed(
                        current, self.reserve(current, refresh_if_blocked=True)
                    )
                except WakeCapacityRefreshRequired as blocked:
                    self.ports.refresh_capacity(blocked.route)
                    return self._placed(current, self.reserve(blocked.route))
            except WakeCapacityRefreshPending:
                return WakeUnavailable(
                    "source node capacity is being refreshed",
                    error_code="node_active_exec_deferred",
                    retry_after=1,
                )
            except WakeSnapshotPublicationRequired as pending:
                # Publication is useful only after proving another node can
                # admit this checkpoint; never hold placement locks over RPCs.
                try:
                    payload = self.ports.publish(pending.route)
                    if payload is not None:
                        published = self.accept_publication(pending.route, payload)
                        if published is not None:
                            return self._placed(current, self.reserve(published))
                except WakeSnapshotPublicationRequired as newer:
                    # Another wake/repark can supersede the accepted capture
                    # before reservation. One request attempts one publication;
                    # the next safe retry evaluates the new generation of work.
                    pending = newer
                except (OSError, ValueError):
                    pass
                return WakeUnavailable(
                    "parked snapshot publication is still in progress",
                    error_code="snapshot_publication_pending",
                    retry_after=1,
                    pending_resources=pending.pending_resources,
                )
        except WakePlacementStopped as stopped:
            return stopped.outcome

    @staticmethod
    def _placed(before: SandboxRoute, after: SandboxRoute) -> WakePlaced:
        return WakePlaced(
            after,
            (before.node_id, before.job_id, before.node_url)
            != (after.node_id, after.job_id, after.node_url),
        )

    def accept_publication(
        self, route: SandboxRoute, payload: dict[str, Any]
    ) -> SandboxRoute | None:
        observed = self.ports.decode_publication(route, payload)
        if observed is None or not is_portable_parked_route(observed):
            return None
        def accept():
            current = self.routes.get_sandbox_readonly(route.sandbox_id)
            if (
                current is None
                or current.state != "parked"
                or current.delete_operation_id
                or not self.admission.same_owner(current, route)
                or current.worker_state != route.worker_state
            ):
                return None
            observed = self.ports.decode_publication(current, payload)
            if (
                observed is None
                or not is_portable_parked_route(observed)
                or (current.node_epoch and observed.node_epoch != current.node_epoch)
                or observed.activity_epoch < current.activity_epoch
            ):
                return None
            accepted = self.routes.upsert_sandbox(observed)
            # Inventory can win the route writer after our read. upsert returns
            # that newer route on a rejected observation, not proof of a commit.
            if (
                not is_portable_parked_route(accepted)
                or not self.admission.same_owner(accepted, observed)
                or accepted.node_epoch != observed.node_epoch
                or accepted.activity_epoch != observed.activity_epoch
                or (
                    accepted.storage_schema,
                    accepted.snapshot_manifest_digest,
                    accepted.snapshot_repository,
                    accepted.snapshot_tag,
                )
                != (
                    observed.storage_schema,
                    observed.snapshot_manifest_digest,
                    observed.snapshot_repository,
                    observed.snapshot_tag,
                )
            ):
                return None
            return accepted

        return self._atomic(accept)

    def mark_waking(self, route: SandboxRoute) -> SandboxRoute:
        waking = self.admission.reserve_current(route)
        if waking is None:
            raise self.changed()
        return waking

    def _demand(self, route: SandboxRoute, reason: str) -> ResourceQuantity:
        _, demand = self.routes.upsert_pending_with_demand(
            wake_pending_demand_id(route.sandbox_id),
            route.resources,
            failure_reason=reason,
        )
        return demand.pending_resources

    def reserve(
        self, requested: SandboxRoute, *, refresh_if_blocked: bool = False
    ) -> SandboxRoute:
        route = requested
        if route.worker_state == "attached":
            local = self.ports.reserve_local(route)
            if local is not None:
                return local
        if route.worker_state == "detaching":
            route = self.current(route)
            detached, message = self.ports.finish_detach(route)
            if detached is None:
                raise WakePlacementStopped(
                    WakeUnavailable(message or "worker detach is incomplete")
                )
            if not self.admission.same_incarnation(detached, route):
                raise self.changed()
            route = detached
        def plan(route=route):
            route = self.current(route)
            if route.state in {"waking", "running"}:
                return route
            if route.state != "parked":
                raise self.changed()
            owner = self.ports.owner(route.job_id)
            occupants = self.ports.occupants(owner) if owner is not None else []
            self.ports.observe_owner(owner, occupants)
            request = ResourceQuantity(
                vcpu=route.resources.vcpu, memory_mb=route.resources.memory_mb
            )
            active_migration = next(
                iter(
                    self.routes.sandbox_migrations(
                        active_only=True, sandbox_id=route.sandbox_id
                    )
                ),
                None,
            )
            local_can_wake = bool(
                route.worker_state == "attached"
                and owner is not None
                and self.admission.owner_ready(owner)
                and self.admission.can_admit(owner, occupants, request)
            )
            if (
                not local_can_wake
                and route.worker_state == "attached"
                and refresh_if_blocked
            ):
                raise WakeCapacityRefreshRequired(route)
            consolidation = None
            if (
                local_can_wake
                and active_migration is None
                and self.admission.consolidation_enabled
            ):
                consolidation = self.ports.destination(
                    route, consolidation_source=owner
                )
            if local_can_wake and active_migration is None and consolidation is None:
                return self.mark_waking(route)
            if route.worker_state == "attached" and not is_portable_parked_route(route):
                if (
                    owner is not None
                    and owner.runtime_metrics is not None
                    and owner.runtime_metrics.storage_error_volumes > 0
                ):
                    self._demand(route, "wake_storage_recovery_required")
                    raise WakePlacementStopped(
                        WakeUnavailable(
                            "source node storage requires recovery before this sandbox can wake",
                            error_code="storage_recovery_required",
                        )
                    )
                if self.ports.destination(route) is None:
                    demand = self._demand(route, "wake_destination_unavailable")
                    raise WakePlacementStopped(
                        WakeUnavailable(
                            "waiting for local or destination wake capacity",
                            error_code="wake_destination_unavailable",
                            retry_after=1,
                            pending_resources=demand,
                        )
                    )
                raise WakeSnapshotPublicationRequired(
                    route, self._demand(route, "wake_snapshot_publication_pending")
                )
            if active_migration is None:
                destination = consolidation or self.ports.destination(route)
                if destination is None:
                    demand = self._demand(route, "wake_destination_unavailable")
                    raise WakePlacementStopped(
                        WakeUnavailable(
                            "parked sandbox has no node with active CPU, memory, and disk capacity",
                            pending_resources=demand,
                        )
                    )
                active_migration = self.routes.begin_sandbox_migration(
                    route,
                    migration_id=f"{'consolidate-wake' if consolidation else 'wake'}-{uuid4().hex}",
                    destination_node_id=destination.node_id,
                    destination_job_id=destination.job_id,
                    destination_node_url=destination.node_url or "",
                )
            return active_migration

        planned = self._atomic(plan)
        if isinstance(planned, SandboxRoute):
            return planned
        active_migration = planned
        if active_migration.migration_id.startswith("consolidate-wake-"):
            self.ports.observe_consolidation(route, active_migration)
        # The durable migration is the destination claim; global placement may
        # now progress during image preparation, transfer, import and activation.
        self.routes.clear_pending(wake_pending_demand_id(route.sandbox_id))
        migration = self.ports.advance_migration(
            active_migration, wake_on_complete=True
        )
        if migration.phase != "complete":
            raise WakePlacementStopped(
                WakeUnavailable(
                    migration.error or "parked sandbox relocation is incomplete",
                    migration=migration,
                )
            )
        if migration.migration_id.startswith("consolidate-wake-"):
            self.ports.observe_consolidation(route, None)
        # A completed journal is not permission to wake a replacement ID.
        return self.mark_waking(self.current(route))


class BlockedOwnerRefresh:
    """Coalesce existing on-demand refreshes, scoped to the exact worker boot."""

    _guard = RLock()
    _refreshes: dict[tuple[str, str, str], tuple[float, bool]] = {}

    @classmethod
    def refresh(
        cls,
        route: SandboxRoute,
        *,
        routes: RoutingStore,
        admission: WakeAdmission,
        read_worker: Callable[[SandboxRoute], NodeHeartbeat | None],
        receive: Callable[[NodeHeartbeat], None],
    ) -> bool:
        previous = admission.read_owner(route.job_id)
        if previous is None:
            return False
        occupants = admission.read_placement(previous)
        requested = ResourceQuantity(
            vcpu=route.resources.vcpu, memory_mb=route.resources.memory_mb
        )
        if (
            previous.is_fresh(utc_now(), admission.heartbeat_ttl_seconds)
            and previous.admission_open
            and not previous.draining
            and admission.can_admit(previous, occupants, requested)
        ):
            return False
        key = str(routes.path), route.job_id, previous.node_epoch
        now = time.monotonic()
        with cls._guard:
            for old_key, (finished, active) in list(cls._refreshes.items()):
                if not active and now - finished > 120:
                    del cls._refreshes[old_key]
            finished, active = cls._refreshes.get(key, (0, False))
            if active:
                raise WakeCapacityRefreshPending()
            if now - finished < 2:
                return False
            cls._refreshes[key] = now, True
        try:
            current = read_worker(route)
            if current is None or (
                current.node_id,
                current.job_id,
                current.node_epoch,
                current.deployment_id,
                current.agent_version,
            ) != (
                previous.node_id,
                previous.job_id,
                previous.node_epoch,
                previous.deployment_id,
                previous.agent_version,
            ):
                return False
            received_at = utc_now()
            receive(
                replace(
                    current,
                    node_url=previous.node_url,
                    received_at=received_at,
                    updated_at=received_at,
                    reported_at=current.reported_at or current.updated_at,
                    idle_since=None,
                )
            )
            return True
        except (OSError, ValueError, TypeError):
            return False
        finally:
            with cls._guard:
                cls._refreshes[key] = time.monotonic(), False
