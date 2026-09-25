"""Authoritative same-owner wake admission, without HTTP or worker I/O.

Callers serialize reserve_batch with the existing gateway placement reservation,
shared with creates and migrations. The service projects every accepted wake into
that one owner view before committing the batch, so concurrent demand cannot
spend the same advertised memory or device slot twice.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Callable, Protocol, Sequence

from .models import NodeHeartbeat, ResourceQuantity, utc_now
from .routing import (
    RoutingStore,
    SandboxRoute,
    is_portable_parked_route,
    wake_pending_demand_id,
)


class PlacementOccupant(Protocol):
    """The accounting shape shared by committed routes and launch reservations."""

    state: str
    resources: ResourceQuantity


@dataclass(frozen=True)
class WakeOwnerView:
    owner: NodeHeartbeat
    occupants: tuple[PlacementOccupant, ...]


@dataclass(frozen=True)
class WakeAdmissionDecision:
    # None requires the normal refresh/publication/relocation decision. It is
    # never permission to dispatch an unreserved worker wake.
    route: SandboxRoute | None = None
    owner_view: WakeOwnerView | None = None


class WakeAdmission:
    def __init__(
        self,
        routes: RoutingStore,
        *,
        read_owner: Callable[[str], NodeHeartbeat | None],
        read_placement: Callable[[NodeHeartbeat], Sequence[PlacementOccupant]],
        can_admit: Callable[
            [NodeHeartbeat, Sequence[PlacementOccupant], ResourceQuantity], bool
        ],
        heartbeat_ttl_seconds: float,
        consolidation_enabled: bool,
    ) -> None:
        self.routes = routes
        self.read_owner = read_owner
        self.read_placement = read_placement
        self.can_admit = can_admit
        self.heartbeat_ttl_seconds = heartbeat_ttl_seconds
        self.consolidation_enabled = consolidation_enabled

    def reserve_batch(
        self, requested_routes: Sequence[SandboxRoute]
    ) -> list[WakeAdmissionDecision]:
        """Revalidate, project capacity and atomically reserve under placement lock."""
        views: dict[str, WakeOwnerView | None] = {}
        admitted: dict[str, SandboxRoute] = {}
        decisions = []
        active_migrations = {
            migration.sandbox_id
            for migration in self.routes.sandbox_migrations(
                active_only=True, sandbox_ids=(route.sandbox_id for route in requested_routes),
            )
        }
        for requested in requested_routes:
            if requested.job_id not in views:
                owner = self.read_owner(requested.job_id)
                views[requested.job_id] = (
                    WakeOwnerView(owner, tuple(self.read_placement(owner)))
                    if owner is not None
                    else None
                )
            view = views[requested.job_id]
            current = (
                next(
                    (
                        route
                        for route in view.occupants
                        if isinstance(route, SandboxRoute)
                        and route.sandbox_id == requested.sandbox_id
                    ),
                    None,
                )
                if view is not None
                else None
            )
            decision = WakeAdmissionDecision()
            if (
                current is not None
                and self.same_owner(current, requested)
                and not current.delete_operation_id
            ):
                if current.state in {"running", "waking"}:
                    decision = WakeAdmissionDecision(current)
                elif (
                    current.state == "parked"
                    and current.worker_state == "attached"
                    and current.sandbox_id not in active_migrations
                    and self.owner_ready(view.owner)
                    and not (
                        is_portable_parked_route(current) and self.consolidation_enabled
                    )
                    and self.can_admit(
                        view.owner,
                        view.occupants,
                        ResourceQuantity(
                            vcpu=current.resources.vcpu,
                            memory_mb=current.resources.memory_mb,
                        ),
                    )
                ):
                    admitted[current.sandbox_id] = current
                    waking = replace(current, state="waking")
                    decision = WakeAdmissionDecision(waking, view)
                    views[requested.job_id] = replace(
                        view,
                        occupants=tuple(
                            waking
                            if isinstance(route, SandboxRoute)
                            and route.sandbox_id == current.sandbox_id
                            else route
                            for route in view.occupants
                        ),
                    )
            decisions.append(decision)
        committed = self.routes.reserve_sandbox_wakes(
            [
                (route, wake_pending_demand_id(route.sandbox_id))
                for route in admitted.values()
            ]
        )
        return [
            replace(decision, route=committed.get(decision.route.sandbox_id))
            if decision.route is not None and decision.route.sandbox_id in admitted
            else decision
            for decision in decisions
        ]

    @staticmethod
    def same_incarnation(current: SandboxRoute, requested: SandboxRoute) -> bool:
        return (
            current.sandbox_id,
            current.generation,
            current.create_operation_id,
            current.spec_hash,
        ) == (
            requested.sandbox_id,
            requested.generation,
            requested.create_operation_id,
            requested.spec_hash,
        )

    @classmethod
    def same_owner(cls, current: SandboxRoute, requested: SandboxRoute) -> bool:
        return cls.same_incarnation(current, requested) and (
            current.node_id,
            current.job_id,
            current.node_url,
        ) == (
            requested.node_id,
            requested.job_id,
            requested.node_url,
        )

    def owner_ready(self, owner: NodeHeartbeat) -> bool:
        return bool(
            owner.node_url
            and not owner.draining
            and owner.admission_open
            and "sandbox" in owner.capabilities
            and owner.is_fresh(utc_now(), self.heartbeat_ttl_seconds)
        )

    def reserve_current(self, route: SandboxRoute) -> SandboxRoute | None:
        """Commit a separately planned wake without replacing a competing route."""
        waking = self.routes.reserve_sandbox_wake(
            route, pending_id=wake_pending_demand_id(route.sandbox_id)
        )
        if waking is not None:
            return waking
        current = self.routes.get_sandbox_readonly(route.sandbox_id)
        if (
            current is not None
            and not current.delete_operation_id
            and self.same_incarnation(current, route)
            and (current.state or "unknown").lower() in {"waking", "running"}
        ):
            return current
        return None
