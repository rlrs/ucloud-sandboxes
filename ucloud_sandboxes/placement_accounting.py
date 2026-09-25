"""Pure placement resource accounting shared by gateway and placement authority.

Inventory credits are incarnation-specific. New route reservations are charged
until the corresponding worker observation includes them. No I/O or locks live
in this module.
"""

from dataclasses import dataclass
from typing import Any

from .capabilities import STORAGE_NATIVE_CAPABILITY
from .models import NodeHeartbeat, ResourceQuantity
from .routing import SandboxRoute, is_portable_parked_route
from .storage_native_migration import SUPPORTED_STORAGE_NATIVE_MIGRATION_SCHEMAS


@dataclass(frozen=True)
class PlacementReservation:
    reservation_id: str
    node_id: str
    job_id: str
    node_url: str
    resources: ResourceQuantity
    image: str
    state: str = "creating"


PlacementRecord = SandboxRoute | PlacementReservation


@dataclass(frozen=True)
class PlacementRouteIndex:
    """Route lookup tables built once for a gateway placement decision."""

    by_node_id: dict[str, tuple[PlacementRecord, ...]]
    by_job_id: dict[str, tuple[PlacementRecord, ...]]
    by_node_url: dict[str, tuple[PlacementRecord, ...]]

    def routes_for(self, heartbeat: NodeHeartbeat) -> list[PlacementRecord]:
        matches: list[PlacementRecord] = []
        seen: set[int] = set()
        keys = (
            self.by_node_id.get(heartbeat.node_id, ()),
            self.by_job_id.get(heartbeat.job_id, ()),
            self.by_node_url.get((heartbeat.node_url or "").rstrip("/"), ()),
        )
        for routes in keys:
            for route in routes:
                identity = id(route)
                if identity in seen:
                    continue
                seen.add(identity)
                matches.append(route)
        return matches


def _node_available_resources(
    heartbeat: NodeHeartbeat,
    routes: list[PlacementRecord],
) -> ResourceQuantity:
    route_reservations = _node_reserved_route_resources(heartbeat, routes)
    free = heartbeat.free_resources
    disk_mb = max(0, free.disk_mb - route_reservations.disk_mb)
    if not _node_has_storage_device_capacity(heartbeat, routes):
        disk_mb = 0
    return ResourceQuantity(
        vcpu=max(0.0, free.vcpu - route_reservations.vcpu),
        memory_mb=max(0, free.memory_mb - route_reservations.memory_mb),
        disk_mb=disk_mb,
    )


def _node_has_storage_device_capacity(
    heartbeat: NodeHeartbeat,
    routes: list[PlacementRecord],
) -> bool:
    metrics = heartbeat.runtime_metrics
    if (
        STORAGE_NATIVE_CAPABILITY not in heartbeat.capabilities
        or metrics is None
        or metrics.storage_ublk_max_devices <= 0
    ):
        return True
    return (
        metrics.storage_ublk_active_devices
        + _node_reserved_storage_device_slots(heartbeat, routes)
        < metrics.storage_ublk_max_devices
    )


def _node_reserved_storage_device_slots(
    heartbeat: NodeHeartbeat,
    routes: list[PlacementRecord],
) -> int:
    """Count assigned volumes not yet represented by backend ownership metrics."""

    inventory_by_identity = {
        (item.sandbox_id, item.generation, item.spec_hash, item.operation_id): item
        for item in heartbeat.inventory
    }
    seen: set[tuple[str, ...]] = set()
    reserved = 0
    for route in routes:
        if not _route_targets_node(route, heartbeat):
            continue
        identity = _placement_identity(route)
        if identity in seen:
            continue
        seen.add(identity)
        if isinstance(route, SandboxRoute):
            if route.worker_state == "detached" or route.state.lower() == "parked":
                continue
            observed = inventory_by_identity.get(
                (
                    route.sandbox_id,
                    route.generation,
                    route.spec_hash,
                    route.create_operation_id,
                )
            )
            if observed is not None:
                # A parked inventory entry has no active device. A wake
                # reserved after that observation must charge one until the
                # worker reports the restored owner in its next heartbeat.
                if (
                    route.state.lower() in {"waking", "running"}
                    and observed.state == "parked"
                ):
                    reserved += 1
                continue
            if route.resources.disk_mb > 0:
                reserved += 1
        elif route.resources.disk_mb > 0:
            reserved += 1
    return reserved


def _node_reserved_route_resources(
    heartbeat: NodeHeartbeat,
    routes: list[PlacementRecord],
) -> ResourceQuantity:
    resources = ResourceQuantity()
    seen_routes: set[tuple[str, ...]] = set()
    inventory_by_identity: dict[tuple[str, int, str, str], Any] = {}
    for item in heartbeat.inventory:
        inventory_by_identity.setdefault(
            (
                item.sandbox_id,
                item.generation,
                item.spec_hash,
                item.operation_id,
            ),
            item,
        )
    for route in routes:
        if not _route_targets_node(route, heartbeat):
            continue
        identity = _placement_identity(route)
        if identity in seen_routes:
            continue
        seen_routes.add(identity)
        if isinstance(route, PlacementReservation):
            resources = resources + route.resources
            continue
        if route.worker_state == "detached" and is_portable_parked_route(route):
            continue
        matching_inventory = inventory_by_identity.get(
            (
                route.sandbox_id,
                route.generation,
                route.spec_hash,
                route.create_operation_id,
            )
        )
        if matching_inventory is not None:
            if (
                route.state.lower() == "waking"
                and (matching_inventory.state or "unknown").lower() == "parked"
            ):
                storage_disk = (
                    route.resources.disk_mb
                    if route.storage_schema
                    in SUPPORTED_STORAGE_NATIVE_MIGRATION_SCHEMAS
                    and bool(route.snapshot_manifest_digest)
                    else 0
                )
                # Published parked inventory does not charge active disk, so
                # waking must reserve the attached writable volume.
                resources = resources + ResourceQuantity(
                    vcpu=route.resources.vcpu,
                    memory_mb=route.resources.memory_mb,
                    disk_mb=storage_disk,
                )
            continue
        if (
            route.state.lower() == "parked"
            and route.storage_schema in SUPPORTED_STORAGE_NATIVE_MIGRATION_SCHEMAS
            and route.snapshot_manifest_digest
        ):
            continue
        resources = resources + route.resources
    return resources


def _placement_route_index(routes: list[PlacementRecord]) -> PlacementRouteIndex:
    by_node_id: dict[str, list[PlacementRecord]] = {}
    by_job_id: dict[str, list[PlacementRecord]] = {}
    by_node_url: dict[str, list[PlacementRecord]] = {}
    for route in routes:
        if route.node_id:
            by_node_id.setdefault(route.node_id, []).append(route)
        if route.job_id:
            by_job_id.setdefault(route.job_id, []).append(route)
        if route.node_url:
            by_node_url.setdefault(route.node_url.rstrip("/"), []).append(route)
    return PlacementRouteIndex(
        by_node_id={key: tuple(value) for key, value in by_node_id.items()},
        by_job_id={key: tuple(value) for key, value in by_job_id.items()},
        by_node_url={key: tuple(value) for key, value in by_node_url.items()},
    )


def _route_targets_node(route: PlacementRecord, heartbeat: NodeHeartbeat) -> bool:
    return bool(
        (route.node_id and route.node_id == heartbeat.node_id)
        or (route.job_id and route.job_id == heartbeat.job_id)
        or (
            route.node_url
            and heartbeat.node_url
            and route.node_url.rstrip("/") == heartbeat.node_url.rstrip("/")
        )
    )


def _placement_identity(route: PlacementRecord) -> tuple[str, ...]:
    if isinstance(route, SandboxRoute):
        return (
            "sandbox",
            route.sandbox_id,
            str(route.generation),
            route.create_operation_id,
        )
    return ("migration", route.reservation_id)
