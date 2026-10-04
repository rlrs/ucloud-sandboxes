"""The process-wide gateway use cases one server shares across requests."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ..control_state import ControlStateStore
from ..images import ImageManager
from ..managed_registry import RegistryUsageStore
from ..metrics import MetricsStore
from ..registry_disk import RegistryDiskMonitor
from ..routing import RoutingStore
from ..telemetry import Telemetry
from .create import CreatePlacement
from .fleet import FleetView
from .groups import GroupCreate
from .heartbeats import HeartbeatIngest, RebootReaper, WorkerDelete
from .image_resolution import (
    REGISTRY_LAYER_METADATA_CACHE_MAX_ENTRIES, ImageResolution, RegistryLayerMetadataCache,
)
from .placement import InflightCreatePlacements, Placement
from .registry_refs import RegistryReferences


@dataclass(frozen=True)
class GatewayServices:
    """Built once by build_server and bound as the handler class's ``services``.

    Every request thread shares each member, so members hold process-wide
    state only; request-scoped I/O reaches a use case as an explicit Exchange.
    """

    registry_refs: RegistryReferences
    fleet: FleetView
    heartbeats: HeartbeatIngest
    placement: Placement
    creates: CreatePlacement
    groups: GroupCreate
    images: ImageResolution


def build_services(
    *, store: ControlStateStore, routing_store: RoutingStore, metrics_store: MetricsStore,
    telemetry: Telemetry, heartbeat_ttl_seconds: int, registry_url: str | None,
    registry_worker_url: str | None, registry_usage_store: RegistryUsageStore | None,
    registry_disk_monitor: RegistryDiskMonitor | None, image_manager: ImageManager,
    deployment_id: str, dependency_resolver: Any, create_target_concurrency_per_node: int,
    delete_on_worker: WorkerDelete | None, api_processes: int = 1,
) -> GatewayServices:
    """The single wiring of the use cases; build_server owns their lifetime.

    Without ``delete_on_worker`` (handlers built without workers) nothing reaps.
    """
    registry_refs = RegistryReferences(
        registry_url=registry_url, registry_worker_url=registry_worker_url,
        usage_store=registry_usage_store, deployment_id=deployment_id,
        dependency_resolver=dependency_resolver,
    )
    fleet = FleetView(store, heartbeat_ttl_seconds, registry_refs=registry_refs)
    # Heartbeats hydrate it and placement scores layer overlap from it.
    layer_cache = RegistryLayerMetadataCache(
        registry_url, registry_worker_url=registry_worker_url,
        max_entries=REGISTRY_LAYER_METADATA_CACHE_MAX_ENTRIES,
    ) if registry_url else None
    creates = CreatePlacement(
        routing_store, store, heartbeat_ttl_seconds=heartbeat_ttl_seconds,
        registry_refs=registry_refs, metrics_store=metrics_store, telemetry=telemetry,
        target_creates_per_node=create_target_concurrency_per_node,
        api_processes=api_processes,
    )
    return GatewayServices(
        registry_refs=registry_refs,
        fleet=fleet,
        heartbeats=HeartbeatIngest(
            store=store, routing_store=routing_store, metrics_store=metrics_store,
            deployment_id=deployment_id, registry_refs=registry_refs, layer_cache=layer_cache,
            reaper=RebootReaper(
                routing_store=routing_store, registry_refs=registry_refs,
                metrics_store=metrics_store, delete_on_worker=delete_on_worker,
            ) if delete_on_worker is not None else None,
        ),
        placement=Placement(
            routing_store, fleet, telemetry=telemetry,
            create_target_concurrency=create_target_concurrency_per_node,
            layer_cache=layer_cache, inflight=InflightCreatePlacements(),
        ),
        creates=creates,
        groups=GroupCreate(creates, target_creates_per_node=create_target_concurrency_per_node),
        images=ImageResolution(
            image_manager=image_manager, registry_url=registry_url,
            registry_worker_url=registry_worker_url, disk_monitor=registry_disk_monitor,
            fleet=fleet,
        ),
    )
