"""Rank ready workers and durably reserve one for a create, wake or migration."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, replace
import fcntl
from pathlib import Path
from threading import RLock
import time
from typing import Any, Callable

from ..admission import FairRLock
from ..capabilities import (
    DISK_QUOTA_CAPABILITY, ENVIRONMENT_CONTRACT_CAPABILITY, HIBERNATE_LOCAL_CAPABILITY,
    MANAGED_PRIMARY_CAPABILITY, STATIC_FILE_MANAGEMENT_CAPABILITY, has_capability,
)
from ..control_state import detached_heartbeat
from ..deployment import agent_version_is_schedulable
from ..managed_registry import RegistryManifestLayers, canonical_image_digest_ref
from ..models import NodeHeartbeat, ResourceQuantity, is_soft_drained
from ..network_policy import SandboxNetworkPolicy
from ..placement_accounting import (
    PlacementRecord, PlacementReservation, _node_available_resources,
    _node_has_storage_device_capacity, _placement_route_index, _route_targets_node,
)
from ..resource_admission import node_accepts_dynamic_request, node_pressure_score
from ..routing import PendingSandboxDemand, RoutingStore, SandboxRoute, SandboxRouteAllocation
from ..sandbox import SandboxSpec
from ..telemetry import Telemetry
from ..wake_placement import (
    WakeCapacityRefreshPending, WakeCapacityRefreshRequired, WakePlacementStopped,
    WakeSnapshotPublicationRequired,
)
from .fleet import FleetView, _requested_image_cache_keys
from .image_resolution import RegistryLayerMetadataCache

# Process-wide, taken before the host-wide placement flock: every server in
# this process queues fairly behind one reservation at a time.
_GATEWAY_SCHEDULING_LOCK = FairRLock()
# Load (0..1 pressure plus in-flight creates per target concurrency) below
# which image locality outranks spreading.
_AFFINITY_LOAD_BAND = 0.6
# Treat each additional distinct cold image like 256 MiB of missing transfer.
# For the observed ~1.1 GiB shared TMax base this spreads after roughly four
# concurrent related pulls instead of concentrating an entire burst on one node.
COLD_PULL_PRESSURE_PENALTY_BYTES = 256 * 1024 * 1024


class GatewaySchedulingBusyError(RuntimeError):
    """Placement serialization is occupied and the caller should retry."""


@dataclass(frozen=True)
class NodePlacementState:
    """Per-node route accounting reused throughout one placement decision."""

    available_resources: ResourceQuantity
    inflight_image_identities: frozenset[str]
    projected_image_identities: frozenset[str]
    active_creates: int
    assigned_shape_pressure: float = 0.0
    assigned_vcpu: float = 0.0
    assigned_memory_mb: int = 0


class InflightCreatePlacements:
    """Selections made by this process whose reservation has not committed.

    Concurrent creates rank workers from the same committed snapshot, so they
    would all choose the same least-assigned worker and queue behind its
    placement turn. Ranking and claiming under one short in-process lock lets
    each selection observe its predecessors. This only steers ranking: fit
    checks and the reservation transaction still use committed routes.
    """

    def __init__(self):
        self._lock = RLock()
        self._claims: dict[str, dict[str, ResourceQuantity]] = {}

    @contextmanager
    def ranking(self):
        with self._lock:
            yield

    def adjusted(
        self,
        heartbeat: NodeHeartbeat,
        state: NodePlacementState,
        committed_ids: frozenset[str] | set[str],
    ) -> NodePlacementState:
        claims = [
            resources for sandbox_id, resources
            in self._claims.get(heartbeat.job_id, {}).items()
            if sandbox_id not in committed_ids
        ]
        if not claims:
            return state
        total = heartbeat.total_resources
        vcpu = state.assigned_vcpu + sum(item.vcpu for item in claims)
        memory_mb = state.assigned_memory_mb + sum(item.memory_mb for item in claims)
        return replace(
            state,
            assigned_vcpu=vcpu,
            assigned_memory_mb=memory_mb,
            assigned_shape_pressure=max(
                vcpu / max(1, total.vcpu), memory_mb / max(1, total.memory_mb),
            ),
            active_creates=state.active_creates + len(claims),
        )

    def claim(self, job_id: str, sandbox_id: str, resources: ResourceQuantity) -> None:
        with self._lock:
            self._claims.setdefault(job_id, {})[sandbox_id] = resources

    def release(self, job_id: str, sandbox_id: str) -> None:
        with self._lock:
            claims = self._claims.get(job_id)
            if claims is not None:
                claims.pop(sandbox_id, None)
                if not claims:
                    del self._claims[job_id]


class Placement:
    """Choose a worker from fresh heartbeats and committed route accounting.

    One instance per server, shared by every request thread: it holds stores,
    configuration and the in-process claim ledger only. Ranking is advisory;
    each reservation rechecks fit under the scheduling lock and flock (SQLite)
    or inside the placement transaction (PostgreSQL).
    """

    def __init__(
        self, routing_store: RoutingStore, fleet: FleetView, *, telemetry: Telemetry,
        create_target_concurrency: int, layer_cache: RegistryLayerMetadataCache | None,
        inflight: InflightCreatePlacements | None,
        image_locality: Callable[..., set[str]] | None = None,
    ) -> None:
        self.routing_store = routing_store
        self.fleet = fleet
        self.telemetry = telemetry
        self.create_target_concurrency = create_target_concurrency
        self.layer_cache = layer_cache
        self.inflight = inflight
        # Ranking reads only heartbeat caches; probing workers whose cache is
        # unknown costs node RPCs and belongs to image distribution.
        self.image_locality = image_locality or fleet.nodes_with_cached_image

    def select(
        self,
        requested: ResourceQuantity,
        *,
        image: str | None = None,
        required_capabilities: tuple[str, ...] = (),
        excluded_job_ids: tuple[str, ...] = (),
        claim_sandbox_id: str | None = None,
    ) -> NodeHeartbeat | None:
        """Rank candidates; optionally claim the choice for an in-flight create.

        A claim must be released by the caller once its reservation settles.
        """
        started = time.monotonic()
        routes = self.routes()
        route_index = _placement_route_index(routes)
        routes_read = time.monotonic()
        # Ranking only reads inventories; skip per-entry defensive copies.
        heartbeats = self.fleet.ready_sandbox_heartbeats(shared=True)
        heartbeats_read = time.monotonic()
        excluded_jobs = frozenset(excluded_job_ids)
        candidate_states: list[tuple[NodeHeartbeat, NodePlacementState]] = []
        for heartbeat in heartbeats:
            if heartbeat.job_id in excluded_jobs:
                continue
            if not heartbeat.admission_open:
                continue
            if not agent_version_is_schedulable(heartbeat.agent_version):
                continue
            if not all(
                has_capability(heartbeat.capabilities, capability)
                for capability in required_capabilities
            ):
                continue
            placement_state = _node_placement_state(heartbeat, route_index.routes_for(heartbeat))
            if not _node_can_fit_available(
                heartbeat, requested, placement_state.available_resources, check_cpu=False,
            ):
                continue
            candidate_states.append((heartbeat, placement_state))
        self.telemetry.add_event("gateway.placement.scan", {
            "routes_read_ms": (routes_read - started) * 1000,
            "heartbeats_read_ms": (heartbeats_read - routes_read) * 1000,
            "candidate_evaluation_ms": (time.monotonic() - heartbeats_read) * 1000,
            "route_count": len(routes), "node_count": len(heartbeats),
            "candidate_count": len(candidate_states),
        })
        if not candidate_states:
            return None
        image_node_ids = self.image_locality(
            image or "", [heartbeat for heartbeat, _state in candidate_states],
        )
        image_identity = canonical_image_digest_ref(image or "") or (image or "").strip()
        inflight_image_node_ids = {
            heartbeat.node_id
            for heartbeat, state in candidate_states
            if image_identity and image_identity in state.inflight_image_identities
        }
        layer_cache = self.layer_cache
        target_manifest = layer_cache.get(image or "") if layer_cache is not None else None
        inflight = self.inflight
        if inflight is None:
            return detached_heartbeat(self.rank(
                candidate_states, requested, image, image_node_ids,
                inflight_image_node_ids, target_manifest,
            ))
        committed_ids = {route.sandbox_id for route in routes if isinstance(route, SandboxRoute)}
        with inflight.ranking():
            chosen = self.rank(
                [(heartbeat, inflight.adjusted(heartbeat, state, committed_ids))
                 for heartbeat, state in candidate_states],
                requested, image, image_node_ids, inflight_image_node_ids, target_manifest,
            )
            if claim_sandbox_id is not None:
                inflight.claim(chosen.job_id, claim_sandbox_id, requested)
        # The winner leaves ranking; never hand out the shared cached object.
        return detached_heartbeat(chosen)

    def rank(
        self, candidate_states, requested, image, image_node_ids,
        inflight_image_node_ids, target_manifest,
    ) -> NodeHeartbeat:
        def rank(item):
            heartbeat, state = item
            # Live pressure plus in-flight creates, which heartbeats do not
            # show yet: a burst overflows a node once its creates approach the
            # per-node target, before a stale heartbeat could funnel it.
            load = node_pressure_score(heartbeat) + state.active_creates / max(
                1, self.create_target_concurrency
            )
            busy = load >= _AFFINITY_LOAD_BAND
            return (
                # A soft-drained worker is emptying: only a last resort, so a
                # create never fails because of soft drain.
                is_soft_drained(heartbeat),
                busy,
                # Busy nodes keep the prior order: durable assigned shapes
                # first, because a cached startup spike must not funnel a
                # burst onto one peer while completed creates are not yet in
                # any heartbeat. Pressure then chooses among them.
                state.assigned_shape_pressure if busy else 0.0,
                load if busy else 0.0,
                # Below the band, prefer a node that already holds the image,
                # then the fewest missing layers: every avoided pull saves
                # time and image-store space (docs/image-placement.md).
                (0 if heartbeat.node_id in image_node_ids else
                 1 if heartbeat.node_id in inflight_image_node_ids else 2),
                _cold_image_placement_cost_for_state(
                    state, target_manifest, self.layer_cache, spread_cold_image=bool(image),
                ),
                # Requested shapes are maximums, not load; they only spread
                # otherwise equivalent nodes.
                state.assigned_shape_pressure,
                load,
                state.active_creates,
                _resource_slack(state.available_resources, requested),
                heartbeat.node_id,
            )

        return min(candidate_states, key=rank)[0]

    def alternate_available(self, spec: SandboxSpec, *, excluded_job_ids: tuple[str, ...]) -> bool:
        return self.select(
            spec.requested_resources(), image=spec.image,
            required_capabilities=_sandbox_required_capabilities(spec.to_dict()),
            excluded_job_ids=excluded_job_ids,
        ) is not None

    def routes(self) -> list[PlacementRecord]:
        """Include in-flight destination imports in normal node admission."""

        routes: list[PlacementRecord] = list(self.routing_store.placement_routes_readonly())
        routes_by_id = {route.sandbox_id: route for route in routes}
        for migration in self.routing_store.sandbox_migrations(active_only=True):
            source = routes_by_id.get(migration.sandbox_id)
            if source is None:
                continue
            # Before route commit the destination may already be allocating
            # quota and restoring metadata, while its heartbeat still has no
            # observation. Reserve the complete shape. After route commit the
            # parked route owns disk itself, but a wake relocation still needs
            # its CPU/RAM reservation through activation. Completion can then
            # atomically turn that parked route into ``waking``.
            reservation = source.resources
            if migration.phase in {"routed", "activated"}:
                reservation = ResourceQuantity(
                    vcpu=source.resources.vcpu, memory_mb=source.resources.memory_mb,
                )
            routes.append(PlacementReservation(
                reservation_id=migration.migration_id, node_id=migration.destination_node_id,
                job_id=migration.destination_job_id, node_url=migration.destination_node_url,
                resources=reservation, image=str(source.spec.get("image") or ""),
            ))
        return routes

    def routes_for_node(self, heartbeat: NodeHeartbeat) -> list[PlacementRecord]:
        """Read fresh owner admission state, including incoming migrations."""

        routes: list[PlacementRecord] = list(
            self.routing_store.sandbox_routes_matching_node_identity(
                node_id=heartbeat.node_id, job_id=heartbeat.job_id,
                node_url=heartbeat.node_url or "",
            )
        )
        by_id = {route.sandbox_id: route for route in routes}
        for migration in self.routing_store.sandbox_migrations(
            active_only=True,
            destination_identity=(heartbeat.node_id, heartbeat.job_id, heartbeat.node_url or ""),
        ):
            destination = PlacementReservation(
                reservation_id=migration.migration_id,
                node_id=migration.destination_node_id,
                job_id=migration.destination_job_id,
                node_url=migration.destination_node_url,
                resources=ResourceQuantity(), image="",
            )
            if not _route_targets_node(destination, heartbeat):
                continue
            source = by_id.get(migration.sandbox_id)
            if source is None:
                source = self.routing_store.get_sandbox_readonly(migration.sandbox_id)
            if source is None:
                continue
            resources = source.resources
            if migration.phase in {"routed", "activated"}:
                resources = ResourceQuantity(
                    vcpu=resources.vcpu, memory_mb=resources.memory_mb,
                )
            routes.append(replace(
                destination, resources=resources,
                image=str(source.spec.get("image") or ""),
            ))
        return routes

    def select_and_reserve(
        self,
        sandbox_id: str,
        requested: ResourceQuantity,
        *,
        image: str | None = None,
        spec: dict[str, Any],
        spec_hash: str,
        excluded_job_ids: tuple[str, ...] = (),
        lock_timeout: float,
    ) -> tuple[NodeHeartbeat, SandboxRoute, PendingSandboxDemand | None] | None:
        """``lock_timeout`` is the caller's admission wait, read per request."""
        if self.routing_store.distributed:
            excluded = set(excluded_job_ids)
            while True:
                # Ranking and image-cache work are advisory and happen before
                # the transaction. Admission rechecks only the chosen worker.
                heartbeat = self.select(requested, image=image,
                    required_capabilities=_sandbox_required_capabilities(spec),
                    excluded_job_ids=tuple(excluded), claim_sandbox_id=sandbox_id)
                if heartbeat is None:
                    return None
                def reserve():
                    existing = self.routing_store.get_sandbox_readonly(sandbox_id)
                    if existing is None:
                        occupants = self.routes_for_node(heartbeat)
                        if (not _node_has_storage_device_capacity(heartbeat, occupants)
                                or not _node_can_fit_available(heartbeat, requested,
                                    _node_available_resources(heartbeat, occupants), check_cpu=False)):
                            return None
                    route,pending = self.routing_store.allocate_sandbox_create_with_pending(
                        SandboxRouteAllocation(sandbox_id=sandbox_id,node_id=heartbeat.node_id,
                            job_id=heartbeat.job_id,node_url=heartbeat.node_url or '',
                            resources=requested,spec=dict(spec),node_epoch=heartbeat.node_epoch,
                            activity_epoch=heartbeat.activity_epoch),spec_hash=spec_hash)
                    owner = heartbeat if route.job_id == heartbeat.job_id else self.fleet.store.get_heartbeat(route.job_id)
                    if owner is None:
                        raise GatewaySchedulingBusyError('reserved worker is unavailable')
                    return owner,route,pending
                try:
                    reserved = self.atomic(reserve, worker_id=heartbeat.job_id)
                finally:
                    if self.inflight is not None:
                        self.inflight.release(heartbeat.job_id, sandbox_id)
                if reserved is not None:
                    return reserved
                excluded.add(heartbeat.job_id)
        started = time.monotonic()
        # Creates already hold bounded startup admission. Queue fairly with
        # wakes/migrations rather than abandoning the reservation after 250 ms
        # and making the client repeat image resolution and HTTP admission.
        if not _GATEWAY_SCHEDULING_LOCK.acquire(timeout=lock_timeout):
            raise GatewaySchedulingBusyError("sandbox placement is already being reserved")
        acquired = time.monotonic()
        try:
            self.telemetry.add_event("gateway.placement.lock", {
                "wait_ms": (acquired - started) * 1000,
            })
            with _gateway_placement_lock(self.routing_store.path, blocking=False):
                heartbeat = self.select(
                    requested, image=image,
                    required_capabilities=_sandbox_required_capabilities(spec),
                    excluded_job_ids=excluded_job_ids,
                )
                if heartbeat is None:
                    return None
                reservation_started = time.monotonic()
                route, pending = self.routing_store.allocate_sandbox_create_with_pending(
                    SandboxRouteAllocation(
                        sandbox_id=sandbox_id, node_id=heartbeat.node_id,
                        job_id=heartbeat.job_id, node_url=heartbeat.node_url or "",
                        resources=requested, spec=dict(spec), node_epoch=heartbeat.node_epoch,
                        activity_epoch=heartbeat.activity_epoch,
                    ),
                    spec_hash=spec_hash,
                )
                self.telemetry.add_event("gateway.placement.reservation", {
                    "duration_ms": (time.monotonic() - reservation_started) * 1000,
                })
                return heartbeat, route, pending
        finally:
            held = time.monotonic() - acquired
            _GATEWAY_SCHEDULING_LOCK.release()
            self.telemetry.add_event("gateway.placement.release", {"hold_ms": held * 1000})

    def atomic(self, operation, *, worker_id=None):
        if not self.routing_store.distributed:
            with self.reservation():
                return operation()
        with self.telemetry.span("gateway.placement.transaction"):
            return self.routing_store.run_placement(operation, worker_id=worker_id, outcomes=(
                WakePlacementStopped, WakeCapacityRefreshRequired,
                WakeCapacityRefreshPending, WakeSnapshotPublicationRequired,
            ))

    @contextmanager
    def reservation(self):
        with self.telemetry.span("gateway.wake.reserve_placement") as span:
            started = time.monotonic()
            with _GATEWAY_SCHEDULING_LOCK:
                process_acquired = time.monotonic()
                with _gateway_placement_lock(self.routing_store.path):
                    acquired = time.monotonic()
                    span.set_attribute("gateway.placement.lock_wait_seconds", acquired - started)
                    span.set_attribute("gateway.placement.process_lock_wait_seconds", process_acquired - started)
                    span.set_attribute("gateway.placement.file_lock_wait_seconds", acquired - process_acquired)
                    try:
                        yield span
                    finally:
                        span.set_attribute("gateway.placement.lock_hold_seconds", time.monotonic() - acquired)


def _sandbox_required_capabilities(spec: dict[str, Any]) -> tuple[str, ...]:
    capabilities = []
    policy = SandboxNetworkPolicy.from_dict(spec.get("network_policy", {}))
    if policy.egress == "relay":
        capabilities.append(policy.capability)
    if bool(spec.get("parkable")):
        capabilities.extend((HIBERNATE_LOCAL_CAPABILITY, DISK_QUOTA_CAPABILITY))
        if bool(spec.get("managed_process")):
            capabilities.append(MANAGED_PRIMARY_CAPABILITY)
    filesystem = spec.get("filesystem") or {}
    security = spec.get("security") or {}
    if (
        spec.get("profile") == "linux_session"
        or spec.get("required_features")
        or spec.get("dns_servers")
        or security.get("supplementary_groups")
        or filesystem.get("shm_mb", 64) != 64
        or filesystem.get("workspace_storage") is not None
        or filesystem.get("management_helper", "shell") != "shell"
    ):
        capabilities.append(ENVIRONMENT_CONTRACT_CAPABILITY)
    if filesystem.get("management_helper") == "static":
        capabilities.append(STATIC_FILE_MANAGEMENT_CAPABILITY)
    return tuple(capabilities)


def _node_can_fit(
    heartbeat: NodeHeartbeat,
    requested: ResourceQuantity,
    routes: list[PlacementRecord],
) -> bool:
    return _node_can_fit_available(
        heartbeat,
        requested,
        _node_available_resources(heartbeat, routes),
    )


def _node_can_fit_available(
    heartbeat: NodeHeartbeat,
    requested: ResourceQuantity,
    available: ResourceQuantity,
    *,
    check_cpu: bool = True,
) -> bool:
    return node_accepts_dynamic_request(
        heartbeat, requested, available, check_cpu=check_cpu,
    )


def _node_placement_state(
    heartbeat: NodeHeartbeat,
    node_routes: list[PlacementRecord],
) -> NodePlacementState:
    # Many sandboxes use the same image. Normalize each reference and inspect
    # the heartbeat cache once per distinct image, not once per sandbox/pass.
    image_identities: dict[str, str] = {}
    inflight_candidates: set[str] = set()
    projected_images = set(heartbeat.cached_images)
    assigned_cpu: list[float] = []
    assigned_memory = 0
    active_creates = 0
    for route in node_routes:
        state = route.state.lower()
        if state not in {"deleted", "failed"}:
            assigned_cpu.append(route.resources.vcpu)
            assigned_memory += route.resources.memory_mb
        if state in {"creating", "planned", "quota_ready", "rootfs_ready", "unknown"}:
            active_creates += 1
        if state not in {"creating", "unknown", "running"}:
            continue
        image = (
            str(route.spec.get("image") or "")
            if isinstance(route, SandboxRoute) else route.image
        ).strip()
        if image not in image_identities:
            image_identities[image] = canonical_image_digest_ref(image) or image
        identity = image_identities[image]
        if identity:
            projected_images.add(identity)
            if state in {"creating", "unknown"}:
                inflight_candidates.add(identity)
    cached_images = set(heartbeat.cached_images)
    inflight_images = frozenset(
        identity for identity in inflight_candidates
        if not heartbeat.cached_images_known
        or not _requested_image_cache_keys(identity).intersection(cached_images)
    )
    # Assigned shapes estimate future load, including parked programs; live
    # capacity reservations still use the canonical inventory accounting below.
    total = heartbeat.total_resources
    return NodePlacementState(
        assigned_shape_pressure=max(
            sum(assigned_cpu) / max(1, total.vcpu),
            assigned_memory / max(1, total.memory_mb),
        ),
        assigned_vcpu=sum(assigned_cpu),
        assigned_memory_mb=assigned_memory,
        available_resources=_node_available_resources(heartbeat, node_routes),
        inflight_image_identities=inflight_images,
        projected_image_identities=frozenset(projected_images),
        active_creates=max(heartbeat.active_sandbox_creates, active_creates),
    )


def _cold_image_placement_cost_for_state(
    state: NodePlacementState,
    target_manifest: RegistryManifestLayers | None,
    layer_cache: RegistryLayerMetadataCache | None,
    *,
    spread_cold_image: bool,
) -> tuple[int, int]:
    if not spread_cold_image:
        return (0, 0)
    pressure = max(len(state.inflight_image_identities), state.active_creates)
    if target_manifest is None or layer_cache is None:
        return (1, pressure)
    available_layers: set[str] = set()
    for image_ref in state.projected_image_identities:
        manifest = layer_cache.get(image_ref)
        if manifest is not None:
            available_layers.update(layer.digest for layer in manifest.layers)
    missing_bytes = sum(
        layer.size
        for layer in target_manifest.layers
        if layer.digest not in available_layers
    )
    return (
        0,
        missing_bytes + pressure * COLD_PULL_PRESSURE_PENALTY_BYTES,
    )


@contextmanager
def _gateway_placement_lock(route_path: Path, *, blocking: bool = True):
    """Serialize route accounting and intent persistence across gateways."""

    lock_path = route_path.with_name(route_path.name + ".placement.lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+", encoding="utf-8") as lock_file:
        operation = fcntl.LOCK_EX if blocking else fcntl.LOCK_EX | fcntl.LOCK_NB
        try:
            fcntl.flock(lock_file.fileno(), operation)
        except BlockingIOError as exc:
            raise GatewaySchedulingBusyError(
                "sandbox placement is reserved by another gateway process"
            ) from exc
        try:
            yield
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def _resource_slack(
    free: ResourceQuantity, requested: ResourceQuantity
) -> tuple[float, int, int]:
    return (
        max(0.0, free.vcpu - requested.vcpu),
        max(0, free.memory_mb - requested.memory_mb),
        max(0, free.disk_mb - requested.disk_mb),
    )


def _has_resource_values(resources: ResourceQuantity) -> bool:
    return resources.vcpu > 0 or resources.memory_mb > 0 or resources.disk_mb > 0
