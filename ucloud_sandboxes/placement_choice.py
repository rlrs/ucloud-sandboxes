"""Power-of-k create placement over a fleet view (plan C4.3; C3.2 ``pack``).

Each API process samples k eligible workers uniformly from its fleet view,
scores them by live pressure, in-flight creates and image residency, and tries
them best first; the node gate admits or rejects. Nothing reads another
process's state or takes a lock shared with it: sampling decorrelates
processes that rank one stale view, and node rejection backstops overbooking.
Randomness and the clock are injected; there is no I/O. Per create::

    for node in chooser.choose(view, request):
        overlay.reserve(node, incarnation, request)
        if <node accepts>: return node
        overlay.release(node.job_id, incarnation)  # only on a definite reject
    # All rejected: resample with their job ids in request.excluded_job_ids.

An unknown outcome (a timeout) keeps its reservation: the worker may hold the
create, and its heartbeat or the TTL settles the charge.
"""
from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime
import random
from threading import Lock
import time
from typing import Any, Callable, Iterable, Mapping, NamedTuple

from .capabilities import has_capability
from .deployment import agent_version_is_schedulable
from .models import NodeHeartbeat, ResourceQuantity, is_soft_drained
from .placement_accounting import (
    PlacementRecord, PlacementReservation, PlacementRouteIndex,
    _node_available_resources, _node_has_storage_device_capacity, _placement_route_index,
    _route_initial_claim_mb,
)
from .resource_admission import node_accepts_dynamic_request, node_pressure_score

DEFAULT_K = 3
# Load is pressure (0..1) plus in-flight creates per target concurrency. A
# resident image is worth RESIDENCY_WEIGHT load: it attracts creates until its
# node is that much busier than a cold peer. An image this process is already
# sending to a node counts as PENDING_RESIDENCY resident there.
RESIDENCY_WEIGHT = 0.25
PENDING_RESIDENCY = 0.5
# A leak bound, not a freshness signal: confirmation or release normally ends a
# reservation. It must exceed a create POST plus one heartbeat and one refresh.
DEFAULT_OVERLAY_TTL_SECONDS = 60.0


class SandboxIncarnation(NamedTuple):
    """The inventory identity a worker reports once it registers a create."""

    sandbox_id: str
    generation: int
    spec_hash: str
    operation_id: str


@dataclass(frozen=True)
class FleetView:
    """Fresh admitting sandbox workers, plus durable reservations (incoming
    migrations) that their heartbeats do not show yet."""

    nodes: tuple[NodeHeartbeat, ...]
    routes: PlacementRouteIndex = field(default_factory=lambda: _placement_route_index([]))

    @classmethod
    def ready(
        cls, heartbeats: Iterable[NodeHeartbeat], *, now: datetime, ttl_seconds: int,
        routes: Iterable[PlacementRecord] = (), agent_version: str | None = None,
    ) -> FleetView:
        return cls(tuple(
            heartbeat for heartbeat in heartbeats
            if heartbeat.node_url and not heartbeat.draining and heartbeat.admission_open
            and has_capability(heartbeat.capabilities, "sandbox")
            and heartbeat.is_fresh(now, ttl_seconds)
            and agent_version_is_schedulable(heartbeat.agent_version, expected=agent_version)
        ), _placement_route_index(list(routes)))


@dataclass(frozen=True)
class PlacementRequest:
    resources: ResourceQuantity
    capabilities: tuple[str, ...] = ()
    image: str = ""
    # The sandbox spec: it decides what a dynamic-claim worker charges.
    spec: dict[str, Any] = field(default_factory=dict)
    # node_id -> fraction (0..1) of the image's startup bytes resident (C2.8).
    residency: Mapping[str, float] = field(default_factory=dict)
    excluded_job_ids: frozenset[str] = frozenset()


@dataclass(frozen=True)
class GroupSlice:
    heartbeat: NodeHeartbeat
    count: int


def node_fits(
    heartbeat: NodeHeartbeat, requested: ResourceQuantity, records: list[PlacementRecord],
) -> bool:
    """The reservation-time fit: each record charges hard disk and a storage
    device until the heartbeat shows it. CPU is only a ranking signal."""

    return _node_has_storage_device_capacity(heartbeat, records) and node_accepts_dynamic_request(
        heartbeat, requested, _node_available_resources(heartbeat, records), check_cpu=False,
    )


class _Claim(NamedTuple):
    resources: ResourceQuantity
    spec: dict[str, Any]


def initial_charge(heartbeat: NodeHeartbeat, request: PlacementRequest) -> ResourceQuantity:
    """What the worker charges a create until it reports it: the initial
    claim that placement_accounting charges an unobserved route."""

    claim = _Claim(request.resources, request.spec)
    return replace(request.resources, disk_mb=_route_initial_claim_mb(claim, heartbeat))


def fit_count(
    heartbeat: NodeHeartbeat, request: PlacementRequest,
    records: list[PlacementRecord], limit: int,
) -> int:
    """How many more of these creates fit, at most ``limit``."""

    charged, charge = list(records), initial_charge(heartbeat, request)
    for count in range(limit):
        if not node_fits(heartbeat, request.resources, charged):
            return count
        charged.append(PlacementReservation(
            f"fit:{count}", heartbeat.node_id, heartbeat.job_id,
            heartbeat.node_url or "", charge, "",
        ))
    return limit


class _Reservation(NamedTuple):
    record: PlacementReservation
    node_epoch: str
    expires_at: float


class InflightOverlay:
    """This process's creates that its fleet view does not show yet.

    A reservation charges its worker like a durable PlacementReservation until
    a heartbeat reports the exact sandbox incarnation, a heartbeat retires the
    worker incarnation it targeted (the create cannot land there; an older view
    never undercharges a newer boot), the caller releases it, or the TTL bounds
    a lost release. It only steers this process; the node gate stays the authority.
    The lock guards this process's map and is never held across I/O. Each
    heartbeat object settles a worker's map once, so a reservation made after
    that waits for the next heartbeat, which can only overcharge.
    """

    def __init__(
        self, *, ttl_seconds: float = DEFAULT_OVERLAY_TTL_SECONDS,
        clock: Callable[[], float] = time.monotonic,
    ):
        if not ttl_seconds > 0:
            raise ValueError("overlay TTL must be positive")  # else no charge survives
        self._ttl, self._clock, self._lock = ttl_seconds, clock, Lock()
        self._by_job: dict[str, dict[SandboxIncarnation, _Reservation]] = {}
        self._settled: dict[str, NodeHeartbeat] = {}
        self._next_sweep = clock() + ttl_seconds

    def reservation_count(self) -> int:
        with self._lock:
            return sum(map(len, self._by_job.values()))

    def reserve(
        self, heartbeat: NodeHeartbeat, incarnation: SandboxIncarnation,
        request: PlacementRequest,
    ) -> None:
        record = PlacementReservation(
            "inflight:" + ":".join(map(str, incarnation)), heartbeat.node_id,
            heartbeat.job_id, heartbeat.node_url or "",
            initial_charge(heartbeat, request), request.image,
        )
        with self._lock:
            now = self._clock()
            if now >= self._next_sweep:
                # records() never sees a worker that left the view; expire its
                # reservations here, at most once per TTL.
                self._next_sweep = now + self._ttl
                for job_id, entries in list(self._by_job.items()):
                    for key in [key for key, entry in entries.items() if entry.expires_at <= now]:
                        del entries[key]
                    if not entries:
                        self._forget(job_id)
            self._by_job.setdefault(heartbeat.job_id, {})[incarnation] = _Reservation(
                record, heartbeat.node_epoch, now + self._ttl,
            )

    def release(self, job_id: str, incarnation: SandboxIncarnation) -> None:
        with self._lock:
            entries = self._by_job.get(job_id, {})
            entries.pop(incarnation, None)
            if not entries:
                self._forget(job_id)

    def records(self, heartbeat: NodeHeartbeat) -> tuple[PlacementReservation, ...]:
        """Charges still owed on this worker, after dropping what it settles."""

        job_id = heartbeat.job_id
        with self._lock:
            entries = self._by_job.get(job_id)
            if not entries:
                return ()
            now, observed = self._clock(), None
            if self._settled.get(job_id) is not heartbeat:
                self._settled[job_id] = heartbeat
                observed = {
                    (item.sandbox_id, item.generation, item.spec_hash, item.operation_id)
                    for item in heartbeat.inventory
                }
            for incarnation, entry in list(entries.items()):
                if entry.expires_at <= now or observed is not None and (
                    incarnation in observed or entry.node_epoch in heartbeat.retired_node_epochs
                ):
                    del entries[incarnation]
            if not entries:
                self._forget(job_id)
            return tuple(entry.record for entry in entries.values())

    def _forget(self, job_id: str) -> None:
        self._by_job.pop(job_id, None)
        self._settled.pop(job_id, None)


class PowerOfKChooser:
    """Order k uniformly sampled eligible workers by placement score.

    ``api_processes`` counts the processes placing onto this fleet. Sampling
    hands each about 1/N of a worker's creates, so N times this process's own
    unconfirmed creates estimates the fleet's creates since that heartbeat.
    Without it, a worker whose heartbeat predates a burst looks idle to every
    process at once. Fit stays exact: only the score extrapolates.
    """

    def __init__(
        self, overlay: InflightOverlay, *, rng: random.Random,
        target_creates_per_node: int, api_processes: int, k: int = DEFAULT_K,
    ):
        if min(k, target_creates_per_node, api_processes) < 1:
            raise ValueError("k, the per-node create target and api_processes must be positive")
        self.overlay, self._rng, self._k = overlay, rng, k
        self._target, self._processes = target_creates_per_node, api_processes

    def choose(
        self, fleet: FleetView, request: PlacementRequest, *, include_job_ids: frozenset[str] = frozenset(),
    ) -> tuple[NodeHeartbeat, ...]:
        """Up to k eligible workers, best first. Soft-drained workers are
        emptying: they fill the list only when fewer than k others are.
        Eligible ``include_job_ids`` (workers already holding the image) join
        the k sampled ones."""

        nodes = [node for node in fleet.nodes if node.job_id not in include_job_ids]
        preferred: list[tuple[float, NodeHeartbeat]] = []
        drained: list[tuple[float, NodeHeartbeat]] = []
        for node in fleet.nodes:
            score = self._score(fleet, node, request) if node.job_id in include_job_ids else None
            if score is not None:
                (drained if is_soft_drained(node) else preferred).append((score, node))
        limit = self._k + len(preferred)
        # The eligible members of a lazily drawn uniform permutation's prefix
        # are a uniform sample of every eligible worker.
        for index in range(len(nodes)):
            if len(preferred) == limit:
                break
            swap = self._rng.randrange(index, len(nodes))
            nodes[index], nodes[swap] = nodes[swap], nodes[index]
            score = self._score(fleet, nodes[index], request)
            if score is not None:
                (drained if is_soft_drained(nodes[index]) else preferred).append(
                    (score, nodes[index])
                )
        # Stable sorts: ties keep their random sample order.
        ranked = sorted(preferred, key=lambda item: item[0])
        ranked += sorted(drained, key=lambda item: item[0])[: self._k - len(ranked)]
        return tuple(heartbeat for _score, heartbeat in ranked)

    def pack(
        self, fleet: FleetView, request: PlacementRequest, count: int, *, per_node_budget: int,
        include_job_ids: frozenset[str] = frozenset(),
    ) -> tuple[GroupSlice, ...]:
        """C3.2 ``pack``: fill the best sampled worker with up to
        ``per_node_budget`` of the group, then overflow to the next. A total
        below ``count`` means the view fits no more; the caller queues the
        rest. The caller charges each placed sandbox in the overlay before
        dispatch, and re-packs a rejected remainder without that worker."""

        if per_node_budget < 1:
            raise ValueError("per-node group budget must be positive")
        slices: list[GroupSlice] = []
        excluded = set(request.excluded_job_ids)
        while count > 0:
            ranked = self.choose(
                fleet, replace(request, excluded_job_ids=frozenset(excluded)),
                include_job_ids=include_job_ids,
            )
            if not ranked:
                break
            excluded.add(ranked[0].job_id)
            placed = fit_count(
                ranked[0], request,
                [*fleet.routes.routes_for(ranked[0]), *self.overlay.records(ranked[0])],
                min(per_node_budget, count),
            )
            if placed:
                slices.append(GroupSlice(ranked[0], placed))
                count -= placed
        return tuple(slices)

    def plan_group(
        self, fleet: FleetView, request: PlacementRequest, count: int, *, per_node_budget: int,
        policy: str = "pack", include_job_ids: frozenset[str] = frozenset(),
    ) -> tuple[GroupSlice, ...]:
        """C3.2: ``pack`` fills few workers so each attaches the image once;
        ``spread`` caps each worker at an even share of the view."""

        if policy == "spread":
            per_node_budget = min(per_node_budget, -(-count // max(1, len(fleet.nodes))))
        elif policy != "pack":
            raise ValueError("group placement policy must be pack or spread")
        return self.pack(
            fleet, request, count, per_node_budget=per_node_budget, include_job_ids=include_job_ids,
        )

    def _score(
        self, fleet: FleetView, heartbeat: NodeHeartbeat, request: PlacementRequest,
    ) -> float | None:
        if heartbeat.job_id in request.excluded_job_ids or not all(
            has_capability(heartbeat.capabilities, capability)
            for capability in request.capabilities
        ):
            return None
        own = self.overlay.records(heartbeat)
        durable = fleet.routes.routes_for(heartbeat)
        if not node_fits(heartbeat, request.resources, [*durable, *own]):
            return None
        residency = request.residency.get(heartbeat.node_id, 0.0)
        if request.image and any(record.image == request.image for record in own):
            residency = max(residency, PENDING_RESIDENCY)
        creates = heartbeat.active_sandbox_creates + self._processes * len(own)
        return (
            node_pressure_score(heartbeat) + creates / self._target
            - RESIDENCY_WEIGHT * min(1.0, max(0.0, residency))
        )
