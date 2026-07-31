from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Iterable

from .models import (
    NodeHeartbeat,
    ProgramScaleSignals,
    ResourceQuantity,
    SandboxPlacementRequest,
    ScalePolicy,
    parse_iso_datetime,
    utc_now,
)
from .routing import ProgramRequestState, SandboxRoute


_STATE_PRIORITY = {
    "model_wait": 0,
    "ready_to_wake": 1,
}


@dataclass(frozen=True)
class WakeNodeCandidate:
    node_id: str
    job_id: str
    available: ResourceQuantity
    total: ResourceQuantity
    pressure: float = 0.0


def node_pressure_score(heartbeat: NodeHeartbeat) -> float:
    """Reduce live node pressure to a bounded destination-ranking score."""

    metrics = heartbeat.runtime_metrics
    if metrics is None:
        return 0.0
    values = [
        max(0.0, min(1.0, float(metrics.cpu_percent or 0.0) / 100.0)),
        max(0.0, min(1.0, float(metrics.memory_percent or 0.0) / 100.0)),
        max(
            0.0,
            min(1.0, float(metrics.memory_psi_full_avg10 or 0.0) / 100.0),
        ),
    ]
    if metrics.storage_max_concurrent_operations > 0:
        values.append(
            max(
                0.0,
                min(
                    1.0,
                    (
                        metrics.storage_active_operations
                        + metrics.storage_waiting_operations
                    )
                    / metrics.storage_max_concurrent_operations,
                ),
            )
        )
    return max(values)


def plan_shadow_wake_queue(
    requests: list[ProgramRequestState],
    routes: list[SandboxRoute],
    candidates: list[WakeNodeCandidate],
    *,
    now: datetime | None = None,
) -> dict[str, object]:
    """Plan an aging-first global wake queue without reserving product state."""

    now = now or utc_now()
    route_by_sandbox = {route.sandbox_id: route for route in routes}
    ready_by_sandbox: dict[str, ProgramRequestState] = {}
    for request in requests:
        if request.state != "ready_to_wake":
            continue
        existing = ready_by_sandbox.get(request.sandbox_id)
        if existing is None or _request_ready_epoch(request) < _request_ready_epoch(
            existing
        ):
            ready_by_sandbox[request.sandbox_id] = request
    queue = sorted(
        ready_by_sandbox.values(),
        key=lambda request: (_request_ready_epoch(request), request.request_id),
    )
    available = {candidate.node_id: candidate.available for candidate in candidates}
    placements: list[dict[str, object]] = []
    unplaced: list[dict[str, object]] = []
    for position, request in enumerate(queue, start=1):
        route = route_by_sandbox.get(request.sandbox_id)
        if (
            route is None
            or route.generation != request.sandbox_generation
            or (route.state or "").lower() not in {"parked", "waking"}
        ):
            unplaced.append(
                {
                    "position": position,
                    "request_id": request.request_id,
                    "sandbox_id": request.sandbox_id,
                    "reason": "route_not_portable",
                }
            )
            continue
        ranked: list[tuple[tuple[object, ...], WakeNodeCandidate, ResourceQuantity]] = []
        for candidate in candidates:
            current_available = available[candidate.node_id]
            local = (
                candidate.node_id == route.node_id
                and candidate.job_id == route.job_id
            )
            required = ResourceQuantity(
                vcpu=route.resources.vcpu,
                memory_mb=route.resources.memory_mb,
                disk_mb=0 if local else route.resources.disk_mb,
            )
            if not required.fits_within(current_available):
                continue
            remaining = ResourceQuantity(
                vcpu=current_available.vcpu - required.vcpu,
                memory_mb=current_available.memory_mb - required.memory_mb,
                disk_mb=current_available.disk_mb - required.disk_mb,
            )
            score = (
                0 if local else 1,
                max(0.0, candidate.pressure),
                _residual_fraction(remaining, candidate.total),
                candidate.node_id,
            )
            ranked.append((score, candidate, remaining))
        if not ranked:
            unplaced.append(
                {
                    "position": position,
                    "request_id": request.request_id,
                    "sandbox_id": request.sandbox_id,
                    "reason": "no_hard_fit",
                }
            )
            continue
        _score, selected, remaining = min(ranked, key=lambda item: item[0])
        available[selected.node_id] = remaining
        placements.append(
            {
                "position": position,
                "request_id": request.request_id,
                "rollout_id": request.rollout_id,
                "sandbox_id": request.sandbox_id,
                "sandbox_generation": request.sandbox_generation,
                "node_id": selected.node_id,
                "job_id": selected.job_id,
                "local": (
                    selected.node_id == route.node_id
                    and selected.job_id == route.job_id
                ),
                "ready_age_seconds": max(
                    0,
                    int(now.timestamp() - _request_ready_epoch(request)),
                ),
            }
        )
    return {
        "mode": "shadow",
        "queued": len(queue),
        "placed": len(placements),
        "unplaced_count": len(unplaced),
        "placements": placements,
        "unplaced": unplaced,
    }


def build_program_scale_signals(
    requests: list[ProgramRequestState],
    routes: list[SandboxRoute],
    policy: ScalePolicy,
    *,
    pending_wake_sandbox_ids: set[str] | None = None,
    now: datetime | None = None,
) -> ProgramScaleSignals:
    """Reduce request phases without counting concurrent calls twice.

    Only parked sandboxes contribute future active-resource demand. Existing
    wake pending records remain authoritative and are excluded here to avoid
    counting the same hard demand twice.
    """

    now = now or utc_now()
    pending_wake_sandbox_ids = pending_wake_sandbox_ids or set()
    route_by_sandbox = {route.sandbox_id: route for route in routes}
    counts = {
        "model_wait": 0,
        "ready_to_wake": 0,
        "waking": 0,
        "acting": 0,
    }
    selected_by_sandbox: dict[str, ProgramRequestState] = {}
    for request in requests:
        if request.state not in counts:
            continue
        counts[request.state] += 1
        if request.state not in _STATE_PRIORITY:
            # Waking/acting work already owns active capacity. It remains
            # observable, but cannot suppress a newer future-demand phase for
            # the same sandbox.
            continue
        selected = selected_by_sandbox.get(request.sandbox_id)
        if selected is None or _STATE_PRIORITY[request.state] > _STATE_PRIORITY[
            selected.state
        ]:
            selected_by_sandbox[request.sandbox_id] = request

    model_wait: list[ProgramRequestState] = []
    ready: list[ProgramRequestState] = []
    for sandbox_id, request in selected_by_sandbox.items():
        route = route_by_sandbox.get(sandbox_id)
        if (
            route is None
            or route.generation != request.sandbox_generation
            or (route.state or "").lower() not in {"parked", "waking"}
        ):
            continue
        if request.state == "model_wait":
            model_wait.append(request)
        elif (
            request.state == "ready_to_wake"
            and sandbox_id not in pending_wake_sandbox_ids
        ):
            ready.append(request)

    model_wait_resources = _active_resources(model_wait)
    ready_resources = _active_resources(ready)
    weighted = model_wait_resources.scaled(
        cpu=max(0.0, policy.model_wait_capacity_weight),
        memory=max(0.0, policy.model_wait_capacity_weight),
        disk=0.0,
    )
    cap = policy.default_node_resources.scaled(
        cpu=max(0, policy.model_wait_max_headroom_nodes),
        memory=max(0, policy.model_wait_max_headroom_nodes),
        disk=0.0,
    )
    weighted = ResourceQuantity(
        vcpu=min(weighted.vcpu, cap.vcpu),
        memory_mb=min(weighted.memory_mb, cap.memory_mb),
        disk_mb=0,
    )
    shadow = ready_resources + weighted
    effective = shadow if policy.program_aware_autoscaling_enabled else ResourceQuantity()
    return ProgramScaleSignals(
        model_wait_requests=counts["model_wait"],
        ready_to_wake_requests=counts["ready_to_wake"],
        waking_requests=counts["waking"],
        acting_requests=counts["acting"],
        model_wait_sandboxes=len(model_wait),
        ready_to_wake_sandboxes=len(ready),
        model_wait_resources=model_wait_resources,
        ready_to_wake_resources=ready_resources,
        weighted_model_wait_resources=weighted,
        effective_resources=effective,
        ready_placement_requests=tuple(
            SandboxPlacementRequest(
                resources=request.resources,
                owned_job_id=route_by_sandbox[request.sandbox_id].job_id,
                owned_disk_mb=request.resources.disk_mb,
            )
            for request in ready
        ),
        oldest_model_wait_seconds=_oldest_seconds(
            (request.accepted_at for request in model_wait),
            now,
        ),
        oldest_ready_to_wake_seconds=_oldest_seconds(
            (request.response_ready_at for request in ready),
            now,
        ),
        action_enabled=policy.program_aware_autoscaling_enabled,
    )


def _active_resources(requests: list[ProgramRequestState]) -> ResourceQuantity:
    total = ResourceQuantity()
    for request in requests:
        total = total + ResourceQuantity(
            vcpu=request.resources.vcpu,
            memory_mb=request.resources.memory_mb,
            disk_mb=0,
        )
    return total


def _request_ready_epoch(request: ProgramRequestState) -> float:
    parsed = parse_iso_datetime(request.response_ready_at or request.updated_at)
    return parsed.timestamp() if parsed is not None else 0.0


def _residual_fraction(
    remaining: ResourceQuantity,
    total: ResourceQuantity,
) -> float:
    fractions = (
        remaining.vcpu / total.vcpu if total.vcpu > 0 else 0.0,
        remaining.memory_mb / total.memory_mb if total.memory_mb > 0 else 0.0,
        remaining.disk_mb / total.disk_mb if total.disk_mb > 0 else 0.0,
    )
    return sum(fractions) / len(fractions)


def _oldest_seconds(timestamps: Iterable[str], now: datetime) -> int:
    oldest = 0
    for timestamp in timestamps:
        parsed = parse_iso_datetime(timestamp)
        if parsed is not None:
            oldest = max(oldest, int((now - parsed).total_seconds()))
    return max(0, oldest)
