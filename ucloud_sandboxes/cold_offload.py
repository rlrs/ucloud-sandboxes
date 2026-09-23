"""Choose cold, already-published parks for the existing detach protocol.

This is advisory maintenance policy. The gateway still validates publication,
leases its registry reference, fences the route and commits worker eviction.
Neither a plan nor a successful HTTP dispatch releases a local disk claim.
"""

from dataclasses import dataclass
from datetime import datetime
import math
from typing import Iterable

from .capabilities import STORAGE_NATIVE_DETACH_CAPABILITY
from .models import SandboxNode, parse_iso_datetime, utc_now
from .routing import ProgramRequestState, SandboxRoute, is_portable_parked_route
from .storage_native_migration import StorageNativeMigration


@dataclass(frozen=True)
class ColdOffloadCandidate:
    route: SandboxRoute
    restore_bytes: int
    score: float
    reason: str

    def to_dict(self):
        return {
            "sandbox_id": self.route.sandbox_id,
            "generation": self.route.generation,
            "job_id": self.route.job_id,
            "disk_claim_mb": self.route.resources.disk_mb,
            "restore_bytes": self.restore_bytes,
            "score": self.score,
            "reason": self.reason,
        }


def plan_cold_offload(
    nodes: Iterable[SandboxNode],
    routes: Iterable[SandboxRoute],
    requests: Iterable[ProgramRequestState],
    *,
    pending_wake_sandbox_ids: Iterable[str] = (),
    excluded_job_ids: Iterable[str] = (),
    limit: int,
    pending_disk_mb: int = 0,
    now: datetime | None = None,
) -> tuple[ColdOffloadCandidate, ...]:
    """Recover hard disk headroom with no new checkpoint or upload traffic.

    Prefer older, larger claims with smaller remote restore cost. Active model
    waits and response deliveries stay local; a model wait is not cold merely
    because its sandbox has reached a durable park. Missing measurements do not
    authorize offload. Target 80% reservation only after reaching 90%.
    """
    now = now or utc_now()
    protected = set(pending_wake_sandbox_ids)
    protected.update(request.sandbox_id for request in requests if not request.is_terminal)
    excluded = set(excluded_job_ids)
    eligible_nodes = {
        node.job_id: node.heartbeat for node in nodes
        if node.is_schedulable and node.heartbeat is not None
        and node.job_id not in excluded
        and STORAGE_NATIVE_DETACH_CAPABILITY in node.heartbeat.capabilities
    }
    targets = {}
    admission_deficits = set()
    for job_id, heartbeat in eligible_nodes.items():
        metrics = heartbeat.runtime_metrics
        if metrics is None or metrics.storage_hard_capacity_mb <= 0:
            continue
        capacity, reserved = metrics.storage_hard_capacity_mb, metrics.storage_hard_reserved_mb
        headroom = max(0, capacity - reserved)
        # A large queued create can need space below the utilization trigger.
        # A request larger than the whole node cannot be fixed by eviction.
        deficit = max(0, pending_disk_mb - headroom) if pending_disk_mb <= capacity else 0
        pressure = max(0, reserved - .8 * capacity) if reserved >= .9 * capacity else 0
        if deficit or pressure:
            targets[job_id] = max(deficit, pressure)
            if deficit and not pressure:
                admission_deficits.add(job_id)
    ranked = []
    for route in routes:
        if (route.job_id not in eligible_nodes or route.sandbox_id in protected
                or route.delete_operation_id or route.worker_state == "detached"
                or not is_portable_parked_route(route) or route.resources.disk_mb <= 0):
            continue
        recovery = route.worker_state == "detaching"
        if not recovery and route.job_id not in targets:
            continue
        try:
            snapshot = StorageNativeMigration.from_dict(route.storage_snapshot)
        except (TypeError, ValueError):
            continue
        # These bytes are a cost estimate only; complete descriptor ownership
        # and immutable registry reachability are checked by /detach as usual.
        restore_bytes = sum(layer.size for layer in snapshot.publication.layers)
        if snapshot.memory_publication is not None:
            restore_bytes += sum(file.size for file in snapshot.memory_publication.files)
        updated = parse_iso_datetime(route.updated_at)
        age = max(0, (now - updated).total_seconds()) if updated else 0
        score = route.resources.disk_mb / max(1, restore_bytes / 1024**2)
        score *= 1 + math.log1p(age / 60)
        ranked.append(ColdOffloadCandidate(route, restore_bytes, score,
                       "resume_detach" if recovery else (
                           "disk_admission_deficit" if route.job_id in admission_deficits
                           else "disk_reservation_pressure")))
    ranked.sort(key=lambda item: (item.reason != "resume_detach", -item.score, item.route.sandbox_id))
    selected = []
    attempted_admission_jobs = set()
    for item in ranked:
        if len(selected) >= max(0, limit):
            break
        if item.reason != "resume_detach" and targets.get(item.route.job_id, 0) <= 0:
            continue
        if item.reason == "disk_admission_deficit":
            job_id = item.route.job_id
            if job_id in attempted_admission_jobs:
                continue
            attempted_admission_jobs.add(job_id)
            batch, recovered = [], 0
            for candidate in ranked:
                if candidate.route.job_id == job_id and candidate.reason == item.reason:
                    batch.append(candidate)
                    recovered += candidate.route.resources.disk_mb
                    if recovered >= targets[job_id]:
                        break
            # Do not churn cold data for a request that the available candidates
            # and this cycle's existing expense budget still cannot make fit.
            if recovered < targets[job_id] or len(batch) > limit - len(selected):
                continue
            selected.extend(batch)
            targets[job_id] -= recovered
        else:
            selected.append(item)
            targets[item.route.job_id] = targets.get(item.route.job_id, 0) - item.route.resources.disk_mb
    return tuple(selected)
