"""Conservative, monotonic placement of portable parks at their next wake."""

from __future__ import annotations

from datetime import datetime
import math
from typing import TYPE_CHECKING, Iterable

from .models import NodeHeartbeat, ResourceQuantity, ScalePolicy, parse_iso_datetime

if TYPE_CHECKING:
    from .routing import SandboxRoute


def consolidation_rank(node: NodeHeartbeat) -> tuple[int, str, str]:
    # UCloud job IDs are increasing decimal strings. This remains a stable
    # total order for other providers and does not depend on changing load.
    return len(node.job_id), node.job_id, node.node_id


def observed_memory_mb(
    route: SandboxRoute,
    heartbeats: Iterable[NodeHeartbeat],
    *,
    now: datetime,
    max_age_seconds: float,
) -> int | None:
    """One exact-owner/incarnation observation; it only tightens the memory check."""
    for heartbeat in heartbeats:
        if (
            heartbeat.job_id != route.job_id
            or heartbeat.node_id != route.node_id
            or (route.node_epoch and heartbeat.node_epoch != route.node_epoch)
            or not heartbeat.is_fresh(now, max_age_seconds)
        ):
            continue
        for entry in heartbeat.inventory:
            if (
                entry.sandbox_id != route.sandbox_id
                or entry.generation != route.generation
                or entry.spec_hash != route.spec_hash
                or entry.memory_observation is None
            ):
                continue
            sample = entry.memory_observation
            sampled_at = parse_iso_datetime(sample.sampled_at)
            if (
                sampled_at is not None
                and 0 <= (now - sampled_at).total_seconds() <= max_age_seconds
            ):
                return (sample.memory_bytes + 1024**2 - 1) // 1024**2
    return None


def can_consolidate_wake(
    source: NodeHeartbeat,
    destination: NodeHeartbeat,
    requested: ResourceQuantity,
    policy: ScalePolicy,
    *,
    now: datetime,
    observed_memory_mb: int | None = None,
) -> bool:
    if not policy.parked_wake_consolidation_enabled:
        return False
    if consolidation_rank(destination) >= consolidation_rank(source):
        return False
    if destination.active_sandboxes < max(1, source.active_sandboxes):
        return False
    for node in (source, destination):
        metrics = node.runtime_metrics
        if (
            not node.inventory_complete
            or not node.resources_known
            or node.draining
            or not node.admission_open
            or not node.is_fresh(now, policy.live_pressure_fresh_seconds)
            or metrics is None
            or not 0
            <= (now - metrics.collected_at).total_seconds()
            <= policy.live_pressure_fresh_seconds
            or metrics.cpu_percent is None
            or not math.isfinite(metrics.cpu_percent)
            or metrics.memory_total_mb <= 0
            or metrics.storage_error_volumes > 0
            or metrics.storage_waiting_operations > 0
            or node.active_sandbox_creates > 0
        ):
            return False
    src = source.runtime_metrics
    dst = destination.runtime_metrics
    assert src is not None and dst is not None
    # Optional packing must not concentrate work onto a worker with worse
    # measured I/O/reclaim stalls merely because it is older and more occupied.
    # Compare like-for-like samples; older agents may not report I/O PSI yet.
    for name in ("io_psi_some_avg10", "io_psi_full_avg10", "memory_psi_some_avg10"):
        source_stall, destination_stall = getattr(src, name), getattr(dst, name)
        if (
            source_stall is not None and destination_stall is not None
            and destination_stall > source_stall
        ):
            return False
    # Evacuate only lightly loaded sources. Charge the full waking shape
    # against live destination headroom, rather than relying on overcommit.
    if src.cpu_percent / 100 > policy.target_cpu_utilization / 2:
        return False
    if destination.total_resources.vcpu <= 0:
        return False
    if (
        dst.cpu_percent / 100 + requested.vcpu / destination.total_resources.vcpu
        > policy.target_cpu_utilization
    ):
        return False
    # File-backed application RAM is reclaimable to Linux, but moving another
    # sandbox onto it still adds fault/writeback work. Use the same observed
    # working-set evidence as autoscaling, without reserving parked limits.
    used_memory = max(
        dst.memory_used_mb,
        dst.memory_total_mb - dst.memory_available_mb,
        dst.memory_working_set_mb,
    )
    if (
        used_memory + max(requested.memory_mb, observed_memory_mb or 0)
    ) / dst.memory_total_mb > policy.target_memory_utilization:
        return False
    if (
        dst.memory_psi_full_avg10 is None
        or not math.isfinite(dst.memory_psi_full_avg10)
        or dst.memory_psi_full_avg10 >= policy.max_memory_psi_full_avg10
    ):
        return False
    if dst.io_psi_full_avg10 is not None and (
        not math.isfinite(dst.io_psi_full_avg10)
        or dst.io_psi_full_avg10 >= policy.max_io_psi_full_avg10
    ):
        return False
    if (
        dst.storage_max_concurrent_operations <= 0
        or dst.storage_active_operations / dst.storage_max_concurrent_operations
        >= policy.target_storage_queue_utilization
    ):
        return False
    return True
