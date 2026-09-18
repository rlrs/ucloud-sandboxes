"""Conservative, monotonic placement of portable parks at their next wake."""

from __future__ import annotations

from datetime import datetime
import math

from .models import NodeHeartbeat, ResourceQuantity, ScalePolicy


def consolidation_rank(node: NodeHeartbeat) -> tuple[int, str, str]:
    # UCloud job IDs are increasing decimal strings. This remains a stable
    # total order for other providers and does not depend on changing load.
    return len(node.job_id), node.job_id, node.node_id


def can_consolidate_wake(
    source: NodeHeartbeat,
    destination: NodeHeartbeat,
    requested: ResourceQuantity,
    policy: ScalePolicy,
    *,
    now: datetime,
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
    used_memory = max(dst.memory_used_mb, dst.memory_total_mb - dst.memory_available_mb)
    if (
        used_memory + requested.memory_mb
    ) / dst.memory_total_mb > policy.target_memory_utilization:
        return False
    if (
        dst.memory_psi_full_avg10 is None
        or not math.isfinite(dst.memory_psi_full_avg10)
        or dst.memory_psi_full_avg10 >= policy.max_memory_psi_full_avg10
    ):
        return False
    if (
        dst.storage_max_concurrent_operations <= 0
        or dst.storage_active_operations / dst.storage_max_concurrent_operations
        >= policy.target_storage_queue_utilization
    ):
        return False
    return True
