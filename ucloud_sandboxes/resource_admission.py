from __future__ import annotations

from .capabilities import (
    DISK_QUOTA_CAPABILITY,
    STORAGE_NATIVE_CAPABILITY,
    has_capability,
)
from .models import NodeHeartbeat, NodeRuntimeMetrics, ResourceQuantity


def dynamic_request_fits(
    requested: ResourceQuantity,
    available: ResourceQuantity,
    total: ResourceQuantity,
) -> bool:
    """Apply the direct-runtime resource accounting contract.

    CPU and memory are reusable, pressure-governed resources. A request must fit
    the physical node shape, but concurrent resident routes do not reserve them.
    Writable disk remains a hard reservation and is consumed from ``available``.
    """

    if not requested.is_valid or not (
        requested.vcpu > 0 or requested.memory_mb > 0 or requested.disk_mb > 0
    ):
        return False
    return bool(
        requested.vcpu <= total.vcpu
        and requested.memory_mb <= total.memory_mb
        and requested.disk_mb <= available.disk_mb
    )


def reserve_dynamic_resources(
    available: ResourceQuantity,
    requested: ResourceQuantity,
) -> ResourceQuantity:
    """Reserve hard disk while retaining reusable CPU and memory headroom."""

    return ResourceQuantity(
        vcpu=available.vcpu,
        memory_mb=available.memory_mb,
        disk_mb=max(0, available.disk_mb - requested.disk_mb),
    )


def reusable_dynamic_resources(
    available: ResourceQuantity,
    total: ResourceQuantity,
) -> ResourceQuantity:
    """Expose physical CPU/RAM shape plus the remaining hard disk."""

    return ResourceQuantity(
        vcpu=total.vcpu,
        memory_mb=total.memory_mb,
        disk_mb=available.disk_mb,
    )


def node_accepts_dynamic_request(
    heartbeat: NodeHeartbeat,
    requested: ResourceQuantity,
    available: ResourceQuantity,
    *,
    total: ResourceQuantity | None = None,
) -> bool:
    """Return one canonical live admission answer for create and wake paths."""

    if not dynamic_request_fits(
        requested,
        available,
        heartbeat.total_resources if total is None else total,
    ):
        return False
    if requested.disk_mb > 0 and not has_capability(
        heartbeat.capabilities,
        DISK_QUOTA_CAPABILITY,
    ):
        return False
    return bool(
        dynamic_pressure_error(heartbeat.runtime_metrics, requested) is None
        and node_storage_pressure_allows(heartbeat, requested)
    )


def dynamic_pressure_error(
    metrics: NodeRuntimeMetrics | None,
    requested: ResourceQuantity,
) -> str | None:
    """Return the shared create, wake, and exec pressure rejection reason."""

    if metrics is None:
        return "direct node has no fresh runtime metrics for dynamic admission"
    if metrics.cpu_percent is not None and metrics.cpu_percent >= 90.0:
        return "direct node CPU pressure blocks active admission"
    if (
        metrics.cpu_count > 0
        and metrics.load_average_1m is not None
        and metrics.load_average_1m >= metrics.cpu_count * 1.25
    ):
        return "direct node CPU load blocks active admission"
    if (
        metrics.memory_psi_full_avg10 is not None
        and metrics.memory_psi_full_avg10 >= 10.0
    ):
        return "direct node memory pressure blocks active admission"
    minimum_headroom_mb = max(2048, requested.memory_mb)
    if metrics.swap_total_mb > 0:
        available_memory_mb = metrics.memory_available_mb + metrics.swap_free_mb
        memory_is_known = True
    else:
        available_memory_mb = metrics.memory_available_mb
        memory_is_known = metrics.memory_total_mb > 0
    if memory_is_known and available_memory_mb < minimum_headroom_mb:
        return "direct node has insufficient live memory headroom"
    return None


def node_storage_pressure_allows(
    heartbeat: NodeHeartbeat,
    requested: ResourceQuantity,
) -> bool:
    if not has_capability(heartbeat.capabilities, STORAGE_NATIVE_CAPABILITY):
        return True
    metrics = heartbeat.runtime_metrics
    if metrics is None or metrics.storage_hard_capacity_mb <= 0:
        return False
    if metrics.storage_error_volumes > 0:
        return False
    if (
        metrics.storage_ublk_max_devices > 0
        and metrics.storage_ublk_active_devices >= metrics.storage_ublk_max_devices
    ):
        return False
    maximum = metrics.storage_max_concurrent_operations
    if maximum > 0 and metrics.storage_waiting_operations >= maximum:
        return False
    hard_available = max(
        0,
        metrics.storage_hard_capacity_mb - metrics.storage_hard_reserved_mb,
    )
    return requested.disk_mb <= hard_available
