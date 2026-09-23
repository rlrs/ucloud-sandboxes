from __future__ import annotations

import os
from pathlib import Path
import time
from typing import Callable

from .models import NodeRuntimeMetrics, utc_now
from .resource_evidence import (
    ResourceEvidenceSampler,
    read_memory_pressure,
    sample_memory_backing,
    cpu_percent_from_samples as cpu_percent_from_samples,
    read_proc_stat_cpu as read_proc_stat_cpu,
    read_proc_meminfo as read_proc_meminfo,
    read_proc_pressure as read_proc_pressure,
)
from .singleflight_cache import GenerationFencedSingleFlightCache


_RESOURCE_EVIDENCE = ResourceEvidenceSampler()

DEFAULT_RUNTIME_METRICS_FRESHNESS_SECONDS = 0.2


class SingleFlightRuntimeMetricsSampler:
    """Coalesce adjacent host samples without serving materially stale pressure.

    The first caller after the freshness window invokes ``provider``. Concurrent
    callers wait for that same sample instead of repeating its /proc reads.
    ``None`` is cached just like a metrics value so an unavailable collector
    remains fail-closed for the short freshness window. Exceptions are not
    cached: waiters are woken and one of them, or the next caller, retries.
    """

    def __init__(
        self,
        provider: Callable[[], NodeRuntimeMetrics | None],
        *,
        freshness_seconds: float = DEFAULT_RUNTIME_METRICS_FRESHNESS_SECONDS,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._provider = provider
        self._cache = GenerationFencedSingleFlightCache[NodeRuntimeMetrics | None](
            ttl_seconds=freshness_seconds,
            clock=clock,
        )

    def __call__(self) -> NodeRuntimeMetrics | None:
        return self._cache.get_or_load(self._provider)


def sample_node_runtime_metrics(
    *,
    proc_root: Path | str = "/proc",
    memory_backing_root: Path | None = None,
) -> NodeRuntimeMetrics:
    proc_path = Path(proc_root)
    cpu_count = os.cpu_count() or 0
    # CPU needs an interval, but foreground admission must not wait for one.
    # Missing or stale observations remain unknown. Physical memory and PSI
    # are still read now, independently of the background CPU cadence.
    cpu_percent = (
        _RESOURCE_EVIDENCE.cached_cpu_percent()
        if proc_path == Path("/proc") else None
    )
    pressure = read_memory_pressure(proc_path)
    memory = pressure.memory
    load = os.getloadavg() if hasattr(os, "getloadavg") else (None, None, None)
    cpu_vcpu = (
        (cpu_percent / 100.0) * cpu_count
        if cpu_percent is not None and cpu_count > 0
        else None
    )
    memory_total_mb = memory.get("MemTotal", 0) // 1024
    memory_available_mb = memory.get("MemAvailable", 0) // 1024
    memory_used_mb = max(0, memory_total_mb - memory_available_mb)
    # MemAvailable treats file-backed guest RAM as reclaimable cache. It can
    # report an almost empty worker while most RAM backs running sandboxes.
    # This estimate guides placement/scaling only; keep raw host admission
    # evidence unchanged. Shmem is already charged in used memory.
    mapped_file_mb = max(0, memory.get("Mapped", 0) - memory.get("Shmem", 0)) // 1024
    memory_working_set_mb = min(memory_total_mb, memory_used_mb + mapped_file_mb)
    swap_total_mb = memory.get("SwapTotal", 0) // 1024
    swap_free_mb = memory.get("SwapFree", 0) // 1024
    swap_used_mb = max(0, swap_total_mb - swap_free_mb)
    memory_pressure = pressure.memory_psi
    io_pressure = pressure.io_psi
    memory_percent = (
        (memory_used_mb / memory_total_mb) * 100.0 if memory_total_mb > 0 else None
    )
    return NodeRuntimeMetrics(
        collected_at=utc_now(),
        memory_backing=sample_memory_backing(memory_backing_root, proc_root=proc_path),
        resource_evidence=_RESOURCE_EVIDENCE.cached()
        if proc_path == Path("/proc")
        else None,
        cpu_percent=cpu_percent,
        cpu_vcpu=cpu_vcpu,
        cpu_count=cpu_count,
        memory_total_mb=memory_total_mb,
        memory_used_mb=memory_used_mb,
        memory_available_mb=memory_available_mb,
        memory_percent=memory_percent,
        memory_working_set_mb=memory_working_set_mb,
        swap_total_mb=swap_total_mb,
        swap_used_mb=swap_used_mb,
        swap_free_mb=swap_free_mb,
        memory_psi_some_avg10=memory_pressure.get("some"),
        memory_psi_full_avg10=memory_pressure.get("full"),
        io_psi_some_avg10=io_pressure.get("some"),
        io_psi_full_avg10=io_pressure.get("full"),
        load_average_1m=load[0],
        load_average_5m=load[1],
        load_average_15m=load[2],
    )
