from __future__ import annotations

import os
from pathlib import Path
import time

from .models import NodeRuntimeMetrics, utc_now


DEFAULT_CPU_SAMPLE_SECONDS = 0.05


def sample_node_runtime_metrics(
    *,
    proc_root: Path | str = "/proc",
    sample_seconds: float = DEFAULT_CPU_SAMPLE_SECONDS,
) -> NodeRuntimeMetrics:
    proc_path = Path(proc_root)
    cpu_count = os.cpu_count() or 0
    first_cpu = read_proc_stat_cpu(proc_path / "stat")
    if first_cpu is not None and sample_seconds > 0:
        time.sleep(sample_seconds)
    second_cpu = read_proc_stat_cpu(proc_path / "stat")
    cpu_percent = cpu_percent_from_samples(first_cpu, second_cpu)
    memory = read_proc_meminfo(proc_path / "meminfo")
    load = os.getloadavg() if hasattr(os, "getloadavg") else (None, None, None)
    cpu_vcpu = (
        (cpu_percent / 100.0) * cpu_count
        if cpu_percent is not None and cpu_count > 0
        else None
    )
    memory_total_mb = memory.get("MemTotal", 0) // 1024
    memory_available_mb = memory.get("MemAvailable", 0) // 1024
    memory_used_mb = max(0, memory_total_mb - memory_available_mb)
    swap_total_mb = memory.get("SwapTotal", 0) // 1024
    swap_free_mb = memory.get("SwapFree", 0) // 1024
    swap_used_mb = max(0, swap_total_mb - swap_free_mb)
    memory_pressure = read_proc_pressure(proc_path / "pressure" / "memory")
    memory_percent = (
        (memory_used_mb / memory_total_mb) * 100.0
        if memory_total_mb > 0
        else None
    )
    return NodeRuntimeMetrics(
        collected_at=utc_now(),
        cpu_percent=cpu_percent,
        cpu_vcpu=cpu_vcpu,
        cpu_count=cpu_count,
        memory_total_mb=memory_total_mb,
        memory_used_mb=memory_used_mb,
        memory_available_mb=memory_available_mb,
        memory_percent=memory_percent,
        swap_total_mb=swap_total_mb,
        swap_used_mb=swap_used_mb,
        swap_free_mb=swap_free_mb,
        memory_psi_some_avg10=memory_pressure.get("some"),
        memory_psi_full_avg10=memory_pressure.get("full"),
        load_average_1m=load[0],
        load_average_5m=load[1],
        load_average_15m=load[2],
    )


def read_proc_stat_cpu(path: Path) -> tuple[int, int] | None:
    try:
        first_line = path.read_text(encoding="utf-8").splitlines()[0]
    except (OSError, IndexError):
        return None
    fields = first_line.split()
    if not fields or fields[0] != "cpu":
        return None
    try:
        values = [int(value) for value in fields[1:]]
    except ValueError:
        return None
    if len(values) < 4:
        return None
    idle = values[3] + (values[4] if len(values) > 4 else 0)
    total = sum(values)
    return total, idle


def read_proc_meminfo(path: Path) -> dict[str, int]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return {}
    result: dict[str, int] = {}
    for line in lines:
        key, separator, value = line.partition(":")
        if not separator:
            continue
        parts = value.strip().split()
        if not parts:
            continue
        try:
            result[key] = int(parts[0])
        except ValueError:
            continue
    if "MemAvailable" not in result and "MemFree" in result:
        result["MemAvailable"] = result["MemFree"]
    return result


def read_proc_pressure(path: Path) -> dict[str, float]:
    """Read the 10-second Linux PSI averages for a pressure resource."""

    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return {}
    result: dict[str, float] = {}
    for line in lines:
        fields = line.split()
        if not fields:
            continue
        sample_type = fields[0]
        for field in fields[1:]:
            key, separator, value = field.partition("=")
            if key != "avg10" or not separator:
                continue
            try:
                result[sample_type] = max(0.0, float(value))
            except ValueError:
                pass
            break
    return result


def cpu_percent_from_samples(
    first: tuple[int, int] | None,
    second: tuple[int, int] | None,
) -> float | None:
    if first is None or second is None:
        return None
    total_delta = second[0] - first[0]
    idle_delta = second[1] - first[1]
    if total_delta <= 0:
        return None
    busy_delta = max(0, total_delta - idle_delta)
    return (busy_delta / total_delta) * 100.0
