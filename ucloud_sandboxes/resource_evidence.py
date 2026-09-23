"""Low-frequency Linux resource evidence; never a sandbox ownership authority.

Disk counters describe guest-visible leaf whole devices, not the provider's
physical disks. They are deliberately not aggregated with partitions, ublk,
loop or device-mapper devices. Cgroup evidence describes this agent's cgroup
only; it must not be labelled as the fleet's or each sandbox's resource usage.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, fields
from datetime import datetime, timezone
import math
import os
from functools import lru_cache
from pathlib import Path
import threading
import time


CPU_OBSERVATION_FRESHNESS_SECONDS = 2.0


@dataclass(frozen=True)
class MemoryBackingCapacity:
    """Configured RAM backing; absent counters mean unknown, never disabled."""

    total_bytes: int | None = None
    available_bytes: int | None = None
    identity: str | None = None

    @classmethod
    def from_dict(cls, raw):
        if not isinstance(raw, dict) or set(raw) != {"total_bytes", "available_bytes", "identity"}:
            return None
        if all(value is None for value in raw.values()):
            return cls()
        total, available, identity = raw["total_bytes"], raw["available_bytes"], raw["identity"]
        if (type(total) is not int or type(available) is not int
                or not 0 <= available <= total <= 2**63 - 1 or total == 0
                or not isinstance(identity, str) or not 0 < len(identity) <= 256):
            return None
        return cls(total, available, identity)


@lru_cache(maxsize=64)
def _verified_memory_mount(proc_root, root, mount_id, device, inode):
    # Mount identity comes from the opened directory's fdinfo, not a parent
    # filesystem guess. Parse the potentially large mount table only once per
    # identity; ordinary samples read bounded fdinfo and fstatvfs counters.
    for line in (proc_root / "self/mountinfo").read_text().splitlines():
        parts = line.split()
        if not parts or parts[0] != mount_id:
            continue
        separator = parts.index("-")
        mount_path = parts[4].replace("\\040", " ").replace("\\011", "\t").replace("\\134", "\\")
        options = set(parts[5].split(",")) | set(parts[separator + 3].split(","))
        return (mount_path == str(root) and parts[separator + 1] == "tmpfs"
                and "noswap" in options and parts[2] == device)
    return False


def sample_memory_backing(root: Path | None, *, proc_root: Path = Path("/proc")) -> MemoryBackingCapacity | None:
    """Sample the configured active-memory filesystem without walking files.

    None means RAM backing is disabled. Configured but missing, replaced or
    unverified mounts produce explicit unknown evidence. The caller's existing
    short pressure/singleflight cache supplies the sample freshness bound.
    """
    if root is None:
        return None
    root = Path(root)
    if not root.is_absolute():
        return MemoryBackingCapacity()
    descriptor = None
    try:
        descriptor = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
        info = os.fstat(descriptor)
        device = f"{os.major(info.st_dev)}:{os.minor(info.st_dev)}"
        mount_id = next(line.split()[1] for line in
                        (proc_root / f"self/fdinfo/{descriptor}").read_text().splitlines()
                        if line.startswith("mnt_id:"))
        if not _verified_memory_mount(proc_root, root, mount_id, device, info.st_ino):
            return MemoryBackingCapacity()
        space = os.fstatvfs(descriptor)
        # Detect replacement while sampling rather than reporting another
        # filesystem's capacity as usable active-memory space.
        current = root.stat()
        if (info.st_dev, info.st_ino) != (current.st_dev, current.st_ino):
            return MemoryBackingCapacity()
        return MemoryBackingCapacity.from_dict({
            "total_bytes": space.f_blocks * space.f_frsize,
            "available_bytes": space.f_bavail * space.f_frsize,
            "identity": f"{mount_id}:{device}:{info.st_ino}",
        }) or MemoryBackingCapacity()
    except (OSError, ValueError, IndexError, StopIteration):
        return MemoryBackingCapacity()
    finally:
        if descriptor is not None:
            os.close(descriptor)


@dataclass(frozen=True)
class DeviceIO:
    identity: str
    name: str
    read_bytes_per_second: float | None = None
    write_bytes_per_second: float | None = None
    read_iops: float | None = None
    write_iops: float | None = None
    read_await_ms: float | None = None
    write_await_ms: float | None = None
    average_queue_depth: float | None = None
    busy_percent: float | None = None
    read_bytes: int | None = None
    write_bytes: int | None = None


@dataclass(frozen=True)
class ResourceEvidence:
    collected_at: str
    interval_seconds: float | None = None
    memory_dirty_bytes: int | None = None
    memory_writeback_bytes: int | None = None
    memory_mapped_bytes: int | None = None
    memory_shared_bytes: int | None = None
    memory_cached_bytes: int | None = None
    host_cpu_usage_usec: int | None = None
    host_cpu_steal_usec: int | None = None
    host_major_faults: int | None = None
    host_refault_anon: int | None = None
    host_refault_file: int | None = None
    cgroup_path: str | None = None
    cgroup_cpu_usage_usec: int | None = None
    cgroup_cpu_throttled_usec: int | None = None
    cgroup_cpu_throttled_periods: int | None = None
    cgroup_memory_current_bytes: int | None = None
    cgroup_memory_anon_bytes: int | None = None
    cgroup_memory_file_bytes: int | None = None
    cgroup_memory_dirty_bytes: int | None = None
    cgroup_memory_writeback_bytes: int | None = None
    cgroup_major_faults: int | None = None
    cgroup_refault_anon: int | None = None
    cgroup_refault_file: int | None = None
    devices: tuple[DeviceIO, ...] | None = None

    def to_dict(self) -> dict:
        value = asdict(self)
        value["devices"] = (
            [asdict(device) for device in self.devices]
            if self.devices is not None
            else None
        )
        return value

    @classmethod
    def from_dict(cls, raw: object) -> ResourceEvidence | None:
        names = {f.name for f in fields(cls)}
        if not isinstance(raw, dict) or set(raw) not in (
            names, names - {"host_cpu_usage_usec", "host_cpu_steal_usec"}
        ):
            return None
        value = dict(raw)
        try:
            timestamp = datetime.fromisoformat(value["collected_at"])
            if timestamp.tzinfo is None:
                return None
        except (TypeError, ValueError):
            return None
        if value["cgroup_path"] is not None and not isinstance(
            value["cgroup_path"], str
        ):
            return None
        for key, number in value.items():
            if key in {"collected_at", "cgroup_path", "devices"}:
                continue
            if not _valid_number(number, integer=key != "interval_seconds"):
                return None
        devices = value["devices"]
        if devices is not None:
            if not isinstance(devices, (list, tuple)) or len(devices) > 256:
                return None
            decoded = []
            for device in devices:
                names = {f.name for f in fields(DeviceIO)}
                if not isinstance(device, dict) or set(device) not in (
                    names, names - {"read_bytes", "write_bytes"}
                ):
                    return None
                if any(
                    not isinstance(device[k], str) or not device[k]
                    for k in ("identity", "name")
                ):
                    return None
                if any(
                    not _valid_number(v, integer=k in {"read_bytes", "write_bytes"})
                    for k, v in device.items()
                    if k not in {"identity", "name"}
                ):
                    return None
                decoded.append(DeviceIO(**device))
            value["devices"] = tuple(decoded)
        return cls(**value)


def _valid_number(value: object, *, integer: bool = False) -> bool:
    if value is None:
        return True
    if (
        isinstance(value, bool)
        or not isinstance(value, int if integer else (int, float))
        or value < 0
    ):
        return False
    try:
        return math.isfinite(value)
    except OverflowError:
        # Untrusted JSON integers can exceed conversion to a finite float.
        # Reject malformed telemetry rather than failing heartbeat decoding.
        return False


def read_counter_file(path: Path) -> dict[str, int]:
    """Canonical nonnegative key/value counter parser for vmstat and cgroup v2."""
    result = {}
    try:
        lines = path.read_text().splitlines()
    except OSError:
        return result
    for line in lines:
        parts = line.split()
        if len(parts) == 2:
            try:
                value = int(parts[1])
            except ValueError:
                continue
            if value >= 0:
                result[parts[0]] = value
    return result


def _read_text(path: Path) -> str | None:
    try:
        return path.read_text().strip()
    except OSError:
        return None


def _read_integer(path: Path) -> int | None:
    value = _read_text(path)
    return int(value) if value is not None and value.isdecimal() else None


def _cgroup_path(proc: Path, cgroup_root: Path) -> tuple[str | None, Path | None]:
    content = _read_text(proc / "self/cgroup")
    for line in (content or "").splitlines():
        if line.startswith("0::/"):
            relative = line[3:].lstrip("/")
            path = (cgroup_root / relative).resolve()
            if path.is_relative_to(cgroup_root.resolve()):
                return "/" + relative, path
    return None, None


def read_leaf_disks(
    proc: Path, sys: Path
) -> dict[str, tuple[str, tuple[int, ...]]] | None:
    content = _read_text(proc / "diskstats")
    boot = _read_text(proc / "sys/kernel/random/boot_id")
    if content is None or boot is None or not (sys / "dev/block").is_dir():
        return None
    result = {}
    for line in content.splitlines():
        parts = line.split()
        if len(parts) < 14:
            continue
        device = sys / "dev/block" / f"{parts[0]}:{parts[1]}"
        try:
            resolved = device.resolve(strict=True)
            if (resolved / "partition").exists() or "virtual" in resolved.parts:
                continue
            if any((resolved / "slaves").iterdir()):
                continue
            counters = tuple(int(value) for value in parts[3:14])
            if any(value < 0 for value in counters):
                continue
            # diskseq protects against name/minor reuse even when counters grow.
            diskseq = _read_integer(resolved / "diskseq")
            if diskseq is None:
                continue  # Unknown identity cannot safely produce interval rates.
        except (OSError, ValueError):
            continue
        identity = f"{boot}:{parts[0]}:{parts[1]}:{diskseq}:{resolved}"
        result[identity] = (parts[2], counters)
        if len(result) >= 256:
            break
    return result


def disk_rates(
    identity: str,
    name: str,
    current: tuple[int, ...],
    previous: tuple[int, ...] | None,
    seconds: float | None,
) -> DeviceIO:
    totals = {"read_bytes": current[2] * 512, "write_bytes": current[6] * 512}
    if previous is None or seconds is None or seconds <= 0:
        return DeviceIO(identity, name, **totals)
    # Field 8 is an instantaneous in-flight gauge; all others are counters.
    delta = [value - old for value, old in zip(current, previous)]
    if any(value < 0 for index, value in enumerate(delta) if index != 8):
        return DeviceIO(identity, name, **totals)
    return DeviceIO(
        identity,
        name,
        read_bytes_per_second=delta[2] * 512 / seconds,
        write_bytes_per_second=delta[6] * 512 / seconds,
        read_iops=delta[0] / seconds,
        write_iops=delta[4] / seconds,
        read_await_ms=delta[3] / delta[0] if delta[0] else None,
        write_await_ms=delta[7] / delta[4] if delta[4] else None,
        average_queue_depth=delta[10] / (seconds * 1000),
        busy_percent=min(100.0, delta[9] / (seconds * 10)),
        **totals,
    )


class ResourceEvidenceSampler:
    """One sequential background collector; request readers only copy a cache.

    Initial/unavailable/stale evidence is None. Rates require two comparable
    samples; disappeared/replaced/reset devices cannot inherit prior counters.
    """

    def __init__(
        self, proc_root=Path("/proc"), sys_root=Path("/sys"), *, clock=time.monotonic
    ):
        self.proc = Path(proc_root)
        self.sys = Path(sys_root)
        self.clock = clock
        self._lock = threading.Lock()
        self._started = False
        self._value = None
        self._at = None
        self._disks = {}
        self._cpu_sample = None
        self._cpu_percent = None

    def _start_locked(self) -> None:
        if not self._started:
            self._started = True
            threading.Thread(
                target=self._run, name="resource-evidence", daemon=True
            ).start()

    def cached(self) -> ResourceEvidence | None:
        with self._lock:
            self._start_locked()
            if self._at is None or self.clock() - self._at > 5:
                return None
            return self._value

    def cached_cpu_percent(self) -> float | None:
        """A recent measured interval, never an assumed idle or blocked sample.

        This uses the same collector as device/cgroup evidence. CPU expires
        sooner because admission uses it, while memory is sampled on demand.
        """
        with self._lock:
            self._start_locked()
            if self._at is None or not 0 <= self.clock() - self._at <= CPU_OBSERVATION_FRESHNESS_SECONDS:
                return None
            return self._cpu_percent

    def _run(self):
        while True:
            try:
                self.collect()
            except (OSError, ValueError):
                # Keep the last observation with its real timestamp; stale
                # evidence expires rather than becoming a healthy zero.
                pass
            time.sleep(1)

    def collect(self) -> ResourceEvidence:

        now = self.clock()
        interval = now - self._at if self._at is not None else None
        memory = read_proc_meminfo(self.proc / "meminfo")
        vm = read_counter_file(self.proc / "vmstat")
        host_cpu = read_proc_cpu_fields(self.proc / "stat")
        clock_ticks = os.sysconf("SC_CLK_TCK")
        scope, cgroup = _cgroup_path(self.proc, self.sys / "fs/cgroup")
        cpu = read_counter_file(cgroup / "cpu.stat") if cgroup else {}
        cm = read_counter_file(cgroup / "memory.stat") if cgroup else {}
        disks = read_leaf_disks(self.proc, self.sys)
        devices = (
            None
            if disks is None
            else tuple(
                disk_rates(
                    identity,
                    name,
                    counters,
                    self._disks.get(identity, (None, None))[1],
                    interval,
                )
                for identity, (name, counters) in disks.items()
            )
        )

        def memory_bytes(key):
            return memory[key] * 1024 if key in memory else None

        value = ResourceEvidence(
            collected_at=datetime.now(timezone.utc).isoformat(),
            interval_seconds=interval,
            memory_dirty_bytes=memory_bytes("Dirty"),
            memory_writeback_bytes=memory_bytes("Writeback"),
            memory_mapped_bytes=memory_bytes("Mapped"),
            memory_shared_bytes=memory_bytes("Shmem"),
            memory_cached_bytes=memory_bytes("Cached"),
            host_cpu_usage_usec=(sum(host_cpu[:3]) + sum(host_cpu[5:7])) * 1_000_000 // clock_ticks
            if host_cpu is not None else None,
            host_cpu_steal_usec=(host_cpu[7] if len(host_cpu) > 7 else 0) * 1_000_000 // clock_ticks
            if host_cpu is not None else None,
            host_major_faults=vm.get("pgmajfault"),
            host_refault_anon=vm.get("workingset_refault_anon"),
            host_refault_file=vm.get("workingset_refault_file"),
            cgroup_path=scope,
            cgroup_cpu_usage_usec=cpu.get("usage_usec"),
            cgroup_cpu_throttled_usec=cpu.get("throttled_usec"),
            cgroup_cpu_throttled_periods=cpu.get("nr_throttled"),
            cgroup_memory_current_bytes=_read_integer(cgroup / "memory.current")
            if cgroup
            else None,
            cgroup_memory_anon_bytes=cm.get("anon"),
            cgroup_memory_file_bytes=cm.get("file"),
            cgroup_memory_dirty_bytes=cm.get("file_dirty"),
            cgroup_memory_writeback_bytes=cm.get("file_writeback"),
            cgroup_major_faults=cm.get("pgmajfault"),
            cgroup_refault_anon=cm.get("workingset_refault_anon"),
            cgroup_refault_file=cm.get("workingset_refault_file"),
            devices=devices,
        )
        cpu_sample = _cpu_totals(host_cpu)
        with self._lock:
            # Do not smooth an outage into a fresh-looking CPU observation.
            # Missing reads discard the baseline for the next tick as well.
            self._cpu_percent = (
                cpu_percent_from_samples(self._cpu_sample, cpu_sample)
                if interval is not None and 0 < interval <= CPU_OBSERVATION_FRESHNESS_SECONDS
                else None
            )
            self._cpu_sample = cpu_sample
            self._at, self._value, self._disks = now, value, disks or {}
        return value


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
            parsed = int(parts[0])
        except ValueError:
            continue
        if parsed >= 0:
            result[key] = parsed
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
                parsed = float(value)
            except ValueError:
                pass
            else:
                if math.isfinite(parsed) and 0.0 <= parsed <= 100.0:
                    result[sample_type] = parsed
            break
    return result


@dataclass(frozen=True)
class MemoryPressure:
    sampled_monotonic: float
    memory: dict[str, int]  # /proc/meminfo values in KiB
    memory_psi: dict[str, float]  # ten-second stall percentages
    io_psi: dict[str, float]


def read_memory_pressure(proc: Path) -> MemoryPressure:
    return MemoryPressure(
        time.monotonic(),
        read_proc_meminfo(proc / "meminfo"),
        read_proc_pressure(proc / "pressure/memory"),
        read_proc_pressure(proc / "pressure/io"),
    )


def read_proc_cpu_fields(path: Path) -> tuple[int, ...] | None:
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
    if len(values) < 4 or any(value < 0 for value in values):
        return None
    return tuple(values)



def _cpu_totals(values: tuple[int, ...] | None) -> tuple[int, int] | None:
    if values is None:
        return None
    idle = values[3] + (values[4] if len(values) > 4 else 0)
    total = sum(values[:8])  # guest/guest_nice already appear in user/nice.
    return total, idle


def read_proc_stat_cpu(path: Path) -> tuple[int, int] | None:
    return _cpu_totals(read_proc_cpu_fields(path))


def cpu_percent_from_samples(
    first: tuple[int, int] | None,
    second: tuple[int, int] | None,
) -> float | None:
    if first is None or second is None:
        return None
    total_delta = second[0] - first[0]
    idle_delta = second[1] - first[1]
    if total_delta <= 0 or idle_delta < 0 or idle_delta > total_delta:
        return None
    return ((total_delta - idle_delta) / total_delta) * 100.0
