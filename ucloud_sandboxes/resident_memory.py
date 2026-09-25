"""Cached, incarnation-fenced cgroup memory observations for wait decisions.

Sampling runs in maintenance, never on request admission. A configured memory
limit is not a resident footprint, and summing shared process RSS is not valid.
Unknown/stale data stays unknown rather than crediting a fictional reclaim.
"""

from dataclasses import dataclass
from pathlib import Path
from threading import Lock
import time

from .hibernation import linux_process_start_time_ticks


@dataclass(frozen=True)
class ResidentMemorySample:
    current_bytes: int
    anonymous_bytes: int
    file_bytes: int
    dirty_bytes: int
    writeback_bytes: int
    refault_file_pages: int
    cgroup_path: str
    cgroup_device: int
    cgroup_inode: int
    sentry_pid: int
    sentry_start_time_ticks: int
    sampled_at: float
    shared_memory_bytes: int = 0
    # memory.peak: this runtime's high-water mark (0 when the kernel lacks it).
    peak_bytes: int = 0

    @property
    def clean_file_bytes(self) -> int:
        return max(
            0,
            min(
                self.current_bytes,
                self.file_bytes
                - self.shared_memory_bytes
                - self.dirty_bytes
                - self.writeback_bytes,
            ),
        )


class ResidentMemorySampler:
    def __init__(
        self,
        *,
        cgroup_root: Path = Path("/sys/fs/cgroup"),
        proc_root: Path = Path("/proc"),
        max_age_seconds: float = 2.5,
    ):
        self.cgroup_root = cgroup_root
        self.proc_root = proc_root
        self.max_age_seconds = max_age_seconds
        self._samples: dict[tuple[str, int], ResidentMemorySample] = {}
        self._guard = Lock()

    def get(self, key: tuple[str, int]) -> ResidentMemorySample | None:
        with self._guard:
            sample = self._samples.get(key)
        if (
            sample is None
            or time.monotonic() - sample.sampled_at > self.max_age_seconds
        ):
            return None
        return sample

    def historical(self, key: tuple[str, int]) -> ResidentMemorySample | None:
        """Last charge for this incarnation, even after its runtime parks.

        Consumers must apply their own age fence; this is never live admission
        evidence. Normal retain/forget ownership cleanup also bounds this cache.
        """
        with self._guard:
            return self._samples.get(key)

    def forget(self, key) -> None:
        with self._guard:
            self._samples.pop(key, None)

    def retain(self, keys) -> None:
        keys = set(keys)
        with self._guard:
            self._samples = {
                key: sample for key, sample in self._samples.items() if key in keys
            }

    def sample(
        self,
        key,
        *,
        pid: int,
        start_time_ticks: int,
        container_id: str,
        expected_path: str | None = None,
    ) -> ResidentMemorySample | None:
        try:
            if (
                linux_process_start_time_ticks(pid, proc_root=self.proc_root)
                != start_time_ticks
            ):
                raise ValueError("sentry identity changed")
            memberships = (
                (self.proc_root / str(pid) / "cgroup").read_text().splitlines()
            )
            unified = [line[3:] for line in memberships if line.startswith("0::")]
            if len(unified) != 1:
                raise ValueError("runtime has no unique unified memory cgroup")
            raw = unified[0]
            relative = Path(raw.removeprefix("/"))
            if (
                not raw.startswith("/")
                or not relative.parts
                or any(part in {".", ".."} for part in raw.split("/")[1:])
            ):
                raise ValueError("runtime cgroup path is invalid")
            if expected_path is not None:
                if raw != expected_path:
                    raise ValueError("runtime cgroup differs from OCI ownership")
            elif relative.name != container_id:
                # Legacy OCI configs let runsc select its default path. Require
                # its exact immutable container ID, never a shared parent cgroup.
                raise ValueError("runtime cgroup is not incarnation-specific")
            path = self.cgroup_root / relative
            if path.resolve() != path or not path.is_dir():
                raise ValueError("runtime cgroup escaped its trusted root")
            identity = path.stat()
            current = int((path / "memory.current").read_text().strip())
            try:
                peak = int((path / "memory.peak").read_text().strip())
            except FileNotFoundError:
                peak = 0
            counters = {}
            for line in (path / "memory.stat").read_text().splitlines():
                name, value = line.split()
                counters[name] = int(value)
            required = (
                "anon",
                "file",
                "file_dirty",
                "file_writeback",
                "workingset_refault_file",
            )
            if (
                current < 0
                or counters["shmem"] < 0
                or any(counters[name] < 0 for name in required)
            ):
                raise ValueError("memory accounting is negative")
            if (
                linux_process_start_time_ticks(pid, proc_root=self.proc_root)
                != start_time_ticks
            ):
                raise ValueError("sentry identity changed during sampling")
            after = path.stat()
            if (identity.st_dev, identity.st_ino) != (after.st_dev, after.st_ino):
                raise ValueError("runtime cgroup was replaced during sampling")
            result = ResidentMemorySample(
                current,
                *(counters[name] for name in required),
                raw,
                identity.st_dev,
                identity.st_ino,
                pid,
                start_time_ticks,
                time.monotonic(),
                counters["shmem"],
                max(peak, current) if peak else 0,
            )
        except (OSError, ValueError, KeyError):
            with self._guard:
                self._samples.pop(key, None)
            return None
        with self._guard:
            self._samples[key] = result
        return result


@dataclass(frozen=True)
class ResidentReclaimResult:
    requested_bytes: int
    reclaimed_bytes: int
    elapsed_seconds: float
    refault_file_pages: int
    reason: str


class ResidentMemoryReclaimer:
    """Best-effort cache reclaim against one already fenced live incarnation.

    The caller's lifecycle check is deliberately short: a blocking kernel
    reclaim write never owns the lifecycle lock. A wake can cancel subsequent
    windows; a window already in the kernel may finish after that wake. Nothing
    here changes runtime authority or promises that requested bytes were freed.
    """

    def __init__(self, sampler: ResidentMemorySampler):
        self.sampler = sampler

    def reclaim(
        self,
        key,
        sample: ResidentMemorySample,
        *,
        target_bytes: int,
        is_current,
        window_bytes: int = 16 * 1024 * 1024,
    ) -> ResidentReclaimResult:
        import errno
        import os

        if target_bytes <= 0 or window_bytes <= 0:
            raise ValueError("reclaim byte budgets must be positive")
        started = time.monotonic()
        requested = 0
        current = sample
        reason = "target_reached"
        target = min(target_bytes, sample.clean_file_bytes)
        if target == 0:
            return ResidentReclaimResult(0, 0, 0.0, 0, "no_reclaimable_cache")
        path = self.sampler.cgroup_root / sample.cgroup_path.removeprefix("/")
        directory_fd = reclaim_fd = None
        try:
            directory_fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            identity = os.fstat(directory_fd)
            if (identity.st_dev, identity.st_ino) != (
                sample.cgroup_device,
                sample.cgroup_inode,
            ):
                return ResidentReclaimResult(0, 0, 0.0, 0, "cgroup_changed")
            reclaim_fd = os.open(
                "memory.reclaim", os.O_WRONLY | os.O_NOFOLLOW, dir_fd=directory_fd
            )
            while requested < target:
                if not is_current():
                    reason = "superseded"
                    break
                observed = self.sampler.sample(
                    key,
                    pid=sample.sentry_pid,
                    start_time_ticks=sample.sentry_start_time_ticks,
                    container_id=path.name,
                    expected_path=sample.cgroup_path,
                )
                if observed is None or (
                    observed.cgroup_device,
                    observed.cgroup_inode,
                ) != (sample.cgroup_device, sample.cgroup_inode):
                    reason = "cgroup_changed"
                    break
                current = observed
                if sample.current_bytes - current.current_bytes >= target:
                    break
                amount = min(window_bytes, target - requested, current.clean_file_bytes)
                if amount <= 0:
                    reason = "no_reclaimable_cache"
                    break
                # No anonymous swap-out: tmpfs application memory is deliberately
                # excluded from the estimate. Unsupported knobs fail closed.
                if not is_current():
                    reason = "superseded"
                    break
                payload = f"{amount} swappiness=0".encode("ascii")
                requested += amount
                try:
                    if os.write(reclaim_fd, payload) != len(payload):
                        reason = "short_write"
                        break
                except OSError as exc:
                    if exc.errno == errno.EAGAIN:
                        reason = "partial_reclaim"
                    else:
                        reason = (
                            "unsupported"
                            if exc.errno
                            in {errno.EINVAL, errno.ENOENT, errno.EOPNOTSUPP}
                            else "kernel_error"
                        )
                        break
            observed = self.sampler.sample(
                key,
                pid=sample.sentry_pid,
                start_time_ticks=sample.sentry_start_time_ticks,
                container_id=path.name,
                expected_path=sample.cgroup_path,
            )
            if observed is not None and (
                observed.cgroup_device,
                observed.cgroup_inode,
            ) == (sample.cgroup_device, sample.cgroup_inode):
                current = observed
        except OSError as exc:
            reason = (
                "unsupported"
                if exc.errno in {errno.ENOENT, errno.EOPNOTSUPP, errno.EACCES}
                else "kernel_error"
            )
        finally:
            if reclaim_fd is not None:
                os.close(reclaim_fd)
            if directory_fd is not None:
                os.close(directory_fd)
        return ResidentReclaimResult(
            requested,
            max(0, sample.current_bytes - current.current_bytes),
            time.monotonic() - started,
            max(0, current.refault_file_pages - sample.refault_file_pages),
            reason,
        )
