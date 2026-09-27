"""Filesystem registry disk pressure: measurement, admission, maintenance state.

Docker Distribution returns HTTP 500 for every push once its filesystem is
full, which fails builds and snapshot publications mid-upload. The gateway,
autoscaler, and registry-pressure unit share one statvfs view of the registry
volume so admission can stop new writes before the disk fills and maintenance
can free space before admission has to (docs/managed-registry.md).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import tempfile
import threading
import time
from typing import Any, Callable

from .config import DeploymentConfig


_LOG = logging.getLogger(__name__)
REGISTRY_DISK_PRESSURE_ERROR_CODE = "registry_disk_pressure"
REGISTRY_DISK_RETRY_AFTER_SECONDS = 60
# statvfs is cheap, but the gateway checks it on every build admission.
REGISTRY_DISK_CACHE_SECONDS = 2.0

StatVfs = Callable[[str], os.statvfs_result]


@dataclass(frozen=True)
class RegistryDiskUsage:
    path: str
    total_bytes: int
    used_bytes: int
    available_bytes: int
    cleanup_percent: float
    refuse_percent: float
    target_percent: float = 0.0

    @property
    def capacity_bytes(self) -> int:
        return self.used_bytes + self.available_bytes

    @property
    def target_used_bytes(self) -> int:
        """Used bytes at which LRU eviction stops."""

        return int(self.capacity_bytes * self.target_percent / 100.0)

    @property
    def used_percent(self) -> float:
        # df(1) semantics: reserved root blocks count as neither used nor free.
        capacity = self.capacity_bytes
        return 100.0 * self.used_bytes / capacity if capacity > 0 else 0.0

    @property
    def cleanup_needed(self) -> bool:
        return self.used_percent >= self.cleanup_percent

    @property
    def refusing_writes(self) -> bool:
        return self.used_percent >= self.refuse_percent

    @property
    def state(self) -> str:
        if self.refusing_writes:
            return "refusing"
        if self.cleanup_needed:
            return "cleanup"
        return "ok"

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "total_bytes": self.total_bytes,
            "used_bytes": self.used_bytes,
            "available_bytes": self.available_bytes,
            "used_percent": round(self.used_percent, 2),
            "cleanup_percent": self.cleanup_percent,
            "target_percent": self.target_percent,
            "refuse_percent": self.refuse_percent,
            "state": self.state,
        }


def registry_disk_paths(config: DeploymentConfig) -> tuple[str, ...]:
    """Candidate paths on the registry volume; empty for non-filesystem stores."""

    if config.registry_store.kind != "filesystem":
        return ()
    # The data directory is root-only (0750); the unprivileged gateway can
    # still statvfs it, but fall back to the mount point if lookup fails.
    return tuple(
        dict.fromkeys(
            item
            for item in (config.registry_store.data_root, config.registry_mount_point)
            if item
        )
    )


def measure_registry_disk(
    paths: tuple[str, ...],
    *,
    cleanup_percent: float,
    refuse_percent: float,
    target_percent: float = 0.0,
    statvfs: StatVfs = os.statvfs,
) -> RegistryDiskUsage | None:
    for path in paths:
        try:
            result = statvfs(path)
        except OSError:
            continue
        fragment = int(result.f_frsize or result.f_bsize)
        total = int(result.f_blocks) * fragment
        free = int(result.f_bfree) * fragment
        available = int(result.f_bavail) * fragment
        return RegistryDiskUsage(
            path=path,
            total_bytes=total,
            used_bytes=max(0, total - free),
            available_bytes=available,
            cleanup_percent=cleanup_percent,
            refuse_percent=refuse_percent,
            target_percent=target_percent,
        )
    return None


def registry_disk_usage(
    config: DeploymentConfig,
    *,
    statvfs: StatVfs = os.statvfs,
) -> RegistryDiskUsage | None:
    return measure_registry_disk(
        registry_disk_paths(config),
        cleanup_percent=config.registry_disk_cleanup_percent,
        refuse_percent=config.registry_disk_refuse_percent,
        target_percent=config.registry_disk_target_percent,
        statvfs=statvfs,
    )


class RegistryDiskMonitor:
    """Cached registry-volume usage for request admission."""

    def __init__(
        self,
        paths: tuple[str, ...],
        *,
        cleanup_percent: float,
        refuse_percent: float,
        target_percent: float = 0.0,
        maintenance_state_file: Path | None = None,
        cache_seconds: float = REGISTRY_DISK_CACHE_SECONDS,
        statvfs: StatVfs = os.statvfs,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.paths = paths
        self.cleanup_percent = cleanup_percent
        self.refuse_percent = refuse_percent
        self.target_percent = target_percent
        self.maintenance_state_file = maintenance_state_file
        self.cache_seconds = cache_seconds
        self._statvfs = statvfs
        self._clock = clock
        self._guard = threading.Lock()
        self._cached: RegistryDiskUsage | None = None
        self._cached_at: float | None = None
        self._last_warning_at: float | None = None
        self._state: dict[str, Any] = {}
        self._state_cached_at: float | None = None

    @classmethod
    def from_config(cls, config: DeploymentConfig) -> "RegistryDiskMonitor | None":
        paths = registry_disk_paths(config)
        if not paths:
            return None
        return cls(
            paths,
            cleanup_percent=config.registry_disk_cleanup_percent,
            refuse_percent=config.registry_disk_refuse_percent,
            target_percent=config.registry_disk_target_percent,
            maintenance_state_file=config.registry_maintenance_state_file(),
        )

    def usage(self) -> RegistryDiskUsage | None:
        now = self._clock()
        with self._guard:
            if self._cached_at is not None and now - self._cached_at < self.cache_seconds:
                return self._cached
            usage = measure_registry_disk(
                self.paths,
                cleanup_percent=self.cleanup_percent,
                refuse_percent=self.refuse_percent,
                target_percent=self.target_percent,
                statvfs=self._statvfs,
            )
            self._cached, self._cached_at = usage, now
            if usage is not None and usage.refusing_writes and (
                self._last_warning_at is None or now - self._last_warning_at >= 60.0
            ):
                self._last_warning_at = now
                _LOG.warning(
                    "registry disk %s is %.1f%% full (refuse threshold %.0f%%); "
                    "refusing image builds and imports",
                    usage.path, usage.used_percent, usage.refuse_percent,
                )
            return usage

    def refusal(self) -> RegistryDiskUsage | None:
        """The current usage when new registry writes must be refused."""

        usage = self.usage()
        return usage if usage is not None and usage.refusing_writes else None

    def maintenance_state(self) -> dict[str, Any]:
        if self.maintenance_state_file is None:
            return {}
        now = self._clock()
        with self._guard:
            if (
                self._state_cached_at is not None
                and now - self._state_cached_at < self.cache_seconds
            ):
                return self._state
            self._state = read_registry_maintenance_state(self.maintenance_state_file)
            self._state_cached_at = now
            return self._state

    def evicted_image(self, image_id: str) -> dict[str, Any] | None:
        return evicted_image(self.maintenance_state(), image_id)

    def status(self) -> dict[str, Any]:
        usage = self.usage()
        result: dict[str, Any] = (
            usage.to_dict()
            if usage is not None
            else {
                "state": "unavailable",
                "cleanup_percent": self.cleanup_percent,
                "refuse_percent": self.refuse_percent,
            }
        )
        if self.maintenance_state_file is not None:
            state = dict(self.maintenance_state())
            evicted = state.pop("evicted_images", None)
            state["evicted_image_count"] = len(evicted) if isinstance(evicted, dict) else 0
            result["maintenance"] = state
        return result


def registry_disk_pressure_payload(usage: RegistryDiskUsage, *, action: str) -> dict[str, Any]:
    return {
        "error": (
            f"registry disk is {usage.used_percent:.1f}% full "
            f"(refuse threshold {usage.refuse_percent:g}%); {action} are paused "
            "while registry retention frees space"
        ),
        "error_code": REGISTRY_DISK_PRESSURE_ERROR_CODE,
        "retryable": True,
        "registry_disk": usage.to_dict(),
    }


# Maintenance state is a small JSON document under the gateway state root:
# the last prune and GC outcomes plus manifests deleted since the last GC, so
# pressure cleanup only garbage collects when it can reclaim something and the
# dashboard can show when space was last reclaimed.


def read_registry_maintenance_state(path: Path) -> dict[str, Any]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return raw if isinstance(raw, dict) else {}


def update_registry_maintenance_state(
    path: Path,
    update: Callable[[dict[str, Any]], dict[str, Any]],
) -> dict[str, Any]:
    """Read-modify-write; callers serialize through the maintenance lock."""

    state = update(dict(read_registry_maintenance_state(path)))
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(
        prefix=path.name + ".", suffix=".tmp", dir=path.parent
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(state, handle, sort_keys=True, separators=(",", ":"))
            handle.write("\n")
            os.fchmod(handle.fileno(), 0o644)
            if os.geteuid() == 0:
                # Root maintenance writes; the gateway service account reads.
                owner = path.parent.stat()
                os.fchown(handle.fileno(), owner.st_uid, owner.st_gid)
        os.replace(temporary, path)
    except BaseException:
        Path(temporary).unlink(missing_ok=True)
        raise
    return state


def record_registry_prune(path: Path, *, deleted: int, now: datetime | None = None) -> dict[str, Any]:
    timestamp = (now or datetime.now(timezone.utc)).isoformat()

    def update(state: dict[str, Any]) -> dict[str, Any]:
        pending = state.get("deleted_since_gc")
        pending = pending if isinstance(pending, int) and pending >= 0 else 0
        state.update(
            last_prune_at=timestamp,
            last_prune_deleted=int(deleted),
            deleted_since_gc=pending + int(deleted),
        )
        return state

    return update_registry_maintenance_state(path, update)


def record_registry_gc(
    path: Path,
    *,
    kind: str,
    deleted_bytes: int | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    timestamp = (now or datetime.now(timezone.utc)).isoformat()

    def update(state: dict[str, Any]) -> dict[str, Any]:
        state.update(
            last_gc_at=timestamp,
            last_gc_kind=kind,
            last_gc_deleted_bytes=deleted_bytes,
            deleted_since_gc=0,
        )
        return state

    return update_registry_maintenance_state(path, update)


# Evicted image ids let the gateway answer a create for an evicted managed
# image with a clear "rebuild it" error instead of a registry manifest miss.
MAX_EVICTED_IMAGE_RECORDS = 4096


def record_image_evictions(
    path: Path,
    evicted: list[dict[str, str]],
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    timestamp = (now or datetime.now(timezone.utc)).isoformat()

    def update(state: dict[str, Any]) -> dict[str, Any]:
        records = state.get("evicted_images")
        records = dict(records) if isinstance(records, dict) else {}
        for item in evicted:
            image_id = str(item.get("image_id") or "")
            if image_id:
                records.pop(image_id, None)
                records[image_id] = {
                    "tag": str(item.get("tag") or ""),
                    "evicted_at": timestamp,
                }
        # Insertion order is eviction order; keep the newest.
        state["evicted_images"] = dict(
            list(records.items())[-MAX_EVICTED_IMAGE_RECORDS:]
        )
        # Gateways drop cached manifest resolutions when this changes.
        state["last_eviction_at"] = timestamp
        return state

    return update_registry_maintenance_state(path, update)


def evicted_image(state: dict[str, Any], image_id: str) -> dict[str, Any] | None:
    records = state.get("evicted_images")
    record = records.get(image_id) if isinstance(records, dict) else None
    return dict(record) if isinstance(record, dict) else None
