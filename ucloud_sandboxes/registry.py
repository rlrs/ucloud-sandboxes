from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict, dataclass, fields, replace
import fcntl
import json
import math
import os
from pathlib import Path
from threading import RLock, get_ident
import time
from typing import Any, Iterable

from .deployment import AGENT_VERSION_LABEL, agent_version_is_schedulable
from .models import (
    NodeHeartbeat,
    NodeRuntimeMetrics,
    ResourceQuantity,
    SandboxInventoryEntry,
    SandboxNode,
    ScalePolicy,
    VmJob,
    parse_iso_datetime,
    utc_now,
)


_HEARTBEAT_FILE_LOCKS_GUARD = RLock()
_HEARTBEAT_FILE_LOCKS: dict[Path, RLock] = {}


def load_heartbeats(path: Path | None) -> dict[str, NodeHeartbeat]:
    if path is None or not path.exists():
        return {}
    with _heartbeat_file_lock(path):
        return _load_heartbeats_unlocked(path)


def _load_heartbeats_unlocked(path: Path) -> dict[str, NodeHeartbeat]:
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        _quarantine_corrupt_heartbeat_file(path)
        return {}
    if not isinstance(raw, dict):
        _quarantine_corrupt_heartbeat_file(path)
        return {}
    nodes = raw.get("nodes", [])
    if not isinstance(nodes, list):
        _quarantine_corrupt_heartbeat_file(path)
        return {}

    heartbeats: dict[str, NodeHeartbeat] = {}
    for item in nodes:
        if not isinstance(item, dict):
            continue
        heartbeat = heartbeat_from_dict(item)
        if heartbeat is not None:
            heartbeats[heartbeat.job_id] = heartbeat
    return heartbeats


def heartbeat_to_dict(heartbeat: NodeHeartbeat) -> dict[str, Any]:
    raw = asdict(heartbeat)
    raw["updated_at"] = heartbeat.updated_at.isoformat()
    raw["idle_since"] = (
        heartbeat.idle_since.isoformat() if heartbeat.idle_since is not None else None
    )
    raw["capabilities"] = list(heartbeat.capabilities)
    raw["cached_images"] = list(heartbeat.cached_images)
    raw["cached_images_known"] = heartbeat.cached_images_known
    raw["total_resources"] = heartbeat.total_resources.to_dict()
    raw["used_resources"] = heartbeat.used_resources.to_dict()
    raw["runtime_metrics"] = (
        heartbeat.runtime_metrics.to_dict()
        if heartbeat.runtime_metrics is not None
        else None
    )
    raw["reported_at"] = (
        heartbeat.reported_at.isoformat() if heartbeat.reported_at is not None else None
    )
    raw["received_at"] = (
        heartbeat.received_at.isoformat() if heartbeat.received_at is not None else None
    )
    raw["node_epoch"] = heartbeat.node_epoch
    raw["retired_node_epochs"] = list(heartbeat.retired_node_epochs)
    raw["activity_epoch"] = heartbeat.activity_epoch
    raw["inventory"] = [item.to_dict() for item in heartbeat.inventory]
    raw["inventory_complete"] = heartbeat.inventory_complete
    raw["reserved_resources"] = heartbeat.reserved_resources.to_dict()
    raw["build_reserved_resources"] = heartbeat.build_reserved_resources.to_dict()
    raw["physical_disk_total_mb"] = heartbeat.physical_disk_total_mb
    raw["physical_disk_free_mb"] = heartbeat.physical_disk_free_mb
    raw["drain_token"] = heartbeat.drain_token
    raw["drain_activity_epoch"] = heartbeat.drain_activity_epoch
    raw["admission_open"] = heartbeat.admission_open
    return raw


@dataclass(frozen=True)
class HeartbeatReceiptResult:
    stored: NodeHeartbeat
    previous: NodeHeartbeat | None
    accepted: bool


class HeartbeatStore:
    def __init__(self, path: Path) -> None:
        self.path = path

    def load(self) -> dict[str, NodeHeartbeat]:
        return load_heartbeats(self.path)

    def upsert(self, heartbeat: NodeHeartbeat) -> dict[str, NodeHeartbeat]:
        with _heartbeat_file_lock(self.path):
            heartbeats = _load_heartbeats_unlocked(self.path)
            heartbeat = normalize_idle_since(
                heartbeat,
                previous=heartbeats.get(heartbeat.job_id),
            )
            heartbeats[heartbeat.job_id] = heartbeat
            _save_heartbeats_unlocked(self.path, heartbeats)
            return heartbeats

    def upsert_received(self, heartbeat: NodeHeartbeat) -> HeartbeatReceiptResult:
        """Atomically persist a gateway-stamped heartbeat if it is still current."""

        if heartbeat.received_at is None:
            raise ValueError("received heartbeat requires a gateway receipt timestamp")
        with _heartbeat_file_lock(self.path):
            heartbeats = _load_heartbeats_unlocked(self.path)
            previous = heartbeats.get(heartbeat.job_id)
            if (
                previous is not None
                and previous.received_at is not None
                and heartbeat.received_at < previous.received_at
            ):
                return HeartbeatReceiptResult(
                    stored=previous,
                    previous=previous,
                    accepted=False,
                )
            retired_node_epochs: set[str] = set()
            if previous is not None:
                retired_node_epochs.update(previous.retired_node_epochs)
                identity_changed = (
                    heartbeat.node_id != previous.node_id
                    or heartbeat.node_url != previous.node_url
                    or heartbeat.deployment_id != previous.deployment_id
                )
                if identity_changed:
                    return HeartbeatReceiptResult(
                        stored=previous,
                        previous=previous,
                        accepted=False,
                    )
                if heartbeat.node_epoch != previous.node_epoch:
                    if (
                        not heartbeat.node_epoch
                        or heartbeat.node_epoch in retired_node_epochs
                        or heartbeat.activity_epoch <= previous.activity_epoch
                    ):
                        return HeartbeatReceiptResult(
                            stored=previous,
                            previous=previous,
                            accepted=False,
                        )
                    if previous.node_epoch:
                        retired_node_epochs.add(previous.node_epoch)
                elif heartbeat.activity_epoch < previous.activity_epoch:
                    return HeartbeatReceiptResult(
                        stored=previous,
                        previous=previous,
                        accepted=False,
                    )
            stored = normalize_idle_since(
                replace(
                    heartbeat,
                    retired_node_epochs=tuple(sorted(retired_node_epochs)),
                ),
                previous=previous,
            )
            heartbeats[heartbeat.job_id] = stored
            _save_heartbeats_unlocked(self.path, heartbeats)
            return HeartbeatReceiptResult(
                stored=stored,
                previous=previous,
                accepted=True,
            )

    def remove(self, job_ids: Iterable[str]) -> dict[str, NodeHeartbeat]:
        target_ids = {str(job_id) for job_id in job_ids if str(job_id)}
        if not target_ids:
            return {}
        with _heartbeat_file_lock(self.path):
            heartbeats = _load_heartbeats_unlocked(self.path)
            removed = {
                job_id: heartbeats.pop(job_id)
                for job_id in sorted(target_ids)
                if job_id in heartbeats
            }
            if removed:
                _save_heartbeats_unlocked(self.path, heartbeats)
            return removed

    def save(self, heartbeats: dict[str, NodeHeartbeat]) -> None:
        with _heartbeat_file_lock(self.path):
            _save_heartbeats_unlocked(self.path, heartbeats)


@contextmanager
def _heartbeat_file_lock(path: Path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    local_lock = _local_heartbeat_lock(path)
    with local_lock:
        lock_path = path.with_name(path.name + ".lock")
        with lock_path.open("a+", encoding="utf-8") as lock_file:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def _local_heartbeat_lock(path: Path) -> RLock:
    resolved = path.resolve()
    with _HEARTBEAT_FILE_LOCKS_GUARD:
        lock = _HEARTBEAT_FILE_LOCKS.get(resolved)
        if lock is None:
            lock = RLock()
            _HEARTBEAT_FILE_LOCKS[resolved] = lock
        return lock


def _save_heartbeats_unlocked(
    path: Path,
    heartbeats: dict[str, NodeHeartbeat],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(
        f"{path.name}.tmp-{os.getpid()}-{get_ident()}-{time.monotonic_ns()}"
    )
    nodes = [heartbeat_to_dict(heartbeats[job_id]) for job_id in sorted(heartbeats)]
    payload = json.dumps({"nodes": nodes}, indent=2, sort_keys=True).encode("utf-8")
    try:
        descriptor = os.open(tmp_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        try:
            view = memoryview(payload)
            while view:
                written = os.write(descriptor, view)
                if written <= 0:
                    raise OSError("failed to persist heartbeat state")
                view = view[written:]
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        os.replace(tmp_path, path)
        os.chmod(path, 0o600)
        try:
            directory_fd = os.open(path.parent, os.O_RDONLY)
        except OSError:
            directory_fd = None
        try:
            if directory_fd is not None:
                try:
                    os.fsync(directory_fd)
                except OSError:
                    pass
        finally:
            if directory_fd is not None:
                os.close(directory_fd)
    finally:
        try:
            tmp_path.unlink()
        except FileNotFoundError:
            pass


def _quarantine_corrupt_heartbeat_file(path: Path) -> None:
    quarantine_path = path.with_name(
        f"{path.name}.corrupt-{int(time.time())}-{os.getpid()}-{get_ident()}"
    )
    try:
        path.replace(quarantine_path)
    except OSError:
        pass


def heartbeat_from_dict(raw: dict[str, Any]) -> NodeHeartbeat | None:
    if set(raw) - {item.name for item in fields(NodeHeartbeat)}:
        return None
    node_id = raw.get("node_id")
    job_id = raw.get("job_id")
    updated_at = parse_iso_datetime(raw.get("updated_at"))
    if not isinstance(node_id, str) or not node_id:
        return None
    if not isinstance(job_id, str) or not job_id:
        return None
    if updated_at is None:
        return None
    active_sandboxes = _strict_nonnegative_int(raw.get("active_sandboxes"), default=0)
    active_image_builds = _strict_nonnegative_int(
        raw.get("active_image_builds"),
        default=0,
    )
    active_sandbox_creates = _strict_nonnegative_int(
        raw.get("active_sandbox_creates"),
        default=0,
    )
    cpu_overcommit = _strict_nonnegative_float(raw.get("cpu_overcommit"), default=1.0)
    memory_overcommit = _strict_nonnegative_float(
        raw.get("memory_overcommit"),
        default=1.0,
    )
    disk_overcommit = _strict_nonnegative_float(
        raw.get("disk_overcommit"),
        default=1.0,
    )
    if None in {
        active_sandboxes,
        active_image_builds,
        active_sandbox_creates,
        cpu_overcommit,
        memory_overcommit,
        disk_overcommit,
    }:
        return None
    resource_fields = (
        raw.get("total_resources"),
        raw.get("used_resources"),
        raw.get("reserved_resources"),
        raw.get("build_reserved_resources"),
    )
    if any(not _valid_resource_payload(value) for value in resource_fields):
        return None
    labels = raw.get("labels")
    if labels is not None and (
        not isinstance(labels, dict)
        or any(
            not isinstance(key, str) or not isinstance(value, str)
            for key, value in labels.items()
        )
    ):
        return None
    draining = _strict_bool(raw.get("draining"), default=False)
    cached_images_known = _strict_bool(
        raw.get("cached_images_known"),
        default=False,
    )
    inventory_complete = _strict_bool(
        raw.get("inventory_complete"),
        default=False,
    )
    admission_open = _strict_bool(
        raw.get("admission_open"),
        default=True,
    )
    if None in {
        draining,
        cached_images_known,
        inventory_complete,
        admission_open,
    }:
        return None
    string_list_fields = {
        name: _strict_string_tuple(raw.get(name))
        for name in ("capabilities", "cached_images", "retired_node_epochs")
    }
    if any(value is None for value in string_list_fields.values()):
        return None
    optional_timestamps = {
        name: parse_iso_datetime(raw.get(name))
        for name in ("idle_since", "reported_at", "received_at")
    }
    if any(
        raw.get(name) is not None and value is None
        for name, value in optional_timestamps.items()
    ):
        return None
    string_fields = (
        "agent_version",
        "deployment_id",
        "init_version",
        "node_epoch",
        "drain_token",
    )
    if any(name in raw and not isinstance(raw[name], str) for name in string_fields):
        return None
    node_url_raw = raw.get("node_url")
    if node_url_raw is not None and not isinstance(node_url_raw, str):
        return None
    integer_fields = {
        name: _strict_nonnegative_int(raw.get(name), default=0)
        for name in (
            "activity_epoch",
            "physical_disk_total_mb",
            "physical_disk_free_mb",
            "drain_activity_epoch",
        )
    }
    if any(value is None for value in integer_fields.values()):
        return None
    runtime_metrics_raw = raw.get("runtime_metrics")
    runtime_metrics = NodeRuntimeMetrics.from_dict(runtime_metrics_raw)
    if runtime_metrics_raw is not None and runtime_metrics is None:
        return None
    raw_inventory = raw.get("inventory")
    assert inventory_complete is not None
    if raw_inventory is None:
        if inventory_complete:
            return None
        inventory = ()
    elif not isinstance(raw_inventory, list):
        return None
    else:
        if any(
            not isinstance(raw_item, dict)
            or not _valid_resource_payload(raw_item.get("resources"))
            for raw_item in raw_inventory
        ):
            return None
        parsed_inventory = tuple(
            SandboxInventoryEntry.from_dict(raw_item) for raw_item in raw_inventory
        )
        if any(item is None for item in parsed_inventory):
            return None
        inventory = tuple(item for item in parsed_inventory if item is not None)
    return NodeHeartbeat(
        node_id=node_id,
        job_id=job_id,
        updated_at=updated_at,
        active_sandboxes=active_sandboxes,
        active_image_builds=active_image_builds,
        active_sandbox_creates=active_sandbox_creates,
        idle_since=optional_timestamps["idle_since"],
        draining=bool(draining),
        node_url=string_or_none(raw.get("node_url")),
        agent_version=str(raw.get("agent_version") or ""),
        deployment_id=str(raw.get("deployment_id") or ""),
        init_version=str(raw.get("init_version") or ""),
        capabilities=string_list_fields["capabilities"] or (),
        total_resources=ResourceQuantity.from_dict(raw.get("total_resources")),
        used_resources=ResourceQuantity.from_dict(raw.get("used_resources")),
        cpu_overcommit=cpu_overcommit,
        memory_overcommit=memory_overcommit,
        disk_overcommit=disk_overcommit,
        labels=dict(labels or {}),
        cached_images=string_list_fields["cached_images"] or (),
        cached_images_known=bool(cached_images_known),
        runtime_metrics=runtime_metrics,
        reported_at=optional_timestamps["reported_at"],
        received_at=optional_timestamps["received_at"],
        node_epoch=str(raw.get("node_epoch") or ""),
        retired_node_epochs=string_list_fields["retired_node_epochs"] or (),
        activity_epoch=integer_fields["activity_epoch"] or 0,
        inventory=inventory,
        inventory_complete=inventory_complete,
        reserved_resources=ResourceQuantity.from_dict(raw.get("reserved_resources")),
        build_reserved_resources=ResourceQuantity.from_dict(
            raw.get("build_reserved_resources")
        ),
        physical_disk_total_mb=integer_fields["physical_disk_total_mb"] or 0,
        physical_disk_free_mb=integer_fields["physical_disk_free_mb"] or 0,
        drain_token=str(raw.get("drain_token") or ""),
        drain_activity_epoch=integer_fields["drain_activity_epoch"] or 0,
        admission_open=bool(admission_open),
    )


def normalize_idle_since(
    heartbeat: NodeHeartbeat,
    *,
    previous: NodeHeartbeat | None,
) -> NodeHeartbeat:
    if heartbeat.active_workloads > 0:
        return replace(heartbeat, idle_since=None)
    if heartbeat.idle_since is not None:
        return heartbeat
    if previous is not None and previous.active_workloads == 0:
        return replace(
            heartbeat,
            idle_since=previous.idle_since or previous.freshness_at,
        )
    return replace(heartbeat, idle_since=heartbeat.freshness_at)


def string_or_none(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None


def _strict_nonnegative_int(
    value: object,
    *,
    default: int,
) -> int | None:
    if value is None:
        return default
    if isinstance(value, bool):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if isinstance(value, float) and not value.is_integer():
        return None
    return parsed if parsed >= 0 else None


def _strict_nonnegative_float(
    value: object,
    *,
    default: float,
) -> float | None:
    if value is None:
        return default
    if isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if math.isfinite(parsed) and parsed >= 0 else None


def _valid_resource_payload(value: object) -> bool:
    if not isinstance(value, dict):
        return False
    if set(value) != {"vcpu", "memory_mb", "disk_mb"}:
        return False
    return (
        _strict_nonnegative_float(value.get("vcpu"), default=0.0) is not None
        and _strict_nonnegative_int(value.get("memory_mb"), default=0) is not None
        and _strict_nonnegative_int(value.get("disk_mb"), default=0) is not None
    )


def _strict_bool(value: object, *, default: bool) -> bool | None:
    if value is None:
        return default
    return value if isinstance(value, bool) else None


def _strict_string_tuple(value: object) -> tuple[str, ...] | None:
    if value is None:
        return ()
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        return None
    items = [item.strip() for item in value]
    return tuple(dict.fromkeys(item for item in items if item))


def merge_jobs_and_heartbeats(
    jobs: list[VmJob],
    heartbeats: dict[str, NodeHeartbeat],
    policy: ScalePolicy,
) -> list[SandboxNode]:
    now = utc_now()
    nodes: list[SandboxNode] = []
    for job in jobs:
        heartbeat = heartbeats.get(job.id)
        heartbeat_fresh = (
            heartbeat.is_fresh(now, policy.heartbeat_ttl_seconds)
            if heartbeat is not None
            else False
        )
        nodes.append(
            SandboxNode(
                job=job,
                heartbeat=heartbeat,
                active_sandboxes=(
                    heartbeat.active_sandboxes if heartbeat is not None else 0
                ),
                heartbeat_fresh=heartbeat_fresh,
                agent_version_compatible=_agent_version_compatible(job, heartbeat),
            )
        )
    return nodes


def _agent_version_compatible(job: VmJob, heartbeat: NodeHeartbeat | None) -> bool:
    version = (
        heartbeat.agent_version
        if heartbeat is not None and heartbeat.agent_version
        else ""
    )
    if not version:
        version = job.labels.get(AGENT_VERSION_LABEL, "")
    return agent_version_is_schedulable(version)
