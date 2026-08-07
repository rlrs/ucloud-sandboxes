from __future__ import annotations

from dataclasses import dataclass, field, fields
from datetime import datetime, timezone
import math
import re
from typing import Any

from .networking import private_network_ids_from_resources


FINAL_JOB_STATES = {"SUCCESS", "FAILURE", "EXPIRED"}
PROVISIONING_JOB_STATES = {"IN_QUEUE", "RUNNING"}
CPU_PRODUCT_RE = re.compile(r"(?:^|[-_])(\d+)[-_]vcpu(?:$|[-_])", re.IGNORECASE)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_millis(value: object) -> datetime | None:
    if (
        not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        or value <= 0
    ):
        return None
    try:
        return datetime.fromtimestamp(value / 1000.0, tz=timezone.utc)
    except (OverflowError, OSError, ValueError):
        return None


def parse_iso_datetime(value: object) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    raw = value.strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def nested_get(payload: object, path: tuple[str, ...]) -> object | None:
    current = payload
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def string_value(value: object) -> str | None:
    if isinstance(value, str) and value:
        return value
    if isinstance(value, dict):
        for key in ("value", "id", "name"):
            candidate = value.get(key)
            if isinstance(candidate, str) and candidate:
                return candidate
    return None


def _optional_float(value: object) -> float | None:
    if value is None or value == "":
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if math.isfinite(parsed) else None


def cpu_count_from_product_id(product_id: str) -> int | None:
    match = CPU_PRODUCT_RE.search(product_id)
    if not match:
        return None
    cpu = int(match.group(1))
    return cpu if cpu > 0 else None


@dataclass(frozen=True)
class ResourceQuantity:
    vcpu: float = 0.0
    memory_mb: int = 0
    disk_mb: int = 0

    @classmethod
    def from_dict(cls, raw: object) -> "ResourceQuantity":
        if not isinstance(raw, dict) or set(raw) != {
            "vcpu",
            "memory_mb",
            "disk_mb",
        }:
            raise ValueError("resource quantity has an invalid schema")
        vcpu = raw["vcpu"]
        memory_mb = raw["memory_mb"]
        disk_mb = raw["disk_mb"]
        if (
            isinstance(vcpu, bool)
            or not isinstance(vcpu, (int, float))
            or not math.isfinite(float(vcpu))
            or float(vcpu) < 0
        ):
            raise ValueError("resource vcpu must be non-negative and finite")
        if (
            isinstance(memory_mb, bool)
            or not isinstance(memory_mb, int)
            or memory_mb < 0
        ):
            raise ValueError("resource memory_mb must be a non-negative integer")
        if isinstance(disk_mb, bool) or not isinstance(disk_mb, int) or disk_mb < 0:
            raise ValueError("resource disk_mb must be a non-negative integer")
        return cls(
            vcpu=float(vcpu),
            memory_mb=memory_mb,
            disk_mb=disk_mb,
        )

    @property
    def is_valid(self) -> bool:
        return (
            math.isfinite(self.vcpu)
            and self.vcpu >= 0
            and self.memory_mb >= 0
            and self.disk_mb >= 0
        )

    def to_dict(self) -> dict[str, float | int]:
        return {
            "vcpu": self.vcpu,
            "memory_mb": self.memory_mb,
            "disk_mb": self.disk_mb,
        }

    def scaled(self, *, cpu: float, memory: float, disk: float) -> "ResourceQuantity":
        return ResourceQuantity(
            vcpu=self.vcpu * cpu,
            memory_mb=int(self.memory_mb * memory),
            disk_mb=int(self.disk_mb * disk),
        )

    def __add__(self, other: "ResourceQuantity") -> "ResourceQuantity":
        return ResourceQuantity(
            vcpu=self.vcpu + other.vcpu,
            memory_mb=self.memory_mb + other.memory_mb,
            disk_mb=self.disk_mb + other.disk_mb,
        )

    def fits_within(self, capacity: "ResourceQuantity") -> bool:
        return (
            self.vcpu <= capacity.vcpu
            and self.memory_mb <= capacity.memory_mb
            and self.disk_mb <= capacity.disk_mb
        )


@dataclass(frozen=True)
class SandboxInventoryEntry:
    """A versioned node-side observation of one sandbox.

    The generation and operation ID let the control plane distinguish a delayed
    response from the current incarnation of a sandbox with the same public ID.
    """

    sandbox_id: str
    generation: int
    operation_id: str
    spec_hash: str
    state: str = ""
    resources: ResourceQuantity = ResourceQuantity()

    def __post_init__(self) -> None:
        if not self.sandbox_id:
            raise ValueError("sandbox inventory id is required")
        if self.generation < 1:
            raise ValueError("sandbox inventory generation must be positive")
        if re.fullmatch(
            r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}", self.operation_id
        ) is None:
            raise ValueError("sandbox inventory operation_id is invalid")
        if re.fullmatch(r"[0-9a-f]{64}", self.spec_hash) is None:
            raise ValueError("sandbox inventory spec_hash is invalid")
        if not self.state:
            raise ValueError("sandbox inventory state is required")
        if not self.resources.is_valid:
            raise ValueError("sandbox inventory resources are invalid")

    @classmethod
    def from_dict(cls, raw: object) -> "SandboxInventoryEntry | None":
        if not isinstance(raw, dict):
            return None
        sandbox_id = str(raw.get("sandbox_id") or "").strip()
        if not sandbox_id:
            return None
        if set(raw) != {
            "sandbox_id",
            "generation",
            "operation_id",
            "spec_hash",
            "state",
            "resources",
        }:
            return None
        generation_raw = raw.get("generation")
        if isinstance(generation_raw, bool):
            return None
        try:
            generation = int(generation_raw)
        except (TypeError, ValueError):
            return None
        if generation < 1:
            return None
        operation_id = raw.get("operation_id")
        spec_hash = raw.get("spec_hash")
        state = raw.get("state")
        if (
            not isinstance(operation_id, str)
            or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}", operation_id)
            is None
            or not isinstance(spec_hash, str)
            or re.fullmatch(r"[0-9a-f]{64}", spec_hash) is None
            or not isinstance(state, str)
            or not state
        ):
            return None
        try:
            resources = ResourceQuantity.from_dict(raw["resources"])
        except ValueError:
            return None
        return cls(
            sandbox_id=sandbox_id,
            generation=generation,
            operation_id=operation_id,
            spec_hash=spec_hash,
            state=state,
            resources=resources,
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "sandbox_id": self.sandbox_id,
            "generation": self.generation,
            "operation_id": self.operation_id,
            "spec_hash": self.spec_hash,
            "state": self.state,
            "resources": self.resources.to_dict(),
        }


@dataclass(frozen=True)
class NodeRuntimeMetrics:
    collected_at: datetime
    cpu_percent: float | None = None
    cpu_vcpu: float | None = None
    cpu_count: int = 0
    memory_total_mb: int = 0
    memory_used_mb: int = 0
    memory_available_mb: int = 0
    memory_percent: float | None = None
    swap_total_mb: int = 0
    swap_used_mb: int = 0
    swap_free_mb: int = 0
    memory_psi_some_avg10: float | None = None
    memory_psi_full_avg10: float | None = None
    load_average_1m: float | None = None
    load_average_5m: float | None = None
    load_average_15m: float | None = None
    storage_hard_capacity_mb: int = 0
    storage_hard_reserved_mb: int = 0
    storage_cache_mb: int = 0
    storage_active_operations: int = 0
    storage_waiting_operations: int = 0
    storage_max_concurrent_operations: int = 0
    storage_published_volumes: int = 0
    storage_error_volumes: int = 0
    storage_device_pool_enabled: bool = False
    storage_device_pool_low_watermark: int = 0
    storage_device_pool_high_watermark: int = 0
    storage_device_pool_idle_devices: int = 0
    storage_ublk_active_devices: int = 0
    storage_ublk_live_devices: int = 0
    storage_device_pool_acquires: int = 0
    storage_device_pool_reused_acquires: int = 0
    storage_device_pool_new_acquires: int = 0
    storage_device_pool_releases: int = 0
    storage_device_pool_discards: int = 0
    image_materialization_active_operations: int = 0
    image_materialization_waiting_operations: int = 0
    image_materialization_max_concurrent_operations: int = 0
    image_pull_active_operations: int = 0
    image_pull_waiting_operations: int = 0
    image_pull_max_concurrent_operations: int = 0

    @classmethod
    def from_dict(cls, raw: object) -> "NodeRuntimeMetrics | None":
        if not isinstance(raw, dict):
            return None
        field_names = {item.name for item in fields(cls)}
        if set(raw) - field_names:
            return None
        collected_at = parse_iso_datetime(raw.get("collected_at"))
        if collected_at is None:
            return None
        float_fields = {
            "cpu_percent",
            "cpu_vcpu",
            "memory_percent",
            "memory_psi_some_avg10",
            "memory_psi_full_avg10",
            "load_average_1m",
            "load_average_5m",
            "load_average_15m",
        }
        values: dict[str, object] = {"collected_at": collected_at}
        for name in field_names - {"collected_at"}:
            value = raw.get(name)
            if name in float_fields:
                parsed = _optional_float(value)
                if value is not None and parsed is None:
                    return None
                values[name] = parsed
            elif name == "storage_device_pool_enabled":
                if value is not None and not isinstance(value, bool):
                    return None
                values[name] = value if value is not None else False
            else:
                if value is None:
                    values[name] = 0
                    continue
                if isinstance(value, bool):
                    return None
                try:
                    parsed_int = int(value)
                except (TypeError, ValueError, OverflowError):
                    return None
                if (
                    parsed_int < 0
                    or isinstance(value, float)
                    and not value.is_integer()
                ):
                    return None
                values[name] = parsed_int
        return cls(**values)

    def to_dict(self) -> dict[str, float | int | str | None]:
        return {
            "collected_at": self.collected_at.isoformat(),
            "cpu_percent": self.cpu_percent,
            "cpu_vcpu": self.cpu_vcpu,
            "cpu_count": self.cpu_count,
            "memory_total_mb": self.memory_total_mb,
            "memory_used_mb": self.memory_used_mb,
            "memory_available_mb": self.memory_available_mb,
            "memory_percent": self.memory_percent,
            "swap_total_mb": self.swap_total_mb,
            "swap_used_mb": self.swap_used_mb,
            "swap_free_mb": self.swap_free_mb,
            "memory_psi_some_avg10": self.memory_psi_some_avg10,
            "memory_psi_full_avg10": self.memory_psi_full_avg10,
            "load_average_1m": self.load_average_1m,
            "load_average_5m": self.load_average_5m,
            "load_average_15m": self.load_average_15m,
            "storage_hard_capacity_mb": self.storage_hard_capacity_mb,
            "storage_hard_reserved_mb": self.storage_hard_reserved_mb,
            "storage_cache_mb": self.storage_cache_mb,
            "storage_active_operations": self.storage_active_operations,
            "storage_waiting_operations": self.storage_waiting_operations,
            "storage_max_concurrent_operations": (
                self.storage_max_concurrent_operations
            ),
            "storage_published_volumes": self.storage_published_volumes,
            "storage_error_volumes": self.storage_error_volumes,
            "storage_device_pool_enabled": self.storage_device_pool_enabled,
            "storage_device_pool_low_watermark": (
                self.storage_device_pool_low_watermark
            ),
            "storage_device_pool_high_watermark": (
                self.storage_device_pool_high_watermark
            ),
            "storage_device_pool_idle_devices": (self.storage_device_pool_idle_devices),
            "storage_ublk_active_devices": self.storage_ublk_active_devices,
            "storage_ublk_live_devices": self.storage_ublk_live_devices,
            "storage_device_pool_acquires": self.storage_device_pool_acquires,
            "storage_device_pool_reused_acquires": (
                self.storage_device_pool_reused_acquires
            ),
            "storage_device_pool_new_acquires": (self.storage_device_pool_new_acquires),
            "storage_device_pool_releases": self.storage_device_pool_releases,
            "storage_device_pool_discards": self.storage_device_pool_discards,
            "image_materialization_active_operations": (
                self.image_materialization_active_operations
            ),
            "image_materialization_waiting_operations": (
                self.image_materialization_waiting_operations
            ),
            "image_materialization_max_concurrent_operations": (
                self.image_materialization_max_concurrent_operations
            ),
            "image_pull_active_operations": self.image_pull_active_operations,
            "image_pull_waiting_operations": self.image_pull_waiting_operations,
            "image_pull_max_concurrent_operations": (
                self.image_pull_max_concurrent_operations
            ),
        }


@dataclass(frozen=True)
class VmJob:
    id: str
    project_id: str | None
    name: str
    application_name: str
    application_version: str
    product_id: str
    product_category: str
    state: str
    hostname: str | None = None
    created_at: datetime | None = None
    started_at: datetime | None = None
    expires_at: datetime | None = None
    cpu: int | None = None
    memory_gb: int | None = None
    disk_gb: int | None = None
    ssh_enabled: bool | None = None
    private_network_ids: tuple[str, ...] = ()
    queue_status: str | None = None
    latest_note: str | None = None
    labels: dict[str, str] = field(default_factory=dict)
    raw: dict[str, Any] = field(default_factory=dict, repr=False, compare=False)

    @property
    def is_vm(self) -> bool:
        return self.application_name.startswith("vm-")

    @property
    def is_final(self) -> bool:
        return self.state in FINAL_JOB_STATES

    @property
    def is_provisioning_or_running(self) -> bool:
        return self.state in PROVISIONING_JOB_STATES or self.is_initially_suspended

    @property
    def is_initially_suspended(self) -> bool:
        """UCloud commonly reports a new VM suspended before its first start."""

        return self.state == "SUSPENDED" and self.started_at is None

    @property
    def is_unexpectedly_suspended(self) -> bool:
        """Return whether a VM that previously ran has been powered off by UCloud."""

        return self.state == "SUSPENDED" and self.started_at is not None

    @property
    def has_post_start_suspension(self) -> bool:
        """Return whether UCloud powered this VM off after it had run.

        UCloud's VM application initially reports ``SUSPENDED`` while a new
        guest is being provisioned.  That first transition is harmless.  A
        later suspension is a destructive power cycle: the resumed job id does
        not retain the guest's local disk.  Inspect the ordered update history
        so a later, misleading ``RUNNING`` state cannot resurrect the old node
        incarnation.
        """

        seen_running = False
        raw_updates = self.raw.get("updates")
        if isinstance(raw_updates, list):
            for update in raw_updates:
                if not isinstance(update, dict):
                    continue
                state = str(update.get("state") or "").strip().upper()
                if state == "RUNNING":
                    seen_running = True
                elif state == "SUSPENDED" and seen_running:
                    return True
        return self.is_unexpectedly_suspended


@dataclass(frozen=True)
class NodeHeartbeat:
    node_id: str
    job_id: str
    updated_at: datetime
    active_sandboxes: int
    active_image_builds: int = 0
    active_sandbox_creates: int = 0
    idle_since: datetime | None = None
    draining: bool = False
    node_url: str | None = None
    agent_version: str = ""
    deployment_id: str = ""
    init_version: str = ""
    capabilities: tuple[str, ...] = ()
    total_resources: ResourceQuantity = ResourceQuantity()
    used_resources: ResourceQuantity = ResourceQuantity()
    cpu_overcommit: float = 1.0
    memory_overcommit: float = 1.0
    disk_overcommit: float = 1.0
    labels: dict[str, str] = field(default_factory=dict)
    cached_images: tuple[str, ...] = ()
    cached_images_known: bool = False
    runtime_metrics: NodeRuntimeMetrics | None = None
    reported_at: datetime | None = None
    received_at: datetime | None = None
    node_epoch: str = ""
    retired_node_epochs: tuple[str, ...] = ()
    activity_epoch: int = 0
    inventory: tuple[SandboxInventoryEntry, ...] = ()
    inventory_complete: bool = False
    reserved_resources: ResourceQuantity = ResourceQuantity()
    build_reserved_resources: ResourceQuantity = ResourceQuantity()
    physical_disk_total_mb: int = 0
    physical_disk_free_mb: int = 0
    drain_token: str = ""
    drain_activity_epoch: int = 0
    admission_open: bool = True

    def is_fresh(self, now: datetime, ttl_seconds: int) -> bool:
        age = (now - self.freshness_at).total_seconds()
        return ttl_seconds >= 0 and 0 <= age <= ttl_seconds

    @property
    def freshness_at(self) -> datetime:
        """Return the gateway-controlled receipt time when it is available."""

        return self.received_at or self.updated_at

    @property
    def effective_resources(self) -> ResourceQuantity:
        return self.total_resources.scaled(
            cpu=max(0.0, self.cpu_overcommit),
            memory=max(0.0, self.memory_overcommit),
            disk=min(1.0, max(0.0, self.disk_overcommit)),
        )

    @property
    def free_resources(self) -> ResourceQuantity:
        effective = self.effective_resources
        unavailable = (
            self.used_resources
            + self.reserved_resources
            + self.build_reserved_resources
        )
        disk_mb = max(0, effective.disk_mb - unavailable.disk_mb)
        metrics = self.runtime_metrics
        if (
            "storage-native-v1" in self.capabilities
            and metrics is not None
            and metrics.storage_hard_capacity_mb > 0
        ):
            # The storage daemon is the physical admission authority. Route
            # accounting can lag a mounted volume transition or a durable
            # deletion retry, so nominal node disk must never over-credit the
            # bytes the daemon has already promised.
            disk_mb = min(
                disk_mb,
                max(
                    0,
                    metrics.storage_hard_capacity_mb - metrics.storage_hard_reserved_mb,
                ),
            )
        return ResourceQuantity(
            vcpu=max(0.0, effective.vcpu - unavailable.vcpu),
            memory_mb=max(0, effective.memory_mb - unavailable.memory_mb),
            disk_mb=disk_mb,
        )

    @property
    def active_workloads(self) -> int:
        reserved = self.reserved_resources + self.build_reserved_resources
        reserved_work = int(
            reserved.vcpu > 0 or reserved.memory_mb > 0 or reserved.disk_mb > 0
        )
        return (
            max(0, self.active_sandboxes)
            + max(0, self.active_image_builds)
            + max(0, self.active_sandbox_creates)
            + reserved_work
        )


@dataclass(frozen=True)
class SandboxNode:
    job: VmJob
    heartbeat: NodeHeartbeat | None
    active_sandboxes: int
    heartbeat_fresh: bool
    agent_version_compatible: bool = True
    permanently_lost: bool = False

    @property
    def job_id(self) -> str:
        return self.job.id

    @property
    def state(self) -> str:
        return self.job.state

    @property
    def is_ready(self) -> bool:
        return bool(
            not self.permanently_lost
            and self.job.state == "RUNNING"
            and self.heartbeat_fresh
        )

    @property
    def is_schedulable(self) -> bool:
        heartbeat = self.heartbeat
        return bool(
            self.is_ready
            and self.agent_version_compatible
            and heartbeat is not None
            and not heartbeat.draining
            and heartbeat.admission_open
        )

    @property
    def is_provisioning(self) -> bool:
        return bool(
            not self.permanently_lost
            and (
                self.job.state == "IN_QUEUE"
                or self.job.is_initially_suspended
                or (
                    self.job.state == "RUNNING"
                    and self.heartbeat is None
                    and not self.heartbeat_fresh
                )
            )
        )

    @property
    def is_unreachable(self) -> bool:
        return bool(
            not self.permanently_lost
            and self.job.state == "RUNNING"
            and self.heartbeat is not None
            and not self.heartbeat_fresh
        )

    @property
    def is_idle(self) -> bool:
        heartbeat_workloads = self.heartbeat.active_workloads if self.heartbeat else 0
        return self.is_ready and self.active_sandboxes == 0 and heartbeat_workloads == 0


@dataclass(frozen=True)
class SandboxPlacementRequest:
    resources: ResourceQuantity
    excluded_job_ids: tuple[str, ...] = ()
    owned_job_id: str = ""
    owned_disk_mb: int = 0


@dataclass(frozen=True)
class SandboxDemand:
    pending_resources: ResourceQuantity = ResourceQuantity()
    suppressed_pending_resources: ResourceQuantity = ResourceQuantity()
    pending_count: int = 0
    suppressed_pending_count: int = 0
    prepared_resources: ResourceQuantity = ResourceQuantity()
    oldest_pending_seconds: int = 0
    placement_requests: tuple[SandboxPlacementRequest, ...] = ()

    @property
    def desired_resources(self) -> ResourceQuantity:
        return self.pending_resources + self.prepared_resources


@dataclass(frozen=True)
class LiveScaleSignals:
    """Bounded recent observations used to retain latency headroom.

    Requested resources remain the hard placement and disk-admission model.
    These signals only let the autoscaler add capacity sooner and retain it
    longer when the ready pool is measurably busy.
    """

    window_seconds: int = 0
    pressure_samples: int = 0
    latest_pressure_age_seconds: int | None = None
    cpu_utilization: float | None = None
    memory_utilization: float | None = None
    memory_psi_full_avg10: float | None = None
    storage_queue_utilization: float | None = None
    image_materialization_queue_utilization: float | None = None
    create_pressure_samples: int = 0
    latest_create_pressure_age_seconds: int | None = None
    sandbox_create_rejections: int = 0
    sandbox_create_limit: int = 0
    provisioning_samples: int = 0
    provisioning_p95_seconds: float | None = None
    scale_up_wait_samples: int = 0
    scale_up_wait_p95_seconds: float | None = None

    def to_dict(self) -> dict[str, float | int | None]:
        return {
            "window_seconds": self.window_seconds,
            "pressure_samples": self.pressure_samples,
            "latest_pressure_age_seconds": self.latest_pressure_age_seconds,
            "cpu_utilization": self.cpu_utilization,
            "memory_utilization": self.memory_utilization,
            "memory_psi_full_avg10": self.memory_psi_full_avg10,
            "storage_queue_utilization": self.storage_queue_utilization,
            "image_materialization_queue_utilization": (
                self.image_materialization_queue_utilization
            ),
            "create_pressure_samples": self.create_pressure_samples,
            "latest_create_pressure_age_seconds": (
                self.latest_create_pressure_age_seconds
            ),
            "sandbox_create_rejections": self.sandbox_create_rejections,
            "sandbox_create_limit": self.sandbox_create_limit,
            "provisioning_samples": self.provisioning_samples,
            "provisioning_p95_seconds": self.provisioning_p95_seconds,
            "scale_up_wait_samples": self.scale_up_wait_samples,
            "scale_up_wait_p95_seconds": self.scale_up_wait_p95_seconds,
        }


@dataclass(frozen=True)
class ProgramScaleSignals:
    """Current rollout phases reduced into bounded autoscaler demand."""

    model_wait_requests: int = 0
    ready_to_wake_requests: int = 0
    waking_requests: int = 0
    acting_requests: int = 0
    model_wait_sandboxes: int = 0
    ready_to_wake_sandboxes: int = 0
    model_wait_resources: ResourceQuantity = ResourceQuantity()
    ready_to_wake_resources: ResourceQuantity = ResourceQuantity()
    weighted_model_wait_resources: ResourceQuantity = ResourceQuantity()
    effective_resources: ResourceQuantity = ResourceQuantity()
    ready_placement_requests: tuple[SandboxPlacementRequest, ...] = ()
    oldest_model_wait_seconds: int = 0
    oldest_ready_to_wake_seconds: int = 0
    action_enabled: bool = False

    def to_dict(self) -> dict[str, object]:
        return {
            "model_wait_requests": self.model_wait_requests,
            "ready_to_wake_requests": self.ready_to_wake_requests,
            "waking_requests": self.waking_requests,
            "acting_requests": self.acting_requests,
            "model_wait_sandboxes": self.model_wait_sandboxes,
            "ready_to_wake_sandboxes": self.ready_to_wake_sandboxes,
            "model_wait_resources": self.model_wait_resources.to_dict(),
            "ready_to_wake_resources": self.ready_to_wake_resources.to_dict(),
            "weighted_model_wait_resources": (
                self.weighted_model_wait_resources.to_dict()
            ),
            "effective_resources": self.effective_resources.to_dict(),
            "oldest_model_wait_seconds": self.oldest_model_wait_seconds,
            "oldest_ready_to_wake_seconds": self.oldest_ready_to_wake_seconds,
            "action_enabled": self.action_enabled,
        }


@dataclass(frozen=True)
class ScalePolicy:
    min_nodes: int = 0
    max_nodes: int = 10
    warm_resources: ResourceQuantity = ResourceQuantity()
    max_create_per_cycle: int = 4
    max_stop_per_cycle: int = 1
    max_provisioning_nodes: int = 8
    provisioning_capacity_weight: float = 1.0
    stale_provisioning_after_seconds: int = 300
    stale_provisioning_capacity_weight: float = 0.0
    unreachable_stop_after_seconds: int = 1800
    scale_down_idle_seconds: int = 600
    builder_scale_down_idle_seconds: int = 900
    heartbeat_ttl_seconds: int = 120
    live_pressure_enabled: bool = True
    live_pressure_window_seconds: int = 60
    live_pressure_min_samples: int = 3
    live_pressure_fresh_seconds: int = 30
    target_cpu_utilization: float = 0.70
    target_memory_utilization: float = 0.80
    max_memory_psi_full_avg10: float = 5.0
    target_storage_queue_utilization: float = 0.75
    create_pressure_enabled: bool = True
    create_pressure_window_seconds: int = 30
    create_pressure_min_samples: int = 2
    create_pressure_fresh_seconds: int = 15
    create_target_concurrency_per_node: int = 8
    create_pressure_max_headroom_nodes: int = 1
    pressure_scale_down_cooldown_seconds: int = 300
    provisioning_latency_lookback_seconds: int = 7 * 24 * 60 * 60
    provisioning_scale_down_multiplier: float = 2.0
    program_aware_autoscaling_enabled: bool = False
    model_wait_capacity_weight: float = 0.10
    model_wait_max_headroom_nodes: int = 1
    dynamic_active_admission_enabled: bool = False
    default_node_resources: ResourceQuantity = ResourceQuantity(
        vcpu=32.0,
        memory_mb=98304,
        # 2-TB worker less 440-GiB image storage, 96-GiB swap,
        # 32-GiB disposable block cache, and 16-GiB safety headroom.
        disk_mb=1_449_984,
    )
    cpu_overcommit: float = 1.0
    memory_overcommit: float = 1.0
    disk_overcommit: float = 1.0

    @property
    def schedulable_node_resources(self) -> ResourceQuantity:
        """Expected scheduler capacity of one autoscaled sandbox node."""

        return self.default_node_resources.scaled(
            cpu=max(0.0, self.cpu_overcommit),
            memory=max(0.0, self.memory_overcommit),
            disk=min(1.0, max(0.0, self.disk_overcommit)),
        )


@dataclass(frozen=True)
class ScaleAction:
    kind: str
    count: int = 0
    job_ids: tuple[str, ...] = ()
    reason: str = ""


@dataclass(frozen=True)
class ScaleDecision:
    actions: tuple[ScaleAction, ...]
    ready_nodes: int
    provisioning_nodes: int
    total_nodes: int
    reasons: tuple[str, ...]
    unreachable_nodes: int = 0
    pending_resources: ResourceQuantity = ResourceQuantity()
    suppressed_pending_resources: ResourceQuantity = ResourceQuantity()
    pending_count: int = 0
    suppressed_pending_count: int = 0
    prepared_resources: ResourceQuantity = ResourceQuantity()
    desired_resources: ResourceQuantity = ResourceQuantity()
    projected_free_resources: ResourceQuantity = ResourceQuantity()
    resource_deficit: ResourceQuantity = ResourceQuantity()
    live_signals: LiveScaleSignals | None = None
    program_signals: ProgramScaleSignals | None = None
    pressure_scale_up: bool = False
    create_pressure_scale_up: bool = False
    effective_scale_down_idle_seconds: int = 0

    @property
    def creates(self) -> int:
        return sum(action.count for action in self.actions if action.kind == "create")

    @property
    def stops(self) -> tuple[str, ...]:
        stopped: list[str] = []
        for action in self.actions:
            if action.kind == "stop":
                stopped.extend(action.job_ids)
        return tuple(stopped)


def vm_job_from_payload(payload: dict[str, Any]) -> VmJob:
    specification = payload.get("specification")
    if not isinstance(specification, dict):
        specification = {}
    status = payload.get("status")
    if not isinstance(status, dict):
        status = {}
    owner = payload.get("owner")
    if not isinstance(owner, dict):
        owner = {}

    app = specification.get("application")
    if not isinstance(app, dict):
        app = {}
    product = specification.get("product")
    if not isinstance(product, dict):
        product = {}

    resolved_product = nested_get(
        status, ("jobParametersJson", "request", "resolvedProduct")
    )
    if not isinstance(resolved_product, dict):
        resolved_product = {}

    machine_type = nested_get(status, ("jobParametersJson", "machineType"))
    if not isinstance(machine_type, dict):
        machine_type = {}

    disk = nested_get(specification, ("parameters", "diskSize", "value"))
    raw_labels = specification.get("labels")
    labels = raw_labels if isinstance(raw_labels, dict) else {}
    cpu_value = resolved_product.get("cpu", machine_type.get("cpu"))
    memory_value = resolved_product.get(
        "memoryInGigs", machine_type.get("memoryInGigs")
    )
    product_id = str(product.get("id") or "")
    cpu = int(cpu_value) if isinstance(cpu_value, (int, float)) else None
    if cpu is None:
        cpu = cpu_count_from_product_id(product_id)

    updates = payload.get("updates")
    latest_update = updates[-1] if isinstance(updates, list) and updates else {}
    latest_note = (
        latest_update.get("status") if isinstance(latest_update, dict) else None
    )

    ssh_enabled = nested_get(status, ("jobParametersJson", "request", "sshEnabled"))
    queue_status = nested_get(
        status,
        ("jobParametersJson", "request", "resolvedSupport", "support", "queueStatus"),
    )

    return VmJob(
        id=str(payload.get("id") or ""),
        project_id=string_value(owner.get("project")),
        name=str(specification.get("name") or ""),
        application_name=str(app.get("name") or ""),
        application_version=str(app.get("version") or ""),
        product_id=product_id,
        product_category=str(product.get("category") or ""),
        state=str(status.get("state") or ""),
        hostname=string_value(specification.get("hostname")),
        created_at=parse_millis(payload.get("createdAt")),
        started_at=parse_millis(status.get("startedAt")),
        expires_at=parse_millis(status.get("expiresAt")),
        cpu=cpu,
        memory_gb=int(memory_value) if isinstance(memory_value, (int, float)) else None,
        disk_gb=int(disk) if isinstance(disk, (int, float)) else None,
        ssh_enabled=ssh_enabled if isinstance(ssh_enabled, bool) else None,
        private_network_ids=private_network_ids_from_resources(
            specification.get("resources")
        ),
        queue_status=queue_status if isinstance(queue_status, str) else None,
        latest_note=latest_note if isinstance(latest_note, str) else None,
        labels={str(k): str(v) for k, v in labels.items()},
        raw=payload,
    )
