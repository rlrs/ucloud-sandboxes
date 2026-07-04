from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import re
from typing import Any

from .networking import private_network_ids_from_resources


FINAL_JOB_STATES = {"SUCCESS", "FAILURE", "EXPIRED"}
PROVISIONING_JOB_STATES = {"IN_QUEUE", "SUSPENDED", "RUNNING"}
CPU_PRODUCT_RE = re.compile(r"(?:^|[-_])(\d+)[-_]vcpu(?:$|[-_])", re.IGNORECASE)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_millis(value: object) -> datetime | None:
    if not isinstance(value, (int, float)) or value <= 0:
        return None
    return datetime.fromtimestamp(value / 1000.0, tz=timezone.utc)


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


def _first_present(raw: dict[str, Any], *keys: str) -> object:
    for key in keys:
        if key in raw:
            return raw[key]
    return None


def _optional_float(value: object) -> float | None:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


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
        if not isinstance(raw, dict):
            return cls()
        return cls(
            vcpu=float(raw.get("vcpu") or raw.get("cpu") or 0.0),
            memory_mb=int(raw.get("memory_mb") or raw.get("memoryMb") or 0),
            disk_mb=int(raw.get("disk_mb") or raw.get("diskMb") or 0),
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
class NodeRuntimeMetrics:
    collected_at: datetime
    cpu_percent: float | None = None
    cpu_vcpu: float | None = None
    cpu_count: int = 0
    memory_total_mb: int = 0
    memory_used_mb: int = 0
    memory_available_mb: int = 0
    memory_percent: float | None = None
    load_average_1m: float | None = None
    load_average_5m: float | None = None
    load_average_15m: float | None = None

    @classmethod
    def from_dict(cls, raw: object) -> "NodeRuntimeMetrics | None":
        if not isinstance(raw, dict):
            return None
        collected_at = parse_iso_datetime(_first_present(raw, "collected_at", "collectedAt"))
        if collected_at is None:
            return None
        return cls(
            collected_at=collected_at,
            cpu_percent=_optional_float(_first_present(raw, "cpu_percent", "cpuPercent")),
            cpu_vcpu=_optional_float(_first_present(raw, "cpu_vcpu", "cpuVcpu")),
            cpu_count=max(0, int(_first_present(raw, "cpu_count", "cpuCount") or 0)),
            memory_total_mb=max(
                0,
                int(_first_present(raw, "memory_total_mb", "memoryTotalMb") or 0),
            ),
            memory_used_mb=max(
                0,
                int(_first_present(raw, "memory_used_mb", "memoryUsedMb") or 0),
            ),
            memory_available_mb=max(
                0,
                int(_first_present(raw, "memory_available_mb", "memoryAvailableMb") or 0),
            ),
            memory_percent=_optional_float(
                _first_present(raw, "memory_percent", "memoryPercent")
            ),
            load_average_1m=_optional_float(
                _first_present(raw, "load_average_1m", "loadAverage1m")
            ),
            load_average_5m=_optional_float(
                _first_present(raw, "load_average_5m", "loadAverage5m")
            ),
            load_average_15m=_optional_float(
                _first_present(raw, "load_average_15m", "loadAverage15m")
            ),
        )

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
            "load_average_1m": self.load_average_1m,
            "load_average_5m": self.load_average_5m,
            "load_average_15m": self.load_average_15m,
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
        return self.state in PROVISIONING_JOB_STATES

@dataclass(frozen=True)
class NodeHeartbeat:
    node_id: str
    job_id: str
    updated_at: datetime
    active_sandboxes: int
    active_image_builds: int = 0
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

    def is_fresh(self, now: datetime, ttl_seconds: int) -> bool:
        age = (now - self.updated_at).total_seconds()
        return age <= ttl_seconds

    @property
    def effective_resources(self) -> ResourceQuantity:
        return self.total_resources.scaled(
            cpu=max(0.0, self.cpu_overcommit),
            memory=max(0.0, self.memory_overcommit),
            disk=max(0.0, self.disk_overcommit),
        )

    @property
    def free_resources(self) -> ResourceQuantity:
        effective = self.effective_resources
        return ResourceQuantity(
            vcpu=max(0.0, effective.vcpu - self.used_resources.vcpu),
            memory_mb=max(0, effective.memory_mb - self.used_resources.memory_mb),
            disk_mb=max(0, effective.disk_mb - self.used_resources.disk_mb),
        )

    @property
    def active_workloads(self) -> int:
        return max(0, self.active_sandboxes) + max(0, self.active_image_builds)


@dataclass(frozen=True)
class SandboxNode:
    job: VmJob
    heartbeat: NodeHeartbeat | None
    active_sandboxes: int
    heartbeat_fresh: bool
    agent_version_compatible: bool = True

    @property
    def job_id(self) -> str:
        return self.job.id

    @property
    def state(self) -> str:
        return self.job.state

    @property
    def is_ready(self) -> bool:
        return self.job.state == "RUNNING" and self.heartbeat_fresh

    @property
    def is_schedulable(self) -> bool:
        return self.is_ready and self.agent_version_compatible

    @property
    def is_provisioning(self) -> bool:
        if not self.agent_version_compatible:
            return False
        return self.job.state in {"IN_QUEUE", "SUSPENDED"} or (
            self.job.state == "RUNNING" and not self.heartbeat_fresh
        )

    @property
    def is_idle(self) -> bool:
        heartbeat_workloads = self.heartbeat.active_workloads if self.heartbeat else 0
        return self.is_ready and self.active_sandboxes == 0 and heartbeat_workloads == 0


@dataclass(frozen=True)
class SandboxDemand:
    pending_resources: ResourceQuantity = ResourceQuantity()
    prepared_resources: ResourceQuantity = ResourceQuantity()
    oldest_pending_seconds: int = 0

    @property
    def desired_resources(self) -> ResourceQuantity:
        return self.pending_resources + self.prepared_resources


@dataclass(frozen=True)
class ScalePolicy:
    min_nodes: int = 0
    max_nodes: int = 10
    warm_resources: ResourceQuantity = ResourceQuantity()
    max_create_per_cycle: int = 1
    max_stop_per_cycle: int = 1
    max_provisioning_nodes: int = 2
    provisioning_capacity_weight: float = 1.0
    stale_provisioning_after_seconds: int = 1800
    stale_provisioning_capacity_weight: float = 0.0
    scale_down_idle_seconds: int = 600
    builder_scale_down_idle_seconds: int = 900
    heartbeat_ttl_seconds: int = 120
    default_node_resources: ResourceQuantity = ResourceQuantity(
        vcpu=16.0,
        memory_mb=32768,
        disk_mb=204800,
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
    pending_resources: ResourceQuantity = ResourceQuantity()
    prepared_resources: ResourceQuantity = ResourceQuantity()
    desired_resources: ResourceQuantity = ResourceQuantity()
    projected_free_resources: ResourceQuantity = ResourceQuantity()
    resource_deficit: ResourceQuantity = ResourceQuantity()

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
    memory_value = resolved_product.get("memoryInGigs", machine_type.get("memoryInGigs"))
    product_id = str(product.get("id") or "")
    cpu = int(cpu_value) if isinstance(cpu_value, (int, float)) else None
    if cpu is None:
        cpu = cpu_count_from_product_id(product_id)

    updates = payload.get("updates")
    latest_update = updates[-1] if isinstance(updates, list) and updates else {}
    latest_note = latest_update.get("status") if isinstance(latest_update, dict) else None

    ssh_enabled = nested_get(status, ("jobParametersJson", "request", "sshEnabled"))
    queue_status = nested_get(
        status, ("jobParametersJson", "request", "resolvedSupport", "support", "queueStatus")
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
