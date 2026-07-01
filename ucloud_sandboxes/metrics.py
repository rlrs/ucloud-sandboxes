from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any

from .models import NodeHeartbeat, ResourceQuantity, parse_iso_datetime, utc_now
from .routing import PendingSandboxDemand, RoutingState, SandboxRoute


DEFAULT_RECENT_EVENT_LIMIT = 50
DEFAULT_SCALE_UP_SAMPLE_LIMIT = 200
DEFAULT_VM_LIFECYCLE_LIMIT = 100


@dataclass(frozen=True)
class MetricEvent:
    timestamp: str
    kind: str
    data: dict[str, Any]

    @classmethod
    def from_dict(cls, raw: object) -> "MetricEvent | None":
        if not isinstance(raw, dict):
            return None
        timestamp = raw.get("timestamp")
        kind = raw.get("kind")
        data = raw.get("data")
        if not isinstance(timestamp, str) or not timestamp:
            return None
        if not isinstance(kind, str) or not kind:
            return None
        if not isinstance(data, dict):
            data = {}
        return cls(
            timestamp=timestamp,
            kind=kind,
            data={str(key): value for key, value in data.items()},
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "kind": self.kind,
            "data": self.data,
        }


class MetricsStore:
    def __init__(self, path: Path) -> None:
        self.path = path

    def append(
        self,
        kind: str,
        data: dict[str, Any] | None = None,
        *,
        timestamp: str | None = None,
    ) -> MetricEvent:
        event = MetricEvent(
            timestamp=timestamp or utc_now().isoformat(),
            kind=kind,
            data=data or {},
        )
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(event.to_dict(), sort_keys=True))
            handle.write("\n")
        return event

    def load_events(self, *, max_events: int = 1000) -> list[MetricEvent]:
        if not self.path.exists():
            return []
        events: list[MetricEvent] = []
        for line in self.path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                parsed = json.loads(line)
            except json.JSONDecodeError:
                continue
            event = MetricEvent.from_dict(parsed)
            if event is not None:
                events.append(event)
        if max_events <= 0:
            return events
        return events[-max_events:]


def record_sandbox_scheduled(
    store: MetricsStore | None,
    *,
    sandbox_id: str,
    route: SandboxRoute,
    resources: ResourceQuantity,
    pending: PendingSandboxDemand | None,
) -> None:
    if store is None:
        return
    now = utc_now()
    pending_created_at = parse_iso_datetime(pending.created_at) if pending else None
    wait_ms = (
        max(0, int((now - pending_created_at).total_seconds() * 1000))
        if pending_created_at is not None
        else None
    )
    store.append(
        "sandbox_scheduled",
        {
            "sandbox_id": sandbox_id,
            "node_id": route.node_id,
            "job_id": route.job_id,
            "resources": resources.to_dict(),
            "had_pending_demand": pending is not None,
            "pending_attempts": pending.attempts if pending is not None else 0,
            "scale_up_wait_ms": wait_ms,
        },
    )


def record_sandbox_pending_deleted(
    store: MetricsStore | None,
    *,
    sandbox_id: str,
    pending: PendingSandboxDemand | None,
) -> None:
    if store is None or pending is None:
        return
    created_at = parse_iso_datetime(pending.created_at)
    age_ms = (
        max(0, int((utc_now() - created_at).total_seconds() * 1000))
        if created_at is not None
        else None
    )
    store.append(
        "sandbox_pending_deleted",
        {
            "sandbox_id": sandbox_id,
            "resources": pending.resources.to_dict(),
            "pending_attempts": pending.attempts,
            "pending_age_ms": age_ms,
        },
    )


def record_autoscaler_cycle(
    store: MetricsStore | None,
    *,
    cycle: int,
    result: dict[str, Any],
) -> None:
    if store is None:
        return
    decision = result.get("decision") if isinstance(result.get("decision"), dict) else {}
    builder_decision = (
        result.get("builderDecision")
        if isinstance(result.get("builderDecision"), dict)
        else {}
    )
    store.append(
        "autoscaler_cycle",
        {
            "cycle": cycle,
            "pending_resources": decision.get("pendingResources", {}),
            "prepared_resources": decision.get("preparedResources", {}),
            "desired_resources": decision.get("desiredResources", {}),
            "projected_free_resources": decision.get("projectedFreeResources", {}),
            "resource_deficit": decision.get("resourceDeficit", {}),
            "ready_nodes": decision.get("readyNodes", 0),
            "provisioning_nodes": decision.get("provisioningNodes", 0),
            "total_nodes": decision.get("totalNodes", 0),
            "actions": decision.get("actions", []),
            "created_job_ids": result.get("createdJobIds", []),
            "stop_job_ids": result.get("stopJobIds", []),
            "pending_image_builds": result.get("pendingImageBuilds", 0),
            "prepared_builder_count": result.get("preparedBuilderCount", 0),
            "builder_actions": builder_decision.get("actions", []),
        },
    )


def record_vm_submitted(
    store: MetricsStore | None,
    *,
    cycle: int,
    job_id: str,
    intent: Any,
) -> None:
    if store is None:
        return
    options = getattr(intent, "options", None)
    labels = getattr(options, "labels", None) or {}
    role = "builder" if labels.get("ucloud-sandboxes/builder") == "true" else "sandbox"
    product = getattr(options, "product", None)
    application = getattr(options, "application", None)
    store.append(
        "vm_submitted",
        {
            "cycle": cycle,
            "job_id": job_id,
            "role": role,
            "node_id": getattr(intent, "node_id", ""),
            "node_url": getattr(intent, "node_url", ""),
            "name": getattr(options, "name", ""),
            "hostname": getattr(options, "hostname", ""),
            "product_id": getattr(product, "id", ""),
            "product_category": getattr(product, "category", ""),
            "application_name": getattr(application, "name", ""),
            "application_version": getattr(application, "version", ""),
            "disk_gb": getattr(options, "disk_gb", None),
        },
    )


def record_vm_observed(
    store: MetricsStore | None,
    *,
    cycle: int,
    node: Any,
) -> None:
    if store is None:
        return
    job = getattr(node, "job", None)
    if job is None:
        return
    store.append(
        "vm_observed",
        {
            "cycle": cycle,
            "job_id": getattr(job, "id", ""),
            "role": _node_role(node),
            "state": getattr(job, "state", ""),
            "name": getattr(job, "name", ""),
            "hostname": getattr(job, "hostname", "") or "",
            "created_at": _iso_or_none(getattr(job, "created_at", None)),
            "started_at": _iso_or_none(getattr(job, "started_at", None)),
            "expires_at": _iso_or_none(getattr(job, "expires_at", None)),
            "latest_note": getattr(job, "latest_note", "") or "",
            "queue_status": getattr(job, "queue_status", "") or "",
            "product_id": getattr(job, "product_id", ""),
            "cpu": getattr(job, "cpu", None),
            "memory_gb": getattr(job, "memory_gb", None),
            "disk_gb": getattr(job, "disk_gb", None),
            "ready": bool(getattr(node, "is_ready", False)),
            "provisioning": bool(getattr(node, "is_provisioning", False)),
            "heartbeat_fresh": bool(getattr(node, "heartbeat_fresh", False)),
        },
    )


def record_vm_init_attempt(
    store: MetricsStore | None,
    *,
    job_id: str,
    node_id: str,
    role: str,
    status: str,
    attempts: int,
    started_at: str,
    finished_at: str,
    duration_ms: int,
    stage_duration_ms: int | None = None,
    run_duration_ms: int | None = None,
    returncode: int | None = None,
    error: str = "",
    skipped: bool = False,
    reason: str = "",
) -> None:
    if store is None:
        return
    store.append(
        "vm_init_attempt",
        {
            "job_id": job_id,
            "node_id": node_id,
            "role": role,
            "status": status,
            "attempts": attempts,
            "started_at": started_at,
            "finished_at": finished_at,
            "duration_ms": duration_ms,
            "stage_duration_ms": stage_duration_ms,
            "run_duration_ms": run_duration_ms,
            "returncode": returncode,
            "error": error,
            "skipped": skipped,
            "reason": reason,
        },
    )


def record_node_heartbeat(
    store: MetricsStore | None,
    heartbeat: NodeHeartbeat,
) -> None:
    if store is None:
        return
    effective = heartbeat.effective_resources
    used = heartbeat.used_resources
    store.append(
        "node_heartbeat",
        {
            "node_id": heartbeat.node_id,
            "job_id": heartbeat.job_id,
            "node_url": heartbeat.node_url or "",
            "active_sandboxes": heartbeat.active_sandboxes,
            "draining": heartbeat.draining,
            "capabilities": list(heartbeat.capabilities),
            "agent_version": heartbeat.agent_version,
            "deployment_id": heartbeat.deployment_id,
            "init_version": heartbeat.init_version,
            "total_resources": heartbeat.total_resources.to_dict(),
            "effective_resources": effective.to_dict(),
            "used_resources": used.to_dict(),
            "free_resources": heartbeat.free_resources.to_dict(),
            "load": _resource_load(used, effective),
            "actual_usage": (
                heartbeat.runtime_metrics.to_dict()
                if heartbeat.runtime_metrics is not None
                else None
            ),
            "idle_since": heartbeat.idle_since.isoformat() if heartbeat.idle_since else None,
            "heartbeat_updated_at": heartbeat.updated_at.isoformat(),
        },
    )


def build_metrics_snapshot(
    heartbeats: dict[str, NodeHeartbeat],
    routing_state: RoutingState | None,
    events: list[MetricEvent],
    *,
    heartbeat_ttl_seconds: int,
) -> dict[str, Any]:
    now = utc_now()
    heartbeat_items = list(heartbeats.values())
    fresh = [
        heartbeat
        for heartbeat in heartbeat_items
        if heartbeat.is_fresh(now, heartbeat_ttl_seconds)
    ]
    sandbox_nodes = [
        heartbeat for heartbeat in fresh if "sandbox" in heartbeat.capabilities
    ]
    builder_nodes = [
        heartbeat for heartbeat in fresh if "image-build" in heartbeat.capabilities
    ]
    routing_state = routing_state or RoutingState({}, {}, {}, {})
    fresh_sandbox_nodes = {heartbeat.node_id: heartbeat for heartbeat in sandbox_nodes}
    routes_on_fresh_nodes = sum(
        1
        for route in routing_state.sandboxes.values()
        if route.node_id in fresh_sandbox_nodes
    )
    active_routes = len(routing_state.sandboxes)
    fresh_resources = _aggregate_node_resources(fresh)
    sandbox_resources = _aggregate_node_resources(sandbox_nodes)
    builder_resources = _aggregate_node_resources(builder_nodes)
    provisional_running = _routes_created_after_heartbeat_count(
        routing_state.sandboxes.values(),
        fresh_sandbox_nodes,
    )
    pending_sandboxes = list(routing_state.pending.values())
    prepared_capacity = [
        item
        for item in routing_state.prepared.values()
        if not item.is_expired(now)
    ]
    prepared_builders = [
        item
        for item in routing_state.prepared_builders.values()
        if not item.is_expired(now)
    ]
    pending_builds = list(routing_state.image_builds.values())
    scale_events = [
        event
        for event in events
        if event.kind == "sandbox_scheduled"
        and isinstance(event.data.get("scale_up_wait_ms"), int)
    ][-DEFAULT_SCALE_UP_SAMPLE_LIMIT:]
    node_events = [
        event
        for event in events
        if event.kind == "node_heartbeat"
    ][-DEFAULT_RECENT_EVENT_LIMIT:]
    scale_values = [int(event.data["scale_up_wait_ms"]) for event in scale_events]

    return {
        "generated_at": now.isoformat(),
        "nodes": {
            "total": len(heartbeat_items),
            "fresh": len(fresh),
            "sandbox": len(sandbox_nodes),
            "builder": len(builder_nodes),
            "items": [_node_metrics(heartbeat, now, heartbeat_ttl_seconds) for heartbeat in heartbeat_items],
            "samples": sum(1 for event in events if event.kind == "node_heartbeat"),
            "recent_samples": [event.to_dict() for event in node_events],
        },
        "resources": {
            "fresh": fresh_resources,
            "sandbox": sandbox_resources,
            "builder": builder_resources,
        },
        "sandboxes": {
            "running": sandbox_resources["active_sandboxes"] + provisional_running,
            "active_routes": active_routes,
            "routes_on_fresh_nodes": routes_on_fresh_nodes,
            "provisional_running_routes": provisional_running,
            "stale_routes": max(0, active_routes - routes_on_fresh_nodes),
            "pending": len(pending_sandboxes),
            "pending_resources": _sum_pending_resources(pending_sandboxes).to_dict(),
            "oldest_pending_seconds": _oldest_age_seconds(pending_sandboxes),
            "pending_attempts": sum(item.attempts for item in pending_sandboxes),
        },
        "capacity": {
            "prepared": len(prepared_capacity),
            "prepared_sandboxes": sum(item.count for item in prepared_capacity),
            "prepared_resources": _sum_prepared_resources(prepared_capacity).to_dict(),
            "oldest_prepared_seconds": _oldest_age_seconds(prepared_capacity),
            "next_expiration_seconds": _next_expiration_seconds(prepared_capacity),
            "items": [item.to_dict() for item in prepared_capacity],
        },
        "exec": {
            "sessions": len(routing_state.exec_sessions),
        },
        "images": {
            "pending_builds": len(pending_builds),
            "oldest_pending_build_seconds": _oldest_age_seconds(pending_builds),
        },
        "builders": {
            "prepared": len(prepared_builders),
            "prepared_builders": sum(item.count for item in prepared_builders),
            "oldest_prepared_seconds": _oldest_age_seconds(prepared_builders),
            "next_expiration_seconds": _next_expiration_seconds(prepared_builders),
            "items": [item.to_dict() for item in prepared_builders],
        },
        "scale_up": _scale_up_summary(scale_values, scale_events),
        "vm_lifecycle": _vm_lifecycle_summary(events),
        "events": {
            "recent": [event.to_dict() for event in events[-DEFAULT_RECENT_EVENT_LIMIT:]],
        },
    }


def _node_metrics(
    heartbeat: NodeHeartbeat,
    now: Any,
    heartbeat_ttl_seconds: int,
) -> dict[str, Any]:
    effective = heartbeat.effective_resources
    used = heartbeat.used_resources
    return {
        "node_id": heartbeat.node_id,
        "job_id": heartbeat.job_id,
        "node_url": heartbeat.node_url or "",
        "fresh": heartbeat.is_fresh(now, heartbeat_ttl_seconds),
        "age_seconds": max(0, int((now - heartbeat.updated_at).total_seconds())),
        "active_sandboxes": heartbeat.active_sandboxes,
        "active_image_builds": heartbeat.active_image_builds,
        "active_workloads": heartbeat.active_workloads,
        "draining": heartbeat.draining,
        "capabilities": list(heartbeat.capabilities),
        "agent_version": heartbeat.agent_version,
        "deployment_id": heartbeat.deployment_id,
        "total_resources": heartbeat.total_resources.to_dict(),
        "effective_resources": effective.to_dict(),
        "used_resources": used.to_dict(),
        "free_resources": heartbeat.free_resources.to_dict(),
        "load": _resource_load(used, effective),
        "actual_usage": (
            heartbeat.runtime_metrics.to_dict()
            if heartbeat.runtime_metrics is not None
            else None
        ),
    }


def _aggregate_node_resources(heartbeats: list[NodeHeartbeat]) -> dict[str, Any]:
    effective = ResourceQuantity()
    used = ResourceQuantity()
    free = ResourceQuantity()
    active_sandboxes = 0
    active_image_builds = 0
    for heartbeat in heartbeats:
        effective = effective + heartbeat.effective_resources
        used = used + heartbeat.used_resources
        free = free + heartbeat.free_resources
        active_sandboxes += heartbeat.active_sandboxes
        active_image_builds += heartbeat.active_image_builds
    return {
        "nodes": len(heartbeats),
        "active_sandboxes": active_sandboxes,
        "active_image_builds": active_image_builds,
        "active_workloads": active_sandboxes + active_image_builds,
        "effective": effective.to_dict(),
        "used": used.to_dict(),
        "free": free.to_dict(),
        "load": _resource_load(used, effective),
        "actual_usage": _aggregate_actual_usage(heartbeats),
    }


def _routes_created_after_heartbeat_count(
    routes: Any,
    heartbeats_by_node_id: dict[str, NodeHeartbeat],
) -> int:
    count = 0
    for route in routes:
        heartbeat = heartbeats_by_node_id.get(route.node_id)
        if heartbeat is None:
            continue
        route_created_at = parse_iso_datetime(route.created_at)
        if route_created_at is not None and route_created_at > heartbeat.updated_at:
            count += 1
    return count


def _aggregate_actual_usage(heartbeats: list[NodeHeartbeat]) -> dict[str, Any]:
    metrics = [
        heartbeat.runtime_metrics
        for heartbeat in heartbeats
        if heartbeat.runtime_metrics is not None
    ]
    if not metrics:
        return {
            "samples": 0,
            "cpu_vcpu": None,
            "cpu_percent_avg": None,
            "memory_total_mb": 0,
            "memory_used_mb": 0,
            "memory_available_mb": 0,
            "memory_percent": None,
            "load_average_1m": None,
            "load_average_5m": None,
            "load_average_15m": None,
        }
    cpu_vcpu_values = [
        item.cpu_vcpu for item in metrics if item.cpu_vcpu is not None
    ]
    cpu_percent_values = [
        item.cpu_percent for item in metrics if item.cpu_percent is not None
    ]
    load_1m_values = [
        item.load_average_1m for item in metrics if item.load_average_1m is not None
    ]
    load_5m_values = [
        item.load_average_5m for item in metrics if item.load_average_5m is not None
    ]
    load_15m_values = [
        item.load_average_15m for item in metrics if item.load_average_15m is not None
    ]
    total_memory = sum(item.memory_total_mb for item in metrics)
    used_memory = sum(item.memory_used_mb for item in metrics)
    available_memory = sum(item.memory_available_mb for item in metrics)
    return {
        "samples": len(metrics),
        "cpu_vcpu": sum(cpu_vcpu_values) if cpu_vcpu_values else None,
        "cpu_percent_avg": _avg(cpu_percent_values),
        "memory_total_mb": total_memory,
        "memory_used_mb": used_memory,
        "memory_available_mb": available_memory,
        "memory_percent": (
            (used_memory / total_memory) * 100.0 if total_memory > 0 else None
        ),
        "load_average_1m": _avg(load_1m_values),
        "load_average_5m": _avg(load_5m_values),
        "load_average_15m": _avg(load_15m_values),
    }


def _resource_load(used: ResourceQuantity, total: ResourceQuantity) -> dict[str, float | None]:
    return {
        "vcpu": _ratio(used.vcpu, total.vcpu),
        "memory": _ratio(used.memory_mb, total.memory_mb),
        "disk": _ratio(used.disk_mb, total.disk_mb),
    }


def _ratio(used: float, total: float) -> float | None:
    if total <= 0:
        return None
    return used / total


def _avg(values: list[float | int]) -> float | None:
    if not values:
        return None
    return float(sum(values)) / len(values)


def _sum_pending_resources(items: list[Any]) -> ResourceQuantity:
    total = ResourceQuantity()
    for item in items:
        resources = getattr(item, "resources", ResourceQuantity())
        if isinstance(resources, ResourceQuantity):
            total = total + resources
    return total


def _sum_prepared_resources(items: list[Any]) -> ResourceQuantity:
    total = ResourceQuantity()
    for item in items:
        resources = getattr(item, "total_resources", ResourceQuantity())
        if isinstance(resources, ResourceQuantity):
            total = total + resources
    return total


def _oldest_age_seconds(items: list[Any]) -> int:
    now = utc_now()
    oldest = 0
    for item in items:
        created_at = parse_iso_datetime(getattr(item, "created_at", ""))
        if created_at is not None:
            oldest = max(oldest, int((now - created_at).total_seconds()))
    return max(0, oldest)


def _next_expiration_seconds(items: list[Any]) -> int | None:
    now = utc_now()
    values: list[int] = []
    for item in items:
        expires_at = parse_iso_datetime(getattr(item, "expires_at", ""))
        if expires_at is not None:
            values.append(max(0, int((expires_at - now).total_seconds())))
    return min(values) if values else None


def _scale_up_summary(
    values: list[int],
    events: list[MetricEvent],
) -> dict[str, Any]:
    if not values:
        return {
            "samples": 0,
            "last_ms": None,
            "avg_ms": None,
            "p50_ms": None,
            "p95_ms": None,
            "max_ms": None,
            "recent": [],
        }
    sorted_values = sorted(values)
    return {
        "samples": len(values),
        "last_ms": values[-1],
        "avg_ms": sum(values) / len(values),
        "p50_ms": _percentile(sorted_values, 0.50),
        "p95_ms": _percentile(sorted_values, 0.95),
        "max_ms": max(values),
        "recent": [event.to_dict() for event in events[-DEFAULT_RECENT_EVENT_LIMIT:]],
    }


def _percentile(sorted_values: list[int], quantile: float) -> int:
    if not sorted_values:
        return 0
    index = int(round((len(sorted_values) - 1) * max(0.0, min(1.0, quantile))))
    return sorted_values[index]


def _vm_lifecycle_summary(events: list[MetricEvent]) -> dict[str, Any]:
    records: dict[str, dict[str, Any]] = {}
    lifecycle_events = [
        event
        for event in events
        if event.kind
        in {
            "vm_submitted",
            "vm_observed",
            "vm_init_attempt",
            "node_heartbeat",
            "sandbox_scheduled",
        }
    ]
    for event in lifecycle_events:
        data = event.data
        job_id = data.get("job_id")
        if not isinstance(job_id, str) or not job_id:
            continue
        record = records.setdefault(
            job_id,
            {
                "job_id": job_id,
                "role": "",
                "state": "",
                "node_id": "",
                "submitted_at": None,
                "ucloud_created_at": None,
                "ucloud_started_at": None,
                "first_heartbeat_at": None,
                "last_heartbeat_at": None,
                "first_sandbox_scheduled_at": None,
                "last_sandbox_scheduled_at": None,
                "last_activity_at": event.timestamp,
                "init_attempts": [],
            },
        )
        record["last_activity_at"] = max(
            str(record.get("last_activity_at") or ""), event.timestamp
        )
        if event.kind == "vm_submitted":
            record["submitted_at"] = record.get("submitted_at") or event.timestamp
            _copy_first(record, data, "role")
            _copy_first(record, data, "node_id")
            _copy_first(record, data, "node_url")
            _copy_first(record, data, "hostname")
            _copy_first(record, data, "product_id")
            _copy_first(record, data, "disk_gb")
        elif event.kind == "vm_observed":
            record["state"] = data.get("state") or record.get("state") or ""
            _copy_first(record, data, "role")
            _copy_first(record, data, "node_id")
            _copy_first(record, data, "hostname")
            _copy_first(record, data, "product_id")
            _copy_first(record, data, "disk_gb")
            record["ucloud_created_at"] = (
                data.get("created_at") or record.get("ucloud_created_at")
            )
            record["ucloud_started_at"] = (
                data.get("started_at") or record.get("ucloud_started_at")
            )
            record["latest_note"] = data.get("latest_note") or record.get("latest_note") or ""
            record["ready"] = bool(data.get("ready"))
            record["provisioning"] = bool(data.get("provisioning"))
        elif event.kind == "vm_init_attempt":
            _copy_first(record, data, "role")
            _copy_first(record, data, "node_id")
            attempts = record.setdefault("init_attempts", [])
            if isinstance(attempts, list):
                attempts.append(
                    {
                        "status": data.get("status"),
                        "attempts": data.get("attempts"),
                        "started_at": data.get("started_at"),
                        "finished_at": data.get("finished_at"),
                        "duration_ms": data.get("duration_ms"),
                        "stage_duration_ms": data.get("stage_duration_ms"),
                        "run_duration_ms": data.get("run_duration_ms"),
                        "returncode": data.get("returncode"),
                        "skipped": data.get("skipped"),
                        "reason": data.get("reason") or "",
                    }
                )
        elif event.kind == "node_heartbeat":
            _copy_first(record, data, "node_id")
            heartbeat_at = data.get("heartbeat_updated_at") or event.timestamp
            if not record.get("first_heartbeat_at"):
                record["first_heartbeat_at"] = heartbeat_at
            record["last_heartbeat_at"] = heartbeat_at
        elif event.kind == "sandbox_scheduled":
            scheduled_at = event.timestamp
            if not record.get("first_sandbox_scheduled_at"):
                record["first_sandbox_scheduled_at"] = scheduled_at
                record["first_sandbox_scale_up_wait_ms"] = data.get("scale_up_wait_ms")
            record["last_sandbox_scheduled_at"] = scheduled_at

    items = sorted(
        records.values(),
        key=lambda item: str(item.get("last_activity_at") or ""),
        reverse=True,
    )[:DEFAULT_VM_LIFECYCLE_LIMIT]
    for item in items:
        item["submit_to_running_ms"] = _duration_ms(
            item.get("submitted_at"),
            item.get("ucloud_started_at"),
        )
        item["ucloud_created_to_running_ms"] = _duration_ms(
            item.get("ucloud_created_at"),
            item.get("ucloud_started_at"),
        )
        item["running_to_first_heartbeat_ms"] = _duration_ms(
            item.get("ucloud_started_at"),
            item.get("first_heartbeat_at"),
        )
        item["submit_to_first_heartbeat_ms"] = _duration_ms(
            item.get("submitted_at"),
            item.get("first_heartbeat_at"),
        )
        item["first_heartbeat_to_first_sandbox_ms"] = _duration_ms(
            item.get("first_heartbeat_at"),
            item.get("first_sandbox_scheduled_at"),
        )
        attempts = item.get("init_attempts")
        if isinstance(attempts, list):
            item["init_attempts"] = attempts[-10:]
            succeeded = [
                attempt
                for attempt in attempts
                if attempt.get("status") == "succeeded"
                and isinstance(attempt.get("duration_ms"), int)
            ]
            item["last_successful_init_duration_ms"] = (
                succeeded[-1]["duration_ms"] if succeeded else None
            )
    return {
        "samples": len(records),
        "items": items,
        "recent_events": [
            event.to_dict()
            for event in lifecycle_events[-DEFAULT_RECENT_EVENT_LIMIT:]
        ],
    }


def _copy_first(target: dict[str, Any], source: dict[str, Any], key: str) -> None:
    if target.get(key) in (None, "") and source.get(key) not in (None, ""):
        target[key] = source.get(key)


def _duration_ms(start: object, end: object) -> int | None:
    start_dt = parse_iso_datetime(start)
    end_dt = parse_iso_datetime(end)
    if start_dt is None or end_dt is None:
        return None
    return max(0, int((end_dt - start_dt).total_seconds() * 1000))


def _node_role(node: Any) -> str:
    job = getattr(node, "job", None)
    labels = getattr(job, "labels", {}) if job is not None else {}
    if labels.get("ucloud-sandboxes/builder") == "true":
        return "builder"
    return "sandbox"


def _iso_or_none(value: Any) -> str | None:
    return value.isoformat() if hasattr(value, "isoformat") else None
