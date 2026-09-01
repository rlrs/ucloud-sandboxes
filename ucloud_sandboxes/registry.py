from __future__ import annotations

from dataclasses import dataclass, fields, replace
import math
from typing import Any
from urllib.parse import urlparse

from .deployment import AGENT_VERSION_LABEL, agent_version_is_schedulable
from .models import (
    NodeHeartbeat,
    NodeRuntimeMetrics,
    ResourceQuantity,
    SandboxInventoryEntry,
    SandboxNode,
    ScalePolicy,
    ProviderInstance,
    parse_iso_datetime,
    utc_now,
)


def heartbeat_to_dict(heartbeat: NodeHeartbeat) -> dict[str, Any]:
    raw = {item.name: getattr(heartbeat, item.name) for item in fields(NodeHeartbeat)}
    raw["updated_at"] = heartbeat.updated_at.isoformat()
    raw["idle_since"] = (
        heartbeat.idle_since.isoformat() if heartbeat.idle_since is not None else None
    )
    raw["capabilities"] = list(heartbeat.capabilities)
    raw["cached_images"] = list(heartbeat.cached_images)
    raw["cached_images_known"] = heartbeat.cached_images_known
    raw["total_resources"] = heartbeat.total_resources.to_dict()
    raw["used_resources"] = heartbeat.used_resources.to_dict()
    raw["labels"] = dict(heartbeat.labels)
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


class HeartbeatIdentityError(ValueError):
    """A heartbeat would make the persisted node/job binding ambiguous."""


def _assert_heartbeat_binding(
    heartbeats: dict[str, NodeHeartbeat],
    heartbeat: NodeHeartbeat,
) -> None:
    if not heartbeat.node_id or not heartbeat.job_id:
        raise HeartbeatIdentityError("heartbeat node_id and job_id are required")
    if not heartbeat.deployment_id.strip():
        raise HeartbeatIdentityError("heartbeat deployment_id is required")
    node_url = _canonical_heartbeat_node_url(heartbeat.node_url)
    for current in heartbeats.values():
        same_job = current.job_id == heartbeat.job_id
        same_node = current.node_id == heartbeat.node_id
        current_node_url = _canonical_heartbeat_node_url(current.node_url)
        same_node_url = current_node_url == node_url
        if not same_job and not same_node and not (node_url and same_node_url):
            continue
        if (
            not same_job
            or not same_node
            or not same_node_url
            or current.deployment_id != heartbeat.deployment_id
        ):
            raise HeartbeatIdentityError(
                "heartbeat node_id, job_id, node_url, or deployment_id is already bound"
            )


def _canonical_heartbeat_node_url(value: str | None) -> str:
    if not value:
        return ""
    parsed = urlparse(value.strip())
    if (
        parsed.scheme.lower() not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.params
        or parsed.query
        or parsed.fragment
    ):
        raise HeartbeatIdentityError(
            "heartbeat node_url must be an absolute HTTP(S) origin"
        )
    try:
        parsed.port
    except ValueError as exc:
        raise HeartbeatIdentityError(
            "heartbeat node_url must be an absolute HTTP(S) origin"
        ) from exc
    return f"{parsed.scheme.lower()}://{parsed.netloc.lower()}"


def heartbeat_from_dict(raw: dict[str, Any]) -> NodeHeartbeat | None:
    if set(raw) != {item.name for item in fields(NodeHeartbeat)}:
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
    active_sandboxes = _strict_nonnegative_int(raw["active_sandboxes"])
    active_image_builds = _strict_nonnegative_int(raw["active_image_builds"])
    active_sandbox_creates = _strict_nonnegative_int(raw["active_sandbox_creates"])
    if None in {
        active_sandboxes,
        active_image_builds,
        active_sandbox_creates,
    }:
        return None
    resource_fields = (
        raw["total_resources"],
        raw["used_resources"],
        raw["reserved_resources"],
        raw["build_reserved_resources"],
    )
    if any(not _valid_resource_payload(value) for value in resource_fields):
        return None
    labels = raw["labels"]
    if not isinstance(labels, dict) or any(
        not isinstance(key, str) or not isinstance(value, str)
        for key, value in labels.items()
    ):
        return None
    draining = _strict_bool(raw["draining"])
    resources_known = _strict_bool(raw["resources_known"])
    cached_images_known = _strict_bool(raw["cached_images_known"])
    inventory_complete = _strict_bool(raw["inventory_complete"])
    admission_open = _strict_bool(raw["admission_open"])
    if None in {
        draining,
        resources_known,
        cached_images_known,
        inventory_complete,
        admission_open,
    }:
        return None
    string_list_fields = {
        name: _strict_string_tuple(raw[name])
        for name in ("capabilities", "cached_images", "retired_node_epochs")
    }
    if any(value is None for value in string_list_fields.values()):
        return None
    optional_timestamps = {
        name: parse_iso_datetime(raw[name])
        for name in ("idle_since", "reported_at", "received_at")
    }
    if any(
        raw[name] is not None and value is None
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
    if any(not isinstance(raw[name], str) for name in string_fields):
        return None
    deployment_id = raw["deployment_id"].strip()
    if not deployment_id:
        return None
    node_url_raw = raw["node_url"]
    if node_url_raw is not None and string_or_none(node_url_raw) is None:
        return None
    integer_fields = {
        name: _strict_nonnegative_int(raw[name])
        for name in (
            "activity_epoch",
            "physical_disk_total_mb",
            "physical_disk_free_mb",
            "drain_activity_epoch",
        )
    }
    if any(value is None for value in integer_fields.values()):
        return None
    runtime_metrics_raw = raw["runtime_metrics"]
    runtime_metrics = NodeRuntimeMetrics.from_dict(runtime_metrics_raw)
    if runtime_metrics_raw is not None and runtime_metrics is None:
        return None
    raw_inventory = raw["inventory"]
    assert inventory_complete is not None
    if not isinstance(raw_inventory, list):
        return None
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
        draining=draining,
        node_url=None if node_url_raw is None else string_or_none(node_url_raw),
        agent_version=raw["agent_version"],
        deployment_id=deployment_id,
        init_version=raw["init_version"],
        capabilities=string_list_fields["capabilities"],
        total_resources=ResourceQuantity.from_dict(raw["total_resources"]),
        resources_known=resources_known,
        used_resources=ResourceQuantity.from_dict(raw["used_resources"]),
        labels=dict(labels),
        cached_images=string_list_fields["cached_images"],
        cached_images_known=cached_images_known,
        runtime_metrics=runtime_metrics,
        reported_at=optional_timestamps["reported_at"],
        received_at=optional_timestamps["received_at"],
        node_epoch=raw["node_epoch"],
        retired_node_epochs=string_list_fields["retired_node_epochs"],
        activity_epoch=integer_fields["activity_epoch"],
        inventory=inventory,
        inventory_complete=inventory_complete,
        reserved_resources=ResourceQuantity.from_dict(raw["reserved_resources"]),
        build_reserved_resources=ResourceQuantity.from_dict(
            raw["build_reserved_resources"]
        ),
        physical_disk_total_mb=integer_fields["physical_disk_total_mb"],
        physical_disk_free_mb=integer_fields["physical_disk_free_mb"],
        drain_token=raw["drain_token"],
        drain_activity_epoch=integer_fields["drain_activity_epoch"],
        admission_open=admission_open,
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
) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value if value >= 0 else None


def _strict_nonnegative_float(
    value: object,
) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    parsed = float(value)
    return parsed if math.isfinite(parsed) and parsed >= 0 else None


def _valid_resource_payload(value: object) -> bool:
    if not isinstance(value, dict):
        return False
    if set(value) != {"vcpu", "memory_mb", "disk_mb"}:
        return False
    return (
        _strict_nonnegative_float(value["vcpu"]) is not None
        and _strict_nonnegative_int(value["memory_mb"]) is not None
        and _strict_nonnegative_int(value["disk_mb"]) is not None
    )


def _strict_bool(value: object) -> bool | None:
    return value if isinstance(value, bool) else None


def _strict_string_tuple(value: object) -> tuple[str, ...] | None:
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        return None
    if any(not item or item != item.strip() for item in value):
        return None
    if len(value) != len(set(value)):
        return None
    return tuple(value)


def merge_jobs_and_heartbeats(
    jobs: list[ProviderInstance],
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


def _agent_version_compatible(
    job: ProviderInstance, heartbeat: NodeHeartbeat | None
) -> bool:
    version = (
        heartbeat.agent_version
        if heartbeat is not None and heartbeat.agent_version
        else ""
    )
    if not version:
        version = job.labels.get(AGENT_VERSION_LABEL, "")
    return agent_version_is_schedulable(version)
