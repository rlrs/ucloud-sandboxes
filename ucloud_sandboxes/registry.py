from __future__ import annotations

from dataclasses import asdict, replace
import json
from pathlib import Path
from typing import Any

from .deployment import AGENT_VERSION_LABEL, agent_version_is_compatible
from .models import (
    NodeHeartbeat,
    NodeRuntimeMetrics,
    ResourceQuantity,
    SandboxNode,
    ScalePolicy,
    VmJob,
    parse_iso_datetime,
    utc_now,
)


def load_heartbeats(path: Path | None) -> dict[str, NodeHeartbeat]:
    if path is None or not path.exists():
        return {}
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("Heartbeat file must contain a JSON object.")
    nodes = raw.get("nodes", [])
    if not isinstance(nodes, list):
        raise ValueError("Heartbeat file must contain a nodes list.")

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
        heartbeat.idle_since.isoformat()
        if heartbeat.idle_since is not None
        else None
    )
    raw["capabilities"] = list(heartbeat.capabilities)
    raw["total_resources"] = heartbeat.total_resources.to_dict()
    raw["used_resources"] = heartbeat.used_resources.to_dict()
    raw["effective_resources"] = heartbeat.effective_resources.to_dict()
    raw["free_resources"] = heartbeat.free_resources.to_dict()
    raw["runtime_metrics"] = (
        heartbeat.runtime_metrics.to_dict()
        if heartbeat.runtime_metrics is not None
        else None
    )
    return raw


class HeartbeatStore:
    def __init__(self, path: Path) -> None:
        self.path = path

    def load(self) -> dict[str, NodeHeartbeat]:
        return load_heartbeats(self.path)

    def upsert(self, heartbeat: NodeHeartbeat) -> dict[str, NodeHeartbeat]:
        heartbeats = self.load()
        heartbeat = normalize_idle_since(
            heartbeat,
            previous=heartbeats.get(heartbeat.job_id),
        )
        heartbeats[heartbeat.job_id] = heartbeat
        self.save(heartbeats)
        return heartbeats

    def save(self, heartbeats: dict[str, NodeHeartbeat]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self.path.with_suffix(self.path.suffix + ".tmp")
        nodes = [
            heartbeat_to_dict(heartbeats[job_id])
            for job_id in sorted(heartbeats)
        ]
        tmp_path.write_text(
            json.dumps({"nodes": nodes}, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        tmp_path.replace(self.path)


def heartbeat_from_dict(raw: dict[str, Any]) -> NodeHeartbeat | None:
    node_id = raw.get("node_id")
    job_id = raw.get("job_id")
    updated_at = parse_iso_datetime(raw.get("updated_at"))
    if not isinstance(node_id, str) or not node_id:
        return None
    if not isinstance(job_id, str) or not job_id:
        return None
    if updated_at is None:
        return None
    labels = raw.get("labels")
    capabilities = raw.get("capabilities", ())
    if isinstance(capabilities, str):
        capability_items = tuple(
            item.strip() for item in capabilities.split(",") if item.strip()
        )
    elif isinstance(capabilities, list):
        capability_items = tuple(str(item) for item in capabilities if str(item))
    else:
        capability_items = ()
    return NodeHeartbeat(
        node_id=node_id,
        job_id=job_id,
        updated_at=updated_at,
        active_sandboxes=max(0, int(raw.get("active_sandboxes", 0))),
        active_image_builds=max(
            0,
            int(raw.get("active_image_builds", raw.get("activeImageBuilds", 0))),
        ),
        idle_since=parse_iso_datetime(raw.get("idle_since")),
        draining=bool(raw.get("draining", False)),
        node_url=string_or_none(raw.get("node_url")),
        agent_version=str(raw.get("agent_version") or ""),
        deployment_id=str(raw.get("deployment_id") or ""),
        init_version=str(raw.get("init_version") or ""),
        capabilities=tuple(dict.fromkeys(capability_items)),
        total_resources=ResourceQuantity.from_dict(raw.get("total_resources")),
        used_resources=ResourceQuantity.from_dict(raw.get("used_resources")),
        cpu_overcommit=float(raw.get("cpu_overcommit", 1.0)),
        memory_overcommit=float(raw.get("memory_overcommit", 1.0)),
        disk_overcommit=float(raw.get("disk_overcommit", 1.0)),
        labels={str(k): str(v) for k, v in dict(labels or {}).items()},
        runtime_metrics=NodeRuntimeMetrics.from_dict(raw.get("runtime_metrics")),
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
            idle_since=previous.idle_since or previous.updated_at,
        )
    return replace(heartbeat, idle_since=heartbeat.updated_at)


def string_or_none(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None


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
    version = heartbeat.agent_version if heartbeat is not None and heartbeat.agent_version else ""
    if not version:
        version = job.labels.get(AGENT_VERSION_LABEL, "")
    return agent_version_is_compatible(version)
