from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
import math
import os
from typing import Mapping
from urllib import error, request

from .deployment import DEFAULT_INIT_VERSION, package_version
from .models import (
    NodeHeartbeat,
    NodeRuntimeMetrics,
    ResourceQuantity,
    SandboxInventoryEntry,
    utc_now,
)


JOB_ID_ENV_KEYS = ("UCLOUD_JOB_ID", "UCLOUD_JOBID", "JOB_ID")


class _RejectNodeRedirects(request.HTTPRedirectHandler):
    def redirect_request(self, *_args: object, **_kwargs: object) -> None:
        return None


@dataclass(frozen=True)
class HeartbeatPostResult:
    status: int
    payload: object


def detect_job_id(env: Mapping[str, str] | None = None) -> str | None:
    source = env if env is not None else os.environ
    for key in JOB_ID_ENV_KEYS:
        value = source.get(key, "").strip()
        if value:
            return value
    return None


def default_node_id(job_id: str, env: Mapping[str, str] | None = None) -> str:
    source = env if env is not None else os.environ
    hostname = source.get("HOSTNAME", "").strip()
    if hostname:
        return hostname
    return f"ucloud-job-{job_id}"


def build_heartbeat(
    *,
    job_id: str,
    node_id: str | None = None,
    active_sandboxes: int = 0,
    active_image_builds: int = 0,
    draining: bool = False,
    node_url: str | None = None,
    agent_version: str | None = None,
    deployment_id: str = "",
    init_version: str = DEFAULT_INIT_VERSION,
    capabilities: tuple[str, ...] = (),
    total_resources: ResourceQuantity | None = None,
    used_resources: ResourceQuantity | None = None,
    cpu_overcommit: float = 1.0,
    memory_overcommit: float = 1.0,
    disk_overcommit: float = 1.0,
    labels: dict[str, str] | None = None,
    cached_images: tuple[str, ...] | None = None,
    runtime_metrics: NodeRuntimeMetrics | None = None,
    node_epoch: str = "",
    activity_epoch: int = 0,
    inventory: tuple[SandboxInventoryEntry, ...] = (),
    inventory_complete: bool = False,
    reserved_resources: ResourceQuantity | None = None,
    build_reserved_resources: ResourceQuantity | None = None,
    physical_disk_total_mb: int = 0,
    physical_disk_free_mb: int = 0,
    drain_token: str = "",
    drain_activity_epoch: int = 0,
    admission_open: bool = True,
    now: datetime | None = None,
) -> NodeHeartbeat:
    cleaned_job_id = job_id.strip()
    if not cleaned_job_id:
        raise ValueError("job_id is required.")
    if active_sandboxes < 0:
        raise ValueError("active sandbox count cannot be negative.")
    if active_image_builds < 0:
        raise ValueError("active image build count cannot be negative.")
    if activity_epoch < 0:
        raise ValueError("activity epoch cannot be negative.")
    if drain_activity_epoch < 0:
        raise ValueError("drain activity epoch cannot be negative.")
    if physical_disk_total_mb < 0 or physical_disk_free_mb < 0:
        raise ValueError("physical disk values cannot be negative.")
    if physical_disk_free_mb > physical_disk_total_mb and physical_disk_total_mb:
        raise ValueError("physical disk free space cannot exceed its total space.")
    resource_fields = {
        "total_resources": total_resources,
        "used_resources": used_resources,
        "reserved_resources": reserved_resources,
        "build_reserved_resources": build_reserved_resources,
    }
    for field_name, quantity in resource_fields.items():
        if quantity is not None and not quantity.is_valid:
            raise ValueError(f"{field_name} cannot contain negative or non-finite values.")
    overcommit_fields = {
        "cpu_overcommit": cpu_overcommit,
        "memory_overcommit": memory_overcommit,
        "disk_overcommit": disk_overcommit,
    }
    for field_name, factor in overcommit_fields.items():
        if not math.isfinite(factor) or factor < 0:
            raise ValueError(f"{field_name} must be finite and non-negative.")
    cleaned_node_url = node_url.strip() if node_url else None
    if cleaned_node_url and ("\n" in cleaned_node_url or "\r" in cleaned_node_url):
        raise ValueError("node_url cannot contain newlines.")
    reported_at = now or utc_now()
    return NodeHeartbeat(
        node_id=(node_id or default_node_id(cleaned_job_id)).strip(),
        job_id=cleaned_job_id,
        updated_at=reported_at,
        active_sandboxes=active_sandboxes,
        active_image_builds=active_image_builds,
        draining=draining,
        node_url=cleaned_node_url,
        agent_version=(agent_version or package_version()).strip(),
        deployment_id=deployment_id.strip(),
        init_version=init_version.strip(),
        capabilities=tuple(dict.fromkeys(capabilities)),
        total_resources=total_resources or ResourceQuantity(),
        used_resources=used_resources or ResourceQuantity(),
        cpu_overcommit=cpu_overcommit,
        memory_overcommit=memory_overcommit,
        disk_overcommit=disk_overcommit,
        labels=labels or {},
        cached_images=tuple(dict.fromkeys(cached_images or ())),
        cached_images_known=cached_images is not None,
        runtime_metrics=runtime_metrics,
        reported_at=reported_at,
        node_epoch=node_epoch.strip(),
        activity_epoch=activity_epoch,
        inventory=inventory,
        inventory_complete=inventory_complete,
        reserved_resources=reserved_resources or ResourceQuantity(),
        build_reserved_resources=build_reserved_resources or ResourceQuantity(),
        physical_disk_total_mb=physical_disk_total_mb,
        physical_disk_free_mb=physical_disk_free_mb,
        drain_token=drain_token.strip(),
        drain_activity_epoch=drain_activity_epoch,
        admission_open=bool(admission_open),
    )


def post_heartbeat(url: str, heartbeat: NodeHeartbeat) -> HeartbeatPostResult:
    from .registry import heartbeat_to_dict

    body = json.dumps(heartbeat_to_dict(heartbeat)).encode("utf-8")
    req = request.Request(
        url,
        data=body,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    return _post_heartbeat_request(req)


def post_heartbeat_with_headers(
    url: str,
    heartbeat: NodeHeartbeat,
    headers: Mapping[str, str],
) -> HeartbeatPostResult:
    from .registry import heartbeat_to_dict

    request_headers = {"Content-Type": "application/json", **dict(headers)}
    body = json.dumps(heartbeat_to_dict(heartbeat)).encode("utf-8")
    req = request.Request(
        url,
        data=body,
        method="POST",
        headers=request_headers,
    )
    return _post_heartbeat_request(req)


def _post_heartbeat_request(req: request.Request) -> HeartbeatPostResult:
    try:
        with request.urlopen(req, timeout=10.0) as response:
            raw = response.read().decode("utf-8")
            return HeartbeatPostResult(response.status, _decode_json(raw))
    except error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        return HeartbeatPostResult(exc.code, _decode_json(raw))
    except error.URLError as exc:
        raise RuntimeError(f"Could not post heartbeat: {exc}") from exc


def fetch_node_agent_heartbeat(
    node_agent_url: str,
    *,
    bearer_token: str | None = None,
) -> NodeHeartbeat:
    from .registry import heartbeat_from_dict

    url = node_agent_url.rstrip("/") + "/v1/heartbeat"
    if bearer_token is not None and not bearer_token.strip():
        raise ValueError("node control bearer token cannot be empty")
    headers = (
        {"Authorization": f"Bearer {bearer_token}"}
        if bearer_token is not None
        else {}
    )
    req = request.Request(url, method="GET", headers=headers)
    try:
        with request.build_opener(_RejectNodeRedirects()).open(
            req,
            timeout=10.0,
        ) as response:
            payload = _decode_json(response.read().decode("utf-8"))
    except error.URLError as exc:
        raise RuntimeError(f"Could not fetch node-agent heartbeat: {exc}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError("Node-agent heartbeat response must be a JSON object.")
    heartbeat_raw = payload.get("heartbeat")
    if not isinstance(heartbeat_raw, dict):
        raise RuntimeError("Node-agent heartbeat response is missing heartbeat object.")
    heartbeat = heartbeat_from_dict(heartbeat_raw)
    if heartbeat is None:
        raise RuntimeError("Node-agent heartbeat response is invalid.")
    return heartbeat


def _decode_json(raw: str) -> object:
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return raw
