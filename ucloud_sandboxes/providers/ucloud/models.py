from __future__ import annotations

from datetime import datetime, timezone
import math
import re
from typing import Any

from ...models import InstancePhase, ProviderInstance
from .payloads import private_network_ids_from_resources


FINAL_JOB_STATES = {"SUCCESS", "FAILURE", "EXPIRED"}
CPU_PRODUCT_RE = re.compile(r"(?:^|[-_])(\d+)[-_]vcpu(?:$|[-_])", re.IGNORECASE)


def instance_from_payload(payload: dict[str, Any]) -> ProviderInstance:
    specification = payload.get("specification")
    if not isinstance(specification, dict):
        specification = {}
    status = payload.get("status")
    if not isinstance(status, dict):
        status = {}
    app = specification.get("application")
    if not isinstance(app, dict):
        app = {}
    product = specification.get("product")
    if not isinstance(product, dict):
        product = {}
    resolved_product = _nested_get(
        status, ("jobParametersJson", "request", "resolvedProduct")
    )
    if not isinstance(resolved_product, dict):
        resolved_product = {}
    machine_type = _nested_get(status, ("jobParametersJson", "machineType"))
    if not isinstance(machine_type, dict):
        machine_type = {}

    disk = _nested_get(specification, ("parameters", "diskSize", "value"))
    raw_labels = specification.get("labels")
    labels = raw_labels if isinstance(raw_labels, dict) else {}
    cpu_value = resolved_product.get("cpu", machine_type.get("cpu"))
    memory_value = resolved_product.get(
        "memoryInGigs", machine_type.get("memoryInGigs")
    )
    product_id = str(product.get("id") or "")
    cpu = int(cpu_value) if isinstance(cpu_value, (int, float)) else None
    if cpu is None:
        cpu = _cpu_count_from_product_id(product_id)

    updates = payload.get("updates")
    latest_update = updates[-1] if isinstance(updates, list) and updates else {}
    latest_note = (
        latest_update.get("status") if isinstance(latest_update, dict) else None
    )
    state = str(status.get("state") or "").strip().upper()
    started_at = _parse_millis(status.get("startedAt"))
    ssh_enabled = _nested_get(status, ("jobParametersJson", "request", "sshEnabled"))
    queue_status = _nested_get(
        status,
        ("jobParametersJson", "request", "resolvedSupport", "support", "queueStatus"),
    )

    return ProviderInstance(
        id=str(payload.get("id") or ""),
        name=str(specification.get("name") or ""),
        application_name=str(app.get("name") or ""),
        application_version=str(app.get("version") or ""),
        product_id=product_id,
        product_category=str(product.get("category") or ""),
        state=state,
        phase=_instance_phase(state, started_at=started_at, updates=updates),
        hostname=_string_value(specification.get("hostname")),
        created_at=_parse_millis(payload.get("createdAt")),
        started_at=started_at,
        expires_at=_parse_millis(status.get("expiresAt")),
        cpu=cpu,
        memory_gb=int(memory_value) if isinstance(memory_value, (int, float)) else None,
        disk_gb=int(disk) if isinstance(disk, (int, float)) else None,
        ssh_enabled=ssh_enabled if isinstance(ssh_enabled, bool) else None,
        private_network_ids=private_network_ids_from_resources(
            specification.get("resources")
        ),
        queue_status=queue_status if isinstance(queue_status, str) else None,
        latest_note=latest_note if isinstance(latest_note, str) else None,
        labels={str(key): str(value) for key, value in labels.items()},
        raw=payload,
    )


def _instance_phase(
    state: str,
    *,
    started_at: datetime | None,
    updates: object,
) -> InstancePhase:
    if _has_post_start_suspension(updates):
        return InstancePhase.LOST
    if state in FINAL_JOB_STATES:
        return InstancePhase.TERMINAL
    if state == "SUSPENDED":
        return InstancePhase.PROVISIONING if started_at is None else InstancePhase.LOST
    if state == "RUNNING":
        return InstancePhase.RUNNING
    return InstancePhase.PROVISIONING


def _has_post_start_suspension(updates: object) -> bool:
    seen_running = False
    if not isinstance(updates, list):
        return False
    for update in updates:
        if not isinstance(update, dict):
            continue
        state = str(update.get("state") or "").strip().upper()
        if state == "RUNNING":
            seen_running = True
        elif state == "SUSPENDED" and seen_running:
            return True
    return False


def _parse_millis(value: object) -> datetime | None:
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


def _nested_get(payload: object, path: tuple[str, ...]) -> object | None:
    current = payload
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _string_value(value: object) -> str | None:
    if isinstance(value, str) and value:
        return value
    if isinstance(value, dict):
        for key in ("value", "id", "name"):
            candidate = value.get(key)
            if isinstance(candidate, str) and candidate:
                return candidate
    return None


def _cpu_count_from_product_id(product_id: str) -> int | None:
    match = CPU_PRODUCT_RE.search(product_id)
    if not match:
        return None
    cpu = int(match.group(1))
    return cpu if cpu > 0 else None
