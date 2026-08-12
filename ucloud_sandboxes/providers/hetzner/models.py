from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from ...models import InstancePhase, ProviderInstance


def instance_from_payload(payload: dict[str, Any]) -> ProviderInstance:
    server_type = _object(payload.get("server_type"))
    image = _object(payload.get("image"))
    raw_labels = payload.get("labels")
    labels = raw_labels if isinstance(raw_labels, dict) else {}
    state = str(payload.get("status") or "unknown").strip().lower()
    private_network_ids = tuple(
        str(item["network"])
        for item in _objects(payload.get("private_net"))
        if _positive_number(item.get("network")) is not None
    )

    image_name = _optional_string(image.get("name"))
    image_description = _optional_string(image.get("description"))
    image_id = _positive_number(image.get("id"))
    application_name = (
        image_name
        or image_description
        or (str(image_id) if image_id is not None else "")
    )

    primary_disk_size_gb = _positive_number(payload.get("primary_disk_size"))
    if primary_disk_size_gb is None:
        primary_disk_size_gb = _positive_number(server_type.get("disk"))

    return ProviderInstance(
        id=_identifier(payload.get("id")),
        name=str(payload.get("name") or ""),
        application_name=application_name,
        application_version=str(image.get("os_version") or ""),
        product_id=str(server_type.get("name") or ""),
        product_category=str(server_type.get("category") or ""),
        state=state,
        phase=instance_phase(state),
        hostname=_optional_string(payload.get("name")),
        created_at=_parse_datetime(payload.get("created")),
        cpu=_positive_number(server_type.get("cores")),
        memory_gb=_positive_number(server_type.get("memory")),
        # Hetzner reports disk sizes in decimal GB. Core resource accounting
        # uses binary GiB (and later converts it to MiB), so retaining the raw
        # number would over-advertise a CPX62's local disk by roughly 44 GiB.
        disk_gb=_decimal_gb_to_binary_gib(primary_disk_size_gb),
        private_network_ids=private_network_ids,
        labels={str(key): str(value) for key, value in labels.items()},
        raw=payload,
    )


def instance_phase(state: str) -> InstancePhase:
    normalized = state.strip().lower()
    if normalized == "running":
        return InstancePhase.RUNNING
    if normalized == "deleting":
        return InstancePhase.TERMINAL
    if normalized in {"off", "stopping"}:
        return InstancePhase.LOST
    return InstancePhase.PROVISIONING


def _object(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _objects(value: object) -> tuple[dict[str, Any], ...]:
    if not isinstance(value, list):
        return ()
    return tuple(item for item in value if isinstance(item, dict))


def _identifier(value: object) -> str:
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        return ""
    return str(value)


def _positive_number(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
        return None
    return int(value)


def _decimal_gb_to_binary_gib(value: int | None) -> int | None:
    if value is None:
        return None
    return max(1, value * 1_000_000_000 // (1024**3))


def _optional_string(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def _parse_datetime(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)
