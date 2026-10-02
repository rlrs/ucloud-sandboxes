"""Pure gateway path, query and payload parsers."""

from __future__ import annotations

from dataclasses import replace
import math
from typing import Any
from urllib.parse import parse_qs, unquote

from ..hibernation import hibernation_disk_reservation_mb
from ..models import ResourceQuantity


def _collection_id_from_path(path: str, prefix: str) -> str | None:
    if not path.startswith(prefix):
        return None
    rest = path[len(prefix) :]
    if not rest:
        return None
    return unquote(rest.split("/", 1)[0])


def _sandbox_id_from_path(path: str) -> str | None:
    return _collection_id_from_path(path, "/v1/sandboxes/")


def _sandbox_migration_id_from_path(path: str) -> str | None:
    prefix = "/v1/sandboxes/"
    suffix = "/migration"
    if not path.startswith(prefix) or not path.endswith(suffix):
        return None
    encoded = path[len(prefix) : -len(suffix)]
    sandbox_id = unquote(encoded)
    if not sandbox_id or "/" in sandbox_id:
        return None
    return sandbox_id


def _sandbox_detach_id_from_path(path: str) -> str | None:
    prefix = "/v1/sandboxes/"
    suffix = "/detach"
    if not path.startswith(prefix) or not path.endswith(suffix):
        return None
    encoded = path[len(prefix) : -len(suffix)]
    sandbox_id = unquote(encoded)
    if not sandbox_id or "/" in sandbox_id:
        return None
    return sandbox_id


def _image_build_key_from_path(path: str) -> str | None:
    return _collection_id_from_path(path, "/v1/images/builds/")


def _exec_session_id_from_path(path: str) -> str | None:
    return _collection_id_from_path(path, "/v1/exec/")


def _prepare_id_from_path(path: str) -> str | None:
    return _collection_id_from_path(path, "/v1/capacity/prepare/")


def _builder_prepare_id_from_path(path: str) -> str | None:
    return _collection_id_from_path(path, "/v1/builders/prepare/")


def _truthy_query_param(parsed: Any, name: str) -> bool:
    values = parse_qs(str(getattr(parsed, "query", ""))).get(name, [])
    return any(
        str(value).lower() in {"1", "true", "yes", "on", "full"} for value in values
    )


def _prepared_resources_from_payload(raw: dict[str, Any]) -> ResourceQuantity:
    resources = {
        "vcpu": raw.get("cpus", 0),
        "memory_mb": raw.get("memory_mb", 0),
        "disk_mb": raw.get("disk_mb", 0),
    }
    vcpu = resources["vcpu"]
    if (
        isinstance(vcpu, bool)
        or not isinstance(vcpu, (int, float))
        or not math.isfinite(float(vcpu))
        or vcpu < 0
    ):
        raise ValueError("cpus must be non-negative and finite.")
    for label in ("memory_mb", "disk_mb"):
        value = resources[label]
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError(f"{label} must be a non-negative integer.")
    prepared = ResourceQuantity.from_dict(resources)
    parkable = raw.get("parkable", False)
    if not isinstance(parkable, bool):
        raise ValueError("parkable must be a boolean.")
    if not parkable:
        return prepared
    if prepared.memory_mb <= 0:
        raise ValueError("parkable prepared capacity requires memory_mb.")
    if prepared.disk_mb <= 0:
        raise ValueError("parkable prepared capacity requires disk_mb.")
    return replace(
        prepared,
        disk_mb=hibernation_disk_reservation_mb(
            memory_mb=prepared.memory_mb,
            writable_disk_mb=prepared.disk_mb,
        ),
    )


def _strict_positive_integer(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{label} must be a positive integer.")
    return value


def _validate_prepared_resources(resources: ResourceQuantity) -> None:
    if resources.vcpu < 0:
        raise ValueError("vcpu must be non-negative.")
    if resources.memory_mb < 0:
        raise ValueError("memory_mb must be non-negative.")
    if resources.disk_mb < 0:
        raise ValueError("disk_mb must be non-negative.")
    if resources == ResourceQuantity():
        raise ValueError("prepared capacity resources are required.")
