from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
import re
from typing import Any


HOSTNAME_MAX_LENGTH = 63
HOSTNAME_RE = re.compile(r"[^a-z0-9-]+")
DEFAULT_PUBLIC_LINK_PORT = 8090


@dataclass(frozen=True)
class PrivateNetworkAttachment:
    network_id: str
    hostname: str

    def __post_init__(self) -> None:
        validate_private_network_id(self.network_id)
        validate_hostname(self.hostname)

    def to_resource(self) -> dict[str, str]:
        return private_network_resource(self.network_id)


@dataclass(frozen=True)
class PublicLinkAttachment:
    link_id: str
    port: int = DEFAULT_PUBLIC_LINK_PORT

    def __post_init__(self) -> None:
        validate_public_link_id(self.link_id)
        validate_port(self.port)

    def to_resource(self) -> dict[str, Any]:
        return public_link_resource(self.link_id, self.port)


def private_network_resource(network_id: str) -> dict[str, str]:
    validate_private_network_id(network_id)
    return {"type": "private_network", "id": network_id}


def public_link_resource(
    link_id: str,
    port: int = DEFAULT_PUBLIC_LINK_PORT,
) -> dict[str, Any]:
    validate_public_link_id(link_id)
    validate_port(port)
    return {"type": "ingress", "id": link_id, "port": int(port)}


def apply_private_network_attachment(
    job_item: dict[str, Any],
    attachment: PrivateNetworkAttachment,
) -> dict[str, Any]:
    """Return a job submission item with private network resource and hostname.

    UCloud represents private network membership as a job resource
    `{"type": "private_network", "id": ...}`. The per-job name inside that
    network is the top-level job `hostname` field.
    """

    updated = deepcopy(job_item)
    raw_resources = updated.get("resources")
    if raw_resources is None:
        resources: list[Any] = []
    elif isinstance(raw_resources, list):
        resources = deepcopy(raw_resources)
    else:
        raise ValueError("job resources must be a list when present.")

    resource = attachment.to_resource()
    if not any(_same_resource(existing, resource) for existing in resources):
        resources.append(resource)
    updated["resources"] = resources
    updated["hostname"] = attachment.hostname
    return updated


def apply_public_link_attachment(
    job_item: dict[str, Any],
    attachment: PublicLinkAttachment,
) -> dict[str, Any]:
    """Return a job submission item with a public-link ingress resource.

    UCloud represents public links as ingress resources. For VM jobs, the live
    product support advertises `jobs.vm.bindLinkToPort`; the `port` field is the
    VM-local service port that should be exposed by the public link.
    """

    updated = deepcopy(job_item)
    raw_resources = updated.get("resources")
    if raw_resources is None:
        resources: list[Any] = []
    elif isinstance(raw_resources, list):
        resources = deepcopy(raw_resources)
    else:
        raise ValueError("job resources must be a list when present.")

    resource = attachment.to_resource()
    for index, existing in enumerate(resources):
        if _same_resource(existing, resource):
            resources[index] = resource
            break
    else:
        resources.append(resource)
    updated["resources"] = resources
    return updated


def private_network_ids_from_resources(raw_resources: object) -> tuple[str, ...]:
    if not isinstance(raw_resources, list):
        return ()
    ids: list[str] = []
    seen: set[str] = set()
    for resource in raw_resources:
        if not isinstance(resource, dict):
            continue
        if resource.get("type") != "private_network":
            continue
        network_id = resource.get("id")
        if not isinstance(network_id, str) or not network_id:
            continue
        if network_id not in seen:
            ids.append(network_id)
            seen.add(network_id)
    return tuple(ids)


def public_link_ids_from_resources(raw_resources: object) -> tuple[str, ...]:
    if not isinstance(raw_resources, list):
        return ()
    ids: list[str] = []
    seen: set[str] = set()
    for resource in raw_resources:
        if not isinstance(resource, dict):
            continue
        if resource.get("type") != "ingress":
            continue
        link_id = resource.get("id")
        if not isinstance(link_id, str) or not link_id:
            continue
        if link_id not in seen:
            ids.append(link_id)
            seen.add(link_id)
    return tuple(ids)


def stable_hostname(seed: str, *, prefix: str = "sandbox-node") -> str:
    base = seed.strip().lower()
    if prefix:
        base = f"{prefix}-{base}"
    hostname = HOSTNAME_RE.sub("-", base)
    hostname = re.sub(r"-+", "-", hostname).strip("-")
    if not hostname:
        hostname = prefix or "sandbox-node"
    hostname = hostname[:HOSTNAME_MAX_LENGTH].strip("-")
    if not hostname:
        hostname = "sandbox-node"
    return hostname


def validate_hostname(value: str) -> None:
    if not value:
        raise ValueError("hostname is required for private network attachment.")
    if len(value) > HOSTNAME_MAX_LENGTH:
        raise ValueError(f"hostname must be at most {HOSTNAME_MAX_LENGTH} characters.")
    if value.startswith("-") or value.endswith("-"):
        raise ValueError("hostname cannot start or end with '-'.")
    if HOSTNAME_RE.search(value) or value.lower() != value:
        raise ValueError("hostname must contain only lowercase letters, digits, and '-'.")


def validate_private_network_id(value: str) -> None:
    if not value:
        raise ValueError("private network id is required.")
    if "\n" in value or "\r" in value:
        raise ValueError("private network id cannot contain newlines.")


def validate_public_link_id(value: str) -> None:
    if not value:
        raise ValueError("public link id is required.")
    if "\n" in value or "\r" in value:
        raise ValueError("public link id cannot contain newlines.")


def validate_port(value: int) -> None:
    if int(value) < 1 or int(value) > 65535:
        raise ValueError("port must be in [1, 65535].")


def _same_resource(left: object, right: dict[str, Any]) -> bool:
    return (
        isinstance(left, dict)
        and left.get("type") == right["type"]
        and left.get("id") == right["id"]
    )
