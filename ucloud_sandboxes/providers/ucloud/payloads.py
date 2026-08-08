"""Render provider-neutral node intent as an SDU UCloud job payload."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Sequence

from ...networking import validate_hostname


DEFAULT_PUBLIC_LINK_PORT = 8090


@dataclass(frozen=True)
class PrivateNetworkAttachment:
    network_id: str
    hostname: str

    def __post_init__(self) -> None:
        _validate_resource_id("private network", self.network_id)
        validate_hostname(self.hostname)

    def to_resource(self) -> dict[str, str]:
        return {"type": "private_network", "id": self.network_id}


@dataclass(frozen=True)
class PublicLinkAttachment:
    link_id: str
    port: int = DEFAULT_PUBLIC_LINK_PORT

    def __post_init__(self) -> None:
        _validate_resource_id("public link", self.link_id)
        _validate_port(self.port)

    def to_resource(self) -> dict[str, Any]:
        return {"type": "ingress", "id": self.link_id, "port": int(self.port)}


def apply_private_network_attachment(
    job_item: dict[str, Any],
    attachment: PrivateNetworkAttachment,
) -> dict[str, Any]:
    updated, resources = _copy_resources(job_item)
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
    updated, resources = _copy_resources(job_item)
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
    return _resource_ids(raw_resources, "private_network")


def public_link_ids_from_resources(raw_resources: object) -> tuple[str, ...]:
    return _resource_ids(raw_resources, "ingress")


def _copy_resources(job_item: dict[str, Any]) -> tuple[dict[str, Any], list[Any]]:
    updated = deepcopy(job_item)
    raw_resources = updated.get("resources")
    if raw_resources is None:
        return updated, []
    if not isinstance(raw_resources, list):
        raise ValueError("job resources must be a list when present.")
    return updated, deepcopy(raw_resources)


def _resource_ids(raw_resources: object, resource_type: str) -> tuple[str, ...]:
    if not isinstance(raw_resources, list):
        return ()
    values: list[str] = []
    for resource in raw_resources:
        if not isinstance(resource, dict) or resource.get("type") != resource_type:
            continue
        value = resource.get("id")
        if isinstance(value, str) and value and value not in values:
            values.append(value)
    return tuple(values)


def _validate_resource_id(label: str, value: str) -> None:
    if not value:
        raise ValueError(f"{label} id is required.")
    if "\n" in value or "\r" in value:
        raise ValueError(f"{label} id cannot contain newlines.")


def _validate_port(value: int) -> None:
    if int(value) < 1 or int(value) > 65535:
        raise ValueError("port must be in [1, 65535].")


def _same_resource(left: object, right: dict[str, Any]) -> bool:
    return (
        isinstance(left, dict)
        and left.get("type") == right["type"]
        and left.get("id") == right["id"]
    )


DEFAULT_VM_APPLICATION_NAME = "vm-ubuntu"
DEFAULT_VM_APPLICATION_VERSION = "24.04"
# Sandbox nodes are deliberately large; the always-on gateway is control-plane
# infrastructure and should not inherit the worker-node product.
DEFAULT_VM_PRODUCT_ID = "cpu-amd-zen5-32-vcpu"
DEFAULT_BUILDER_PRODUCT_ID = "cpu-amd-zen5-16-vcpu"
DEFAULT_GATEWAY_VM_PRODUCT_ID = "cpu-amd-zen5-2-vcpu"
DEFAULT_VM_PRODUCT_CATEGORY = "cpu-amd-zen5"
DEFAULT_VM_PRODUCT_PROVIDER = "ucloud"
DEFAULT_VM_DISK_GB = 250
DEFAULT_BUILDER_DISK_GB = 250


@dataclass(frozen=True)
class VmProductRef:
    id: str = DEFAULT_VM_PRODUCT_ID
    category: str = DEFAULT_VM_PRODUCT_CATEGORY
    provider: str = DEFAULT_VM_PRODUCT_PROVIDER

    def to_dict(self) -> dict[str, str]:
        validate_required("product id", self.id)
        validate_required("product category", self.category)
        validate_required("product provider", self.provider)
        return {
            "id": self.id,
            "category": self.category,
            "provider": self.provider,
        }


@dataclass(frozen=True)
class VmApplicationRef:
    name: str = DEFAULT_VM_APPLICATION_NAME
    version: str = DEFAULT_VM_APPLICATION_VERSION

    def to_dict(self) -> dict[str, str]:
        validate_required("application name", self.name)
        validate_required("application version", self.version)
        return {
            "name": self.name,
            "version": self.version,
        }


@dataclass(frozen=True)
class VmTimeAllocation:
    hours: int = 1
    minutes: int = 0
    seconds: int = 0

    def to_dict(self) -> dict[str, int]:
        if self.hours < 0 or self.minutes < 0 or self.seconds < 0:
            raise ValueError("time allocation cannot be negative.")
        if self.minutes >= 60 or self.seconds >= 60:
            raise ValueError("time allocation minutes/seconds must be in [0, 59].")
        return {
            "hours": self.hours,
            "minutes": self.minutes,
            "seconds": self.seconds,
        }


@dataclass(frozen=True)
class VmFileMount:
    path: str
    read_only: bool = False

    def to_resource(self) -> dict[str, Any]:
        validate_required("file mount path", self.path)
        if not self.path.startswith("/"):
            raise ValueError("file mount path must be an absolute UCloud path.")
        return file_mount_resource(self.path, read_only=self.read_only)


@dataclass(frozen=True)
class VmSubmissionOptions:
    name: str
    hostname: str
    private_network_id: str | None
    public_link_id: str | None = None
    public_link_port: int = DEFAULT_PUBLIC_LINK_PORT
    product: VmProductRef = VmProductRef()
    application: VmApplicationRef = VmApplicationRef()
    disk_gb: int = DEFAULT_VM_DISK_GB
    replicas: int = 1
    time_allocation: VmTimeAllocation = VmTimeAllocation()
    ssh_enabled: bool = False
    allow_duplicate_job: bool = False
    labels: dict[str, str] | None = None
    file_mounts: tuple[VmFileMount, ...] = ()

    def job_item(self) -> dict[str, Any]:
        validate_vm_submission_options(self)
        item: dict[str, Any] = {
            "name": self.name,
            "application": self.application.to_dict(),
            "product": self.product.to_dict(),
            "replicas": self.replicas,
            "allowDuplicateJob": self.allow_duplicate_job,
            "sshEnabled": self.ssh_enabled,
            "hostname": self.hostname,
            "parameters": {
                "diskSize": disk_size_parameter(self.disk_gb),
            },
            "resources": [],
            "timeAllocation": self.time_allocation.to_dict(),
        }
        if self.labels:
            item["labels"] = dict(sorted(self.labels.items()))
        if self.private_network_id:
            item = apply_private_network_attachment(
                item,
                PrivateNetworkAttachment(
                    network_id=self.private_network_id,
                    hostname=self.hostname,
                ),
            )
        if self.public_link_id:
            item = apply_public_link_attachment(
                item,
                PublicLinkAttachment(
                    link_id=self.public_link_id,
                    port=self.public_link_port,
                ),
            )
        item["resources"].extend(mount.to_resource() for mount in self.file_mounts)
        return item

    def bulk_payload(self) -> dict[str, Any]:
        return bulk_submission_payload([self])


def bulk_submission_payload(options: Sequence[VmSubmissionOptions]) -> dict[str, Any]:
    return {
        "type": "bulk",
        "items": [item.job_item() for item in options],
    }


def disk_size_parameter(disk_gb: int) -> dict[str, Any]:
    if disk_gb <= 0:
        raise ValueError("disk size must be positive.")
    return {
        "type": "integer",
        "path": "",
        "mountPath": "",
        "readOnly": False,
        "value": disk_gb,
        "hostname": "",
        "jobId": "",
        "id": "",
        "specification": {
            "applicationName": "",
            "language": "",
            "init": None,
            "job": None,
            "readme": None,
            "inputs": None,
        },
        "modules": None,
        "port": 0,
    }


def file_mount_resource(path: str, *, read_only: bool = False) -> dict[str, Any]:
    validate_required("file mount path", path)
    if not path.startswith("/"):
        raise ValueError("file mount path must be an absolute UCloud path.")
    return {
        "type": "file",
        "path": path,
        "mountPath": "",
        "readOnly": bool(read_only),
        "value": None,
        "hostname": "",
        "jobId": "",
        "id": "",
        "specification": {
            "applicationName": "",
            "language": "",
            "init": None,
            "job": None,
            "readme": None,
            "inputs": None,
        },
        "modules": None,
        "port": 0,
    }


def validate_vm_submission_options(options: VmSubmissionOptions) -> None:
    validate_required("job name", options.name)
    validate_hostname(options.hostname)
    if options.replicas < 1:
        raise ValueError("replicas must be positive.")
    if options.disk_gb <= 0:
        raise ValueError("disk size must be positive.")
    seen_mounts: set[str] = set()
    for mount in options.file_mounts:
        if mount.path in seen_mounts:
            raise ValueError(f"duplicate file mount path: {mount.path}")
        seen_mounts.add(mount.path)
        mount.to_resource()
    for key, value in (options.labels or {}).items():
        validate_required("label key", key)
        reject_newline("label key", key)
        reject_newline("label value", value)
        if "=" in key:
            raise ValueError("label keys cannot contain '='.")


def validate_required(name: str, value: str) -> None:
    if not value:
        raise ValueError(f"{name} is required.")
    reject_newline(name, value)


def reject_newline(name: str, value: str) -> None:
    if "\n" in value or "\r" in value:
        raise ValueError(f"{name} cannot contain newlines.")
