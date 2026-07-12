from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence

from .networking import (
    DEFAULT_PUBLIC_LINK_PORT,
    PrivateNetworkAttachment,
    PublicLinkAttachment,
    apply_private_network_attachment,
    apply_public_link_attachment,
    validate_hostname,
)


DEFAULT_VM_APPLICATION_NAME = "vm-ubuntu"
DEFAULT_VM_APPLICATION_VERSION = "24.04"
DEFAULT_VM_PRODUCT_ID = "cpu-amd-zen5-32-vcpu"
DEFAULT_VM_PRODUCT_CATEGORY = "cpu-amd-zen5"
DEFAULT_VM_PRODUCT_PROVIDER = "ucloud"
DEFAULT_VM_DISK_GB = 250


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
