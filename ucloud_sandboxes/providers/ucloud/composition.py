from __future__ import annotations

from typing import Any, Callable

from ..base import ProviderConfiguration
from .adapter import UCloudCreateProfile, UCloudProvider
from .api import SessionStore, UCloudClient
from .config import UCloudSettings
from .payloads import (
    DEFAULT_BUILDER_DISK_GB,
    DEFAULT_BUILDER_PRODUCT_ID,
    DEFAULT_VM_APPLICATION_NAME,
    DEFAULT_VM_APPLICATION_VERSION,
    DEFAULT_VM_DISK_GB,
    DEFAULT_VM_PRODUCT_CATEGORY,
    DEFAULT_VM_PRODUCT_ID,
    DEFAULT_VM_PRODUCT_PROVIDER,
    VmApplicationRef,
    VmProductRef,
    VmTimeAllocation,
)


def provider_from_configuration(
    configuration: ProviderConfiguration,
    options: Any,
    *,
    client_factory: Callable[[SessionStore], UCloudClient] = UCloudClient,
) -> UCloudProvider:
    settings = UCloudSettings.from_provider(configuration)
    return UCloudProvider(
        settings.project_id,
        session_file=settings.session_file,
        client_factory=client_factory,
        sandbox_profile=_create_profile(options, settings, "sandbox"),
        builder_profile=_create_profile(options, settings, "builder"),
    )


def _create_profile(
    options: Any,
    settings: UCloudSettings,
    role: str,
) -> UCloudCreateProfile:
    no_private_network = bool(getattr(options, "no_private_network", False))
    private_network_id = (
        None
        if no_private_network
        else (
            getattr(options, "private_network_id", None) or settings.private_network_id
        )
    )
    ssh_requested = bool(getattr(options, "ssh", False))
    if ssh_requested and bool(getattr(options, "no_ssh", False)):
        raise ValueError("--ssh and --no-ssh cannot be used together.")
    return UCloudCreateProfile(
        private_network_id=private_network_id,
        require_private_network=not no_private_network,
        product=VmProductRef(
            id=(
                getattr(options, "builder_product_id", DEFAULT_BUILDER_PRODUCT_ID)
                if role == "builder"
                else getattr(options, "product_id", DEFAULT_VM_PRODUCT_ID)
            ),
            category=getattr(
                options,
                "product_category",
                DEFAULT_VM_PRODUCT_CATEGORY,
            ),
            provider=getattr(
                options,
                "product_provider",
                DEFAULT_VM_PRODUCT_PROVIDER,
            ),
        ),
        application=VmApplicationRef(
            name=getattr(options, "app_name", DEFAULT_VM_APPLICATION_NAME),
            version=getattr(options, "app_version", DEFAULT_VM_APPLICATION_VERSION),
        ),
        disk_gb=(
            getattr(options, "builder_disk_gb", DEFAULT_BUILDER_DISK_GB)
            if role == "builder"
            else getattr(options, "disk_gb", DEFAULT_VM_DISK_GB)
        ),
        time_allocation=VmTimeAllocation(
            hours=getattr(options, "time_hours", 1),
            minutes=getattr(options, "time_minutes", 0),
            seconds=getattr(options, "time_seconds", 0),
        ),
        ssh_enabled=ssh_requested,
        allow_duplicate_job=bool(getattr(options, "allow_duplicate_job", False)),
    )
