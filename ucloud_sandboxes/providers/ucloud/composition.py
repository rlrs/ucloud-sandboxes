from __future__ import annotations

from typing import Callable

from ..base import ProviderConfiguration
from .adapter import UCloudCreateProfile, UCloudProvider
from .api import SessionStore, UCloudClient
from .config import UCloudSettings
from .payloads import (
    DEFAULT_VM_APPLICATION_NAME,
    DEFAULT_VM_APPLICATION_VERSION,
    DEFAULT_VM_PRODUCT_CATEGORY,
    DEFAULT_VM_PRODUCT_PROVIDER,
    VmApplicationRef,
    VmProductRef,
    VmTimeAllocation,
)


def provider_from_configuration(
    configuration: ProviderConfiguration,
    *,
    session_file: str,
    sandbox_product_id: str,
    sandbox_disk_gb: int,
    builder_product_id: str,
    builder_disk_gb: int,
    client_factory: Callable[[SessionStore], UCloudClient] = UCloudClient,
) -> UCloudProvider:
    settings = UCloudSettings.from_provider(configuration)
    return UCloudProvider(
        settings.project_id,
        session_file=session_file,
        client_factory=client_factory,
        sandbox_profile=_create_profile(
            settings,
            product_id=sandbox_product_id,
            disk_gb=sandbox_disk_gb,
        ),
        builder_profile=_create_profile(
            settings,
            product_id=builder_product_id,
            disk_gb=builder_disk_gb,
        ),
    )


def _create_profile(
    settings: UCloudSettings,
    *,
    product_id: str,
    disk_gb: int,
) -> UCloudCreateProfile:
    return UCloudCreateProfile(
        private_network_id=settings.private_network_id,
        require_private_network=True,
        product=VmProductRef(
            id=product_id,
            category=DEFAULT_VM_PRODUCT_CATEGORY,
            provider=DEFAULT_VM_PRODUCT_PROVIDER,
        ),
        application=VmApplicationRef(
            name=DEFAULT_VM_APPLICATION_NAME,
            version=DEFAULT_VM_APPLICATION_VERSION,
        ),
        disk_gb=disk_gb,
        time_allocation=VmTimeAllocation(hours=1),
        ssh_enabled=False,
        allow_duplicate_job=False,
    )
