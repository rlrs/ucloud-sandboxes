from __future__ import annotations

from typing import Callable

from ..base import ProviderConfiguration
from .adapter import HetznerCreateProfile, HetznerProvider
from .api import HetznerClient
from .config import HetznerSettings


def provider_from_configuration(
    configuration: ProviderConfiguration,
    *,
    client_factory: Callable[..., HetznerClient] = HetznerClient,
) -> HetznerProvider:
    settings = HetznerSettings.from_provider(configuration)
    common = {
        "location": settings.location,
        "network_id": settings.network_id,
        "ssh_key_ids": settings.ssh_key_ids,
        "firewall_ids": settings.firewall_ids,
        "placement_group_id": settings.placement_group_id,
        "enable_ipv4": settings.enable_ipv4,
        "enable_ipv6": settings.enable_ipv6,
        "enable_private_egress": settings.enable_private_egress,
        "private_dns_servers": settings.private_dns_servers,
    }
    return HetznerProvider(
        settings.project_name,
        api_token_env=settings.api_token_env,
        api_base_url=settings.api_base_url,
        ssh_user=settings.ssh_user,
        sandbox_profile=HetznerCreateProfile(
            server_type=settings.sandbox_server_type,
            image=settings.sandbox_image,
            **common,
        ),
        builder_profile=HetznerCreateProfile(
            server_type=settings.builder_server_type,
            image=settings.builder_image,
            **common,
        ),
        client_factory=client_factory,
    )
