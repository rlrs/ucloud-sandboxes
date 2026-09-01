"""Cloud-provider boundary for autoscaled compute instances."""

from dataclasses import replace

from .base import (
    ComputeProvider,
    DestructiveInstanceLoss,
    InstanceBootstrapAccess,
    InstanceCreateIntent,
    ProviderConfiguration,
    ProviderError,
    ProviderMutationResult,
)
from .loader import PROVIDER_ENTRY_POINT_GROUP, load_external_provider


def default_provider_configuration(scope_id: str = "") -> ProviderConfiguration:
    """Return the product's built-in provider default without leaking it to core."""

    from .ucloud.config import UCloudSettings

    return replace(UCloudSettings.default(), project_id=scope_id).to_provider()


def validate_provider_configuration(
    configuration: ProviderConfiguration,
) -> None:
    """Validate built-in provider settings; external factories validate theirs."""

    if configuration.kind == "ucloud":
        from .ucloud.config import UCloudSettings

        UCloudSettings.from_provider(configuration)
    elif configuration.kind == "hetzner":
        from .hetzner.config import HetznerSettings

        HetznerSettings.from_provider(configuration)


__all__ = [
    "ComputeProvider",
    "DestructiveInstanceLoss",
    "InstanceBootstrapAccess",
    "InstanceCreateIntent",
    "ProviderConfiguration",
    "ProviderError",
    "ProviderMutationResult",
    "PROVIDER_ENTRY_POINT_GROUP",
    "load_external_provider",
    "default_provider_configuration",
    "validate_provider_configuration",
]
