from __future__ import annotations

from importlib.metadata import entry_points
from typing import Any, Callable, cast

from .base import ComputeProvider, ProviderConfiguration


PROVIDER_ENTRY_POINT_GROUP = "ucloud_sandboxes.compute_providers"
ProviderFactory = Callable[[ProviderConfiguration, Any], ComputeProvider]


def load_external_provider(
    configuration: ProviderConfiguration,
    options: Any,
) -> ComputeProvider:
    """Load a provider adapter installed through a Python entry point.

    A factory receives only its tagged provider configuration plus the parsed
    CLI options. Cloud-specific defaults should live in the tagged settings,
    while shared autoscaler flags may be read from ``options``.
    """

    matches = tuple(
        entry_points().select(
            group=PROVIDER_ENTRY_POINT_GROUP,
            name=configuration.kind,
        )
    )
    if not matches:
        raise ValueError(
            f"No compute provider named {configuration.kind!r} is installed. "
            f"Register one in the {PROVIDER_ENTRY_POINT_GROUP!r} entry-point group."
        )
    if len(matches) != 1:
        raise ValueError(
            f"Multiple compute providers named {configuration.kind!r} are installed."
        )
    factory = cast(ProviderFactory, matches[0].load())
    provider = factory(configuration, options)
    if provider.kind != configuration.kind:
        raise ValueError(
            "Compute provider factory returned kind "
            f"{provider.kind!r}, expected {configuration.kind!r}."
        )
    return provider
