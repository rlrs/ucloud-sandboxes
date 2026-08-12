from __future__ import annotations

from dataclasses import dataclass
import ipaddress
import re
from typing import TypeAlias
from urllib.parse import urlsplit

from ..base import ProviderConfiguration


DEFAULT_HETZNER_API_BASE_URL = "https://api.hetzner.cloud/v1"
DEFAULT_HETZNER_API_TOKEN_ENV = "HETZNER_API_KEY"
HetznerImage: TypeAlias = int | str
_ENV_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SSH_USER_RE = re.compile(r"^[a-z_][a-z0-9_-]{0,31}$")


@dataclass(frozen=True)
class HetznerSettings:
    """Exact provider-owned configuration for Hetzner Cloud compute."""

    project_name: str
    network_id: int
    sandbox_image: HetznerImage
    builder_image: HetznerImage
    api_token_env: str = DEFAULT_HETZNER_API_TOKEN_ENV
    api_base_url: str = DEFAULT_HETZNER_API_BASE_URL
    location: str = "hel1"
    sandbox_server_type: str = "cpx62"
    builder_server_type: str = "cpx62"
    ssh_user: str = "root"
    ssh_key_ids: tuple[int, ...] = ()
    firewall_ids: tuple[int, ...] = ()
    placement_group_id: int | None = None
    enable_ipv4: bool = False
    enable_ipv6: bool = False
    enable_private_egress: bool = False
    private_dns_servers: tuple[str, ...] = ("1.1.1.1", "8.8.8.8")

    @classmethod
    def from_provider(cls, provider: ProviderConfiguration) -> "HetznerSettings":
        if provider.kind != "hetzner":
            raise ValueError(f"expected hetzner provider, got {provider.kind!r}")
        allowed = {
            "api_token_env",
            "api_base_url",
            "network_id",
            "location",
            "sandbox_server_type",
            "sandbox_image",
            "builder_server_type",
            "builder_image",
            "ssh_user",
            "ssh_key_ids",
            "firewall_ids",
            "placement_group_id",
            "enable_ipv4",
            "enable_ipv6",
            "enable_private_egress",
            "private_dns_servers",
        }
        unknown = sorted(set(provider.settings) - allowed)
        if unknown:
            raise ValueError(
                "provider hetzner contains unknown fields: " + ", ".join(unknown)
            )

        project_name = _required_string("scope_id", provider.scope_id)
        network_id = _positive_int("network_id", provider.settings.get("network_id"))
        sandbox_image = _image("sandbox_image", provider.settings.get("sandbox_image"))
        builder_image = _image("builder_image", provider.settings.get("builder_image"))
        ssh_key_ids = _positive_int_tuple(
            "ssh_key_ids", provider.settings.get("ssh_key_ids", [])
        )
        if not ssh_key_ids:
            raise ValueError(
                "provider hetzner ssh_key_ids must contain at least one key"
            )
        firewall_ids = _positive_int_tuple(
            "firewall_ids", provider.settings.get("firewall_ids", [])
        )
        enable_ipv4 = _boolean(
            "enable_ipv4", provider.settings.get("enable_ipv4", False)
        )
        enable_ipv6 = _boolean(
            "enable_ipv6", provider.settings.get("enable_ipv6", False)
        )
        enable_private_egress = _boolean(
            "enable_private_egress",
            provider.settings.get("enable_private_egress", False),
        )
        private_dns_servers = _ipv4_tuple(
            "private_dns_servers",
            provider.settings.get("private_dns_servers", ["1.1.1.1", "8.8.8.8"]),
        )
        if (enable_ipv4 or enable_ipv6) and not firewall_ids:
            raise ValueError(
                "provider hetzner public networking requires at least one firewall_id"
            )
        if enable_private_egress and (enable_ipv4 or enable_ipv6):
            raise ValueError(
                "provider hetzner private egress cannot be combined with public networking"
            )
        return cls(
            project_name=project_name,
            network_id=network_id,
            sandbox_image=sandbox_image,
            builder_image=builder_image,
            api_token_env=_environment_name(
                provider.settings.get("api_token_env", DEFAULT_HETZNER_API_TOKEN_ENV),
            ),
            api_base_url=_api_base_url(
                provider.settings.get("api_base_url", DEFAULT_HETZNER_API_BASE_URL)
            ),
            location=_required_string(
                "location", provider.settings.get("location", "hel1")
            ),
            sandbox_server_type=_required_string(
                "sandbox_server_type",
                provider.settings.get("sandbox_server_type", "cpx62"),
            ),
            builder_server_type=_required_string(
                "builder_server_type",
                provider.settings.get("builder_server_type", "cpx62"),
            ),
            ssh_user=_ssh_user(provider.settings.get("ssh_user", "root")),
            ssh_key_ids=ssh_key_ids,
            firewall_ids=firewall_ids,
            placement_group_id=_optional_positive_int(
                "placement_group_id",
                provider.settings.get("placement_group_id"),
            ),
            enable_ipv4=enable_ipv4,
            enable_ipv6=enable_ipv6,
            enable_private_egress=enable_private_egress,
            private_dns_servers=private_dns_servers,
        )


def _required_string(name: str, value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"provider hetzner {name} is required")
    return value.strip()


def _positive_int(name: str, value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"provider hetzner {name} must be a positive integer")
    return value


def _optional_positive_int(name: str, value: object) -> int | None:
    if value in (None, ""):
        return None
    return _positive_int(name, value)


def _positive_int_tuple(name: str, value: object) -> tuple[int, ...]:
    if not isinstance(value, (list, tuple)):
        raise ValueError(f"provider hetzner {name} must be a list")
    parsed = tuple(_positive_int(name, item) for item in value)
    if len(parsed) != len(set(parsed)):
        raise ValueError(f"provider hetzner {name} cannot contain duplicates")
    return parsed


def _ipv4_tuple(name: str, value: object) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)) or not value:
        raise ValueError(f"provider hetzner {name} must be a non-empty list")
    parsed: list[str] = []
    for item in value:
        if not isinstance(item, str):
            raise ValueError(f"provider hetzner {name} must contain IPv4 addresses")
        try:
            address = ipaddress.ip_address(item.strip())
        except ValueError as exc:
            raise ValueError(
                f"provider hetzner {name} must contain IPv4 addresses"
            ) from exc
        if address.version != 4 or address.is_unspecified or address.is_multicast:
            raise ValueError(f"provider hetzner {name} contains an unsafe address")
        normalized = str(address)
        if normalized in parsed:
            raise ValueError(f"provider hetzner {name} cannot contain duplicates")
        parsed.append(normalized)
    return tuple(parsed)


def _image(name: str, value: object) -> HetznerImage:
    if isinstance(value, bool):
        raise ValueError(
            f"provider hetzner {name} must be an image name or positive image id"
        )
    if isinstance(value, int):
        return _positive_int(name, value)
    return _required_string(name, value)


def _boolean(name: str, value: object) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"provider hetzner {name} must be a boolean")
    return value


def _ssh_user(value: object) -> str:
    user = _required_string("ssh_user", value)
    if not _SSH_USER_RE.fullmatch(user):
        raise ValueError("provider hetzner ssh_user contains unsupported characters")
    return user


def _api_base_url(value: object) -> str:
    base_url = _required_string("api_base_url", value).rstrip("/")
    parsed = urlsplit(base_url)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("provider hetzner api_base_url must be a safe HTTPS URL")
    return base_url


def _environment_name(value: object) -> str:
    name = _required_string("api_token_env", value)
    if not _ENV_NAME_RE.fullmatch(name):
        raise ValueError("provider hetzner api_token_env is not a valid variable name")
    return name
