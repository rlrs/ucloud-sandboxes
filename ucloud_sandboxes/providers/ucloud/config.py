from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os
import sys

from ..base import ProviderConfiguration
from .payloads import DEFAULT_PUBLIC_LINK_PORT


@dataclass(frozen=True)
class UCloudSettings:
    project_id: str = ""
    session_file: str = ""
    template_job_id: str | None = None
    private_network_id: str | None = None
    gateway_public_link_id: str | None = None
    gateway_public_link_port: int = DEFAULT_PUBLIC_LINK_PORT

    @classmethod
    def default(cls) -> "UCloudSettings":
        return cls(session_file=str(default_session_path()))

    @classmethod
    def from_provider(cls, provider: ProviderConfiguration) -> "UCloudSettings":
        if provider.kind != "ucloud":
            raise ValueError(f"expected ucloud provider, got {provider.kind!r}")
        defaults = cls.default()
        allowed = {
            "session_file",
            "template_job_id",
            "private_network_id",
            "gateway_public_link_id",
            "gateway_public_link_port",
        }
        unknown = sorted(set(provider.settings) - allowed)
        if unknown:
            raise ValueError(
                "provider ucloud contains unknown fields: " + ", ".join(unknown)
            )
        port = _port(
            provider.settings.get(
                "gateway_public_link_port", defaults.gateway_public_link_port
            )
        )
        return cls(
            project_id=provider.scope_id,
            session_file=str(
                provider.settings.get("session_file") or defaults.session_file
            ),
            template_job_id=_optional_string(provider.settings.get("template_job_id")),
            private_network_id=_optional_string(
                provider.settings.get("private_network_id")
            ),
            gateway_public_link_id=_optional_string(
                provider.settings.get("gateway_public_link_id")
            ),
            gateway_public_link_port=port,
        )

    def to_provider(self) -> ProviderConfiguration:
        return ProviderConfiguration(
            kind="ucloud",
            scope_id=self.project_id,
            settings={
                "template_job_id": self.template_job_id,
                "private_network_id": self.private_network_id,
                "gateway_public_link_id": self.gateway_public_link_id,
                "gateway_public_link_port": self.gateway_public_link_port,
            },
        )


def default_session_path() -> Path:
    override = os.environ.get("UCLOUD_SESSION_FILE")
    if override:
        return Path(override).expanduser()
    if sys.platform == "darwin":
        return (
            Path.home()
            / "Library"
            / "Application Support"
            / "ucloud-cli"
            / "session.json"
        )
    if sys.platform.startswith("win"):
        return Path.home() / "AppData" / "Roaming" / "ucloud-cli" / "session.json"
    return Path.home() / ".config" / "ucloud-cli" / "session.json"


def _optional_string(value: object) -> str | None:
    return str(value) if value not in (None, "") else None


def _port(value: object) -> int:
    if isinstance(value, bool):
        raise ValueError("provider ucloud gateway_public_link_port must be an integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(
            "provider ucloud gateway_public_link_port must be an integer"
        ) from exc
    if not 1 <= parsed <= 65535:
        raise ValueError(
            "provider ucloud gateway_public_link_port must be in [1, 65535]"
        )
    return parsed
