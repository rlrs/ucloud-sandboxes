from __future__ import annotations

from dataclasses import asdict, dataclass
import json
import math
import os
from pathlib import Path
import sys
from typing import Any

from .networking import DEFAULT_PUBLIC_LINK_PORT
from .models import ResourceQuantity, ScalePolicy


APP_NAME = "ucloud-sandboxes"


def default_ucloud_session_path() -> Path:
    override = os.environ.get("UCLOUD_SESSION_FILE")
    if override:
        return Path(override).expanduser()
    if sys.platform == "darwin":
        return Path.home() / "Library" / "Application Support" / "ucloud-cli" / "session.json"
    if sys.platform.startswith("win"):
        return Path.home() / "AppData" / "Roaming" / "ucloud-cli" / "session.json"
    return Path.home() / ".config" / "ucloud-cli" / "session.json"


def default_state_dir() -> Path:
    if sys.platform == "darwin":
        return Path.home() / "Library" / "Application Support" / APP_NAME
    if sys.platform.startswith("win"):
        return Path.home() / "AppData" / "Roaming" / APP_NAME
    return Path.home() / ".local" / "state" / APP_NAME


@dataclass(frozen=True)
class AutoscalerConfig:
    project_id: str
    deployment_id: str = ""
    job_name_prefix: str = "ucloud-sandbox-node-"
    template_job_id: str | None = None
    private_network_id: str | None = None
    gateway_public_link_id: str | None = None
    gateway_public_link_port: int = DEFAULT_PUBLIC_LINK_PORT
    node_hostname_prefix: str = "sandbox-node"
    ucloud_session_file: str = ""
    state_dir: str = ""
    metrics_file: str = ""
    policy: ScalePolicy = ScalePolicy()

    @classmethod
    def default(cls, project_id: str = "") -> "AutoscalerConfig":
        return cls(
            project_id=project_id,
            ucloud_session_file=str(default_ucloud_session_path()),
            state_dir=str(default_state_dir()),
        )

    @classmethod
    def from_file(cls, path: Path) -> "AutoscalerConfig":
        raw = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise ValueError("Config file must contain a JSON object.")
        return cls.from_dict(raw)

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "AutoscalerConfig":
        if not isinstance(raw, dict):
            raise ValueError("Config must be a JSON object.")
        policy_raw = raw.get("policy") or {}
        if not isinstance(policy_raw, dict):
            raise ValueError("Config policy must be a JSON object.")
        defaults = cls.default()
        policy_defaults = ScalePolicy()
        warm_resources_raw = policy_raw.get(
            "warm_resources",
            policy_defaults.warm_resources.to_dict(),
        )
        default_node_resources_raw = policy_raw.get(
            "default_node_resources",
            policy_defaults.default_node_resources.to_dict(),
        )
        _validate_resource_quantity("policy.warm_resources", warm_resources_raw)
        _validate_resource_quantity(
            "policy.default_node_resources",
            default_node_resources_raw,
        )
        policy = ScalePolicy(
            min_nodes=_config_int(
                "policy.min_nodes",
                policy_raw.get("min_nodes", policy_defaults.min_nodes),
                minimum=0,
            ),
            max_nodes=_config_int(
                "policy.max_nodes",
                policy_raw.get("max_nodes", policy_defaults.max_nodes),
                minimum=0,
            ),
            warm_resources=ResourceQuantity.from_dict(warm_resources_raw),
            max_create_per_cycle=_config_int(
                "policy.max_create_per_cycle",
                policy_raw.get(
                    "max_create_per_cycle",
                    policy_defaults.max_create_per_cycle,
                ),
                minimum=0,
            ),
            max_stop_per_cycle=_config_int(
                "policy.max_stop_per_cycle",
                policy_raw.get(
                    "max_stop_per_cycle",
                    policy_defaults.max_stop_per_cycle,
                ),
                minimum=0,
            ),
            max_provisioning_nodes=_config_int(
                "policy.max_provisioning_nodes",
                policy_raw.get(
                    "max_provisioning_nodes",
                    policy_defaults.max_provisioning_nodes,
                ),
                minimum=0,
            ),
            provisioning_capacity_weight=_config_float(
                "policy.provisioning_capacity_weight",
                policy_raw.get(
                    "provisioning_capacity_weight",
                    policy_defaults.provisioning_capacity_weight,
                ),
                minimum=0.0,
                maximum=1.0,
            ),
            stale_provisioning_after_seconds=_config_int(
                "policy.stale_provisioning_after_seconds",
                policy_raw.get(
                    "stale_provisioning_after_seconds",
                    policy_defaults.stale_provisioning_after_seconds,
                ),
                minimum=0,
            ),
            stale_provisioning_capacity_weight=_config_float(
                "policy.stale_provisioning_capacity_weight",
                policy_raw.get(
                    "stale_provisioning_capacity_weight",
                    policy_defaults.stale_provisioning_capacity_weight,
                ),
                minimum=0.0,
                maximum=1.0,
            ),
            scale_down_idle_seconds=_config_int(
                "policy.scale_down_idle_seconds",
                policy_raw.get(
                    "scale_down_idle_seconds",
                    policy_defaults.scale_down_idle_seconds,
                ),
                minimum=0,
            ),
            builder_scale_down_idle_seconds=_config_int(
                "policy.builder_scale_down_idle_seconds",
                policy_raw.get(
                    "builder_scale_down_idle_seconds",
                    policy_defaults.builder_scale_down_idle_seconds,
                ),
                minimum=0,
            ),
            heartbeat_ttl_seconds=_config_int(
                "policy.heartbeat_ttl_seconds",
                policy_raw.get(
                    "heartbeat_ttl_seconds",
                    policy_defaults.heartbeat_ttl_seconds,
                ),
                minimum=1,
            ),
            default_node_resources=ResourceQuantity.from_dict(default_node_resources_raw),
        )
        if policy.min_nodes > policy.max_nodes:
            raise ValueError("policy.min_nodes cannot exceed policy.max_nodes.")
        if (
            policy.default_node_resources.vcpu <= 0
            or policy.default_node_resources.memory_mb <= 0
            or policy.default_node_resources.disk_mb <= 0
        ):
            raise ValueError(
                "policy.default_node_resources values must all be positive."
            )
        gateway_public_link_port = _config_int(
            "gateway_public_link_port",
            raw.get("gateway_public_link_port", defaults.gateway_public_link_port),
            minimum=1,
            maximum=65535,
        )
        return cls(
            project_id=str(raw.get("project_id", defaults.project_id)),
            deployment_id=str(raw.get("deployment_id", defaults.deployment_id)),
            job_name_prefix=str(raw.get("job_name_prefix", defaults.job_name_prefix)),
            template_job_id=(
                str(raw["template_job_id"]) if raw.get("template_job_id") else None
            ),
            private_network_id=(
                str(raw["private_network_id"]) if raw.get("private_network_id") else None
            ),
            gateway_public_link_id=(
                str(raw["gateway_public_link_id"])
                if raw.get("gateway_public_link_id")
                else None
            ),
            gateway_public_link_port=gateway_public_link_port,
            node_hostname_prefix=str(
                raw.get("node_hostname_prefix") or defaults.node_hostname_prefix
            ),
            ucloud_session_file=str(
                raw.get("ucloud_session_file") or defaults.ucloud_session_file
            ),
            state_dir=str(raw.get("state_dir") or defaults.state_dir),
            metrics_file=str(raw.get("metrics_file") or defaults.metrics_file),
            policy=policy,
        )

    def with_project_id(self, project_id: str | None) -> "AutoscalerConfig":
        if not project_id:
            return self
        return AutoscalerConfig(
            project_id=project_id,
            deployment_id=self.deployment_id,
            job_name_prefix=self.job_name_prefix,
            template_job_id=self.template_job_id,
            private_network_id=self.private_network_id,
            gateway_public_link_id=self.gateway_public_link_id,
            gateway_public_link_port=self.gateway_public_link_port,
            node_hostname_prefix=self.node_hostname_prefix,
            ucloud_session_file=self.ucloud_session_file,
            state_dir=self.state_dir,
            metrics_file=self.metrics_file,
            policy=self.policy,
        )

    def with_state_dir(self, state_dir: str | None) -> "AutoscalerConfig":
        if not state_dir:
            return self
        return AutoscalerConfig(
            project_id=self.project_id,
            deployment_id=self.deployment_id,
            job_name_prefix=self.job_name_prefix,
            template_job_id=self.template_job_id,
            private_network_id=self.private_network_id,
            gateway_public_link_id=self.gateway_public_link_id,
            gateway_public_link_port=self.gateway_public_link_port,
            node_hostname_prefix=self.node_hostname_prefix,
            ucloud_session_file=self.ucloud_session_file,
            state_dir=state_dir,
            metrics_file=self.metrics_file,
            policy=self.policy,
        )

    def heartbeat_file(self) -> Path:
        return Path(self.state_dir).expanduser() / "heartbeats.json"

    def sandbox_file(self) -> Path:
        return Path(self.state_dir).expanduser() / "sandboxes.json"

    def image_file(self) -> Path:
        return Path(self.state_dir).expanduser() / "images.json"

    def routing_file(self) -> Path:
        return Path(self.state_dir).expanduser() / "routes.sqlite"

    def registry_usage_file(self) -> Path:
        return Path(self.state_dir).expanduser() / "registry-usage.json"

    def bootstrap_file(self) -> Path:
        return Path(self.state_dir).expanduser() / "vm-bootstrap.json"

    def metrics_path(self) -> Path:
        if self.metrics_file:
            return Path(self.metrics_file).expanduser()
        return Path(self.state_dir).expanduser() / "metrics.jsonl"

    def to_dict(self) -> dict[str, Any]:
        raw = asdict(self)
        raw["policy"] = asdict(self.policy)
        return raw


def _config_int(
    label: str,
    value: object,
    *,
    minimum: int | None = None,
    maximum: int | None = None,
) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{label} must be an integer.")
    try:
        parsed = int(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{label} must be an integer.") from exc
    if isinstance(value, float) and not value.is_integer():
        raise ValueError(f"{label} must be an integer.")
    if minimum is not None and parsed < minimum:
        raise ValueError(f"{label} must be at least {minimum}.")
    if maximum is not None and parsed > maximum:
        raise ValueError(f"{label} must be at most {maximum}.")
    return parsed


def _config_float(
    label: str,
    value: object,
    *,
    minimum: float | None = None,
    maximum: float | None = None,
) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{label} must be a finite number.")
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{label} must be a finite number.") from exc
    if not math.isfinite(parsed):
        raise ValueError(f"{label} must be a finite number.")
    if minimum is not None and parsed < minimum:
        raise ValueError(f"{label} must be at least {minimum}.")
    if maximum is not None and parsed > maximum:
        raise ValueError(f"{label} must be at most {maximum}.")
    return parsed


def _validate_resource_quantity(label: str, value: object) -> None:
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be a JSON object.")
    aliases = {
        "vcpu": ("vcpu", "cpu"),
        "memory_mb": ("memory_mb", "memoryMb"),
        "disk_mb": ("disk_mb", "diskMb"),
    }
    for field, keys in aliases.items():
        raw_value: object = 0
        for key in keys:
            if key in value:
                raw_value = value[key]
                break
        if field == "vcpu":
            _config_float(f"{label}.{field}", raw_value, minimum=0.0)
        else:
            _config_int(f"{label}.{field}", raw_value, minimum=0)
