from __future__ import annotations

from dataclasses import asdict, dataclass
import json
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
        policy_raw = raw.get("policy") or {}
        if not isinstance(policy_raw, dict):
            raise ValueError("Config policy must be a JSON object.")
        defaults = cls.default()
        policy_defaults = ScalePolicy()
        policy = ScalePolicy(
            min_nodes=int(policy_raw.get("min_nodes", policy_defaults.min_nodes)),
            max_nodes=int(policy_raw.get("max_nodes", policy_defaults.max_nodes)),
            warm_resources=ResourceQuantity.from_dict(
                policy_raw.get("warm_resources")
                or policy_defaults.warm_resources.to_dict()
            ),
            max_create_per_cycle=int(
                policy_raw.get("max_create_per_cycle", policy_defaults.max_create_per_cycle)
            ),
            max_stop_per_cycle=int(
                policy_raw.get("max_stop_per_cycle", policy_defaults.max_stop_per_cycle)
            ),
            max_provisioning_nodes=int(
                policy_raw.get(
                    "max_provisioning_nodes",
                    policy_defaults.max_provisioning_nodes,
                )
            ),
            provisioning_capacity_weight=float(
                policy_raw.get(
                    "provisioning_capacity_weight",
                    policy_defaults.provisioning_capacity_weight,
                )
            ),
            stale_provisioning_after_seconds=int(
                policy_raw.get(
                    "stale_provisioning_after_seconds",
                    policy_defaults.stale_provisioning_after_seconds,
                )
            ),
            stale_provisioning_capacity_weight=float(
                policy_raw.get(
                    "stale_provisioning_capacity_weight",
                    policy_defaults.stale_provisioning_capacity_weight,
                )
            ),
            scale_down_idle_seconds=int(
                policy_raw.get(
                    "scale_down_idle_seconds",
                    policy_defaults.scale_down_idle_seconds,
                )
            ),
            builder_scale_down_idle_seconds=int(
                policy_raw.get(
                    "builder_scale_down_idle_seconds",
                    policy_defaults.builder_scale_down_idle_seconds,
                )
            ),
            heartbeat_ttl_seconds=int(
                policy_raw.get("heartbeat_ttl_seconds", policy_defaults.heartbeat_ttl_seconds)
            ),
            default_node_resources=ResourceQuantity.from_dict(
                policy_raw.get("default_node_resources")
                or policy_defaults.default_node_resources.to_dict()
            ),
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
            gateway_public_link_port=int(
                raw.get("gateway_public_link_port", defaults.gateway_public_link_port)
            ),
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
