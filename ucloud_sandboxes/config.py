from __future__ import annotations

from dataclasses import asdict, dataclass, fields, replace
import json
import math
from pathlib import Path
from typing import Any

from .models import ResourceQuantity, ScalePolicy
from .providers import (
    ProviderConfiguration,
    default_provider_configuration,
    validate_provider_configuration,
)


DEPLOYMENT_CONFIG_SCHEMA = 2
DEFAULT_DATA_ROOT = "/work/data/ucloud-sandboxes/state"
DEFAULT_REGISTRY_MOUNT_POINT = "/work/data"
DEFAULT_REGISTRY_DATA_ROOT = "/work/data/ucloud-sandbox-registry/docker-registry"
DEFAULT_REGISTRY_ALIAS = "ucloud-sandbox-registry"
DEFAULT_INSTALL_ROOT = "/work/ucloud-sandboxes"
DEFAULT_DIRECT_RUNSC_COMMIT = "0" * 40
_RUNTIME_POLICY_FIELDS = {
    "builder_scale_down_idle_seconds",
    "heartbeat_ttl_seconds",
    "default_node_resources",
}


@dataclass(frozen=True)
class SandboxPoolConfig:
    product_id: str = "cpu-amd-zen5-32-vcpu"
    disk_gb: int = 2000
    default_vcpu: float = 32.0
    default_memory_mb: int = 98_304
    docker_quota_image_gb: int = 440
    swap_gb: int = 96
    direct_runsc_commit: str = DEFAULT_DIRECT_RUNSC_COMMIT
    direct_network_allow_tcp: tuple[str, ...] = ()
    storage_native_repository: str = "ucloud-sandbox-snapshots"
    storage_native_cache_gb: int = 32
    storage_native_pool_low_watermark: int = 2
    storage_native_pool_high_watermark: int = 16
    direct_disk_headroom_mb: int = 16 * 1024
    direct_max_concurrent_restores: int = 8
    max_concurrent_image_pulls: int = 8

    @property
    def resources(self) -> ResourceQuantity:
        reserved_mb = (
            self.docker_quota_image_gb * 1024
            + self.swap_gb * 1024
            + self.storage_native_cache_gb * 1024
            + self.direct_disk_headroom_mb
        )
        return ResourceQuantity(
            vcpu=self.default_vcpu,
            memory_mb=self.default_memory_mb,
            disk_mb=self.disk_gb * 1024 - reserved_mb,
        )

    @classmethod
    def from_dict(cls, raw: object) -> "SandboxPoolConfig":
        values = _exact_dataclass_values("sandbox", raw, cls())
        values["direct_network_allow_tcp"] = _string_tuple(
            "sandbox.direct_network_allow_tcp",
            values["direct_network_allow_tcp"],
        )
        result = cls(**values)
        _require_string("sandbox.product_id", result.product_id)
        _require_int("sandbox.disk_gb", result.disk_gb, minimum=1)
        _require_float("sandbox.default_vcpu", result.default_vcpu, minimum=0.01)
        _require_int("sandbox.default_memory_mb", result.default_memory_mb, minimum=1)
        for name in (
            "docker_quota_image_gb",
            "storage_native_cache_gb",
            "storage_native_pool_high_watermark",
            "direct_disk_headroom_mb",
            "direct_max_concurrent_restores",
            "max_concurrent_image_pulls",
        ):
            _require_int(f"sandbox.{name}", getattr(result, name), minimum=1)
        for name in ("swap_gb", "storage_native_pool_low_watermark"):
            _require_int(f"sandbox.{name}", getattr(result, name), minimum=0)
        if (
            result.storage_native_pool_low_watermark
            > result.storage_native_pool_high_watermark
        ):
            raise ValueError(
                "sandbox.storage_native_pool_low_watermark cannot exceed "
                "sandbox.storage_native_pool_high_watermark"
            )
        _require_sha1("sandbox.direct_runsc_commit", result.direct_runsc_commit)
        _require_repository(
            "sandbox.storage_native_repository", result.storage_native_repository
        )
        if result.resources.disk_mb < 1:
            raise ValueError(
                "sandbox disk must exceed Docker, swap, storage cache, and "
                "direct-runtime headroom"
            )
        return result


@dataclass(frozen=True)
class BuilderPoolConfig:
    product_id: str = "cpu-amd-zen5-16-vcpu"
    disk_gb: int = 250
    docker_quota_image_gb: int = 200
    max_nodes: int = 1
    scale_down_idle_seconds: int = 900
    max_concurrent_image_pulls: int = 8
    buildx_cache_ref: str = ""

    @classmethod
    def from_dict(cls, raw: object) -> "BuilderPoolConfig":
        result = cls(**_exact_dataclass_values("builder", raw, cls()))
        _require_string("builder.product_id", result.product_id)
        for name in ("disk_gb", "max_concurrent_image_pulls"):
            _require_int(f"builder.{name}", getattr(result, name), minimum=1)
        for name in (
            "docker_quota_image_gb",
            "max_nodes",
            "scale_down_idle_seconds",
        ):
            _require_int(f"builder.{name}", getattr(result, name), minimum=0)
        if result.disk_gb < result.docker_quota_image_gb + 32:
            raise ValueError(
                "builder.disk_gb must leave at least 32 GB outside the Docker quota"
            )
        if not isinstance(result.buildx_cache_ref, str):
            raise ValueError("builder.buildx_cache_ref must be a string")
        return result


@dataclass(frozen=True)
class DeploymentConfig:
    schema: int
    deployment_id: str
    provider: ProviderConfiguration
    data_root: str
    registry_mount_point: str
    registry_data_root: str
    gateway_private_host: str
    registry_private_ip: str
    gateway_port: int
    gateway_heartbeat_ttl_seconds: int
    gateway_max_concurrent_sandbox_creates: int
    gateway_max_http_request_threads: int
    relay_port: int
    relay_request_timeout_seconds: int
    relay_worker_lease_seconds: int
    relay_completed_request_retention_seconds: int
    registry_port: int
    registry_retention_days: float
    registry_keep_per_repository: int
    autoscaler_interval_seconds: float
    autoscaler_max_init_per_cycle: int
    autoscaler_init_retry_seconds: int
    autoscaler_init_timeout_seconds: int
    autoscaler_max_storage_native_detaches_per_cycle: int
    heartbeat_interval_seconds: int
    policy: ScalePolicy
    sandbox: SandboxPoolConfig
    builder: BuilderPoolConfig

    @classmethod
    def default(cls, scope_id: str = "project-id") -> "DeploymentConfig":
        sandbox = SandboxPoolConfig()
        builder = BuilderPoolConfig()
        return cls(
            schema=DEPLOYMENT_CONFIG_SCHEMA,
            deployment_id="production",
            provider=default_provider_configuration(scope_id),
            data_root=DEFAULT_DATA_ROOT,
            registry_mount_point=DEFAULT_REGISTRY_MOUNT_POINT,
            registry_data_root=DEFAULT_REGISTRY_DATA_ROOT,
            gateway_private_host="sandbox-gateway-production",
            registry_private_ip="",
            gateway_port=8090,
            gateway_heartbeat_ttl_seconds=120,
            gateway_max_concurrent_sandbox_creates=64,
            gateway_max_http_request_threads=128,
            relay_port=8092,
            relay_request_timeout_seconds=7200,
            relay_worker_lease_seconds=600,
            relay_completed_request_retention_seconds=3600,
            registry_port=5000,
            registry_retention_days=30.0,
            registry_keep_per_repository=0,
            autoscaler_interval_seconds=5.0,
            autoscaler_max_init_per_cycle=4,
            autoscaler_init_retry_seconds=30,
            autoscaler_init_timeout_seconds=1800,
            autoscaler_max_storage_native_detaches_per_cycle=2,
            heartbeat_interval_seconds=20,
            policy=replace(
                ScalePolicy(),
                heartbeat_ttl_seconds=120,
                builder_scale_down_idle_seconds=builder.scale_down_idle_seconds,
                default_node_resources=sandbox.resources,
            ),
            sandbox=sandbox,
            builder=builder,
        )

    @classmethod
    def from_file(cls, path: Path) -> "DeploymentConfig":
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise ValueError("deployment config is not valid JSON") from exc
        return cls.from_dict(raw)

    @classmethod
    def from_dict(cls, raw: object) -> "DeploymentConfig":
        if not isinstance(raw, dict):
            raise ValueError("deployment config must be a JSON object")
        expected = {item.name for item in fields(cls)}
        schema = _require_int("schema", raw.get("schema"), minimum=1)
        if schema == 1:
            legacy_expected = expected - {
                "registry_data_root",
                "registry_mount_point",
                "autoscaler_max_storage_native_detaches_per_cycle",
            } | {"autoscaler_max_storage_native_migrations_per_cycle"}
            _require_exact_keys("deployment config", raw, legacy_expected)
            legacy_data_root = _require_absolute_path("data_root", raw["data_root"])
            raw = {
                **raw,
                "schema": DEPLOYMENT_CONFIG_SCHEMA,
                "registry_data_root": str(
                    Path(legacy_data_root).parent.parent
                    / "ucloud-sandbox-registry/docker-registry"
                ),
                "registry_mount_point": str(Path(legacy_data_root).parent.parent),
                "autoscaler_max_storage_native_detaches_per_cycle": raw[
                    "autoscaler_max_storage_native_migrations_per_cycle"
                ],
            }
            raw.pop("autoscaler_max_storage_native_migrations_per_cycle")
            schema = DEPLOYMENT_CONFIG_SCHEMA
        elif schema == DEPLOYMENT_CONFIG_SCHEMA:
            _require_exact_keys("deployment config", raw, expected)
        else:
            raise ValueError(f"unsupported deployment config schema: {schema}")
        provider = ProviderConfiguration.from_dict(raw["provider"])
        if provider.kind == "ucloud" and "session_file" in provider.settings:
            raise ValueError("provider ucloud contains unknown fields: session_file")
        validate_provider_configuration(provider)
        sandbox = SandboxPoolConfig.from_dict(raw["sandbox"])
        builder = BuilderPoolConfig.from_dict(raw["builder"])
        heartbeat_ttl = _require_int(
            "gateway_heartbeat_ttl_seconds",
            raw["gateway_heartbeat_ttl_seconds"],
            minimum=1,
        )
        policy = _decode_policy(
            raw["policy"],
            heartbeat_ttl_seconds=heartbeat_ttl,
            builder_scale_down_idle_seconds=builder.scale_down_idle_seconds,
            default_node_resources=sandbox.resources,
        )
        result = cls(
            schema=schema,
            deployment_id=_require_string("deployment_id", raw["deployment_id"]),
            provider=provider,
            data_root=_require_absolute_path("data_root", raw["data_root"]),
            registry_mount_point=_require_absolute_path(
                "registry_mount_point", raw["registry_mount_point"]
            ),
            registry_data_root=_require_absolute_path(
                "registry_data_root", raw["registry_data_root"]
            ),
            gateway_private_host=_require_string(
                "gateway_private_host", raw["gateway_private_host"]
            ),
            registry_private_ip=_require_optional_string(
                "registry_private_ip", raw["registry_private_ip"]
            ),
            gateway_port=_require_port("gateway_port", raw["gateway_port"]),
            gateway_heartbeat_ttl_seconds=heartbeat_ttl,
            gateway_max_concurrent_sandbox_creates=_require_int(
                "gateway_max_concurrent_sandbox_creates",
                raw["gateway_max_concurrent_sandbox_creates"],
                minimum=0,
            ),
            gateway_max_http_request_threads=_require_int(
                "gateway_max_http_request_threads",
                raw["gateway_max_http_request_threads"],
                minimum=1,
            ),
            relay_port=_require_port("relay_port", raw["relay_port"]),
            relay_request_timeout_seconds=_require_int(
                "relay_request_timeout_seconds",
                raw["relay_request_timeout_seconds"],
                minimum=1,
            ),
            relay_worker_lease_seconds=_require_int(
                "relay_worker_lease_seconds",
                raw["relay_worker_lease_seconds"],
                minimum=1,
            ),
            relay_completed_request_retention_seconds=_require_int(
                "relay_completed_request_retention_seconds",
                raw["relay_completed_request_retention_seconds"],
                minimum=1,
            ),
            registry_port=_require_port("registry_port", raw["registry_port"]),
            registry_retention_days=_require_float(
                "registry_retention_days", raw["registry_retention_days"], minimum=0.01
            ),
            registry_keep_per_repository=_require_int(
                "registry_keep_per_repository",
                raw["registry_keep_per_repository"],
                minimum=0,
            ),
            autoscaler_interval_seconds=_require_float(
                "autoscaler_interval_seconds",
                raw["autoscaler_interval_seconds"],
                minimum=1.0,
            ),
            autoscaler_max_init_per_cycle=_require_int(
                "autoscaler_max_init_per_cycle",
                raw["autoscaler_max_init_per_cycle"],
                minimum=0,
            ),
            autoscaler_init_retry_seconds=_require_int(
                "autoscaler_init_retry_seconds",
                raw["autoscaler_init_retry_seconds"],
                minimum=0,
            ),
            autoscaler_init_timeout_seconds=_require_int(
                "autoscaler_init_timeout_seconds",
                raw["autoscaler_init_timeout_seconds"],
                minimum=1,
            ),
            autoscaler_max_storage_native_detaches_per_cycle=_require_int(
                "autoscaler_max_storage_native_detaches_per_cycle",
                raw["autoscaler_max_storage_native_detaches_per_cycle"],
                minimum=0,
            ),
            heartbeat_interval_seconds=_require_int(
                "heartbeat_interval_seconds",
                raw["heartbeat_interval_seconds"],
                minimum=1,
            ),
            policy=policy,
            sandbox=sandbox,
            builder=builder,
        )
        if result.gateway_port in {result.relay_port, result.registry_port} or (
            result.relay_port == result.registry_port
        ):
            raise ValueError("gateway, relay, and registry ports must be distinct")
        if not Path(result.registry_data_root).is_relative_to(
            Path(result.registry_mount_point)
        ):
            raise ValueError("registry_data_root must be inside registry_mount_point")
        return result

    def control_state_file(self) -> Path:
        return self._state_file("control-state.sqlite")

    def image_file(self) -> Path:
        return self._state_file("images.sqlite")

    def routing_file(self) -> Path:
        return self._state_file("routes.sqlite")

    def registry_usage_file(self) -> Path:
        return self._state_file("registry-usage.sqlite")

    def metrics_path(self) -> Path:
        return self._state_file("metrics.sqlite")

    def autoscaler_state_file(self) -> Path:
        return self._state_file("autoscaler-state.sqlite")

    def session_file(self) -> Path:
        return self._state_file("ucloud-session.json")

    def gateway_token_file(self) -> Path:
        return self._state_file("gateway-token")

    def sandbox_api_token_file(self) -> Path:
        return self._state_file("sandbox-api-token")

    def heartbeat_token_file(self) -> Path:
        return self._state_file("heartbeat-token")

    def node_control_token_file(self) -> Path:
        return self._state_file("node-control-token")

    def relay_sandbox_token_file(self) -> Path:
        return self._state_file("relay-sandbox-token")

    def relay_worker_token_file(self) -> Path:
        return self._state_file("relay-worker-token")

    def relay_state_file(self) -> Path:
        return self._state_file("model-relay.sqlite3")

    def init_ssh_private_key_file(self) -> Path:
        return self._state_file("ssh/gateway-init")

    def init_authorized_key_file(self) -> Path:
        return self._state_file("ssh/gateway-init.pub")

    def sandbox_node_package_bundle(self) -> Path:
        return Path(DEFAULT_INSTALL_ROOT) / "release/sandbox-node-package.tar.gz"

    def builder_node_package_bundle(self) -> Path:
        return Path(DEFAULT_INSTALL_ROOT) / "release/builder-node-package.tar.gz"

    def registry_data_dir(self) -> Path:
        return Path(self.registry_data_root)

    @property
    def registry_endpoint_host(self) -> str:
        return (
            DEFAULT_REGISTRY_ALIAS
            if self.registry_private_ip
            else self.gateway_private_host
        )

    @property
    def registry_url(self) -> str:
        return f"http://127.0.0.1:{self.registry_port}"

    @property
    def registry_worker_url(self) -> str:
        return f"http://{self.registry_endpoint_host}:{self.registry_port}"

    @property
    def registry_host_alias(self) -> str:
        if not self.registry_private_ip:
            return ""
        return f"{DEFAULT_REGISTRY_ALIAS}={self.registry_private_ip}"

    @property
    def heartbeat_url(self) -> str:
        return (
            f"http://{self.gateway_private_host}:{self.gateway_port}"
            "/v1/nodes/heartbeat"
        )

    def to_dict(self) -> dict[str, Any]:
        policy = asdict(self.policy)
        for name in _RUNTIME_POLICY_FIELDS:
            policy.pop(name)
        provider = self.provider.to_dict()
        if self.provider.kind == "ucloud":
            provider.pop("session_file", None)
        sandbox = asdict(self.sandbox)
        sandbox["direct_network_allow_tcp"] = list(
            self.sandbox.direct_network_allow_tcp
        )
        return {
            "schema": self.schema,
            "deployment_id": self.deployment_id,
            "provider": provider,
            "data_root": self.data_root,
            "registry_mount_point": self.registry_mount_point,
            "registry_data_root": self.registry_data_root,
            "gateway_private_host": self.gateway_private_host,
            "registry_private_ip": self.registry_private_ip,
            "gateway_port": self.gateway_port,
            "gateway_heartbeat_ttl_seconds": self.gateway_heartbeat_ttl_seconds,
            "gateway_max_concurrent_sandbox_creates": (
                self.gateway_max_concurrent_sandbox_creates
            ),
            "gateway_max_http_request_threads": self.gateway_max_http_request_threads,
            "relay_port": self.relay_port,
            "relay_request_timeout_seconds": self.relay_request_timeout_seconds,
            "relay_worker_lease_seconds": self.relay_worker_lease_seconds,
            "relay_completed_request_retention_seconds": (
                self.relay_completed_request_retention_seconds
            ),
            "registry_port": self.registry_port,
            "registry_retention_days": self.registry_retention_days,
            "registry_keep_per_repository": self.registry_keep_per_repository,
            "autoscaler_interval_seconds": self.autoscaler_interval_seconds,
            "autoscaler_max_init_per_cycle": self.autoscaler_max_init_per_cycle,
            "autoscaler_init_retry_seconds": self.autoscaler_init_retry_seconds,
            "autoscaler_init_timeout_seconds": self.autoscaler_init_timeout_seconds,
            "autoscaler_max_storage_native_detaches_per_cycle": (
                self.autoscaler_max_storage_native_detaches_per_cycle
            ),
            "heartbeat_interval_seconds": self.heartbeat_interval_seconds,
            "policy": policy,
            "sandbox": sandbox,
            "builder": asdict(self.builder),
        }

    def _state_file(self, relative: str) -> Path:
        return Path(self.data_root) / relative


def _decode_policy(
    raw: object,
    *,
    heartbeat_ttl_seconds: int,
    builder_scale_down_idle_seconds: int,
    default_node_resources: ResourceQuantity,
) -> ScalePolicy:
    if not isinstance(raw, dict):
        raise ValueError("policy must be a JSON object")
    defaults = ScalePolicy()
    expected = {item.name for item in fields(defaults)} - _RUNTIME_POLICY_FIELDS
    _require_exact_keys("policy", raw, expected)
    values: dict[str, object] = {}
    integer_minimums = {
        "live_pressure_window_seconds": 1,
        "live_pressure_min_samples": 1,
        "live_pressure_fresh_seconds": 1,
        "create_pressure_window_seconds": 1,
        "create_pressure_min_samples": 1,
        "create_pressure_fresh_seconds": 1,
        "create_target_concurrency_per_node": 1,
        "provisioning_latency_lookback_seconds": 60,
    }
    unit_interval_fields = {
        "provisioning_capacity_weight",
        "stale_provisioning_capacity_weight",
        "target_cpu_utilization",
        "target_memory_utilization",
        "target_storage_queue_utilization",
        "model_wait_capacity_weight",
    }
    bool_fields = {
        item.name
        for item in fields(defaults)
        if isinstance(getattr(defaults, item.name), bool)
    }
    for name in expected:
        value = raw[name]
        default = getattr(defaults, name)
        if name == "warm_resources":
            values[name] = _resource_quantity("policy.warm_resources", value)
        elif name in bool_fields:
            if not isinstance(value, bool):
                raise ValueError(f"policy.{name} must be a boolean")
            values[name] = value
        elif isinstance(default, int):
            values[name] = _require_int(
                f"policy.{name}", value, minimum=integer_minimums.get(name, 0)
            )
        elif isinstance(default, float):
            minimum = 0.01 if name in unit_interval_fields else 0.0
            maximum = 1.0 if name in unit_interval_fields else None
            if name in {
                "provisioning_capacity_weight",
                "stale_provisioning_capacity_weight",
                "model_wait_capacity_weight",
            }:
                minimum = 0.0
            values[name] = _require_float(
                f"policy.{name}", value, minimum=minimum, maximum=maximum
            )
        else:
            raise AssertionError(f"unsupported ScalePolicy field: {name}")
    result = replace(
        defaults,
        **values,
        heartbeat_ttl_seconds=heartbeat_ttl_seconds,
        builder_scale_down_idle_seconds=builder_scale_down_idle_seconds,
        default_node_resources=default_node_resources,
    )
    if result.min_nodes > result.max_nodes:
        raise ValueError("policy.min_nodes cannot exceed policy.max_nodes")
    return result


def _exact_dataclass_values(
    label: str,
    raw: object,
    default: object,
) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ValueError(f"{label} must be a JSON object")
    expected = {item.name for item in fields(default)}
    _require_exact_keys(label, raw, expected)
    return dict(raw)


def _require_exact_keys(label: str, raw: dict[str, Any], expected: set[str]) -> None:
    missing = sorted(expected - set(raw))
    extra = sorted(set(raw) - expected)
    if missing or extra:
        details = []
        if missing:
            details.append("missing: " + ", ".join(missing))
        if extra:
            details.append("unknown: " + ", ".join(extra))
        raise ValueError(f"{label} fields do not match schema ({'; '.join(details)})")


def _require_string(label: str, value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} must be a non-empty string")
    if any(character in value for character in ("\x00", "\r", "\n")):
        raise ValueError(f"{label} contains invalid characters")
    return value.strip()


def _require_optional_string(label: str, value: object) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{label} must be a string")
    if any(character in value for character in ("\x00", "\r", "\n")):
        raise ValueError(f"{label} contains invalid characters")
    return value.strip()


def _require_absolute_path(label: str, value: object) -> str:
    path = _require_string(label, value)
    if not Path(path).is_absolute() or path == "/":
        raise ValueError(f"{label} must be an absolute non-root path")
    return path


def _require_int(
    label: str,
    value: object,
    *,
    minimum: int | None = None,
    maximum: int | None = None,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{label} must be an integer")
    if minimum is not None and value < minimum:
        raise ValueError(f"{label} must be at least {minimum}")
    if maximum is not None and value > maximum:
        raise ValueError(f"{label} must be at most {maximum}")
    return value


def _require_float(
    label: str,
    value: object,
    *,
    minimum: float | None = None,
    maximum: float | None = None,
) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{label} must be a finite number")
    parsed = float(value)
    if not math.isfinite(parsed):
        raise ValueError(f"{label} must be a finite number")
    if minimum is not None and parsed < minimum:
        raise ValueError(f"{label} must be at least {minimum:g}")
    if maximum is not None and parsed > maximum:
        raise ValueError(f"{label} must be at most {maximum:g}")
    return parsed


def _require_port(label: str, value: object) -> int:
    return _require_int(label, value, minimum=1, maximum=65535)


def _string_tuple(label: str, value: object) -> tuple[str, ...]:
    if not isinstance(value, list) or not all(
        isinstance(item, str) and item.strip() for item in value
    ):
        raise ValueError(f"{label} must be an array of non-empty strings")
    return tuple(item.strip() for item in value)


def _require_sha1(label: str, value: object) -> None:
    if (
        not isinstance(value, str)
        or len(value) != 40
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError(f"{label} must be a lowercase 40-character commit")


def _require_repository(label: str, value: object) -> None:
    repository = _require_string(label, value)
    allowed = set("abcdefghijklmnopqrstuvwxyz0123456789._/-")
    if any(character not in allowed for character in repository):
        raise ValueError(f"{label} is invalid")


def _resource_quantity(label: str, raw: object) -> ResourceQuantity:
    if not isinstance(raw, dict):
        raise ValueError(f"{label} must be a JSON object")
    _require_exact_keys(label, raw, {"vcpu", "memory_mb", "disk_mb"})
    return ResourceQuantity(
        vcpu=_require_float(f"{label}.vcpu", raw["vcpu"], minimum=0.0),
        memory_mb=_require_int(f"{label}.memory_mb", raw["memory_mb"], minimum=0),
        disk_mb=_require_int(f"{label}.disk_mb", raw["disk_mb"], minimum=0),
    )
