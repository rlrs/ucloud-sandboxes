from __future__ import annotations

from dataclasses import asdict, dataclass, field, fields, replace
import json
import math
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from .models import ResourceQuantity, ScalePolicy
from .providers import (
    ProviderConfiguration,
    default_provider_configuration,
    validate_provider_configuration,
)
from .storage_native_publication import DEFAULT_MAX_CONCURRENT_PUBLICATIONS
from .telemetry import TelemetrySettings
from .environment_config import EnvironmentDeploymentConfig


DEPLOYMENT_CONFIG_SCHEMA = 5
DEFAULT_DATA_ROOT = "/work/data/ucloud-sandboxes/state"
DEFAULT_REGISTRY_MOUNT_POINT = "/work/data"
DEFAULT_REGISTRY_DATA_ROOT = "/work/data/ucloud-sandbox-registry/docker-registry"
DEFAULT_REGISTRY_ALIAS = "ucloud-sandbox-registry"
DEFAULT_INSTALL_ROOT = "/work/ucloud-sandboxes"
DEFAULT_DIRECT_RUNSC_COMMIT = "0" * 40
_REGISTRY_GUARD_DEFAULTS = {
    "registry_disk_cleanup_percent": 70.0,
    "registry_disk_target_percent": 60.0,
    "registry_disk_refuse_percent": 90.0,
    "registry_disk_gc_interval_seconds": 1800,
    "registry_reference_grace_seconds": 3600,
    "registry_blob_grace_seconds": 7200,
}
CREATE_PLACEMENTS = ("ranked", "power_of_k")
_RUNTIME_POLICY_FIELDS = {
    "builder_scale_down_idle_seconds",
    "heartbeat_ttl_seconds",
    "default_node_resources",
}


@dataclass(frozen=True)
class SnapshotStoreConfig:
    """Durable authority for detached storage-native snapshots.

    Secrets are deliberately named by environment variable rather than stored
    in deployment.json.  The autoscaler resolves them only while rendering a
    worker's root-only credential file.
    """

    kind: str = "registry"
    endpoint: str = ""
    bucket: str = ""
    region: str = ""
    prefix: str = "ucloud-sandboxes"
    access_key_id_env: str = "UCLOUD_SNAPSHOT_S3_ACCESS_KEY_ID"
    secret_access_key_env: str = "UCLOUD_SNAPSHOT_S3_SECRET_ACCESS_KEY"
    security_token_env: str = "UCLOUD_SNAPSHOT_S3_SECURITY_TOKEN"

    @classmethod
    def from_dict(cls, raw: object) -> "SnapshotStoreConfig":
        result = cls(**_exact_dataclass_values("snapshot_store", raw, cls()))
        if result.kind not in {"registry", "s3"}:
            raise ValueError("snapshot_store.kind must be registry or s3")
        for name in (
            "endpoint",
            "bucket",
            "region",
            "prefix",
            "access_key_id_env",
            "secret_access_key_env",
            "security_token_env",
        ):
            value = getattr(result, name)
            if not isinstance(value, str) or any(
                character in value for character in ("\x00", "\r", "\n")
            ):
                raise ValueError(f"snapshot_store.{name} must be a string")
        for name in (
            "access_key_id_env",
            "secret_access_key_env",
            "security_token_env",
        ):
            value = getattr(result, name)
            if (
                not value
                or not value.replace("_", "A").isalnum()
                or not (value[0].isalpha() or value[0] == "_")
            ):
                raise ValueError(
                    f"snapshot_store.{name} must be an environment variable name"
                )
        if result.kind == "registry":
            if result.endpoint or result.bucket or result.region:
                raise ValueError(
                    "registry snapshot store cannot configure S3 endpoint, bucket, or region"
                )
            return result
        if not result.endpoint.startswith(("http://", "https://")):
            raise ValueError("snapshot_store.endpoint must be HTTP(S) for S3")
        if "/" in result.bucket or not result.bucket.strip():
            raise ValueError("snapshot_store.bucket is invalid")
        if not result.region.strip():
            raise ValueError("snapshot_store.region is required for S3")
        normalized_prefix = result.prefix.strip("/")
        if not normalized_prefix or any(
            part in {"", ".", ".."} for part in normalized_prefix.split("/")
        ):
            raise ValueError("snapshot_store.prefix is invalid")
        endpoint = normalize_s3_endpoint(
            result.endpoint,
            bucket=result.bucket.strip(),
            region=result.region.strip(),
        )
        return replace(
            result,
            endpoint=endpoint,
            bucket=result.bucket.strip(),
            region=result.region.strip(),
            prefix=normalized_prefix,
        )


@dataclass(frozen=True)
class RegistryStoreConfig:
    """Storage driver used by the deployment's private OCI registry.

    Filesystem deployments retain the existing fail-closed mount contract.
    S3 deployments keep credentials out of deployment.json and let Docker
    Distribution store OCI blobs below an isolated bucket prefix.
    """

    kind: str = "filesystem"
    mount_point: str = DEFAULT_REGISTRY_MOUNT_POINT
    data_root: str = DEFAULT_REGISTRY_DATA_ROOT
    endpoint: str = ""
    bucket: str = ""
    region: str = ""
    prefix: str = ""
    access_key_id_env: str = "UCLOUD_REGISTRY_S3_ACCESS_KEY_ID"
    secret_access_key_env: str = "UCLOUD_REGISTRY_S3_SECRET_ACCESS_KEY"
    force_path_style: bool = False

    @classmethod
    def from_dict(cls, raw: object) -> "RegistryStoreConfig":
        result = cls(**_exact_dataclass_values("registry_store", raw, cls()))
        if result.kind not in {"filesystem", "s3"}:
            raise ValueError("registry_store.kind must be filesystem or s3")
        for name in (
            "mount_point",
            "data_root",
            "endpoint",
            "bucket",
            "region",
            "prefix",
            "access_key_id_env",
            "secret_access_key_env",
        ):
            value = getattr(result, name)
            if not isinstance(value, str) or any(
                character in value for character in ("\x00", "\r", "\n")
            ):
                raise ValueError(f"registry_store.{name} must be a string")
        if not isinstance(result.force_path_style, bool):
            raise ValueError("registry_store.force_path_style must be a boolean")
        for name in ("access_key_id_env", "secret_access_key_env"):
            value = getattr(result, name)
            if (
                not value
                or not value.replace("_", "A").isalnum()
                or not (value[0].isalpha() or value[0] == "_")
            ):
                raise ValueError(
                    f"registry_store.{name} must be an environment variable name"
                )
        if result.kind == "filesystem":
            mount_point = _require_absolute_path(
                "registry_store.mount_point", result.mount_point
            )
            data_root = _require_absolute_path(
                "registry_store.data_root", result.data_root
            )
            if not Path(data_root).is_relative_to(Path(mount_point)):
                raise ValueError(
                    "registry_store.data_root must be inside registry_store.mount_point"
                )
            if result.endpoint or result.bucket or result.region or result.prefix:
                raise ValueError(
                    "filesystem registry store cannot configure S3 endpoint, "
                    "bucket, region, or prefix"
                )
            if result.force_path_style:
                raise ValueError(
                    "registry_store.force_path_style requires an S3 registry store"
                )
            return replace(
                result,
                mount_point=mount_point,
                data_root=data_root,
            )
        if result.mount_point or result.data_root:
            raise ValueError(
                "S3 registry store cannot configure filesystem mount_point or data_root"
            )
        if not result.endpoint.startswith(("http://", "https://")):
            raise ValueError("registry_store.endpoint must be HTTP(S) for S3")
        if "/" in result.bucket or not result.bucket.strip():
            raise ValueError("registry_store.bucket is invalid")
        if not result.region.strip():
            raise ValueError("registry_store.region is required for S3")
        normalized_prefix = result.prefix.strip("/")
        if not normalized_prefix or any(
            part in {"", ".", ".."} for part in normalized_prefix.split("/")
        ):
            raise ValueError("registry_store.prefix is invalid")
        endpoint = normalize_s3_endpoint(
            result.endpoint,
            bucket=result.bucket.strip(),
            region=result.region.strip(),
            field_name="registry_store.endpoint",
        )
        return replace(
            result,
            endpoint=endpoint,
            bucket=result.bucket.strip(),
            region=result.region.strip(),
            prefix=normalized_prefix,
        )


def normalize_s3_endpoint(
    endpoint: str,
    *,
    bucket: str,
    region: str,
    field_name: str = "snapshot_store.endpoint",
) -> str:
    """Return an SDK endpoint origin, accepting Hetzner's bucket URL too."""

    parsed = urlsplit(endpoint)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError(f"{field_name} must be an HTTP(S) origin")
    hostname = parsed.hostname.rstrip(".").lower()
    if hostname == f"{bucket}.{region}.your-objectstorage.com".lower():
        hostname = f"{region}.your-objectstorage.com".lower()
    netloc = hostname
    if parsed.port is not None:
        netloc += f":{parsed.port}"
    return urlunsplit((parsed.scheme, netloc, "", "", ""))


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
    network_relays: dict[str, str] = field(default_factory=dict)
    storage_native_repository: str = "ucloud-sandbox-snapshots"
    storage_native_cache_gb: int = 32
    storage_native_pool_low_watermark: int = 2
    storage_native_pool_high_watermark: int = 16
    # Optional operator override. Disk quota and live memory admission provide
    # the default capacity bounds; a fixed device count ignores machine size.
    storage_native_max_ublk_devices: int = 0
    storage_native_max_concurrent_publications: int = (
        DEFAULT_MAX_CONCURRENT_PUBLICATIONS
    )
    direct_disk_headroom_mb: int = 16 * 1024
    direct_max_concurrent_restores: int = 8
    direct_idle_park_seconds: float = 0.0
    direct_split_memory_backing: bool = False
    direct_ram_memory_backing: bool = False
    direct_reflink_memory_restore: bool = False
    # C1.1: idle and model-wait parks pause in place; needs swap_gb > 0.
    direct_pause_tier: bool = False
    # zswap ahead of the pause tier's swap: only for measured compressible heaps.
    direct_pause_tier_zswap: bool = False
    # Node-local model waits (docs/node-local-model-waits.md): nodes pause on a
    # sandbox's outstanding plaintext call to a private relay endpoint and thaw
    # on its answer; the relay sends no park and wakes only an unacknowledged
    # answer. Needs the pause tier.
    direct_local_model_waits: bool = False
    # runtime/noded (Rust) owns the node's port and forwards to the Python agent
    # on a Unix socket (docs/rust-node-daemon-plan.md, phase 0).
    direct_node_front_door: bool = False
    # noded owns the node registry and runs creates; the agent keeps admission
    # (docs/rust-node-daemon-plan.md, phase 1). Requires the front door.
    direct_node_rust_create: bool = False
    # noded runs execs on running, unpaused sandboxes; the agent fences them
    # through flock files (docs/rust-node-daemon-plan.md, phase 2a).
    # Requires the front door.
    direct_node_rust_exec: bool = False
    # noded owns the pause tier: pause and thaw, idle pauses, local waits,
    # paused reclaim and escalation decisions (docs/rust-node-daemon-plan.md,
    # phase 3a). Requires the pause tier and Rust execs.
    direct_node_rust_pause: bool = False
    # Split workspaces start with an XFS filesystem this large and grow online
    # toward disk_mb (docs/disk-density.md). 0 formats full-size workspaces.
    direct_workspace_initial_grant_mb: int = 512
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
        # Optional extension of schema 5; existing deployment files remain valid.
        if isinstance(raw, dict):
            raw = {"network_relays": {}, "direct_split_memory_backing": False,
                   "direct_ram_memory_backing": False,
                   "direct_reflink_memory_restore": False,
                   "direct_pause_tier": False, "direct_pause_tier_zswap": False,
                   "direct_local_model_waits": False, "direct_node_front_door": False,
                   "direct_node_rust_create": False, "direct_node_rust_exec": False,
                   "direct_node_rust_pause": False,
                   "direct_workspace_initial_grant_mb": cls.direct_workspace_initial_grant_mb,
                   **raw}
        values = _exact_dataclass_values("sandbox", raw, cls())
        values["direct_network_allow_tcp"] = _string_tuple(
            "sandbox.direct_network_allow_tcp",
            values["direct_network_allow_tcp"],
        )
        from .relay_network import parse_network_relays

        values["network_relays"] = {
            name: relay.endpoint
            for name, relay in parse_network_relays(values["network_relays"]).items()
        }
        result = cls(**values)
        _require_string("sandbox.product_id", result.product_id)
        _require_int("sandbox.disk_gb", result.disk_gb, minimum=1)
        _require_float("sandbox.default_vcpu", result.default_vcpu, minimum=0.01)
        _require_int("sandbox.default_memory_mb", result.default_memory_mb, minimum=1)
        for name in (
            "docker_quota_image_gb",
            "storage_native_cache_gb",
            "storage_native_pool_high_watermark",
            "storage_native_max_concurrent_publications",
            "direct_disk_headroom_mb",
            "direct_max_concurrent_restores",
            "max_concurrent_image_pulls",
        ):
            _require_int(f"sandbox.{name}", getattr(result, name), minimum=1)
        for name in (
            "swap_gb", "storage_native_pool_low_watermark",
            "storage_native_max_ublk_devices",
        ):
            _require_int(f"sandbox.{name}", getattr(result, name), minimum=0)
        _require_float(
            "sandbox.direct_idle_park_seconds",
            result.direct_idle_park_seconds,
            minimum=0.0,
        )
        if (
            result.storage_native_pool_low_watermark
            > result.storage_native_pool_high_watermark
        ):
            raise ValueError(
                "sandbox.storage_native_pool_low_watermark cannot exceed "
                "sandbox.storage_native_pool_high_watermark"
            )
        if (
            result.storage_native_max_ublk_devices > 0
            and result.storage_native_pool_high_watermark
            > result.storage_native_max_ublk_devices
        ):
            raise ValueError(
                "sandbox.storage_native_pool_high_watermark cannot exceed "
                "sandbox.storage_native_max_ublk_devices"
            )
        if not isinstance(result.direct_split_memory_backing, bool):
            raise ValueError("sandbox.direct_split_memory_backing must be a boolean")
        if not isinstance(result.direct_ram_memory_backing, bool):
            raise ValueError("sandbox.direct_ram_memory_backing must be a boolean")
        if result.direct_ram_memory_backing and not result.direct_split_memory_backing:
            raise ValueError("RAM memory backing requires split memory backing")
        if not isinstance(result.direct_reflink_memory_restore, bool):
            raise ValueError("sandbox.direct_reflink_memory_restore must be a boolean")
        if result.direct_reflink_memory_restore and not result.direct_split_memory_backing:
            raise ValueError("reflink memory restore requires split memory backing")
        if not isinstance(result.direct_pause_tier, bool):
            raise ValueError("sandbox.direct_pause_tier must be a boolean")
        if result.direct_pause_tier and result.swap_gb < 1:
            raise ValueError("the pause tier requires sandbox.swap_gb")
        if not isinstance(result.direct_pause_tier_zswap, bool):
            raise ValueError("sandbox.direct_pause_tier_zswap must be a boolean")
        if result.direct_pause_tier_zswap and not result.direct_pause_tier:
            raise ValueError("sandbox.direct_pause_tier_zswap requires the pause tier")
        if not isinstance(result.direct_local_model_waits, bool):
            raise ValueError("sandbox.direct_local_model_waits must be a boolean")
        if result.direct_local_model_waits and not result.direct_pause_tier:
            raise ValueError("sandbox.direct_local_model_waits requires the pause tier")
        if not isinstance(result.direct_node_front_door, bool):
            raise ValueError("sandbox.direct_node_front_door must be a boolean")
        if not isinstance(result.direct_node_rust_create, bool):
            raise ValueError("sandbox.direct_node_rust_create must be a boolean")
        if result.direct_node_rust_create and not result.direct_node_front_door:
            raise ValueError("sandbox.direct_node_rust_create requires sandbox.direct_node_front_door")
        if not isinstance(result.direct_node_rust_exec, bool):
            raise ValueError("sandbox.direct_node_rust_exec must be a boolean")
        if result.direct_node_rust_exec and not (result.direct_node_front_door and result.direct_node_rust_create):
            # noded serves execs from the node state its create pipeline owns.
            raise ValueError("sandbox.direct_node_rust_exec requires sandbox.direct_node_front_door "
                             "and sandbox.direct_node_rust_create")
        if not isinstance(result.direct_node_rust_pause, bool):
            raise ValueError("sandbox.direct_node_rust_pause must be a boolean")
        if result.direct_node_rust_pause and not (result.direct_pause_tier and result.direct_node_rust_exec):
            raise ValueError("sandbox.direct_node_rust_pause requires sandbox.direct_pause_tier "
                             "and sandbox.direct_node_rust_exec")
        grant = result.direct_workspace_initial_grant_mb
        if isinstance(grant, bool) or not isinstance(grant, int) or (grant and grant < 512):
            raise ValueError("sandbox.direct_workspace_initial_grant_mb must be 0 or at least 512")
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
    buildx_cache_max_bytes: int = 32 * 1024**3
    buildx_cache_max_entries: int = 64
    buildx_cache_max_age_seconds: int = 7 * 86400
    build_execution_timeout_seconds: float = 1800.0
    max_finishing_builds: int = 0

    @classmethod
    def from_dict(cls, raw: object) -> "BuilderPoolConfig":
        if isinstance(raw, dict):
            raw = {"build_execution_timeout_seconds": cls.build_execution_timeout_seconds,
                   "max_finishing_builds": cls.max_finishing_builds, **raw}
        result = cls(**_exact_dataclass_values("builder", raw, cls()))
        _require_float("builder.build_execution_timeout_seconds", result.build_execution_timeout_seconds, minimum=0.01)
        _require_int("builder.max_finishing_builds", result.max_finishing_builds, minimum=0)
        if result.max_finishing_builds > 2:
            raise ValueError("builder.max_finishing_builds must not exceed 2")
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
        for name in ("buildx_cache_max_bytes", "buildx_cache_max_entries", "buildx_cache_max_age_seconds"):
            _require_int(f"builder.{name}", getattr(result, name), minimum=1)
        if result.buildx_cache_ref:
            from .build_cache import RegistryBuildCache
            RegistryBuildCache(result.buildx_cache_ref)
        return result


@dataclass(frozen=True)
class RelayPostgresConfig:
    dsn_file: str
    schema: str = "ucloud_shared"
    max_connections: int = 16
    storage_budget_bytes: int = 64 * 1024**3

    @classmethod
    def from_dict(cls, raw):
        if raw is None:
            return None
        if not isinstance(raw, dict):
            raise ValueError("relay_postgres must be an object or null")
        values = {"schema": "ucloud_shared", "max_connections": 16,
                  "storage_budget_bytes": 64 * 1024**3, **raw}
        _require_exact_keys("relay_postgres", values, {item.name for item in fields(cls)})
        import re
        schema = _require_string("relay_postgres.schema", values["schema"])
        if not re.fullmatch(r"ucloud_shared(?:_[a-z0-9_]+)?", schema) or len(schema) > 63:
            raise ValueError("invalid relay PostgreSQL schema")
        return cls(
            _require_absolute_path("relay_postgres.dsn_file", values["dsn_file"]), schema,
            _require_int("relay_postgres.max_connections", values["max_connections"], minimum=1),
            _require_int("relay_postgres.storage_budget_bytes", values["storage_budget_bytes"], minimum=32 * 1024**2 + 65536),
        )


@dataclass(frozen=True)
class UpstreamMirror:
    registry: str
    port: int
    # Root-only env file with REGISTRY_PROXY_USERNAME/REGISTRY_PROXY_PASSWORD.
    credentials_file: str = ""

    @property
    def remote_url(self) -> str:
        return "https://" + ("registry-1.docker.io" if self.registry == "docker.io" else self.registry)


@dataclass(frozen=True)
class UpstreamMirrorConfig:
    """C2.15: one Distribution proxy-mode instance per upstream registry."""

    listen_address: str
    storage_root: str
    upstreams: tuple[UpstreamMirror, ...]
    ttl_hours: int = 168
    max_bytes: int = 256 * 1024**3

    @classmethod
    def from_dict(cls, raw: object) -> "UpstreamMirrorConfig | None":
        if raw is None:
            return None
        if not isinstance(raw, dict):
            raise ValueError("upstream_mirror must be an object or null")
        values = {"ttl_hours": cls.ttl_hours, "max_bytes": cls.max_bytes, **raw}
        _require_exact_keys("upstream_mirror", values, {item.name for item in fields(cls)})
        import ipaddress
        try:
            address = ipaddress.IPv4Address(_require_string("upstream_mirror.listen_address", values["listen_address"]))
        except ValueError as exc:
            raise ValueError("upstream_mirror.listen_address must be an IPv4 address") from exc
        if not address.is_unspecified and (address.is_loopback or not address.is_private):
            raise ValueError("upstream_mirror.listen_address must be a private address or 0.0.0.0")
        if not isinstance(values["upstreams"], list) or not values["upstreams"]:
            raise ValueError("upstream_mirror.upstreams must be a non-empty array")
        upstreams = []
        for item in values["upstreams"]:
            item = {"credentials_file": "", **item} if isinstance(item, dict) else item
            upstream = UpstreamMirror(**_exact_dataclass_values("upstream_mirror.upstreams[]", item, UpstreamMirror("", 0)))
            registry = _require_string("upstream_mirror.upstreams[].registry", upstream.registry)
            if (registry != registry.lower() or "." not in registry or registry.endswith(".docker.io")
                    or any(c not in "abcdefghijklmnopqrstuvwxyz0123456789.-" for c in registry)):
                raise ValueError(f"upstream_mirror registry {registry!r} must be a lowercase host such as docker.io")
            if upstream.credentials_file:
                _require_absolute_path("upstream_mirror.upstreams[].credentials_file", upstream.credentials_file)
            elif not isinstance(upstream.credentials_file, str):
                raise ValueError("upstream_mirror.upstreams[].credentials_file must be a string")
            upstreams.append(replace(upstream, registry=registry,
                                     port=_require_port("upstream_mirror.upstreams[].port", upstream.port)))
        if len({u.registry for u in upstreams}) != len(upstreams) or len({u.port for u in upstreams}) != len(upstreams):
            raise ValueError("upstream_mirror upstream registries and ports must be distinct")
        return cls(
            listen_address=str(address),
            storage_root=_require_absolute_path("upstream_mirror.storage_root", values["storage_root"]),
            upstreams=tuple(upstreams),
            # Distribution treats a zero TTL as "never expire"; the cache is always bounded.
            ttl_hours=_require_int("upstream_mirror.ttl_hours", values["ttl_hours"], minimum=1),
            max_bytes=_require_int("upstream_mirror.max_bytes", values["max_bytes"], minimum=1024**3),
        )

    def to_dict(self) -> dict[str, Any]:
        return {**asdict(self), "upstreams": [asdict(item) for item in self.upstreams]}

    def storage_dir(self, upstream: UpstreamMirror) -> Path:
        return Path(self.storage_root) / upstream.registry

    def local_url(self, upstream: UpstreamMirror) -> str:
        """How processes on the gateway itself reach a mirror."""

        host = "127.0.0.1" if self.listen_address == "0.0.0.0" else self.listen_address
        return f"http://{host}:{upstream.port}"


@dataclass(frozen=True)
class DeploymentConfig:
    schema: int
    deployment_id: str
    provider: ProviderConfiguration
    data_root: str
    registry_store: RegistryStoreConfig
    gateway_private_host: str
    registry_private_ip: str
    gateway_port: int
    gateway_heartbeat_ttl_seconds: int
    gateway_max_concurrent_sandbox_creates: int
    gateway_max_http_request_threads: int
    # Public gateway processes sharing the port (SO_REUSEPORT) on this host.
    gateway_processes: int
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
    autoscaler_max_pending_delete_retries_per_cycle: int
    autoscaler_max_orphaned_migration_reconciles_per_cycle: int
    autoscaler_max_storage_native_detaches_per_cycle: int
    heartbeat_interval_seconds: int
    telemetry: TelemetrySettings
    snapshot_store: SnapshotStoreConfig
    policy: ScalePolicy
    sandbox: SandboxPoolConfig
    builder: BuilderPoolConfig
    node_package_root: str = DEFAULT_INSTALL_ROOT + "/release"
    relay_postgres: RelayPostgresConfig | None = None
    immutable_environments: EnvironmentDeploymentConfig | None = None
    upstream_mirror: UpstreamMirrorConfig | None = None
    # How a create picks its worker: "ranked" scans the fleet and reserves
    # capacity transactionally; "power_of_k" samples k workers and lets the
    # node's admission decide (C4.3, docs/c43-placement-wiring-plan.md).
    gateway_create_placement: str = "ranked"
    # Filesystem registry disk guard (docs/managed-registry.md): at the cleanup
    # threshold the registry-pressure unit prunes, evicts least-recently-used
    # managed images down to the target, and garbage collects; at the refuse
    # threshold the gateway stops accepting image builds and imports.
    registry_disk_cleanup_percent: float = 70.0
    registry_disk_target_percent: float = 60.0
    registry_disk_refuse_percent: float = 90.0
    # Minimum spacing between pressure-triggered blob sweeps without eviction.
    registry_disk_gc_interval_seconds: int = 1800
    # Unreferenced snapshots, environments, and evictable images younger than
    # this stay.
    registry_reference_grace_seconds: int = 3600
    # The quiescent blob collector keeps unreferenced blobs and links younger than
    # this to preserve uploads between requests; writers are always fenced.
    registry_blob_grace_seconds: int = 7200

    @classmethod
    def default(cls, scope_id: str = "project-id") -> "DeploymentConfig":
        sandbox = SandboxPoolConfig()
        builder = BuilderPoolConfig()
        return cls(
            schema=DEPLOYMENT_CONFIG_SCHEMA,
            deployment_id="production",
            provider=default_provider_configuration(scope_id),
            data_root=DEFAULT_DATA_ROOT,
            registry_store=RegistryStoreConfig(),
            gateway_private_host="sandbox-gateway-production",
            registry_private_ip="",
            gateway_port=8090,
            gateway_heartbeat_ttl_seconds=120,
            gateway_max_concurrent_sandbox_creates=0,
            gateway_max_http_request_threads=1536,
            gateway_processes=1,
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
            autoscaler_max_pending_delete_retries_per_cycle=16,
            autoscaler_max_orphaned_migration_reconciles_per_cycle=128,
            autoscaler_max_storage_native_detaches_per_cycle=2,
            heartbeat_interval_seconds=20,
            telemetry=TelemetrySettings(),
            snapshot_store=SnapshotStoreConfig(),
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
        raw = {"node_package_root": DEFAULT_INSTALL_ROOT + "/release", "relay_postgres": None, "immutable_environments": None, "upstream_mirror": None, "gateway_processes": 1, "gateway_create_placement": "ranked", **_REGISTRY_GUARD_DEFAULTS, **raw}
        expected = {item.name for item in fields(cls)}
        schema = _require_int("schema", raw.get("schema"), minimum=1)
        if schema != DEPLOYMENT_CONFIG_SCHEMA:
            raise ValueError(f"unsupported deployment config schema: {schema}")
        _require_exact_keys("deployment config", raw, expected)
        provider = ProviderConfiguration.from_dict(raw["provider"])
        if provider.kind == "ucloud" and "session_file" in provider.settings:
            raise ValueError("provider ucloud contains unknown fields: session_file")
        validate_provider_configuration(provider)
        sandbox = SandboxPoolConfig.from_dict(raw["sandbox"])
        builder = BuilderPoolConfig.from_dict(raw["builder"])
        registry_store = RegistryStoreConfig.from_dict(raw["registry_store"])
        snapshot_store = SnapshotStoreConfig.from_dict(raw["snapshot_store"])
        telemetry = TelemetrySettings(
            **_exact_dataclass_values(
                "telemetry",
                raw["telemetry"],
                TelemetrySettings(),
            )
        ).validated()
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
            relay_postgres=RelayPostgresConfig.from_dict(raw["relay_postgres"]),
            immutable_environments=EnvironmentDeploymentConfig.from_dict(raw["immutable_environments"]),
            upstream_mirror=UpstreamMirrorConfig.from_dict(raw["upstream_mirror"]),
            provider=provider,
            data_root=_require_absolute_path("data_root", raw["data_root"]),
            node_package_root=_require_absolute_path(
                "node_package_root", raw["node_package_root"]
            ),
            registry_store=registry_store,
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
            gateway_processes=_require_int(
                "gateway_processes", raw["gateway_processes"], minimum=1, maximum=16,
            ),
            gateway_create_placement=_require_choice(
                "gateway_create_placement", raw["gateway_create_placement"], CREATE_PLACEMENTS),
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
            autoscaler_max_pending_delete_retries_per_cycle=_require_int(
                "autoscaler_max_pending_delete_retries_per_cycle",
                raw["autoscaler_max_pending_delete_retries_per_cycle"],
                minimum=0,
            ),
            autoscaler_max_orphaned_migration_reconciles_per_cycle=_require_int(
                "autoscaler_max_orphaned_migration_reconciles_per_cycle",
                raw["autoscaler_max_orphaned_migration_reconciles_per_cycle"],
                minimum=0,
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
            telemetry=telemetry,
            snapshot_store=snapshot_store,
            policy=policy,
            sandbox=sandbox,
            builder=builder,
            registry_disk_cleanup_percent=_require_float(
                "registry_disk_cleanup_percent",
                raw["registry_disk_cleanup_percent"],
                minimum=1.0,
                maximum=100.0,
            ),
            registry_disk_target_percent=_require_float(
                "registry_disk_target_percent",
                raw["registry_disk_target_percent"],
                minimum=1.0,
                maximum=100.0,
            ),
            registry_disk_refuse_percent=_require_float(
                "registry_disk_refuse_percent",
                raw["registry_disk_refuse_percent"],
                minimum=1.0,
                maximum=100.0,
            ),
            registry_disk_gc_interval_seconds=_require_int(
                "registry_disk_gc_interval_seconds",
                raw["registry_disk_gc_interval_seconds"],
                minimum=60,
            ),
            registry_reference_grace_seconds=_require_int(
                "registry_reference_grace_seconds",
                raw["registry_reference_grace_seconds"],
                minimum=300,
            ),
            registry_blob_grace_seconds=_require_int(
                "registry_blob_grace_seconds",
                raw["registry_blob_grace_seconds"],
                minimum=1800,
            ),
        )
        if not (
            result.registry_disk_target_percent
            <= result.registry_disk_cleanup_percent
            <= result.registry_disk_refuse_percent
        ):
            raise ValueError(
                "registry disk thresholds must satisfy target <= cleanup <= refuse"
            )
        if result.sandbox.direct_split_memory_backing and result.snapshot_store.kind != "registry":
            raise ValueError("split memory backing requires registry checkpoint publication; S3 split checkpoints are unsupported")
        if result.gateway_port in {result.relay_port, result.registry_port} or (
            result.relay_port == result.registry_port
        ):
            raise ValueError("gateway, relay, and registry ports must be distinct")
        if result.builder.buildx_cache_ref:
            authority = result.builder.buildx_cache_ref.partition("/")[0]
            if authority != f"{result.registry_endpoint_host}:{result.registry_port}":
                raise ValueError("builder.buildx_cache_ref must use this deployment's private registry")
        if (mirror := result.upstream_mirror) is not None:
            # 5001 is the private registry's loopback debug listener.
            if {u.port for u in mirror.upstreams} & {result.gateway_port, result.relay_port, result.registry_port, 5001}:
                raise ValueError("upstream_mirror ports must differ from the gateway, relay and registry ports")
            root = Path(mirror.storage_root)
            if result.registry_store.kind == "filesystem" and (
                    not root.is_relative_to(result.registry_store.mount_point)
                    or root.is_relative_to(result.registry_data_dir())
                    or result.registry_data_dir().is_relative_to(root)):
                raise ValueError("upstream_mirror.storage_root must be on the registry Volume, "
                                 "outside registry_store.data_root")
            # Docker's own builder mirrors only Docker Hub; other upstreams need
            # the shared docker-container BuildKit builder (buildkitd.toml).
            if any(u.registry != "docker.io" for u in mirror.upstreams) and not result.builder.buildx_cache_ref:
                raise ValueError("upstream_mirror upstreams other than docker.io require builder.buildx_cache_ref")
        return result

    def control_state_file(self) -> Path:
        return self._state_file("control-state.sqlite")

    def image_file(self) -> Path:
        return self._state_file("images.sqlite")

    def routing_file(self) -> Path:
        return self._state_file("routes.sqlite")

    def registry_usage_file(self) -> Path:
        return self._state_file("registry-usage.sqlite")

    def registry_maintenance_state_file(self) -> Path:
        return self._state_file("registry-maintenance.json")

    def metrics_path(self) -> Path:
        return self._state_file("metrics.sqlite")

    def usage_history_file(self) -> Path:
        return self._state_file("usage-history.json")

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
        return Path(self.node_package_root) / "sandbox-node-package.tar.gz"

    def builder_node_package_bundle(self) -> Path:
        return Path(self.node_package_root) / "builder-node-package.tar.gz"

    def registry_data_dir(self) -> Path:
        if self.registry_store.kind != "filesystem":
            raise ValueError("S3 registry store has no local registry data directory")
        return Path(self.registry_store.data_root)

    @property
    def registry_mount_point(self) -> str:
        """Compatibility accessor for filesystem-specific deployment code."""

        return self.registry_store.mount_point

    @property
    def registry_data_root(self) -> str:
        """Compatibility accessor for filesystem-specific deployment code."""

        return self.registry_store.data_root

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

    def upstream_mirror_authorities(self) -> dict[str, str]:
        """Upstream registry -> the mirror authority builders and nodes use."""

        mirror = self.upstream_mirror
        if mirror is None:
            return {}
        host = self.registry_endpoint_host if mirror.listen_address == "0.0.0.0" else mirror.listen_address
        return {u.registry: f"{host}:{u.port}" for u in mirror.upstreams}

    @property
    def heartbeat_url(self) -> str:
        return (
            f"http://{self.gateway_private_host}:{self.gateway_port}/v1/nodes/heartbeat"
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
            **({"relay_postgres": asdict(self.relay_postgres)} if self.relay_postgres is not None else {}),
            **({"immutable_environments": self.immutable_environments.to_dict()} if self.immutable_environments is not None else {}),
            **({"upstream_mirror": self.upstream_mirror.to_dict()} if self.upstream_mirror is not None else {}),
            "provider": provider,
            "data_root": self.data_root,
            **(
                {"node_package_root": self.node_package_root}
                if self.node_package_root != DEFAULT_INSTALL_ROOT + "/release"
                else {}
            ),
            "registry_store": asdict(self.registry_store),
            "gateway_private_host": self.gateway_private_host,
            "registry_private_ip": self.registry_private_ip,
            "gateway_port": self.gateway_port,
            "gateway_heartbeat_ttl_seconds": self.gateway_heartbeat_ttl_seconds,
            "gateway_max_concurrent_sandbox_creates": (
                self.gateway_max_concurrent_sandbox_creates
            ),
            "gateway_max_http_request_threads": self.gateway_max_http_request_threads,
            "gateway_processes": self.gateway_processes,
            # Omitted at its default, so releases without the field still read it.
            **({"gateway_create_placement": self.gateway_create_placement}
               if self.gateway_create_placement != "ranked" else {}),
            "relay_port": self.relay_port,
            "relay_request_timeout_seconds": self.relay_request_timeout_seconds,
            "relay_worker_lease_seconds": self.relay_worker_lease_seconds,
            "relay_completed_request_retention_seconds": (
                self.relay_completed_request_retention_seconds
            ),
            "registry_port": self.registry_port,
            "registry_retention_days": self.registry_retention_days,
            "registry_keep_per_repository": self.registry_keep_per_repository,
            "registry_disk_cleanup_percent": self.registry_disk_cleanup_percent,
            "registry_disk_target_percent": self.registry_disk_target_percent,
            "registry_disk_refuse_percent": self.registry_disk_refuse_percent,
            "registry_disk_gc_interval_seconds": self.registry_disk_gc_interval_seconds,
            "registry_reference_grace_seconds": self.registry_reference_grace_seconds,
            "registry_blob_grace_seconds": self.registry_blob_grace_seconds,
            "autoscaler_interval_seconds": self.autoscaler_interval_seconds,
            "autoscaler_max_init_per_cycle": self.autoscaler_max_init_per_cycle,
            "autoscaler_init_retry_seconds": self.autoscaler_init_retry_seconds,
            "autoscaler_init_timeout_seconds": self.autoscaler_init_timeout_seconds,
            "autoscaler_max_pending_delete_retries_per_cycle": (
                self.autoscaler_max_pending_delete_retries_per_cycle
            ),
            "autoscaler_max_orphaned_migration_reconciles_per_cycle": (
                self.autoscaler_max_orphaned_migration_reconciles_per_cycle
            ),
            "autoscaler_max_storage_native_detaches_per_cycle": (
                self.autoscaler_max_storage_native_detaches_per_cycle
            ),
            "heartbeat_interval_seconds": self.heartbeat_interval_seconds,
            "telemetry": asdict(self.telemetry),
            "snapshot_store": asdict(self.snapshot_store),
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
    # Existing deployments retain local wake placement until explicitly enabled.
    raw = {"parked_wake_consolidation_enabled": False,
           "max_io_psi_full_avg10": defaults.max_io_psi_full_avg10,
           "drain_on_park_enabled": defaults.drain_on_park_enabled,
           "drain_on_park_moves_per_cycle": defaults.drain_on_park_moves_per_cycle,
           **raw}
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
            if name == "max_io_psi_full_avg10":
                maximum = 100.0
            if name in {
                "provisioning_capacity_weight",
                "stale_provisioning_capacity_weight",
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


def _require_choice(label: str, value: object, choices: tuple[str, ...]) -> str:
    if value not in choices:
        raise ValueError(f"{label} must be one of {', '.join(choices)}")
    return str(value)


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
