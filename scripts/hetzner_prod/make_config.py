"""Render the Hetzner production deployment.json from current code defaults.

Sizing is CCX63-specific; policy knobs follow the UCloud production deployment
so both platforms are exercised with the same control-plane behaviour.
"""

import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from ucloud_sandboxes.config import DeploymentConfig  # noqa: E402
from ucloud_sandboxes.gvisor_distribution import GVISOR_COMMIT  # noqa: E402

GIB = 1024

raw = DeploymentConfig.default().to_dict()
bucket = "ucloud-sandboxes-prod-20260926"
s3 = {
    "endpoint": "https://hel1.your-objectstorage.com",
    "bucket": bucket,
    "region": "hel1",
    "access_key_id_env": "HETZNER_S3_ACCESS_KEY",
    "secret_access_key_env": "HETZNER_S3_SECRET_KEY",
}
raw.update({
    "deployment_id": "hetzner-sandboxes-prod",
    "data_root": "/var/lib/ucloud-sandboxes/state",
    "gateway_private_host": "10.42.0.2",
    "provider": {
        "kind": "hetzner",
        "scope_id": "ucloud-sandboxes-production",
        "api_token_env": "HETZNER_API_KEY",
        "network_id": 12539764,
        "location": "hel1",
        "sandbox_server_type": "ccx63",
        "sandbox_image": int(sys.argv[1]) if len(sys.argv) > 1 else "ubuntu-26.04",
        # Builder bundles also carry a kernel-module closure: boot builders
        # from the same pinned-kernel snapshot, on a smaller shape.
        "builder_server_type": "ccx33",
        "builder_image": int(sys.argv[1]) if len(sys.argv) > 1 else "ubuntu-26.04",
        "ssh_user": "root",
        "ssh_key_ids": [116985947],
        "firewall_ids": [11454113],
        "enable_ipv4": False,
        "enable_ipv6": False,
        "enable_private_egress": True,
        "private_dns_servers": ["1.1.1.1", "8.8.8.8"],
    },
    # The registry lives on a Hetzner Volume on the gateway. The production
    # image-preparation storage ceiling is 3000 GB. Object Storage
    # took ~1.3 s per upload and ~320 ms per read against 50 ms and 3 ms on the
    # volume (docs/image-import.md). `hz.py volume sandboxes-registry 1000 ...`,
    # grown online with `hz.py resize-volume sandboxes-registry <GB>` + resize2fs.
    "registry_store": {"kind": "filesystem",
                       "mount_point": "/mnt/ucloud-registry",
                       "data_root": "/mnt/ucloud-registry/docker-registry",
                       "endpoint": "", "bucket": "", "region": "", "prefix": "",
                       "access_key_id_env": "UCLOUD_REGISTRY_S3_ACCESS_KEY_ID",
                       "secret_access_key_env": "UCLOUD_REGISTRY_S3_SECRET_ACCESS_KEY",
                       "force_path_style": False},
    # Split memory backing uses registry checkpoint publication on the Volume.
    "snapshot_store": {"kind": "registry", "endpoint": "", "bucket": "", "region": "",
                       "prefix": "ucloud-sandboxes",
                       "access_key_id_env": "UCLOUD_SNAPSHOT_S3_ACCESS_KEY_ID",
                       "secret_access_key_env": "UCLOUD_SNAPSHOT_S3_SECRET_ACCESS_KEY",
                       "security_token_env": "UCLOUD_SNAPSHOT_S3_SECURITY_TOKEN"},
    "relay_postgres": {
        "schema": "ucloud_shared_prod",
        "dsn_file": "/etc/ucloud-sandboxes/postgres.dsn",
        "max_connections": 16,
        # Each pending request reserves its possible 32 MiB response. 8 GiB
        # capped production near 250 concurrent model calls, before CPU or RAM
        # filled. Allow 512+ agents plus observation/completion overlap and
        # retained results; this is a logical ceiling, not preallocated memory.
        "storage_budget_bytes": 64 * 1024**3,
    },
    # UCloud production knobs (autoscaler/relay behaviour), node cap raised to 6.
    "autoscaler_max_init_per_cycle": 4,
    "autoscaler_init_retry_seconds": 30,
    "autoscaler_init_timeout_seconds": 1800,
    "gateway_max_http_request_threads": 1536,
    "heartbeat_interval_seconds": 20,
    "relay_request_timeout_seconds": 7200,
    "registry_keep_per_repository": 2,
    # Six HTTP processes qualified on four dedicated gateway vCPUs together
    # with placement, relay, PostgreSQL, registry and NAT traffic.
    "gateway_processes": 6,
})
# CCX63: 48 dedicated vCPU, 188,669 MiB visible, 915.5 GiB disk.
disk_gib = 915
sandbox = raw["sandbox"]
sandbox.update({
    "product_id": "ccx63",
    "disk_gb": disk_gib,
    "default_vcpu": 48.0,
    "default_memory_mb": 180 * GIB,  # ~4 GiB host margin below the visible 188,669 MiB
    # Docker image store; sandbox rootfs mount its overlay2 layers directly.
    # Pulled images are kept and evicted least recently used above 85%
    # (docs/image-placement.md). 64 GB filled during an agentic test.
    "docker_quota_image_gb": 256,
    "swap_gb": 0,
    "direct_runsc_commit": GVISOR_COMMIT,
    "direct_network_allow_tcp": ["10.42.0.2:8092"],
    # Relay-only egress (network-policy-relay-v1:default), as on UCloud.
    "network_relays": {"default": "10.42.0.2:8092"},
    "storage_native_cache_gb": 32,
    # Unlimited, as on UCloud: 128 capped the first 540-sandbox run at 128.
    "storage_native_max_ublk_devices": 0,
    "direct_disk_headroom_mb": 24 * GIB,
    "direct_idle_park_seconds": 1.0,
    "direct_split_memory_backing": True,
    "direct_ram_memory_backing": True,
    # Density over wake latency: reflink restore would charge every parked
    # owner the lifetime formula memory claim (docs/disk-density.md).
    "direct_reflink_memory_restore": False,
    "direct_workspace_initial_grant_mb": 512,
})
# Immutable-environment (EROFS) workers read image chunks on demand from the
# volume-backed registry (docs/immutable-environments.md, docs/image-import.md).
IMMUTABLE_WORKERS = True
PRODUCER_KEYS = "/var/lib/ucloud-sandboxes/state/environment-producer"
immutable_environments = {
    "trusted_keys_file": f"{PRODUCER_KEYS}/producers.json",
    "signing_key_file": f"{PRODUCER_KEYS}/producer.pem",
    "repository": "environments",
    "worker_enabled": True,
    "builder_enabled": True,
    # Every top-level path except runtime mounts: imported and task images
    # keep content anywhere (/testbed, /app, /opt/conda).
    "allow_paths": ["*"],
    "cache_bytes": 128 * 1024**3,
    # Layout-2 components keep file mtimes, so Python's .pyc caches stay
    # valid (C2.11). Turn on only after every worker and gateway runs a
    # release that reads layout 2; builders need erofs-utils 1.9+. While
    # false it is not rendered, so the previous release reads the config.
    "preserve_mtimes": False,
    # Off switch for attach-time metadata/trace prefetch. Live backends keep
    # their mode; newly provisioned workers apply a change.
    "prefetch_enabled": True,
}
if IMMUTABLE_WORKERS:
    raw["immutable_environments"] = immutable_environments
    # Workers pull nothing large; half the headroom is the chunk cache.
    sandbox["docker_quota_image_gb"] = 32
    sandbox["direct_disk_headroom_mb"] = 256 * GIB
builder = raw["builder"]
# Offline image preparation can spread across eight builders. Admission still
# bounds execution and finishing on each node; idle nodes stop after 5 minutes.
builder.update({"product_id": "ccx33", "disk_gb": 223, "docker_quota_image_gb": 160, "max_nodes": 8,
                "scale_down_idle_seconds": 300})
# Shared final-layer BuildKit cache survives ephemeral builders. Runtime exports
# are immutable; hourly retention caps referenced bytes/entries, with the
# existing fenced registry GC reclaiming unreferenced blobs later.
builder.update({
    "buildx_cache_ref": f"{raw['gateway_private_host']}:{raw['registry_port']}/ucloud-build-cache:shared",
    "buildx_cache_max_bytes": 32 * 1024**3,
    "buildx_cache_max_entries": 512,
    "buildx_cache_max_age_seconds": 7 * 86400,
    "build_execution_timeout_seconds": 1800,
    # Keep four context/build slots busy while at most two builds finish
    # publication/cleanup. Full finishing capacity applies backpressure.
    # Same-node qualification: docs/benchmarks/build-pipeline-2026-09-29.
    "max_finishing_builds": 2,
})
policy = raw["policy"]
policy.update({
    "min_nodes": 0,
    # MAX_NODES overrides the cap for experiments (for example one-worker tests).
    "max_nodes": int(os.environ.get("MAX_NODES", "3")),
    "max_create_per_cycle": 2,
    "max_provisioning_nodes": 3,
    # Idle workers are released after 5 minutes.
    "scale_down_idle_seconds": 300,
    # Plain Ubuntu 26.04 private-only boots open SSH after ~130 s; the golden
    # snapshot removes that delay. Do not evict nodes before init can finish.
    "unreachable_stop_after_seconds": 900,
    # EROFS creates take ~1.3 s (200 in 92 s on one worker at 16-way). At 8 a
    # burst of 33 creates queued past the 30 s startup rule and bought two
    # temporary workers.
    "create_target_concurrency_per_node": 32,
})
# C2.15 pull-through mirrors on the gateway (docs/managed-registry.md). Turn on
# after the gateway runs a release with them and the Docker Hub token is staged
# as /tmp/ucloud-sandboxes-upstream-mirror-docker.io.env; builders then follow.
UPSTREAM_MIRROR = False
if UPSTREAM_MIRROR:
    raw["upstream_mirror"] = {
        "listen_address": "10.42.0.2",
        "storage_root": "/mnt/ucloud-registry/upstream-mirror",
        "upstreams": [
            {"registry": "docker.io", "port": 5010,
             "credentials_file": "/etc/ucloud-sandboxes/upstream-mirror-docker.io.env"},
            {"registry": "ghcr.io", "port": 5011},
            {"registry": "quay.io", "port": 5012},
            {"registry": "mcr.microsoft.com", "port": 5013},
        ],
        "ttl_hours": 14 * 24,
        "max_bytes": 300 * 1024**3,
    }
config = DeploymentConfig.from_dict(raw)  # validate the exact document
Path(__file__).resolve().parents[2].joinpath("build", "hetzner-prod", "deployment.json").write_text(
    json.dumps(config.to_dict(), indent=2, sort_keys=True) + "\n"
)
print("wrote deployment.json; resources:", config.sandbox.resources)
