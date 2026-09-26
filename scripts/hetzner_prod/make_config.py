"""Render the Hetzner production deployment.json from current code defaults.

Sizing is CCX63-specific; policy knobs follow the UCloud production deployment
so both platforms are exercised with the same control-plane behaviour.
"""

import json
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
    "registry_store": {"kind": "s3", "mount_point": "", "data_root": "",
                       "prefix": "prod/oci", "force_path_style": False, **s3},
    # Split memory backing (UCloud production mode) requires registry checkpoint
    # publication; the registry itself is S3-backed.
    "snapshot_store": {"kind": "registry", "endpoint": "", "bucket": "", "region": "",
                       "prefix": "ucloud-sandboxes",
                       "access_key_id_env": "UCLOUD_SNAPSHOT_S3_ACCESS_KEY_ID",
                       "secret_access_key_env": "UCLOUD_SNAPSHOT_S3_SECRET_ACCESS_KEY",
                       "security_token_env": "UCLOUD_SNAPSHOT_S3_SECURITY_TOKEN"},
    "relay_postgres": {
        "schema": "ucloud_shared_prod",
        "dsn_file": "/etc/ucloud-sandboxes/postgres.dsn",
        "max_connections": 16,
        "storage_budget_bytes": 8 * 1024**3,
    },
    # UCloud production knobs (autoscaler/relay behaviour), node cap raised to 6.
    "autoscaler_max_init_per_cycle": 4,
    "autoscaler_init_retry_seconds": 30,
    "autoscaler_init_timeout_seconds": 1800,
    "gateway_max_http_request_threads": 1536,
    "heartbeat_interval_seconds": 20,
    "relay_request_timeout_seconds": 7200,
    "registry_keep_per_repository": 2,
    # Host-local gateway replicas (SO_REUSEPORT), as on UCloud since rc42: one
    # process is GIL-bound at 540 sandboxes per node.
    "gateway_processes": 3,
})
# CCX63: 48 dedicated vCPU, 188,669 MiB visible, 915.5 GiB disk.
disk_gib = 915
sandbox = raw["sandbox"]
sandbox.update({
    "product_id": "ccx63",
    "disk_gb": disk_gib,
    "default_vcpu": 48.0,
    "default_memory_mb": 180 * GIB,  # ~4 GiB host margin below the visible 188,669 MiB
    "docker_quota_image_gb": 64,
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
builder = raw["builder"]
builder.update({"product_id": "ccx33", "disk_gb": 223, "docker_quota_image_gb": 64, "max_nodes": 1})
policy = raw["policy"]
policy.update({
    "min_nodes": 0,
    "max_nodes": 3,
    "max_create_per_cycle": 2,
    "max_provisioning_nodes": 3,
    "scale_down_idle_seconds": 900,
    # Plain Ubuntu 26.04 private-only boots open SSH after ~130 s; the golden
    # snapshot removes that delay. Do not evict nodes before init can finish.
    "unreachable_stop_after_seconds": 900,
    "create_target_concurrency_per_node": 8,
})
config = DeploymentConfig.from_dict(raw)  # validate the exact document
Path(__file__).resolve().parents[2].joinpath("build", "hetzner-prod", "deployment.json").write_text(
    json.dumps(config.to_dict(), indent=2, sort_keys=True) + "\n"
)
print("wrote deployment.json; resources:", config.sandbox.resources)
