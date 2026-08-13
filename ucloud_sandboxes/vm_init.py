from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from functools import lru_cache
from pathlib import Path
from pathlib import PurePosixPath
import re
import shlex
import subprocess
import sys
from typing import Literal

from .deployment import DEFAULT_INIT_VERSION, package_version
from .direct_network import DirectNetworkTcpEgress
from .models import ResourceQuantity


DEFAULT_WORK_DIR = "/work/ucloud-sandboxes"
DEFAULT_NODE_AGENT_HOST = "0.0.0.0"
DEFAULT_NODE_AGENT_PORT = 8090
DEFAULT_SSH_PORT_START = 22000
DEFAULT_SSH_PORT_END = 22999
DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 20
DEFAULT_PACKAGE_SPEC = "ucloud-sandboxes"
DEFAULT_DOCKER_QUOTA_IMAGE_GB = 200
DEFAULT_SWAP_GB = 0
DEFAULT_DOCKER_STORAGE_DIR = "/var/lib/ucloud-sandboxes"
DEFAULT_DOCKER_MTU = 0
DEFAULT_DOCKER_MAX_CONCURRENT_DOWNLOADS = 3
DEFAULT_MAX_CONCURRENT_IMAGE_PULLS = 8
DEFAULT_REMOTE_PACKAGE_DIR = "/var/cache/ucloud-sandboxes/init-packages"
DEFAULT_REMOTE_PACKAGE_FILENAME = "node-package.tar.gz"
STATIC_RUNTIME_RECEIPT_SCHEMA = 1
DEFAULT_DIRECT_RUNSC = "/usr/local/libexec/ucloud-direct-runsc"
DEFAULT_MANAGED_INIT = "/usr/local/libexec/ucloud-sandbox-init"
DEFAULT_STORAGE_NATIVE_BACKEND = "/usr/local/libexec/ucloud-storage-native-backend"
DEFAULT_STORAGE_NATIVE_BACKEND_SOCKET = (
    "/run/ucloud-sandboxes/storage-native/backend.sock"
)
DEFAULT_STORAGE_NATIVE_SERVICE_SOCKET = (
    "/run/ucloud-sandboxes/storage-native/service.sock"
)
DEFAULT_STORAGE_NATIVE_ROOT = "/var/lib/ucloud-sandboxes/storage-native"
DEFAULT_STORAGE_NATIVE_CACHE_ROOT = "/var/lib/ucloud-sandboxes/storage-native-cache"
DEFAULT_STORAGE_NATIVE_BACKEND_CONFIG = (
    "/etc/ucloud-sandboxes/storage-native-backend.json"
)
DEFAULT_STORAGE_NATIVE_RESIZE_BACKEND_CONFIG = (
    "/etc/ucloud-sandboxes/storage-native-resize-backend.json"
)
DEFAULT_STORAGE_NATIVE_CREDENTIAL_FILE = (
    "/etc/ucloud-sandboxes/storage-native-s3-credentials.json"
)
DEFAULT_STORAGE_NATIVE_CREDENTIAL_PROCESS = (
    "/usr/local/libexec/ucloud-storage-native-s3-credentials"
)
DEFAULT_STORAGE_NATIVE_CACHE_GB = 32
DEFAULT_STORAGE_NATIVE_REPOSITORY = "ucloud-sandbox-snapshots"
DEFAULT_STORAGE_NATIVE_POOL_LOW_WATERMARK = 2
DEFAULT_STORAGE_NATIVE_POOL_HIGH_WATERMARK = 16
DEFAULT_STORAGE_NATIVE_COMPACT_AFTER_LAYERS = 8
DEFAULT_STORAGE_NATIVE_COMPACT_AFTER_BYTES = 4 * 1024 * 1024 * 1024
PINNED_STORAGE_NATIVE_AGENTENV_COMMIT = "db1492b7915a408b37f863c9e3a34b2ccb2fb1b0"
DEFAULT_DIRECT_DISK_HEADROOM_MB = 16 * 1024
DEFAULT_DIRECT_MAX_CONCURRENT_RESTORES = 8
SANDBOX_RUNTIME_PACKAGES = (
    "xfsprogs",
    "docker-ce",
    "docker-ce-cli",
    "containerd.io",
)
BUILDER_RUNTIME_PACKAGES = (
    *SANDBOX_RUNTIME_PACKAGES,
    "docker-buildx-plugin",
)
RUNTIME_KERNEL_MODULES = (
    # UCloud project mounts use virtiofs.  The host may have loaded this module
    # before our init script starts without retaining the module on the guest
    # filesystem, which makes the node unable to remount /work after a reboot.
    # Carry it in the same verified closure as the container runtime modules.
    "virtiofs",
    "xfs",
    "overlay",
    "ublk_drv",
    "bridge",
    "br_netfilter",
    "veth",
    "nf_tables",
    "nft_chain_nat",
    "nft_compat",
    "ip_tables",
    "iptable_nat",
    "xt_addrtype",
    "xt_conntrack",
    "xt_MASQUERADE",
)
DEFAULT_SSH_OPTIONS = (
    "-o",
    "BatchMode=yes",
    "-o",
    "ConnectTimeout=10",
    "-o",
    "StrictHostKeyChecking=accept-new",
)


@dataclass(frozen=True)
class VmInitOptions:
    job_id: str
    heartbeat_url: str
    role: Literal["sandbox", "builder"] = "sandbox"
    heartbeat_bearer_token_file: str = ""
    heartbeat_bearer_token: str = ""
    node_control_bearer_token_file: str = ""
    node_control_bearer_token: str = ""
    service_user: str = "ucloud"
    init_authorized_keys: tuple[str, ...] = ()
    node_id: str = ""
    work_dir: str = DEFAULT_WORK_DIR
    package_spec: str = DEFAULT_PACKAGE_SPEC
    package_sha256: str = ""
    node_agent_host: str = DEFAULT_NODE_AGENT_HOST
    node_agent_port: int = DEFAULT_NODE_AGENT_PORT
    deployment_id: str = ""
    telemetry_otlp_endpoint: str = ""
    telemetry_cloud_provider: str = ""
    telemetry_cloud_machine_type: str = ""
    telemetry_trace_sample_ratio: float = 0.1
    telemetry_export_interval_ms: int = 5_000
    telemetry_export_timeout_ms: int = 3_000
    telemetry_max_queue_size: int = 4_096
    telemetry_max_export_batch_size: int = 512
    ssh_port_start: int = DEFAULT_SSH_PORT_START
    ssh_port_end: int = DEFAULT_SSH_PORT_END
    total_resources: ResourceQuantity = ResourceQuantity()
    docker_quota_image_gb: int = DEFAULT_DOCKER_QUOTA_IMAGE_GB
    swap_gb: int = DEFAULT_SWAP_GB
    max_concurrent_image_pulls: int = DEFAULT_MAX_CONCURRENT_IMAGE_PULLS
    docker_insecure_registries: tuple[str, ...] = ()
    host_aliases: tuple[str, ...] = ()
    buildx_cache_ref: str = ""
    direct_runsc_commit: str = ""
    direct_network: str = "none"
    direct_network_allow_tcp: tuple[str, ...] = ()
    storage_native_registry_url: str = ""
    storage_native_repository: str = DEFAULT_STORAGE_NATIVE_REPOSITORY
    storage_native_snapshot_backend: Literal["registry", "s3"] = "registry"
    storage_native_s3_endpoint: str = ""
    storage_native_s3_bucket: str = ""
    storage_native_s3_region: str = ""
    storage_native_s3_prefix: str = ""
    storage_native_s3_access_key_id: str = ""
    storage_native_s3_secret_access_key: str = ""
    storage_native_s3_security_token: str = ""
    storage_native_cache_gb: int = DEFAULT_STORAGE_NATIVE_CACHE_GB
    storage_native_pool_low_watermark: int = DEFAULT_STORAGE_NATIVE_POOL_LOW_WATERMARK
    storage_native_pool_high_watermark: int = DEFAULT_STORAGE_NATIVE_POOL_HIGH_WATERMARK
    direct_disk_headroom_mb: int = DEFAULT_DIRECT_DISK_HEADROOM_MB
    direct_max_concurrent_restores: int = DEFAULT_DIRECT_MAX_CONCURRENT_RESTORES
    heartbeat_interval_seconds: int = DEFAULT_HEARTBEAT_INTERVAL_SECONDS
    labels: dict[str, str] | None = None

    def normalized_node_id(self) -> str:
        return self.node_id or f"ucloud-vm-{self.job_id}"

    def advertised_node_url(self) -> str:
        return f"http://{self.normalized_node_id()}:{self.node_agent_port}"

    def capabilities(self) -> tuple[str, ...]:
        if self.role == "builder":
            return ("image-cache", "image-build", "snapshot")
        return ("sandbox", "image-cache")


@dataclass(frozen=True)
class VmInitRunResult:
    command: tuple[str, ...]
    returncode: int
    phase_durations_ms: tuple[tuple[str, int], ...] = ()
    total_duration_ms: int | None = None


@dataclass(frozen=True)
class VmInitPackageStageResult:
    local_path: Path
    remote_path: str
    command: tuple[str, ...]
    returncode: int
    package_sha256: str = ""
    reused: bool = False


def render_vm_init_script(options: VmInitOptions) -> str:
    validate_vm_init_options(options)
    work_dir = _clean_posix_path(options.work_dir)
    agent_bin = str(PurePosixPath(work_dir) / "bin" / "ucloud-sandboxes")
    storage_agent_bin = str(
        PurePosixPath(work_dir) / "bin" / "ucloud-sandboxes-storage"
    )
    docker_storage_dir = _clean_posix_path(DEFAULT_DOCKER_STORAGE_DIR)
    docker_data_root = str(PurePosixPath(docker_storage_dir) / "docker")
    docker_quota_image = str(PurePosixPath(docker_storage_dir) / "docker-xfs.img")
    docker_quota_root = str(PurePosixPath(docker_storage_dir) / "docker-xfs")
    swap_file = str(PurePosixPath(docker_storage_dir) / "swapfile")
    state_dir = str(PurePosixPath(work_dir) / "state")
    package_cache_dir = str(PurePosixPath(options.package_spec).parent)
    static_runtime_receipt = static_runtime_receipt_for_remote_package(
        options.package_spec,
        role=options.role,
    )
    cached_agent_runtime_dir = str(PurePosixPath(package_cache_dir) / "agent-runtime")
    direct_runsc = DEFAULT_DIRECT_RUNSC
    storage_native_backend = DEFAULT_STORAGE_NATIVE_BACKEND
    storage_native_backend_socket = DEFAULT_STORAGE_NATIVE_BACKEND_SOCKET
    storage_native_service_socket = DEFAULT_STORAGE_NATIVE_SERVICE_SOCKET
    storage_native_root = DEFAULT_STORAGE_NATIVE_ROOT
    storage_native_cache_root = DEFAULT_STORAGE_NATIVE_CACHE_ROOT
    storage_native_backend_config = DEFAULT_STORAGE_NATIVE_BACKEND_CONFIG
    storage_native_resize_backend_config = DEFAULT_STORAGE_NATIVE_RESIZE_BACKEND_CONFIG
    env_file = "/etc/ucloud-sandboxes/node.env"
    node_service = "/etc/systemd/system/ucloud-sandbox-node.service"
    storage_backend_service = (
        "/etc/systemd/system/ucloud-storage-native-backend.service"
    )
    storage_service = "/etc/systemd/system/ucloud-storage-native.service"
    heartbeat_service = "/etc/systemd/system/ucloud-sandbox-heartbeat.service"
    heartbeat_timer = "/etc/systemd/system/ucloud-sandbox-heartbeat.timer"
    authorized_keys_blob = "\n".join(options.init_authorized_keys)
    runtime_role = options.role
    runtime_packages = (
        BUILDER_RUNTIME_PACKAGES
        if options.role == "builder"
        else SANDBOX_RUNTIME_PACKAGES
    )
    runtime_packages_python = repr(list(runtime_packages))
    runtime_kernel_modules_python = repr(list(RUNTIME_KERNEL_MODULES))
    runtime_kernel_modules_shell = " ".join(
        shlex.quote(module) for module in RUNTIME_KERNEL_MODULES
    )
    label_args = " ".join(
        f"--label {shlex.quote(key + '=' + value)}"
        for key, value in sorted((options.labels or {}).items())
    )
    builder_flags = ""
    if options.role == "builder":
        builder_flags += " --buildx-direct-push"
    if options.role == "builder" and options.buildx_cache_ref:
        builder_flags += f" --buildx-cache-ref {shlex.quote(options.buildx_cache_ref)}"
    deployment_flag = " --deployment-id ${UCLOUD_DEPLOYMENT_ID}"
    heartbeat_auth_flag = " --bearer-token-file ${UCLOUD_HEARTBEAT_BEARER_TOKEN_FILE}"
    node_control_auth_flag = (
        " --node-control-bearer-token-file ${UCLOUD_NODE_CONTROL_BEARER_TOKEN_FILE}"
    )
    version_flags = (
        " --agent-version ${UCLOUD_AGENT_VERSION} --init-version ${UCLOUD_INIT_VERSION}"
    )
    storage_publication_args = (
        " --snapshot-backend ${UCLOUD_STORAGE_NATIVE_SNAPSHOT_BACKEND}"
        " --snapshot-registry-url ${UCLOUD_STORAGE_NATIVE_REGISTRY_URL}"
        " --snapshot-repository ${UCLOUD_STORAGE_NATIVE_REPOSITORY}"
    )
    telemetry_args = (
        ' --telemetry-otlp-endpoint "${UCLOUD_TELEMETRY_OTLP_ENDPOINT}"'
        ' --telemetry-cloud-provider "${UCLOUD_TELEMETRY_CLOUD_PROVIDER}"'
        ' --telemetry-cloud-machine-type "${UCLOUD_TELEMETRY_CLOUD_MACHINE_TYPE}"'
        " --telemetry-trace-sample-ratio ${UCLOUD_TELEMETRY_TRACE_SAMPLE_RATIO}"
        " --telemetry-export-interval-ms ${UCLOUD_TELEMETRY_EXPORT_INTERVAL_MS}"
        " --telemetry-export-timeout-ms ${UCLOUD_TELEMETRY_EXPORT_TIMEOUT_MS}"
        " --telemetry-max-queue-size ${UCLOUD_TELEMETRY_MAX_QUEUE_SIZE}"
        " --telemetry-max-export-batch-size "
        "${UCLOUD_TELEMETRY_MAX_EXPORT_BATCH_SIZE}"
    )
    if options.storage_native_snapshot_backend == "s3":
        storage_publication_args += (
            " --snapshot-s3-endpoint ${UCLOUD_STORAGE_NATIVE_S3_ENDPOINT}"
            " --snapshot-s3-bucket ${UCLOUD_STORAGE_NATIVE_S3_BUCKET}"
            " --snapshot-s3-region ${UCLOUD_STORAGE_NATIVE_S3_REGION}"
            " --snapshot-s3-prefix ${UCLOUD_STORAGE_NATIVE_S3_PREFIX}"
            " --snapshot-s3-credential-process "
            "${UCLOUD_STORAGE_NATIVE_S3_CREDENTIAL_PROCESS}"
        )
    if options.role == "sandbox":
        writable_disk_mb = (
            int(options.total_resources.disk_mb)
            - options.docker_quota_image_gb * 1024
            - options.swap_gb * 1024
            - options.storage_native_cache_gb * 1024
            - options.direct_disk_headroom_mb
        )
        if writable_disk_mb < 1:
            raise ValueError(
                "direct runtime has no guaranteed writable disk after Docker, "
                "swap, storage cache, and safety headroom"
            )
        direct_network_allow_flags = "".join(
            " --direct-network-allow-tcp " + shlex.quote(endpoint)
            for endpoint in options.direct_network_allow_tcp
        )
        direct_agent_command = (
            f"{agent_bin} serve-direct-node-agent"
            " --job-id ${UCLOUD_JOB_ID}"
            " --node-id ${UCLOUD_NODE_ID}"
            " --node-url ${UCLOUD_NODE_URL}"
            " --host ${UCLOUD_NODE_AGENT_HOST}"
            " --port ${UCLOUD_NODE_AGENT_PORT}"
            f"{deployment_flag}{version_flags}"
            " --state-root ${UCLOUD_STATE_DIR}/direct-runtime"
            " --image-cache-root ${UCLOUD_DIRECT_IMAGE_CACHE_ROOT}"
            " --image-file ${UCLOUD_STATE_DIR}/images.sqlite"
            " --volume-mount-root ${UCLOUD_STORAGE_NATIVE_MOUNT_ROOT}"
            " --runsc ${UCLOUD_DIRECT_RUNSC}"
            " --runsc-commit ${UCLOUD_DIRECT_RUNSC_COMMIT}"
            " --network ${UCLOUD_DIRECT_NETWORK}"
            f"{direct_network_allow_flags}"
            " --init-binary ${UCLOUD_DIRECT_INIT_BINARY}"
            " --managed-init-binary ${UCLOUD_MANAGED_INIT}"
            " --storage-native-socket ${UCLOUD_STORAGE_NATIVE_SERVICE_SOCKET}"
            " --max-concurrent-restores ${UCLOUD_DIRECT_MAX_CONCURRENT_RESTORES}"
            " --max-concurrent-image-pulls ${UCLOUD_MAX_CONCURRENT_IMAGE_PULLS}"
            " --idle-park-seconds 0"
            " --total-vcpu ${UCLOUD_TOTAL_VCPU}"
            " --total-memory-mb ${UCLOUD_TOTAL_MEMORY_MB}"
            " --total-disk-mb ${UCLOUD_DIRECT_WRITABLE_DISK_MB}"
            f"{node_control_auth_flag}"
            f"{telemetry_args}"
        )
        node_service_user = "root"
        node_service_group = "root"
        node_service_supplementary_groups = ""
        node_service_exec_start_pre = ""
        node_service_wants = (
            "network-online.target docker.service " "ucloud-storage-native.service"
        )
        node_service_after = (
            "network-online.target docker.service " "ucloud-storage-native.service"
        )
        node_service_requires = "docker.service ucloud-storage-native.service"
    else:
        writable_disk_mb = int(options.total_resources.disk_mb)
        direct_agent_command = (
            f"{agent_bin} serve-builder-agent"
            " --job-id ${UCLOUD_JOB_ID}"
            " --node-id ${UCLOUD_NODE_ID}"
            " --node-url ${UCLOUD_NODE_URL}"
            " --host ${UCLOUD_NODE_AGENT_HOST}"
            " --port ${UCLOUD_NODE_AGENT_PORT}"
            f"{deployment_flag}{version_flags}"
            " --state-file ${UCLOUD_STATE_DIR}/builder-node.json"
            " --image-file ${UCLOUD_STATE_DIR}/images.sqlite"
            " --total-vcpu ${UCLOUD_TOTAL_VCPU}"
            " --total-memory-mb ${UCLOUD_TOTAL_MEMORY_MB}"
            " --total-disk-mb ${UCLOUD_TOTAL_DISK_MB}"
            " --max-concurrent-image-pulls ${UCLOUD_MAX_CONCURRENT_IMAGE_PULLS}"
            f"{builder_flags}{node_control_auth_flag}"
            f"{telemetry_args}"
        )
        node_service_user = "$UCLOUD_SERVICE_USER"
        node_service_group = "$UCLOUD_SERVICE_GROUP"
        node_service_supplementary_groups = "SupplementaryGroups=docker"
        node_service_exec_start_pre = ""
        node_service_wants = "network-online.target docker.service"
        node_service_after = "network-online.target docker.service"
        node_service_requires = "docker.service"

    script = f"""#!/usr/bin/env bash
set -euo pipefail

if [ "$(id -u)" -eq 0 ]; then
  SUDO=""
else
  SUDO="sudo"
fi

export DEBIAN_FRONTEND=noninteractive

UCLOUD_JOB_ID={shlex.quote(options.job_id)}
UCLOUD_NODE_ID={shlex.quote(options.normalized_node_id())}
UCLOUD_HEARTBEAT_URL={shlex.quote(options.heartbeat_url)}
UCLOUD_HEARTBEAT_BEARER_TOKEN_FILE={shlex.quote(options.heartbeat_bearer_token_file)}
UCLOUD_HEARTBEAT_BEARER_TOKEN={shlex.quote(options.heartbeat_bearer_token)}
UCLOUD_NODE_CONTROL_BEARER_TOKEN_FILE={shlex.quote(options.node_control_bearer_token_file)}
UCLOUD_NODE_CONTROL_BEARER_TOKEN={shlex.quote(options.node_control_bearer_token)}
UCLOUD_SERVICE_USER={shlex.quote(options.service_user)}
UCLOUD_WORK_DIR={shlex.quote(work_dir)}
UCLOUD_AGENT_BIN={shlex.quote(agent_bin)}
UCLOUD_STORAGE_AGENT_BIN={shlex.quote(storage_agent_bin)}
UCLOUD_STATE_DIR={shlex.quote(state_dir)}
UCLOUD_DOCKER_DATA_ROOT={shlex.quote(docker_data_root)}
UCLOUD_PACKAGE_SPEC={shlex.quote(options.package_spec)}
UCLOUD_PACKAGE_EXPECTED_SHA256={shlex.quote(options.package_sha256)}
UCLOUD_PACKAGE_CACHE_DIR={shlex.quote(package_cache_dir)}
UCLOUD_PACKAGE_CACHE_ROOT="$(dirname "$UCLOUD_PACKAGE_CACHE_DIR")"
UCLOUD_STATIC_RUNTIME_RECEIPT={shlex.quote(static_runtime_receipt)}
UCLOUD_CACHED_AGENT_RUNTIME_DIR={shlex.quote(cached_agent_runtime_dir)}
UCLOUD_NODE_AGENT_HOST={shlex.quote(options.node_agent_host)}
UCLOUD_NODE_AGENT_PORT={options.node_agent_port}
UCLOUD_NODE_URL={shlex.quote(options.advertised_node_url())}
UCLOUD_AGENT_VERSION={shlex.quote(package_version())}
UCLOUD_DEPLOYMENT_ID={shlex.quote(options.deployment_id)}
UCLOUD_TELEMETRY_OTLP_ENDPOINT={shlex.quote(options.telemetry_otlp_endpoint)}
UCLOUD_TELEMETRY_CLOUD_PROVIDER={shlex.quote(options.telemetry_cloud_provider)}
UCLOUD_TELEMETRY_CLOUD_MACHINE_TYPE={shlex.quote(options.telemetry_cloud_machine_type)}
UCLOUD_TELEMETRY_TRACE_SAMPLE_RATIO={options.telemetry_trace_sample_ratio}
UCLOUD_TELEMETRY_EXPORT_INTERVAL_MS={options.telemetry_export_interval_ms}
UCLOUD_TELEMETRY_EXPORT_TIMEOUT_MS={options.telemetry_export_timeout_ms}
UCLOUD_TELEMETRY_MAX_QUEUE_SIZE={options.telemetry_max_queue_size}
UCLOUD_TELEMETRY_MAX_EXPORT_BATCH_SIZE={options.telemetry_max_export_batch_size}
UCLOUD_INIT_VERSION={shlex.quote(DEFAULT_INIT_VERSION)}
UCLOUD_SSH_PORT_START={options.ssh_port_start}
UCLOUD_SSH_PORT_END={options.ssh_port_end}
UCLOUD_TOTAL_VCPU={options.total_resources.vcpu}
UCLOUD_TOTAL_MEMORY_MB={options.total_resources.memory_mb}
UCLOUD_TOTAL_DISK_MB={options.total_resources.disk_mb}
UCLOUD_DOCKER_QUOTA_IMAGE_GB={options.docker_quota_image_gb}
UCLOUD_SWAP_GB={options.swap_gb}
UCLOUD_DOCKER_MTU={DEFAULT_DOCKER_MTU}
UCLOUD_DOCKER_MAX_CONCURRENT_DOWNLOADS={DEFAULT_DOCKER_MAX_CONCURRENT_DOWNLOADS}
UCLOUD_MAX_CONCURRENT_IMAGE_PULLS={options.max_concurrent_image_pulls}
UCLOUD_DOCKER_QUOTA_IMAGE={shlex.quote(docker_quota_image)}
UCLOUD_DOCKER_QUOTA_ROOT={shlex.quote(docker_quota_root)}
UCLOUD_SWAP_FILE={shlex.quote(swap_file)}
UCLOUD_DOCKER_INSECURE_REGISTRIES_JSON={shlex.quote(json.dumps(list(options.docker_insecure_registries)))}
UCLOUD_HOST_ALIASES_JSON={shlex.quote(json.dumps(list(options.host_aliases)))}
UCLOUD_NODE_ROLE={shlex.quote(runtime_role)}
UCLOUD_DIRECT_RUNSC={shlex.quote(direct_runsc)}
UCLOUD_DIRECT_RUNSC_COMMIT={shlex.quote(options.direct_runsc_commit)}
UCLOUD_MANAGED_INIT={shlex.quote(DEFAULT_MANAGED_INIT)}
UCLOUD_DIRECT_NETWORK={shlex.quote(options.direct_network)}
UCLOUD_DIRECT_INIT_BINARY=/usr/libexec/docker-init
UCLOUD_DIRECT_IMAGE_CACHE_ROOT=$UCLOUD_DOCKER_QUOTA_ROOT/ucloud-rootfs-cache
UCLOUD_DIRECT_WRITABLE_DISK_MB={writable_disk_mb}
UCLOUD_DIRECT_MAX_CONCURRENT_RESTORES={options.direct_max_concurrent_restores}
UCLOUD_STORAGE_NATIVE_BACKEND={shlex.quote(storage_native_backend)}
UCLOUD_STORAGE_NATIVE_BACKEND_SOCKET={shlex.quote(storage_native_backend_socket)}
UCLOUD_STORAGE_NATIVE_SERVICE_SOCKET={shlex.quote(storage_native_service_socket)}
UCLOUD_STORAGE_NATIVE_ROOT={shlex.quote(storage_native_root)}
UCLOUD_STORAGE_NATIVE_MOUNT_ROOT=$UCLOUD_STORAGE_NATIVE_ROOT/mounts
UCLOUD_STORAGE_NATIVE_CACHE_ROOT={shlex.quote(storage_native_cache_root)}
UCLOUD_STORAGE_NATIVE_BACKEND_CONFIG={shlex.quote(storage_native_backend_config)}
UCLOUD_STORAGE_NATIVE_RESIZE_BACKEND_CONFIG={shlex.quote(storage_native_resize_backend_config)}
UCLOUD_STORAGE_NATIVE_CACHE_GB={options.storage_native_cache_gb}
UCLOUD_STORAGE_NATIVE_POOL_LOW_WATERMARK={options.storage_native_pool_low_watermark}
UCLOUD_STORAGE_NATIVE_POOL_HIGH_WATERMARK={options.storage_native_pool_high_watermark}
UCLOUD_STORAGE_NATIVE_REGISTRY_URL={shlex.quote(options.storage_native_registry_url)}
UCLOUD_STORAGE_NATIVE_REPOSITORY={shlex.quote(options.storage_native_repository)}
UCLOUD_STORAGE_NATIVE_SNAPSHOT_BACKEND={shlex.quote(options.storage_native_snapshot_backend)}
UCLOUD_STORAGE_NATIVE_S3_ENDPOINT={shlex.quote(options.storage_native_s3_endpoint)}
UCLOUD_STORAGE_NATIVE_S3_BUCKET={shlex.quote(options.storage_native_s3_bucket)}
UCLOUD_STORAGE_NATIVE_S3_REGION={shlex.quote(options.storage_native_s3_region)}
UCLOUD_STORAGE_NATIVE_S3_PREFIX={shlex.quote(options.storage_native_s3_prefix)}
UCLOUD_STORAGE_NATIVE_S3_ACCESS_KEY_ID={shlex.quote(options.storage_native_s3_access_key_id)}
UCLOUD_STORAGE_NATIVE_S3_SECRET_ACCESS_KEY={shlex.quote(options.storage_native_s3_secret_access_key)}
UCLOUD_STORAGE_NATIVE_S3_SECURITY_TOKEN={shlex.quote(options.storage_native_s3_security_token)}
UCLOUD_STORAGE_NATIVE_S3_CREDENTIAL_FILE={shlex.quote(DEFAULT_STORAGE_NATIVE_CREDENTIAL_FILE)}
UCLOUD_STORAGE_NATIVE_S3_CREDENTIAL_PROCESS={shlex.quote(DEFAULT_STORAGE_NATIVE_CREDENTIAL_PROCESS)}
UCLOUD_STORAGE_NATIVE_HARD_CAPACITY_BYTES={writable_disk_mb * 1024 * 1024}
UCLOUD_INIT_AUTHORIZED_KEYS=$(cat <<'UCLOUD_AUTHORIZED_KEYS'
{authorized_keys_blob}
UCLOUD_AUTHORIZED_KEYS
)

echo "Initializing UCloud sandbox node $UCLOUD_NODE_ID for job $UCLOUD_JOB_ID"
UCLOUD_INIT_STARTED_NS="$(date +%s%N)"
UCLOUD_INIT_STARTED_MS=$((10#$UCLOUD_INIT_STARTED_NS / 1000000))
UCLOUD_INIT_PHASE_MS="$UCLOUD_INIT_STARTED_MS"

log_init_phase() {{
  local phase="$1"
  local now_ns now duration total
  now_ns="$(date +%s%N)"
  now=$((10#$now_ns / 1000000))
  duration=$((now - UCLOUD_INIT_PHASE_MS))
  total=$((now - UCLOUD_INIT_STARTED_MS))
  echo "Init phase complete: $phase duration_ms=${{duration}} total_ms=${{total}}"
  echo "UCLOUD_INIT_PHASE name=$phase duration_ms=${{duration}} total_ms=${{total}}"
  UCLOUD_INIT_PHASE_MS="$now"
}}

if ! id "$UCLOUD_SERVICE_USER" >/dev/null 2>&1; then
  $SUDO useradd --create-home --shell /bin/bash "$UCLOUD_SERVICE_USER"
fi
UCLOUD_SERVICE_GROUP="$(id -gn "$UCLOUD_SERVICE_USER")"
UCLOUD_SERVICE_HOME="$(getent passwd "$UCLOUD_SERVICE_USER" | cut -d: -f6)"
if [ -z "$UCLOUD_SERVICE_HOME" ]; then
  echo "Could not determine home for $UCLOUD_SERVICE_USER" >&2
  exit 1
fi
$SUDO mkdir -p "$UCLOUD_WORK_DIR" "$(dirname "$UCLOUD_DOCKER_DATA_ROOT")" /etc/ucloud-sandboxes
$SUDO chown "$UCLOUD_SERVICE_USER:$UCLOUD_SERVICE_GROUP" "$UCLOUD_WORK_DIR"
$SUDO install -d -m 0700 -o "$UCLOUD_SERVICE_USER" -g "$UCLOUD_SERVICE_GROUP" "$UCLOUD_STATE_DIR"

if [ -n "$UCLOUD_INIT_AUTHORIZED_KEYS" ]; then
  $SUDO install -d -m 700 -o "$UCLOUD_SERVICE_USER" -g "$UCLOUD_SERVICE_GROUP" "$UCLOUD_SERVICE_HOME/.ssh"
  $SUDO touch "$UCLOUD_SERVICE_HOME/.ssh/authorized_keys"
  while IFS= read -r key; do
    [ -n "$key" ] || continue
    if ! $SUDO grep -Fx -- "$key" "$UCLOUD_SERVICE_HOME/.ssh/authorized_keys" >/dev/null 2>&1; then
      printf '%s\\n' "$key" | $SUDO tee -a "$UCLOUD_SERVICE_HOME/.ssh/authorized_keys" >/dev/null
    fi
  done <<< "$UCLOUD_INIT_AUTHORIZED_KEYS"
  $SUDO chown "$UCLOUD_SERVICE_USER:$UCLOUD_SERVICE_GROUP" "$UCLOUD_SERVICE_HOME/.ssh/authorized_keys"
  $SUDO chmod 600 "$UCLOUD_SERVICE_HOME/.ssh/authorized_keys"
fi

if [ -n "$UCLOUD_HEARTBEAT_BEARER_TOKEN_FILE" ] && [ -n "$UCLOUD_HEARTBEAT_BEARER_TOKEN" ]; then
  echo "Installing heartbeat bearer token"
  $SUDO install -d -m 700 -o "$UCLOUD_SERVICE_USER" -g "$UCLOUD_SERVICE_GROUP" "$(dirname "$UCLOUD_HEARTBEAT_BEARER_TOKEN_FILE")"
  printf '%s' "$UCLOUD_HEARTBEAT_BEARER_TOKEN" | $SUDO tee "$UCLOUD_HEARTBEAT_BEARER_TOKEN_FILE" >/dev/null
  $SUDO chown "$UCLOUD_SERVICE_USER:$UCLOUD_SERVICE_GROUP" "$UCLOUD_HEARTBEAT_BEARER_TOKEN_FILE"
  $SUDO chmod 600 "$UCLOUD_HEARTBEAT_BEARER_TOKEN_FILE"
fi
if [ -n "$UCLOUD_NODE_CONTROL_BEARER_TOKEN_FILE" ] && [ -n "$UCLOUD_NODE_CONTROL_BEARER_TOKEN" ]; then
  echo "Installing node-control bearer token"
  $SUDO install -d -m 700 -o "$UCLOUD_SERVICE_USER" -g "$UCLOUD_SERVICE_GROUP" "$(dirname "$UCLOUD_NODE_CONTROL_BEARER_TOKEN_FILE")"
  printf '%s' "$UCLOUD_NODE_CONTROL_BEARER_TOKEN" | $SUDO tee "$UCLOUD_NODE_CONTROL_BEARER_TOKEN_FILE" >/dev/null
  $SUDO chown "$UCLOUD_SERVICE_USER:$UCLOUD_SERVICE_GROUP" "$UCLOUD_NODE_CONTROL_BEARER_TOKEN_FILE"
  $SUDO chmod 600 "$UCLOUD_NODE_CONTROL_BEARER_TOKEN_FILE"
fi
log_init_phase "users-and-secrets"

UCLOUD_OS_ID="$(. /etc/os-release && printf '%s' "$ID")"
UCLOUD_OS_VERSION_ID="$(. /etc/os-release && printf '%s' "$VERSION_ID")"
UCLOUD_OS_CODENAME="$(. /etc/os-release && printf '%s' "${{UBUNTU_CODENAME:-${{VERSION_CODENAME:-}}}}")"
UCLOUD_ARCHITECTURE="$(dpkg --print-architecture)"
UCLOUD_PACKAGE_BUNDLE_SHA256="$UCLOUD_PACKAGE_EXPECTED_SHA256"
UCLOUD_PACKAGE_BUNDLE_DIR="$UCLOUD_PACKAGE_CACHE_DIR/extracted"
UCLOUD_STATIC_RUNTIME_READY=0

receipt_value() {{
  local key="$1"
  awk -F= -v key="$key" '$1 == key {{ print substr($0, length(key) + 2); exit }}' \
    "$UCLOUD_STATIC_RUNTIME_RECEIPT"
}}

# The receipt and every referenced artifact are root-owned immutable snapshot
# inputs. A matching receipt lets a clone skip the large artifact verification
# and all static installation work. Any inconsistency falls back to the staged,
# digest-verified bundle below.
if [ -f "$UCLOUD_STATIC_RUNTIME_RECEIPT" ] \
  && [ "$(stat -c '%u:%a' "$UCLOUD_STATIC_RUNTIME_RECEIPT")" = "0:444" ] \
  && [ "$(receipt_value schema)" = {STATIC_RUNTIME_RECEIPT_SCHEMA} ] \
  && [ "$(receipt_value bundle_sha256)" = "$UCLOUD_PACKAGE_EXPECTED_SHA256" ] \
  && [ "$(receipt_value init_version)" = "$UCLOUD_INIT_VERSION" ] \
  && [ "$(receipt_value role)" = "$UCLOUD_NODE_ROLE" ] \
  && [ "$(receipt_value kernel_release)" = "$(uname -r)" ] \
  && [ -d "$UCLOUD_CACHED_AGENT_RUNTIME_DIR/site-packages/ucloud_sandboxes" ] \
  && command -v docker >/dev/null 2>&1 \
  && [ -f "/lib/modules/$(uname -r)/updates/ucloud-sandboxes/.bundle-sha256" ] \
  && [ "$(cat "/lib/modules/$(uname -r)/updates/ucloud-sandboxes/.bundle-sha256")" = "$UCLOUD_PACKAGE_EXPECTED_SHA256" ]; then
  UCLOUD_PREBUILT_AGENT_SHA256="$(receipt_value agent_sha256)"
  if [[ "$UCLOUD_PREBUILT_AGENT_SHA256" =~ ^[0-9a-f]{{64}}$ ]]; then
    if [ "$UCLOUD_NODE_ROLE" != sandbox ] \
      || {{ [ -x "$UCLOUD_DIRECT_RUNSC" ] \
        && [ -x "$UCLOUD_MANAGED_INIT" ] \
        && [ -x "$UCLOUD_STORAGE_NATIVE_BACKEND" ]; }}; then
      UCLOUD_STATIC_RUNTIME_READY=1
      echo "Using snapshot-baked runtime $UCLOUD_PACKAGE_BUNDLE_SHA256"
    fi
  fi
fi

if [ "$UCLOUD_STATIC_RUNTIME_READY" -eq 0 ]; then
  if [ ! -f "$UCLOUD_PACKAGE_SPEC" ]; then
    echo "A staged node package bundle is required" >&2
    exit 1
  fi
  if [ -z "$UCLOUD_PACKAGE_EXPECTED_SHA256" ]; then
    echo "The staged node package bundle requires an expected SHA-256 digest" >&2
    exit 1
  fi
  UCLOUD_ACTUAL_PACKAGE_SHA256="$(sha256sum "$UCLOUD_PACKAGE_SPEC" | awk '{{print $1}}')"
  if [ "$UCLOUD_ACTUAL_PACKAGE_SHA256" != "$UCLOUD_PACKAGE_EXPECTED_SHA256" ]; then
    echo "Node package bundle checksum does not match the staged artifact" >&2
    exit 1
  fi
  if ! tar -tzf "$UCLOUD_PACKAGE_SPEC" package-bundle.json >/dev/null 2>&1; then
    echo "The staged node package bundle is invalid" >&2
    exit 1
  fi
  # Static binaries are shared host paths. Once a different bundle is
  # installed, no older receipt may advertise those paths as ready.
  $SUDO find "$UCLOUD_PACKAGE_CACHE_ROOT" -mindepth 2 -maxdepth 2 \
    -type f -name "runtime-ready-v*-$UCLOUD_NODE_ROLE" -delete 2>/dev/null || true
  UCLOUD_PACKAGE_BUNDLE_TMP="$UCLOUD_PACKAGE_BUNDLE_DIR.tmp.$$"
  $SUDO rm -rf "$UCLOUD_PACKAGE_BUNDLE_TMP"
  $SUDO install -d -m 0755 -o root -g root "$UCLOUD_PACKAGE_CACHE_DIR"
  $SUDO install -d -m 0755 -o root -g root "$UCLOUD_PACKAGE_BUNDLE_TMP"
  $SUDO tar --no-same-owner --no-same-permissions \
    -xzf "$UCLOUD_PACKAGE_SPEC" -C "$UCLOUD_PACKAGE_BUNDLE_TMP"
  $SUDO rm -rf "$UCLOUD_PACKAGE_BUNDLE_DIR"
  $SUDO mv "$UCLOUD_PACKAGE_BUNDLE_TMP" "$UCLOUD_PACKAGE_BUNDLE_DIR"
  echo "Using verified node package bundle $UCLOUD_PACKAGE_BUNDLE_SHA256"
UCLOUD_PACKAGE_METADATA="$(python3 - \
      "$UCLOUD_PACKAGE_BUNDLE_DIR" \
      "$UCLOUD_OS_ID" "$UCLOUD_OS_VERSION_ID" "$UCLOUD_OS_CODENAME" \
      "$UCLOUD_ARCHITECTURE" <<'PY'
import hashlib
import json
import os
import re
from pathlib import Path
import sys

bundle_dir = Path(sys.argv[1])
expected_platform = {{
    "os_id": sys.argv[2],
    "version_id": sys.argv[3],
    "codename": sys.argv[4],
    "architecture": sys.argv[5],
}}
manifest = json.loads(
    (bundle_dir / "package-bundle.json").read_text(encoding="utf-8")
)
if manifest.get("version") != 1:
    raise SystemExit("unsupported node package bundle version")
runtime = manifest.get("runtime")
if not isinstance(runtime, dict) or runtime.get("platform") != expected_platform:
    raise SystemExit("bundled runtime platform does not match this VM")
if runtime.get("role") != {runtime_role!r}:
    raise SystemExit("bundled runtime role does not match this VM")
if runtime.get("packages") != {runtime_packages_python}:
    raise SystemExit("invalid bundled runtime package list")

def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()

def verified_artifact(metadata: dict, file_name: str, label: str) -> str:
    sha256 = str(metadata.get("sha256") or "")
    size = metadata.get("size")
    if not re.fullmatch(r"[0-9a-f]{{64}}", sha256) or not isinstance(size, int) or size <= 0:
        raise SystemExit(f"invalid {{label}} metadata")
    path = bundle_dir / file_name
    if not path.is_file():
        raise SystemExit(f"missing {{label}}")
    if path.stat().st_size != size:
        raise SystemExit(f"{{label}} size mismatch")
    if digest(path) != sha256:
        raise SystemExit(f"{{label}} checksum mismatch")
    return sha256

package_dir = bundle_dir / "runtime" / "debs"
actual_files = {{path.name for path in package_dir.glob("*.deb")}}
declared_files = set()
files = runtime.get("files")
if not isinstance(files, list) or not files:
    raise SystemExit("bundled runtime package set is empty")
for item in files:
    if not isinstance(item, dict):
        raise SystemExit("invalid bundled runtime file")
    filename = str(item.get("name") or "")
    if Path(filename).name != filename or not filename.endswith(".deb"):
        raise SystemExit("invalid bundled runtime filename")
    declared_files.add(filename)
    path = package_dir / filename
    if not path.is_file() or path.stat().st_size != item.get("size"):
        raise SystemExit(f"bundled runtime file size mismatch: {{filename}}")
    if digest(path) != item.get("sha256"):
        raise SystemExit(f"runtime file checksum mismatch: {{filename}}")
if actual_files != declared_files:
    raise SystemExit("bundled runtime file set mismatch")
agent = runtime.get("agent")
if not isinstance(agent, dict):
    raise SystemExit("preassembled node-agent runtime metadata is absent")
if agent.get("file") != "runtime/agent/node-agent-runtime.tar":
    raise SystemExit("invalid preassembled node-agent runtime filename")
if agent.get("python") != f"{{sys.version_info.major}}.{{sys.version_info.minor}}":
    raise SystemExit("preassembled node-agent Python version does not match this VM")
agent_sha256 = verified_artifact(
    agent,
    "runtime/agent/node-agent-runtime.tar",
    "preassembled node-agent runtime",
)
kernel = runtime.get("kernel")
if not isinstance(kernel, dict):
    raise SystemExit("bundled kernel metadata is absent")
kernel_release = os.uname().release
if kernel.get("release") != kernel_release:
    raise SystemExit("bundled kernel module release does not match this VM")
if kernel.get("load") != {runtime_kernel_modules_python}:
    raise SystemExit("bundled kernel module load list does not match this runtime")
module_dir = bundle_dir / "runtime" / "kernel" / kernel_release
actual_modules = {{path.name for path in module_dir.glob("*.ko*")}}
declared_modules = set()
modules = kernel.get("files")
if not isinstance(modules, list) or not modules:
    raise SystemExit("bundled kernel module closure is absent")
for module in modules:
    if not isinstance(module, dict):
        raise SystemExit("bundled kernel module metadata is invalid")
    file_name = str(module.get("name") or "")
    if Path(file_name).name != file_name or not re.fullmatch(
        r"[A-Za-z0-9_.-]+\\.ko(?:\\.(?:gz|xz|zst))?", file_name
    ):
        raise SystemExit("invalid bundled kernel module filename")
    declared_modules.add(file_name)
    module_path = module_dir / file_name
    if not module_path.is_file() or module_path.stat().st_size != module.get("size"):
        raise SystemExit(f"bundled kernel module size mismatch: {{file_name}}")
    if digest(module_path) != module.get("sha256"):
        raise SystemExit(f"kernel module checksum mismatch: {{file_name}}")
if actual_modules != declared_modules:
    raise SystemExit("bundled kernel module file set mismatch")

result = [agent_sha256]
if runtime.get("role") == "sandbox":
    direct = runtime.get("direct_runsc")
    if not isinstance(direct, dict):
        raise SystemExit("direct runtime binary metadata is absent")
    if direct.get("file") != "runtime/direct/runsc":
        raise SystemExit("invalid direct runtime binary filename")
    commit = str(direct.get("commit") or "")
    if not re.fullmatch(r"[0-9a-f]{{40}}", commit):
        raise SystemExit("invalid direct runtime binary metadata")
    if commit != {options.direct_runsc_commit!r}:
        raise SystemExit(
            "bundled direct runsc commit does not match deployment configuration"
        )
    direct_sha256 = verified_artifact(
        direct, "runtime/direct/runsc", "direct runtime binary"
    )

    managed = runtime.get("managed_init")
    if not isinstance(managed, dict):
        raise SystemExit("managed-process init metadata is absent")
    if managed.get("file") != "runtime/direct/ucloud-sandbox-init":
        raise SystemExit("invalid managed-process init filename")
    managed_sha256 = verified_artifact(
        managed,
        "runtime/direct/ucloud-sandbox-init",
        "managed-process init",
    )

    storage = runtime.get("storage_native")
    if not isinstance(storage, dict):
        raise SystemExit("storage-native backend metadata is absent")
    expected_files = {{
        "file": "runtime/storage-native/backend",
        "manifest_file": "runtime/storage-native/build-manifest.json",
        "license_file": "runtime/storage-native/LICENSE",
    }}
    for key, expected in expected_files.items():
        if storage.get(key) != expected:
            raise SystemExit(f"invalid storage-native {{key}}")
    for key in ("sha256", "manifest_sha256", "license_sha256"):
        if not re.fullmatch(r"[0-9a-f]{{64}}", str(storage.get(key) or "")):
            raise SystemExit(f"invalid storage-native {{key}}")
    if storage.get("agentenv_commit") != {PINNED_STORAGE_NATIVE_AGENTENV_COMMIT!r}:
        raise SystemExit("storage-native backend is not the pinned AgentEnv commit")
    host_arch = str(storage.get("host_architecture") or "")
    accepted_arches = {{"amd64": {{"x86_64"}}, "arm64": {{"aarch64"}}}}
    if host_arch not in accepted_arches.get(expected_platform["architecture"], set()):
        raise SystemExit("storage-native backend architecture does not match this VM")
    storage_sha256 = verified_artifact(
        storage, expected_files["file"], "storage-native backend"
    )
    paths = {{key: bundle_dir / value for key, value in expected_files.items()}}
    for key in ("manifest_file", "license_file"):
        if not paths[key].is_file():
            raise SystemExit(f"missing storage-native {{key}}")
    if digest(paths["manifest_file"]) != storage["manifest_sha256"]:
        raise SystemExit("storage-native build manifest checksum mismatch")
    if digest(paths["license_file"]) != storage["license_sha256"]:
        raise SystemExit("storage-native license checksum mismatch")
    build = json.loads(paths["manifest_file"].read_text(encoding="utf-8"))
    patches = build.get("patches")
    expected_patches = [
        "agentenv-streaming-dense-export.patch",
        "agentenv-pooled-delete.patch",
        "agentenv-owner-identity.patch",
    ]
    if (
        build.get("schema") != 3
        or build.get("agentenv_commit") != storage["agentenv_commit"]
        or build.get("artifact_sha256") != storage["sha256"]
        or build.get("host_architecture") != host_arch
        or build.get("license") != "MIT"
        or not isinstance(patches, list)
        or [item.get("name") for item in patches if isinstance(item, dict)]
        != expected_patches
        or not all(
            isinstance(item, dict)
            and re.fullmatch(r"[0-9a-f]{{64}}", str(item.get("sha256") or ""))
            for item in patches
        )
    ):
        raise SystemExit("storage-native build manifest provenance mismatch")
    result.extend((direct_sha256, managed_sha256, storage_sha256))

print("\\t".join(result))
PY
)"
IFS=$'\t' read -r \
  UCLOUD_PREBUILT_AGENT_SHA256 \
  UCLOUD_BUNDLED_DIRECT_RUNSC_SHA256 \
  UCLOUD_BUNDLED_MANAGED_INIT_SHA256 \
  UCLOUD_BUNDLED_STORAGE_NATIVE_BACKEND_SHA256 \
  <<< "$UCLOUD_PACKAGE_METADATA"
UCLOUD_PREBUILT_AGENT_ARCHIVE="$UCLOUD_PACKAGE_BUNDLE_DIR/runtime/agent/node-agent-runtime.tar"
UCLOUD_BUNDLED_KERNEL_MODULE_DIR="$UCLOUD_PACKAGE_BUNDLE_DIR/runtime/kernel/$(uname -r)"
if [ "$UCLOUD_NODE_ROLE" = sandbox ]; then
  UCLOUD_BUNDLED_DIRECT_RUNSC="$UCLOUD_PACKAGE_BUNDLE_DIR/runtime/direct/runsc"
  UCLOUD_BUNDLED_MANAGED_INIT="$UCLOUD_PACKAGE_BUNDLE_DIR/runtime/direct/ucloud-sandbox-init"
  UCLOUD_BUNDLED_STORAGE_NATIVE_BACKEND="$UCLOUD_PACKAGE_BUNDLE_DIR/runtime/storage-native/backend"
  UCLOUD_BUNDLED_STORAGE_NATIVE_BACKEND_MANIFEST="$UCLOUD_PACKAGE_BUNDLE_DIR/runtime/storage-native/build-manifest.json"
  UCLOUD_BUNDLED_STORAGE_NATIVE_BACKEND_LICENSE="$UCLOUD_PACKAGE_BUNDLE_DIR/runtime/storage-native/LICENSE"
fi
echo "Verified pinned Docker/gVisor bundle for $UCLOUD_OS_ID $UCLOUD_OS_VERSION_ID $UCLOUD_ARCHITECTURE"
fi
log_init_phase "package-bundle"

install_bundled_runtime() {{
  local package_dir="$UCLOUD_PACKAGE_BUNDLE_DIR/runtime/debs"
  local package_file package_name install_status offline_apt_root
  local policy_rc_d_created=0
  local -a local_packages=()
  local -a portable_packages=()
  shopt -s nullglob
  local package_files=("$package_dir"/*.deb)
  shopt -u nullglob
  for package_file in "${{package_files[@]}}"; do
    package_name="$(dpkg-deb -f "$package_file" Package)"
    case "$package_name" in
      docker-ce|docker-ce-cli|containerd.io|docker-buildx-plugin|runsc)
        portable_packages+=("$package_file")
        ;;
      *)
        local_packages+=("$package_file")
        ;;
    esac
  done
  install_status=0
  if [ "${{#local_packages[@]}}" -gt 0 ]; then
    # APT 3 on Ubuntu 26.04 rejects even explicitly supplied local .deb files
    # when --no-download is set. Give APT an empty source configuration
    # instead: dependencies may be satisfied only by the verified bundle or
    # packages already installed on the image, with no network fallback.
    offline_apt_root="$(mktemp -d)"
    : > "$offline_apt_root/sources.list"
    mkdir -p "$offline_apt_root/sources.list.d"
    # Docker and containerd are configured and started once below. Prevent
    # support-package scripts from starting services with vendor defaults.
    if [ ! -e /usr/sbin/policy-rc.d ]; then
      printf '#!/bin/sh\nexit 101\n' | $SUDO tee /usr/sbin/policy-rc.d >/dev/null
      $SUDO chmod 0755 /usr/sbin/policy-rc.d
      policy_rc_d_created=1
    fi
    if $SUDO apt-get install --no-install-recommends -y \
      -o DPkg::Lock::Timeout=60 -o Dpkg::Use-Pty=0 \
      -o Dir::Etc::sourcelist="$offline_apt_root/sources.list" \
      -o Dir::Etc::sourceparts="$offline_apt_root/sources.list.d" \
      -o APT::Get::List-Cleanup=0 "${{local_packages[@]}}"; then
      install_status=0
    else
      install_status=$?
    fi
    rm -rf "$offline_apt_root"
  fi
  if [ "$policy_rc_d_created" -eq 1 ]; then
    $SUDO rm -f /usr/sbin/policy-rc.d
  fi
  if [ "$install_status" -ne 0 ]; then
    return "$install_status"
  fi
  # Runtime packages contain self-contained binaries and systemd units. Install
  # their verified payloads exactly; the host package database is not runtime
  # authority. Ubuntu uses merged-/usr directory symlinks such as
  # /lib -> usr/lib. GNU tar otherwise replaces those symlinks when a package
  # contains a compatibility ./lib directory header, leaving kernel modules
  # and boot services split across two trees after the next reboot.
  for package_file in "${{portable_packages[@]}}"; do
    if ! dpkg-deb --fsys-tarfile "$package_file" \
      | $SUDO tar --extract --keep-directory-symlink --file=- --directory=/; then
      return 1
    fi
  done
  # Keep a canonical /usr copy even when containerd.io ships this unit through
  # the compatibility /lib path.
  if [ -f /lib/systemd/system/containerd.service ] \
    && [ ! /lib/systemd/system/containerd.service \
      -ef /usr/lib/systemd/system/containerd.service ]; then
    $SUDO install -m 0644 /lib/systemd/system/containerd.service \
      /usr/lib/systemd/system/containerd.service
  fi
  if ! getent group docker >/dev/null 2>&1; then
    $SUDO groupadd --system docker
  fi
  return 0
}}

if [ "$UCLOUD_STATIC_RUNTIME_READY" -eq 0 ]; then
  echo "Installing Docker, gVisor, and host support from the verified bundle"
  install_bundled_runtime
else
  echo "Docker, gVisor, and host support are already snapshot-baked"
fi
command -v docker >/dev/null 2>&1
log_init_phase "runtime-bundle"

if [ "$UCLOUD_HOST_ALIASES_JSON" != "[]" ]; then
  echo "Installing host aliases"
  export UCLOUD_HOST_ALIASES_JSON
  HOSTS_TMP="$(mktemp)"
  $SUDO cp /etc/hosts "$HOSTS_TMP"
  python3 - <<'PY' "$HOSTS_TMP"
import json
import os
import sys

hosts_path = sys.argv[1]
aliases = json.loads(os.environ.get("UCLOUD_HOST_ALIASES_JSON") or "[]")
marker_prefix = "# ucloud-sandboxes host-alias "
with open(hosts_path, encoding="utf-8") as handle:
    lines = [
        line
        for line in handle.readlines()
        if marker_prefix not in line
    ]
for alias in aliases:
    host, address = alias.split("=", 1)
    lines.append(f"{{address}}\t{{host}}\t{{marker_prefix}}{{host}}\\n")
with open(hosts_path, "w", encoding="utf-8") as handle:
    handle.writelines(lines)
PY
  $SUDO install -m 0644 "$HOSTS_TMP" /etc/hosts
  rm -f "$HOSTS_TMP"
fi
log_init_phase "host-aliases"

UCLOUD_RUNTIME_KERNEL_MODULES=({runtime_kernel_modules_shell})
UCLOUD_KERNEL_MODULE_TARGET="/lib/modules/$(uname -r)/updates/ucloud-sandboxes"
UCLOUD_KERNEL_MODULE_MARKER="$UCLOUD_KERNEL_MODULE_TARGET/.bundle-sha256"
if [ ! -f "$UCLOUD_KERNEL_MODULE_MARKER" ] \
  || [ "$(cat "$UCLOUD_KERNEL_MODULE_MARKER")" != "$UCLOUD_PACKAGE_BUNDLE_SHA256" ]; then
  echo "Installing bundled container-runtime kernel module closure"
  $SUDO rm -rf "$UCLOUD_KERNEL_MODULE_TARGET"
  $SUDO mkdir -p "$UCLOUD_KERNEL_MODULE_TARGET"
  for module_file in "$UCLOUD_BUNDLED_KERNEL_MODULE_DIR"/*.ko*; do
    [ -f "$module_file" ] || {{ echo "Bundled kernel module closure is empty" >&2; exit 1; }}
    $SUDO install -m 0644 "$module_file" "$UCLOUD_KERNEL_MODULE_TARGET/${{module_file##*/}}"
  done
  for module_metadata in modules.order modules.builtin modules.builtin.modinfo; do
    if [ ! -e "/lib/modules/$(uname -r)/$module_metadata" ]; then
      $SUDO touch "/lib/modules/$(uname -r)/$module_metadata"
    fi
  done
  $SUDO depmod -a "$(uname -r)"
  printf '%s\n' "$UCLOUD_PACKAGE_BUNDLE_SHA256" \
    | $SUDO tee "$UCLOUD_KERNEL_MODULE_MARKER" >/dev/null
fi
for module in "${{UCLOUD_RUNTIME_KERNEL_MODULES[@]}}"; do
  $SUDO modprobe "$module"
done
log_init_phase "kernel-modules"


if [ "$UCLOUD_SWAP_GB" -gt 0 ]; then
  echo "Preparing bounded host swap"
  $SUDO mkdir -p "$(dirname "$UCLOUD_SWAP_FILE")"
  UCLOUD_EXPECTED_SWAP_BYTES=$((UCLOUD_SWAP_GB * 1024 * 1024 * 1024))
  if [ -e "$UCLOUD_SWAP_FILE" ]; then
    UCLOUD_ACTUAL_SWAP_BYTES="$($SUDO stat -c %s "$UCLOUD_SWAP_FILE")"
    if [ "$UCLOUD_ACTUAL_SWAP_BYTES" -ne "$UCLOUD_EXPECTED_SWAP_BYTES" ]; then
      echo "Existing swap file has unexpected size; refusing an unsafe live resize" >&2
      exit 1
    fi
  else
    if ! $SUDO fallocate -l "${{UCLOUD_SWAP_GB}}G" "$UCLOUD_SWAP_FILE"; then
      $SUDO dd if=/dev/zero of="$UCLOUD_SWAP_FILE" bs=1M \
        count=$((UCLOUD_SWAP_GB * 1024)) status=progress
    fi
  fi
  $SUDO chmod 0600 "$UCLOUD_SWAP_FILE"
  if [ "$($SUDO blkid -s TYPE -o value "$UCLOUD_SWAP_FILE" 2>/dev/null || true)" != "swap" ]; then
    $SUDO mkswap "$UCLOUD_SWAP_FILE"
  fi
  if ! $SUDO swapon --show=NAME --noheadings | grep -Fx "$UCLOUD_SWAP_FILE" >/dev/null; then
    $SUDO swapon "$UCLOUD_SWAP_FILE"
  fi
  if ! grep -F "$UCLOUD_SWAP_FILE none swap sw 0 0" /etc/fstab >/dev/null 2>&1; then
    echo "$UCLOUD_SWAP_FILE none swap sw 0 0" | $SUDO tee -a /etc/fstab >/dev/null
  fi
  echo "vm.swappiness=60" | $SUDO tee /etc/sysctl.d/90-ucloud-sandbox-swap.conf >/dev/null
  $SUDO sysctl -q -p /etc/sysctl.d/90-ucloud-sandbox-swap.conf
fi
log_init_phase "swap"

if [ "$UCLOUD_DOCKER_QUOTA_IMAGE_GB" -gt 0 ]; then
  echo "Preparing XFS/project-quota Docker data root"
  if ! grep -qw xfs /proc/filesystems; then
    echo "XFS kernel support is unavailable" >&2
    exit 1
  fi
  $SUDO mkdir -p "$UCLOUD_DOCKER_QUOTA_ROOT"
  if [ ! -f "$UCLOUD_DOCKER_QUOTA_IMAGE" ]; then
    $SUDO truncate -s "${{UCLOUD_DOCKER_QUOTA_IMAGE_GB}}G" "$UCLOUD_DOCKER_QUOTA_IMAGE"
  fi
  if ! $SUDO blkid "$UCLOUD_DOCKER_QUOTA_IMAGE" >/dev/null 2>&1; then
    $SUDO mkfs.xfs -f -m reflink=1 "$UCLOUD_DOCKER_QUOTA_IMAGE"
  fi
  if ! findmnt -M "$UCLOUD_DOCKER_QUOTA_ROOT" >/dev/null 2>&1; then
    $SUDO mount -o loop,pquota "$UCLOUD_DOCKER_QUOTA_IMAGE" "$UCLOUD_DOCKER_QUOTA_ROOT"
  fi
  if ! grep -F " $UCLOUD_DOCKER_QUOTA_ROOT xfs " /etc/fstab >/dev/null 2>&1; then
    echo "$UCLOUD_DOCKER_QUOTA_IMAGE $UCLOUD_DOCKER_QUOTA_ROOT xfs loop,pquota,nofail 0 0" | $SUDO tee -a /etc/fstab >/dev/null
  fi
  UCLOUD_DOCKER_DATA_ROOT="$UCLOUD_DOCKER_QUOTA_ROOT"
fi
log_init_phase "docker-storage"

if ! grep -qw overlay /proc/filesystems; then
  echo "overlay filesystem support is unavailable" >&2
  exit 1
fi

if [ "$UCLOUD_NODE_ROLE" = sandbox ]; then
  # The direct daemon is the sole owner of its registry, OCI bundles, and
  # lifecycle journals.
  $SUDO install -d -m 0700 -o root -g root "$UCLOUD_STATE_DIR/direct-runtime"
  $SUDO install -d -m 0700 -o root -g root "$UCLOUD_DIRECT_IMAGE_CACHE_ROOT"
  if [ "$UCLOUD_STATIC_RUNTIME_READY" -eq 0 ]; then
    echo "Installing bundle-verified direct runsc runtime"
    $SUDO install -d -m 0755 -o root -g root "$(dirname "$UCLOUD_DIRECT_RUNSC")"
    $SUDO install -m 0755 -o root -g root "$UCLOUD_BUNDLED_DIRECT_RUNSC" "$UCLOUD_DIRECT_RUNSC"
    printf '%s  %s\n' "$UCLOUD_BUNDLED_DIRECT_RUNSC_SHA256" "$UCLOUD_DIRECT_RUNSC" | sha256sum --check --status -
    "$UCLOUD_DIRECT_RUNSC" --version >/dev/null
    echo "Installing bundle-verified managed-process init"
    $SUDO install -m 0755 -o root -g root "$UCLOUD_BUNDLED_MANAGED_INIT" "$UCLOUD_MANAGED_INIT"
    printf '%s  %s\n' "$UCLOUD_BUNDLED_MANAGED_INIT_SHA256" "$UCLOUD_MANAGED_INIT" | sha256sum --check --status -
    test "$("$UCLOUD_MANAGED_INIT" version)" = managed-primary-v1
    echo "Installing bundle-verified storage-native backend"
    $SUDO install -m 0755 -o root -g root \
      "$UCLOUD_BUNDLED_STORAGE_NATIVE_BACKEND" "$UCLOUD_STORAGE_NATIVE_BACKEND"
    printf '%s  %s\n' \
      "$UCLOUD_BUNDLED_STORAGE_NATIVE_BACKEND_SHA256" \
      "$UCLOUD_STORAGE_NATIVE_BACKEND" | sha256sum --check --status -
    $SUDO install -d -m 0755 -o root -g root \
      /usr/share/doc/ucloud-sandboxes/storage-native
    $SUDO install -m 0644 -o root -g root \
      "$UCLOUD_BUNDLED_STORAGE_NATIVE_BACKEND_MANIFEST" \
      /usr/share/doc/ucloud-sandboxes/storage-native/build-manifest.json
    $SUDO install -m 0644 -o root -g root \
      "$UCLOUD_BUNDLED_STORAGE_NATIVE_BACKEND_LICENSE" \
      /usr/share/doc/ucloud-sandboxes/storage-native/LICENSE
  fi
  $SUDO install -d -m 0700 -o root -g root \
    "$UCLOUD_STORAGE_NATIVE_ROOT" \
    "$UCLOUD_STORAGE_NATIVE_ROOT/runtime" \
    "$UCLOUD_STORAGE_NATIVE_ROOT/mounts" \
    "$UCLOUD_STORAGE_NATIVE_CACHE_ROOT" \
    "$UCLOUD_STORAGE_NATIVE_CACHE_ROOT/remote-blocks" \
    "$UCLOUD_STORAGE_NATIVE_CACHE_ROOT/resize-blocks"
  if [ "$UCLOUD_STORAGE_NATIVE_SNAPSHOT_BACKEND" = s3 ]; then
    export \
      UCLOUD_STORAGE_NATIVE_S3_ACCESS_KEY_ID \
      UCLOUD_STORAGE_NATIVE_S3_SECRET_ACCESS_KEY \
      UCLOUD_STORAGE_NATIVE_S3_SECURITY_TOKEN
    UCLOUD_STORAGE_NATIVE_CREDENTIAL_TMP="$($SUDO mktemp "/etc/ucloud-sandboxes/.storage-native-s3-credentials.XXXXXX")"
    python3 - <<'PY' | $SUDO tee "$UCLOUD_STORAGE_NATIVE_CREDENTIAL_TMP" >/dev/null
import json
import os

payload = {{
    "AccessKeyId": os.environ["UCLOUD_STORAGE_NATIVE_S3_ACCESS_KEY_ID"],
    "SecretAccessKey": os.environ["UCLOUD_STORAGE_NATIVE_S3_SECRET_ACCESS_KEY"],
}}
token = os.environ.get("UCLOUD_STORAGE_NATIVE_S3_SECURITY_TOKEN", "")
if token:
    payload["SecurityToken"] = token
print(json.dumps(payload, ensure_ascii=True, separators=(",", ":")))
PY
    $SUDO chown root:root "$UCLOUD_STORAGE_NATIVE_CREDENTIAL_TMP"
    $SUDO chmod 0600 "$UCLOUD_STORAGE_NATIVE_CREDENTIAL_TMP"
    $SUDO mv -f \
      "$UCLOUD_STORAGE_NATIVE_CREDENTIAL_TMP" \
      "$UCLOUD_STORAGE_NATIVE_S3_CREDENTIAL_FILE"
    $SUDO install -d -m 0755 -o root -g root \
      "$(dirname "$UCLOUD_STORAGE_NATIVE_S3_CREDENTIAL_PROCESS")"
    printf '%s\n' \
      '#!/bin/sh' \
      "exec cat '$UCLOUD_STORAGE_NATIVE_S3_CREDENTIAL_FILE'" \
      | $SUDO tee "$UCLOUD_STORAGE_NATIVE_S3_CREDENTIAL_PROCESS" >/dev/null
    $SUDO chown root:root "$UCLOUD_STORAGE_NATIVE_S3_CREDENTIAL_PROCESS"
    $SUDO chmod 0700 "$UCLOUD_STORAGE_NATIVE_S3_CREDENTIAL_PROCESS"
    unset \
      UCLOUD_STORAGE_NATIVE_S3_ACCESS_KEY_ID \
      UCLOUD_STORAGE_NATIVE_S3_SECRET_ACCESS_KEY \
      UCLOUD_STORAGE_NATIVE_S3_SECURITY_TOKEN
  fi
  UCLOUD_STORAGE_NATIVE_CONFIG_TMP="$($SUDO mktemp "/etc/ucloud-sandboxes/.storage-native-backend.XXXXXX")"
  python3 - \
    "$UCLOUD_STORAGE_NATIVE_CACHE_ROOT/remote-blocks" \
    "$UCLOUD_STORAGE_NATIVE_CACHE_GB" \
    "$UCLOUD_STORAGE_NATIVE_SNAPSHOT_BACKEND" \
    "$UCLOUD_STORAGE_NATIVE_S3_ENDPOINT" \
    "$UCLOUD_STORAGE_NATIVE_S3_REGION" \
    "$UCLOUD_STORAGE_NATIVE_S3_CREDENTIAL_PROCESS" <<'PY' \
    | $SUDO tee "$UCLOUD_STORAGE_NATIVE_CONFIG_TMP" >/dev/null
import json
import sys

config = {{
    "cacheConfig": {{
        "cacheDir": sys.argv[1],
        "cacheSizeGB": int(sys.argv[2]),
        "cacheType": "file",
        "refillSize": 262144,
    }},
    "download": {{"enable": False}},
    "nrIoRings": 4,
    "registryFsVersion": "v2",
}}
if sys.argv[3] == "s3":
    config["ossConfig"] = {{
        "enable": True,
        "accessKeyId": "",
        "secretAccessKey": "",
        "securityToken": "",
        "credentialProcess": sys.argv[6],
        "defaultEndpoint": sys.argv[4],
        "defaultRegion": sys.argv[5],
    }}
print(json.dumps(config, sort_keys=True))
PY
  $SUDO chown root:root "$UCLOUD_STORAGE_NATIVE_CONFIG_TMP"
  $SUDO chmod 0600 "$UCLOUD_STORAGE_NATIVE_CONFIG_TMP"
  $SUDO mv -f \
    "$UCLOUD_STORAGE_NATIVE_CONFIG_TMP" "$UCLOUD_STORAGE_NATIVE_BACKEND_CONFIG"
  # AgentEnv's offline resize helper uses the C++ file cache, whose eviction
  # rules are not compatible with the daemon's shared Rust runtime cache.
  # Keep it in an isolated directory even though background download is off.
  UCLOUD_STORAGE_NATIVE_RESIZE_CONFIG_TMP="$($SUDO mktemp "/etc/ucloud-sandboxes/.storage-native-resize-backend.XXXXXX")"
  python3 - "$UCLOUD_STORAGE_NATIVE_CACHE_ROOT/resize-blocks" <<'PY' \
    | $SUDO tee "$UCLOUD_STORAGE_NATIVE_RESIZE_CONFIG_TMP" >/dev/null
import json
import sys

print(json.dumps({{
    "cacheConfig": {{
        "cacheDir": sys.argv[1],
        "cacheSizeGB": 1,
        "cacheType": "file",
        "refillSize": 262144,
    }},
    "download": {{"enable": False}},
    "nrIoRings": 1,
    "registryFsVersion": "v2",
}}, sort_keys=True))
PY
  $SUDO chown root:root "$UCLOUD_STORAGE_NATIVE_RESIZE_CONFIG_TMP"
  $SUDO chmod 0600 "$UCLOUD_STORAGE_NATIVE_RESIZE_CONFIG_TMP"
  $SUDO mv -f \
    "$UCLOUD_STORAGE_NATIVE_RESIZE_CONFIG_TMP" \
    "$UCLOUD_STORAGE_NATIVE_RESIZE_BACKEND_CONFIG"
  if [ ! -x "$UCLOUD_DIRECT_INIT_BINARY" ]; then
    for direct_init_candidate in \
      /usr/libexec/docker/docker-init \
      /usr/bin/docker-init; do
      if [ -x "$direct_init_candidate" ]; then
        UCLOUD_DIRECT_INIT_BINARY="$direct_init_candidate"
        break
      fi
    done
  fi
  if [ -z "$UCLOUD_DIRECT_INIT_BINARY" ] || [ ! -x "$UCLOUD_DIRECT_INIT_BINARY" ]; then
    echo "Direct runtime requires docker-init" >&2
    exit 1
  fi
fi
log_init_phase "direct-runtime"
detect_routed_mtu() {{
  local iface iface_mtu mtu=""
  while IFS= read -r iface; do
    [ -n "$iface" ] || continue
    [ "$iface" != lo ] || continue
    [ -r "/sys/class/net/$iface/mtu" ] || continue
    iface_mtu="$(cat "/sys/class/net/$iface/mtu")"
    if [[ "$iface_mtu" =~ ^[0-9]+$ ]] \
      && [ "$iface_mtu" -ge 576 ] \
      && {{ [ -z "$mtu" ] || [ "$iface_mtu" -lt "$mtu" ]; }}; then
      mtu="$iface_mtu"
    fi
  done < <(
    ip -o route show table main 2>/dev/null \
      | awk '{{for (i=1; i<=NF; i++) if ($i=="dev") print $(i+1)}}' \
      | sort -u
  )
  if ! [[ "${{mtu:-}}" =~ ^[0-9]+$ ]] || [ "$mtu" -lt 576 ]; then
    mtu=1420
  fi
  printf '%s\\n' "$mtu"
}}

if [ "$UCLOUD_DOCKER_MTU" -eq 0 ]; then
  UCLOUD_DOCKER_MTU="$(detect_routed_mtu)"
fi
export UCLOUD_DOCKER_DATA_ROOT UCLOUD_DOCKER_QUOTA_IMAGE_GB UCLOUD_DOCKER_MTU UCLOUD_DOCKER_MAX_CONCURRENT_DOWNLOADS UCLOUD_DOCKER_INSECURE_REGISTRIES_JSON
echo "Configuring Docker daemon with bridge MTU $UCLOUD_DOCKER_MTU"
$SUDO mkdir -p /etc/docker
DOCKER_DAEMON_JSON="$(mktemp)"
python3 - <<'PY' > "$DOCKER_DAEMON_JSON"
import json
import os

config = {{
    "data-root": os.environ["UCLOUD_DOCKER_DATA_ROOT"],
    "experimental": True,
    "max-concurrent-downloads": int(os.environ["UCLOUD_DOCKER_MAX_CONCURRENT_DOWNLOADS"]),
    "max-concurrent-uploads": 8,
}}
insecure_registries = json.loads(os.environ.get("UCLOUD_DOCKER_INSECURE_REGISTRIES_JSON") or "[]")
if insecure_registries:
    config["insecure-registries"] = insecure_registries
docker_mtu = int(os.environ.get("UCLOUD_DOCKER_MTU") or "0")
if docker_mtu > 0:
    config["mtu"] = docker_mtu
if int(os.environ["UCLOUD_DOCKER_QUOTA_IMAGE_GB"]) > 0:
    config["storage-driver"] = "overlay2"
    config["features"] = {{"containerd-snapshotter": False}}
print(json.dumps(config, indent=2))
PY
if [ ! -f /etc/docker/daemon.json ] || ! cmp -s "$DOCKER_DAEMON_JSON" /etc/docker/daemon.json; then
  $SUDO install -m 0644 "$DOCKER_DAEMON_JSON" /etc/docker/daemon.json
  UCLOUD_DOCKER_RESTART_NEEDED=1
else
  UCLOUD_DOCKER_RESTART_NEEDED=0
fi
rm -f "$DOCKER_DAEMON_JSON"
if [ "$UCLOUD_STATIC_RUNTIME_READY" -eq 0 ]; then
  $SUDO systemctl daemon-reload
fi
$SUDO systemctl enable containerd.service
if ! systemctl is-active --quiet containerd.service; then
  if ! $SUDO systemctl restart containerd.service; then
    $SUDO journalctl -u containerd.service -n 80 --no-pager >&2 || true
    exit 1
  fi
fi
if [ "$UCLOUD_STATIC_RUNTIME_READY" -eq 0 ] \
  || [ "$UCLOUD_DOCKER_RESTART_NEEDED" -eq 1 ]; then
  $SUDO dockerd --validate --config-file /etc/docker/daemon.json
fi
$SUDO systemctl enable docker
if [ "$UCLOUD_DOCKER_RESTART_NEEDED" -eq 1 ] || ! systemctl is-active --quiet docker; then
  if ! $SUDO systemctl restart docker; then
    $SUDO journalctl -u containerd.service -u docker.service -n 80 --no-pager >&2 || true
    exit 1
  fi
else
  echo "Docker daemon already configured and running"
fi
if [ "$UCLOUD_DOCKER_MTU" -gt 0 ] && ip link show docker0 >/dev/null 2>&1; then
  $SUDO ip link set docker0 mtu "$UCLOUD_DOCKER_MTU" || true
fi
$SUDO usermod -aG docker "$UCLOUD_SERVICE_USER"
log_init_phase "docker-daemon"


echo "Activating bundled ucloud-sandboxes runtime"
UCLOUD_AGENT_RUNTIME_DIR="$UCLOUD_CACHED_AGENT_RUNTIME_DIR"
UCLOUD_AGENT_RUNTIME_TMP="$UCLOUD_AGENT_RUNTIME_DIR.tmp.$$"
if [ "$UCLOUD_STATIC_RUNTIME_READY" -eq 0 ]; then
  $SUDO rm -rf "$UCLOUD_AGENT_RUNTIME_TMP"
  $SUDO install -d -m 0755 -o root -g root "$UCLOUD_AGENT_RUNTIME_TMP"
  $SUDO tar --no-same-owner --no-same-permissions \
    -xf "$UCLOUD_PREBUILT_AGENT_ARCHIVE" -C "$UCLOUD_AGENT_RUNTIME_TMP"
  test -d "$UCLOUD_AGENT_RUNTIME_TMP/site-packages/ucloud_sandboxes"
  $SUDO rm -rf "$UCLOUD_AGENT_RUNTIME_DIR"
  $SUDO mv "$UCLOUD_AGENT_RUNTIME_TMP" "$UCLOUD_AGENT_RUNTIME_DIR"
fi
test -d "$UCLOUD_AGENT_RUNTIME_DIR/site-packages/ucloud_sandboxes"
$SUDO install -d -m 0755 -o "$UCLOUD_SERVICE_USER" -g "$UCLOUD_SERVICE_GROUP" "$(dirname "$UCLOUD_AGENT_BIN")"
UCLOUD_AGENT_LAUNCHER="$(mktemp)"
printf '#!/bin/sh\nexec env PYTHONPATH=%q /usr/bin/python3 -m ucloud_sandboxes.cli "$@"\n' \
  "$UCLOUD_AGENT_RUNTIME_DIR/site-packages" > "$UCLOUD_AGENT_LAUNCHER"
$SUDO install -m 0755 -o "$UCLOUD_SERVICE_USER" -g "$UCLOUD_SERVICE_GROUP" "$UCLOUD_AGENT_LAUNCHER" "$UCLOUD_AGENT_BIN"
rm -f "$UCLOUD_AGENT_LAUNCHER"
UCLOUD_STORAGE_AGENT_LAUNCHER="$(mktemp)"
printf '#!/bin/sh\nexec env PYTHONPATH=%q /usr/bin/python3 -m ucloud_sandboxes.storage_native_service "$@"\n' \
  "$UCLOUD_AGENT_RUNTIME_DIR/site-packages" > "$UCLOUD_STORAGE_AGENT_LAUNCHER"
$SUDO install -m 0755 -o root -g root "$UCLOUD_STORAGE_AGENT_LAUNCHER" "$UCLOUD_STORAGE_AGENT_BIN"
rm -f "$UCLOUD_STORAGE_AGENT_LAUNCHER"
if [ "$UCLOUD_STATIC_RUNTIME_READY" -eq 0 ]; then
  UCLOUD_STATIC_RUNTIME_RECEIPT_TMP="$($SUDO mktemp "$UCLOUD_PACKAGE_CACHE_DIR/.runtime-ready.XXXXXX")"
  printf '%s\n' \
    'schema={STATIC_RUNTIME_RECEIPT_SCHEMA}' \
    "bundle_sha256=$UCLOUD_PACKAGE_BUNDLE_SHA256" \
    "init_version=$UCLOUD_INIT_VERSION" \
    "role=$UCLOUD_NODE_ROLE" \
    "kernel_release=$(uname -r)" \
    "agent_sha256=$UCLOUD_PREBUILT_AGENT_SHA256" \
    | $SUDO tee "$UCLOUD_STATIC_RUNTIME_RECEIPT_TMP" >/dev/null
  $SUDO chown root:root "$UCLOUD_STATIC_RUNTIME_RECEIPT_TMP"
  $SUDO chmod 0444 "$UCLOUD_STATIC_RUNTIME_RECEIPT_TMP"
  $SUDO mv -f "$UCLOUD_STATIC_RUNTIME_RECEIPT_TMP" "$UCLOUD_STATIC_RUNTIME_RECEIPT"
  echo "Recorded snapshot-ready runtime $UCLOUD_PACKAGE_BUNDLE_SHA256"
fi
log_init_phase "python-package"

echo "Writing node environment"
$SUDO tee {shlex.quote(env_file)} >/dev/null <<NODE_ENV
UCLOUD_JOB_ID=$UCLOUD_JOB_ID
UCLOUD_NODE_ID=$UCLOUD_NODE_ID
UCLOUD_HEARTBEAT_URL=$UCLOUD_HEARTBEAT_URL
UCLOUD_HEARTBEAT_BEARER_TOKEN_FILE=$UCLOUD_HEARTBEAT_BEARER_TOKEN_FILE
UCLOUD_NODE_CONTROL_BEARER_TOKEN_FILE=$UCLOUD_NODE_CONTROL_BEARER_TOKEN_FILE
UCLOUD_SERVICE_USER=$UCLOUD_SERVICE_USER
UCLOUD_SERVICE_GROUP=$UCLOUD_SERVICE_GROUP
UCLOUD_WORK_DIR=$UCLOUD_WORK_DIR
UCLOUD_STATE_DIR=$UCLOUD_STATE_DIR
UCLOUD_NODE_AGENT_HOST=$UCLOUD_NODE_AGENT_HOST
UCLOUD_NODE_AGENT_PORT=$UCLOUD_NODE_AGENT_PORT
UCLOUD_NODE_URL=$UCLOUD_NODE_URL
UCLOUD_AGENT_VERSION=$UCLOUD_AGENT_VERSION
UCLOUD_DEPLOYMENT_ID=$UCLOUD_DEPLOYMENT_ID
UCLOUD_INIT_VERSION=$UCLOUD_INIT_VERSION
UCLOUD_TELEMETRY_OTLP_ENDPOINT=$UCLOUD_TELEMETRY_OTLP_ENDPOINT
UCLOUD_TELEMETRY_CLOUD_PROVIDER=$UCLOUD_TELEMETRY_CLOUD_PROVIDER
UCLOUD_TELEMETRY_CLOUD_MACHINE_TYPE=$UCLOUD_TELEMETRY_CLOUD_MACHINE_TYPE
UCLOUD_TELEMETRY_TRACE_SAMPLE_RATIO=$UCLOUD_TELEMETRY_TRACE_SAMPLE_RATIO
UCLOUD_TELEMETRY_EXPORT_INTERVAL_MS=$UCLOUD_TELEMETRY_EXPORT_INTERVAL_MS
UCLOUD_TELEMETRY_EXPORT_TIMEOUT_MS=$UCLOUD_TELEMETRY_EXPORT_TIMEOUT_MS
UCLOUD_TELEMETRY_MAX_QUEUE_SIZE=$UCLOUD_TELEMETRY_MAX_QUEUE_SIZE
UCLOUD_TELEMETRY_MAX_EXPORT_BATCH_SIZE=$UCLOUD_TELEMETRY_MAX_EXPORT_BATCH_SIZE
UCLOUD_SSH_PORT_START=$UCLOUD_SSH_PORT_START
UCLOUD_SSH_PORT_END=$UCLOUD_SSH_PORT_END
UCLOUD_TOTAL_VCPU=$UCLOUD_TOTAL_VCPU
UCLOUD_TOTAL_MEMORY_MB=$UCLOUD_TOTAL_MEMORY_MB
UCLOUD_TOTAL_DISK_MB=$UCLOUD_TOTAL_DISK_MB
UCLOUD_DOCKER_DATA_ROOT=$UCLOUD_DOCKER_DATA_ROOT
UCLOUD_DOCKER_QUOTA_IMAGE_GB=$UCLOUD_DOCKER_QUOTA_IMAGE_GB
UCLOUD_DOCKER_MTU=$UCLOUD_DOCKER_MTU
UCLOUD_MAX_CONCURRENT_IMAGE_PULLS=$UCLOUD_MAX_CONCURRENT_IMAGE_PULLS
UCLOUD_DOCKER_QUOTA_IMAGE=$UCLOUD_DOCKER_QUOTA_IMAGE
UCLOUD_DOCKER_QUOTA_ROOT=$UCLOUD_DOCKER_QUOTA_ROOT
UCLOUD_DOCKER_INSECURE_REGISTRIES_JSON=$UCLOUD_DOCKER_INSECURE_REGISTRIES_JSON
UCLOUD_HOST_ALIASES_JSON=$UCLOUD_HOST_ALIASES_JSON
UCLOUD_NODE_ROLE=$UCLOUD_NODE_ROLE
UCLOUD_DIRECT_RUNSC=$UCLOUD_DIRECT_RUNSC
UCLOUD_DIRECT_RUNSC_COMMIT=$UCLOUD_DIRECT_RUNSC_COMMIT
UCLOUD_MANAGED_INIT=$UCLOUD_MANAGED_INIT
UCLOUD_DIRECT_INIT_BINARY=$UCLOUD_DIRECT_INIT_BINARY
UCLOUD_DIRECT_IMAGE_CACHE_ROOT=$UCLOUD_DIRECT_IMAGE_CACHE_ROOT
UCLOUD_DIRECT_WRITABLE_DISK_MB=$UCLOUD_DIRECT_WRITABLE_DISK_MB
UCLOUD_DIRECT_MAX_CONCURRENT_RESTORES=$UCLOUD_DIRECT_MAX_CONCURRENT_RESTORES
UCLOUD_STORAGE_NATIVE_BACKEND=$UCLOUD_STORAGE_NATIVE_BACKEND
UCLOUD_STORAGE_NATIVE_BACKEND_SOCKET=$UCLOUD_STORAGE_NATIVE_BACKEND_SOCKET
UCLOUD_STORAGE_NATIVE_SERVICE_SOCKET=$UCLOUD_STORAGE_NATIVE_SERVICE_SOCKET
UCLOUD_STORAGE_NATIVE_ROOT=$UCLOUD_STORAGE_NATIVE_ROOT
UCLOUD_STORAGE_NATIVE_MOUNT_ROOT=$UCLOUD_STORAGE_NATIVE_MOUNT_ROOT
UCLOUD_STORAGE_NATIVE_CACHE_ROOT=$UCLOUD_STORAGE_NATIVE_CACHE_ROOT
UCLOUD_STORAGE_NATIVE_BACKEND_CONFIG=$UCLOUD_STORAGE_NATIVE_BACKEND_CONFIG
UCLOUD_STORAGE_NATIVE_RESIZE_BACKEND_CONFIG=$UCLOUD_STORAGE_NATIVE_RESIZE_BACKEND_CONFIG
UCLOUD_STORAGE_NATIVE_CACHE_GB=$UCLOUD_STORAGE_NATIVE_CACHE_GB
UCLOUD_STORAGE_NATIVE_POOL_LOW_WATERMARK=$UCLOUD_STORAGE_NATIVE_POOL_LOW_WATERMARK
UCLOUD_STORAGE_NATIVE_POOL_HIGH_WATERMARK=$UCLOUD_STORAGE_NATIVE_POOL_HIGH_WATERMARK
UCLOUD_STORAGE_NATIVE_REGISTRY_URL=$UCLOUD_STORAGE_NATIVE_REGISTRY_URL
UCLOUD_STORAGE_NATIVE_REPOSITORY=$UCLOUD_STORAGE_NATIVE_REPOSITORY
UCLOUD_STORAGE_NATIVE_SNAPSHOT_BACKEND=$UCLOUD_STORAGE_NATIVE_SNAPSHOT_BACKEND
UCLOUD_STORAGE_NATIVE_S3_ENDPOINT=$UCLOUD_STORAGE_NATIVE_S3_ENDPOINT
UCLOUD_STORAGE_NATIVE_S3_BUCKET=$UCLOUD_STORAGE_NATIVE_S3_BUCKET
UCLOUD_STORAGE_NATIVE_S3_REGION=$UCLOUD_STORAGE_NATIVE_S3_REGION
UCLOUD_STORAGE_NATIVE_S3_PREFIX=$UCLOUD_STORAGE_NATIVE_S3_PREFIX
UCLOUD_STORAGE_NATIVE_S3_CREDENTIAL_PROCESS=$UCLOUD_STORAGE_NATIVE_S3_CREDENTIAL_PROCESS
UCLOUD_STORAGE_NATIVE_HARD_CAPACITY_BYTES=$UCLOUD_STORAGE_NATIVE_HARD_CAPACITY_BYTES
NODE_ENV

if [ "$UCLOUD_NODE_ROLE" = sandbox ]; then
  echo "Writing storage-native backend and service units"
  $SUDO tee {shlex.quote(storage_backend_service)} >/dev/null <<STORAGE_BACKEND_SERVICE
[Unit]
Description=UCloud storage-native ublk backend
Wants=network-online.target
After=network-online.target

[Service]
Type=simple
User=root
Group=root
RuntimeDirectory=ucloud-sandboxes/storage-native
RuntimeDirectoryMode=0700
ExecStartPre=/usr/sbin/modprobe ublk_drv
ExecStart=${{UCLOUD_STORAGE_NATIVE_BACKEND}} --socket-path ${{UCLOUD_STORAGE_NATIVE_BACKEND_SOCKET}} --global-config ${{UCLOUD_STORAGE_NATIVE_BACKEND_CONFIG}} --resize-global-config ${{UCLOUD_STORAGE_NATIVE_RESIZE_BACKEND_CONFIG}} --metrics-listen-addr "127.0.0.1:9103" --enable-pool --pool-low-watermark ${{UCLOUD_STORAGE_NATIVE_POOL_LOW_WATERMARK}} --pool-high-watermark ${{UCLOUD_STORAGE_NATIVE_POOL_HIGH_WATERMARK}} --pool-startup-prewarm true
Restart=always
RestartSec=2

[Install]
WantedBy=multi-user.target
STORAGE_BACKEND_SERVICE

  $SUDO tee {shlex.quote(storage_service)} >/dev/null <<STORAGE_SERVICE
[Unit]
Description=UCloud storage-native node service
Wants=network-online.target
After=network-online.target ucloud-storage-native-backend.service
Requires=ucloud-storage-native-backend.service

[Service]
Type=simple
User=root
Group=root
EnvironmentFile={env_file}
WorkingDirectory={work_dir}
ExecStart=${{UCLOUD_STORAGE_AGENT_BIN}} --socket ${{UCLOUD_STORAGE_NATIVE_SERVICE_SOCKET}} --backend-socket ${{UCLOUD_STORAGE_NATIVE_BACKEND_SOCKET}} --backend-global-config ${{UCLOUD_STORAGE_NATIVE_BACKEND_CONFIG}} --journal ${{UCLOUD_STORAGE_NATIVE_ROOT}}/journal.sqlite --runtime-root ${{UCLOUD_STORAGE_NATIVE_ROOT}}/runtime --mount-root ${{UCLOUD_STORAGE_NATIVE_ROOT}}/mounts --hard-capacity-bytes ${{UCLOUD_STORAGE_NATIVE_HARD_CAPACITY_BYTES}}{storage_publication_args} --snapshot-compact-after-layers {DEFAULT_STORAGE_NATIVE_COMPACT_AFTER_LAYERS} --snapshot-compact-after-bytes {DEFAULT_STORAGE_NATIVE_COMPACT_AFTER_BYTES} --device-pool-enabled --device-pool-low-watermark ${{UCLOUD_STORAGE_NATIVE_POOL_LOW_WATERMARK}} --device-pool-high-watermark ${{UCLOUD_STORAGE_NATIVE_POOL_HIGH_WATERMARK}}{telemetry_args} --deployment-id ${{UCLOUD_DEPLOYMENT_ID}} --node-id ${{UCLOUD_NODE_ID}}
Restart=always
RestartSec=2

[Install]
WantedBy=multi-user.target
STORAGE_SERVICE
fi

echo "Writing node-agent systemd service"
$SUDO tee {shlex.quote(node_service)} >/dev/null <<NODE_SERVICE
[Unit]
Description=UCloud sandbox node agent
Wants={node_service_wants}
After={node_service_after}
Requires={node_service_requires}

[Service]
Type=simple
User={node_service_user}
Group={node_service_group}
{node_service_supplementary_groups}
EnvironmentFile={env_file}
WorkingDirectory={work_dir}
{node_service_exec_start_pre}
ExecStart={direct_agent_command}
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
NODE_SERVICE

echo "Writing heartbeat systemd service and timer"
$SUDO tee {shlex.quote(heartbeat_service)} >/dev/null <<HEARTBEAT_SERVICE
[Unit]
Description=UCloud sandbox node heartbeat
After=network-online.target ucloud-sandbox-node.service

[Service]
Type=oneshot
User=$UCLOUD_SERVICE_USER
Group=$UCLOUD_SERVICE_GROUP
SupplementaryGroups=docker
EnvironmentFile={env_file}
WorkingDirectory={work_dir}
ExecStart={agent_bin} agent-heartbeat --from-node-agent-url http://127.0.0.1:${{UCLOUD_NODE_AGENT_PORT}} --post-url ${{UCLOUD_HEARTBEAT_URL}}{deployment_flag}{node_control_auth_flag} {heartbeat_auth_flag} {label_args}
HEARTBEAT_SERVICE

$SUDO tee {shlex.quote(heartbeat_timer)} >/dev/null <<HEARTBEAT_TIMER
[Unit]
Description=Run UCloud sandbox node heartbeat periodically

[Timer]
OnBootSec=10s
OnUnitActiveSec={options.heartbeat_interval_seconds}s
AccuracySec=5s
Persistent=true
Unit=ucloud-sandbox-heartbeat.service

[Install]
WantedBy=timers.target
HEARTBEAT_TIMER

$SUDO systemctl daemon-reload
if [ "$UCLOUD_NODE_ROLE" = sandbox ]; then
  $SUDO systemctl enable ucloud-storage-native-backend.service
  $SUDO systemctl enable ucloud-storage-native.service
  $SUDO systemctl restart ucloud-storage-native-backend.service
  $SUDO systemctl restart ucloud-storage-native.service
fi
$SUDO systemctl enable ucloud-sandbox-node.service
$SUDO systemctl restart ucloud-sandbox-node.service
NODE_AGENT_READY=0
for _ in $(seq 1 100); do
  if curl -fsS "http://127.0.0.1:${{UCLOUD_NODE_AGENT_PORT}}/healthz" >/dev/null; then
    NODE_AGENT_READY=1
    break
  fi
  sleep 0.1
done
if [ "$NODE_AGENT_READY" -ne 1 ]; then
  $SUDO systemctl status ucloud-sandbox-node.service --no-pager -l || true
  echo "Node agent did not become healthy after service start" >&2
  exit 1
fi
$SUDO systemctl enable --now ucloud-sandbox-heartbeat.timer
$SUDO systemctl start ucloud-sandbox-heartbeat.service

echo "UCloud sandbox node init complete. Waiting for heartbeat readiness in the control plane."
log_init_phase "systemd-services"
"""
    return script


def validate_vm_init_options(options: VmInitOptions) -> None:
    if not options.job_id:
        raise ValueError("job id is required.")
    if not options.heartbeat_url:
        raise ValueError("heartbeat url is required.")
    if not options.package_spec:
        raise ValueError("package spec is required.")
    if options.node_agent_port < 1 or options.node_agent_port > 65535:
        raise ValueError("node agent port must be in [1, 65535].")
    if options.ssh_port_start < 1 or options.ssh_port_start > 65535:
        raise ValueError("ssh port start must be in [1, 65535].")
    if options.ssh_port_end < 1 or options.ssh_port_end > 65535:
        raise ValueError("ssh port end must be in [1, 65535].")
    if options.ssh_port_start > options.ssh_port_end:
        raise ValueError("ssh port start must be <= ssh port end.")
    if options.heartbeat_interval_seconds < 1:
        raise ValueError("heartbeat interval must be positive.")
    if options.role not in {"sandbox", "builder"}:
        raise ValueError("node role must be sandbox or builder")
    if options.role == "sandbox":
        if not re.fullmatch(r"[0-9a-f]{40}", options.direct_runsc_commit):
            raise ValueError(
                "direct runtime requires an exact 40-character runsc commit"
            )
        if options.direct_network not in {"none", "sandbox"}:
            raise ValueError(
                "direct runtime network must be either 'none' or 'sandbox'."
            )
        for endpoint in options.direct_network_allow_tcp:
            DirectNetworkTcpEgress.parse(endpoint)
        if options.direct_network == "none" and options.direct_network_allow_tcp:
            raise ValueError("direct network TCP egress requires sandbox networking.")
        if options.docker_quota_image_gb < 1:
            raise ValueError(
                "direct runtime requires bounded Docker image infrastructure."
            )
        if not re.fullmatch(r"https?://[^/\s]+", options.storage_native_registry_url):
            raise ValueError(
                "direct runtime requires an HTTP(S) storage-native Registry origin"
            )
        if options.storage_native_snapshot_backend not in {"registry", "s3"}:
            raise ValueError("storage-native snapshot backend must be registry or s3")
        if options.storage_native_snapshot_backend == "s3":
            if not re.fullmatch(r"https?://[^\s]+", options.storage_native_s3_endpoint):
                raise ValueError("storage-native S3 endpoint must be HTTP(S)")
            if (
                not options.storage_native_s3_bucket
                or "/" in options.storage_native_s3_bucket
                or not options.storage_native_s3_region
                or not options.storage_native_s3_prefix.strip("/")
            ):
                raise ValueError(
                    "storage-native S3 bucket, region, and prefix are required"
                )
            if (
                not options.storage_native_s3_access_key_id
                or not options.storage_native_s3_secret_access_key
            ):
                raise ValueError("storage-native S3 credentials are required")
        if (
            not options.storage_native_repository
            or options.storage_native_repository.startswith("/")
            or options.storage_native_repository.endswith("/")
            or not re.fullmatch(
                r"[a-z0-9]+(?:[._/-][a-z0-9]+)*",
                options.storage_native_repository,
            )
        ):
            raise ValueError("invalid storage-native repository")
        if options.storage_native_cache_gb < 1:
            raise ValueError("storage-native cache size must be positive.")
        if options.storage_native_pool_low_watermark < 0:
            raise ValueError("storage-native pool low watermark cannot be negative.")
        if options.storage_native_pool_high_watermark < 1:
            raise ValueError("storage-native pool high watermark must be positive.")
        if (
            options.storage_native_pool_low_watermark
            > options.storage_native_pool_high_watermark
        ):
            raise ValueError(
                "storage-native pool low watermark cannot exceed high watermark."
            )
        if options.direct_disk_headroom_mb < 1:
            raise ValueError("direct runtime disk headroom must be positive.")
        guaranteed_mb = (
            int(options.total_resources.disk_mb)
            - options.docker_quota_image_gb * 1024
            - options.swap_gb * 1024
            - options.storage_native_cache_gb * 1024
            - options.direct_disk_headroom_mb
        )
        if guaranteed_mb < 1:
            raise ValueError(
                "direct runtime physical disk cannot guarantee Docker, swap, "
                "cache, headroom, and one writable MiB"
            )
        if options.direct_max_concurrent_restores < 1:
            raise ValueError("direct max concurrent restores must be positive.")
    if options.docker_quota_image_gb < 0:
        raise ValueError("docker quota image size cannot be negative.")
    if options.swap_gb < 0:
        raise ValueError("swap size cannot be negative.")
    if options.max_concurrent_image_pulls < 1:
        raise ValueError("max concurrent image pulls must be positive.")
    _validate_service_user(options.service_user)
    for value_name, value in {
        "job id": options.job_id,
        "heartbeat url": options.heartbeat_url,
        "heartbeat bearer token file": options.heartbeat_bearer_token_file,
        "heartbeat bearer token": options.heartbeat_bearer_token,
        "node control bearer token file": options.node_control_bearer_token_file,
        "node control bearer token": options.node_control_bearer_token,
        "service user": options.service_user,
        "node id": options.node_id,
        "node agent host": options.node_agent_host,
        "deployment id": options.deployment_id,
        "work dir": options.work_dir,
        "package spec": options.package_spec,
        "package sha256": options.package_sha256,
        "storage-native registry URL": options.storage_native_registry_url,
        "storage-native repository": options.storage_native_repository,
        "storage-native snapshot backend": options.storage_native_snapshot_backend,
        "storage-native S3 endpoint": options.storage_native_s3_endpoint,
        "storage-native S3 bucket": options.storage_native_s3_bucket,
        "storage-native S3 region": options.storage_native_s3_region,
        "storage-native S3 prefix": options.storage_native_s3_prefix,
    }.items():
        _reject_newline(value_name, value)
    if not options.package_sha256:
        raise ValueError("package sha256 is required.")
    if len(options.package_sha256) != 64 or any(
        character not in "0123456789abcdef" for character in options.package_sha256
    ):
        raise ValueError("package sha256 must be a lowercase SHA-256 digest.")
    for name, value in (
        ("deployment id", options.deployment_id),
        ("heartbeat bearer token file", options.heartbeat_bearer_token_file),
        ("heartbeat bearer token", options.heartbeat_bearer_token),
        ("node control bearer token file", options.node_control_bearer_token_file),
        ("node control bearer token", options.node_control_bearer_token),
    ):
        if not value.strip():
            raise ValueError(f"{name} is required")
    for registry in options.docker_insecure_registries:
        if not registry.strip():
            raise ValueError("docker insecure registry cannot be empty.")
        _reject_newline("docker insecure registry", registry)
    for alias in options.host_aliases:
        if not alias.strip():
            raise ValueError("host alias cannot be empty.")
        _reject_newline("host alias", alias)
        if alias.count("=") != 1:
            raise ValueError("host alias must use HOST=ADDRESS.")
        host, address = alias.split("=", 1)
        if not host or not address:
            raise ValueError("host alias must use HOST=ADDRESS.")
        if any(ch.isspace() for ch in host + address):
            raise ValueError("host alias cannot contain whitespace.")
    for key, value in (options.labels or {}).items():
        _reject_newline("label key", key)
        _reject_newline("label value", value)
        if "=" in key:
            raise ValueError("label keys cannot contain '='.")
    _reject_newline("buildx cache ref", options.buildx_cache_ref)
    if options.buildx_cache_ref and options.role != "builder":
        raise ValueError("buildx_cache_ref requires the builder role")
    for key in options.init_authorized_keys:
        if not key.strip():
            raise ValueError("init authorized keys cannot contain empty keys.")
        _reject_newline("init authorized key", key)


def ssh_init_command(
    ssh_command: str,
    *,
    private_key_file: str | None = None,
    known_hosts_file: str | None = None,
) -> tuple[str, ...]:
    return (
        *ssh_command_with_options(
            ssh_command,
            private_key_file=private_key_file,
            known_hosts_file=known_hosts_file,
        ),
        "bash",
        "-s",
    )


def ssh_remote_command(
    ssh_command: str,
    remote_command: str,
    *,
    private_key_file: str | None = None,
    known_hosts_file: str | None = None,
) -> tuple[str, ...]:
    if not remote_command:
        raise ValueError("remote command is required.")
    return (
        *ssh_command_with_options(
            ssh_command,
            private_key_file=private_key_file,
            known_hosts_file=known_hosts_file,
        ),
        remote_command,
    )


def ssh_command_with_options(
    ssh_command: str,
    *,
    private_key_file: str | None = None,
    known_hosts_file: str | None = None,
) -> tuple[str, ...]:
    argv = tuple(shlex.split(ssh_command))
    if not argv:
        raise ValueError("SSH command is empty.")
    if argv[0] != "ssh":
        raise ValueError(f"Expected ssh command, got: {argv[0]}")
    private_key_args: tuple[str, ...] = ()
    if private_key_file:
        _reject_newline("private key file", private_key_file)
        private_key_args = ("-i", private_key_file)
    known_hosts_args: tuple[str, ...] = ()
    if known_hosts_file:
        _reject_newline("known hosts file", known_hosts_file)
        known_hosts_args = ("-o", f"UserKnownHostsFile={known_hosts_file}")
    return (
        argv[0],
        *DEFAULT_SSH_OPTIONS,
        *known_hosts_args,
        *private_key_args,
        *argv[1:],
    )


def run_init_over_ssh(
    ssh_command: str,
    script: str,
    *,
    timeout_seconds: int | None = None,
    private_key_file: str | None = None,
    known_hosts_file: str | None = None,
) -> VmInitRunResult:
    command = ssh_init_command(
        ssh_command,
        private_key_file=private_key_file,
        known_hosts_file=known_hosts_file,
    )
    completed = subprocess.run(
        command,
        input=script,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
        timeout=timeout_seconds,
    )
    output = completed.stdout or ""
    if output:
        print(output, end="" if output.endswith("\n") else "\n", file=sys.stderr)
    phases, total_duration_ms = parse_vm_init_phases(output)
    return VmInitRunResult(
        command=command,
        returncode=completed.returncode,
        phase_durations_ms=tuple(phases.items()),
        total_duration_ms=total_duration_ms,
    )


_INIT_PHASE_PATTERN = re.compile(
    r"^UCLOUD_INIT_PHASE name=([a-z0-9-]+) duration_ms=([0-9]+) total_ms=([0-9]+)$"
)


def parse_vm_init_phases(output: str) -> tuple[dict[str, int], int | None]:
    phases: dict[str, int] = {}
    total_duration_ms: int | None = None
    for line in output.splitlines():
        match = _INIT_PHASE_PATTERN.fullmatch(line.strip())
        if match is None:
            continue
        phases[match.group(1)] = int(match.group(2))
        total_duration_ms = int(match.group(3))
    return phases, total_duration_ms


def local_package_spec_path(package_spec: str) -> Path:
    path = Path(package_spec).expanduser()
    if not path.is_file():
        raise ValueError("node package spec must be a local bundle path")
    return path


def local_package_sha256(path: Path) -> str:
    sidecar = Path(f"{path}.sha256")
    if (
        sidecar.is_file()
        and sidecar.stat().st_size <= 256
        and sidecar.stat().st_mtime_ns >= path.stat().st_mtime_ns
    ):
        candidate = sidecar.read_text(encoding="ascii").strip().split()[0]
        if len(candidate) == 64 and all(
            character in "0123456789abcdef" for character in candidate
        ):
            return candidate
    stat = path.stat()
    return _cached_file_sha256(str(path.resolve()), stat.st_size, stat.st_mtime_ns)


@lru_cache(maxsize=16)
def _cached_file_sha256(path: str, size: int, mtime_ns: int) -> str:
    del size, mtime_ns
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def remote_package_spec_for_local_path(
    options: VmInitOptions,
    local_path: Path,
    *,
    package_sha256: str = "",
    remote_package_dir: str = DEFAULT_REMOTE_PACKAGE_DIR,
) -> str:
    del options
    _reject_newline("remote package dir", remote_package_dir)
    remote_dir = _clean_posix_path(remote_package_dir)
    digest = package_sha256 or local_package_sha256(local_path)
    if not re.fullmatch(r"[0-9a-f]{64}", digest):
        raise ValueError("package sha256 must be a lowercase SHA-256 digest.")
    return str(PurePosixPath(remote_dir) / digest / DEFAULT_REMOTE_PACKAGE_FILENAME)


def static_runtime_receipt_for_remote_package(
    remote_package: str,
    *,
    role: str,
    init_version: str = DEFAULT_INIT_VERSION,
) -> str:
    remote_path = _clean_posix_path(remote_package)
    if role not in {"sandbox", "builder"}:
        raise ValueError("node role must be sandbox or builder")
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", init_version):
        raise ValueError("init version is unsafe for a runtime receipt path")
    return str(
        PurePosixPath(remote_path).parent / f"runtime-ready-v{init_version}-{role}"
    )


def stage_vm_init_package_over_ssh(
    ssh_command: str,
    options: VmInitOptions,
    *,
    timeout_seconds: int | None = None,
    private_key_file: str | None = None,
    known_hosts_file: str | None = None,
    remote_package_dir: str = DEFAULT_REMOTE_PACKAGE_DIR,
) -> VmInitPackageStageResult:
    local_path = local_package_spec_path(options.package_spec)
    package_size = local_path.stat().st_size
    package_sha256 = local_package_sha256(local_path)
    remote_path = remote_package_spec_for_local_path(
        options,
        local_path,
        package_sha256=package_sha256,
        remote_package_dir=remote_package_dir,
    )
    remote_parent = str(PurePosixPath(remote_path).parent)
    remote_marker = f"{remote_path}.sha256"
    remote_temporary = f"{remote_path}.tmp"
    remote_marker_temporary = f"{remote_marker}.tmp"
    runtime_receipt = static_runtime_receipt_for_remote_package(
        remote_path,
        role=options.role,
    )
    quoted_parent = shlex.quote(remote_parent)
    quoted_path = shlex.quote(remote_path)
    quoted_marker = shlex.quote(remote_marker)
    quoted_temporary = shlex.quote(remote_temporary)
    quoted_marker_temporary = shlex.quote(remote_marker_temporary)
    quoted_receipt = shlex.quote(runtime_receipt)
    receipt_lines = (
        f"schema={STATIC_RUNTIME_RECEIPT_SCHEMA}",
        f"bundle_sha256={package_sha256}",
        f"init_version={DEFAULT_INIT_VERSION}",
        f"role={options.role}",
    )
    receipt_probe = " && ".join(
        f"grep -Fx -- {shlex.quote(line)} {quoted_receipt} >/dev/null"
        for line in receipt_lines
    )
    probe_command = (
        f"(test -f {quoted_receipt} && "
        f'test "$(stat -c %u:%a {quoted_receipt})" = 0:444 && '
        f"{receipt_probe} && "
        f'test "$(awk -F= \'$1 == "kernel_release" {{print substr($0, 16); exit}}\' '
        f'{quoted_receipt})" = "$(uname -r)") || '
        f"(test -f {quoted_path} && "
        f'test "$(stat -c %u:%a {quoted_path})" = 0:444 && '
        f'test "$(stat -c %s {quoted_path})" = {package_size} && '
        f'test "$(cat {quoted_marker} 2>/dev/null)" = {package_sha256})'
    )
    probe = subprocess.run(
        ssh_remote_command(
            ssh_command,
            probe_command,
            private_key_file=private_key_file,
            known_hosts_file=known_hosts_file,
        ),
        check=False,
        timeout=timeout_seconds,
    )
    if probe.returncode == 0:
        return VmInitPackageStageResult(
            local_path=local_path,
            remote_path=remote_path,
            command=ssh_remote_command(
                ssh_command,
                probe_command,
                private_key_file=private_key_file,
                known_hosts_file=known_hosts_file,
            ),
            returncode=0,
            package_sha256=package_sha256,
            reused=True,
        )
    if probe.returncode == 255:
        return VmInitPackageStageResult(
            local_path=local_path,
            remote_path=remote_path,
            command=ssh_remote_command(
                ssh_command,
                probe_command,
                private_key_file=private_key_file,
                known_hosts_file=known_hosts_file,
            ),
            returncode=255,
            package_sha256=package_sha256,
        )
    remote_command = (
        'if [ "$(id -u)" -eq 0 ]; then STAGE_SUDO=""; '
        "else STAGE_SUDO=sudo; fi; "
        f"$STAGE_SUDO install -d -m 0755 -o root -g root {quoted_parent} && "
        f"$STAGE_SUDO rm -f {quoted_temporary} {quoted_marker_temporary} && "
        f"$STAGE_SUDO tee {quoted_temporary} >/dev/null && "
        f'test "$($STAGE_SUDO stat -c %s {quoted_temporary})" = {package_size} && '
        f"$STAGE_SUDO chown root:root {quoted_temporary} && "
        f"$STAGE_SUDO chmod 0444 {quoted_temporary} && "
        f"$STAGE_SUDO mv {quoted_temporary} {quoted_path} && "
        f"printf '%s\\n' {package_sha256} | "
        f"$STAGE_SUDO tee {quoted_marker_temporary} >/dev/null && "
        f"$STAGE_SUDO chown root:root {quoted_marker_temporary} && "
        f"$STAGE_SUDO chmod 0444 {quoted_marker_temporary} && "
        f"$STAGE_SUDO mv {quoted_marker_temporary} {quoted_marker}"
    )
    command = ssh_remote_command(
        ssh_command,
        remote_command,
        private_key_file=private_key_file,
        known_hosts_file=known_hosts_file,
    )
    # Runtime bundles are large enough that concurrent bootstrap workers must
    # not each retain a complete copy in controller memory.
    with local_path.open("rb") as source:
        completed = subprocess.run(
            command,
            stdin=source,
            check=False,
            timeout=timeout_seconds,
        )
    return VmInitPackageStageResult(
        local_path=local_path,
        remote_path=remote_path,
        command=command,
        returncode=completed.returncode,
        package_sha256=package_sha256,
    )


def _clean_posix_path(value: str) -> str:
    if not value.startswith("/"):
        raise ValueError("work dir must be an absolute path.")
    normalized = str(PurePosixPath(value))
    if normalized == "/":
        raise ValueError("work dir cannot be '/'.")
    return normalized


def _reject_newline(name: str, value: str) -> None:
    if "\n" in value or "\r" in value:
        raise ValueError(f"{name} cannot contain newlines.")


def _validate_service_user(value: str) -> None:
    if not value:
        raise ValueError("service user is required.")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_-]{0,31}", value):
        raise ValueError("service user must be a safe local account name.")
