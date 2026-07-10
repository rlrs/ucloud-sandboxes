from __future__ import annotations

from dataclasses import dataclass
from importlib import resources
from pathlib import Path, PurePosixPath
import shlex
import subprocess
from typing import Any

from .deployment import package_version
from .vm_init import (
    BUILDER_RUNTIME_PACKAGES,
    SANDBOX_RUNTIME_PACKAGES,
    ssh_init_command,
    ssh_remote_command,
)


DEFAULT_INSTALL_ROOT = "/work/ucloud-sandboxes"
DEFAULT_PROJECT_MOUNT_DIR = "/work/data"
DEFAULT_REGISTRY_ALIAS = "ucloud-sandbox-registry"
AUTO_REGISTRY_PRIVATE_IP_TOKEN = "__UCLOUD_REGISTRY_PRIVATE_IP__"
SYSTEMD_UNIT_NAMES = (
    "ucloud-sandbox-gateway.service",
    "ucloud-sandbox-relay.service",
    "ucloud-sandbox-registry.service",
    "ucloud-sandbox-registry-prune.service",
    "ucloud-sandbox-registry-prune.timer",
    "ucloud-sandbox-registry-gc.service",
    "ucloud-sandbox-registry-gc.timer",
    "ucloud-sandbox-autoscaler.service",
)


@dataclass(frozen=True)
class AllInOneDeployPlan:
    job_id: str
    project_id: str
    deployment_id: str
    local_wheel: Path
    install_root: str = DEFAULT_INSTALL_ROOT
    project_mount_dir: str = DEFAULT_PROJECT_MOUNT_DIR
    service_user: str = "ucloud"
    package_version: str = package_version()
    gateway_port: int = 8090
    relay_port: int = 8092
    registry_port: int = 5000
    heartbeat_ttl_seconds: int = 120
    registry_alias: str = DEFAULT_REGISTRY_ALIAS
    registry_private_ip: str = ""
    gateway_private_host: str = ""
    private_network_id: str = ""
    sandbox_product_id: str = "cpu-amd-zen5-16-vcpu"
    sandbox_disk_gb: int = 250
    sandbox_idle_seconds: int = 600
    builder_product_id: str = "cpu-amd-zen5-16-vcpu"
    builder_disk_gb: int = 250
    builder_idle_seconds: int = 900
    max_builder_nodes: int = 1
    max_init_per_cycle: int = 4
    init_retry_seconds: int = 30
    init_timeout_seconds: int = 1800
    autoscaler_interval_seconds: float = 5.0
    cpu_overcommit: float = 2.0
    memory_overcommit: float = 1.2
    disk_overcommit: float = 1.0
    docker_quota_image_gb: int = 200
    request_timeout_seconds: int = 7200
    worker_lease_seconds: int = 600
    completed_request_retention_seconds: int = 3600
    registry_retention_days: float = 30.0
    registry_keep_per_repository: int = 0

    @property
    def state_dir(self) -> str:
        return str(PurePosixPath(self.install_root) / "state")

    @property
    def release_dir(self) -> str:
        return str(PurePosixPath(self.install_root) / "release")

    @property
    def venv_dir(self) -> str:
        return str(PurePosixPath(self.install_root) / "gateway-venv")

    @property
    def remote_wheel_path(self) -> str:
        return str(PurePosixPath(self.release_dir) / self.local_wheel.name)

    @property
    def node_package_bundle_path(self) -> str:
        return self.sandbox_node_package_bundle_path

    @property
    def sandbox_node_package_bundle_path(self) -> str:
        return str(
            PurePosixPath(self.release_dir)
            / f"{self.local_wheel.stem}-sandbox-node-package.tar.gz"
        )

    @property
    def builder_node_package_bundle_path(self) -> str:
        return str(
            PurePosixPath(self.release_dir)
            / f"{self.local_wheel.stem}-builder-node-package.tar.gz"
        )

    @property
    def remote_session_file(self) -> str:
        return str(PurePosixPath(self.state_dir) / "ucloud-session.json")

    @property
    def gateway_token_file(self) -> str:
        return str(PurePosixPath(self.state_dir) / "gateway-token")

    @property
    def heartbeat_token_file(self) -> str:
        return str(PurePosixPath(self.state_dir) / "heartbeat-token")

    @property
    def node_control_token_file(self) -> str:
        return str(PurePosixPath(self.state_dir) / "node-control-token")

    @property
    def relay_sandbox_token_file(self) -> str:
        return str(PurePosixPath(self.state_dir) / "relay-sandbox-token")

    @property
    def relay_worker_token_file(self) -> str:
        return str(PurePosixPath(self.state_dir) / "relay-worker-token")

    @property
    def init_ssh_private_key_file(self) -> str:
        return str(PurePosixPath(self.state_dir) / "ssh" / "gateway-init")

    @property
    def init_authorized_key_file(self) -> str:
        return self.init_ssh_private_key_file + ".pub"

    @property
    def registry_usage_file(self) -> str:
        return str(PurePosixPath(self.state_dir) / "registry-usage.json")

    @property
    def image_file(self) -> str:
        return str(PurePosixPath(self.state_dir) / "images.json")

    @property
    def registry_data_dir(self) -> str:
        return str(
            PurePosixPath(self.project_mount_dir)
            / "ucloud-sandbox-registry"
            / "docker-registry"
        )

    @property
    def init_heartbeat_url(self) -> str:
        host = self.gateway_private_host
        if not host:
            raise ValueError("gateway private host is required.")
        return f"http://{host}:{self.gateway_port}/v1/nodes/heartbeat"

    @property
    def registry_url(self) -> str:
        return f"http://127.0.0.1:{self.registry_port}"

    @property
    def docker_insecure_registry(self) -> str:
        return f"{self.registry_alias}:{self.registry_port}"

    @property
    def docker_host_alias(self) -> str:
        registry_private_ip = self.registry_private_ip or AUTO_REGISTRY_PRIVATE_IP_TOKEN
        return f"{self.registry_alias}={registry_private_ip}"

    def validate(self) -> None:
        for label, value in {
            "job id": self.job_id,
            "project id": self.project_id,
            "deployment id": self.deployment_id,
            "install root": self.install_root,
            "project mount dir": self.project_mount_dir,
            "service user": self.service_user,
            "gateway private host": self.gateway_private_host,
            "private network id": self.private_network_id,
        }.items():
            _reject_bad_text(label, value)
            if not value:
                raise ValueError(f"{label} is required.")
        _reject_bad_text("registry private IP", self.registry_private_ip)
        if not self.local_wheel.is_file():
            raise ValueError(f"wheel file not found: {self.local_wheel}")
        for label, value in {
            "gateway port": self.gateway_port,
            "relay port": self.relay_port,
            "registry port": self.registry_port,
            "sandbox disk GB": self.sandbox_disk_gb,
            "builder disk GB": self.builder_disk_gb,
            "sandbox idle seconds": self.sandbox_idle_seconds,
            "builder idle seconds": self.builder_idle_seconds,
            "max builder nodes": self.max_builder_nodes,
            "max init per cycle": self.max_init_per_cycle,
            "init retry seconds": self.init_retry_seconds,
            "init timeout seconds": self.init_timeout_seconds,
            "docker quota image GB": self.docker_quota_image_gb,
            "registry keep per repository": self.registry_keep_per_repository,
        }.items():
            if value < 0:
                raise ValueError(f"{label} cannot be negative.")
        if self.registry_retention_days <= 0:
            raise ValueError("registry retention days must be positive.")
        for port_label, port in {
            "gateway port": self.gateway_port,
            "relay port": self.relay_port,
            "registry port": self.registry_port,
        }.items():
            if port < 1 or port > 65535:
                raise ValueError(f"{port_label} must be in [1, 65535].")

    def to_dict(self) -> dict[str, Any]:
        return {
            "jobId": self.job_id,
            "projectId": self.project_id,
            "deploymentId": self.deployment_id,
            "packageVersion": self.package_version,
            "localWheel": str(self.local_wheel),
            "remoteWheelPath": self.remote_wheel_path,
            "nodePackageBundlePath": self.node_package_bundle_path,
            "installRoot": self.install_root,
            "stateDir": self.state_dir,
            "projectMountDir": self.project_mount_dir,
            "registryDataDir": self.registry_data_dir,
            "gatewayPort": self.gateway_port,
            "relayPort": self.relay_port,
            "registryPort": self.registry_port,
            "registryRetentionDays": self.registry_retention_days,
            "registryKeepPerRepository": self.registry_keep_per_repository,
            "registryUsageFile": self.registry_usage_file,
            "imageFile": self.image_file,
            "gatewayPrivateHost": self.gateway_private_host,
            "registryAlias": self.registry_alias,
            "registryPrivateIp": self.registry_private_ip,
            "privateNetworkId": self.private_network_id,
            "initHeartbeatUrl": self.init_heartbeat_url,
            "dockerInsecureRegistry": self.docker_insecure_registry,
            "dockerHostAlias": self.docker_host_alias,
            "remoteSessionFile": self.remote_session_file,
            "gatewayTokenFile": self.gateway_token_file,
            "heartbeatTokenFile": self.heartbeat_token_file,
            "nodeControlTokenFile": self.node_control_token_file,
            "relaySandboxTokenFile": self.relay_sandbox_token_file,
            "relayWorkerTokenFile": self.relay_worker_token_file,
            "initSshPrivateKeyFile": self.init_ssh_private_key_file,
            "initAuthorizedKeyFile": self.init_authorized_key_file,
            "autoscaler": {
                "intervalSeconds": self.autoscaler_interval_seconds,
                "sandboxProductId": self.sandbox_product_id,
                "sandboxDiskGb": self.sandbox_disk_gb,
                "sandboxIdleSeconds": self.sandbox_idle_seconds,
                "builderProductId": self.builder_product_id,
                "builderDiskGb": self.builder_disk_gb,
                "builderIdleSeconds": self.builder_idle_seconds,
                "maxBuilderNodes": self.max_builder_nodes,
                "cpuOvercommit": self.cpu_overcommit,
                "memoryOvercommit": self.memory_overcommit,
                "diskOvercommit": self.disk_overcommit,
            },
        }


@dataclass(frozen=True)
class RemoteCommandResult:
    command: tuple[str, ...]
    returncode: int
    stdout: str = ""
    stderr: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "command": list(self.command),
            "returncode": self.returncode,
            "stdout": self.stdout,
            "stderr": self.stderr,
        }


def gateway_env(plan: AllInOneDeployPlan) -> dict[str, str]:
    return {
        "UCLOUD_DEPLOYMENT_ID": plan.deployment_id,
        "UCLOUD_STATE_DIR": plan.state_dir,
        "UCLOUD_SESSION_FILE": plan.remote_session_file,
        "UCLOUD_GATEWAY_PORT": str(plan.gateway_port),
        "UCLOUD_HEARTBEAT_TTL_SECONDS": str(plan.heartbeat_ttl_seconds),
        "UCLOUD_GATEWAY_TOKEN_FILE": plan.gateway_token_file,
        "UCLOUD_HEARTBEAT_TOKEN_FILE": plan.heartbeat_token_file,
        "UCLOUD_NODE_CONTROL_TOKEN_FILE": plan.node_control_token_file,
        "UCLOUD_REGISTRY_URL": plan.registry_url,
        "UCLOUD_REGISTRY_USAGE_FILE": plan.registry_usage_file,
    }


def relay_env(plan: AllInOneDeployPlan) -> dict[str, str]:
    return {
        "UCLOUD_RELAY_HOST": "0.0.0.0",
        "UCLOUD_RELAY_PORT": str(plan.relay_port),
        "UCLOUD_RELAY_SANDBOX_TOKEN_FILE": plan.relay_sandbox_token_file,
        "UCLOUD_RELAY_WORKER_TOKEN_FILE": plan.relay_worker_token_file,
        "UCLOUD_RELAY_REQUEST_TIMEOUT_SECONDS": str(plan.request_timeout_seconds),
        "UCLOUD_RELAY_WORKER_LEASE_SECONDS": str(plan.worker_lease_seconds),
        "UCLOUD_RELAY_COMPLETED_REQUEST_RETENTION_SECONDS": str(
            plan.completed_request_retention_seconds
        ),
    }


def registry_env(plan: AllInOneDeployPlan) -> dict[str, str]:
    return {
        "UCLOUD_REGISTRY_URL": plan.registry_url,
        "UCLOUD_REGISTRY_IMAGE": "registry:2",
        "UCLOUD_REGISTRY_BIND": "0.0.0.0",
        "UCLOUD_REGISTRY_PORT": str(plan.registry_port),
        "UCLOUD_REGISTRY_DATA_DIR": plan.registry_data_dir,
        "UCLOUD_REGISTRY_RETENTION_DAYS": f"{plan.registry_retention_days:g}",
        "UCLOUD_REGISTRY_KEEP_PER_REPOSITORY": str(
            plan.registry_keep_per_repository
        ),
        "UCLOUD_REGISTRY_USAGE_FILE": plan.registry_usage_file,
        "UCLOUD_IMAGE_FILE": plan.image_file,
    }


def autoscaler_env(plan: AllInOneDeployPlan) -> dict[str, str]:
    return {
        "UCLOUD_PROJECT_ID": plan.project_id,
        "UCLOUD_DEPLOYMENT_ID": plan.deployment_id,
        "UCLOUD_STATE_DIR": plan.state_dir,
        "UCLOUD_SESSION_FILE": plan.remote_session_file,
        "UCLOUD_PRIVATE_NETWORK_ID": plan.private_network_id,
        "UCLOUD_AUTOSCALER_INTERVAL_SECONDS": f"{plan.autoscaler_interval_seconds:g}",
        "UCLOUD_SANDBOX_PRODUCT_ID": plan.sandbox_product_id,
        "UCLOUD_SANDBOX_DISK_GB": str(plan.sandbox_disk_gb),
        "UCLOUD_SANDBOX_IDLE_SECONDS": str(plan.sandbox_idle_seconds),
        "UCLOUD_BUILDER_PRODUCT_ID": plan.builder_product_id,
        "UCLOUD_BUILDER_DISK_GB": str(plan.builder_disk_gb),
        "UCLOUD_BUILDER_IDLE_SECONDS": str(plan.builder_idle_seconds),
        "UCLOUD_MAX_BUILDER_NODES": str(plan.max_builder_nodes),
        "UCLOUD_MAX_INIT_PER_CYCLE": str(plan.max_init_per_cycle),
        "UCLOUD_INIT_RETRY_SECONDS": str(plan.init_retry_seconds),
        "UCLOUD_INIT_TIMEOUT_SECONDS": str(plan.init_timeout_seconds),
        "UCLOUD_INIT_HEARTBEAT_URL": plan.init_heartbeat_url,
        "UCLOUD_INIT_HEARTBEAT_TOKEN_FILE": plan.heartbeat_token_file,
        "UCLOUD_INIT_HEARTBEAT_TOKEN_SOURCE_FILE": plan.heartbeat_token_file,
        "UCLOUD_NODE_CONTROL_TOKEN_FILE": plan.node_control_token_file,
        "UCLOUD_INIT_NODE_CONTROL_TOKEN_FILE": plan.node_control_token_file,
        "UCLOUD_INIT_NODE_CONTROL_TOKEN_SOURCE_FILE": plan.node_control_token_file,
        "UCLOUD_GATEWAY_TOKEN_FILE": plan.gateway_token_file,
        "UCLOUD_INIT_AUTHORIZED_KEY_FILE": plan.init_authorized_key_file,
        "UCLOUD_INIT_SSH_PRIVATE_KEY_FILE": plan.init_ssh_private_key_file,
        "UCLOUD_INIT_PACKAGE_SPEC": plan.sandbox_node_package_bundle_path,
        "UCLOUD_INIT_BUILDER_PACKAGE_SPEC": plan.builder_node_package_bundle_path,
        "UCLOUD_DOCKER_INSECURE_REGISTRY": plan.docker_insecure_registry,
        "UCLOUD_DOCKER_HOST_ALIAS": plan.docker_host_alias,
        "UCLOUD_INIT_DOCKER_QUOTA_IMAGE_GB": str(plan.docker_quota_image_gb),
        "UCLOUD_INIT_CPU_OVERCOMMIT": f"{plan.cpu_overcommit:g}",
        "UCLOUD_INIT_MEMORY_OVERCOMMIT": f"{plan.memory_overcommit:g}",
        "UCLOUD_INIT_DISK_OVERCOMMIT": f"{plan.disk_overcommit:g}",
    }


def render_env_file(values: dict[str, str]) -> str:
    lines: list[str] = []
    for key, value in values.items():
        _reject_bad_env_key(key)
        _reject_bad_text(key, value)
        lines.append(f"{key}={_systemd_env_quote(value)}")
    return "\n".join(lines) + "\n"


def packaged_systemd_units() -> dict[str, str]:
    root = resources.files("ucloud_sandboxes").joinpath("systemd")
    units: dict[str, str] = {}
    for name in SYSTEMD_UNIT_NAMES:
        units[name] = root.joinpath(name).read_text(encoding="utf-8")
    return units


def render_remote_deploy_script(
    plan: AllInOneDeployPlan,
    *,
    units: dict[str, str] | None = None,
) -> str:
    plan.validate()
    unit_texts = units if units is not None else packaged_systemd_units()
    env_files = {
        "/etc/ucloud-sandboxes/gateway.env": render_env_file(gateway_env(plan)),
        "/etc/ucloud-sandboxes/relay.env": render_env_file(relay_env(plan)),
        "/etc/ucloud-sandboxes/registry.env": render_env_file(registry_env(plan)),
        "/etc/ucloud-sandboxes/autoscaler.env": render_env_file(autoscaler_env(plan)),
    }
    unit_files = {
        f"/etc/systemd/system/{name}": unit_texts[name]
        for name in SYSTEMD_UNIT_NAMES
    }
    sandbox_runtime_packages = " ".join(SANDBOX_RUNTIME_PACKAGES)
    builder_runtime_packages = " ".join(BUILDER_RUNTIME_PACKAGES)
    script_parts = [
        "#!/usr/bin/env bash",
        "set -euo pipefail",
        f"INSTALL_ROOT={shlex.quote(plan.install_root)}",
        f"STATE_DIR={shlex.quote(plan.state_dir)}",
        f"RELEASE_DIR={shlex.quote(plan.release_dir)}",
        f"VENV_DIR={shlex.quote(plan.venv_dir)}",
        f"REMOTE_WHEEL={shlex.quote(plan.remote_wheel_path)}",
        f"SANDBOX_NODE_PACKAGE_BUNDLE={shlex.quote(plan.sandbox_node_package_bundle_path)}",
        f"BUILDER_NODE_PACKAGE_BUNDLE={shlex.quote(plan.builder_node_package_bundle_path)}",
        f"SERVICE_USER={shlex.quote(plan.service_user)}",
        f"SESSION_FILE={shlex.quote(plan.remote_session_file)}",
        f"INIT_KEY={shlex.quote(plan.init_ssh_private_key_file)}",
        f"INIT_KEY_COMMENT={shlex.quote(plan.deployment_id + ' gateway init')}",
        f"REGISTRY_PRIVATE_IP={shlex.quote(plan.registry_private_ip)}",
        "",
        'SERVICE_GROUP="$(id -gn "$SERVICE_USER")"',
        'detect_registry_private_ip() {',
        '  ip -o -4 addr show scope global | awk \'',
        "    {",
        '      split($4, addr, "/")',
        "      ip = addr[1]",
        '      if (ip !~ /^127\\./ && ip !~ /^169\\.254\\./ && ip !~ /^172\\.17\\./) {',
        "        print ip",
        "        exit",
        "      }",
        "    }",
        "  '",
        "}",
        'if [ -z "$REGISTRY_PRIVATE_IP" ]; then',
        '  REGISTRY_PRIVATE_IP="$(detect_registry_private_ip)"',
        "fi",
        'if [ -z "$REGISTRY_PRIVATE_IP" ]; then',
        '  echo "Could not detect registry private IPv4 address; pass --registry-private-ip." >&2',
        "  exit 1",
        "fi",
        "",
        'sudo install -d -m 0755 -o "$SERVICE_USER" -g "$SERVICE_GROUP" "$INSTALL_ROOT"',
        'sudo install -d -m 0755 -o "$SERVICE_USER" -g "$SERVICE_GROUP" "$RELEASE_DIR"',
        'sudo install -d -m 0700 -o "$SERVICE_USER" -g "$SERVICE_GROUP" "$STATE_DIR"',
        'sudo install -d -m 0700 -o "$SERVICE_USER" -g "$SERVICE_GROUP" "$STATE_DIR/ssh"',
        'test -s "$REMOTE_WHEEL"',
        'test -s "$SESSION_FILE"',
        'chmod 600 "$SESSION_FILE"',
        'sudo chown "$SERVICE_USER:$SERVICE_GROUP" "$SESSION_FILE"',
        "sudo apt-get update",
        "sudo env DEBIAN_FRONTEND=noninteractive apt-get install -y "
        "ca-certificates curl docker.io gnupg openssh-client openssl python3-venv",
        "",
        'if [ ! -x "$VENV_DIR/bin/python" ]; then',
        '  python3 -m venv "$VENV_DIR"',
        "fi",
        'sudo chown -R "$SERVICE_USER:$SERVICE_GROUP" "$VENV_DIR"',
        '"$VENV_DIR/bin/pip" install --upgrade pip',
        '"$VENV_DIR/bin/pip" install --force-reinstall "$REMOTE_WHEEL"',
        'NODE_PACKAGE_WORK="$(mktemp -d)"',
        'trap \'rm -rf "$NODE_PACKAGE_WORK"\' EXIT',
        'mkdir -p "$NODE_PACKAGE_WORK/wheels"',
        '"$VENV_DIR/bin/pip" download --disable-pip-version-check '
        '--only-binary=:all: --dest "$NODE_PACKAGE_WORK/wheels" "$REMOTE_WHEEL"',
        'NODE_AGENT_RUNTIME_DIR="$NODE_PACKAGE_WORK/node-agent-runtime"',
        'NODE_AGENT_RUNTIME_ARCHIVE="$NODE_PACKAGE_WORK/node-agent-runtime.tar"',
        'mkdir -p "$NODE_AGENT_RUNTIME_DIR/site-packages"',
        '"$VENV_DIR/bin/pip" install --disable-pip-version-check '
        '--no-compile --target "$NODE_AGENT_RUNTIME_DIR/site-packages" "$REMOTE_WHEEL"',
        'tar --sort=name --mtime=@0 --owner=0 --group=0 --numeric-owner '
        '-cf "$NODE_AGENT_RUNTIME_ARCHIVE" -C "$NODE_AGENT_RUNTIME_DIR" .',
        'RUNTIME_OS_ID="$(. /etc/os-release && printf \'%s\' "$ID")"',
        'RUNTIME_VERSION_ID="$(. /etc/os-release && printf \'%s\' "$VERSION_ID")"',
        'RUNTIME_CODENAME="$(. /etc/os-release && printf \'%s\' "${UBUNTU_CODENAME:-${VERSION_CODENAME:-}}")"',
        'RUNTIME_ARCHITECTURE="$(dpkg --print-architecture)"',
        "RUNTIME_BUNDLE_READY=0",
        "BUILDER_RUNTIME_BUNDLE_READY=0",
        "download_runtime_packages() {",
        '  runtime_name="$1"',
        "  shift",
        '  archive_dir="$NODE_PACKAGE_WORK/$runtime_name/debs"',
        '  status_file="$NODE_PACKAGE_WORK/$runtime_name-empty-dpkg-status"',
        '  mkdir -p "$archive_dir/partial"',
        '  : > "$status_file"',
        "  sudo apt-get --download-only --no-install-recommends -y "
        "-o Debug::NoLocking=1 "
        '-o Dir::State::status="$status_file" '
        '-o Dir::Cache::archives="$archive_dir" install "$@" || return 1',
        "  find \"$archive_dir\" -maxdepth 1 -type f -name '*.deb' -print -quit | grep -q .",
        "}",
        "prune_runsc_package() {",
        '  archive_dir="$1/debs"',
        '  runsc_package=""',
        '  for package_file in "$archive_dir"/*.deb; do',
        '    if [ "$(dpkg-deb -f "$package_file" Package)" = runsc ]; then',
        '      runsc_package="$package_file"',
        "      break",
        "    fi",
        "  done",
        '  [ -n "$runsc_package" ] || return 1',
        '  unpack_dir="$1/runsc-pruned"',
        '  rm -rf "$unpack_dir"',
        '  dpkg-deb --raw-extract "$runsc_package" "$unpack_dir" || return 1',
        '  rm -f "$unpack_dir/usr/bin/containerd-shim-runsc-v1"',
        '  rm -f "$unpack_dir/usr/bin/runsc-metric-server"',
        '  rm -f "$unpack_dir/DEBIAN/md5sums"',
        '  replacement="$runsc_package.pruned"',
        '  SOURCE_DATE_EPOCH=0 dpkg-deb --build --root-owner-group -Zgzip -z1 '
        '"$unpack_dir" "$replacement" >/dev/null || return 1',
        '  mv "$replacement" "$runsc_package"',
        '  rm -rf "$unpack_dir"',
        "}",
        "build_runtime_bundle() {",
        '  if [ "$RUNTIME_OS_ID" != ubuntu ] || [ -z "$RUNTIME_CODENAME" ]; then',
        '    echo "Offline runtime bundle is supported only for Ubuntu; nodes will use repository fallback" >&2',
        "    return 1",
        "  fi",
        "  sudo install -m 0755 -d /etc/apt/keyrings || return 1",
        "  if [ ! -s /etc/apt/keyrings/docker.asc ]; then",
        "    sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc || return 1",
        "    sudo chmod a+r /etc/apt/keyrings/docker.asc || return 1",
        "  fi",
        "  sudo tee /etc/apt/sources.list.d/docker.sources >/dev/null <<DOCKER_SOURCES",
        "Types: deb",
        "URIs: https://download.docker.com/linux/ubuntu",
        "Suites: $RUNTIME_CODENAME",
        "Components: stable",
        "Architectures: $RUNTIME_ARCHITECTURE",
        "Signed-By: /etc/apt/keyrings/docker.asc",
        "DOCKER_SOURCES",
        "  [ -s /etc/apt/sources.list.d/docker.sources ] || return 1",
        "  if [ ! -s /usr/share/keyrings/gvisor-archive-keyring.gpg ]; then",
        "    curl -fsSL https://gvisor.dev/archive.key | sudo gpg --dearmor --yes -o /usr/share/keyrings/gvisor-archive-keyring.gpg || return 1",
        "  fi",
        '  echo "deb [arch=$RUNTIME_ARCHITECTURE signed-by=/usr/share/keyrings/gvisor-archive-keyring.gpg] https://storage.googleapis.com/gvisor/releases release main" | sudo tee /etc/apt/sources.list.d/gvisor.list >/dev/null || return 1',
        "  sudo apt-get update || return 1",
        f"  download_runtime_packages runtime {sandbox_runtime_packages} || return 1",
        '  prune_runsc_package "$NODE_PACKAGE_WORK/runtime" || return 1',
        '  sudo chmod -R a+rX "$NODE_PACKAGE_WORK/runtime" || return 1',
        "}",
        "if build_runtime_bundle; then",
        "  RUNTIME_BUNDLE_READY=1",
        "else",
        '  echo "WARNING: could not build offline Docker/gVisor bundle; cold nodes will use repository fallback" >&2',
        '  rm -rf "$NODE_PACKAGE_WORK/runtime"',
        "fi",
        "build_probe_image_bundle() {",
        '  probe_dir="$NODE_PACKAGE_WORK/runtime/images"',
        '  mkdir -p "$probe_dir"',
        "  sudo systemctl start docker || return 1",
        "  sudo docker pull busybox || return 1",
        '  probe_architecture="$(sudo docker image inspect --format \'{{.Architecture}}\' busybox)"',
        '  [ "$probe_architecture" = "$RUNTIME_ARCHITECTURE" ] || return 1',
        '  sudo docker image inspect busybox > "$probe_dir/runtime-conformance-busybox.inspect.json" || return 1',
        '  sudo docker save --output "$probe_dir/runtime-conformance-busybox.tar" busybox || return 1',
        '  sudo chmod a+r "$probe_dir/runtime-conformance-busybox.tar" || return 1',
        "}",
        'if [ "$RUNTIME_BUNDLE_READY" -eq 1 ] && ! build_probe_image_bundle; then',
        '  echo "WARNING: could not bundle the busybox conformance image; cold nodes may pull it" >&2',
        '  rm -rf "$NODE_PACKAGE_WORK/runtime/images"',
        "fi",
        'if [ "$RUNTIME_BUNDLE_READY" -eq 1 ]; then',
        '  cp -a "$NODE_PACKAGE_WORK/runtime" "$NODE_PACKAGE_WORK/runtime-builder"',
        '  if download_runtime_packages runtime-builder docker-buildx-plugin; then',
        '    sudo chmod -R a+rX "$NODE_PACKAGE_WORK/runtime-builder"',
        "    BUILDER_RUNTIME_BUNDLE_READY=1",
        "  else",
        '    echo "WARNING: could not add Buildx to builder bundle; cold builders will use repository fallback" >&2',
        '    rm -rf "$NODE_PACKAGE_WORK/runtime-builder"',
        "  fi",
        "fi",
        "for BUNDLE_ROLE in sandbox builder; do",
        '  if [ "$BUNDLE_ROLE" = builder ]; then',
        '    BUNDLE_TARGET="$BUILDER_NODE_PACKAGE_BUNDLE"',
        '    BUNDLE_RUNTIME_DIR="$NODE_PACKAGE_WORK/runtime-builder"',
        '    BUNDLE_RUNTIME_READY="$BUILDER_RUNTIME_BUNDLE_READY"',
        f"    BUNDLE_PACKAGES={shlex.quote(builder_runtime_packages)}",
        "  else",
        '    BUNDLE_TARGET="$SANDBOX_NODE_PACKAGE_BUNDLE"',
        '    BUNDLE_RUNTIME_DIR="$NODE_PACKAGE_WORK/runtime"',
        '    BUNDLE_RUNTIME_READY="$RUNTIME_BUNDLE_READY"',
        f"    BUNDLE_PACKAGES={shlex.quote(sandbox_runtime_packages)}",
        "  fi",
        '  python3 - "$REMOTE_WHEEL" "$NODE_PACKAGE_WORK/wheels" "$BUNDLE_TARGET" '
        '"$BUNDLE_RUNTIME_DIR" "$BUNDLE_RUNTIME_READY" "$RUNTIME_OS_ID" '
        '"$RUNTIME_VERSION_ID" "$RUNTIME_CODENAME" "$RUNTIME_ARCHITECTURE" '
        '"$BUNDLE_ROLE" "$BUNDLE_PACKAGES" "$NODE_AGENT_RUNTIME_ARCHIVE" <<\'PY\'',
        "import hashlib",
        "import gzip",
        "import io",
        "import json",
        "import os",
        "from pathlib import Path",
        "import tarfile",
        "import sys",
        "",
        "wheel = Path(sys.argv[1])",
        "wheel_dir = Path(sys.argv[2])",
        "target = Path(sys.argv[3])",
        "runtime_dir = Path(sys.argv[4])",
        "runtime_ready = sys.argv[5] == '1'",
        "runtime_platform = {",
        "    'os_id': sys.argv[6],",
        "    'version_id': sys.argv[7],",
        "    'codename': sys.argv[8],",
        "    'architecture': sys.argv[9],",
        "}",
        "runtime_role = sys.argv[10]",
        "packages = sys.argv[11].split()",
        "agent_runtime_archive = Path(sys.argv[12])",
        "package_file = wheel_dir / wheel.name",
        "if not package_file.is_file():",
        "    raise SystemExit(f'pip download did not retain {wheel.name}')",
        "def sha256_file(path):",
        "    digest = hashlib.sha256()",
        "    with path.open('rb') as handle:",
        "        for chunk in iter(lambda: handle.read(1024 * 1024), b''):",
        "            digest.update(chunk)",
        "    return digest.hexdigest()",
        "",
        "manifest_payload = {'version': 1, 'package_file': wheel.name}",
        "if runtime_ready:",
        "    files = [",
        "        {'name': path.name, 'sha256': sha256_file(path), 'size': path.stat().st_size}",
        "        for path in sorted((runtime_dir / 'debs').glob('*.deb'), key=lambda item: item.name)",
        "    ]",
        "    if not files:",
        "        raise SystemExit('offline runtime package set is empty')",
        "    manifest_payload['runtime'] = {",
        "        'role': runtime_role,",
        "        'platform': runtime_platform,",
        "        'packages': packages,",
        "        'files': files,",
        "    }",
        "    if not agent_runtime_archive.is_file():",
        "        raise SystemExit('preassembled node-agent runtime is absent')",
        "    manifest_payload['runtime']['agent'] = {",
        "        'file': 'runtime/agent/node-agent-runtime.tar',",
        "        'python': f'{sys.version_info.major}.{sys.version_info.minor}',",
        "        'sha256': sha256_file(agent_runtime_archive),",
        "        'size': agent_runtime_archive.stat().st_size,",
        "    }",
        "    probe_archive = runtime_dir / 'images' / 'runtime-conformance-busybox.tar'",
        "    probe_inspect = runtime_dir / 'images' / 'runtime-conformance-busybox.inspect.json'",
        "    if probe_archive.is_file() and probe_inspect.is_file():",
        "        image = json.loads(probe_inspect.read_text(encoding='utf-8'))[0]",
        "        accepted_ids = {str(image.get('Id') or '')}",
        "        accepted_ids.update(",
        "            str(item).rsplit('@', 1)[-1]",
        "            for item in image.get('RepoDigests') or []",
        "            if '@sha256:' in str(item)",
        "        )",
        "        with tarfile.open(probe_archive, mode='r') as saved:",
        "            saved_manifest = json.load(saved.extractfile('manifest.json'))",
        "        config_name = Path(saved_manifest[0]['Config']).name.removesuffix('.json')",
        "        if len(config_name) == 64:",
        "            accepted_ids.add(f'sha256:{config_name}')",
        "        accepted_ids = sorted(",
        "            item for item in accepted_ids if item.startswith('sha256:')",
        "        )",
        "        manifest_payload['runtime']['probe_image'] = {",
        "            'reference': 'busybox',",
        "            'file': 'runtime/images/runtime-conformance-busybox.tar',",
        "            'image_id': image['Id'],",
        "            'accepted_ids': accepted_ids,",
        "            'os': image['Os'],",
        "            'architecture': image['Architecture'],",
        "            'sha256': sha256_file(probe_archive),",
        "            'size': probe_archive.stat().st_size,",
        "        }",
        "manifest = json.dumps(",
        "    manifest_payload,",
        "    sort_keys=True,",
        "    separators=(',', ':'),",
        ").encode('utf-8') + b'\\n'",
        "temporary = target.with_suffix(target.suffix + '.tmp')",
        "with temporary.open('wb') as raw:",
        "    with gzip.GzipFile(filename='', mode='wb', fileobj=raw, compresslevel=1, mtime=0) as compressed:",
        "        with tarfile.open(fileobj=compressed, mode='w|') as archive:",
        "            info = tarfile.TarInfo('package-bundle.json')",
        "            info.size = len(manifest)",
        "            info.mode = 0o644",
        "            info.mtime = 0",
        "            archive.addfile(info, io.BytesIO(manifest))",
        "            archive_paths = [",
        "                (path, f'wheels/{path.name}')",
        "                for path in sorted(wheel_dir.iterdir(), key=lambda item: item.name)",
        "            ]",
        "            if runtime_ready:",
        "                archive_paths.extend(",
        "                    (path, f'runtime/debs/{path.name}')",
        "                    for path in sorted((runtime_dir / 'debs').glob('*.deb'), key=lambda item: item.name)",
        "                )",
        "                probe_archive = runtime_dir / 'images' / 'runtime-conformance-busybox.tar'",
        "                if probe_archive.is_file():",
        "                    archive_paths.append((probe_archive, 'runtime/images/runtime-conformance-busybox.tar'))",
        "                archive_paths.append((agent_runtime_archive, 'runtime/agent/node-agent-runtime.tar'))",
        "            for path, arcname in archive_paths:",
        "                info = archive.gettarinfo(str(path), arcname=arcname)",
        "                info.uid = info.gid = 0",
        "                info.uname = info.gname = ''",
        "                info.mtime = 0",
        "                with path.open('rb') as handle:",
        "                    archive.addfile(info, handle)",
        "os.replace(temporary, target)",
        "target_digest = sha256_file(target)",
        "target.with_name(target.name + '.sha256').write_text(target_digest + '\\n', encoding='ascii')",
        "PY",
        "done",
        'rm -rf "$NODE_PACKAGE_WORK"',
        "trap - EXIT",
        "",
        "create_secret() {",
        '  path="$1"',
        '  if [ ! -s "$path" ]; then',
        "    umask 077",
        '    openssl rand -hex 32 > "$path"',
        "  fi",
        '  chmod 600 "$path"',
        '  sudo chown "$SERVICE_USER:$SERVICE_GROUP" "$path"',
        "}",
        f"create_secret {shlex.quote(plan.gateway_token_file)}",
        f"create_secret {shlex.quote(plan.heartbeat_token_file)}",
        f"create_secret {shlex.quote(plan.node_control_token_file)}",
        f"create_secret {shlex.quote(plan.relay_sandbox_token_file)}",
        f"create_secret {shlex.quote(plan.relay_worker_token_file)}",
        "",
        'if [ ! -s "$INIT_KEY" ]; then',
        '  ssh-keygen -t ed25519 -N "" -C "$INIT_KEY_COMMENT" -f "$INIT_KEY"',
        "fi",
        'chmod 600 "$INIT_KEY"',
        'chmod 644 "$INIT_KEY.pub"',
        'sudo chown "$SERVICE_USER:$SERVICE_GROUP" "$INIT_KEY" "$INIT_KEY.pub"',
        "",
        "sudo install -d -m 0755 /etc/ucloud-sandboxes",
    ]
    for path, content in env_files.items():
        script_parts.append(_install_root_file_snippet(path, content, mode="0640"))
    script_parts.extend(
        [
            "sudo sed -i "
            f"{shlex.quote('s|' + AUTO_REGISTRY_PRIVATE_IP_TOKEN + '|')}"
            '"$REGISTRY_PRIVATE_IP"'
            f"{shlex.quote('|g')} /etc/ucloud-sandboxes/autoscaler.env",
        ]
    )
    for path, content in unit_files.items():
        script_parts.append(_install_root_file_snippet(path, content, mode="0644"))
    script_parts.extend(
        [
            "sudo systemctl daemon-reload",
            "sudo systemctl enable --now ucloud-sandbox-registry.service",
            "sudo systemctl enable --now ucloud-sandbox-registry-prune.timer",
            "sudo systemctl enable --now ucloud-sandbox-registry-gc.timer",
            "sudo systemctl enable --now ucloud-sandbox-gateway.service",
            "sudo systemctl enable --now ucloud-sandbox-relay.service",
            "sudo systemctl enable --now ucloud-sandbox-autoscaler.service",
            "sudo systemctl restart ucloud-sandbox-registry.service",
            "sudo systemctl restart ucloud-sandbox-gateway.service",
            "sudo systemctl restart ucloud-sandbox-relay.service",
            "sudo systemctl restart ucloud-sandbox-autoscaler.service",
            "sleep 2",
            f"curl -fsS http://127.0.0.1:{plan.gateway_port}/healthz",
            "printf '\\n'",
            f"curl -fsS http://127.0.0.1:{plan.relay_port}/healthz",
            "printf '\\n'",
            f"curl -fsS http://127.0.0.1:{plan.registry_port}/v2/_catalog",
            "printf '\\n'",
        ]
    )
    return "\n".join(script_parts) + "\n"


def stage_file_over_ssh(
    ssh_command: str,
    local_path: Path,
    remote_path: str,
    *,
    mode: str = "0644",
    timeout_seconds: int | None = None,
    private_key_file: str | None = None,
) -> RemoteCommandResult:
    if not local_path.is_file():
        raise ValueError(f"local file not found: {local_path}")
    _reject_bad_text("remote path", remote_path)
    _reject_bad_text("mode", mode)
    remote_parent = str(PurePosixPath(remote_path).parent)
    remote_command = (
        f"mkdir -p {shlex.quote(remote_parent)} && "
        f"cat > {shlex.quote(remote_path)} && "
        f"chmod {shlex.quote(mode)} {shlex.quote(remote_path)}"
    )
    command = ssh_remote_command(
        ssh_command,
        remote_command,
        private_key_file=private_key_file,
    )
    completed = subprocess.run(
        command,
        input=local_path.read_bytes(),
        check=False,
        capture_output=True,
        timeout=timeout_seconds,
    )
    if completed.returncode != 0:
        raise ValueError(
            f"failed to stage {local_path} to {remote_path}: exit {completed.returncode}"
        )
    return RemoteCommandResult(
        command=command,
        returncode=completed.returncode,
        stdout=completed.stdout.decode("utf-8", errors="replace"),
        stderr=completed.stderr.decode("utf-8", errors="replace"),
    )


def run_remote_script_over_ssh(
    ssh_command: str,
    script: str,
    *,
    timeout_seconds: int | None = None,
    private_key_file: str | None = None,
) -> RemoteCommandResult:
    command = ssh_init_command(ssh_command, private_key_file=private_key_file)
    completed = subprocess.run(
        command,
        input=script,
        text=True,
        check=False,
        capture_output=True,
        timeout=timeout_seconds,
    )
    if completed.returncode != 0:
        raise ValueError(f"remote all-in-one deploy failed with exit {completed.returncode}")
    return RemoteCommandResult(
        command=command,
        returncode=completed.returncode,
        stdout=completed.stdout,
        stderr=completed.stderr,
    )


def read_remote_text_over_ssh(
    ssh_command: str,
    remote_path: str,
    *,
    timeout_seconds: int | None = None,
    private_key_file: str | None = None,
) -> str:
    command = ssh_remote_command(
        ssh_command,
        f"cat {shlex.quote(remote_path)}",
        private_key_file=private_key_file,
    )
    completed = subprocess.run(
        command,
        text=True,
        capture_output=True,
        check=False,
        timeout=timeout_seconds,
    )
    if completed.returncode != 0:
        raise ValueError(f"failed to read remote file {remote_path}: exit {completed.returncode}")
    return completed.stdout


def _install_root_file_snippet(path: str, content: str, *, mode: str) -> str:
    _reject_bad_text("install path", path)
    marker = "__UCLOUD_SANDBOX_DEPLOY_FILE__"
    if marker in content:
        raise ValueError("file content contains heredoc marker.")
    return "\n".join(
        [
            "tmp_file=$(mktemp)",
            f"cat > \"$tmp_file\" <<'{marker}'",
            content.rstrip("\n"),
            marker,
            f"sudo install -m {shlex.quote(mode)} \"$tmp_file\" {shlex.quote(path)}",
            "rm -f \"$tmp_file\"",
        ]
    )


def _systemd_env_quote(value: str) -> str:
    if value == "":
        return ""
    safe = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_@%+=:,./-"
    if all(char in safe for char in value):
        return value
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def _reject_bad_env_key(key: str) -> None:
    if not key:
        raise ValueError("environment key cannot be empty.")
    allowed = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_"
    if any(char not in allowed for char in key):
        raise ValueError(f"invalid environment key: {key}")
    if key[0].isdigit():
        raise ValueError(f"environment key cannot start with a digit: {key}")


def _reject_bad_text(label: str, value: str) -> None:
    if "\x00" in value or "\n" in value or "\r" in value:
        raise ValueError(f"{label} cannot contain control newlines.")
