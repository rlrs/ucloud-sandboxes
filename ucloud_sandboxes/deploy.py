from __future__ import annotations

from dataclasses import dataclass
from importlib import resources
from pathlib import Path, PurePosixPath
import shlex
import subprocess
from typing import Any

from .deployment import package_version
from .vm_init import ssh_init_command, ssh_remote_command


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
    max_init_per_cycle: int = 1
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
    def remote_session_file(self) -> str:
        return str(PurePosixPath(self.state_dir) / "ucloud-session.json")

    @property
    def gateway_token_file(self) -> str:
        return str(PurePosixPath(self.state_dir) / "gateway-token")

    @property
    def heartbeat_token_file(self) -> str:
        return str(PurePosixPath(self.state_dir) / "heartbeat-token")

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
        "UCLOUD_GATEWAY_TOKEN_FILE": plan.gateway_token_file,
        "UCLOUD_INIT_AUTHORIZED_KEY_FILE": plan.init_authorized_key_file,
        "UCLOUD_INIT_SSH_PRIVATE_KEY_FILE": plan.init_ssh_private_key_file,
        "UCLOUD_INIT_PACKAGE_SPEC": plan.remote_wheel_path,
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
    script_parts = [
        "#!/usr/bin/env bash",
        "set -euo pipefail",
        f"INSTALL_ROOT={shlex.quote(plan.install_root)}",
        f"STATE_DIR={shlex.quote(plan.state_dir)}",
        f"RELEASE_DIR={shlex.quote(plan.release_dir)}",
        f"VENV_DIR={shlex.quote(plan.venv_dir)}",
        f"REMOTE_WHEEL={shlex.quote(plan.remote_wheel_path)}",
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
        "",
        "sudo apt-get update",
        "sudo env DEBIAN_FRONTEND=noninteractive apt-get install -y "
        "curl docker.io openssh-client openssl python3-venv",
        "",
        'if [ ! -x "$VENV_DIR/bin/python" ]; then',
        '  python3 -m venv "$VENV_DIR"',
        "fi",
        'sudo chown -R "$SERVICE_USER:$SERVICE_GROUP" "$VENV_DIR"',
        '"$VENV_DIR/bin/pip" install --upgrade pip',
        '"$VENV_DIR/bin/pip" install --force-reinstall "$REMOTE_WHEEL"',
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
