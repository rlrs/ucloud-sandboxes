from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from pathlib import PurePosixPath
import shlex
import subprocess
from typing import Any

from .deployment import DEFAULT_INIT_VERSION, package_version
from .models import ResourceQuantity, VmJob, vm_job_from_payload


DEFAULT_WORK_DIR = "/work/ucloud-sandboxes"
DEFAULT_NODE_AGENT_HOST = "0.0.0.0"
DEFAULT_NODE_AGENT_PORT = 8090
DEFAULT_SSH_PORT_START = 22000
DEFAULT_SSH_PORT_END = 22999
DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 20
DEFAULT_PACKAGE_SPEC = "ucloud-sandboxes"
DEFAULT_DOCKER_QUOTA_IMAGE_GB = 200
DEFAULT_REMOTE_PACKAGE_DIR = "/tmp/ucloud-sandboxes-init-packages"
DEFAULT_SSH_OPTIONS = (
    "-o",
    "BatchMode=yes",
    "-o",
    "ConnectTimeout=30",
    "-o",
    "StrictHostKeyChecking=accept-new",
)


@dataclass(frozen=True)
class VmInitOptions:
    job_id: str
    heartbeat_url: str
    heartbeat_bearer_token_file: str = ""
    heartbeat_bearer_token: str = ""
    service_user: str = "ucloud"
    init_authorized_keys: tuple[str, ...] = ()
    node_id: str = ""
    work_dir: str = DEFAULT_WORK_DIR
    package_spec: str = DEFAULT_PACKAGE_SPEC
    node_agent_host: str = DEFAULT_NODE_AGENT_HOST
    node_agent_port: int = DEFAULT_NODE_AGENT_PORT
    node_url: str = ""
    agent_version: str = ""
    deployment_id: str = ""
    init_version: str = DEFAULT_INIT_VERSION
    ssh_port_start: int = DEFAULT_SSH_PORT_START
    ssh_port_end: int = DEFAULT_SSH_PORT_END
    total_resources: ResourceQuantity = ResourceQuantity()
    cpu_overcommit: float = 1.0
    memory_overcommit: float = 1.0
    disk_overcommit: float = 1.0
    docker_quota_image_gb: int = DEFAULT_DOCKER_QUOTA_IMAGE_GB
    enable_image_builds: bool = False
    runtime_dry_run: bool = False
    heartbeat_interval_seconds: int = DEFAULT_HEARTBEAT_INTERVAL_SECONDS
    labels: dict[str, str] | None = None

    def normalized_node_id(self) -> str:
        return self.node_id or f"ucloud-vm-{self.job_id}"

    def advertised_node_url(self) -> str:
        return self.node_url or f"http://{self.normalized_node_id()}:{self.node_agent_port}"

    def capabilities(self) -> tuple[str, ...]:
        if self.enable_image_builds:
            return ("image-cache", "image-build", "snapshot")
        return ("sandbox", "image-cache")


@dataclass(frozen=True)
class VmInitPlan:
    job: VmJob
    ssh_command: str | None
    runnable: bool
    reason: str


@dataclass(frozen=True)
class VmInitRunResult:
    command: tuple[str, ...]
    returncode: int


@dataclass(frozen=True)
class VmInitPackageStageResult:
    local_path: Path
    remote_path: str
    command: tuple[str, ...]
    returncode: int


def plan_vm_init(payload: dict[str, Any]) -> VmInitPlan:
    job = vm_job_from_payload(payload)
    ssh_command = extract_ssh_command(payload)
    if job.state != "RUNNING":
        return VmInitPlan(
            job=job,
            ssh_command=ssh_command,
            runnable=False,
            reason=f"VM is not running yet; current state is {job.state or 'unknown'}.",
        )
    if not ssh_command:
        return VmInitPlan(
            job=job,
            ssh_command=None,
            runnable=False,
            reason="No SSH access command has been announced by UCloud yet.",
        )
    return VmInitPlan(
        job=job,
        ssh_command=ssh_command,
        runnable=True,
        reason="VM is running and SSH access is available.",
    )


def extract_ssh_command(payload: dict[str, Any]) -> str | None:
    updates = payload.get("updates")
    if not isinstance(updates, list):
        return None
    for update in reversed(updates):
        if not isinstance(update, dict):
            continue
        status = update.get("status")
        if not isinstance(status, str):
            continue
        command = extract_ssh_command_from_text(status)
        if command:
            return command
    return None


def extract_ssh_command_from_text(text: str) -> str | None:
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        lower = line.lower()
        prefix = "SSH Access:"
        if line.startswith(prefix):
            candidate = line[len(prefix) :].strip()
            if candidate.lower().startswith("ssh "):
                return candidate
        marker = "Available at:"
        if line.startswith("SSH:") and marker in line:
            candidate = line[line.index(marker) + len(marker) :].strip()
            if candidate.lower().startswith("ssh "):
                return candidate
        if lower.startswith("ssh "):
            return line
    return None


def render_vm_init_script(options: VmInitOptions) -> str:
    validate_vm_init_options(options)
    work_dir = _clean_posix_path(options.work_dir)
    venv_dir = str(PurePosixPath(work_dir) / "venv")
    agent_bin = str(PurePosixPath(venv_dir) / "bin" / "ucloud-sandboxes")
    docker_data_root = str(PurePosixPath(work_dir) / "docker")
    docker_quota_image = str(PurePosixPath(work_dir) / "docker-xfs.img")
    docker_quota_root = str(PurePosixPath(work_dir) / "docker-xfs")
    state_dir = str(PurePosixPath(work_dir) / "state")
    runtime_conformance_file = str(PurePosixPath(state_dir) / "runtime-conformance.json")
    env_file = "/etc/ucloud-sandboxes/node.env"
    node_service = "/etc/systemd/system/ucloud-sandbox-node.service"
    heartbeat_service = "/etc/systemd/system/ucloud-sandbox-heartbeat.service"
    heartbeat_timer = "/etc/systemd/system/ucloud-sandbox-heartbeat.timer"
    authorized_keys_blob = "\n".join(options.init_authorized_keys)
    label_args = " ".join(
        f"--label {shlex.quote(key + '=' + value)}"
        for key, value in sorted((options.labels or {}).items())
    )
    build_flag = " --enable-image-builds" if options.enable_image_builds else ""
    runtime_flag = "" if options.runtime_dry_run else " --execute-runtime"
    deployment_flag = " --deployment-id ${UCLOUD_DEPLOYMENT_ID}" if options.deployment_id else ""
    heartbeat_auth_flag = (
        " --bearer-token-file ${UCLOUD_HEARTBEAT_BEARER_TOKEN_FILE}"
        if options.heartbeat_bearer_token_file
        else ""
    )
    version_flags = " --agent-version ${UCLOUD_AGENT_VERSION} --init-version ${UCLOUD_INIT_VERSION}"

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
UCLOUD_SERVICE_USER={shlex.quote(options.service_user)}
UCLOUD_WORK_DIR={shlex.quote(work_dir)}
UCLOUD_VENV_DIR={shlex.quote(venv_dir)}
UCLOUD_STATE_DIR={shlex.quote(state_dir)}
UCLOUD_DOCKER_DATA_ROOT={shlex.quote(docker_data_root)}
UCLOUD_PACKAGE_SPEC={shlex.quote(options.package_spec)}
UCLOUD_NODE_AGENT_HOST={shlex.quote(options.node_agent_host)}
UCLOUD_NODE_AGENT_PORT={options.node_agent_port}
UCLOUD_NODE_URL={shlex.quote(options.advertised_node_url())}
UCLOUD_AGENT_VERSION={shlex.quote(options.agent_version or package_version())}
UCLOUD_DEPLOYMENT_ID={shlex.quote(options.deployment_id)}
UCLOUD_INIT_VERSION={shlex.quote(options.init_version)}
UCLOUD_SSH_PORT_START={options.ssh_port_start}
UCLOUD_SSH_PORT_END={options.ssh_port_end}
UCLOUD_TOTAL_VCPU={options.total_resources.vcpu}
UCLOUD_TOTAL_MEMORY_MB={options.total_resources.memory_mb}
UCLOUD_TOTAL_DISK_MB={options.total_resources.disk_mb}
UCLOUD_CPU_OVERCOMMIT={options.cpu_overcommit}
UCLOUD_MEMORY_OVERCOMMIT={options.memory_overcommit}
UCLOUD_DISK_OVERCOMMIT={options.disk_overcommit}
UCLOUD_DOCKER_QUOTA_IMAGE_GB={options.docker_quota_image_gb}
UCLOUD_DOCKER_QUOTA_IMAGE={shlex.quote(docker_quota_image)}
UCLOUD_DOCKER_QUOTA_ROOT={shlex.quote(docker_quota_root)}
UCLOUD_RUNTIME_CONFORMANCE_FILE={shlex.quote(runtime_conformance_file)}
UCLOUD_INIT_AUTHORIZED_KEYS=$(cat <<'UCLOUD_AUTHORIZED_KEYS'
{authorized_keys_blob}
UCLOUD_AUTHORIZED_KEYS
)

echo "Initializing UCloud sandbox node $UCLOUD_NODE_ID for job $UCLOUD_JOB_ID"

if ! id "$UCLOUD_SERVICE_USER" >/dev/null 2>&1; then
  $SUDO useradd --create-home --shell /bin/bash "$UCLOUD_SERVICE_USER"
fi
UCLOUD_SERVICE_GROUP="$(id -gn "$UCLOUD_SERVICE_USER")"
UCLOUD_SERVICE_HOME="$(getent passwd "$UCLOUD_SERVICE_USER" | cut -d: -f6)"
if [ -z "$UCLOUD_SERVICE_HOME" ]; then
  echo "Could not determine home for $UCLOUD_SERVICE_USER" >&2
  exit 1
fi

run_as_service_user() {{
  if [ "$(id -un)" = "$UCLOUD_SERVICE_USER" ]; then
    "$@"
  elif [ "$(id -u)" -eq 0 ]; then
    runuser -u "$UCLOUD_SERVICE_USER" -- "$@"
  else
    sudo -u "$UCLOUD_SERVICE_USER" "$@"
  fi
}}

$SUDO mkdir -p "$UCLOUD_WORK_DIR" "$UCLOUD_STATE_DIR" /etc/ucloud-sandboxes
$SUDO chown "$UCLOUD_SERVICE_USER:$UCLOUD_SERVICE_GROUP" "$UCLOUD_WORK_DIR"
$SUDO chown -R "$UCLOUD_SERVICE_USER:$UCLOUD_SERVICE_GROUP" "$UCLOUD_STATE_DIR"
$SUDO chmod 700 "$UCLOUD_STATE_DIR"

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

echo "Installing base packages"
$SUDO apt-get update
$SUDO apt-get install -y ca-certificates curl gnupg apt-transport-https python3 python3-venv python3-pip xfsprogs

if ! command -v docker >/dev/null 2>&1; then
  echo "Installing Docker Engine"
  $SUDO install -m 0755 -d /etc/apt/keyrings
  $SUDO curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
  $SUDO chmod a+r /etc/apt/keyrings/docker.asc
  $SUDO tee /etc/apt/sources.list.d/docker.sources >/dev/null <<'DOCKER_SOURCES'
Types: deb
URIs: https://download.docker.com/linux/ubuntu
Suites: __UBUNTU_CODENAME__
Components: stable
Architectures: __ARCHITECTURE__
Signed-By: /etc/apt/keyrings/docker.asc
DOCKER_SOURCES
  UBUNTU_CODENAME="$(. /etc/os-release && echo "${{UBUNTU_CODENAME:-$VERSION_CODENAME}}")"
  ARCHITECTURE="$(dpkg --print-architecture)"
  $SUDO sed -i "s/__UBUNTU_CODENAME__/$UBUNTU_CODENAME/g; s/__ARCHITECTURE__/$ARCHITECTURE/g" /etc/apt/sources.list.d/docker.sources
  $SUDO apt-get update
  $SUDO apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
fi

if ! command -v runsc >/dev/null 2>&1; then
  echo "Installing gVisor runsc"
  $SUDO rm -f /usr/share/keyrings/gvisor-archive-keyring.gpg
  curl -fsSL https://gvisor.dev/archive.key | $SUDO gpg --dearmor -o /usr/share/keyrings/gvisor-archive-keyring.gpg
  echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/gvisor-archive-keyring.gpg] https://storage.googleapis.com/gvisor/releases release main" | $SUDO tee /etc/apt/sources.list.d/gvisor.list >/dev/null
  $SUDO apt-get update
  $SUDO apt-get install -y runsc
fi

if [ "$UCLOUD_DOCKER_QUOTA_IMAGE_GB" -gt 0 ]; then
  echo "Preparing XFS/project-quota Docker data root"
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

RUNSC_PATH="$(command -v runsc)"
export RUNSC_PATH UCLOUD_DOCKER_DATA_ROOT UCLOUD_DOCKER_QUOTA_IMAGE_GB
echo "Configuring Docker daemon"
$SUDO mkdir -p /etc/docker
python3 - <<'PY' | $SUDO tee /etc/docker/daemon.json >/dev/null
import json
import os

config = {{
    "data-root": os.environ["UCLOUD_DOCKER_DATA_ROOT"],
    "runtimes": {{"runsc": {{"path": os.environ["RUNSC_PATH"]}}}},
}}
if int(os.environ["UCLOUD_DOCKER_QUOTA_IMAGE_GB"]) > 0:
    config["storage-driver"] = "overlay2"
    config["features"] = {{"containerd-snapshotter": False}}
print(json.dumps(config, indent=2))
PY
$SUDO systemctl enable docker
$SUDO systemctl restart docker
$SUDO usermod -aG docker "$UCLOUD_SERVICE_USER"

echo "Installing ucloud-sandboxes package: $UCLOUD_PACKAGE_SPEC"
if [ -d "$UCLOUD_VENV_DIR" ]; then
  $SUDO chown -R "$UCLOUD_SERVICE_USER:$UCLOUD_SERVICE_GROUP" "$UCLOUD_VENV_DIR"
fi
run_as_service_user python3 -m venv "$UCLOUD_VENV_DIR"
run_as_service_user "$UCLOUD_VENV_DIR/bin/python" -m pip install --upgrade pip
run_as_service_user "$UCLOUD_VENV_DIR/bin/python" -m pip install --upgrade "$UCLOUD_PACKAGE_SPEC"

echo "Running runtime conformance probe"
set +e
$SUDO "$UCLOUD_VENV_DIR/bin/ucloud-sandboxes" runtime-conformance --sudo --execute --output json | $SUDO tee "$UCLOUD_RUNTIME_CONFORMANCE_FILE" >/dev/null
CONFORMANCE_STATUS=${{PIPESTATUS[0]}}
set -e
if [ "$CONFORMANCE_STATUS" -ne 0 ]; then
  echo "Runtime conformance failed; node will not advertise conformance-derived capabilities"
fi
$SUDO chown "$UCLOUD_SERVICE_USER:$UCLOUD_SERVICE_GROUP" "$UCLOUD_RUNTIME_CONFORMANCE_FILE" 2>/dev/null || true

echo "Writing node environment"
$SUDO tee {shlex.quote(env_file)} >/dev/null <<NODE_ENV
UCLOUD_JOB_ID=$UCLOUD_JOB_ID
UCLOUD_NODE_ID=$UCLOUD_NODE_ID
UCLOUD_HEARTBEAT_URL=$UCLOUD_HEARTBEAT_URL
UCLOUD_HEARTBEAT_BEARER_TOKEN_FILE=$UCLOUD_HEARTBEAT_BEARER_TOKEN_FILE
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
UCLOUD_SSH_PORT_START=$UCLOUD_SSH_PORT_START
UCLOUD_SSH_PORT_END=$UCLOUD_SSH_PORT_END
UCLOUD_TOTAL_VCPU=$UCLOUD_TOTAL_VCPU
UCLOUD_TOTAL_MEMORY_MB=$UCLOUD_TOTAL_MEMORY_MB
UCLOUD_TOTAL_DISK_MB=$UCLOUD_TOTAL_DISK_MB
UCLOUD_CPU_OVERCOMMIT=$UCLOUD_CPU_OVERCOMMIT
UCLOUD_MEMORY_OVERCOMMIT=$UCLOUD_MEMORY_OVERCOMMIT
UCLOUD_DISK_OVERCOMMIT=$UCLOUD_DISK_OVERCOMMIT
UCLOUD_DOCKER_DATA_ROOT=$UCLOUD_DOCKER_DATA_ROOT
UCLOUD_DOCKER_QUOTA_IMAGE_GB=$UCLOUD_DOCKER_QUOTA_IMAGE_GB
UCLOUD_DOCKER_QUOTA_IMAGE=$UCLOUD_DOCKER_QUOTA_IMAGE
UCLOUD_DOCKER_QUOTA_ROOT=$UCLOUD_DOCKER_QUOTA_ROOT
UCLOUD_RUNTIME_CONFORMANCE_FILE=$UCLOUD_RUNTIME_CONFORMANCE_FILE
NODE_ENV

echo "Writing node-agent systemd service"
$SUDO tee {shlex.quote(node_service)} >/dev/null <<NODE_SERVICE
[Unit]
Description=UCloud sandbox node agent
Wants=network-online.target docker.service
After=network-online.target docker.service
Requires=docker.service

[Service]
Type=simple
User=$UCLOUD_SERVICE_USER
Group=$UCLOUD_SERVICE_GROUP
SupplementaryGroups=docker
EnvironmentFile={env_file}
WorkingDirectory={work_dir}
ExecStart={agent_bin} serve-node-agent --job-id ${{UCLOUD_JOB_ID}} --node-id ${{UCLOUD_NODE_ID}} --node-url ${{UCLOUD_NODE_URL}} --host ${{UCLOUD_NODE_AGENT_HOST}} --port ${{UCLOUD_NODE_AGENT_PORT}}{deployment_flag}{version_flags} --sandbox-file ${{UCLOUD_STATE_DIR}}/sandboxes.json --image-file ${{UCLOUD_STATE_DIR}}/images.json --ssh-port-start ${{UCLOUD_SSH_PORT_START}} --ssh-port-end ${{UCLOUD_SSH_PORT_END}} --total-vcpu ${{UCLOUD_TOTAL_VCPU}} --total-memory-mb ${{UCLOUD_TOTAL_MEMORY_MB}} --total-disk-mb ${{UCLOUD_TOTAL_DISK_MB}} --cpu-overcommit ${{UCLOUD_CPU_OVERCOMMIT}} --memory-overcommit ${{UCLOUD_MEMORY_OVERCOMMIT}} --disk-overcommit ${{UCLOUD_DISK_OVERCOMMIT}} --runtime-conformance-file ${{UCLOUD_RUNTIME_CONFORMANCE_FILE}}{build_flag}{runtime_flag}
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
ExecStart={agent_bin} agent-heartbeat --from-node-agent-url http://127.0.0.1:${{UCLOUD_NODE_AGENT_PORT}} --post-url ${{UCLOUD_HEARTBEAT_URL}}{deployment_flag} {heartbeat_auth_flag} {label_args}
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
$SUDO systemctl enable ucloud-sandbox-node.service
$SUDO systemctl restart ucloud-sandbox-node.service
$SUDO systemctl enable --now ucloud-sandbox-heartbeat.timer
$SUDO systemctl start ucloud-sandbox-heartbeat.service

echo "UCloud sandbox node init complete. Waiting for heartbeat readiness in the control plane."
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
    if options.docker_quota_image_gb < 0:
        raise ValueError("docker quota image size cannot be negative.")
    _validate_service_user(options.service_user)
    for value_name, value in {
        "job id": options.job_id,
        "heartbeat url": options.heartbeat_url,
        "heartbeat bearer token file": options.heartbeat_bearer_token_file,
        "heartbeat bearer token": options.heartbeat_bearer_token,
        "service user": options.service_user,
        "node id": options.node_id,
        "node agent host": options.node_agent_host,
        "node url": options.node_url,
        "agent version": options.agent_version,
        "deployment id": options.deployment_id,
        "init version": options.init_version,
        "work dir": options.work_dir,
        "package spec": options.package_spec,
    }.items():
        _reject_newline(value_name, value)
    for key, value in (options.labels or {}).items():
        _reject_newline("label key", key)
        _reject_newline("label value", value)
        if "=" in key:
            raise ValueError("label keys cannot contain '='.")
    for key in options.init_authorized_keys:
        if not key.strip():
            raise ValueError("init authorized keys cannot contain empty keys.")
        _reject_newline("init authorized key", key)


def ssh_init_command(
    ssh_command: str,
    *,
    private_key_file: str | None = None,
) -> tuple[str, ...]:
    return (*ssh_command_with_options(ssh_command, private_key_file=private_key_file), "bash", "-s")


def ssh_remote_command(
    ssh_command: str,
    remote_command: str,
    *,
    private_key_file: str | None = None,
) -> tuple[str, ...]:
    if not remote_command:
        raise ValueError("remote command is required.")
    return (*ssh_command_with_options(ssh_command, private_key_file=private_key_file), remote_command)


def ssh_command_with_options(
    ssh_command: str,
    *,
    private_key_file: str | None = None,
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
    return (argv[0], *DEFAULT_SSH_OPTIONS, *private_key_args, *argv[1:])


def run_init_over_ssh(
    ssh_command: str,
    script: str,
    *,
    timeout_seconds: int | None = None,
    private_key_file: str | None = None,
) -> VmInitRunResult:
    command = ssh_init_command(ssh_command, private_key_file=private_key_file)
    completed = subprocess.run(
        command,
        input=script,
        text=True,
        check=False,
        timeout=timeout_seconds,
    )
    return VmInitRunResult(command=command, returncode=completed.returncode)


def local_package_spec_path(package_spec: str) -> Path | None:
    if not package_spec:
        return None
    if "://" in package_spec or package_spec.startswith(("git+", "hg+", "svn+", "bzr+")):
        return None
    path = Path(package_spec).expanduser()
    if not path.is_file():
        return None
    return path


def remote_package_spec_for_local_path(
    options: VmInitOptions,
    local_path: Path,
    *,
    remote_package_dir: str = DEFAULT_REMOTE_PACKAGE_DIR,
) -> str:
    _reject_newline("remote package dir", remote_package_dir)
    remote_dir = _clean_posix_path(remote_package_dir)
    filename = local_path.name
    if not filename or filename in {".", ".."} or "/" in filename:
        raise ValueError("local package path must have a valid filename.")
    _reject_newline("local package filename", filename)
    job_component = options.job_id.replace("/", "_").replace(":", "_")
    _reject_newline("job id", job_component)
    return str(PurePosixPath(remote_dir) / job_component / filename)


def stage_vm_init_package_over_ssh(
    ssh_command: str,
    options: VmInitOptions,
    *,
    timeout_seconds: int | None = None,
    private_key_file: str | None = None,
    remote_package_dir: str = DEFAULT_REMOTE_PACKAGE_DIR,
) -> VmInitPackageStageResult | None:
    local_path = local_package_spec_path(options.package_spec)
    if local_path is None:
        return None
    remote_path = remote_package_spec_for_local_path(
        options,
        local_path,
        remote_package_dir=remote_package_dir,
    )
    remote_parent = str(PurePosixPath(remote_path).parent)
    quoted_parent = shlex.quote(remote_parent)
    quoted_path = shlex.quote(remote_path)
    remote_command = (
        f"mkdir -p {quoted_parent} && "
        f"chmod 755 {quoted_parent} && "
        f"cat > {quoted_path} && "
        f"chmod 644 {quoted_path}"
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
        timeout=timeout_seconds,
    )
    return VmInitPackageStageResult(
        local_path=local_path,
        remote_path=remote_path,
        command=command,
        returncode=completed.returncode,
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
    if value.startswith("-"):
        raise ValueError("service user cannot start with '-'.")
    if "/" in value or ":" in value:
        raise ValueError("service user cannot contain '/' or ':'.")
    if any(character.isspace() for character in value):
        raise ValueError("service user cannot contain whitespace.")
