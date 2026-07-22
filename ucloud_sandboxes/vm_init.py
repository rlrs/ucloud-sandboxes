from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
import hashlib
import json
from pathlib import Path
from pathlib import PurePosixPath
import re
import shlex
import subprocess
import sys
from typing import Any

from .checkpoint_helper import render_checkpoint_helper_script
from .deployment import DEFAULT_INIT_VERSION, package_version
from .models import ResourceQuantity, VmJob, vm_job_from_payload
from .runsc_restore import render_runsc_restore_script


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
DEFAULT_REMOTE_PACKAGE_DIR = "/tmp/ucloud-sandboxes-init-packages"
DEFAULT_CHECKPOINT_HELPER = "/usr/local/libexec/ucloud-sandbox-checkpoint"
DEFAULT_CHECKPOINT_HELPER_CONFIG = "/etc/ucloud-sandboxes/checkpoint-helper.json"
DEFAULT_CHECKPOINT_HELPER_SUDOERS = "/etc/sudoers.d/ucloud-sandbox-checkpoint"
DEFAULT_RUNSC_RESTORE_WRAPPER = "/usr/local/libexec/ucloud-runsc-restore"
DEFAULT_RUNSC_RESTORE_CONFIG = "/etc/ucloud-sandboxes/runsc-restore.json"
DEFAULT_RUNSC_RESTORE_STATE_ROOT = "/run/ucloud-sandboxes/runsc-restore"
SANDBOX_RUNTIME_PACKAGES = (
    "xfsprogs",
    "docker-ce",
    "docker-ce-cli",
    "containerd.io",
    "runsc",
)
BUILDER_RUNTIME_PACKAGES = (
    *SANDBOX_RUNTIME_PACKAGES[:-1],
    "docker-buildx-plugin",
    SANDBOX_RUNTIME_PACKAGES[-1],
)
RUNTIME_KERNEL_MODULES = (
    "xfs",
    "overlay",
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
    swap_gb: int = DEFAULT_SWAP_GB
    docker_mtu: int = DEFAULT_DOCKER_MTU
    docker_insecure_registries: tuple[str, ...] = ()
    host_aliases: tuple[str, ...] = ()
    enable_image_builds: bool = False
    buildx_direct_push: bool = False
    buildx_cache_ref: str = ""
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
    agent_bin = str(PurePosixPath(work_dir) / "bin" / "ucloud-sandboxes")
    docker_storage_dir = _clean_posix_path(DEFAULT_DOCKER_STORAGE_DIR)
    docker_data_root = str(PurePosixPath(docker_storage_dir) / "docker")
    docker_quota_image = str(PurePosixPath(docker_storage_dir) / "docker-xfs.img")
    docker_quota_root = str(PurePosixPath(docker_storage_dir) / "docker-xfs")
    swap_file = str(PurePosixPath(docker_storage_dir) / "swapfile")
    state_dir = str(PurePosixPath(work_dir) / "state")
    runtime_conformance_file = str(PurePosixPath(state_dir) / "runtime-conformance.json")
    checkpoint_helper = DEFAULT_CHECKPOINT_HELPER
    checkpoint_helper_config = DEFAULT_CHECKPOINT_HELPER_CONFIG
    checkpoint_helper_sudoers = DEFAULT_CHECKPOINT_HELPER_SUDOERS
    checkpoint_helper_source = render_checkpoint_helper_script(
        config_path=checkpoint_helper_config
    )
    runsc_restore_wrapper = DEFAULT_RUNSC_RESTORE_WRAPPER
    runsc_restore_config = DEFAULT_RUNSC_RESTORE_CONFIG
    runsc_restore_state_root = DEFAULT_RUNSC_RESTORE_STATE_ROOT
    runsc_restore_source = render_runsc_restore_script(
        config_path=runsc_restore_config
    )
    env_file = "/etc/ucloud-sandboxes/node.env"
    node_service = "/etc/systemd/system/ucloud-sandbox-node.service"
    heartbeat_service = "/etc/systemd/system/ucloud-sandbox-heartbeat.service"
    heartbeat_timer = "/etc/systemd/system/ucloud-sandbox-heartbeat.timer"
    authorized_keys_blob = "\n".join(options.init_authorized_keys)
    runtime_role = "builder" if options.enable_image_builds else "sandbox"
    runtime_packages = (
        BUILDER_RUNTIME_PACKAGES
        if options.enable_image_builds
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
    build_flag = " --enable-image-builds" if options.enable_image_builds else ""
    if options.enable_image_builds and options.buildx_direct_push:
        build_flag += " --buildx-direct-push"
    if options.enable_image_builds and options.buildx_cache_ref:
        build_flag += f" --buildx-cache-ref {shlex.quote(options.buildx_cache_ref)}"
    runtime_flag = "" if options.runtime_dry_run else " --execute-runtime"
    deployment_flag = " --deployment-id ${UCLOUD_DEPLOYMENT_ID}" if options.deployment_id else ""
    heartbeat_auth_flag = (
        " --bearer-token-file ${UCLOUD_HEARTBEAT_BEARER_TOKEN_FILE}"
        if options.heartbeat_bearer_token_file
        else ""
    )
    node_control_auth_flag = (
        " --node-control-bearer-token-file ${UCLOUD_NODE_CONTROL_BEARER_TOKEN_FILE}"
        if options.node_control_bearer_token_file
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
UCLOUD_NODE_CONTROL_BEARER_TOKEN_FILE={shlex.quote(options.node_control_bearer_token_file)}
UCLOUD_NODE_CONTROL_BEARER_TOKEN={shlex.quote(options.node_control_bearer_token)}
UCLOUD_SERVICE_USER={shlex.quote(options.service_user)}
UCLOUD_WORK_DIR={shlex.quote(work_dir)}
UCLOUD_VENV_DIR={shlex.quote(venv_dir)}
UCLOUD_AGENT_BIN={shlex.quote(agent_bin)}
UCLOUD_STATE_DIR={shlex.quote(state_dir)}
UCLOUD_DOCKER_DATA_ROOT={shlex.quote(docker_data_root)}
UCLOUD_PACKAGE_SPEC={shlex.quote(options.package_spec)}
UCLOUD_PACKAGE_EXPECTED_SHA256={shlex.quote(options.package_sha256)}
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
UCLOUD_SWAP_GB={options.swap_gb}
UCLOUD_DOCKER_MTU={options.docker_mtu}
UCLOUD_DOCKER_QUOTA_IMAGE={shlex.quote(docker_quota_image)}
UCLOUD_DOCKER_QUOTA_ROOT={shlex.quote(docker_quota_root)}
UCLOUD_SWAP_FILE={shlex.quote(swap_file)}
UCLOUD_DOCKER_INSECURE_REGISTRIES_JSON={shlex.quote(json.dumps(list(options.docker_insecure_registries)))}
UCLOUD_HOST_ALIASES_JSON={shlex.quote(json.dumps(list(options.host_aliases)))}
UCLOUD_RUNTIME_CONFORMANCE_FILE={shlex.quote(runtime_conformance_file)}
UCLOUD_CHECKPOINT_HELPER={shlex.quote(checkpoint_helper)}
UCLOUD_CHECKPOINT_HELPER_CONFIG={shlex.quote(checkpoint_helper_config)}
UCLOUD_CHECKPOINT_HELPER_SUDOERS={shlex.quote(checkpoint_helper_sudoers)}
UCLOUD_RUNSC_RESTORE_WRAPPER={shlex.quote(runsc_restore_wrapper)}
UCLOUD_RUNSC_RESTORE_CONFIG={shlex.quote(runsc_restore_config)}
UCLOUD_RUNSC_RESTORE_STATE_ROOT={shlex.quote(runsc_restore_state_root)}
UCLOUD_INIT_AUTHORIZED_KEYS=$(cat <<'UCLOUD_AUTHORIZED_KEYS'
{authorized_keys_blob}
UCLOUD_AUTHORIZED_KEYS
)

echo "Initializing UCloud sandbox node $UCLOUD_NODE_ID for job $UCLOUD_JOB_ID"
UCLOUD_INIT_STARTED_MS="$(date +%s%3N)"
UCLOUD_INIT_PHASE_MS="$UCLOUD_INIT_STARTED_MS"

log_init_phase() {{
  local phase="$1"
  local now duration total
  now="$(date +%s%3N)"
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

run_as_service_user() {{
  if [ "$(id -un)" = "$UCLOUD_SERVICE_USER" ]; then
    "$@"
  elif [ "$(id -u)" -eq 0 ]; then
    runuser -u "$UCLOUD_SERVICE_USER" -- "$@"
  else
    sudo -u "$UCLOUD_SERVICE_USER" "$@"
  fi
}}

$SUDO mkdir -p "$UCLOUD_WORK_DIR" "$UCLOUD_STATE_DIR" "$(dirname "$UCLOUD_DOCKER_DATA_ROOT")" /etc/ucloud-sandboxes
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
UCLOUD_PACKAGE_INSTALL_SPEC="$UCLOUD_PACKAGE_SPEC"
UCLOUD_PACKAGE_INSTALL_ARGS=()
UCLOUD_PACKAGE_BUNDLE_DIR=""
UCLOUD_PACKAGE_BUNDLE_SHA256=""
UCLOUD_OFFLINE_RUNTIME_AVAILABLE=0
UCLOUD_OFFLINE_PROBE_IMAGE_ARCHIVE=""
UCLOUD_OFFLINE_PROBE_IMAGE_IDS=""
UCLOUD_PREBUILT_AGENT_ARCHIVE=""
UCLOUD_PREBUILT_AGENT_SHA256=""
UCLOUD_OFFLINE_KERNEL_MODULE_DIR=""
if [ -f "$UCLOUD_PACKAGE_SPEC" ] \
  && tar -tzf "$UCLOUD_PACKAGE_SPEC" package-bundle.json >/dev/null 2>&1; then
  UCLOUD_PACKAGE_BUNDLE_SHA256="$(sha256sum "$UCLOUD_PACKAGE_SPEC" | awk '{{print $1}}')"
  if [ -n "$UCLOUD_PACKAGE_EXPECTED_SHA256" ] \
    && [ "$UCLOUD_PACKAGE_BUNDLE_SHA256" != "$UCLOUD_PACKAGE_EXPECTED_SHA256" ]; then
    echo "Node package bundle checksum does not match the staged artifact" >&2
    exit 1
  fi
  UCLOUD_PACKAGE_BUNDLE_DIR="$UCLOUD_STATE_DIR/package-bundles/$UCLOUD_PACKAGE_BUNDLE_SHA256"
  if [ ! -f "$UCLOUD_PACKAGE_BUNDLE_DIR/.complete" ]; then
    UCLOUD_PACKAGE_BUNDLE_TMP="$UCLOUD_PACKAGE_BUNDLE_DIR.tmp.$$"
    rm -rf "$UCLOUD_PACKAGE_BUNDLE_TMP"
    mkdir -p "$UCLOUD_PACKAGE_BUNDLE_TMP"
    tar --no-same-owner --no-same-permissions -xzf "$UCLOUD_PACKAGE_SPEC" -C "$UCLOUD_PACKAGE_BUNDLE_TMP"
    touch "$UCLOUD_PACKAGE_BUNDLE_TMP/.complete"
    rm -rf "$UCLOUD_PACKAGE_BUNDLE_DIR"
    mv "$UCLOUD_PACKAGE_BUNDLE_TMP" "$UCLOUD_PACKAGE_BUNDLE_DIR"
  fi
  UCLOUD_PACKAGE_BUNDLE_FILE="$(python3 - "$UCLOUD_PACKAGE_BUNDLE_DIR/package-bundle.json" <<'PY'
import json
from pathlib import Path
import sys

manifest = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
if manifest.get("version") != 1:
    raise SystemExit("unsupported node package bundle version")
package_file = str(manifest.get("package_file") or "")
if not package_file or Path(package_file).name != package_file or not package_file.endswith(".whl"):
    raise SystemExit("invalid package_file in node package bundle")
print(package_file)
PY
)"
  UCLOUD_PACKAGE_INSTALL_SPEC="$UCLOUD_PACKAGE_BUNDLE_DIR/wheels/$UCLOUD_PACKAGE_BUNDLE_FILE"
  test -f "$UCLOUD_PACKAGE_INSTALL_SPEC"
  UCLOUD_PACKAGE_INSTALL_ARGS=(--no-index --find-links "$UCLOUD_PACKAGE_BUNDLE_DIR/wheels")
  echo "Using offline node package bundle $UCLOUD_PACKAGE_BUNDLE_SHA256"
  if [ -d "$UCLOUD_PACKAGE_BUNDLE_DIR/runtime" ]; then
    if python3 - \
      "$UCLOUD_PACKAGE_BUNDLE_DIR/package-bundle.json" \
      "$UCLOUD_PACKAGE_BUNDLE_DIR" \
      "$UCLOUD_OS_ID" "$UCLOUD_OS_VERSION_ID" "$UCLOUD_OS_CODENAME" \
      "$UCLOUD_ARCHITECTURE" "$UCLOUD_PACKAGE_EXPECTED_SHA256" <<'PY'
import hashlib
import json
import os
import re
from pathlib import Path
import sys

manifest_path = Path(sys.argv[1])
bundle_dir = Path(sys.argv[2])
expected_platform = {{
    "os_id": sys.argv[3],
    "version_id": sys.argv[4],
    "codename": sys.argv[5],
    "architecture": sys.argv[6],
}}
archive_digest_verified = bool(sys.argv[7])
manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
runtime = manifest.get("runtime")
if not isinstance(runtime, dict) or runtime.get("platform") != expected_platform:
    raise SystemExit("offline runtime platform does not match this VM")
if runtime.get("role") != {runtime_role!r}:
    raise SystemExit("offline runtime role does not match this VM")
expected_packages = {runtime_packages_python}
if runtime.get("packages") != expected_packages:
    raise SystemExit("invalid offline runtime package list")
package_dir = bundle_dir / "runtime" / "debs"
actual_files = {{path.name for path in package_dir.glob("*.deb")}}
declared_files = set()
files = runtime.get("files")
if not isinstance(files, list) or not files:
    raise SystemExit("offline runtime package set is empty")
for item in files:
    if not isinstance(item, dict):
        raise SystemExit("invalid offline runtime file")
    filename = str(item.get("name") or "")
    if Path(filename).name != filename or not filename.endswith(".deb"):
        raise SystemExit("invalid offline runtime filename")
    declared_files.add(filename)
    path = package_dir / filename
    if not path.is_file() or path.stat().st_size != item.get("size"):
        raise SystemExit(f"offline runtime file size mismatch: {{filename}}")
    if not archive_digest_verified:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        if digest.hexdigest() != item.get("sha256"):
            raise SystemExit(f"offline runtime file checksum mismatch: {{filename}}")
if actual_files != declared_files:
    raise SystemExit("offline runtime file set mismatch")
agent = runtime.get("agent")
if not isinstance(agent, dict):
    raise SystemExit("preassembled node-agent runtime metadata is absent")
if agent.get("file") != "runtime/agent/node-agent-runtime.tar":
    raise SystemExit("invalid preassembled node-agent runtime filename")
if agent.get("python") != f"{{sys.version_info.major}}.{{sys.version_info.minor}}":
    raise SystemExit("preassembled node-agent Python version does not match this VM")
agent_archive = bundle_dir / agent["file"]
if not agent_archive.is_file() or agent_archive.stat().st_size != agent.get("size"):
    raise SystemExit("preassembled node-agent runtime size mismatch")
agent_digest = hashlib.sha256()
with agent_archive.open("rb") as handle:
    for chunk in iter(lambda: handle.read(1024 * 1024), b""):
        agent_digest.update(chunk)
if agent_digest.hexdigest() != agent.get("sha256"):
    raise SystemExit("preassembled node-agent runtime checksum mismatch")
kernel = runtime.get("kernel")
if not isinstance(kernel, dict):
    raise SystemExit("offline kernel metadata is absent")
kernel_release = os.uname().release
if kernel.get("release") != kernel_release:
    raise SystemExit("offline kernel module release does not match this VM")
if kernel.get("load") != {runtime_kernel_modules_python}:
    raise SystemExit("offline kernel module load list does not match this runtime")
module_dir = bundle_dir / "runtime" / "kernel" / kernel_release
actual_modules = {{path.name for path in module_dir.glob("*.ko*")}}
declared_modules = set()
modules = kernel.get("files")
if not isinstance(modules, list) or not modules:
    raise SystemExit("offline kernel module closure is absent")
for module in modules:
    if not isinstance(module, dict):
        raise SystemExit("offline kernel module metadata is invalid")
    file_name = str(module.get("name") or "")
    if Path(file_name).name != file_name or not re.fullmatch(
        r"[A-Za-z0-9_.-]+\\.ko(?:\\.(?:gz|xz|zst))?", file_name
    ):
        raise SystemExit("invalid offline kernel module filename")
    declared_modules.add(file_name)
    module_path = module_dir / file_name
    if not module_path.is_file() or module_path.stat().st_size != module.get("size"):
        raise SystemExit(f"offline kernel module size mismatch: {{file_name}}")
    if not archive_digest_verified:
        module_digest = hashlib.sha256()
        with module_path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                module_digest.update(chunk)
        if module_digest.hexdigest() != module.get("sha256"):
            raise SystemExit(f"offline kernel module checksum mismatch: {{file_name}}")
if actual_modules != declared_modules:
    raise SystemExit("offline kernel module file set mismatch")
PY
    then
      UCLOUD_OFFLINE_RUNTIME_AVAILABLE=1
      echo "Verified offline Docker/gVisor packages for $UCLOUD_OS_ID $UCLOUD_OS_VERSION_ID $UCLOUD_ARCHITECTURE"
      UCLOUD_AGENT_RUNTIME_SPEC="$(python3 - "$UCLOUD_PACKAGE_BUNDLE_DIR/package-bundle.json" <<'PY'
import json
from pathlib import Path
import sys

runtime = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))["runtime"]
agent = runtime["agent"]
print(f"{{agent['sha256']}}\\t{{agent['size']}}")
PY
)"
      IFS=$'\t' read -r UCLOUD_PREBUILT_AGENT_SHA256 UCLOUD_PREBUILT_AGENT_SIZE <<< "$UCLOUD_AGENT_RUNTIME_SPEC"
      UCLOUD_PREBUILT_AGENT_ARCHIVE="$UCLOUD_PACKAGE_BUNDLE_DIR/runtime/agent/node-agent-runtime.tar"
      UCLOUD_OFFLINE_KERNEL_MODULE_DIR="$UCLOUD_PACKAGE_BUNDLE_DIR/runtime/kernel/$(uname -r)"
      UCLOUD_PROBE_IMAGE_ARCHIVE="$UCLOUD_PACKAGE_BUNDLE_DIR/runtime/images/runtime-conformance-busybox.tar"
      if [ -f "$UCLOUD_PROBE_IMAGE_ARCHIVE" ]; then
        if UCLOUD_PROBE_IMAGE_SPEC="$(python3 - \
          "$UCLOUD_PACKAGE_BUNDLE_DIR/package-bundle.json" \
          "$UCLOUD_ARCHITECTURE" <<'PY'
import json
from pathlib import Path
import sys

runtime = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8")).get("runtime")
probe = runtime.get("probe_image") if isinstance(runtime, dict) else None
if not isinstance(probe, dict):
    raise SystemExit("offline probe image metadata is absent")
if probe.get("reference") != "busybox":
    raise SystemExit("invalid offline probe image reference")
if probe.get("file") != "runtime/images/runtime-conformance-busybox.tar":
    raise SystemExit("invalid offline probe image filename")
if probe.get("os") != "linux" or probe.get("architecture") != sys.argv[2]:
    raise SystemExit("offline probe image platform does not match this VM")
accepted_ids = probe.get("accepted_ids")
if not isinstance(accepted_ids, list):
    accepted_ids = [probe.get("image_id")]
accepted_ids = [str(item or "") for item in accepted_ids]
checksum = str(probe.get("sha256") or "")
size = probe.get("size")
if (
    not accepted_ids
    or any(not item.startswith("sha256:") for item in accepted_ids)
    or len(checksum) != 64
    or not isinstance(size, int)
    or size <= 0
):
    raise SystemExit("invalid offline probe image metadata")
print(f"{{checksum}}\t{{size}}\t{{','.join(sorted(set(accepted_ids)))}}")
PY
)" \
          && IFS=$'\t' read -r UCLOUD_PROBE_IMAGE_SHA256 UCLOUD_PROBE_IMAGE_SIZE UCLOUD_PROBE_IMAGE_IDS <<< "$UCLOUD_PROBE_IMAGE_SPEC" \
          && [ "$(stat -c %s "$UCLOUD_PROBE_IMAGE_ARCHIVE")" = "$UCLOUD_PROBE_IMAGE_SIZE" ] \
          && printf '%s  %s\\n' "$UCLOUD_PROBE_IMAGE_SHA256" "$UCLOUD_PROBE_IMAGE_ARCHIVE" | sha256sum --check --status -; then
          UCLOUD_OFFLINE_PROBE_IMAGE_ARCHIVE="$UCLOUD_PROBE_IMAGE_ARCHIVE"
          UCLOUD_OFFLINE_PROBE_IMAGE_IDS="$UCLOUD_PROBE_IMAGE_IDS"
          echo "Verified offline busybox conformance image"
        else
          echo "WARNING: offline busybox conformance image is invalid; the probe may pull it" >&2
        fi
      fi
    else
      echo "WARNING: offline runtime bundle is incompatible or corrupt; using package repositories" >&2
    fi
  fi
fi
log_init_phase "package-bundle"

install_offline_runtime() {{
  local package_dir="$UCLOUD_PACKAGE_BUNDLE_DIR/runtime/debs"
  local package_file package_name candidate_version installed_version install_status
  local policy_rc_d_created=0
  local -a local_packages=()
  local -a portable_packages=()
  shopt -s nullglob
  local package_files=("$package_dir"/*.deb)
  shopt -u nullglob
  for package_file in "${{package_files[@]}}"; do
    package_name="$(dpkg-deb -f "$package_file" Package)"
    candidate_version="$(dpkg-deb -f "$package_file" Version)"
    installed_version=""
    if dpkg-query -W -f='${{Status}}' "$package_name" 2>/dev/null | grep -q "install ok installed"; then
      installed_version="$(dpkg-query -W -f='${{Version}}' "$package_name")"
    fi
    case "$package_name" in
      docker-ce|docker-ce-cli|containerd.io|docker-buildx-plugin|runsc)
        if [ -n "$installed_version" ] \
          && dpkg --compare-versions "$installed_version" ge "$candidate_version"; then
          continue
        fi
        portable_packages+=("$package_file")
        ;;
      *)
        # The stock VM already contains a coherent Ubuntu base. Do not turn
        # the portable bundle into a partial distribution upgrade merely
        # because the gateway downloaded a newer patch version.
        if [ -n "$installed_version" ]; then
          continue
        fi
        local_packages+=("$package_file")
        ;;
    esac
  done
  install_status=0
  if [ "${{#local_packages[@]}}" -gt 0 ]; then
    # Docker and containerd are configured and started once below. Prevent
    # support-package scripts from starting services with vendor defaults.
    if [ ! -e /usr/sbin/policy-rc.d ]; then
      printf '#!/bin/sh\nexit 101\n' | $SUDO tee /usr/sbin/policy-rc.d >/dev/null
      $SUDO chmod 0755 /usr/sbin/policy-rc.d
      policy_rc_d_created=1
    fi
    if $SUDO apt-get install --no-download --no-install-recommends -y \
      -o DPkg::Lock::Timeout=60 -o Dpkg::Use-Pty=0 "${{local_packages[@]}}"; then
      install_status=0
    else
      install_status=$?
    fi
  fi
  if [ "$policy_rc_d_created" -eq 1 ]; then
    $SUDO rm -f /usr/sbin/policy-rc.d
  fi
  if [ "$install_status" -ne 0 ]; then
    return "$install_status"
  fi
  # These vendor packages contain self-contained Go binaries and systemd
  # units. Extracting their bundle-verified payloads avoids dpkg database/fsync and
  # maintainer-script overhead on every ephemeral VM. The normal repository
  # path below remains the fallback if any command is unusable afterwards.
  for package_file in "${{portable_packages[@]}}"; do
    if ! dpkg-deb --fsys-tarfile "$package_file" \
      | $SUDO tar --extract --file=- --directory=/; then
      return 1
    fi
  done
  # containerd.io still ships this unit under /lib. The UCloud stock VM is
  # not merged-/usr and systemd searches /usr/lib/systemd/system instead.
  if [ -f /lib/systemd/system/containerd.service ]; then
    $SUDO install -m 0644 /lib/systemd/system/containerd.service \
      /usr/lib/systemd/system/containerd.service
  fi
  if ! getent group docker >/dev/null 2>&1; then
    $SUDO groupadd --system docker
  fi
  return "$install_status"
}}

required_packages_installed() {{
  local package
  for package in "$@"; do
    if ! dpkg-query -W -f='${{Status}}' "$package" 2>/dev/null | grep -q "install ok installed"; then
      return 1
    fi
  done
}}

OFFLINE_REQUIRED_PACKAGES=(xfsprogs)
NEED_DOCKER_REPOSITORY=0
NEED_GVISOR_REPOSITORY=0
command -v docker >/dev/null 2>&1 || NEED_DOCKER_REPOSITORY=1
command -v runsc >/dev/null 2>&1 || NEED_GVISOR_REPOSITORY=1
OFFLINE_RUNTIME_FAILED=0
if [ "$UCLOUD_OFFLINE_RUNTIME_AVAILABLE" -eq 1 ] \
  && [ "$NEED_DOCKER_REPOSITORY" -eq 1 ] \
  && [ "$NEED_GVISOR_REPOSITORY" -eq 1 ]; then
  echo "Installing base packages, Docker Engine, and gVisor from verified offline packages"
  if install_offline_runtime \
    && required_packages_installed "${{OFFLINE_REQUIRED_PACKAGES[@]}}" \
    && command -v docker >/dev/null 2>&1 \
    && command -v runsc >/dev/null 2>&1; then
    NEED_DOCKER_REPOSITORY=0
    NEED_GVISOR_REPOSITORY=0
    echo "Sandbox runtime installed without repository access"
  else
    OFFLINE_RUNTIME_FAILED=1
    echo "WARNING: offline runtime install failed; using package repository fallback" >&2
  fi
fi
log_init_phase "offline-runtime"

BASE_PACKAGES=(python3 xfsprogs)
if [ -z "$UCLOUD_PREBUILT_AGENT_ARCHIVE" ]; then
  BASE_PACKAGES+=(python3-venv)
fi
MISSING_BASE_PACKAGES=()
for package in "${{BASE_PACKAGES[@]}}"; do
  if ! dpkg-query -W -f='${{Status}}' "$package" 2>/dev/null | grep -q "install ok installed"; then
    MISSING_BASE_PACKAGES+=("$package")
  fi
done
NEED_BASE_REPOSITORY=0
if [ "${{#MISSING_BASE_PACKAGES[@]}}" -gt 0 ]; then
  NEED_BASE_REPOSITORY=1
fi
if [ "$OFFLINE_RUNTIME_FAILED" -eq 1 ]; then
  NEED_BASE_REPOSITORY=1
  NEED_DOCKER_REPOSITORY=1
  NEED_GVISOR_REPOSITORY=1
fi

APT_REPOSITORY_PACKAGES=()
if [ "$NEED_DOCKER_REPOSITORY" -eq 1 ] || [ "$NEED_GVISOR_REPOSITORY" -eq 1 ]; then
  APT_REPOSITORY_PACKAGES=(ca-certificates curl gnupg)
fi
MISSING_APT_REPOSITORY_PACKAGES=()
for package in "${{APT_REPOSITORY_PACKAGES[@]}}"; do
  if ! dpkg-query -W -f='${{Status}}' "$package" 2>/dev/null | grep -q "install ok installed"; then
    MISSING_APT_REPOSITORY_PACKAGES+=("$package")
  fi
done
if [ "${{#MISSING_APT_REPOSITORY_PACKAGES[@]}}" -gt 0 ]; then
  echo "Installing package-repository prerequisites: ${{MISSING_APT_REPOSITORY_PACKAGES[*]}}"
  $SUDO apt-get update
  $SUDO apt-get install --no-install-recommends -y \
    -o Dpkg::Use-Pty=0 "${{MISSING_APT_REPOSITORY_PACKAGES[@]}}"
fi
log_init_phase "repository-prerequisites"

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

CONTAINER_PACKAGES=()
$SUDO install -m 0755 -d /etc/apt/keyrings
UBUNTU_CODENAME="$UCLOUD_OS_CODENAME"
ARCHITECTURE="$UCLOUD_ARCHITECTURE"

if [ "$NEED_DOCKER_REPOSITORY" -eq 1 ]; then
  echo "Preparing Docker Engine repository"
  if [ ! -s /etc/apt/keyrings/docker.asc ]; then
    $SUDO curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
    $SUDO chmod a+r /etc/apt/keyrings/docker.asc
  fi
  $SUDO tee /etc/apt/sources.list.d/docker.sources >/dev/null <<DOCKER_SOURCES
Types: deb
URIs: https://download.docker.com/linux/ubuntu
Suites: $UBUNTU_CODENAME
Components: stable
Architectures: $ARCHITECTURE
Signed-By: /etc/apt/keyrings/docker.asc
DOCKER_SOURCES
  CONTAINER_PACKAGES+=(docker-ce docker-ce-cli containerd.io)
  if [ "{runtime_role}" = builder ]; then
    CONTAINER_PACKAGES+=(docker-buildx-plugin)
  fi
fi

if [ "$NEED_GVISOR_REPOSITORY" -eq 1 ]; then
  echo "Preparing gVisor runsc repository"
  if [ ! -s /usr/share/keyrings/gvisor-archive-keyring.gpg ]; then
    curl -fsSL https://gvisor.dev/archive.key | $SUDO gpg --dearmor -o /usr/share/keyrings/gvisor-archive-keyring.gpg
  fi
  echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/gvisor-archive-keyring.gpg] https://storage.googleapis.com/gvisor/releases release main" | $SUDO tee /etc/apt/sources.list.d/gvisor.list >/dev/null
  CONTAINER_PACKAGES+=(runsc)
fi

PACKAGES_TO_INSTALL=("${{MISSING_BASE_PACKAGES[@]}}" "${{CONTAINER_PACKAGES[@]}}")
if [ "${{#PACKAGES_TO_INSTALL[@]}}" -gt 0 ]; then
  echo "Installing base and container packages: ${{PACKAGES_TO_INSTALL[*]}}"
  $SUDO apt-get update
  $SUDO apt-get install --no-install-recommends -y \
    -o Dpkg::Use-Pty=0 "${{PACKAGES_TO_INSTALL[@]}}"
else
  echo "Base and container packages already installed"
fi
log_init_phase "base-packages"
log_init_phase "container-packages"

UCLOUD_RUNTIME_KERNEL_MODULES=({runtime_kernel_modules_shell})
if [ -n "$UCLOUD_OFFLINE_KERNEL_MODULE_DIR" ]; then
  UCLOUD_KERNEL_MODULE_TARGET="/lib/modules/$(uname -r)/updates/ucloud-sandboxes"
  UCLOUD_KERNEL_MODULE_MARKER="$UCLOUD_KERNEL_MODULE_TARGET/.bundle-sha256"
  if [ ! -f "$UCLOUD_KERNEL_MODULE_MARKER" ] \
    || [ "$(cat "$UCLOUD_KERNEL_MODULE_MARKER")" != "$UCLOUD_PACKAGE_BUNDLE_SHA256" ]; then
    echo "Installing bundled container-runtime kernel module closure"
    $SUDO rm -rf "$UCLOUD_KERNEL_MODULE_TARGET"
    $SUDO mkdir -p "$UCLOUD_KERNEL_MODULE_TARGET"
    for module_file in "$UCLOUD_OFFLINE_KERNEL_MODULE_DIR"/*.ko*; do
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
else
  echo "Installing container-runtime kernel module fallback"
  $SUDO apt-get update
  $SUDO apt-get install --no-install-recommends -y \
    -o Dpkg::Use-Pty=0 "linux-modules-extra-$(uname -r)"
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
UCLOUD_CHECKPOINT_ROOT="$UCLOUD_DOCKER_DATA_ROOT/ucloud-checkpoints"
log_init_phase "docker-storage"

if ! grep -qw overlay /proc/filesystems; then
  echo "overlay filesystem support is unavailable" >&2
  exit 1
fi

RUNSC_PATH="$(command -v runsc)"
export RUNSC_PATH UCLOUD_DOCKER_DATA_ROOT UCLOUD_CHECKPOINT_ROOT UCLOUD_RUNSC_RESTORE_WRAPPER UCLOUD_RUNSC_RESTORE_STATE_ROOT

echo "Installing raw runsc restore wrapper"
$SUDO install -d -m 0755 -o root -g root "$(dirname "$UCLOUD_RUNSC_RESTORE_WRAPPER")" /etc/ucloud-sandboxes
$SUDO install -d -m 0700 -o root -g root "$UCLOUD_CHECKPOINT_ROOT"
UCLOUD_RUNSC_RESTORE_TMP="$($SUDO mktemp "$(dirname "$UCLOUD_RUNSC_RESTORE_WRAPPER")/.ucloud-runsc-restore.XXXXXX")"
$SUDO tee "$UCLOUD_RUNSC_RESTORE_TMP" >/dev/null <<'UCLOUD_RUNSC_RESTORE_PY'
{runsc_restore_source}UCLOUD_RUNSC_RESTORE_PY
$SUDO chown root:root "$UCLOUD_RUNSC_RESTORE_TMP"
$SUDO chmod 0755 "$UCLOUD_RUNSC_RESTORE_TMP"
$SUDO mv -f "$UCLOUD_RUNSC_RESTORE_TMP" "$UCLOUD_RUNSC_RESTORE_WRAPPER"
UCLOUD_RUNSC_RESTORE_CONFIG_TMP="$($SUDO mktemp "/etc/ucloud-sandboxes/.runsc-restore.XXXXXX")"
python3 - <<'PY' | $SUDO tee "$UCLOUD_RUNSC_RESTORE_CONFIG_TMP" >/dev/null
import json
import os

print(json.dumps({{
    "version": 1,
    "real_runsc": os.environ["RUNSC_PATH"],
    "docker_root": os.environ["UCLOUD_DOCKER_DATA_ROOT"],
    "checkpoint_root": os.environ["UCLOUD_CHECKPOINT_ROOT"],
    "state_root": os.environ["UCLOUD_RUNSC_RESTORE_STATE_ROOT"],
}}, sort_keys=True))
PY
$SUDO chown root:root "$UCLOUD_RUNSC_RESTORE_CONFIG_TMP"
$SUDO chmod 0600 "$UCLOUD_RUNSC_RESTORE_CONFIG_TMP"
$SUDO mv -f "$UCLOUD_RUNSC_RESTORE_CONFIG_TMP" "$UCLOUD_RUNSC_RESTORE_CONFIG"

detect_default_route_mtu() {{
  local iface mtu
  iface="$(ip -o route get 1.1.1.1 2>/dev/null | awk '{{for (i=1; i<=NF; i++) if ($i=="dev") {{print $(i+1); exit}}}}')"
  if [ -z "$iface" ]; then
    iface="$(ip -o route show default 2>/dev/null | awk '{{for (i=1; i<=NF; i++) if ($i=="dev") {{print $(i+1); exit}}}}')"
  fi
  if [ -n "$iface" ] && [ -r "/sys/class/net/$iface/mtu" ]; then
    mtu="$(cat "/sys/class/net/$iface/mtu")"
  fi
  if ! [[ "${{mtu:-}}" =~ ^[0-9]+$ ]] || [ "$mtu" -lt 576 ]; then
    mtu=1420
  fi
  printf '%s\\n' "$mtu"
}}

if [ "$UCLOUD_DOCKER_MTU" -eq 0 ]; then
  UCLOUD_DOCKER_MTU="$(detect_default_route_mtu)"
fi
export RUNSC_PATH UCLOUD_DOCKER_DATA_ROOT UCLOUD_DOCKER_QUOTA_IMAGE_GB UCLOUD_DOCKER_MTU UCLOUD_DOCKER_INSECURE_REGISTRIES_JSON UCLOUD_CHECKPOINT_HELPER UCLOUD_CHECKPOINT_ROOT UCLOUD_RUNSC_RESTORE_WRAPPER UCLOUD_RUNSC_RESTORE_STATE_ROOT
echo "Configuring Docker daemon with bridge MTU $UCLOUD_DOCKER_MTU"
$SUDO mkdir -p /etc/docker
DOCKER_DAEMON_JSON="$(mktemp)"
python3 - <<'PY' > "$DOCKER_DAEMON_JSON"
import json
import os

config = {{
    "data-root": os.environ["UCLOUD_DOCKER_DATA_ROOT"],
    "experimental": True,
    "max-concurrent-downloads": 8,
    "max-concurrent-uploads": 8,
    "runtimes": {{
        "runsc": {{
            "path": os.environ["RUNSC_PATH"],
            "runtimeArgs": [
                "--allow-live-tcp-migration=false",
                "--net-disconnect-ok=true",
                "--allow-connected-on-save=false",
            ],
        }},
        "runsc-restore": {{
            "path": os.environ["UCLOUD_RUNSC_RESTORE_WRAPPER"],
            "runtimeArgs": [
                "--allow-live-tcp-migration=false",
                "--net-disconnect-ok=true",
                "--allow-connected-on-save=false",
            ],
        }},
    }},
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
$SUDO systemctl daemon-reload
$SUDO systemctl enable containerd.service
if ! systemctl is-active --quiet containerd.service; then
  if ! $SUDO systemctl restart containerd.service; then
    $SUDO journalctl -u containerd.service -n 80 --no-pager >&2 || true
    exit 1
  fi
fi
$SUDO dockerd --validate --config-file /etc/docker/daemon.json
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

echo "Installing ucloud-sandboxes package: $UCLOUD_PACKAGE_SPEC"
UCLOUD_PACKAGE_MARKER="$UCLOUD_STATE_DIR/installed-package.fingerprint"
UCLOUD_PACKAGE_FINGERPRINT="$UCLOUD_PACKAGE_SPEC"
if [ -n "$UCLOUD_PACKAGE_BUNDLE_SHA256" ]; then
  UCLOUD_PACKAGE_FINGERPRINT="$UCLOUD_PACKAGE_SPEC $UCLOUD_PACKAGE_BUNDLE_SHA256"
elif [ -f "$UCLOUD_PACKAGE_SPEC" ]; then
  UCLOUD_PACKAGE_FINGERPRINT="$UCLOUD_PACKAGE_SPEC $(sha256sum "$UCLOUD_PACKAGE_SPEC" | awk '{{print $1}}')"
fi
if [ -n "$UCLOUD_PREBUILT_AGENT_ARCHIVE" ]; then
  echo "Activating preassembled ucloud-sandboxes runtime"
  UCLOUD_AGENT_RUNTIME_DIR="$UCLOUD_STATE_DIR/agent-runtimes/$UCLOUD_PREBUILT_AGENT_SHA256"
  if [ ! -f "$UCLOUD_AGENT_RUNTIME_DIR/.complete" ]; then
    UCLOUD_AGENT_RUNTIME_TMP="$UCLOUD_AGENT_RUNTIME_DIR.tmp.$$"
    rm -rf "$UCLOUD_AGENT_RUNTIME_TMP"
    mkdir -p "$UCLOUD_AGENT_RUNTIME_TMP"
    tar --no-same-owner --no-same-permissions -xf "$UCLOUD_PREBUILT_AGENT_ARCHIVE" -C "$UCLOUD_AGENT_RUNTIME_TMP"
    test -d "$UCLOUD_AGENT_RUNTIME_TMP/site-packages/ucloud_sandboxes"
    touch "$UCLOUD_AGENT_RUNTIME_TMP/.complete"
    rm -rf "$UCLOUD_AGENT_RUNTIME_DIR"
    mv "$UCLOUD_AGENT_RUNTIME_TMP" "$UCLOUD_AGENT_RUNTIME_DIR"
  fi
  $SUDO install -d -m 0755 -o "$UCLOUD_SERVICE_USER" -g "$UCLOUD_SERVICE_GROUP" "$(dirname "$UCLOUD_AGENT_BIN")"
  UCLOUD_AGENT_LAUNCHER="$(mktemp)"
  printf '#!/bin/sh\nexec env PYTHONPATH=%q /usr/bin/python3 -m ucloud_sandboxes.cli "$@"\n' \
    "$UCLOUD_AGENT_RUNTIME_DIR/site-packages" > "$UCLOUD_AGENT_LAUNCHER"
  $SUDO install -m 0755 -o "$UCLOUD_SERVICE_USER" -g "$UCLOUD_SERVICE_GROUP" "$UCLOUD_AGENT_LAUNCHER" "$UCLOUD_AGENT_BIN"
  rm -f "$UCLOUD_AGENT_LAUNCHER"
else
  echo "Preassembled runtime unavailable; installing ucloud-sandboxes into a virtual environment"
  if [ -d "$UCLOUD_VENV_DIR" ]; then
    $SUDO chown -R "$UCLOUD_SERVICE_USER:$UCLOUD_SERVICE_GROUP" "$UCLOUD_VENV_DIR"
  fi
  run_as_service_user python3 -m venv "$UCLOUD_VENV_DIR"
  if [ -x "$UCLOUD_VENV_DIR/bin/ucloud-sandboxes" ] \
    && [ -f "$UCLOUD_PACKAGE_MARKER" ] \
    && grep -Fx -- "$UCLOUD_PACKAGE_FINGERPRINT" "$UCLOUD_PACKAGE_MARKER" >/dev/null 2>&1; then
    echo "ucloud-sandboxes package already installed for current fingerprint"
  else
    run_as_service_user "$UCLOUD_VENV_DIR/bin/python" -m pip install --disable-pip-version-check --upgrade "${{UCLOUD_PACKAGE_INSTALL_ARGS[@]}}" "$UCLOUD_PACKAGE_INSTALL_SPEC"
    printf '%s\n' "$UCLOUD_PACKAGE_FINGERPRINT" | $SUDO tee "$UCLOUD_PACKAGE_MARKER" >/dev/null
    $SUDO chown "$UCLOUD_SERVICE_USER:$UCLOUD_SERVICE_GROUP" "$UCLOUD_PACKAGE_MARKER"
  fi
  $SUDO install -d -m 0755 -o "$UCLOUD_SERVICE_USER" -g "$UCLOUD_SERVICE_GROUP" "$(dirname "$UCLOUD_AGENT_BIN")"
  $SUDO ln -sfn "$UCLOUD_VENV_DIR/bin/ucloud-sandboxes" "$UCLOUD_AGENT_BIN"
fi
log_init_phase "python-package"

echo "Installing privileged checkpoint helper"
$SUDO install -d -m 0755 -o root -g root "$(dirname "$UCLOUD_CHECKPOINT_HELPER")"
$SUDO install -d -m 0700 -o root -g root "$UCLOUD_CHECKPOINT_ROOT"
UCLOUD_CHECKPOINT_HELPER_TMP="$($SUDO mktemp "$(dirname "$UCLOUD_CHECKPOINT_HELPER")/.ucloud-checkpoint-helper.XXXXXX")"
$SUDO tee "$UCLOUD_CHECKPOINT_HELPER_TMP" >/dev/null <<'UCLOUD_CHECKPOINT_HELPER_PY'
{checkpoint_helper_source}UCLOUD_CHECKPOINT_HELPER_PY
$SUDO chown root:root "$UCLOUD_CHECKPOINT_HELPER_TMP"
$SUDO chmod 0755 "$UCLOUD_CHECKPOINT_HELPER_TMP"
$SUDO mv -f "$UCLOUD_CHECKPOINT_HELPER_TMP" "$UCLOUD_CHECKPOINT_HELPER"

UCLOUD_CHECKPOINT_CONFIG_TMP="$($SUDO mktemp "/etc/ucloud-sandboxes/.checkpoint-helper.XXXXXX")"
python3 - <<'PY' | $SUDO tee "$UCLOUD_CHECKPOINT_CONFIG_TMP" >/dev/null
import json
import os

print(json.dumps({{
    "version": 1,
    "docker_root": os.environ["UCLOUD_DOCKER_DATA_ROOT"],
    "checkpoint_root": os.environ["UCLOUD_CHECKPOINT_ROOT"],
}}, sort_keys=True))
PY
$SUDO chown root:root "$UCLOUD_CHECKPOINT_CONFIG_TMP"
$SUDO chmod 0600 "$UCLOUD_CHECKPOINT_CONFIG_TMP"
$SUDO mv -f "$UCLOUD_CHECKPOINT_CONFIG_TMP" "$UCLOUD_CHECKPOINT_HELPER_CONFIG"

UCLOUD_CHECKPOINT_SUDOERS_TMP="$($SUDO mktemp "/etc/sudoers.d/.ucloud-sandbox-checkpoint.XXXXXX")"
printf '%s ALL=(root) NOPASSWD: %s\n' "$UCLOUD_SERVICE_USER" "$UCLOUD_CHECKPOINT_HELPER" | $SUDO tee "$UCLOUD_CHECKPOINT_SUDOERS_TMP" >/dev/null
$SUDO chown root:root "$UCLOUD_CHECKPOINT_SUDOERS_TMP"
$SUDO chmod 0440 "$UCLOUD_CHECKPOINT_SUDOERS_TMP"
$SUDO visudo -cf "$UCLOUD_CHECKPOINT_SUDOERS_TMP" >/dev/null
$SUDO mv -f "$UCLOUD_CHECKPOINT_SUDOERS_TMP" "$UCLOUD_CHECKPOINT_HELPER_SUDOERS"
$SUDO "$UCLOUD_CHECKPOINT_HELPER" gc >/dev/null
log_init_phase "checkpoint-helper"

if [ -n "$UCLOUD_OFFLINE_PROBE_IMAGE_ARCHIVE" ]; then
  echo "Loading offline busybox conformance image"
  LOADED_PROBE_IMAGE_IDS=""
  probe_image_identity_matches() {{
    local expected actual
    IFS=',' read -ra expected_ids <<< "$UCLOUD_OFFLINE_PROBE_IMAGE_IDS"
    for expected in "${{expected_ids[@]}}"; do
      while read -r actual; do
        actual="${{actual##*@}}"
        if [ "$actual" = "$expected" ]; then
          return 0
        fi
      done < <(tr ' ' '\n' <<< "$LOADED_PROBE_IMAGE_IDS")
    done
    return 1
  }}
  if $SUDO docker load --input "$UCLOUD_OFFLINE_PROBE_IMAGE_ARCHIVE" >/dev/null \
    && LOADED_PROBE_IMAGE_IDS="$($SUDO docker image inspect --format '{{{{.Id}}}} {{{{range .RepoDigests}}}}{{{{.}}}} {{{{end}}}}' busybox 2>/dev/null)" \
    && probe_image_identity_matches; then
    echo "Loaded verified busybox conformance image without registry access"
  else
    echo "WARNING: offline busybox image load failed; the conformance probe may pull it" >&2
    $SUDO docker image rm --force busybox >/dev/null 2>&1 || true
  fi
fi

echo "Running runtime conformance probe"
set +e
$SUDO "$UCLOUD_AGENT_BIN" runtime-conformance --sudo --execute --output json --probe-live-fork --checkpoint-helper "$UCLOUD_CHECKPOINT_HELPER" --checkpoint-root "$UCLOUD_CHECKPOINT_ROOT" | $SUDO tee "$UCLOUD_RUNTIME_CONFORMANCE_FILE" >/dev/null
CONFORMANCE_STATUS=${{PIPESTATUS[0]}}
set -e
if [ "$CONFORMANCE_STATUS" -ne 0 ]; then
  echo "Runtime conformance failed; node will not advertise conformance-derived capabilities"
fi
$SUDO chown "$UCLOUD_SERVICE_USER:$UCLOUD_SERVICE_GROUP" "$UCLOUD_RUNTIME_CONFORMANCE_FILE" 2>/dev/null || true
log_init_phase "runtime-conformance"

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
UCLOUD_DOCKER_MTU=$UCLOUD_DOCKER_MTU
UCLOUD_DOCKER_QUOTA_IMAGE=$UCLOUD_DOCKER_QUOTA_IMAGE
UCLOUD_DOCKER_QUOTA_ROOT=$UCLOUD_DOCKER_QUOTA_ROOT
UCLOUD_DOCKER_INSECURE_REGISTRIES_JSON=$UCLOUD_DOCKER_INSECURE_REGISTRIES_JSON
UCLOUD_HOST_ALIASES_JSON=$UCLOUD_HOST_ALIASES_JSON
UCLOUD_RUNTIME_CONFORMANCE_FILE=$UCLOUD_RUNTIME_CONFORMANCE_FILE
UCLOUD_CHECKPOINT_HELPER=$UCLOUD_CHECKPOINT_HELPER
UCLOUD_CHECKPOINT_ROOT=$UCLOUD_CHECKPOINT_ROOT
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
ExecStartPre=/usr/bin/sudo -n ${{UCLOUD_CHECKPOINT_HELPER}} gc
ExecStart={agent_bin} serve-node-agent --job-id ${{UCLOUD_JOB_ID}} --node-id ${{UCLOUD_NODE_ID}} --node-url ${{UCLOUD_NODE_URL}} --host ${{UCLOUD_NODE_AGENT_HOST}} --port ${{UCLOUD_NODE_AGENT_PORT}}{deployment_flag}{version_flags} --sandbox-file ${{UCLOUD_STATE_DIR}}/sandboxes.json --image-file ${{UCLOUD_STATE_DIR}}/images.json --ssh-port-start ${{UCLOUD_SSH_PORT_START}} --ssh-port-end ${{UCLOUD_SSH_PORT_END}} --total-vcpu ${{UCLOUD_TOTAL_VCPU}} --total-memory-mb ${{UCLOUD_TOTAL_MEMORY_MB}} --total-disk-mb ${{UCLOUD_TOTAL_DISK_MB}} --cpu-overcommit ${{UCLOUD_CPU_OVERCOMMIT}} --memory-overcommit ${{UCLOUD_MEMORY_OVERCOMMIT}} --disk-overcommit ${{UCLOUD_DISK_OVERCOMMIT}} --runtime-conformance-file ${{UCLOUD_RUNTIME_CONFORMANCE_FILE}} --checkpoint-helper ${{UCLOUD_CHECKPOINT_HELPER}} --checkpoint-root ${{UCLOUD_CHECKPOINT_ROOT}}{build_flag}{runtime_flag}{node_control_auth_flag}
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
$SUDO systemctl enable ucloud-sandbox-node.service
$SUDO systemctl restart ucloud-sandbox-node.service
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
    if options.docker_quota_image_gb < 0:
        raise ValueError("docker quota image size cannot be negative.")
    if options.swap_gb < 0:
        raise ValueError("swap size cannot be negative.")
    if options.docker_mtu < 0:
        raise ValueError("docker mtu cannot be negative.")
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
        "node url": options.node_url,
        "agent version": options.agent_version,
        "deployment id": options.deployment_id,
        "init version": options.init_version,
        "work dir": options.work_dir,
        "package spec": options.package_spec,
        "package sha256": options.package_sha256,
    }.items():
        _reject_newline(value_name, value)
    if options.package_sha256 and (
        len(options.package_sha256) != 64
        or any(character not in "0123456789abcdef" for character in options.package_sha256)
    ):
        raise ValueError("package sha256 must be a lowercase SHA-256 digest.")
    if (
        options.node_control_bearer_token_file
        and not options.node_control_bearer_token
    ):
        raise ValueError(
            "node control bearer token is required when its file is configured."
        )
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
    if options.buildx_cache_ref and not options.buildx_direct_push:
        raise ValueError("buildx_cache_ref requires buildx_direct_push.")
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


def local_package_spec_path(package_spec: str) -> Path | None:
    if not package_spec:
        return None
    if "://" in package_spec or package_spec.startswith(("git+", "hg+", "svn+", "bzr+")):
        return None
    path = Path(package_spec).expanduser()
    if not path.is_file():
        return None
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
    known_hosts_file: str | None = None,
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
    remote_marker = f"{remote_path}.sha256"
    remote_temporary = f"{remote_path}.tmp"
    quoted_parent = shlex.quote(remote_parent)
    quoted_path = shlex.quote(remote_path)
    quoted_marker = shlex.quote(remote_marker)
    quoted_temporary = shlex.quote(remote_temporary)
    package_size = local_path.stat().st_size
    package_sha256 = local_package_sha256(local_path)
    probe_command = (
        f"test -f {quoted_path} && "
        f"test \"$(stat -c %s {quoted_path})\" = {package_size} && "
        f"test \"$(cat {quoted_marker} 2>/dev/null)\" = {package_sha256}"
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
        f"mkdir -p {quoted_parent} && "
        f"chmod 755 {quoted_parent} && "
        f"rm -f {quoted_temporary} && "
        f"cat > {quoted_temporary} && "
        f"test \"$(stat -c %s {quoted_temporary})\" = {package_size} && "
        f"chmod 644 {quoted_temporary} && "
        f"mv {quoted_temporary} {quoted_path} && "
        f"printf '%s\\n' {package_sha256} > {quoted_marker}"
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
