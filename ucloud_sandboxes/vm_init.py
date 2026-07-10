from __future__ import annotations

from dataclasses import dataclass
import json
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
DEFAULT_DOCKER_STORAGE_DIR = "/var/lib/ucloud-sandboxes"
DEFAULT_DOCKER_MTU = 0
DEFAULT_REMOTE_PACKAGE_DIR = "/tmp/ucloud-sandboxes-init-packages"
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
    docker_storage_dir = _clean_posix_path(DEFAULT_DOCKER_STORAGE_DIR)
    docker_data_root = str(PurePosixPath(docker_storage_dir) / "docker")
    docker_quota_image = str(PurePosixPath(docker_storage_dir) / "docker-xfs.img")
    docker_quota_root = str(PurePosixPath(docker_storage_dir) / "docker-xfs")
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
UCLOUD_DOCKER_MTU={options.docker_mtu}
UCLOUD_DOCKER_QUOTA_IMAGE={shlex.quote(docker_quota_image)}
UCLOUD_DOCKER_QUOTA_ROOT={shlex.quote(docker_quota_root)}
UCLOUD_DOCKER_INSECURE_REGISTRIES_JSON={shlex.quote(json.dumps(list(options.docker_insecure_registries)))}
UCLOUD_HOST_ALIASES_JSON={shlex.quote(json.dumps(list(options.host_aliases)))}
UCLOUD_RUNTIME_CONFORMANCE_FILE={shlex.quote(runtime_conformance_file)}
UCLOUD_INIT_AUTHORIZED_KEYS=$(cat <<'UCLOUD_AUTHORIZED_KEYS'
{authorized_keys_blob}
UCLOUD_AUTHORIZED_KEYS
)

echo "Initializing UCloud sandbox node $UCLOUD_NODE_ID for job $UCLOUD_JOB_ID"
UCLOUD_INIT_STARTED_EPOCH="$(date +%s)"
UCLOUD_INIT_PHASE_EPOCH="$UCLOUD_INIT_STARTED_EPOCH"

log_init_phase() {{
  local phase="$1"
  local now
  now="$(date +%s)"
  echo "Init phase complete: $phase phase=$((now - UCLOUD_INIT_PHASE_EPOCH))s total=$((now - UCLOUD_INIT_STARTED_EPOCH))s"
  UCLOUD_INIT_PHASE_EPOCH="$now"
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
UCLOUD_OFFLINE_RUNTIME_AVAILABLE=0
UCLOUD_OFFLINE_PROBE_IMAGE_ARCHIVE=""
UCLOUD_OFFLINE_PROBE_IMAGE_ID=""
if [ -f "$UCLOUD_PACKAGE_SPEC" ] \
  && tar -tzf "$UCLOUD_PACKAGE_SPEC" package-bundle.json >/dev/null 2>&1; then
  UCLOUD_PACKAGE_BUNDLE_SHA256="$(sha256sum "$UCLOUD_PACKAGE_SPEC" | awk '{{print $1}}')"
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
      "$UCLOUD_ARCHITECTURE" <<'PY'
import hashlib
import json
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
manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
runtime = manifest.get("runtime")
if not isinstance(runtime, dict) or runtime.get("platform") != expected_platform:
    raise SystemExit("offline runtime platform does not match this VM")
expected_packages = [
    "apt-transport-https", "python3", "python3-venv", "python3-pip", "xfsprogs",
    "docker-ce", "docker-ce-cli", "containerd.io", "docker-buildx-plugin",
    "docker-compose-plugin", "runsc",
]
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
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    if digest.hexdigest() != item.get("sha256"):
        raise SystemExit(f"offline runtime file checksum mismatch: {{filename}}")
if actual_files != declared_files:
    raise SystemExit("offline runtime file set mismatch")
PY
    then
      UCLOUD_OFFLINE_RUNTIME_AVAILABLE=1
      echo "Verified offline Docker/gVisor packages for $UCLOUD_OS_ID $UCLOUD_OS_VERSION_ID $UCLOUD_ARCHITECTURE"
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
image_id = str(probe.get("image_id") or "")
checksum = str(probe.get("sha256") or "")
size = probe.get("size")
if not image_id.startswith("sha256:") or len(checksum) != 64 or not isinstance(size, int) or size <= 0:
    raise SystemExit("invalid offline probe image metadata")
print(f"{{checksum}}\t{{size}}\t{{image_id}}")
PY
)" \
          && IFS=$'\t' read -r UCLOUD_PROBE_IMAGE_SHA256 UCLOUD_PROBE_IMAGE_SIZE UCLOUD_PROBE_IMAGE_ID <<< "$UCLOUD_PROBE_IMAGE_SPEC" \
          && [ "$(stat -c %s "$UCLOUD_PROBE_IMAGE_ARCHIVE")" = "$UCLOUD_PROBE_IMAGE_SIZE" ] \
          && printf '%s  %s\\n' "$UCLOUD_PROBE_IMAGE_SHA256" "$UCLOUD_PROBE_IMAGE_ARCHIVE" | sha256sum --check --status -; then
          UCLOUD_OFFLINE_PROBE_IMAGE_ARCHIVE="$UCLOUD_PROBE_IMAGE_ARCHIVE"
          UCLOUD_OFFLINE_PROBE_IMAGE_ID="$UCLOUD_PROBE_IMAGE_ID"
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
  local package_file package_name candidate_version installed_version
  local -a local_packages=()
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
    if [ -n "$installed_version" ] && dpkg --compare-versions "$installed_version" ge "$candidate_version"; then
      continue
    fi
    local_packages+=("$package_file")
  done
  if [ "${{#local_packages[@]}}" -eq 0 ]; then
    return 0
  fi
  $SUDO apt-get install --no-download --no-install-recommends -y "${{local_packages[@]}}"
}}

required_packages_installed() {{
  local package
  for package in "$@"; do
    if ! dpkg-query -W -f='${{Status}}' "$package" 2>/dev/null | grep -q "install ok installed"; then
      return 1
    fi
  done
}}

OFFLINE_REQUIRED_PACKAGES=(
  apt-transport-https python3 python3-venv python3-pip xfsprogs
  docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin runsc
)
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

BASE_PACKAGES=(apt-transport-https python3 python3-venv python3-pip xfsprogs)
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
  $SUDO apt-get install -y "${{MISSING_APT_REPOSITORY_PACKAGES[@]}}"
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
  CONTAINER_PACKAGES+=(docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin)
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
  $SUDO apt-get install -y "${{PACKAGES_TO_INSTALL[@]}}"
else
  echo "Base and container packages already installed"
fi
log_init_phase "base-packages"
log_init_phase "container-packages"

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
log_init_phase "docker-storage"

RUNSC_PATH="$(command -v runsc)"

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
export RUNSC_PATH UCLOUD_DOCKER_DATA_ROOT UCLOUD_DOCKER_QUOTA_IMAGE_GB UCLOUD_DOCKER_MTU UCLOUD_DOCKER_INSECURE_REGISTRIES_JSON
echo "Configuring Docker daemon with bridge MTU $UCLOUD_DOCKER_MTU"
$SUDO mkdir -p /etc/docker
DOCKER_DAEMON_JSON="$(mktemp)"
python3 - <<'PY' > "$DOCKER_DAEMON_JSON"
import json
import os

config = {{
    "data-root": os.environ["UCLOUD_DOCKER_DATA_ROOT"],
    "runtimes": {{"runsc": {{"path": os.environ["RUNSC_PATH"]}}}},
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
$SUDO systemctl enable docker
if [ "$UCLOUD_DOCKER_RESTART_NEEDED" -eq 1 ] || ! systemctl is-active --quiet docker; then
  $SUDO systemctl restart docker
else
  echo "Docker daemon already configured and running"
fi
if [ "$UCLOUD_DOCKER_MTU" -gt 0 ] && ip link show docker0 >/dev/null 2>&1; then
  $SUDO ip link set docker0 mtu "$UCLOUD_DOCKER_MTU" || true
fi
$SUDO usermod -aG docker "$UCLOUD_SERVICE_USER"
log_init_phase "docker-daemon"

echo "Installing ucloud-sandboxes package: $UCLOUD_PACKAGE_SPEC"
if [ -d "$UCLOUD_VENV_DIR" ]; then
  $SUDO chown -R "$UCLOUD_SERVICE_USER:$UCLOUD_SERVICE_GROUP" "$UCLOUD_VENV_DIR"
fi
run_as_service_user python3 -m venv "$UCLOUD_VENV_DIR"
UCLOUD_PACKAGE_MARKER="$UCLOUD_STATE_DIR/installed-package.fingerprint"
UCLOUD_PACKAGE_FINGERPRINT="$UCLOUD_PACKAGE_SPEC"
if [ -f "$UCLOUD_PACKAGE_SPEC" ]; then
  UCLOUD_PACKAGE_FINGERPRINT="$UCLOUD_PACKAGE_SPEC $(sha256sum "$UCLOUD_PACKAGE_SPEC" | awk '{{print $1}}')"
fi
if [ -x "$UCLOUD_VENV_DIR/bin/ucloud-sandboxes" ] \
  && [ -f "$UCLOUD_PACKAGE_MARKER" ] \
  && grep -Fx -- "$UCLOUD_PACKAGE_FINGERPRINT" "$UCLOUD_PACKAGE_MARKER" >/dev/null 2>&1; then
  echo "ucloud-sandboxes package already installed for current fingerprint"
else
  run_as_service_user "$UCLOUD_VENV_DIR/bin/python" -m pip install --disable-pip-version-check --upgrade "${{UCLOUD_PACKAGE_INSTALL_ARGS[@]}}" "$UCLOUD_PACKAGE_INSTALL_SPEC"
  printf '%s\n' "$UCLOUD_PACKAGE_FINGERPRINT" | $SUDO tee "$UCLOUD_PACKAGE_MARKER" >/dev/null
  $SUDO chown "$UCLOUD_SERVICE_USER:$UCLOUD_SERVICE_GROUP" "$UCLOUD_PACKAGE_MARKER"
fi
log_init_phase "python-package"

if [ -n "$UCLOUD_OFFLINE_PROBE_IMAGE_ARCHIVE" ]; then
  echo "Loading offline busybox conformance image"
  if $SUDO docker load --input "$UCLOUD_OFFLINE_PROBE_IMAGE_ARCHIVE" >/dev/null \
    && [ "$($SUDO docker image inspect --format '{{{{.Id}}}}' busybox 2>/dev/null)" = "$UCLOUD_OFFLINE_PROBE_IMAGE_ID" ]; then
    echo "Loaded verified busybox conformance image without registry access"
  else
    echo "WARNING: offline busybox image load failed; the conformance probe may pull it" >&2
    $SUDO docker image rm --force busybox >/dev/null 2>&1 || true
  fi
fi

echo "Running runtime conformance probe"
set +e
$SUDO "$UCLOUD_VENV_DIR/bin/ucloud-sandboxes" runtime-conformance --sudo --execute --output json | $SUDO tee "$UCLOUD_RUNTIME_CONFORMANCE_FILE" >/dev/null
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
ExecStart={agent_bin} serve-node-agent --job-id ${{UCLOUD_JOB_ID}} --node-id ${{UCLOUD_NODE_ID}} --node-url ${{UCLOUD_NODE_URL}} --host ${{UCLOUD_NODE_AGENT_HOST}} --port ${{UCLOUD_NODE_AGENT_PORT}}{deployment_flag}{version_flags} --sandbox-file ${{UCLOUD_STATE_DIR}}/sandboxes.json --image-file ${{UCLOUD_STATE_DIR}}/images.json --ssh-port-start ${{UCLOUD_SSH_PORT_START}} --ssh-port-end ${{UCLOUD_SSH_PORT_END}} --total-vcpu ${{UCLOUD_TOTAL_VCPU}} --total-memory-mb ${{UCLOUD_TOTAL_MEMORY_MB}} --total-disk-mb ${{UCLOUD_TOTAL_DISK_MB}} --cpu-overcommit ${{UCLOUD_CPU_OVERCOMMIT}} --memory-overcommit ${{UCLOUD_MEMORY_OVERCOMMIT}} --disk-overcommit ${{UCLOUD_DISK_OVERCOMMIT}} --runtime-conformance-file ${{UCLOUD_RUNTIME_CONFORMANCE_FILE}}{build_flag}{runtime_flag}{node_control_auth_flag}
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
    }.items():
        _reject_newline(value_name, value)
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
