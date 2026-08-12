from __future__ import annotations

from dataclasses import dataclass
from importlib import resources
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import shlex
import subprocess
from typing import Any

from .config import DeploymentConfig
from .deployment import package_version
from .vm_init import (
    BUILDER_RUNTIME_PACKAGES,
    PINNED_STORAGE_NATIVE_AGENTENV_COMMIT,
    RUNTIME_KERNEL_MODULES,
    SANDBOX_RUNTIME_PACKAGES,
    ssh_init_command,
    ssh_remote_command,
)


DEFAULT_INSTALL_ROOT = "/work/ucloud-sandboxes"
DEFAULT_PROJECT_MOUNT_DIR = "/work/data"
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
PERSISTENT_STATE_SYSTEMD_UNITS = (
    "ucloud-sandbox-gateway.service",
    "ucloud-sandbox-relay.service",
    "ucloud-sandbox-registry-prune.service",
    "ucloud-sandbox-autoscaler.service",
)
REGISTRY_STORAGE_SYSTEMD_UNITS = (
    "ucloud-sandbox-registry.service",
    "ucloud-sandbox-registry-gc.service",
)
# The bundle is resolved against an empty dpkg status so it remains usable on a
# freshly booted image.  APT still treats several Essential/systemd packages as
# ambient and can otherwise download a newer libsystemd0 without the matching
# service packages.  Include that version-locked closure explicitly.
BUNDLED_SYSTEMD_RUNTIME_PACKAGES = (
    "apparmor",
    "libnss-systemd",
    "libpam-systemd",
    "libsystemd-shared",
    "systemd",
    "systemd-cryptsetup",
    "systemd-resolved",
    "systemd-sysv",
    "udev",
)


@dataclass(frozen=True)
class StorageNativeBuildArtifacts:
    backend: Path
    manifest: Path
    license: Path
    metadata: dict[str, Any]


def storage_native_build_artifacts(
    manifest_path: Path,
) -> StorageNativeBuildArtifacts:
    manifest = manifest_path.resolve()
    if not manifest.is_file():
        raise ValueError(f"storage-native build manifest not found: {manifest}")
    try:
        payload = json.loads(manifest.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError("invalid storage-native build manifest JSON") from exc
    if not isinstance(payload, dict) or payload.get("schema") != 3:
        raise ValueError("unsupported storage-native build manifest")
    artifact_name = str(payload.get("artifact") or "")
    if (
        not artifact_name
        or Path(artifact_name).name != artifact_name
        or artifact_name in {".", ".."}
    ):
        raise ValueError("invalid storage-native backend artifact name")
    backend = manifest.parent / artifact_name
    license_path = manifest.parent / f"{artifact_name}.LICENSE"
    if not backend.is_file() or not license_path.is_file():
        raise ValueError(
            "storage-native backend binary and license must be beside its manifest"
        )
    expected_digest = str(payload.get("artifact_sha256") or "")
    patches = payload.get("patches")
    expected_patches = (
        "agentenv-streaming-dense-export.patch",
        "agentenv-pooled-delete.patch",
        "agentenv-owner-identity.patch",
    )
    patches_valid = (
        isinstance(patches, list)
        and len(patches) == len(expected_patches)
        and all(
            isinstance(item, dict)
            and item.get("name") == expected_name
            and re.fullmatch(r"[0-9a-f]{64}", str(item.get("sha256") or ""))
            for item, expected_name in zip(patches, expected_patches, strict=True)
        )
    )
    if (
        payload.get("agentenv_commit") != PINNED_STORAGE_NATIVE_AGENTENV_COMMIT
        or payload.get("cargo_package") != "uvm-ublk-daemon"
        or payload.get("license") != "MIT"
        or payload.get("host_architecture") not in {"x86_64", "aarch64"}
        or not patches_valid
        or not re.fullmatch(r"[0-9a-f]{64}", expected_digest)
    ):
        raise ValueError("storage-native backend provenance is not pinned")
    if _sha256_file(backend) != expected_digest:
        raise ValueError("storage-native backend digest does not match its manifest")
    return StorageNativeBuildArtifacts(
        backend=backend,
        manifest=manifest,
        license=license_path,
        metadata=payload,
    )


@dataclass(frozen=True)
class AllInOneDeployPlan:
    job_id: str
    config: DeploymentConfig
    local_wheel: Path
    local_direct_runsc: Path | None = None
    local_managed_init: Path | None = None
    local_storage_native_manifest: Path | None = None

    @property
    def release_dir(self) -> str:
        return str(PurePosixPath(DEFAULT_INSTALL_ROOT) / "release")

    @property
    def venv_dir(self) -> str:
        return str(PurePosixPath(DEFAULT_INSTALL_ROOT) / "gateway-venv")

    @property
    def remote_wheel_path(self) -> str:
        return str(PurePosixPath(self.release_dir) / self.local_wheel.name)

    @property
    def remote_direct_runsc_path(self) -> str:
        return str(PurePosixPath(self.release_dir) / "ucloud-direct-runsc")

    @property
    def remote_managed_init_path(self) -> str:
        return str(PurePosixPath(self.release_dir) / "ucloud-sandbox-init")

    @property
    def remote_storage_native_backend_path(self) -> str:
        return str(PurePosixPath(self.release_dir) / "storage-native-backend")

    @property
    def remote_storage_native_manifest_path(self) -> str:
        return str(
            PurePosixPath(self.release_dir) / "storage-native-build-manifest.json"
        )

    @property
    def remote_storage_native_license_path(self) -> str:
        return str(PurePosixPath(self.release_dir) / "storage-native-LICENSE")

    @property
    def staged_session_file(self) -> str:
        return str(PurePosixPath(self.release_dir) / ".deploy-ucloud-session.json")

    @property
    def remote_session_file(self) -> str:
        return str(self.config.session_file())

    @property
    def sandbox_node_package_bundle_path(self) -> str:
        return str(self.config.sandbox_node_package_bundle())

    @property
    def builder_node_package_bundle_path(self) -> str:
        return str(self.config.builder_node_package_bundle())

    @property
    def install_root(self) -> str:
        return DEFAULT_INSTALL_ROOT

    @property
    def project_mount_dir(self) -> str:
        return DEFAULT_PROJECT_MOUNT_DIR

    @property
    def service_user(self) -> str:
        return "ucloud"

    @property
    def registry_data_dir(self) -> str:
        return str(self.config.registry_data_dir())

    @property
    def gateway_token_file(self) -> str:
        return str(self.config.gateway_token_file())

    @property
    def heartbeat_token_file(self) -> str:
        return str(self.config.heartbeat_token_file())

    @property
    def node_control_token_file(self) -> str:
        return str(self.config.node_control_token_file())

    @property
    def relay_sandbox_token_file(self) -> str:
        return str(self.config.relay_sandbox_token_file())

    @property
    def relay_worker_token_file(self) -> str:
        return str(self.config.relay_worker_token_file())

    @property
    def relay_state_file(self) -> str:
        return str(self.config.relay_state_file())

    @property
    def init_ssh_private_key_file(self) -> str:
        return str(self.config.init_ssh_private_key_file())

    @property
    def init_authorized_key_file(self) -> str:
        return str(self.config.init_authorized_key_file())

    @property
    def direct_runsc_commit(self) -> str:
        return self.config.sandbox.direct_runsc_commit

    @property
    def package_version(self) -> str:
        return package_version()

    def validate(self) -> None:
        if not self.job_id.strip():
            raise ValueError("job id is required")
        if not self.local_wheel.is_file():
            raise ValueError(f"wheel file not found: {self.local_wheel}")
        if self.local_direct_runsc is None or not self.local_direct_runsc.is_file():
            raise ValueError("deployment requires a local patched runsc binary")
        if self.local_managed_init is None or not self.local_managed_init.is_file():
            raise ValueError("deployment requires a local managed-process init binary")
        if self.local_storage_native_manifest is None:
            raise ValueError(
                "deployment requires a pinned storage-native build manifest"
            )
        storage_native_build_artifacts(self.local_storage_native_manifest)

    def to_dict(self) -> dict[str, Any]:
        return {
            "jobId": self.job_id,
            "deployment": self.config.to_dict(),
            "packageVersion": self.package_version,
            "localWheel": str(self.local_wheel),
            "remoteWheelPath": self.remote_wheel_path,
            "localDirectRunsc": str(self.local_direct_runsc),
            "remoteDirectRunscPath": self.remote_direct_runsc_path,
            "localManagedInit": str(self.local_managed_init),
            "remoteManagedInitPath": self.remote_managed_init_path,
            "localStorageNativeManifest": str(self.local_storage_native_manifest),
            "sandboxNodePackageBundlePath": self.sandbox_node_package_bundle_path,
            "builderNodePackageBundlePath": self.builder_node_package_bundle_path,
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
    storage_artifacts = (
        storage_native_build_artifacts(plan.local_storage_native_manifest)
        if plan.local_storage_native_manifest is not None
        else None
    )
    unit_texts = units if units is not None else packaged_systemd_units()
    deployment_json = (
        json.dumps(
            plan.config.to_dict(),
            indent=2,
            sort_keys=True,
        )
        + "\n"
    )
    unit_files = {
        f"/etc/systemd/system/{name}": unit_texts[name] for name in SYSTEMD_UNIT_NAMES
    }

    def mount_dropin(mount_point: str) -> str:
        return "\n".join(
            (
                "[Unit]",
                f"RequiresMountsFor={mount_point}",
                "",
                "[Service]",
                f"ExecStartPre=/usr/bin/mountpoint -q {mount_point}",
                "",
            )
        )

    persistent_storage_dropin = mount_dropin(plan.project_mount_dir)
    registry_storage_dropin = mount_dropin(plan.config.registry_mount_point)
    node_runtime_packages = " ".join(SANDBOX_RUNTIME_PACKAGES)
    builder_runtime_packages = " ".join(BUILDER_RUNTIME_PACKAGES)
    runtime_kernel_modules = " ".join(RUNTIME_KERNEL_MODULES)
    bundled_systemd_runtime_packages = " ".join(BUNDLED_SYSTEMD_RUNTIME_PACKAGES)
    script_parts = [
        "#!/usr/bin/env bash",
        "set -euo pipefail",
        f"INSTALL_ROOT={shlex.quote(plan.install_root)}",
        f"PROJECT_MOUNT_DIR={shlex.quote(plan.project_mount_dir)}",
        f"DATA_ROOT={shlex.quote(plan.config.data_root)}",
        f"REGISTRY_MOUNT_POINT={shlex.quote(plan.config.registry_mount_point)}",
        f"REGISTRY_DATA_ROOT={shlex.quote(plan.config.registry_data_root)}",
        f"RELEASE_DIR={shlex.quote(plan.release_dir)}",
        f"VENV_DIR={shlex.quote(plan.venv_dir)}",
        f"REMOTE_WHEEL={shlex.quote(plan.remote_wheel_path)}",
        f"DIRECT_RUNSC={shlex.quote(plan.remote_direct_runsc_path)}",
        f"DIRECT_RUNSC_COMMIT={shlex.quote(plan.direct_runsc_commit)}",
        f"MANAGED_INIT={shlex.quote(plan.remote_managed_init_path)}",
        f"STORAGE_NATIVE_BACKEND={shlex.quote(plan.remote_storage_native_backend_path if storage_artifacts is not None else '')}",
        f"STORAGE_NATIVE_MANIFEST={shlex.quote(plan.remote_storage_native_manifest_path if storage_artifacts is not None else '')}",
        f"STORAGE_NATIVE_LICENSE={shlex.quote(plan.remote_storage_native_license_path if storage_artifacts is not None else '')}",
        f"SANDBOX_NODE_PACKAGE_BUNDLE={shlex.quote(plan.sandbox_node_package_bundle_path)}",
        f"BUILDER_NODE_PACKAGE_BUNDLE={shlex.quote(plan.builder_node_package_bundle_path)}",
        f"SERVICE_USER={shlex.quote(plan.service_user)}",
        f"SESSION_FILE={shlex.quote(plan.remote_session_file)}",
        f"STAGED_SESSION_FILE={shlex.quote(plan.staged_session_file)}",
        f"INIT_KEY={shlex.quote(plan.init_ssh_private_key_file)}",
        f"INIT_KEY_COMMENT={shlex.quote(plan.config.deployment_id + ' gateway init')}",
        "",
        'SERVICE_GROUP="$(id -gn "$SERVICE_USER")"',
        'if ! mountpoint -q "$PROJECT_MOUNT_DIR"; then',
        '  echo "Persistent project drive is not mounted at $PROJECT_MOUNT_DIR" >&2',
        "  exit 1",
        "fi",
        'if ! mountpoint -q "$REGISTRY_MOUNT_POINT"; then',
        '  echo "Registry storage is not mounted at $REGISTRY_MOUNT_POINT" >&2',
        "  exit 1",
        "fi",
        'if [ ! -s "$STAGED_SESSION_FILE" ] && [ ! -s "$SESSION_FILE" ]; then',
        '  echo "No staged or persistent UCloud session is available" >&2',
        "  exit 1",
        "fi",
        'sudo install -d -m 0755 -o "$SERVICE_USER" -g "$SERVICE_GROUP" "$INSTALL_ROOT"',
        'sudo install -d -m 0755 -o "$SERVICE_USER" -g "$SERVICE_GROUP" "$RELEASE_DIR"',
        'test -s "$REMOTE_WHEEL"',
        'if [ -n "$DIRECT_RUNSC" ]; then test -x "$DIRECT_RUNSC"; fi',
        'if [ -n "$MANAGED_INIT" ]; then test -x "$MANAGED_INIT"; fi',
        'if [ -n "$STORAGE_NATIVE_BACKEND" ]; then',
        '  test -x "$STORAGE_NATIVE_BACKEND"',
        '  test -s "$STORAGE_NATIVE_MANIFEST"',
        '  test -s "$STORAGE_NATIVE_LICENSE"',
        "fi",
        "sudo apt-get update",
        "DEPLOY_SUPPORT_PACKAGES=(ca-certificates curl docker.io openssh-client "
        "openssl python3-venv)",
        'DEPLOY_MODULES_EXTRA="linux-modules-extra-$(uname -r)"',
        'if apt-cache show "$DEPLOY_MODULES_EXTRA" >/dev/null 2>&1; then',
        '  DEPLOY_SUPPORT_PACKAGES+=("$DEPLOY_MODULES_EXTRA")',
        "fi",
        "sudo env DEBIAN_FRONTEND=noninteractive apt-get install -y "
        '"${DEPLOY_SUPPORT_PACKAGES[@]}"',
        "",
        'if [ ! -x "$VENV_DIR/bin/python" ]; then',
        '  python3 -m venv "$VENV_DIR"',
        "fi",
        'sudo chown -R "$SERVICE_USER:$SERVICE_GROUP" "$VENV_DIR"',
        '"$VENV_DIR/bin/pip" install --upgrade pip',
        'NODE_PACKAGE_WORK="$(mktemp -d)"',
        "trap 'rm -rf \"$NODE_PACKAGE_WORK\"' EXIT",
        'NODE_AGENT_RUNTIME_DIR="$NODE_PACKAGE_WORK/node-agent-runtime"',
        'NODE_AGENT_RUNTIME_ARCHIVE="$NODE_PACKAGE_WORK/node-agent-runtime.tar"',
        'mkdir -p "$NODE_AGENT_RUNTIME_DIR/site-packages"',
        '"$VENV_DIR/bin/pip" install --disable-pip-version-check '
        '--no-compile --target "$NODE_AGENT_RUNTIME_DIR/site-packages" "$REMOTE_WHEEL"',
        "tar --sort=name --mtime=@0 --owner=0 --group=0 --numeric-owner "
        '-cf "$NODE_AGENT_RUNTIME_ARCHIVE" -C "$NODE_AGENT_RUNTIME_DIR" .',
        'RUNTIME_OS_ID="$(. /etc/os-release && printf \'%s\' "$ID")"',
        'RUNTIME_VERSION_ID="$(. /etc/os-release && printf \'%s\' "$VERSION_ID")"',
        'RUNTIME_CODENAME="$(. /etc/os-release && printf \'%s\' "${UBUNTU_CODENAME:-${VERSION_CODENAME:-}}")"',
        'RUNTIME_ARCHITECTURE="$(dpkg --print-architecture)"',
        'RUNTIME_KERNEL_RELEASE="$(uname -r)"',
        'RUNTIME_KERNEL_MODULE_DIR="$NODE_PACKAGE_WORK/kernel-modules/$RUNTIME_KERNEL_RELEASE"',
        f"RUNTIME_KERNEL_MODULES={shlex.quote(runtime_kernel_modules)}",
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
        "collect_runtime_kernel_modules() {",
        '  rm -rf "$RUNTIME_KERNEL_MODULE_DIR"',
        '  mkdir -p "$RUNTIME_KERNEL_MODULE_DIR"',
        '  module_paths="$(for module in $RUNTIME_KERNEL_MODULES; do',
        '    sudo modprobe --show-depends "$module" || exit 1',
        '  done | awk \'$1 == "insmod" {print $2}\' | sort -u)" || return 1',
        '  [ -n "$module_paths" ] || return 1',
        "  while IFS= read -r module_path; do",
        '    [ -f "$module_path" ] || return 1',
        '    module_target="$RUNTIME_KERNEL_MODULE_DIR/${module_path##*/}"',
        '    if [ -f "$module_target" ] && ! cmp -s "$module_path" "$module_target"; then',
        '      echo "Conflicting kernel module basename: ${module_path##*/}" >&2',
        "      return 1",
        "    fi",
        '    cp "$module_path" "$module_target" || return 1',
        '  done <<< "$module_paths"',
        "  find \"$RUNTIME_KERNEL_MODULE_DIR\" -type f -name '*.ko*' -print -quit | grep -q .",
        "}",
        "build_runtime_bundle() {",
        '  if [ "$RUNTIME_OS_ID" != ubuntu ] || [ -z "$RUNTIME_CODENAME" ]; then',
        '    echo "Verified runtime bundles require Ubuntu" >&2',
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
        "  sudo apt-get update || return 1",
        "  collect_runtime_kernel_modules || return 1",
        f"  download_runtime_packages runtime {node_runtime_packages} {bundled_systemd_runtime_packages} || return 1",
        '  sudo chmod -R a+rX "$NODE_PACKAGE_WORK/runtime" || return 1',
        "}",
        "build_runtime_bundle",
        'cp -a "$NODE_PACKAGE_WORK/runtime" "$NODE_PACKAGE_WORK/runtime-builder"',
        "download_runtime_packages runtime-builder docker-buildx-plugin",
        'sudo chmod -R a+rX "$NODE_PACKAGE_WORK/runtime-builder"',
        "for BUNDLE_ROLE in sandbox builder; do",
        '  if [ "$BUNDLE_ROLE" = builder ]; then',
        '    BUNDLE_TARGET="$BUILDER_NODE_PACKAGE_BUNDLE"',
        '    BUNDLE_RUNTIME_DIR="$NODE_PACKAGE_WORK/runtime-builder"',
        f"    BUNDLE_PACKAGES={shlex.quote(builder_runtime_packages)}",
        "  else",
        '    BUNDLE_TARGET="$SANDBOX_NODE_PACKAGE_BUNDLE"',
        '    BUNDLE_RUNTIME_DIR="$NODE_PACKAGE_WORK/runtime"',
        f"    BUNDLE_PACKAGES={shlex.quote(node_runtime_packages)}",
        "  fi",
        '  python3 - "$BUNDLE_TARGET" "$BUNDLE_RUNTIME_DIR" "$RUNTIME_OS_ID" '
        '"$RUNTIME_VERSION_ID" "$RUNTIME_CODENAME" "$RUNTIME_ARCHITECTURE" '
        '"$BUNDLE_ROLE" "$BUNDLE_PACKAGES" "$NODE_AGENT_RUNTIME_ARCHIVE" '
        '"$RUNTIME_KERNEL_RELEASE" "$RUNTIME_KERNEL_MODULE_DIR" '
        '"$RUNTIME_KERNEL_MODULES" "$DIRECT_RUNSC" "$DIRECT_RUNSC_COMMIT" '
        '"$STORAGE_NATIVE_BACKEND" "$STORAGE_NATIVE_MANIFEST" '
        '"$STORAGE_NATIVE_LICENSE" "$MANAGED_INIT" '
        "<<'PY'",
        "import hashlib",
        "import gzip",
        "import io",
        "import json",
        "import os",
        "from pathlib import Path",
        "import re",
        "import tarfile",
        "import sys",
        "",
        "target = Path(sys.argv[1])",
        "runtime_dir = Path(sys.argv[2])",
        "runtime_platform = {",
        "    'os_id': sys.argv[3],",
        "    'version_id': sys.argv[4],",
        "    'codename': sys.argv[5],",
        "    'architecture': sys.argv[6],",
        "}",
        "runtime_role = sys.argv[7]",
        "packages = sys.argv[8].split()",
        "agent_runtime_archive = Path(sys.argv[9])",
        "kernel_release = sys.argv[10]",
        "kernel_module_dir = Path(sys.argv[11])",
        "kernel_load_modules = sys.argv[12].split()",
        "direct_runsc = Path(sys.argv[13]) if sys.argv[13] else None",
        "direct_runsc_commit = sys.argv[14]",
        "storage_backend = Path(sys.argv[15]) if sys.argv[15] else None",
        "storage_manifest = Path(sys.argv[16]) if sys.argv[16] else None",
        "storage_license = Path(sys.argv[17]) if sys.argv[17] else None",
        "managed_init = Path(sys.argv[18]) if sys.argv[18] else None",
        "def sha256_file(path):",
        "    digest = hashlib.sha256()",
        "    with path.open('rb') as handle:",
        "        for chunk in iter(lambda: handle.read(1024 * 1024), b''):",
        "            digest.update(chunk)",
        "    return digest.hexdigest()",
        "",
        "manifest_payload = {'version': 1}",
        "files = [",
        "    {'name': path.name, 'sha256': sha256_file(path), 'size': path.stat().st_size}",
        "    for path in sorted((runtime_dir / 'debs').glob('*.deb'), key=lambda item: item.name)",
        "]",
        "if not files:",
        "    raise SystemExit('bundled runtime package set is empty')",
        "manifest_payload['runtime'] = {",
        "    'role': runtime_role,",
        "    'platform': runtime_platform,",
        "    'packages': packages,",
        "    'files': files,",
        "}",
        "if not agent_runtime_archive.is_file():",
        "    raise SystemExit('preassembled node-agent runtime is absent')",
        "manifest_payload['runtime']['agent'] = {",
        "    'file': 'runtime/agent/node-agent-runtime.tar',",
        "    'python': f'{sys.version_info.major}.{sys.version_info.minor}',",
        "    'sha256': sha256_file(agent_runtime_archive),",
        "    'size': agent_runtime_archive.stat().st_size,",
        "}",
        "kernel_module_files = sorted(kernel_module_dir.glob('*.ko*'))",
        "if not kernel_module_files or not kernel_load_modules:",
        "    raise SystemExit('kernel module closure is absent')",
        "manifest_payload['runtime']['kernel'] = {",
        "    'release': kernel_release,",
        "    'load': kernel_load_modules,",
        "    'files': [",
        "        {",
        "            'name': module_path.name,",
        "            'sha256': sha256_file(module_path),",
        "            'size': module_path.stat().st_size,",
        "        }",
        "        for module_path in kernel_module_files",
        "    ],",
        "}",
        "if runtime_role == 'sandbox':",
        "    if direct_runsc is None or not direct_runsc.is_file() or len(direct_runsc_commit) != 40:",
        "        raise SystemExit('direct runsc artifact is invalid')",
        "    if managed_init is None or not managed_init.is_file():",
        "        raise SystemExit('managed-process init artifact is absent')",
        "    manifest_payload['runtime']['direct_runsc'] = {",
        "        'commit': direct_runsc_commit,",
        "        'file': 'runtime/direct/runsc',",
        "        'sha256': sha256_file(direct_runsc),",
        "        'size': direct_runsc.stat().st_size,",
        "    }",
        "    manifest_payload['runtime']['managed_init'] = {",
        "        'file': 'runtime/direct/ucloud-sandbox-init',",
        "        'sha256': sha256_file(managed_init),",
        "        'size': managed_init.stat().st_size,",
        "    }",
        "    if storage_backend is None or storage_manifest is None or storage_license is None:",
        "        raise SystemExit('storage-native build artifacts are absent')",
        "    if not all(path.is_file() for path in (storage_backend, storage_manifest, storage_license)):",
        "        raise SystemExit('storage-native build artifact is missing')",
        "    storage_build = json.loads(storage_manifest.read_text(encoding='utf-8'))",
        f"    if storage_build.get('agentenv_commit') != {PINNED_STORAGE_NATIVE_AGENTENV_COMMIT!r}:",
        "        raise SystemExit('storage-native AgentEnv commit is not pinned')",
        "    if storage_build.get('schema') != 3 or storage_build.get('license') != 'MIT':",
        "        raise SystemExit('invalid storage-native build provenance')",
        "    storage_patches = storage_build.get('patches')",
        "    expected_storage_patches = ['agentenv-streaming-dense-export.patch', 'agentenv-pooled-delete.patch', 'agentenv-owner-identity.patch']",
        "    if not isinstance(storage_patches, list) or [item.get('name') for item in storage_patches if isinstance(item, dict)] != expected_storage_patches:",
        "        raise SystemExit('invalid storage-native patch set')",
        "    if not all(re.fullmatch(r'[0-9a-f]{64}', str(item.get('sha256') or '')) for item in storage_patches):",
        "        raise SystemExit('invalid storage-native patch digest')",
        "    storage_digest = sha256_file(storage_backend)",
        "    if storage_build.get('artifact_sha256') != storage_digest:",
        "        raise SystemExit('storage-native backend digest mismatch')",
        "    manifest_payload['runtime']['storage_native'] = {",
        "        'agentenv_commit': storage_build['agentenv_commit'],",
        "        'file': 'runtime/storage-native/backend',",
        "        'host_architecture': storage_build.get('host_architecture'),",
        "        'license_file': 'runtime/storage-native/LICENSE',",
        "        'license_sha256': sha256_file(storage_license),",
        "        'manifest_file': 'runtime/storage-native/build-manifest.json',",
        "        'manifest_sha256': sha256_file(storage_manifest),",
        "        'sha256': storage_digest,",
        "        'size': storage_backend.stat().st_size,",
        "    }",
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
        "            archive_paths = []",
        "            archive_paths.extend(",
        "                (path, f'runtime/debs/{path.name}')",
        "                for path in sorted((runtime_dir / 'debs').glob('*.deb'), key=lambda item: item.name)",
        "            )",
        "            archive_paths.append((agent_runtime_archive, 'runtime/agent/node-agent-runtime.tar'))",
        "            archive_paths.extend(",
        "                (path, f'runtime/kernel/{kernel_release}/{path.name}')",
        "                for path in kernel_module_files",
        "            )",
        "            if runtime_role == 'sandbox':",
        "                archive_paths.append((direct_runsc, 'runtime/direct/runsc'))",
        "                archive_paths.append((managed_init, 'runtime/direct/ucloud-sandbox-init'))",
        "                archive_paths.extend((",
        "                    (storage_backend, 'runtime/storage-native/backend'),",
        "                    (storage_manifest, 'runtime/storage-native/build-manifest.json'),",
        "                    (storage_license, 'runtime/storage-native/LICENSE'),",
        "                ))",
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
        # Publish package metadata only after every node artifact is complete.
        '"$VENV_DIR/bin/pip" install --force-reinstall "$REMOTE_WHEEL"',
        "",
        "for unit in \\",
        "  ucloud-sandbox-registry-prune.timer \\",
        "  ucloud-sandbox-registry-gc.timer \\",
        "  ucloud-sandbox-autoscaler.service \\",
        "  ucloud-sandbox-gateway.service \\",
        "  ucloud-sandbox-relay.service \\",
        "  ucloud-sandbox-registry-prune.service \\",
        "  ucloud-sandbox-registry-gc.service; do",
        '  if sudo systemctl cat "$unit" >/dev/null 2>&1; then',
        '    sudo systemctl stop "$unit"',
        "  fi",
        "done",
        'sudo install -d -m 0700 -o "$SERVICE_USER" -g "$SERVICE_GROUP" "$DATA_ROOT"',
        'sudo chmod 0700 "$DATA_ROOT"',
        'sudo chown -R "$SERVICE_USER:$SERVICE_GROUP" "$DATA_ROOT"',
        'sudo install -d -m 0700 -o "$SERVICE_USER" -g "$SERVICE_GROUP" "$DATA_ROOT/ssh"',
        'sudo install -d -m 0750 "$REGISTRY_DATA_ROOT"',
        'if [ -s "$STAGED_SESSION_FILE" ]; then',
        '  sudo install -m 0600 -o "$SERVICE_USER" -g "$SERVICE_GROUP" "$STAGED_SESSION_FILE" "$SESSION_FILE"',
        '  rm -f "$STAGED_SESSION_FILE"',
        "fi",
        'if [ ! -s "$SESSION_FILE" ]; then',
        '  echo "UCloud session file is missing from persistent state: $SESSION_FILE" >&2',
        "  exit 1",
        "fi",
        'chmod 600 "$SESSION_FILE"',
        'sudo chown "$SERVICE_USER:$SERVICE_GROUP" "$SESSION_FILE"',
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
    script_parts.append(
        _install_root_file_snippet(
            "/etc/ucloud-sandboxes/deployment.json",
            deployment_json,
            mode="0644",
        )
    )
    for path, content in unit_files.items():
        script_parts.append(_install_root_file_snippet(path, content, mode="0644"))
    for unit_name, storage_dropin in (
        *((name, persistent_storage_dropin) for name in PERSISTENT_STATE_SYSTEMD_UNITS),
        *((name, registry_storage_dropin) for name in REGISTRY_STORAGE_SYSTEMD_UNITS),
    ):
        dropin_dir = f"/etc/systemd/system/{unit_name}.d"
        script_parts.append(f"sudo install -d -m 0755 {shlex.quote(dropin_dir)}")
        script_parts.append(
            _install_root_file_snippet(
                f"{dropin_dir}/persistent-storage.conf",
                storage_dropin,
                mode="0644",
            )
        )
    script_parts.extend(
        [
            "sudo systemctl daemon-reload",
            "sudo systemctl enable ucloud-sandbox-registry.service",
            "sudo systemctl enable --now ucloud-sandbox-registry-prune.timer",
            "sudo systemctl enable --now ucloud-sandbox-registry-gc.timer",
            "sudo systemctl enable ucloud-sandbox-gateway.service",
            "sudo systemctl enable ucloud-sandbox-relay.service",
            "sudo systemctl enable ucloud-sandbox-autoscaler.service",
            "sudo systemctl restart ucloud-sandbox-registry.service",
            "sudo systemctl restart ucloud-sandbox-gateway.service",
            "sudo systemctl restart ucloud-sandbox-relay.service",
            "sudo systemctl restart ucloud-sandbox-autoscaler.service",
            "wait_for_http() {",
            '  name="$1"',
            '  url="$2"',
            "  attempt=1",
            '  while [ "$attempt" -le 30 ]; do',
            '    if curl -fsS "$url"; then',
            "      printf '\\n'",
            "      return 0",
            "    fi",
            "    sleep 1",
            "    attempt=$((attempt + 1))",
            "  done",
            '  printf "Timed out waiting for %s at %s\\n" "$name" "$url" >&2',
            "  return 1",
            "}",
            f"wait_for_http gateway http://127.0.0.1:{plan.config.gateway_port}/healthz",
            f"wait_for_http relay http://127.0.0.1:{plan.config.relay_port}/healthz",
            f"wait_for_http registry http://127.0.0.1:{plan.config.registry_port}/v2/_catalog",
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
        diagnostic = "\n".join(
            part.strip()
            for part in (completed.stdout[-4096:], completed.stderr[-4096:])
            if part.strip()
        )
        suffix = f":\n{diagnostic}" if diagnostic else ""
        raise ValueError(
            f"remote all-in-one deploy failed with exit {completed.returncode}{suffix}"
        )
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
        raise ValueError(
            f"failed to read remote file {remote_path}: exit {completed.returncode}"
        )
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
            f'sudo install -m {shlex.quote(mode)} "$tmp_file" {shlex.quote(path)}',
            'rm -f "$tmp_file"',
        ]
    )


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _reject_bad_text(label: str, value: str) -> None:
    if "\x00" in value or "\n" in value or "\r" in value:
        raise ValueError(f"{label} cannot contain control newlines.")
