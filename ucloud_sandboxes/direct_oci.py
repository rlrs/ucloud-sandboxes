from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import re
import shutil
import stat
import tempfile
from typing import Any

from .image_rootfs import DockerImageConfig, MaterializedRootfs
from .sandbox import SandboxSpec, linux_host_entrypoint_script


_ENV_KEY = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_NUMERIC_USER = re.compile(r"([0-9]+)(?::([0-9]+))?\Z")
_CAPABILITY = re.compile(r"(?:CAP_)?([A-Z0-9_]+)\Z")
_LINUX_CAPABILITIES = {
    "AUDIT_CONTROL",
    "AUDIT_READ",
    "AUDIT_WRITE",
    "BLOCK_SUSPEND",
    "BPF",
    "CHECKPOINT_RESTORE",
    "CHOWN",
    "DAC_OVERRIDE",
    "DAC_READ_SEARCH",
    "FOWNER",
    "FSETID",
    "IPC_LOCK",
    "IPC_OWNER",
    "KILL",
    "LEASE",
    "LINUX_IMMUTABLE",
    "MAC_ADMIN",
    "MAC_OVERRIDE",
    "MKNOD",
    "NET_ADMIN",
    "NET_BIND_SERVICE",
    "NET_BROADCAST",
    "NET_RAW",
    "PERFMON",
    "SETFCAP",
    "SETGID",
    "SETPCAP",
    "SETUID",
    "SYSLOG",
    "SYS_ADMIN",
    "SYS_BOOT",
    "SYS_CHROOT",
    "SYS_MODULE",
    "SYS_NICE",
    "SYS_PACCT",
    "SYS_PTRACE",
    "SYS_RAWIO",
    "SYS_RESOURCE",
    "SYS_TIME",
    "SYS_TTY_CONFIG",
    "WAKE_ALARM",
}
_DEFAULT_CAPABILITIES = {
    f"CAP_{item}"
    for item in {
        "AUDIT_WRITE",
        "CHOWN",
        "DAC_OVERRIDE",
        "FOWNER",
        "FSETID",
        "KILL",
        "MKNOD",
        "NET_BIND_SERVICE",
        "NET_RAW",
        "SETFCAP",
        "SETGID",
        "SETPCAP",
        "SETUID",
        "SYS_CHROOT",
    }
}


class DirectOciConfigError(ValueError):
    pass


@dataclass(frozen=True)
class DirectOciConfigBuilder:
    """Translate the product sandbox contract into a deterministic OCI config."""

    init_binary: Path | None = None

    def __post_init__(self) -> None:
        if self.init_binary is not None and not self.init_binary.is_absolute():
            raise ValueError("direct runtime init binary must be absolute")

    def build(
        self,
        spec: SandboxSpec,
        image: MaterializedRootfs,
    ) -> dict[str, Any]:
        if spec.forkable:
            raise DirectOciConfigError("fork is deferred from the direct runtime")
        spec.validate()
        if spec.memory_mb is None or spec.disk_mb is None:
            raise DirectOciConfigError(
                "direct sandboxes require explicit memory_mb and disk_mb limits"
            )
        if spec.ssh.enabled:
            raise DirectOciConfigError(
                "direct runtime SSH requires node network integration"
            )

        image_config = image.image_config
        args = self._process_args(spec, image_config)
        environment = self._environment(spec, image_config)
        if spec.profile == "linux_host":
            args = (
                "/bin/sh",
                "-lc",
                linux_host_entrypoint_script(),
                "ucloud-linux-host",
                *spec.command,
            )
            environment.update(self._linux_host_environment(spec))

        mounts = self._mounts(spec)
        if spec.security.init:
            if self.init_binary is None:
                raise DirectOciConfigError(
                    "security.init requires a configured direct-runtime init binary"
                )
            self._validate_init_binary(self.init_binary)
            args = ("/.ucloud-init", "--", *args)

        uid, gid = self._numeric_user(spec.security.user or image_config.user or "0")
        capabilities = self._capabilities(spec)
        memory_bytes = spec.memory_mb * 1024 * 1024
        linux_resources: dict[str, Any] = {
            "memory": {
                "limit": memory_bytes,
                # One additional memory bound of swap, matching the existing
                # product's 2x combined memory+swap admission.
                "swap": memory_bytes,
            }
        }
        if spec.cpus is not None:
            linux_resources["cpu"] = {
                "period": 100_000,
                "quota": max(1, round(spec.cpus * 100_000)),
            }
        if spec.security.pids_limit is not None:
            linux_resources["pids"] = {"limit": spec.security.pids_limit}

        annotations = {
            "dev.ucloud-sandboxes.image-id": image.image_id,
            "dev.ucloud-sandboxes.profile": spec.profile,
            "dev.ucloud-sandboxes.sandbox-id": spec.id,
        }
        annotations.update(
            {f"dev.ucloud-sandboxes.label.{key}": value for key, value in spec.labels.items()}
        )
        return {
            "annotations": dict(sorted(annotations.items())),
            "hostname": spec.id,
            "linux": {
                "maskedPaths": [
                    "/proc/acpi",
                    "/proc/asound",
                    "/proc/kcore",
                    "/proc/keys",
                    "/proc/latency_stats",
                    "/proc/timer_list",
                    "/proc/timer_stats",
                    "/proc/sched_debug",
                    "/sys/firmware",
                ],
                "namespaces": [
                    {"type": "pid"},
                    {"type": "network"},
                    {"type": "ipc"},
                    {"type": "uts"},
                    {"type": "mount"},
                ],
                "readonlyPaths": [
                    "/proc/bus",
                    "/proc/fs",
                    "/proc/irq",
                    "/proc/sys",
                    "/proc/sysrq-trigger",
                ],
                "resources": linux_resources,
            },
            "mounts": mounts,
            "ociVersion": "1.0.2",
            "process": {
                "args": list(args),
                "capabilities": {
                    kind: sorted(capabilities)
                    for kind in ("bounding", "effective", "inheritable", "permitted")
                },
                "cwd": self._working_directory(spec, image_config),
                "env": [f"{key}={value}" for key, value in sorted(environment.items())],
                "noNewPrivileges": spec.security.no_new_privileges,
                "rlimits": [
                    {"hard": 1_048_576, "soft": 1_048_576, "type": "RLIMIT_NOFILE"}
                ],
                "terminal": False,
                "user": {"gid": gid, "uid": uid},
            },
            "root": {
                "path": "rootfs",
                "readonly": spec.security.read_only_rootfs,
            },
        }

    def install_init(self, rootfs: Path, *, enabled: bool) -> None:
        """Atomically install the trusted init into a prepared writable rootfs."""
        if not enabled:
            return
        if self.init_binary is None:
            raise DirectOciConfigError(
                "security.init requires a configured direct-runtime init binary"
            )
        if not rootfs.is_absolute() or not rootfs.is_dir() or rootfs.is_symlink():
            raise DirectOciConfigError(
                "direct-runtime init target must be an absolute rootfs directory"
            )
        self._validate_init_binary(self.init_binary)

        source_fd = -1
        target_fd = -1
        temporary = ""
        try:
            source_fd = os.open(
                self.init_binary,
                os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
            )
            self._validate_init_stat(os.fstat(source_fd))
            target_fd, temporary = tempfile.mkstemp(
                prefix=".ucloud-init.",
                dir=rootfs,
            )
            with (
                os.fdopen(source_fd, "rb") as source,
                os.fdopen(target_fd, "wb") as target,
            ):
                source_fd = -1
                target_fd = -1
                shutil.copyfileobj(source, target)
                target.flush()
                os.fchmod(target.fileno(), 0o755)
                os.fsync(target.fileno())
            os.replace(temporary, rootfs / ".ucloud-init")
            temporary = ""
            directory_fd = os.open(rootfs, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        except OSError as exc:
            raise DirectOciConfigError(
                "failed to install direct-runtime init into sandbox rootfs"
            ) from exc
        finally:
            if source_fd >= 0:
                os.close(source_fd)
            if target_fd >= 0:
                os.close(target_fd)
            if temporary:
                try:
                    os.unlink(temporary)
                except FileNotFoundError:
                    pass

    @staticmethod
    def _process_args(
        spec: SandboxSpec,
        image: DockerImageConfig,
    ) -> tuple[str, ...]:
        command = spec.command if spec.command else image.command
        args = (*image.entrypoint, *command)
        if not args or not args[0]:
            raise DirectOciConfigError(
                "sandbox image and request do not define an initial process"
            )
        if any("\0" in item for item in args):
            raise DirectOciConfigError("sandbox process arguments contain NUL")
        return args

    @staticmethod
    def _environment(
        spec: SandboxSpec,
        image: DockerImageConfig,
    ) -> dict[str, str]:
        environment: dict[str, str] = {}
        for item in image.env:
            key, separator, value = item.partition("=")
            if not separator or not _ENV_KEY.fullmatch(key):
                raise DirectOciConfigError("Docker image contains an invalid environment")
            environment[key] = value
        environment.update(spec.env)
        return environment

    @staticmethod
    def _working_directory(spec: SandboxSpec, image: DockerImageConfig) -> str:
        directory = spec.working_dir or image.working_dir or "/"
        if not directory.startswith("/") or "\0" in directory:
            raise DirectOciConfigError("sandbox working directory must be absolute")
        return directory

    @staticmethod
    def _numeric_user(value: str) -> tuple[int, int]:
        match = _NUMERIC_USER.fullmatch(value)
        if match is None:
            raise DirectOciConfigError(
                "direct runtime requires a numeric OCI user (uid or uid:gid)"
            )
        uid = int(match.group(1))
        gid = int(match.group(2) or match.group(1))
        if uid > 2**32 - 2 or gid > 2**32 - 2:
            raise DirectOciConfigError("OCI uid/gid is out of range")
        return uid, gid

    @staticmethod
    def _capabilities(spec: SandboxSpec) -> set[str]:
        capabilities = set(_DEFAULT_CAPABILITIES)
        for raw in spec.security.cap_drop:
            if raw.upper() == "ALL":
                capabilities.clear()
                continue
            capabilities.discard(DirectOciConfigBuilder._capability(raw))
        for raw in spec.security.cap_add:
            if raw.upper() == "ALL":
                capabilities.update(f"CAP_{item}" for item in _LINUX_CAPABILITIES)
                continue
            capabilities.add(DirectOciConfigBuilder._capability(raw))
        return capabilities

    @staticmethod
    def _capability(raw: str) -> str:
        match = _CAPABILITY.fullmatch(raw.upper())
        if match is None or match.group(1) not in _LINUX_CAPABILITIES:
            raise DirectOciConfigError(f"unsupported Linux capability: {raw}")
        return f"CAP_{match.group(1)}"

    @staticmethod
    def _mounts(spec: SandboxSpec) -> list[dict[str, Any]]:
        mounts: list[dict[str, Any]] = [
            {
                "destination": "/proc",
                "options": ["nosuid", "noexec", "nodev"],
                "source": "proc",
                "type": "proc",
            },
            {
                "destination": "/dev",
                "options": ["nosuid", "strictatime", "mode=755", "size=65536k"],
                "source": "tmpfs",
                "type": "tmpfs",
            },
            {
                "destination": "/dev/pts",
                "options": [
                    "nosuid",
                    "noexec",
                    "newinstance",
                    "ptmxmode=0666",
                    "mode=0620",
                    "gid=5",
                ],
                "source": "devpts",
                "type": "devpts",
            },
            {
                "destination": "/dev/shm",
                "options": ["nosuid", "noexec", "nodev", "mode=1777", "size=65536k"],
                "source": "shm",
                "type": "tmpfs",
            },
            {
                "destination": "/dev/mqueue",
                "options": ["nosuid", "noexec", "nodev"],
                "source": "mqueue",
                "type": "mqueue",
            },
            {
                "destination": "/sys",
                "options": ["nosuid", "noexec", "nodev", "ro"],
                "source": "sysfs",
                "type": "sysfs",
            },
            {
                "destination": "/tmp",
                "options": [
                    "nosuid",
                    "nodev",
                    "mode=1777",
                    f"size={spec.filesystem.tmpfs_mb * 1024 * 1024}",
                ],
                "source": "tmpfs",
                "type": "tmpfs",
            },
            {
                "destination": "/run",
                "options": [
                    "nosuid",
                    "nodev",
                    "mode=755",
                    f"size={spec.filesystem.run_tmpfs_mb * 1024 * 1024}",
                ],
                "source": "tmpfs",
                "type": "tmpfs",
            },
        ]
        if spec.filesystem.enforce_disk_quota:
            assert spec.disk_mb is not None
            mounts.append(
                {
                    "destination": spec.filesystem.workspace_path,
                    "options": [
                        "nosuid",
                        "nodev",
                        "mode=755",
                        f"size={spec.disk_mb * 1024 * 1024}",
                    ],
                    "source": "tmpfs",
                    "type": "tmpfs",
                }
            )
        return mounts

    @staticmethod
    def _linux_host_environment(spec: SandboxSpec) -> dict[str, str]:
        values = {
            "UCLOUD_SANDBOX_ENABLE_CRON": (
                "1" if spec.linux_host.enable_cron else "0"
            ),
            "UCLOUD_SANDBOX_ENABLE_SSHD": (
                "1" if spec.linux_host.enable_sshd or spec.ssh.enabled else "0"
            ),
            "UCLOUD_SANDBOX_KEEP_ALIVE": (
                "1" if spec.linux_host.keep_alive else "0"
            ),
            "UCLOUD_SANDBOX_LINUX_HOST_PATHS": ":".join(
                spec.linux_host.writable_paths
            ),
            "UCLOUD_SANDBOX_PROFILE": "linux_host",
            "UCLOUD_SANDBOX_SSH_PORT": str(spec.ssh.container_port),
            "UCLOUD_SANDBOX_SSH_USER": spec.ssh.user,
        }
        if spec.ssh.authorized_keys:
            values["UCLOUD_SANDBOX_SSH_AUTHORIZED_KEYS"] = "\n".join(
                spec.ssh.authorized_keys
            )
        return values

    @staticmethod
    def _validate_init_binary(path: Path) -> None:
        try:
            info = path.stat()
        except OSError as exc:
            raise DirectOciConfigError(
                "direct-runtime init binary is unavailable"
            ) from exc
        if path.is_symlink():
            raise DirectOciConfigError(
                "direct-runtime init binary must be root-owned, executable, and immutable"
            )
        DirectOciConfigBuilder._validate_init_stat(info)

    @staticmethod
    def _validate_init_stat(info: os.stat_result) -> None:
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != 0
            or not info.st_mode & 0o111
            or info.st_mode & 0o022
        ):
            raise DirectOciConfigError(
                "direct-runtime init binary must be root-owned, executable, and immutable"
            )
