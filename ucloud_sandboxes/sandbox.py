from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
from threading import Condition, RLock
from typing import Any, Iterator, Protocol

from .hibernation import hibernation_disk_reservation_mb
from .models import ResourceQuantity, utc_now


SANDBOX_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")
OPERATION_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
ENV_KEY_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
SANDBOX_RESERVED_LABEL_PREFIX = "ucloud-sandboxes."
DEFAULT_SANDBOX_USER = "1000:1000"
DEFAULT_PIDS_LIMIT = 256
SECURITY_VALUE_RE = re.compile(r"^[A-Za-z0-9_.:@/-]+$")
CONTAINER_PATH_RE = re.compile(r"^/[A-Za-z0-9_./-]+$")
SANDBOX_PROFILES = {"container", "linux_host"}
DEFAULT_LINUX_HOST_WRITABLE_PATHS = (
    "/run",
    "/run/lock",
    "/run/sshd",
    "/tmp",
    "/var/tmp",
    "/var/run",
    "/var/lock",
    "/var/spool/cron",
    "/var/spool/cron/crontabs",
    "/etc/cron.d",
    "/logs",
    "/logs/agent",
    "/logs/verifier",
    "/tests",
    "/task",
    "/oracle",
    "/workspace",
)


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    """Durably replace a JSON file using a process-unique sibling temporary."""
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, raw_tmp_path = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
    )
    tmp_path = Path(raw_tmp_path)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
        try:
            directory_fd = os.open(path.parent, os.O_RDONLY)
        except OSError:
            return
        try:
            try:
                os.fsync(directory_fd)
            except OSError:
                pass
        finally:
            os.close(directory_fd)
    finally:
        try:
            tmp_path.unlink()
        except FileNotFoundError:
            pass


class SandboxConflictError(ValueError):
    pass


class SandboxAdmissionClosedError(RuntimeError):
    pass


class SandboxCapacityUnavailableError(RuntimeError):
    """The node cannot currently admit the requested sandbox resources."""


class SandboxFileTooLargeError(ValueError):
    """A sandbox file exceeded the configured download response limit."""


class SandboxBusyError(SandboxConflictError):
    """The sandbox has activity that cannot cross a lifecycle transition."""


@dataclass(frozen=True)
class SandboxOperation:
    operation_id: str
    generation: int
    kind: str
    spec_hash: str

    @classmethod
    def from_dict(cls, raw: object) -> "SandboxOperation":
        if not isinstance(raw, dict):
            raise ValueError("_ucloud_operation must be a JSON object")
        if set(raw) != {"generation", "kind", "operation_id", "spec_hash"}:
            raise ValueError("_ucloud_operation has an invalid schema")
        operation_id = raw.get("operation_id")
        kind = raw.get("kind")
        spec_hash = raw.get("spec_hash")
        generation = raw.get("generation")
        if isinstance(generation, bool) or not isinstance(generation, int):
            raise ValueError("operation generation must be an integer")
        if generation <= 0:
            raise ValueError("operation generation must be positive")
        if not isinstance(operation_id, str) or not operation_id.strip():
            raise ValueError("operation_id is required")
        operation_id = operation_id.strip()
        if not OPERATION_ID_RE.fullmatch(operation_id):
            raise ValueError("operation_id contains unsupported characters")
        if not isinstance(kind, str):
            raise ValueError("operation kind must be a string")
        if kind != "create":
            raise ValueError("operation kind must be create")
        if not isinstance(spec_hash, str) or not spec_hash:
            raise ValueError("operation spec_hash is required")
        return cls(
            operation_id=operation_id,
            generation=generation,
            kind=kind,
            spec_hash=spec_hash,
        )

    def validate_spec(self, spec: "SandboxSpec") -> None:
        expected = sandbox_spec_fingerprint(spec)
        if self.spec_hash != expected:
            raise ValueError(
                f"operation spec_hash does not match sandbox spec: {self.spec_hash} != {expected}"
            )


@dataclass(frozen=True)
class SandboxSecuritySpec:
    user: str | None = DEFAULT_SANDBOX_USER
    cap_drop: tuple[str, ...] = ("ALL",)
    cap_add: tuple[str, ...] = ()
    no_new_privileges: bool = True
    pids_limit: int | None = DEFAULT_PIDS_LIMIT
    read_only_rootfs: bool = False
    init: bool = True

    @classmethod
    def from_dict(cls, raw: object) -> "SandboxSecuritySpec":
        if raw is None:
            return cls()
        raw = _json_object(
            raw,
            "security",
            {
                "cap_add",
                "cap_drop",
                "init",
                "no_new_privileges",
                "pids_limit",
                "read_only_rootfs",
                "user",
            },
        )
        user = raw.get("user", DEFAULT_SANDBOX_USER)
        if user is not None and not isinstance(user, str):
            raise ValueError("security user must be a string or null")
        return cls(
            user=user or None,
            cap_drop=_json_string_list(raw.get("cap_drop", ["ALL"]), "cap_drop"),
            cap_add=_json_string_list(raw.get("cap_add", []), "cap_add"),
            no_new_privileges=_json_bool(
                raw.get("no_new_privileges", True), "no_new_privileges"
            ),
            pids_limit=_json_optional_int(
                raw.get("pids_limit", DEFAULT_PIDS_LIMIT), "pids_limit"
            ),
            read_only_rootfs=_json_bool(
                raw.get("read_only_rootfs", False), "read_only_rootfs"
            ),
            init=_json_bool(raw.get("init", True), "init"),
        )

    def validate(self) -> None:
        if self.user is not None:
            validate_security_value("security user", self.user)
        for item in self.cap_drop:
            validate_security_value("cap_drop", item)
        for item in self.cap_add:
            validate_security_value("cap_add", item)
        if self.pids_limit is not None and self.pids_limit <= 0:
            raise ValueError("pids_limit must be positive.")

    def to_dict(self) -> dict[str, Any]:
        return {
            "user": self.user,
            "cap_drop": list(self.cap_drop),
            "cap_add": list(self.cap_add),
            "no_new_privileges": self.no_new_privileges,
            "pids_limit": self.pids_limit,
            "read_only_rootfs": self.read_only_rootfs,
            "init": self.init,
        }


@dataclass(frozen=True)
class SandboxFilesystemSpec:
    enforce_disk_quota: bool = False
    workspace_path: str = "/workspace"
    tmpfs_mb: int = 64
    run_tmpfs_mb: int = 16

    @classmethod
    def from_dict(cls, raw: object) -> "SandboxFilesystemSpec":
        if raw is None:
            return cls()
        raw = _json_object(
            raw,
            "filesystem",
            {"enforce_disk_quota", "run_tmpfs_mb", "tmpfs_mb", "workspace_path"},
        )
        return cls(
            enforce_disk_quota=_json_bool(
                raw.get("enforce_disk_quota", False), "enforce_disk_quota"
            ),
            workspace_path=_json_string(
                raw.get("workspace_path", "/workspace"), "workspace_path"
            ),
            tmpfs_mb=_json_int(raw.get("tmpfs_mb", 64), "tmpfs_mb"),
            run_tmpfs_mb=_json_int(raw.get("run_tmpfs_mb", 16), "run_tmpfs_mb"),
        )

    def validate(self) -> None:
        validate_container_path("workspace_path", self.workspace_path)
        if self.tmpfs_mb <= 0:
            raise ValueError("tmpfs_mb must be positive.")
        if self.run_tmpfs_mb <= 0:
            raise ValueError("run_tmpfs_mb must be positive.")

    def to_dict(self) -> dict[str, Any]:
        return {
            "enforce_disk_quota": self.enforce_disk_quota,
            "workspace_path": self.workspace_path,
            "tmpfs_mb": self.tmpfs_mb,
            "run_tmpfs_mb": self.run_tmpfs_mb,
        }


@dataclass(frozen=True)
class SandboxLinuxHostSpec:
    enable_cron: bool = False
    enable_sshd: bool = False
    keep_alive: bool = True
    writable_paths: tuple[str, ...] = DEFAULT_LINUX_HOST_WRITABLE_PATHS

    @classmethod
    def from_dict(cls, raw: object) -> "SandboxLinuxHostSpec":
        if raw is None:
            return cls()
        raw = _json_object(
            raw,
            "linux_host",
            {"enable_cron", "enable_sshd", "keep_alive", "writable_paths"},
        )
        return cls(
            enable_cron=_json_bool(raw.get("enable_cron", False), "enable_cron"),
            enable_sshd=_json_bool(raw.get("enable_sshd", False), "enable_sshd"),
            keep_alive=_json_bool(raw.get("keep_alive", True), "keep_alive"),
            writable_paths=_json_string_list(
                raw.get("writable_paths", list(DEFAULT_LINUX_HOST_WRITABLE_PATHS)),
                "writable_paths",
            ),
        )

    def validate(self) -> None:
        for path in self.writable_paths:
            validate_container_path("linux_host writable path", path)

    def to_dict(self) -> dict[str, Any]:
        return {
            "enable_cron": self.enable_cron,
            "enable_sshd": self.enable_sshd,
            "keep_alive": self.keep_alive,
            "writable_paths": list(self.writable_paths),
        }


@dataclass(frozen=True)
class SandboxSshSpec:
    enabled: bool = False
    user: str = "root"
    host: str = "127.0.0.1"
    host_port: int | None = None
    container_port: int = 22
    authorized_keys: tuple[str, ...] = ()

    @classmethod
    def from_dict(cls, raw: object) -> "SandboxSshSpec":
        if raw is None:
            return cls()
        raw = _json_object(
            raw,
            "ssh",
            {
                "authorized_keys",
                "container_port",
                "enabled",
                "host",
                "host_port",
                "user",
            },
        )
        return cls(
            enabled=_json_bool(raw.get("enabled", False), "enabled"),
            user=_json_string(raw.get("user", "root"), "user"),
            host=_json_string(raw.get("host", "127.0.0.1"), "host"),
            host_port=_json_optional_int(raw.get("host_port"), "host_port"),
            container_port=_json_int(raw.get("container_port", 22), "container_port"),
            authorized_keys=_json_string_list(
                raw.get("authorized_keys", []), "authorized_keys"
            ),
        )

    def validate(self) -> None:
        if not self.enabled:
            return
        if not self.user.strip():
            raise ValueError("ssh user cannot be empty.")
        if self.host_port is not None and not _valid_port(self.host_port):
            raise ValueError("ssh host_port must be in [1, 65535].")
        if not _valid_port(self.container_port):
            raise ValueError("ssh container_port must be in [1, 65535].")

    def to_dict(self) -> dict[str, Any]:
        raw = asdict(self)
        raw["authorized_keys"] = list(self.authorized_keys)
        return raw


@dataclass(frozen=True)
class SandboxSpec:
    id: str
    image: str
    profile: str = "container"
    command: tuple[str, ...] = ()
    env: dict[str, str] = field(default_factory=dict)
    working_dir: str | None = None
    memory_mb: int | None = None
    cpus: float | None = None
    disk_mb: int | None = None
    network: str = "none"
    ttl_seconds: int | None = None
    parkable: bool = False
    managed_process: bool = False
    ssh: SandboxSshSpec = SandboxSshSpec()
    security: SandboxSecuritySpec = SandboxSecuritySpec()
    filesystem: SandboxFilesystemSpec = SandboxFilesystemSpec()
    linux_host: SandboxLinuxHostSpec = SandboxLinuxHostSpec()
    labels: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "SandboxSpec":
        if not isinstance(raw, dict):
            raise ValueError("sandbox must be a JSON object")
        allowed = {
            "command",
            "cpus",
            "disk_mb",
            "env",
            "filesystem",
            "id",
            "image",
            "labels",
            "linux_host",
            "managed_process",
            "memory_mb",
            "network",
            "parkable",
            "profile",
            "security",
            "ssh",
            "ttl_seconds",
            "working_dir",
        }
        unsupported = sorted(set(raw) - allowed)
        if unsupported:
            raise ValueError("unsupported sandbox fields: " + ", ".join(unsupported))
        profile = _json_string(raw.get("profile", "container"), "profile")
        command_items = _json_string_list(raw.get("command", []), "command")
        env = _json_string_map(raw.get("env", {}), "env")
        labels = _json_string_map(raw.get("labels", {}), "labels")
        security = SandboxSecuritySpec.from_dict(raw.get("security"))
        filesystem = SandboxFilesystemSpec.from_dict(raw.get("filesystem"))
        if profile == "linux_host":
            if raw.get("security") is None:
                security = linux_host_default_security()
            if raw.get("filesystem") is None:
                filesystem = linux_host_default_filesystem()
        return cls(
            id=_json_string(raw.get("id", ""), "id"),
            image=_json_string(raw.get("image", ""), "image"),
            profile=profile,
            command=command_items,
            env=env,
            working_dir=(
                _json_string(raw["working_dir"], "working_dir")
                if raw.get("working_dir") is not None
                else None
            ),
            memory_mb=(
                _json_int(raw["memory_mb"], "memory_mb")
                if raw.get("memory_mb") is not None
                else None
            ),
            cpus=(
                _json_number(raw["cpus"], "cpus")
                if raw.get("cpus") is not None
                else None
            ),
            disk_mb=(
                _json_int(raw["disk_mb"], "disk_mb")
                if raw.get("disk_mb") is not None
                else None
            ),
            network=_json_string(raw.get("network", "none"), "network"),
            ttl_seconds=(
                _json_int(raw["ttl_seconds"], "ttl_seconds")
                if raw.get("ttl_seconds") is not None
                else None
            ),
            parkable=_json_bool(raw.get("parkable", False), "parkable"),
            managed_process=_json_bool(
                raw.get("managed_process", False), "managed_process"
            ),
            ssh=SandboxSshSpec.from_dict(raw.get("ssh")),
            security=security,
            filesystem=filesystem,
            linux_host=SandboxLinuxHostSpec.from_dict(raw.get("linux_host")),
            labels=labels,
        )

    def validate(self) -> None:
        if not SANDBOX_ID_RE.match(self.id):
            raise ValueError(
                "sandbox id must be 1-64 characters of letters, digits, _, . or - "
                "and start with a letter or digit."
            )
        if not self.image.strip():
            raise ValueError("sandbox image is required.")
        for key in self.env:
            if not ENV_KEY_RE.match(key):
                raise ValueError(f"invalid environment variable name: {key!r}")
        reserved_labels = sorted(
            key
            for key in self.labels
            if key.lower().startswith(SANDBOX_RESERVED_LABEL_PREFIX)
        )
        if reserved_labels:
            raise ValueError(
                "sandbox labels must not use the reserved "
                f"{SANDBOX_RESERVED_LABEL_PREFIX!r} prefix: {reserved_labels[0]!r}"
            )
        if self.memory_mb is not None and self.memory_mb <= 0:
            raise ValueError("memory_mb must be positive.")
        if self.cpus is not None and self.cpus <= 0:
            raise ValueError("cpus must be positive.")
        if self.disk_mb is not None and self.disk_mb <= 0:
            raise ValueError("disk_mb must be positive.")
        if self.requested_resources() == ResourceQuantity():
            raise ValueError("sandbox resources are required.")
        if self.ttl_seconds is not None and self.ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be positive.")
        if self.parkable and self.memory_mb is None:
            raise ValueError("parkable sandboxes require an explicit memory_mb limit.")
        if self.parkable and self.disk_mb is None:
            raise ValueError("parkable sandboxes require an explicit disk_mb limit.")
        if self.parkable and self.ssh.enabled:
            raise ValueError(
                "parkable sandboxes cannot expose SSH because direct host-port "
                "sessions bypass the hibernation lifecycle barrier."
            )
        if self.managed_process and not self.parkable:
            raise ValueError("managed_process requires a parkable sandbox.")
        if self.managed_process and self.profile != "container":
            raise ValueError(
                "managed_process currently requires the container profile."
            )
        if self.managed_process and self.command:
            raise ValueError(
                "managed_process sandboxes start their primary command through the job API."
            )
        if self.managed_process and self.security.read_only_rootfs:
            raise ValueError(
                "managed_process requires a writable rootfs for its checkpointed ledger."
            )
        if self.profile not in SANDBOX_PROFILES:
            raise ValueError(
                "profile must be one of: " + ", ".join(sorted(SANDBOX_PROFILES))
            )
        if self.network not in {"none", "bridge"}:
            raise ValueError("network must be either 'none' or 'bridge'.")
        self.ssh.validate()
        self.security.validate()
        self.filesystem.validate()
        self.linux_host.validate()
        if self.working_dir is not None:
            validate_container_path("working_dir", self.working_dir)
        if self.ssh.enabled and self.network != "bridge":
            raise ValueError("ssh-enabled sandboxes must use bridge networking.")

    def to_dict(self) -> dict[str, Any]:
        raw = asdict(self)
        raw["command"] = list(self.command)
        raw["ssh"] = self.ssh.to_dict()
        raw["security"] = self.security.to_dict()
        raw["filesystem"] = self.filesystem.to_dict()
        raw["linux_host"] = self.linux_host.to_dict()
        return raw

    def requested_resources(self) -> ResourceQuantity:
        disk_mb = self.disk_mb or 0
        if self.parkable:
            # Validation requires both bounds. Keep this defensive so resource
            # accounting never silently falls back to sparse allocated blocks.
            if self.memory_mb is None or self.disk_mb is None:
                raise ValueError(
                    "parkable sandbox resources require memory_mb and disk_mb"
                )
            disk_mb = hibernation_disk_reservation_mb(
                memory_mb=self.memory_mb,
                writable_disk_mb=self.disk_mb,
            )
        return ResourceQuantity(
            vcpu=self.cpus or 0.0,
            memory_mb=self.memory_mb or 0,
            disk_mb=disk_mb,
        )


@dataclass(frozen=True)
class SandboxRecord:
    spec: SandboxSpec
    container_name: str
    state: str
    created_at: datetime
    updated_at: datetime
    generation: int
    operation_id: str
    spec_hash: str

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "id": self.spec.id,
            "image": self.spec.image,
            "labels": dict(self.spec.labels),
            "spec": self.spec.to_dict(),
            "container_name": self.container_name,
            "state": self.state,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "generation": self.generation,
            "operation_id": self.operation_id,
            "spec_hash": self.spec_hash or sandbox_spec_fingerprint(self.spec),
        }
        if self.spec.ssh.enabled and self.spec.ssh.host_port is not None:
            payload["ssh"] = {
                "host": self.spec.ssh.host,
                "port": self.spec.ssh.host_port,
                "user": self.spec.ssh.user,
                "command": (
                    f"ssh -p {self.spec.ssh.host_port} "
                    f"{self.spec.ssh.user}@{self.spec.ssh.host}"
                ),
            }
        return payload

    def is_expired(self, now: datetime | None = None) -> bool:
        if self.spec.ttl_seconds is None:
            return False
        return (now or utc_now()) >= self.created_at + timedelta(
            seconds=self.spec.ttl_seconds
        )


@dataclass(frozen=True)
class NodeDrainState:
    draining: bool = False
    token: str = ""
    drain_activity_epoch: int = 0
    admission_open: bool = True

    @classmethod
    def from_dict(cls, raw: object) -> "NodeDrainState":
        if not isinstance(raw, dict):
            return cls()
        draining = bool(raw.get("draining", False))
        token = str(raw.get("token") or "").strip()
        try:
            drain_activity_epoch = int(raw.get("drain_activity_epoch", 0))
        except (TypeError, ValueError) as exc:
            raise ValueError("drain activity epoch must be an integer") from exc
        if drain_activity_epoch < 0:
            raise ValueError("drain activity epoch cannot be negative")
        if draining and not token:
            raise ValueError("persisted draining state requires a token")
        return cls(
            draining=draining,
            token=token,
            drain_activity_epoch=drain_activity_epoch,
            admission_open=not draining,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "draining": self.draining,
            "token": self.token,
            "drain_activity_epoch": self.drain_activity_epoch,
            "admission_open": self.admission_open,
        }


@dataclass(frozen=True)
class SandboxActivitySnapshot:
    records: tuple[SandboxRecord, ...]
    active_sandboxes: int
    used_resources: ResourceQuantity
    reserved_resources: ResourceQuantity
    activity_revision: int
    active_operations: int = 0


@dataclass(frozen=True)
class NodeDrainSnapshot:
    activity: SandboxActivitySnapshot
    drain: NodeDrainState
    active_image_builds: int

    @property
    def ready(self) -> bool:
        return (
            self.drain.draining
            and not self.drain.admission_open
            and self.drain.drain_activity_epoch == self.activity.activity_revision
            and not self.activity.records
            and self.activity.active_operations == 0
            and self.active_image_builds == 0
        )


@dataclass(frozen=True)
class CommandResult:
    argv: tuple[str, ...]
    exit_code: int
    stdout: str = ""
    stderr: str = ""
    stdout_bytes: bytes = b""
    stderr_bytes: bytes = b""


class SandboxLifecycleCoordinator:
    """Coordinates exec/file activity with park, wake, migration, and delete."""

    def __init__(self) -> None:
        self._condition = Condition(RLock())
        self._shared: dict[str, int] = {}
        self._exclusive: set[str] = set()

    def acquire_shared(self, sandbox_id: str) -> None:
        with self._condition:
            if sandbox_id in self._exclusive:
                raise SandboxBusyError(
                    f"sandbox lifecycle transition is in progress: {sandbox_id}"
                )
            self._shared[sandbox_id] = self._shared.get(sandbox_id, 0) + 1

    def release_shared(self, sandbox_id: str) -> None:
        with self._condition:
            count = self._shared.get(sandbox_id, 0)
            if count <= 1:
                self._shared.pop(sandbox_id, None)
            else:
                self._shared[sandbox_id] = count - 1
            self._condition.notify_all()

    @contextmanager
    def shared(self, sandbox_id: str) -> Iterator[None]:
        self.acquire_shared(sandbox_id)
        try:
            yield
        finally:
            self.release_shared(sandbox_id)

    @contextmanager
    def exclusive(
        self,
        *sandbox_ids: str,
        allow_shared: bool = False,
    ) -> Iterator[None]:
        ids = tuple(sorted(set(sandbox_ids)))
        with self._condition:
            conflicts = [
                sandbox_id
                for sandbox_id in ids
                if sandbox_id in self._exclusive
                or (not allow_shared and self._shared.get(sandbox_id, 0) > 0)
            ]
            if conflicts:
                raise SandboxBusyError(
                    "sandbox has active exec/file activity: " + ", ".join(conflicts)
                )
            self._exclusive.update(ids)
        try:
            yield
        finally:
            with self._condition:
                self._exclusive.difference_update(ids)
                self._condition.notify_all()


class CommandExecutor(Protocol):
    def run(
        self, argv: tuple[str, ...], *, input: bytes | None = None
    ) -> CommandResult: ...


class SubprocessExecutor:
    def run(
        self, argv: tuple[str, ...], *, input: bytes | None = None
    ) -> CommandResult:
        completed = subprocess.run(
            list(argv),
            input=input,
            check=False,
            capture_output=True,
        )
        stdout = completed.stdout or b""
        stderr = completed.stderr or b""
        return CommandResult(
            argv=argv,
            exit_code=completed.returncode,
            stdout=stdout.decode("utf-8", errors="replace"),
            stderr=stderr.decode("utf-8", errors="replace"),
            stdout_bytes=stdout,
            stderr_bytes=stderr,
        )


def linux_host_default_security() -> SandboxSecuritySpec:
    return SandboxSecuritySpec(
        user=None,
        cap_drop=(),
        cap_add=(),
        no_new_privileges=False,
        pids_limit=None,
        read_only_rootfs=False,
        init=True,
    )


def linux_host_default_filesystem() -> SandboxFilesystemSpec:
    return SandboxFilesystemSpec(
        enforce_disk_quota=False,
        workspace_path="/workspace",
        tmpfs_mb=256,
        run_tmpfs_mb=64,
    )


def linux_host_entrypoint_script() -> str:
    return r"""set -eu

install_service_shim() {
  if command -v service >/dev/null 2>&1; then
    return 0
  fi
  mkdir -p /usr/local/bin 2>/dev/null || return 0
  cat > /usr/local/bin/service <<'UCLOUD_SERVICE_SHIM'
#!/bin/sh
name="${1:-}"
action="${2:-}"
case "$name:$action" in
  cron:start|crond:start)
    if command -v cron >/dev/null 2>&1; then cron >/tmp/ucloud-cron.log 2>&1 || true; exit 0; fi
    if command -v crond >/dev/null 2>&1; then crond >/tmp/ucloud-cron.log 2>&1 || true; exit 0; fi
    exit 0
    ;;
  ssh:start|sshd:start)
    if command -v sshd >/dev/null 2>&1; then sshd >/tmp/ucloud-sshd.log 2>&1 || true; exit 0; fi
    if [ -x /usr/sbin/sshd ]; then /usr/sbin/sshd >/tmp/ucloud-sshd.log 2>&1 || true; exit 0; fi
    exit 0
    ;;
esac
exit 0
UCLOUD_SERVICE_SHIM
  chmod +x /usr/local/bin/service 2>/dev/null || true
}

prepare_paths() {
  old_ifs="$IFS"
  IFS=:
  for path in ${UCLOUD_SANDBOX_LINUX_HOST_PATHS:-}; do
    [ -n "$path" ] || continue
    mkdir -p -- "$path" 2>/dev/null || true
  done
  IFS="$old_ifs"
  chmod 1777 /tmp /var/tmp 2>/dev/null || true
  chmod 0777 /tests /logs /logs/agent /logs/verifier /task /oracle /workspace 2>/dev/null || true
}

start_cron() {
  [ "${UCLOUD_SANDBOX_ENABLE_CRON:-0}" = "1" ] || return 0
  if command -v service >/dev/null 2>&1; then
    service cron start >/tmp/ucloud-cron.log 2>&1 || service crond start >/tmp/ucloud-cron.log 2>&1 || true
  fi
  if command -v cron >/dev/null 2>&1; then
    cron >/tmp/ucloud-cron.log 2>&1 || true
    return 0
  fi
  if command -v crond >/dev/null 2>&1; then
    crond >/tmp/ucloud-cron.log 2>&1 || true
  fi
}

start_sshd() {
  [ "${UCLOUD_SANDBOX_ENABLE_SSHD:-0}" = "1" ] || return 0
  user="${UCLOUD_SANDBOX_SSH_USER:-root}"
  home_dir="$(getent passwd "$user" 2>/dev/null | awk -F: '{print $6}' || true)"
  [ -n "$home_dir" ] || home_dir=/root
  mkdir -p "$home_dir/.ssh" /run/sshd 2>/dev/null || true
  if [ -n "${UCLOUD_SANDBOX_SSH_AUTHORIZED_KEYS:-}" ]; then
    printf '%s\n' "$UCLOUD_SANDBOX_SSH_AUTHORIZED_KEYS" > "$home_dir/.ssh/authorized_keys" 2>/dev/null || true
    chmod 700 "$home_dir/.ssh" 2>/dev/null || true
    chmod 600 "$home_dir/.ssh/authorized_keys" 2>/dev/null || true
    chown -R "$user" "$home_dir/.ssh" 2>/dev/null || true
  fi
  if command -v ssh-keygen >/dev/null 2>&1; then
    ssh-keygen -A >/tmp/ucloud-ssh-keygen.log 2>&1 || true
  fi
  sshd_path=
  if command -v sshd >/dev/null 2>&1; then
    sshd_path="$(command -v sshd)"
  elif [ -x /usr/sbin/sshd ]; then
    sshd_path=/usr/sbin/sshd
  fi
  if [ -n "$sshd_path" ]; then
    "$sshd_path" -p "${UCLOUD_SANDBOX_SSH_PORT:-22}" >/tmp/ucloud-sshd.log 2>&1 || true
  fi
}

install_service_shim
prepare_paths
start_cron
start_sshd

if [ "$#" -gt 0 ]; then
  exec "$@"
fi

[ "${UCLOUD_SANDBOX_KEEP_ALIVE:-1}" = "1" ] || exit 0
trap 'exit 0' INT TERM
while :; do
  sleep 3600 &
  wait "$!" || true
done
"""


def sandbox_spec_fingerprint(spec: SandboxSpec) -> str:
    raw = json.dumps(spec.to_dict(), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def sandbox_specs_match(existing: SandboxSpec, requested: SandboxSpec) -> bool:
    return existing == requested


def _json_object(
    raw: object,
    name: str,
    fields: set[str],
) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ValueError(f"{name} must be a JSON object")
    unsupported = sorted(set(raw) - fields)
    if unsupported:
        raise ValueError(f"unsupported {name} fields: " + ", ".join(unsupported))
    return raw


def _json_bool(raw: object, name: str) -> bool:
    if type(raw) is not bool:
        raise ValueError(f"{name} must be a boolean")
    return raw


def _json_int(raw: object, name: str) -> int:
    if type(raw) is not int:
        raise ValueError(f"{name} must be an integer")
    return raw


def _json_optional_int(raw: object, name: str) -> int | None:
    return None if raw is None else _json_int(raw, name)


def _json_number(raw: object, name: str) -> float:
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        raise ValueError(f"{name} must be a number")
    return float(raw)


def _json_string(raw: object, name: str) -> str:
    if not isinstance(raw, str):
        raise ValueError(f"{name} must be a string")
    return raw


def _json_string_list(raw: object, name: str) -> tuple[str, ...]:
    if not isinstance(raw, list) or any(not isinstance(item, str) for item in raw):
        raise ValueError(f"{name} must be a list of strings")
    return tuple(raw)


def _json_string_map(raw: object, name: str) -> dict[str, str]:
    if not isinstance(raw, dict) or any(
        not isinstance(key, str) or not isinstance(value, str)
        for key, value in raw.items()
    ):
        raise ValueError(f"{name} must be an object of string values")
    return dict(raw)


def _valid_port(value: int) -> bool:
    return 1 <= value <= 65535


def validate_security_value(name: str, value: str) -> None:
    if not value:
        raise ValueError(f"{name} cannot be empty.")
    if "\n" in value or "\r" in value:
        raise ValueError(f"{name} cannot contain newlines.")
    if not SECURITY_VALUE_RE.match(value):
        raise ValueError(f"{name} contains unsupported characters.")


def validate_container_path(name: str, value: str) -> None:
    if not value.startswith("/"):
        raise ValueError(f"{name} must be an absolute container path.")
    if "\n" in value or "\r" in value or ":" in value or "," in value:
        raise ValueError(f"{name} contains unsupported characters.")
    if ".." in Path(value).parts:
        raise ValueError(f"{name} cannot contain '..'.")
    if not CONTAINER_PATH_RE.match(value):
        raise ValueError(f"{name} contains unsupported characters.")
