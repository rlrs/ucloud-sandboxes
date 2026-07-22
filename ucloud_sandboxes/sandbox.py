from __future__ import annotations

from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timedelta
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import signal
import subprocess
import tempfile
from threading import Condition, Event, RLock, Thread, local
import time
from typing import Any, Callable, ContextManager, Iterator, Protocol, Sequence

from .models import ResourceQuantity, parse_iso_datetime, utc_now
from .runsc_restore import (
    DEFAULT_RUNTIME_NAME as DEFAULT_FORK_RUNTIME_NAME,
    RESTORE_CHECKPOINT_ANNOTATION,
)


SANDBOX_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")
OPERATION_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
ENV_KEY_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
FORK_NONCE_RE = re.compile(r"^[0-9a-f]{64}$")
DEFAULT_CONTAINER_PREFIX = "ucloud-sandbox-"
SANDBOX_GENERATION_LABEL = "ucloud-sandboxes.generation"
SANDBOX_OPERATION_ID_LABEL = "ucloud-sandboxes.operation-id"
SANDBOX_SPEC_HASH_LABEL = "ucloud-sandboxes.spec-sha256"
SANDBOX_RESERVED_LABEL_PREFIX = "ucloud-sandboxes."
DEFAULT_SANDBOX_USER = "1000:1000"
DEFAULT_PIDS_LIMIT = 256
SECURITY_VALUE_RE = re.compile(r"^[A-Za-z0-9_.:@/-]+$")
CONTAINER_PATH_RE = re.compile(r"^/[A-Za-z0-9_./-]+$")
SANDBOX_PROFILES = {"container", "linux_host"}
MAX_FORK_FANOUT = 64
DEFAULT_FORK_RESTORE_PARALLELISM = 8
MAX_FORK_RESTORE_PARALLELISM = 16
MAX_FORK_PROTOCOL_TIMEOUT_SECONDS = 60
MAX_FORK_CHECKPOINT_TIMEOUT_SECONDS = 300
MAX_FORK_RESTORE_TIMEOUT_SECONDS = 180
MAX_CHECKPOINT_HELPER_INVENTORY_BYTES = 8 * 1024 * 1024
FORK_CHILD_SETUP_ALLOWANCE_SECONDS = 30
FORK_CAPTURE_SETUP_ALLOWANCE_SECONDS = 5 * 60
FORK_SETUP_CLEANUP_ALLOWANCE_SECONDS = 30
FORK_RECOVERY_INSPECTION_ALLOWANCE_SECONDS = 30
FORK_REQUEST_TRANSPORT_ALLOWANCE_SECONDS = 60
_MAX_FORK_RESTORE_WAVES = (
    MAX_FORK_FANOUT + DEFAULT_FORK_RESTORE_PARALLELISM - 1
) // DEFAULT_FORK_RESTORE_PARALLELISM
_DERIVED_FORK_REQUEST_TIMEOUT_SECONDS = (
    # Source prepare, checkpoint, and resume.
    MAX_FORK_PROTOCOL_TIMEOUT_SECONDS
    + MAX_FORK_CHECKPOINT_TIMEOUT_SECONDS
    + MAX_FORK_PROTOCOL_TIMEOUT_SECONDS
    # Bounded waves of child create/stage, restore, and readiness.
    + _MAX_FORK_RESTORE_WAVES
    * (
        FORK_CHILD_SETUP_ALLOWANCE_SECONDS
        + MAX_FORK_RESTORE_TIMEOUT_SECONDS
        + MAX_FORK_PROTOCOL_TIMEOUT_SECONDS
    )
    # Recovered and pending children share the 64-child cap. The coupled
    # maximum is eight restore waves plus one readiness wave (1..7 recovered,
    # 57..63 pending); more recovered children necessarily remove a restore
    # wave, whose per-child bound is larger.
    + FORK_RECOVERY_INSPECTION_ALLOWANCE_SECONDS
    + MAX_FORK_PROTOCOL_TIMEOUT_SECONDS
    + FORK_CAPTURE_SETUP_ALLOWANCE_SECONDS
    + FORK_SETUP_CLEANUP_ALLOWANCE_SECONDS
    # Leave room for request parsing, serialization, and the proxy hop outside
    # the node's bounded runtime work.
    + FORK_REQUEST_TRANSPORT_ALLOWANCE_SECONDS
)
# Round the derived budget to a stable client-facing deadline.
FORK_REQUEST_TIMEOUT_SECONDS = (
    (_DERIVED_FORK_REQUEST_TIMEOUT_SECONDS + 5 * 60 - 1) // (5 * 60)
) * (5 * 60)
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


class _AdvisoryFileLock:
    """A thread-reentrant advisory lock shared through a sidecar lock file."""

    def __init__(self, data_path: Path) -> None:
        self.lock_path = data_path.with_name(f".{data_path.name}.lock")
        self._thread_lock = RLock()
        self._local = local()

    @contextmanager
    def hold(self, *, exclusive: bool) -> Iterator[None]:
        with self._thread_lock:
            depth = int(getattr(self._local, "depth", 0))
            if depth == 0:
                self.lock_path.parent.mkdir(parents=True, exist_ok=True)
                handle = self.lock_path.open("a+b")
                try:
                    fcntl.flock(
                        handle.fileno(),
                        fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH,
                    )
                except Exception:
                    handle.close()
                    raise
                self._local.handle = handle
                self._local.exclusive = exclusive
            elif exclusive and not bool(getattr(self._local, "exclusive", False)):
                raise RuntimeError("cannot upgrade a shared state-file lock")
            self._local.depth = depth + 1
            try:
                yield
            finally:
                remaining = int(self._local.depth) - 1
                self._local.depth = remaining
                if remaining == 0:
                    handle = self._local.handle
                    try:
                        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
                    finally:
                        handle.close()
                        del self._local.handle
                        del self._local.exclusive


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
                # Some network and virtual filesystems do not support
                # directory fsync.  The file itself was already fsynced and
                # atomically replaced, so retain compatibility there.
                pass
        finally:
            os.close(directory_fd)
    finally:
        try:
            tmp_path.unlink()
        except FileNotFoundError:
            pass


_SANDBOX_LOCKS_GUARD = RLock()
_SANDBOX_LOCKS: dict[Path, _AdvisoryFileLock] = {}


class SandboxConflictError(ValueError):
    pass


class SandboxStaleOperationError(SandboxConflictError):
    pass


class SandboxAdmissionClosedError(RuntimeError):
    pass


class SandboxCapacityUnavailableError(RuntimeError):
    """The node cannot currently admit the requested sandbox resources."""


class SandboxFileTooLargeError(ValueError):
    """A sandbox file exceeded the configured download response limit."""


class SandboxBusyError(SandboxConflictError):
    """The sandbox has activity that cannot cross a lifecycle transition."""


class SandboxForkUnsupportedError(RuntimeError):
    """The node runtime has not passed live-fork conformance."""


class SandboxForkCommandTimeoutError(RuntimeError):
    """A fork command timed out with externally ambiguous Docker state."""


class SandboxRuntimeCommandError(RuntimeError):
    """A runtime subprocess failed; its sensitive argv is never reflected."""

    def __init__(self, result: CommandResult) -> None:
        self.result = result
        super().__init__(
            f"sandbox runtime command failed with exit code {result.exit_code}"
        )


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
        operation_id = str(raw.get("operation_id") or "").strip()
        kind = str(raw.get("kind") or "").strip()
        spec_hash = str(raw.get("spec_hash") or "").strip()
        try:
            generation = int(raw.get("generation"))
        except (TypeError, ValueError) as exc:
            raise ValueError("operation generation must be an integer") from exc
        if generation < 0:
            raise ValueError("operation generation cannot be negative")
        if not operation_id:
            raise ValueError("operation_id is required")
        if not OPERATION_ID_RE.match(operation_id):
            raise ValueError("operation_id contains unsupported characters")
        if kind != "create":
            raise ValueError("operation kind must be create")
        if not spec_hash:
            raise ValueError("operation spec_hash is required")
        return cls(
            operation_id=operation_id,
            generation=generation,
            kind=kind,
            spec_hash=spec_hash,
        )

    @classmethod
    def legacy_create(cls, spec: "SandboxSpec") -> "SandboxOperation":
        return cls(
            operation_id="",
            generation=0,
            kind="create",
            spec_hash=sandbox_spec_fingerprint(spec),
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
        if not isinstance(raw, dict):
            raise ValueError("security must be a JSON object.")
        user = raw.get("user", DEFAULT_SANDBOX_USER)
        return cls(
            user=str(user) if user not in (None, "") else None,
            cap_drop=_string_tuple(raw.get("cap_drop"), default=("ALL",)),
            cap_add=_string_tuple(raw.get("cap_add"), default=()),
            no_new_privileges=bool(raw.get("no_new_privileges", True)),
            pids_limit=(
                int(raw["pids_limit"]) if raw.get("pids_limit") is not None else None
            ),
            read_only_rootfs=bool(raw.get("read_only_rootfs", False)),
            init=bool(raw.get("init", True)),
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
        if not isinstance(raw, dict):
            raise ValueError("filesystem must be a JSON object.")
        return cls(
            enforce_disk_quota=bool(raw.get("enforce_disk_quota", False)),
            workspace_path=str(raw.get("workspace_path") or "/workspace"),
            tmpfs_mb=int(raw.get("tmpfs_mb", 64)),
            run_tmpfs_mb=int(raw.get("run_tmpfs_mb", 16)),
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
        if not isinstance(raw, dict):
            raise ValueError("linux_host must be a JSON object.")
        return cls(
            enable_cron=bool(raw.get("enable_cron", raw.get("cron", False))),
            enable_sshd=bool(raw.get("enable_sshd", raw.get("sshd", False))),
            keep_alive=bool(raw.get("keep_alive", True)),
            writable_paths=_string_tuple(
                raw.get("writable_paths"),
                default=DEFAULT_LINUX_HOST_WRITABLE_PATHS,
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
        if isinstance(raw, bool):
            return cls(enabled=raw)
        if not isinstance(raw, dict):
            raise ValueError("ssh must be a boolean or JSON object.")
        keys = raw.get("authorized_keys", ())
        if isinstance(keys, str):
            authorized_keys = (keys,)
        elif isinstance(keys, list) and all(isinstance(item, str) for item in keys):
            authorized_keys = tuple(keys)
        else:
            authorized_keys = ()
        return cls(
            enabled=bool(raw.get("enabled", False)),
            user=str(raw.get("user") or "root"),
            host=str(raw.get("host") or "127.0.0.1"),
            host_port=int(raw["host_port"])
            if raw.get("host_port") is not None
            else None,
            container_port=(
                int(raw["container_port"])
                if raw.get("container_port") is not None
                else 22
            ),
            authorized_keys=authorized_keys,
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
class SandboxForkProtocolSpec:
    """Workload-side quiesce and post-restore identity handshake.

    The node invokes each command through ``docker exec`` and appends the
    checkpoint id, a per-operation nonce, and a role (``prepare``, ``resume``,
    or ``restore``). Commands must communicate with the initial process tree;
    the short-lived exec process itself is not present in the checkpoint.
    """

    version: str = ""
    prepare_command: tuple[str, ...] = ()
    ready_command: tuple[str, ...] = ()
    timeout_seconds: int = 30

    @classmethod
    def from_dict(cls, raw: object) -> "SandboxForkProtocolSpec":
        if raw is None:
            return cls()
        if not isinstance(raw, dict):
            raise ValueError("fork_protocol must be a JSON object.")
        return cls(
            version=str(raw.get("version") or ""),
            prepare_command=_string_tuple(raw.get("prepare_command"), default=()),
            ready_command=_string_tuple(raw.get("ready_command"), default=()),
            timeout_seconds=int(raw.get("timeout_seconds", 30)),
        )

    @property
    def enabled(self) -> bool:
        return bool(self.version or self.prepare_command or self.ready_command)

    def validate(self, *, required: bool) -> None:
        if not self.enabled:
            if required:
                raise ValueError(
                    "forkable sandboxes require fork_protocol agent-v1 hooks."
                )
            return
        if not required:
            raise ValueError("fork_protocol requires forkable=true.")
        if self.version != "agent-v1":
            raise ValueError("fork_protocol version must be 'agent-v1'.")
        for name, command in (
            ("prepare_command", self.prepare_command),
            ("ready_command", self.ready_command),
        ):
            if not command or any(not item or "\x00" in item for item in command):
                raise ValueError(f"fork_protocol {name} must be a non-empty argv list.")
        if not 1 <= self.timeout_seconds <= MAX_FORK_PROTOCOL_TIMEOUT_SECONDS:
            raise ValueError(
                "fork_protocol timeout_seconds must be in "
                f"[1, {MAX_FORK_PROTOCOL_TIMEOUT_SECONDS}]."
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "prepare_command": list(self.prepare_command),
            "ready_command": list(self.ready_command),
            "timeout_seconds": self.timeout_seconds,
        }


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
    forkable: bool = False
    fork_protocol: SandboxForkProtocolSpec = SandboxForkProtocolSpec()
    ssh: SandboxSshSpec = SandboxSshSpec()
    security: SandboxSecuritySpec = SandboxSecuritySpec()
    filesystem: SandboxFilesystemSpec = SandboxFilesystemSpec()
    linux_host: SandboxLinuxHostSpec = SandboxLinuxHostSpec()
    labels: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "SandboxSpec":
        profile = str(raw.get("profile") or raw.get("runtime_profile") or "container")
        command = raw.get("command", ())
        if isinstance(command, str):
            command_items: tuple[str, ...] = (command,)
        elif isinstance(command, list) and all(
            isinstance(item, str) for item in command
        ):
            command_items = tuple(command)
        else:
            command_items = ()
        env = raw.get("env") or {}
        labels = raw.get("labels") or {}
        security = SandboxSecuritySpec.from_dict(raw.get("security"))
        filesystem = SandboxFilesystemSpec.from_dict(raw.get("filesystem"))
        if profile == "linux_host":
            if raw.get("security") is None:
                security = linux_host_default_security()
            if raw.get("filesystem") is None:
                filesystem = linux_host_default_filesystem()
        return cls(
            id=str(raw.get("id") or ""),
            image=str(raw.get("image") or ""),
            profile=profile,
            command=command_items,
            env={str(k): str(v) for k, v in dict(env).items()},
            working_dir=str(raw["working_dir"]) if raw.get("working_dir") else None,
            memory_mb=(
                int(raw["memory_mb"]) if raw.get("memory_mb") is not None else None
            ),
            cpus=float(raw["cpus"]) if raw.get("cpus") is not None else None,
            disk_mb=int(raw["disk_mb"]) if raw.get("disk_mb") is not None else None,
            network=str(raw.get("network") or "none"),
            ttl_seconds=(
                int(raw["ttl_seconds"]) if raw.get("ttl_seconds") is not None else None
            ),
            forkable=bool(raw.get("forkable", False)),
            fork_protocol=SandboxForkProtocolSpec.from_dict(raw.get("fork_protocol")),
            ssh=SandboxSshSpec.from_dict(raw.get("ssh", raw.get("ssh_enabled"))),
            security=security,
            filesystem=filesystem,
            linux_host=SandboxLinuxHostSpec.from_dict(raw.get("linux_host")),
            labels={str(k): str(v) for k, v in dict(labels).items()},
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
        if self.forkable and self.memory_mb is None:
            raise ValueError("forkable sandboxes require an explicit memory_mb limit.")
        if self.forkable and self.disk_mb is None:
            raise ValueError(
                "forkable sandboxes require an explicit disk_mb limit so the "
                "checkpoint size can be admitted before quiescing the workload."
            )
        if self.forkable and self.ssh.enabled:
            raise ValueError(
                "forkable sandboxes cannot expose SSH because direct host-port "
                "sessions bypass the fork lifecycle barrier."
            )
        self.fork_protocol.validate(required=self.forkable)
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
        raw["fork_protocol"] = self.fork_protocol.to_dict()
        raw["ssh"] = self.ssh.to_dict()
        raw["security"] = self.security.to_dict()
        raw["filesystem"] = self.filesystem.to_dict()
        raw["linux_host"] = self.linux_host.to_dict()
        return raw

    def requested_resources(self) -> ResourceQuantity:
        return ResourceQuantity(
            vcpu=self.cpus or 0.0,
            memory_mb=self.memory_mb or 0,
            disk_mb=self.disk_mb or 0,
        )


@dataclass(frozen=True)
class SandboxRecord:
    spec: SandboxSpec
    container_name: str
    state: str
    created_at: datetime
    updated_at: datetime
    generation: int = 0
    operation_id: str = ""
    spec_hash: str = ""
    delete_operation_id: str = ""
    creation_kind: str = "fresh"
    source_sandbox_id: str = ""
    source_generation: int = 0
    checkpoint_id: str = ""
    fork_nonce: str = ""

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "SandboxRecord":
        spec_raw = raw.get("spec")
        if not isinstance(spec_raw, dict):
            raise ValueError("sandbox record is missing spec.")
        created_at = parse_iso_datetime(raw.get("created_at"))
        updated_at = parse_iso_datetime(raw.get("updated_at"))
        if created_at is None or updated_at is None:
            raise ValueError("sandbox record has invalid timestamps.")
        spec = SandboxSpec.from_dict(spec_raw)
        try:
            generation = int(raw.get("generation", 0))
        except (TypeError, ValueError) as exc:
            raise ValueError("sandbox record generation must be an integer.") from exc
        if generation < 0:
            raise ValueError("sandbox record generation cannot be negative.")
        return cls(
            spec=spec,
            container_name=str(raw.get("container_name") or ""),
            state=str(raw.get("state") or ""),
            created_at=created_at,
            updated_at=updated_at,
            generation=generation,
            operation_id=str(raw.get("operation_id") or ""),
            spec_hash=str(raw.get("spec_hash") or sandbox_spec_fingerprint(spec)),
            delete_operation_id=str(raw.get("delete_operation_id") or ""),
            creation_kind=str(raw.get("creation_kind") or "fresh"),
            source_sandbox_id=str(raw.get("source_sandbox_id") or ""),
            source_generation=max(0, int(raw.get("source_generation", 0))),
            checkpoint_id=str(raw.get("checkpoint_id") or ""),
            fork_nonce=str(raw.get("fork_nonce") or ""),
        )

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "id": self.spec.id,
            "sandbox_id": self.spec.id,
            "name": self.container_name,
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
            "delete_operation_id": self.delete_operation_id,
            "creation_kind": self.creation_kind,
            "source_sandbox_id": self.source_sandbox_id,
            "source_generation": self.source_generation,
            "checkpoint_id": self.checkpoint_id,
            "fork_nonce": self.fork_nonce,
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
class SandboxTombstone:
    sandbox_id: str
    generation: int
    operation_id: str
    spec_hash: str
    updated_at: datetime

    @classmethod
    def from_dict(cls, raw: object) -> "SandboxTombstone | None":
        if not isinstance(raw, dict):
            return None
        sandbox_id = str(raw.get("sandbox_id") or "").strip()
        if not sandbox_id:
            return None
        try:
            generation = int(raw.get("generation", 0))
        except (TypeError, ValueError):
            return None
        updated_at = parse_iso_datetime(raw.get("updated_at"))
        if generation < 0 or updated_at is None:
            return None
        return cls(
            sandbox_id=sandbox_id,
            generation=generation,
            operation_id=str(raw.get("operation_id") or ""),
            spec_hash=str(raw.get("spec_hash") or ""),
            updated_at=updated_at,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "sandbox_id": self.sandbox_id,
            "generation": self.generation,
            "operation_id": self.operation_id,
            "spec_hash": self.spec_hash,
            "updated_at": self.updated_at.isoformat(),
        }


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
class SandboxStoreState:
    records: dict[str, SandboxRecord]
    tombstones: dict[str, SandboxTombstone]
    revision: int
    drain: NodeDrainState = NodeDrainState()


@dataclass(frozen=True)
class SandboxActivitySnapshot:
    records: tuple[SandboxRecord, ...]
    active_sandboxes: int
    used_resources: ResourceQuantity
    reserved_resources: ResourceQuantity
    activity_revision: int


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


@dataclass(frozen=True)
class SandboxForkRuntimeResult:
    checkpoint_id: str
    commands: tuple[tuple[str, ...], ...]
    restored: bool = True


@dataclass
class _ForkSetupBudget:
    limit_seconds: float
    remaining_seconds: float

    @classmethod
    def start(cls, limit_seconds: float) -> "_ForkSetupBudget":
        return cls(
            limit_seconds=float(limit_seconds),
            remaining_seconds=float(limit_seconds),
        )


class SandboxLifecycleCoordinator:
    """Coordinates exec/file activity with checkpoint, restore, and delete.

    A gVisor restore deliberately kills exec-origin processes. Fork therefore
    takes an exclusive lease on both source and destination and fails fast if
    attached activity is present. Termination instead preempts shared activity:
    it closes admission before removing the container, while existing exec/file
    operations are severed by runtime deletion.
    """

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

    def run_with_timeout(
        self,
        argv: tuple[str, ...],
        *,
        timeout_seconds: float,
        input: bytes | None = None,
        max_stdout_bytes: int = 64 * 1024,
        max_stderr_bytes: int = 64 * 1024,
    ) -> CommandResult:
        """Run a lifecycle hook with a hard host-side deadline.

        A fresh process group ensures helper children cannot keep the node
        operation wedged after the deadline.  Exit 124 is reserved for timeout.
        """

        if max_stdout_bytes < 0 or max_stderr_bytes < 0:
            raise ValueError("command output limits cannot be negative")
        process = subprocess.Popen(
            list(argv),
            stdin=subprocess.PIPE if input is not None else subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
        )
        assert process.stdout is not None
        assert process.stderr is not None
        stdout = bytearray()
        stderr = bytearray()
        output_overflow = Event()

        def drain(
            stream: Any,
            retained: bytearray,
            limit: int,
        ) -> None:
            try:
                while chunk := stream.read(64 * 1024):
                    remaining = limit - len(retained)
                    if remaining > 0:
                        retained.extend(chunk[:remaining])
                    if len(chunk) > remaining and not output_overflow.is_set():
                        output_overflow.set()
                        try:
                            os.killpg(process.pid, signal.SIGKILL)
                        except ProcessLookupError:
                            pass
            finally:
                stream.close()

        stdout_thread = Thread(
            target=drain,
            args=(process.stdout, stdout, max_stdout_bytes),
            daemon=True,
        )
        stderr_thread = Thread(
            target=drain,
            args=(process.stderr, stderr, max_stderr_bytes),
            daemon=True,
        )
        stdout_thread.start()
        stderr_thread.start()
        if input is not None:
            assert process.stdin is not None
            try:
                process.stdin.write(input)
            except BrokenPipeError:
                pass
            finally:
                process.stdin.close()
        try:
            exit_code = process.wait(timeout=timeout_seconds)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait()
            detail = f"command timed out after {timeout_seconds:g}s".encode()
            stderr.extend(detail[: max(0, max_stderr_bytes - len(stderr))])
            exit_code = 124
        stdout_thread.join()
        stderr_thread.join()
        if output_overflow.is_set() and exit_code != 124:
            detail = b"command output exceeded limit"
            stderr.extend(detail[: max(0, max_stderr_bytes - len(stderr))])
            exit_code = 125
        stdout_bytes = bytes(stdout)
        stderr_bytes = bytes(stderr)
        return CommandResult(
            argv=argv,
            exit_code=exit_code,
            stdout=stdout_bytes.decode("utf-8", errors="replace"),
            stderr=stderr_bytes.decode("utf-8", errors="replace"),
            stdout_bytes=stdout_bytes,
            stderr_bytes=stderr_bytes,
        )

    def run_bounded_stdout(
        self,
        argv: tuple[str, ...],
        *,
        max_stdout_bytes: int,
        max_stderr_bytes: int = 64 * 1024,
    ) -> CommandResult:
        """Run a command while retaining at most stdout-limit+1 and bounded stderr.

        Both pipes are drained concurrently so a noisy diagnostic stream cannot
        deadlock the child.  Once one byte beyond the stdout limit is observed,
        the child is killed; readers continue draining until the pipes close.
        """
        if max_stdout_bytes < 0 or max_stderr_bytes < 0:
            raise ValueError("command output limits cannot be negative")
        process = subprocess.Popen(
            list(argv),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        assert process.stdout is not None
        assert process.stderr is not None
        stdout = bytearray()
        stderr = bytearray()
        stdout_overflow = Event()

        def drain_stdout() -> None:
            try:
                while chunk := process.stdout.read(64 * 1024):
                    retained_limit = max_stdout_bytes + 1
                    remaining = retained_limit - len(stdout)
                    if remaining > 0:
                        stdout.extend(chunk[:remaining])
                    if len(stdout) > max_stdout_bytes and not stdout_overflow.is_set():
                        stdout_overflow.set()
                        try:
                            process.kill()
                        except ProcessLookupError:
                            pass
            finally:
                process.stdout.close()

        def drain_stderr() -> None:
            try:
                while chunk := process.stderr.read(64 * 1024):
                    remaining = max_stderr_bytes - len(stderr)
                    if remaining > 0:
                        stderr.extend(chunk[:remaining])
            finally:
                process.stderr.close()

        stdout_thread = Thread(target=drain_stdout, daemon=True)
        stderr_thread = Thread(target=drain_stderr, daemon=True)
        stdout_thread.start()
        stderr_thread.start()
        exit_code = process.wait()
        stdout_thread.join()
        stderr_thread.join()
        stdout_bytes = bytes(stdout)
        stderr_bytes = bytes(stderr)
        return CommandResult(
            argv=argv,
            exit_code=exit_code,
            stdout=stdout_bytes.decode("utf-8", errors="replace"),
            stderr=stderr_bytes.decode("utf-8", errors="replace"),
            stdout_bytes=stdout_bytes,
            stderr_bytes=stderr_bytes,
        )


class RecordingExecutor:
    def __init__(
        self,
        exit_code: int = 0,
        *,
        stdout: str = "",
        stderr: str = "",
        stdout_bytes: bytes | None = None,
        stderr_bytes: bytes | None = None,
    ) -> None:
        self.exit_code = exit_code
        self.stdout = stdout
        self.stderr = stderr
        self.stdout_bytes = (
            stdout_bytes if stdout_bytes is not None else stdout.encode()
        )
        self.stderr_bytes = (
            stderr_bytes if stderr_bytes is not None else stderr.encode()
        )
        self.commands: list[tuple[str, ...]] = []
        self.inputs: list[bytes | None] = []

    def run(
        self, argv: tuple[str, ...], *, input: bytes | None = None
    ) -> CommandResult:
        self.commands.append(argv)
        self.inputs.append(input)
        return CommandResult(
            argv=argv,
            exit_code=self.exit_code,
            stdout=self.stdout,
            stderr=self.stderr,
            stdout_bytes=self.stdout_bytes,
            stderr_bytes=self.stderr_bytes,
        )

    def run_with_timeout(
        self,
        argv: tuple[str, ...],
        *,
        timeout_seconds: float,
        input: bytes | None = None,
    ) -> CommandResult:
        del timeout_seconds
        return self.run(argv, input=input)

    def run_bounded_stdout(
        self,
        argv: tuple[str, ...],
        *,
        max_stdout_bytes: int,
        max_stderr_bytes: int = 64 * 1024,
    ) -> CommandResult:
        self.commands.append(argv)
        self.inputs.append(None)
        stdout_bytes = self.stdout_bytes[: max_stdout_bytes + 1]
        stderr_bytes = self.stderr_bytes[:max_stderr_bytes]
        return CommandResult(
            argv=argv,
            exit_code=self.exit_code,
            stdout=stdout_bytes.decode("utf-8", errors="replace"),
            stderr=stderr_bytes.decode("utf-8", errors="replace"),
            stdout_bytes=stdout_bytes,
            stderr_bytes=stderr_bytes,
        )


class DockerGvisorRuntime:
    def __init__(
        self,
        *,
        executor: CommandExecutor | None = None,
        docker_binary: str = "docker",
        runtime_name: str = "runsc",
        fork_runtime_name: str = DEFAULT_FORK_RUNTIME_NAME,
        container_prefix: str = DEFAULT_CONTAINER_PREFIX,
        allow_storage_opt_quota: bool = False,
        allow_tmpfs_workspace: bool = False,
        fork_enabled: bool = False,
        checkpoint_root: Path | None = None,
        checkpoint_helper: str = "/usr/local/libexec/ucloud-sandbox-checkpoint",
        checkpoint_helper_sudo: bool = True,
        fork_command_timeout_seconds: int = MAX_FORK_CHECKPOINT_TIMEOUT_SECONDS,
        fork_restore_timeout_seconds: int = MAX_FORK_RESTORE_TIMEOUT_SECONDS,
        fork_restore_parallelism: int = DEFAULT_FORK_RESTORE_PARALLELISM,
        dry_run: bool = False,
    ) -> None:
        self.executor = executor or SubprocessExecutor()
        self.docker_binary = docker_binary
        self.runtime_name = runtime_name
        self.fork_runtime_name = fork_runtime_name
        self.container_prefix = container_prefix
        self.allow_storage_opt_quota = allow_storage_opt_quota
        self.allow_tmpfs_workspace = allow_tmpfs_workspace
        self.fork_enabled = bool(fork_enabled)
        self.checkpoint_root = checkpoint_root
        self.checkpoint_helper = checkpoint_helper
        self.checkpoint_helper_sudo = checkpoint_helper_sudo
        self.fork_command_timeout_seconds = int(fork_command_timeout_seconds)
        self.fork_restore_timeout_seconds = int(fork_restore_timeout_seconds)
        self.fork_restore_parallelism = int(fork_restore_parallelism)
        self.dry_run = dry_run
        self._operation_local = local()

        if self.fork_enabled and self.checkpoint_root is None:
            raise ValueError("checkpoint_root is required when live fork is enabled")
        validate_security_value("fork runtime name", self.fork_runtime_name)
        if not (
            1
            <= self.fork_command_timeout_seconds
            <= MAX_FORK_CHECKPOINT_TIMEOUT_SECONDS
        ):
            raise ValueError(
                "fork checkpoint timeout must be in "
                f"[1, {MAX_FORK_CHECKPOINT_TIMEOUT_SECONDS}] seconds"
            )
        if not (
            1 <= self.fork_restore_timeout_seconds <= MAX_FORK_RESTORE_TIMEOUT_SECONDS
        ):
            raise ValueError(
                "fork restore timeout must be in "
                f"[1, {MAX_FORK_RESTORE_TIMEOUT_SECONDS}] seconds"
            )
        if not (
            DEFAULT_FORK_RESTORE_PARALLELISM
            <= self.fork_restore_parallelism
            <= MAX_FORK_RESTORE_PARALLELISM
        ):
            raise ValueError(
                "fork restore parallelism must be in "
                f"[{DEFAULT_FORK_RESTORE_PARALLELISM}, "
                f"{MAX_FORK_RESTORE_PARALLELISM}]"
            )

    def container_name(self, sandbox_id: str) -> str:
        return f"{self.container_prefix}{sandbox_id}"

    def prepare_application_checkpoint(
        self,
        spec: SandboxSpec,
        operation: SandboxOperation,
        *,
        _fork_setup_budget: _ForkSetupBudget | None = None,
    ) -> CommandResult:
        """Create the private runsc application-checkpoint directory."""

        if not spec.forkable:
            return CommandResult(argv=(), exit_code=0)
        if not self.fork_enabled or self.checkpoint_root is None:
            raise SandboxForkUnsupportedError(
                "live sandbox fork is unavailable; this node has not passed "
                "gVisor cross-container restore conformance"
            )
        application_id = application_checkpoint_id(
            spec.id,
            operation.generation,
            operation.spec_hash,
        )
        command = self._checkpoint_helper_command("app-prepare", application_id)
        if _fork_setup_budget is not None:
            return self._run_fork_setup_required(
                command,
                budget=_fork_setup_budget,
                phase="prepare application checkpoint directory",
            )
        return self._run(command)

    def drop_application_checkpoint(
        self,
        sandbox_id: str,
        generation: int,
        spec_hash: str,
        *,
        _fork_setup_budget: _ForkSetupBudget | None = None,
    ) -> CommandResult:
        """Remove one generation's private runsc checkpoint directory."""

        if self.checkpoint_root is None:
            return CommandResult(argv=(), exit_code=0)
        application_id = application_checkpoint_id(
            sandbox_id,
            generation,
            spec_hash,
        )
        command = self._checkpoint_helper_command("app-drop", application_id)
        if _fork_setup_budget is not None:
            return self._run_fork_setup_required(
                command,
                budget=_fork_setup_budget,
                phase="drop application checkpoint directory",
            )
        return self._run(command)

    def _drop_application_checkpoint_best_effort(
        self,
        sandbox_id: str,
        generation: int,
        spec_hash: str,
        *,
        _fork_setup_budget: _ForkSetupBudget | None = None,
    ) -> CommandResult:
        if self.checkpoint_root is None:
            return CommandResult(argv=(), exit_code=0)
        command = self._checkpoint_helper_command(
            "app-drop",
            application_checkpoint_id(sandbox_id, generation, spec_hash),
        )
        if _fork_setup_budget is not None:
            return self._run_fork_setup_best_effort(
                command,
                budget=_fork_setup_budget,
                phase="drop application checkpoint directory",
            )
        return self._run_best_effort(command)

    def create(
        self,
        spec: SandboxSpec,
        operation: SandboxOperation | None = None,
    ) -> CommandResult:
        spec.validate()
        operation = operation or getattr(self._operation_local, "operation", None)
        operation = operation or SandboxOperation.legacy_create(spec)
        if spec.forkable:
            self.prepare_application_checkpoint(spec, operation)
        try:
            return self._run(self.create_command(spec, operation=operation))
        except SandboxRuntimeCommandError:
            # Docker returned a definite create failure, so no runsc process can
            # still be writing the generation-scoped application directory.
            if spec.forkable:
                self._drop_application_checkpoint_best_effort(
                    spec.id,
                    operation.generation,
                    operation.spec_hash,
                )
            raise

    def create_with_operation(
        self,
        spec: SandboxSpec,
        operation: SandboxOperation,
    ) -> CommandResult:
        # Preserve pre-protocol runtime subclasses for legacy requests.  The
        # production runtime takes the versioned path so its container labels
        # carry the fencing identity.
        if type(self).create is DockerGvisorRuntime.create:
            return self.create(spec, operation=operation)
        self._operation_local.operation = operation
        try:
            return self.create(spec)
        finally:
            del self._operation_local.operation

    def delete(
        self,
        sandbox_id: str,
        *,
        _fork_setup_budget: _ForkSetupBudget | None = None,
    ) -> CommandResult:
        if not SANDBOX_ID_RE.match(sandbox_id):
            raise ValueError("invalid sandbox id.")
        argv = (
            self.docker_binary,
            "rm",
            "-f",
            self.container_name(sandbox_id),
        )
        if self.dry_run:
            return CommandResult(argv=argv, exit_code=0)
        result = (
            self._run_fork_setup_command(
                argv,
                budget=_fork_setup_budget,
                phase="remove destination container",
            )
            if _fork_setup_budget is not None
            else self.executor.run(argv)
        )
        if result.exit_code == 0:
            return result
        if self._is_container_not_found(result):
            return replace(result, exit_code=0)
        raise RuntimeError(
            f"command failed with exit code {result.exit_code}: {' '.join(argv)}\n"
            f"{result.stderr}"
        )

    def snapshot(self, sandbox_id: str, target_image: str) -> CommandResult:
        if not SANDBOX_ID_RE.match(sandbox_id):
            raise ValueError("invalid sandbox id.")
        if not target_image.strip():
            raise ValueError("target image is required.")
        return self._run(
            (
                self.docker_binary,
                "commit",
                self.container_name(sandbox_id),
                target_image,
            )
        )

    def copy_to_container(
        self,
        sandbox_id: str,
        source_path: Path,
        container_path: str,
    ) -> CommandResult:
        if not SANDBOX_ID_RE.match(sandbox_id):
            raise ValueError("invalid sandbox id.")
        validate_container_file_path("container_path", container_path)
        if not source_path.is_file():
            raise ValueError("source_path must be a file.")
        return self._run(
            (
                self.docker_binary,
                "cp",
                str(source_path),
                f"{self.container_name(sandbox_id)}:{container_path}",
            )
        )

    def copy_from_container(
        self,
        sandbox_id: str,
        container_path: str,
        target_path: Path,
    ) -> CommandResult:
        if not SANDBOX_ID_RE.match(sandbox_id):
            raise ValueError("invalid sandbox id.")
        validate_container_file_path("container_path", container_path)
        return self._run(
            (
                self.docker_binary,
                "cp",
                f"{self.container_name(sandbox_id)}:{container_path}",
                str(target_path),
            )
        )

    def write_file_to_container(
        self,
        sandbox_id: str,
        container_path: str,
        content: bytes,
        *,
        owner: str | None = None,
    ) -> CommandResult:
        validate_container_file_path("container_path", container_path)
        env = {"UCLOUD_SANDBOX_FILE": container_path}
        if owner:
            validate_security_value("file owner", owner)
            env["UCLOUD_SANDBOX_OWNER"] = owner
        return self._run(
            self.exec_command(
                sandbox_id,
                (
                    "sh",
                    "-c",
                    (
                        "set -eu\n"
                        'target="${UCLOUD_SANDBOX_FILE:?}"\n'
                        'parent="${target%/*}"\n'
                        'if [ -z "$parent" ] || [ "$parent" = "$target" ]; then parent=/; fi\n'
                        'mkdir -p -- "$parent"\n'
                        'cat > "$target"\n'
                        'if [ -n "${UCLOUD_SANDBOX_OWNER:-}" ]; then '
                        'chown "$UCLOUD_SANDBOX_OWNER" "$target" 2>/dev/null || true; '
                        "fi\n"
                        'chmod u+rw,go+r "$target" 2>/dev/null || true\n'
                    ),
                ),
                env=env,
                interactive=True,
                user="0",
            ),
            input=content,
        )

    def read_file_from_container(
        self,
        sandbox_id: str,
        container_path: str,
        *,
        max_bytes: int | None = None,
    ) -> tuple[bytes, CommandResult]:
        validate_container_file_path("container_path", container_path)
        argv = self.exec_command(
            sandbox_id,
            ("sh", "-c", 'cat "${UCLOUD_SANDBOX_FILE:?}"'),
            env={"UCLOUD_SANDBOX_FILE": container_path},
            interactive=False,
            user="0",
        )
        if max_bytes is None or self.dry_run:
            result = self._run(argv)
        else:
            bounded_run = getattr(self.executor, "run_bounded_stdout", None)
            if bounded_run is None:
                # Compatibility for injected executors.  Production uses the
                # bounded SubprocessExecutor path above.
                result = self.executor.run(argv)
            else:
                result = bounded_run(argv, max_stdout_bytes=max_bytes)
            if len(result.stdout_bytes) > max_bytes:
                raise SandboxFileTooLargeError(
                    f"sandbox file exceeds the {max_bytes} byte download limit"
                )
            if result.exit_code != 0:
                raise RuntimeError(
                    f"command failed with exit code {result.exit_code}: {' '.join(argv)}\n"
                    f"{result.stderr}"
                )
        return result.stdout_bytes, result

    def exec_command(
        self,
        sandbox_id: str,
        command: tuple[str, ...],
        *,
        env: dict[str, str] | None = None,
        working_dir: str | None = None,
        interactive: bool = True,
        tty: bool = False,
        user: str | None = None,
    ) -> tuple[str, ...]:
        if not SANDBOX_ID_RE.match(sandbox_id):
            raise ValueError("invalid sandbox id.")
        if not command:
            raise ValueError("exec command cannot be empty.")
        for key in env or {}:
            if not ENV_KEY_RE.match(key):
                raise ValueError(f"invalid environment variable name: {key!r}")
        if user is not None:
            validate_security_value("exec user", user)
        argv: list[str] = [self.docker_binary, "exec"]
        if interactive:
            argv.append("-i")
        if tty:
            argv.append("-t")
        if working_dir:
            argv.extend(["-w", working_dir])
        for key in sorted(env or {}):
            argv.extend(["-e", f"{key}={env[key]}"])
        if user is not None:
            argv.extend(["-u", user])
        argv.append(self.container_name(sandbox_id))
        argv.extend(command)
        return tuple(argv)

    def create_command(
        self,
        spec: SandboxSpec,
        operation: SandboxOperation | None = None,
        *,
        start: bool = True,
        image_override: str | None = None,
        extra_env: dict[str, str] | None = None,
        runtime_override: str | None = None,
        extra_annotations: dict[str, str] | None = None,
    ) -> tuple[str, ...]:
        spec.validate()
        operation = operation or SandboxOperation.legacy_create(spec)
        argv: list[str] = [
            self.docker_binary,
            "run" if start else "create",
        ]
        if start:
            argv.append("-d")
        argv.extend(
            [
                "--name",
                self.container_name(spec.id),
                "--runtime",
                runtime_override or self.runtime_name,
                "--network",
                spec.network,
                "--label",
                "ucloud-sandboxes.managed=true",
                "--label",
                f"ucloud-sandboxes.sandbox-id={spec.id}",
                "--label",
                f"{SANDBOX_SPEC_HASH_LABEL}={operation.spec_hash}",
                "--label",
                f"{SANDBOX_GENERATION_LABEL}={operation.generation}",
                "--label",
                f"{SANDBOX_OPERATION_ID_LABEL}={operation.operation_id}",
            ]
        )
        if spec.forkable and self.checkpoint_root is not None:
            # The workload can open /proc/gvisor/checkpoint before a fork to
            # distinguish source resume from child restore, and read the
            # child's fresh OCI environment from /proc/gvisor/spec_environ.
            application_path = (
                self.checkpoint_root
                / "application"
                / application_checkpoint_id(
                    spec.id,
                    operation.generation,
                    operation.spec_hash,
                )
            )
            for key, value in (
                ("dev.gvisor.internal.checkpoint.path", str(application_path)),
                ("dev.gvisor.internal.checkpoint.resume", "true"),
                ("dev.gvisor.internal.checkpoint.compression", "none"),
            ):
                argv.extend(["--annotation", f"{key}={value}"])
        for key, value in sorted((extra_annotations or {}).items()):
            if key != RESTORE_CHECKPOINT_ANNOTATION:
                raise ValueError(f"unsupported internal OCI annotation: {key}")
            if not value or "\n" in value or "\r" in value or "=" in value:
                raise ValueError("invalid internal OCI annotation value")
            argv.extend(["--annotation", f"{key}={value}"])
        if spec.memory_mb is not None:
            argv.extend(["--memory", f"{spec.memory_mb}m"])
            # Make the intended bounded swap allowance explicit. Docker's
            # --memory-swap value is the combined memory + swap ceiling.
            argv.extend(["--memory-swap", f"{spec.memory_mb * 2}m"])
        if spec.cpus is not None:
            argv.extend(["--cpus", _format_float(spec.cpus)])
        disk_quota_enforced = (
            spec.disk_mb is not None and spec.filesystem.enforce_disk_quota
        )
        if disk_quota_enforced and not self.allow_tmpfs_workspace:
            raise ValueError(
                "filesystem.enforce_disk_quota requires a node runtime with "
                "validated tmpfs workspace support."
            )
        if (
            spec.disk_mb is not None
            and not disk_quota_enforced
            and not self.allow_storage_opt_quota
        ):
            raise ValueError(
                "disk_mb requires a node runtime with validated Docker storage "
                "quota support."
            )
        if spec.disk_mb is not None and not disk_quota_enforced:
            argv.extend(["--storage-opt", f"size={spec.disk_mb}m"])
        if spec.security.init:
            argv.append("--init")
        if spec.security.user:
            argv.extend(["--user", spec.security.user])
        if spec.security.no_new_privileges:
            argv.extend(["--security-opt", "no-new-privileges"])
        for item in spec.security.cap_drop:
            argv.extend(["--cap-drop", item])
        for item in spec.security.cap_add:
            argv.extend(["--cap-add", item])
        if spec.security.pids_limit is not None:
            argv.extend(["--pids-limit", str(spec.security.pids_limit)])
        if spec.security.read_only_rootfs or disk_quota_enforced:
            argv.append("--read-only")
        argv.extend(
            [
                "--tmpfs",
                f"/tmp:rw,nosuid,nodev,size={spec.filesystem.tmpfs_mb}m",
                "--tmpfs",
                f"/run:rw,nosuid,nodev,size={spec.filesystem.run_tmpfs_mb}m",
            ]
        )
        if disk_quota_enforced:
            argv.extend(
                [
                    "--tmpfs",
                    (
                        f"{spec.filesystem.workspace_path}:"
                        f"rw,nosuid,nodev,size={spec.disk_mb}m"
                    ),
                ]
            )
        effective_workdir = spec.working_dir
        if disk_quota_enforced and effective_workdir is None:
            effective_workdir = spec.filesystem.workspace_path
        if effective_workdir:
            argv.extend(["--workdir", effective_workdir])
        effective_env = dict(spec.env)
        effective_env.update(extra_env or {})
        if spec.forkable:
            effective_env["UCLOUD_SANDBOX_ID"] = spec.id
        for key in sorted(effective_env):
            if not ENV_KEY_RE.match(key):
                raise ValueError(f"invalid environment variable name: {key!r}")
            argv.extend(["-e", f"{key}={effective_env[key]}"])
        if spec.ssh.enabled and spec.ssh.authorized_keys:
            argv.extend(
                [
                    "-e",
                    "UCLOUD_SANDBOX_SSH_AUTHORIZED_KEYS="
                    + "\n".join(spec.ssh.authorized_keys),
                ]
            )
        if spec.ssh.enabled:
            if spec.ssh.host_port is None:
                raise ValueError(
                    "ssh host_port must be assigned before runtime create."
                )
            argv.extend(
                [
                    "-p",
                    (
                        f"{spec.ssh.host}:{spec.ssh.host_port}:"
                        f"{spec.ssh.container_port}"
                    ),
                ]
            )
        if spec.profile == "linux_host":
            argv.extend(
                [
                    "-e",
                    "UCLOUD_SANDBOX_PROFILE=linux_host",
                    "-e",
                    "UCLOUD_SANDBOX_LINUX_HOST_PATHS="
                    + ":".join(spec.linux_host.writable_paths),
                    "-e",
                    "UCLOUD_SANDBOX_ENABLE_CRON="
                    + ("1" if spec.linux_host.enable_cron else "0"),
                    "-e",
                    "UCLOUD_SANDBOX_ENABLE_SSHD="
                    + ("1" if spec.linux_host.enable_sshd or spec.ssh.enabled else "0"),
                    "-e",
                    f"UCLOUD_SANDBOX_SSH_USER={spec.ssh.user}",
                    "-e",
                    f"UCLOUD_SANDBOX_SSH_PORT={spec.ssh.container_port}",
                    "-e",
                    "UCLOUD_SANDBOX_KEEP_ALIVE="
                    + ("1" if spec.linux_host.keep_alive else "0"),
                    "--entrypoint",
                    "/bin/sh",
                ]
            )
        for key in sorted(spec.labels):
            argv.extend(["--label", f"{key}={spec.labels[key]}"])
        argv.append(image_override or spec.image)
        if spec.profile == "linux_host":
            argv.extend(
                [
                    "-lc",
                    linux_host_entrypoint_script(),
                    "ucloud-linux-host",
                ]
            )
        argv.extend(spec.command)
        return tuple(argv)

    def create_stopped_command(
        self,
        spec: SandboxSpec,
        operation: SandboxOperation,
        *,
        image: str,
        extra_env: dict[str, str] | None = None,
        restore_checkpoint: str | None = None,
    ) -> tuple[str, ...]:
        return self.create_command(
            spec,
            operation=operation,
            start=False,
            image_override=image,
            extra_env=extra_env,
            runtime_override=(self.fork_runtime_name if restore_checkpoint else None),
            extra_annotations=(
                {RESTORE_CHECKPOINT_ANNOTATION: restore_checkpoint}
                if restore_checkpoint
                else None
            ),
        )

    def fork(
        self,
        source: SandboxSpec,
        target: SandboxSpec,
        operation: SandboxOperation,
        *,
        source_generation: int,
        source_spec_hash: str,
        source_operation_id: str = "",
        checkpoint_id: str,
        fork_nonce: str,
        _checkpoint_prepared: bool = False,
        _require_existing_checkpoint: bool = False,
        _source_fenced: bool = False,
        _capture_only: bool = False,
    ) -> SandboxForkRuntimeResult:
        """Checkpoint ``source`` once and restore it as a distinct container.

        The root-owned helper performs a validated XFS reflink import into the
        destination's private checkpoint directory. Docker/containerd create
        the child and remain lifecycle owners, while the dedicated OCI runtime
        wrapper substitutes raw ``runsc restore`` for the ordinary start.
        """

        if not self.fork_enabled or self.checkpoint_root is None:
            raise SandboxForkUnsupportedError(
                "live sandbox fork is unavailable; this node has not passed "
                "gVisor cross-container restore conformance"
            )
        validate_fork_compatibility(source, target)
        if source.memory_mb is None:
            raise ValueError("live fork requires an explicit source memory_mb limit")
        if source.disk_mb is None:
            raise ValueError("live fork requires an explicit source disk_mb limit")
        validate_checkpoint_id(checkpoint_id)
        validate_fork_nonce(fork_nonce)
        checkpoint_name = "state"
        commands: list[tuple[str, ...]] = []
        capture_setup_budget = _ForkSetupBudget.start(
            FORK_CAPTURE_SETUP_ALLOWANCE_SECONDS
        )
        child_setup_budget = _ForkSetupBudget.start(FORK_CHILD_SETUP_ALLOWANCE_SECONDS)
        source_setup_budget = (
            child_setup_budget if _checkpoint_prepared else capture_setup_budget
        )

        if not self.dry_run and not _source_fenced:
            source_identity = self.managed_container_identity(
                source.id,
                _fork_setup_budget=source_setup_budget,
            )
            expected_source_identity = (
                source_generation,
                source_operation_id,
                source_spec_hash,
            )
            if source_identity != expected_source_identity:
                raise SandboxStaleOperationError(
                    f"source runtime identity changed before fork: {source.id}"
                )
            if not self.managed_container_running(
                source.id,
                _fork_setup_budget=source_setup_budget,
            ):
                raise SandboxStaleOperationError(
                    f"source runtime is not running before fork: {source.id}"
                )

        source_container_id, source_image_id = self._container_runtime_info(
            source.id,
            _fork_setup_budget=source_setup_budget,
        )
        expected_manifest = {
            "artifact_id": checkpoint_id,
            "checkpoint_id": checkpoint_name,
            "source_container_id": source_container_id,
            "source_image_id": source_image_id,
            "source_spec_hash": source_spec_hash,
        }

        checkpoint_ready = _checkpoint_prepared
        if not checkpoint_ready:
            status_command = self._checkpoint_helper_command("status", checkpoint_id)
            commands.append(status_command)
        if not checkpoint_ready and not self.dry_run:
            status = self._run_fork_setup_command(
                status_command,
                budget=capture_setup_budget,
                phase="inspect checkpoint artifact",
            )
            if status.exit_code == 0:
                try:
                    manifest = json.loads(status.stdout)
                except json.JSONDecodeError as exc:
                    raise RuntimeError(
                        "checkpoint helper returned an invalid artifact manifest"
                    ) from exc
                if not isinstance(manifest, dict) or any(
                    str(manifest.get(key) or "") != value
                    for key, value in expected_manifest.items()
                ):
                    raise SandboxConflictError(
                        f"checkpoint artifact identity conflicts with fork operation: "
                        f"{checkpoint_id}"
                    )
                checkpoint_ready = True
            elif status.exit_code not in {3, 4}:
                raise RuntimeError(
                    f"checkpoint helper status failed with exit code "
                    f"{status.exit_code}: {status.stderr}"
                )

            if not checkpoint_ready and status.exit_code == 3:
                # The process may have died after Docker completed the save
                # but before the helper sealed it.  Adopt that exact checkpoint
                # before considering a new save, preserving replay semantics.
                seal_recovery = self._checkpoint_helper_command("seal", checkpoint_id)
                commands.append(seal_recovery)
                sealed = self._run_fork_setup_command(
                    seal_recovery,
                    budget=capture_setup_budget,
                    phase="recover checkpoint artifact",
                )
                if sealed.exit_code != 0:
                    raise RuntimeError(
                        "checkpoint artifact is pending without a durable Docker "
                        f"completion marker; refusing to recapture: {checkpoint_id}"
                    )
                try:
                    manifest = json.loads(sealed.stdout)
                except json.JSONDecodeError as exc:
                    raise RuntimeError(
                        "checkpoint helper returned an invalid sealed manifest"
                    ) from exc
                if not isinstance(manifest, dict) or any(
                    str(manifest.get(key) or "") != value
                    for key, value in expected_manifest.items()
                ):
                    raise SandboxConflictError(
                        f"checkpoint artifact identity conflicts with fork "
                        f"operation: {checkpoint_id}"
                    )
                checkpoint_ready = True

        if not checkpoint_ready and _require_existing_checkpoint:
            raise SandboxConflictError(
                "sealed checkpoint disappeared after a sibling was restored; "
                f"refusing to recapture a different memory instant: {checkpoint_id}"
            )

        if not checkpoint_ready:
            reset_command = self._checkpoint_helper_command("drop", checkpoint_id)
            commands.append(reset_command)
            self._run_fork_setup_best_effort(
                reset_command,
                budget=capture_setup_budget,
                phase="reset checkpoint artifact",
            )
            prepare_command = self._checkpoint_helper_command(
                "prepare",
                checkpoint_id,
                source_container_id,
                source_image_id,
                source_spec_hash,
                checkpoint_name,
                str(source.memory_mb),
                str(source.disk_mb),
                str(source.filesystem.tmpfs_mb),
                str(source.filesystem.run_tmpfs_mb),
            )
            commands.append(prepare_command)
            # Capacity admission must complete before asking the workload to
            # quiesce. A low-space refusal therefore leaves PID 1 untouched.
            try:
                self._run_fork_setup_required(
                    prepare_command,
                    budget=capture_setup_budget,
                    phase="prepare checkpoint artifact",
                )
            except SandboxForkCommandTimeoutError:
                # This helper process and its reflink children are in the
                # killed local process group; Docker checkpointing has not
                # started and PID 1 has not been quiesced, so exact cleanup is
                # safe even if prepare committed just before client timeout.
                cleanup_budget = _ForkSetupBudget.start(
                    FORK_SETUP_CLEANUP_ALLOWANCE_SECONDS
                )
                self._run_fork_setup_best_effort(
                    reset_command,
                    budget=cleanup_budget,
                    phase="clean timed-out checkpoint preparation",
                )
                raise
            checkpoint_parent = self.checkpoint_root / checkpoint_id / "pending"
            checkpoint_command = (
                self.docker_binary,
                "checkpoint",
                "create",
                "--leave-running",
                "--checkpoint-dir",
                str(checkpoint_parent),
                self.container_name(source.id),
                checkpoint_name,
            )
            try:
                prepare_hook = self.fork_protocol_command(
                    source,
                    source.fork_protocol.prepare_command,
                    checkpoint_id=checkpoint_id,
                    fork_nonce=fork_nonce,
                    role="prepare",
                )
                commands.append(prepare_hook)
                self._run_fork_protocol_hook(
                    prepare_hook,
                    sandbox_id=source.id,
                    role="prepare",
                    fork_nonce=fork_nonce,
                    timeout_seconds=source.fork_protocol.timeout_seconds,
                )
                commands.append(checkpoint_command)
                self._run_fork_command(checkpoint_command, phase="checkpoint")
                if not self.dry_run and not self.managed_container_running(
                    source.id,
                    _fork_setup_budget=capture_setup_budget,
                ):
                    raise RuntimeError(
                        f"source sandbox did not resume after checkpoint: {source.id}"
                    )
                complete_command = self._checkpoint_helper_command(
                    "complete", checkpoint_id
                )
                commands.append(complete_command)
                self._run_fork_setup_required(
                    complete_command,
                    budget=capture_setup_budget,
                    phase="record checkpoint completion",
                )
                seal_command = self._checkpoint_helper_command("seal", checkpoint_id)
                commands.append(seal_command)
                self._run_fork_setup_required(
                    seal_command,
                    budget=capture_setup_budget,
                    phase="seal checkpoint artifact",
                )
            except SandboxForkCommandTimeoutError:
                # The Docker client deadline does not prove dockerd/runsc has
                # stopped writing. Leave the unsealed artifact for fail-closed
                # replay instead of racing it with cleanup or recapture.
                raise
            except Exception:
                # Docker either has not started or returned a definite
                # failure/completion. Tell the source agent to abandon its
                # quiesced checkpoint state before removing the artifact.
                cancel_hook = self.fork_protocol_command(
                    source,
                    source.fork_protocol.ready_command,
                    checkpoint_id=checkpoint_id,
                    fork_nonce=fork_nonce,
                    role="cancel",
                )
                commands.append(cancel_hook)
                cancel_acknowledged = False
                try:
                    self._run_fork_protocol_hook(
                        cancel_hook,
                        sandbox_id=source.id,
                        role="cancel",
                        fork_nonce=fork_nonce,
                        timeout_seconds=source.fork_protocol.timeout_seconds,
                    )
                    cancel_acknowledged = True
                except RuntimeError:
                    pass
                if cancel_acknowledged:
                    # A partial artifact is never a valid replay point. The
                    # helper scopes deletion to this validated artifact ID.
                    drop_command = self._checkpoint_helper_command(
                        "drop", checkpoint_id
                    )
                    commands.append(drop_command)
                    self._run_fork_setup_best_effort(
                        drop_command,
                        budget=capture_setup_budget,
                        phase="drop failed checkpoint artifact",
                    )
                # Without a nonce-acknowledged cancel, retain the pending
                # artifact. It durably quarantines source activity and target
                # deletion rather than assuming PID 1 left its quiesced state.
                raise

        if not _checkpoint_prepared:
            resume_hook = self.fork_protocol_command(
                source,
                source.fork_protocol.ready_command,
                checkpoint_id=checkpoint_id,
                fork_nonce=fork_nonce,
                role="resume",
            )
            commands.append(resume_hook)
            self._run_fork_protocol_hook(
                resume_hook,
                sandbox_id=source.id,
                role="resume",
                fork_nonce=fork_nonce,
                timeout_seconds=source.fork_protocol.timeout_seconds,
            )

        if _capture_only:
            return SandboxForkRuntimeResult(
                checkpoint_id=checkpoint_id,
                commands=tuple(commands),
            )

        existing_identity = self.managed_container_identity(
            target.id,
            _fork_setup_budget=child_setup_budget,
        )
        if existing_identity is not None:
            expected_identity = (
                operation.generation,
                operation.operation_id,
                operation.spec_hash,
            )
            if existing_identity != expected_identity:
                raise SandboxConflictError(
                    f"runtime sandbox already exists with another identity: {target.id}"
                )
            if self.managed_container_running(
                target.id,
                _fork_setup_budget=child_setup_budget,
            ):
                ready = self.wait_fork_ready(
                    target,
                    checkpoint_id=checkpoint_id,
                    fork_nonce=fork_nonce,
                )
                commands.append(ready.argv)
                return SandboxForkRuntimeResult(
                    checkpoint_id=checkpoint_id,
                    commands=tuple(commands),
                )
            remove_result = self.delete(
                target.id,
                _fork_setup_budget=child_setup_budget,
            )
            commands.append(remove_result.argv)

        application_id = application_checkpoint_id(
            target.id,
            operation.generation,
            operation.spec_hash,
        )
        app_prepare_command = self._checkpoint_helper_command(
            "app-prepare", application_id
        )
        commands.append(app_prepare_command)
        try:
            self._run_fork_setup_required(
                app_prepare_command,
                budget=child_setup_budget,
                phase="prepare application checkpoint directory",
            )
        except Exception:
            cleanup_budget = _ForkSetupBudget.start(
                FORK_SETUP_CLEANUP_ALLOWANCE_SECONDS
            )
            app_drop_command = self._checkpoint_helper_command(
                "app-drop", application_id
            )
            commands.append(app_drop_command)
            self._run_fork_setup_best_effort(
                app_drop_command,
                budget=cleanup_budget,
                phase="drop failed application checkpoint directory",
            )
            raise

        create_command = self.create_stopped_command(
            target,
            operation,
            image=source_image_id,
            extra_env={
                "UCLOUD_SANDBOX_FORK_PARENT": source.id,
                "UCLOUD_SANDBOX_FORK_PARENT_GENERATION": str(source_generation),
                "UCLOUD_SANDBOX_CHECKPOINT_ID": checkpoint_id,
                "UCLOUD_SANDBOX_FORK_NONCE": fork_nonce,
            },
            restore_checkpoint=checkpoint_name,
        )
        commands.append(create_command)
        try:
            self._run_fork_setup_required(
                create_command,
                budget=child_setup_budget,
                phase="create destination container",
            )
        except SandboxForkCommandTimeoutError:
            # Docker may have created the destination after its client timed
            # out. Preserve both the container and its application directory
            # for identity-based replay.
            raise
        except Exception:
            cleanup_budget = _ForkSetupBudget.start(
                FORK_SETUP_CLEANUP_ALLOWANCE_SECONDS
            )
            remove_command = (
                self.docker_binary,
                "rm",
                "-f",
                self.container_name(target.id),
            )
            commands.append(remove_command)
            self._run_fork_setup_best_effort(
                remove_command,
                budget=cleanup_budget,
                phase="remove failed destination container",
            )
            app_drop_command = self._checkpoint_helper_command(
                "app-drop", application_id
            )
            commands.append(app_drop_command)
            self._run_fork_setup_best_effort(
                app_drop_command,
                budget=cleanup_budget,
                phase="drop failed application checkpoint directory",
            )
            raise
        target_container_id, _target_image_id = self._container_runtime_info(
            target.id,
            _fork_setup_budget=child_setup_budget,
        )
        stage_command = self._checkpoint_helper_command(
            "stage",
            checkpoint_id,
            target_container_id,
            checkpoint_name,
        )
        commands.append(stage_command)

        def cleanup_unstarted_destination() -> None:
            cleanup_budget = _ForkSetupBudget.start(
                FORK_SETUP_CLEANUP_ALLOWANCE_SECONDS
            )
            unstage_command = self._checkpoint_helper_command(
                "unstage", target_container_id, checkpoint_name
            )
            commands.append(unstage_command)
            self._run_fork_setup_best_effort(
                unstage_command,
                budget=cleanup_budget,
                phase="unstage failed destination checkpoint",
            )
            remove_command = (
                self.docker_binary,
                "rm",
                "-f",
                self.container_name(target.id),
            )
            commands.append(remove_command)
            self._run_fork_setup_best_effort(
                remove_command,
                budget=cleanup_budget,
                phase="remove failed destination container",
            )
            app_drop_command = self._checkpoint_helper_command(
                "app-drop", application_id
            )
            commands.append(app_drop_command)
            self._run_fork_setup_best_effort(
                app_drop_command,
                budget=cleanup_budget,
                phase="drop failed application checkpoint directory",
            )

        try:
            self._run_fork_setup_required(
                stage_command,
                budget=child_setup_budget,
                phase="stage destination checkpoint",
            )
        except SandboxForkCommandTimeoutError:
            # The local helper and its copy process were killed before Docker
            # restore started, so staged state can be removed deterministically.
            cleanup_unstarted_destination()
            raise
        except Exception:
            cleanup_unstarted_destination()
            raise

        start_command = (
            self.docker_binary,
            "start",
            self.container_name(target.id),
        )
        commands.append(start_command)
        try:
            self._run_fork_command(start_command, phase="restore")
        except SandboxForkCommandTimeoutError:
            # The daemon may complete after its client times out. Preserve the
            # container and staged checkpoint so replay can inspect exact state.
            raise
        except Exception:
            cleanup_unstarted_destination()
            raise

        unstage_command = self._checkpoint_helper_command(
            "unstage", target_container_id, checkpoint_name
        )
        commands.append(unstage_command)
        self._run_fork_setup_best_effort(
            unstage_command,
            budget=child_setup_budget,
            phase="unstage restored checkpoint",
        )
        ready = self.wait_fork_ready(
            target,
            checkpoint_id=checkpoint_id,
            fork_nonce=fork_nonce,
        )
        commands.append(ready.argv)
        return SandboxForkRuntimeResult(
            checkpoint_id=checkpoint_id,
            commands=tuple(commands),
        )

    def fork_many(
        self,
        source: SandboxSpec,
        targets: Sequence[tuple[SandboxSpec, SandboxOperation]],
        *,
        source_generation: int,
        source_spec_hash: str,
        source_operation_id: str = "",
        checkpoint_id: str,
        fork_nonce: str,
        require_existing_checkpoint: bool = False,
    ) -> tuple[SandboxForkRuntimeResult, ...]:
        """Restore multiple children from one immutable source checkpoint.

        Capture and source resume complete once before any destination starts.
        Child restores then run with bounded parallelism and are collected in
        request order. If one fails, no more queued children start; already
        running workers finish into their durable ``restoring`` intents so an
        exact retry can adopt them. If a caller has already observed a restored
        sibling, ``require_existing`` fences recovery against silently taking
        a newer source checkpoint.
        """

        requested_targets = tuple(targets)
        if not requested_targets:
            raise ValueError("fork fan-out requires at least one target")
        if len(requested_targets) > MAX_FORK_FANOUT:
            raise ValueError(f"fork fan-out cannot exceed {MAX_FORK_FANOUT} sandboxes")
        target_ids = [target.id for target, _operation in requested_targets]
        if len(set(target_ids)) != len(target_ids):
            raise ValueError("fork fan-out target ids must be unique")
        for target, _operation in requested_targets:
            validate_fork_compatibility(source, target)

        capture_target, capture_operation = requested_targets[0]
        capture_result = self.fork(
            source,
            capture_target,
            capture_operation,
            source_generation=source_generation,
            source_spec_hash=source_spec_hash,
            source_operation_id=source_operation_id,
            checkpoint_id=checkpoint_id,
            fork_nonce=fork_nonce,
            _checkpoint_prepared=False,
            _require_existing_checkpoint=require_existing_checkpoint,
            _source_fenced=False,
            _capture_only=True,
        )

        def restore_one(index: int) -> SandboxForkRuntimeResult:
            target, operation = requested_targets[index]
            return self.fork(
                source,
                target,
                operation,
                source_generation=source_generation,
                source_spec_hash=source_spec_hash,
                source_operation_id=source_operation_id,
                checkpoint_id=checkpoint_id,
                fork_nonce=fork_nonce,
                _checkpoint_prepared=True,
                _require_existing_checkpoint=False,
                # Re-inspect on every child. Docker state can change outside
                # this process even while the in-process lifecycle lease is held.
                _source_fenced=False,
            )

        worker_count = min(self.fork_restore_parallelism, len(requested_targets))
        result_slots: list[SandboxForkRuntimeResult | None] = [
            None for _target in requested_targets
        ]
        failures: list[tuple[int, Exception]] = []
        next_index = 0
        with ThreadPoolExecutor(
            max_workers=worker_count,
            thread_name_prefix="sandbox-fork-restore",
        ) as pool:
            in_flight: dict[Future[SandboxForkRuntimeResult], int] = {}

            def submit(index: int) -> None:
                in_flight[pool.submit(restore_one, index)] = index

            while next_index < worker_count:
                submit(next_index)
                next_index += 1

            while in_flight:
                completed, _pending = wait(
                    tuple(in_flight),
                    return_when=FIRST_COMPLETED,
                )
                for future in sorted(completed, key=lambda item: in_flight[item]):
                    index = in_flight.pop(future)
                    try:
                        result_slots[index] = future.result()
                    except Exception as exc:
                        failures.append((index, exc))

                # Do not start another destination after any observed failure.
                # The at-most-N workers already in progress are allowed to
                # finish, preserving inspectable runtime state for exact replay.
                while (
                    not failures
                    and next_index < len(requested_targets)
                    and len(in_flight) < worker_count
                ):
                    submit(next_index)
                    next_index += 1

        if failures:
            _index, failure = min(failures, key=lambda item: item[0])
            raise failure

        if any(result is None for result in result_slots):
            raise RuntimeError("fork fan-out produced incomplete runtime results")
        ordered_results = tuple(result for result in result_slots if result is not None)
        first = ordered_results[0]
        return (
            replace(
                first,
                commands=capture_result.commands + first.commands,
            ),
            *ordered_results[1:],
        )

    def fork_protocol_command(
        self,
        spec: SandboxSpec,
        command: tuple[str, ...],
        *,
        checkpoint_id: str,
        fork_nonce: str,
        role: str,
    ) -> tuple[str, ...]:
        validate_checkpoint_id(checkpoint_id)
        validate_fork_nonce(fork_nonce)
        if role not in {"prepare", "resume", "restore", "cancel"}:
            raise ValueError("invalid fork protocol role")
        spec.fork_protocol.validate(required=True)
        return (
            self.docker_binary,
            "exec",
            self.container_name(spec.id),
            *command,
            checkpoint_id,
            fork_nonce,
            role,
        )

    def wait_fork_ready(
        self,
        spec: SandboxSpec,
        *,
        checkpoint_id: str,
        fork_nonce: str,
    ) -> CommandResult:
        command = self.fork_protocol_command(
            spec,
            spec.fork_protocol.ready_command,
            checkpoint_id=checkpoint_id,
            fork_nonce=fork_nonce,
            role="restore",
        )
        return self._run_fork_protocol_hook(
            command,
            sandbox_id=spec.id,
            role="restore",
            fork_nonce=fork_nonce,
            timeout_seconds=spec.fork_protocol.timeout_seconds,
        )

    def _run_fork_protocol_hook(
        self,
        command: tuple[str, ...],
        *,
        sandbox_id: str,
        role: str,
        fork_nonce: str,
        timeout_seconds: int,
    ) -> CommandResult:
        if self.dry_run:
            return CommandResult(argv=command, exit_code=0)
        run_with_timeout = getattr(self.executor, "run_with_timeout", None)
        if callable(run_with_timeout):
            result = run_with_timeout(
                command,
                timeout_seconds=float(timeout_seconds),
            )
        else:
            # Test/custom executors may not implement the optional deadline
            # method. Production uses SubprocessExecutor, which always does.
            result = self.executor.run(command)
        if result.exit_code != 0:
            if result.exit_code in {124, 125}:
                # Killing the local ``docker exec`` client does not prove the
                # daemon-side exec has stopped. A deadline or bounded-output
                # kill is therefore an ambiguous lifecycle transition:
                # callers retain the restoring intent and checkpoint artifact,
                # quarantining both source and target until reconciliation.
                raise SandboxForkCommandTimeoutError(
                    f"fork protocol {role} hook did not complete within its "
                    f"{timeout_seconds}s/output bound for sandbox {sandbox_id}; "
                    "workload state is ambiguous"
                )
            detail = "timed out" if result.exit_code == 124 else "was rejected"
            raise RuntimeError(
                f"fork protocol {role} hook {detail} for sandbox {sandbox_id}"
            )
        prefix = "UCLOUD_FORK_PREPARED=" if role == "prepare" else "UCLOUD_FORK_READY="
        expected = (
            f"{prefix}{fork_nonce}"
            if role == "prepare"
            else f"{prefix}{fork_nonce}:{role}"
        )
        if expected not in {line.strip() for line in result.stdout.splitlines()}:
            raise RuntimeError(
                f"fork protocol {role} hook returned no nonce acknowledgment "
                f"for sandbox {sandbox_id}"
            )
        return result

    def release_checkpoint(
        self,
        checkpoint_id: str,
        *,
        _fork_setup_budget: _ForkSetupBudget | None = None,
    ) -> CommandResult:
        validate_checkpoint_id(checkpoint_id)
        command = self._checkpoint_helper_command("drop", checkpoint_id)
        if _fork_setup_budget is not None:
            return self._run_fork_setup_required(
                command,
                budget=_fork_setup_budget,
                phase="release checkpoint artifact",
            )
        return self._run(command)

    def checkpoint_artifact_state(self, checkpoint_id: str) -> str:
        """Return ``sealed``, ``pending``, or ``absent`` without mutation."""

        validate_checkpoint_id(checkpoint_id)
        if self.dry_run:
            return "sealed"
        result = self.executor.run(
            self._checkpoint_helper_command("status", checkpoint_id)
        )
        if result.exit_code == 0:
            return "sealed"
        if result.exit_code == 3:
            return "pending"
        if result.exit_code == 4:
            return "absent"
        raise RuntimeError(
            f"checkpoint helper status failed with exit code {result.exit_code}: "
            f"{result.stderr}"
        )

    def checkpoint_helper_inventory(self) -> dict[str, Any]:
        """Return the helper's validated artifact/application/staging inventory."""

        if self.checkpoint_root is None:
            return {"version": 1, "artifacts": [], "applications": [], "staged": []}
        if self.dry_run:
            return {"version": 1, "artifacts": [], "applications": [], "staged": []}
        command = self._checkpoint_helper_command("list")
        run_bounded = getattr(self.executor, "run_bounded_stdout", None)
        if callable(run_bounded):
            result = run_bounded(
                command,
                max_stdout_bytes=MAX_CHECKPOINT_HELPER_INVENTORY_BYTES,
            )
        else:
            result = self.executor.run(command)
        if result.exit_code != 0:
            raise RuntimeError(
                "checkpoint helper inventory failed with exit code "
                f"{result.exit_code}"
            )
        if len(result.stdout_bytes or result.stdout.encode("utf-8")) > (
            MAX_CHECKPOINT_HELPER_INVENTORY_BYTES
        ):
            raise RuntimeError("checkpoint helper inventory exceeded its size limit")
        try:
            inventory = json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                "checkpoint helper returned invalid inventory JSON"
            ) from exc
        if (
            not isinstance(inventory, dict)
            or inventory.get("version") != 1
            or not isinstance(inventory.get("artifacts"), list)
            or not isinstance(inventory.get("applications"), list)
            or not isinstance(inventory.get("staged"), list)
        ):
            raise RuntimeError("checkpoint helper returned an invalid inventory schema")
        return inventory

    def cleanup_staged_checkpoint(
        self,
        target_container_id: str,
        checkpoint_id: str,
    ) -> CommandResult:
        if re.fullmatch(r"[0-9a-f]{64}", target_container_id) is None:
            raise ValueError("target container id must be a full lowercase Docker id")
        validate_checkpoint_id(checkpoint_id)
        return self._run(
            self._checkpoint_helper_command(
                "unstage", target_container_id, checkpoint_id
            )
        )

    def drop_application_checkpoint_id(self, application_id: str) -> CommandResult:
        if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", application_id) is None:
            raise ValueError("invalid application checkpoint id")
        return self._run(self._checkpoint_helper_command("app-drop", application_id))

    def cleanup_restored_checkpoint(
        self,
        sandbox_id: str,
        *,
        _fork_setup_budget: _ForkSetupBudget | None = None,
    ) -> CommandResult:
        container_id, _image_id = self._container_runtime_info(
            sandbox_id,
            _fork_setup_budget=_fork_setup_budget,
        )
        command = self._checkpoint_helper_command("unstage", container_id, "state")
        if _fork_setup_budget is not None:
            return self._run_fork_setup_required(
                command,
                budget=_fork_setup_budget,
                phase="clean restored checkpoint",
            )
        return self._run(command)

    def managed_container_running(
        self,
        sandbox_id: str,
        *,
        _fork_setup_budget: _ForkSetupBudget | None = None,
    ) -> bool:
        if not SANDBOX_ID_RE.match(sandbox_id):
            raise ValueError("invalid sandbox id.")
        if self.dry_run:
            return False
        argv = (
            self.docker_binary,
            "inspect",
            "--format",
            "{{.State.Running}}",
            self.container_name(sandbox_id),
        )
        result = (
            self._run_fork_setup_command(
                argv,
                budget=_fork_setup_budget,
                phase="inspect container state",
            )
            if _fork_setup_budget is not None
            else self.executor.run(argv)
        )
        if result.exit_code != 0:
            if self._is_container_not_found(result):
                return False
            raise RuntimeError(
                f"command failed with exit code {result.exit_code}: {' '.join(argv)}\n"
                f"{result.stderr}"
            )
        return result.stdout.strip().lower() == "true"

    def _container_runtime_info(
        self,
        sandbox_id: str,
        *,
        _fork_setup_budget: _ForkSetupBudget | None = None,
    ) -> tuple[str, str]:
        if not SANDBOX_ID_RE.match(sandbox_id):
            raise ValueError("invalid sandbox id.")
        if self.dry_run:
            return ("0" * 64, "sha256:" + "0" * 64)
        argv = (
            self.docker_binary,
            "inspect",
            "--format",
            "{{.Id}} {{.Image}}",
            self.container_name(sandbox_id),
        )
        result = (
            self._run_fork_setup_command(
                argv,
                budget=_fork_setup_budget,
                phase="inspect container identity",
            )
            if _fork_setup_budget is not None
            else self.executor.run(argv)
        )
        if result.exit_code != 0:
            raise RuntimeError(
                f"command failed with exit code {result.exit_code}: {' '.join(argv)}\n"
                f"{result.stderr}"
            )
        parts = result.stdout.strip().split()
        if (
            len(parts) != 2
            or re.fullmatch(r"[0-9a-f]{64}", parts[0]) is None
            or re.fullmatch(r"sha256:[0-9a-f]{64}", parts[1]) is None
        ):
            raise RuntimeError("docker inspect returned invalid container/image IDs")
        return parts[0], parts[1]

    def _checkpoint_helper_command(self, *args: str) -> tuple[str, ...]:
        prefix = ("sudo", "-n") if self.checkpoint_helper_sudo else ()
        return prefix + (self.checkpoint_helper, *args)

    def _run_best_effort(self, argv: tuple[str, ...]) -> CommandResult:
        if self.dry_run:
            return CommandResult(argv=argv, exit_code=0)
        return self.executor.run(argv)

    def _run_fork_setup_command(
        self,
        argv: tuple[str, ...],
        *,
        budget: _ForkSetupBudget,
        phase: str,
    ) -> CommandResult:
        """Run one fork-only setup command against a cumulative deadline."""

        if self.dry_run:
            return CommandResult(argv=argv, exit_code=0)
        if budget.remaining_seconds <= 0:
            raise SandboxForkCommandTimeoutError(
                f"sandbox fork {phase} exhausted its "
                f"{budget.limit_seconds:g}s setup budget; runtime state is ambiguous"
            )
        started = time.monotonic()
        run_with_timeout = getattr(self.executor, "run_with_timeout", None)
        if callable(run_with_timeout):
            result = run_with_timeout(
                argv,
                timeout_seconds=budget.remaining_seconds,
            )
        else:
            # Production uses SubprocessExecutor. Narrow injected executors may
            # omit deadline support, but still participate in elapsed accounting.
            result = self.executor.run(argv)
        budget.remaining_seconds = max(
            0.0,
            budget.remaining_seconds - (time.monotonic() - started),
        )
        if result.exit_code in {124, 125}:
            raise SandboxForkCommandTimeoutError(
                f"sandbox fork {phase} exhausted its "
                f"{budget.limit_seconds:g}s/output setup budget; runtime state is "
                "ambiguous"
            )
        return result

    def _run_fork_setup_required(
        self,
        argv: tuple[str, ...],
        *,
        budget: _ForkSetupBudget,
        phase: str,
    ) -> CommandResult:
        result = self._run_fork_setup_command(
            argv,
            budget=budget,
            phase=phase,
        )
        if result.exit_code != 0:
            raise SandboxRuntimeCommandError(result)
        return result

    def _run_fork_setup_best_effort(
        self,
        argv: tuple[str, ...],
        *,
        budget: _ForkSetupBudget,
        phase: str,
    ) -> CommandResult:
        try:
            return self._run_fork_setup_command(
                argv,
                budget=budget,
                phase=phase,
            )
        except SandboxForkCommandTimeoutError as exc:
            return CommandResult(argv=argv, exit_code=124, stderr=str(exc))

    def _run_fork_command(
        self,
        argv: tuple[str, ...],
        *,
        phase: str,
    ) -> CommandResult:
        if self.dry_run:
            return CommandResult(argv=argv, exit_code=0)
        if phase == "checkpoint":
            timeout_seconds = self.fork_command_timeout_seconds
        elif phase == "restore":
            timeout_seconds = self.fork_restore_timeout_seconds
        else:
            raise ValueError(f"unsupported sandbox fork command phase: {phase}")
        run_with_timeout = getattr(self.executor, "run_with_timeout", None)
        if callable(run_with_timeout):
            result = run_with_timeout(
                argv,
                timeout_seconds=float(timeout_seconds),
            )
        else:
            # Production uses SubprocessExecutor. The fallback preserves
            # compatibility for narrow test/custom executors.
            result = self.executor.run(argv)
        if result.exit_code in {124, 125}:
            raise SandboxForkCommandTimeoutError(
                f"sandbox fork {phase} exceeded its {timeout_seconds}s/output "
                "bound; runtime state is ambiguous"
            )
        if result.exit_code != 0:
            raise RuntimeError(
                f"sandbox fork {phase} failed with exit code {result.exit_code}"
            )
        return result

    def _run(
        self, argv: tuple[str, ...], *, input: bytes | None = None
    ) -> CommandResult:
        if self.dry_run:
            return CommandResult(argv=argv, exit_code=0)
        result = self.executor.run(argv, input=input)
        if result.exit_code != 0:
            raise SandboxRuntimeCommandError(result)
        return result

    def is_container_name_conflict(self, error: BaseException) -> bool:
        detail = (
            error.result.stderr if isinstance(error, SandboxRuntimeCommandError) else ""
        )
        message = f"{error} {detail}".lower()
        return (
            "conflict" in message
            and "container name" in message
            and "already in use" in message
        )

    @staticmethod
    def _is_container_not_found(result: CommandResult) -> bool:
        message = f"{result.stdout}\n{result.stderr}".lower()
        return (
            "no such container" in message
            or "container not found" in message
            or "no such object" in message
        )

    def managed_container_matches(
        self,
        spec: SandboxSpec,
        operation: SandboxOperation | None = None,
    ) -> bool:
        operation = operation or SandboxOperation.legacy_create(spec)
        labels = self._container_labels(spec.id)
        if labels.get("ucloud-sandboxes.managed") != "true":
            return False
        if labels.get("ucloud-sandboxes.sandbox-id") != spec.id:
            return False
        existing_fingerprint = labels.get(SANDBOX_SPEC_HASH_LABEL)
        fingerprint_matches = existing_fingerprint == operation.spec_hash
        if operation.generation == 0 and not operation.operation_id:
            fingerprint_matches = fingerprint_matches or (
                existing_fingerprint in sandbox_spec_fingerprints(spec)
            )
        if existing_fingerprint and not fingerprint_matches:
            return False
        raw_generation = labels.get(SANDBOX_GENERATION_LABEL)
        try:
            existing_generation = int(raw_generation or 0)
        except ValueError:
            return False
        if existing_generation != operation.generation:
            return False
        existing_operation_id = labels.get(SANDBOX_OPERATION_ID_LABEL, "")
        if existing_operation_id != operation.operation_id:
            return False
        return bool(existing_fingerprint) or operation.generation == 0

    def _container_labels(self, sandbox_id: str) -> dict[str, str]:
        if not SANDBOX_ID_RE.match(sandbox_id):
            raise ValueError("invalid sandbox id.")
        result = self.executor.run(
            (
                self.docker_binary,
                "inspect",
                "--format",
                "{{json .Config.Labels}}",
                self.container_name(sandbox_id),
            )
        )
        if result.exit_code != 0:
            return {}
        try:
            raw = json.loads(result.stdout or "{}")
        except json.JSONDecodeError:
            return {}
        if not isinstance(raw, dict):
            return {}
        return {str(key): str(value) for key, value in raw.items()}

    def managed_container_identity(
        self,
        sandbox_id: str,
        *,
        _fork_setup_budget: _ForkSetupBudget | None = None,
    ) -> tuple[int, str, str] | None:
        """Inspect a runtime-only sandbox without mutating another generation."""
        if not SANDBOX_ID_RE.match(sandbox_id):
            raise ValueError("invalid sandbox id.")
        if self.dry_run:
            return None
        argv = (
            self.docker_binary,
            "inspect",
            "--format",
            "{{json .Config.Labels}}",
            self.container_name(sandbox_id),
        )
        result = (
            self._run_fork_setup_command(
                argv,
                budget=_fork_setup_budget,
                phase="inspect managed container labels",
            )
            if _fork_setup_budget is not None
            else self.executor.run(argv)
        )
        if result.exit_code != 0:
            if self._is_container_not_found(result):
                return None
            raise RuntimeError(
                f"command failed with exit code {result.exit_code}: {' '.join(argv)}\n"
                f"{result.stderr}"
            )
        try:
            raw = json.loads(result.stdout or "{}")
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                "docker inspect returned invalid container labels"
            ) from exc
        if not isinstance(raw, dict):
            raise RuntimeError("docker inspect returned invalid container labels")
        labels = {str(key): str(value) for key, value in raw.items()}
        if labels.get("ucloud-sandboxes.managed") != "true":
            raise SandboxConflictError(
                f"runtime container is not managed by ucloud-sandboxes: {sandbox_id}"
            )
        try:
            generation = int(labels.get(SANDBOX_GENERATION_LABEL) or 0)
        except ValueError as exc:
            raise RuntimeError(
                "runtime container has an invalid generation label"
            ) from exc
        if generation < 0:
            raise RuntimeError("runtime container has a negative generation label")
        return (
            generation,
            labels.get(SANDBOX_OPERATION_ID_LABEL, ""),
            labels.get(SANDBOX_SPEC_HASH_LABEL, ""),
        )


class SandboxStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = _sandbox_lock(path)

    def load(self) -> dict[str, SandboxRecord]:
        records, _revision = self.load_with_revision()
        return records

    def load_with_revision(self) -> tuple[dict[str, SandboxRecord], int]:
        state = self.load_state()
        return state.records, state.revision

    def load_state(self) -> SandboxStoreState:
        with self._lock.hold(exclusive=False):
            return self._load_unlocked()

    def save(self, records: dict[str, SandboxRecord]) -> int:
        with self._lock.hold(exclusive=True):
            state = self._load_unlocked()
            return self._save_unlocked(
                records,
                state.tombstones,
                state.drain,
                revision=state.revision + 1,
            )

    def save_state(
        self,
        records: dict[str, SandboxRecord],
        tombstones: dict[str, SandboxTombstone],
        drain: NodeDrainState | None = None,
    ) -> int:
        with self._lock.hold(exclusive=True):
            state = self._load_unlocked()
            return self._save_unlocked(
                records,
                tombstones,
                drain or state.drain,
                revision=state.revision + 1,
            )

    def upsert(self, record: SandboxRecord) -> dict[str, SandboxRecord]:
        with self._lock.hold(exclusive=True):
            state = self._load_unlocked()
            records = state.records
            records[record.spec.id] = record
            self._save_unlocked(
                records,
                state.tombstones,
                state.drain,
                revision=state.revision + 1,
            )
            return records

    def delete(self, sandbox_id: str) -> SandboxRecord | None:
        with self._lock.hold(exclusive=True):
            state = self._load_unlocked()
            records = state.records
            record = records.pop(sandbox_id, None)
            self._save_unlocked(
                records,
                state.tombstones,
                state.drain,
                revision=state.revision + 1,
            )
            return record

    def _load_unlocked(self) -> SandboxStoreState:
        if not self.path.exists():
            return SandboxStoreState(records={}, tombstones={}, revision=0)
        raw = json.loads(self.path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise ValueError("sandbox store must contain a JSON object.")
        try:
            revision = int(raw.get("revision", 0))
        except (TypeError, ValueError) as exc:
            raise ValueError("sandbox store revision must be an integer.") from exc
        if revision < 0:
            raise ValueError("sandbox store revision cannot be negative.")
        items = raw.get("sandboxes", [])
        if not isinstance(items, list):
            raise ValueError("sandbox store must contain a sandboxes list.")
        records: dict[str, SandboxRecord] = {}
        for item in items:
            if not isinstance(item, dict):
                continue
            record = SandboxRecord.from_dict(item)
            records[record.spec.id] = record
        tombstone_items = raw.get("tombstones", [])
        if not isinstance(tombstone_items, list):
            raise ValueError("sandbox store must contain a tombstones list.")
        tombstones: dict[str, SandboxTombstone] = {}
        for item in tombstone_items:
            tombstone = SandboxTombstone.from_dict(item)
            if tombstone is None:
                continue
            previous = tombstones.get(tombstone.sandbox_id)
            if previous is None or tombstone.generation > previous.generation:
                tombstones[tombstone.sandbox_id] = tombstone
        return SandboxStoreState(
            records=records,
            tombstones=tombstones,
            revision=revision,
            drain=NodeDrainState.from_dict(raw.get("drain")),
        )

    def _save_unlocked(
        self,
        records: dict[str, SandboxRecord],
        tombstones: dict[str, SandboxTombstone],
        drain: NodeDrainState,
        *,
        revision: int,
    ) -> int:
        payload = {
            "revision": revision,
            "sandboxes": [
                records[sandbox_id].to_dict() for sandbox_id in sorted(records)
            ],
            "tombstones": [
                tombstones[sandbox_id].to_dict() for sandbox_id in sorted(tombstones)
            ],
            "drain": drain.to_dict(),
        }
        _atomic_write_json(self.path, payload)
        return revision


def _sandbox_lock(path: Path) -> _AdvisoryFileLock:
    key = path.resolve()
    with _SANDBOX_LOCKS_GUARD:
        lock = _SANDBOX_LOCKS.get(key)
        if lock is None:
            lock = _AdvisoryFileLock(key)
            _SANDBOX_LOCKS[key] = lock
        return lock


def _sandbox_create_lock(path: Path, sandbox_id: str) -> ContextManager[None]:
    # SSH ports and the backing JSON store are shared by every sandbox.  A
    # per-sandbox lock allowed concurrent creates to reserve the same port.
    # Reuse the path-wide re-entrant store lock so allocation, runtime create,
    # and persistence are one in-process critical section.
    del sandbox_id
    return _sandbox_lock(path).hold(exclusive=True)


def _sandbox_fork_runtime_lock(path: Path) -> ContextManager[None]:
    """Serialize checkpoint artifacts across threads and node-agent processes."""

    coordination_path = path.with_name(f"{path.name}.fork-runtime")
    return _sandbox_lock(coordination_path).hold(exclusive=True)


class SandboxManager:
    def __init__(
        self,
        store: SandboxStore,
        runtime: DockerGvisorRuntime,
        *,
        ssh_port_range: tuple[int, int] | None = None,
        effective_capacity: ResourceQuantity | None = None,
        lifecycle: SandboxLifecycleCoordinator | None = None,
    ) -> None:
        if effective_capacity is not None and not effective_capacity.is_valid:
            raise ValueError(
                "effective_capacity cannot contain negative or non-finite values"
            )
        self.store = store
        self.runtime = runtime
        self.ssh_port_range = ssh_port_range
        self.effective_capacity = effective_capacity
        self.lifecycle = lifecycle or SandboxLifecycleCoordinator()

    def reconcile_checkpoint_storage(self) -> dict[str, int]:
        """Collect only helper state proven unreferenced by durable intents.

        Pending artifacts are never guessed safe: an unreferenced pending save
        may belong to dockerd after a state-store loss, so startup fails closed
        and leaves it for operator reconciliation. Sealed artifacts, staged
        reflinks, and application directories can be removed once no durable
        record needs them.
        """

        counters = {
            "sealed_removed": 0,
            "staged_removed": 0,
            "applications_removed": 0,
            "pending_retained": 0,
        }
        if self.runtime.checkpoint_root is None or self.runtime.dry_run:
            return counters

        with _sandbox_fork_runtime_lock(self.store.path):
            state = self.store.load_state()
            active_artifacts = {
                record.checkpoint_id
                for record in state.records.values()
                if record.state == "restoring" and record.checkpoint_id
            }
            active_applications: set[str] = set()
            for record in state.records.values():
                if not record.spec.forkable:
                    continue
                try:
                    active_applications.add(
                        application_checkpoint_id(
                            record.spec.id,
                            record.generation,
                            record.spec_hash,
                        )
                    )
                except ValueError as exc:
                    raise RuntimeError(
                        "forkable sandbox record has invalid application identity: "
                        f"{record.spec.id}"
                    ) from exc

            inventory = self.runtime.checkpoint_helper_inventory()
            raw_artifacts = inventory["artifacts"]
            raw_staged = inventory["staged"]
            raw_applications = inventory["applications"]

            orphan_pending: list[str] = []
            for raw in raw_artifacts:
                if not isinstance(raw, dict):
                    raise RuntimeError(
                        "checkpoint inventory contains an invalid artifact"
                    )
                artifact_id = str(raw.get("artifact_id") or "")
                artifact_state = str(raw.get("state") or "")
                validate_checkpoint_id(artifact_id)
                if artifact_state not in {"pending", "sealed"}:
                    raise RuntimeError(
                        f"checkpoint inventory has invalid state for {artifact_id}"
                    )
                if artifact_state == "pending" and artifact_id not in active_artifacts:
                    orphan_pending.append(artifact_id)

            counters["pending_retained"] = len(orphan_pending)
            if orphan_pending:
                # An orphan pending save can still be owned by dockerd/runsc.
                # Do not mutate *any* helper state until an operator has
                # established that no writer or restored task can reference it.
                raise RuntimeError(
                    "unreferenced pending checkpoint artifacts require operator "
                    "reconciliation before the node can serve requests: "
                    + ", ".join(sorted(orphan_pending))
                )

            for raw in raw_staged:
                if not isinstance(raw, dict):
                    raise RuntimeError(
                        "checkpoint inventory contains invalid staged data"
                    )
                artifact_id = str(raw.get("artifact_id") or "")
                if artifact_id in active_artifacts:
                    continue
                self.runtime.cleanup_staged_checkpoint(
                    str(raw.get("target_container_id") or ""),
                    str(raw.get("checkpoint_id") or ""),
                )
                counters["staged_removed"] += 1

            for raw in raw_artifacts:
                assert isinstance(raw, dict)
                artifact_id = str(raw.get("artifact_id") or "")
                if (
                    str(raw.get("state") or "") == "sealed"
                    and artifact_id not in active_artifacts
                ):
                    self.runtime.release_checkpoint(artifact_id)
                    counters["sealed_removed"] += 1

            for raw in raw_applications:
                if not isinstance(raw, str):
                    raise RuntimeError(
                        "checkpoint inventory contains an invalid application id"
                    )
                if raw not in active_applications:
                    self.runtime.drop_application_checkpoint_id(raw)
                    counters["applications_removed"] += 1
        return counters

    def list(self) -> list[SandboxRecord]:
        return list(self.activity_snapshot().records)

    def get(self, sandbox_id: str) -> SandboxRecord | None:
        return next(
            (
                record
                for record in self.activity_snapshot().records
                if record.spec.id == sandbox_id
            ),
            None,
        )

    def require_activity_sandbox(self, sandbox_id: str) -> SandboxRecord:
        """Return a sandbox only when exec/file activity is safe.

        A failed or timed-out fork leaves its destination intent in
        ``restoring``. That durable intent quarantines both the destination and
        source after the in-process lifecycle lease is released, including
        across node-agent restarts.
        """

        with _sandbox_create_lock(self.store.path, sandbox_id):
            state = self.store.load_state()
            record = state.records.get(sandbox_id)
            if record is None:
                raise ValueError(f"sandbox not found: {sandbox_id}")
            if not self.runtime.dry_run and record.state != "running":
                raise SandboxBusyError(
                    f"sandbox is not ready for activity: {sandbox_id} "
                    f"({record.state})"
                )
            if any(
                candidate.state == "restoring"
                and candidate.source_sandbox_id == sandbox_id
                for candidate in state.records.values()
            ):
                raise SandboxBusyError(
                    f"sandbox has an unfinished fork restore: {sandbox_id}"
                )
            return record

    def create(
        self,
        spec: SandboxSpec,
        *,
        operation: SandboxOperation | None = None,
    ) -> tuple[SandboxRecord, CommandResult]:
        record, result, _timings = self.create_with_timings(
            spec,
            operation=operation,
        )
        return record, result

    def create_with_timings(
        self,
        spec: SandboxSpec,
        *,
        operation: SandboxOperation | None = None,
    ) -> tuple[SandboxRecord, CommandResult, dict[str, Any]]:
        started = time.monotonic()
        phases: dict[str, int] = {}
        spec.validate()
        if spec.forkable and not self.runtime.fork_enabled:
            raise SandboxForkUnsupportedError(
                "forkable sandbox requires a node with fork-local-v1"
            )
        operation = operation or SandboxOperation.legacy_create(spec)
        operation.validate_spec(spec)
        with _sandbox_create_lock(self.store.path, spec.id):
            phase = time.monotonic()
            self.cleanup_expired()
            phases["cleanup_expired_ms"] = _elapsed_ms(phase)
            phase = time.monotonic()
            state = self.store.load_state()
            records = state.records
            phases["load_store_ms"] = _elapsed_ms(phase)
            tombstone = state.tombstones.get(spec.id)
            # This deliberately includes generation zero: a legacy create has
            # no identity capable of distinguishing an intentional reuse from
            # a delayed request, so a legacy tombstone permanently fences that
            # ID until callers adopt a higher versioned generation.
            if tombstone is not None and operation.generation <= tombstone.generation:
                raise SandboxStaleOperationError(
                    f"sandbox create generation {operation.generation} is fenced by "
                    f"tombstone generation {tombstone.generation}: {spec.id}"
                )
            existing = records.get(spec.id)
            replaying_planned = False
            if existing is not None:
                if operation.generation < existing.generation:
                    raise SandboxStaleOperationError(
                        f"sandbox create generation {operation.generation} is older than "
                        f"live generation {existing.generation}: {spec.id}"
                    )
                if operation.generation > existing.generation:
                    raise SandboxConflictError(
                        f"sandbox generation {existing.generation} must be deleted before "
                        f"creating generation {operation.generation}: {spec.id}"
                    )
                same_spec = existing.spec_hash == operation.spec_hash
                if existing.generation == 0 and not existing.operation_id:
                    same_spec = same_spec or sandbox_specs_match(existing.spec, spec)
                if existing.operation_id != operation.operation_id or not same_spec:
                    raise SandboxConflictError(
                        f"sandbox generation {operation.generation} already exists with "
                        f"a different operation or spec: {spec.id}"
                    )
                phases["idempotency_check_ms"] = _elapsed_ms(phase)
                if existing.state == "deleting":
                    raise SandboxConflictError(
                        f"sandbox generation {operation.generation} is being deleted: "
                        f"{spec.id}"
                    )
                if existing.state != "planned":
                    return (
                        existing,
                        CommandResult(argv=(), exit_code=0),
                        {
                            "total_ms": _elapsed_ms(started),
                            "phases": phases,
                            "idempotent": True,
                            "recovered": "store",
                        },
                    )
                # A planned record is a durable pre-runtime intent.  Replays
                # first reconcile it against the runtime so a process crash
                # after docker created the container cannot make the sandbox
                # invisible, and a crash before docker can safely resume.
                inspect_phase = time.monotonic()
                runtime_identity = self.runtime.managed_container_identity(spec.id)
                phases["planned_replay_inspect_ms"] = _elapsed_ms(inspect_phase)
                if runtime_identity is not None:
                    if not self._runtime_identity_matches(
                        runtime_identity,
                        operation=operation,
                        spec=existing.spec,
                    ):
                        raise SandboxConflictError(
                            f"runtime sandbox identity conflicts with planned "
                            f"generation {operation.generation}: {spec.id}"
                        )
                    record = replace(
                        existing,
                        state="running",
                        updated_at=utc_now(),
                    )
                    records[spec.id] = record
                    store_phase = time.monotonic()
                    self.store.save_state(records, state.tombstones)
                    phases["store_record_ms"] = _elapsed_ms(store_phase)
                    return (
                        record,
                        CommandResult(argv=(), exit_code=0),
                        {
                            "total_ms": _elapsed_ms(started),
                            "phases": phases,
                            "idempotent": True,
                            "recovered": "container",
                        },
                    )
                spec = existing.spec
                planned_record = existing
                replaying_planned = True
            else:
                if not state.drain.admission_open:
                    runtime_identity = self.runtime.managed_container_identity(spec.id)
                    expected_identity = (
                        operation.generation,
                        operation.operation_id,
                        operation.spec_hash,
                    )
                    if runtime_identity != expected_identity:
                        raise SandboxAdmissionClosedError(
                            f"sandbox create admission is closed while drain token "
                            f"{state.drain.token!r} is active"
                        )
                self._require_available_capacity(spec, records)
                phase = time.monotonic()
                spec = self._assign_ssh_port(spec, records)
                phases["assign_ssh_port_ms"] = _elapsed_ms(phase)
                phase = time.monotonic()
                spec.validate()
                phases["validate_spec_ms"] = _elapsed_ms(phase)
                now = utc_now()
                planned_record = SandboxRecord(
                    spec=spec,
                    container_name=self.runtime.container_name(spec.id),
                    state="planned",
                    created_at=now,
                    updated_at=now,
                    generation=operation.generation,
                    operation_id=operation.operation_id,
                    spec_hash=operation.spec_hash,
                )
                records[spec.id] = planned_record
                store_phase = time.monotonic()
                self.store.save_state(records, state.tombstones)
                phases["store_intent_ms"] = _elapsed_ms(store_phase)
            phase = time.monotonic()
            try:
                result = self.runtime.create_with_operation(spec, operation)
            except RuntimeError as exc:
                phases["docker_create_ms"] = _elapsed_ms(phase)
                if not self.runtime.is_container_name_conflict(exc):
                    raise
                inspect_phase = time.monotonic()
                if not self.runtime.managed_container_matches(
                    spec, operation=operation
                ):
                    phases["docker_conflict_inspect_ms"] = _elapsed_ms(inspect_phase)
                    raise SandboxConflictError(
                        f"sandbox already exists with different spec: {spec.id}"
                    ) from exc
                phases["docker_conflict_inspect_ms"] = _elapsed_ms(inspect_phase)
                record = replace(
                    planned_record,
                    state="running",
                    updated_at=utc_now(),
                )
                store_phase = time.monotonic()
                records[spec.id] = record
                self.store.save_state(records, state.tombstones)
                phases["store_record_ms"] = _elapsed_ms(store_phase)
                return (
                    record,
                    CommandResult(argv=(), exit_code=0),
                    {
                        "total_ms": _elapsed_ms(started),
                        "phases": phases,
                        "idempotent": True,
                        "recovered": "container",
                    },
                )
            phases["docker_create_ms"] = _elapsed_ms(phase)
            record = planned_record
            if not self.runtime.dry_run:
                phase = time.monotonic()
                record = replace(
                    planned_record,
                    state="running",
                    updated_at=utc_now(),
                )
                records[spec.id] = record
                self.store.save_state(records, state.tombstones)
                phases["store_record_ms"] = _elapsed_ms(phase)
            return (
                record,
                result,
                {
                    "total_ms": _elapsed_ms(started),
                    "phases": phases,
                    "idempotent": replaying_planned,
                },
            )

    def fork(
        self,
        source_sandbox_id: str,
        target: SandboxSpec,
        *,
        operation: SandboxOperation | None = None,
        source_generation: int | None = None,
        source_spec_hash: str | None = None,
    ) -> tuple[SandboxRecord, SandboxForkRuntimeResult]:
        record, result, _timings = self.fork_with_timings(
            source_sandbox_id,
            target,
            operation=operation,
            source_generation=source_generation,
            source_spec_hash=source_spec_hash,
        )
        return record, result

    def fork_with_timings(
        self,
        source_sandbox_id: str,
        target: SandboxSpec,
        *,
        operation: SandboxOperation | None = None,
        source_generation: int | None = None,
        source_spec_hash: str | None = None,
    ) -> tuple[SandboxRecord, SandboxForkRuntimeResult, dict[str, Any]]:
        records, results, timings = self.fork_many_with_timings(
            source_sandbox_id,
            (target,),
            operations=(operation,) if operation is not None else None,
            source_generation=source_generation,
            source_spec_hash=source_spec_hash,
        )
        return records[0], results[0], timings

    def fork_many(
        self,
        source_sandbox_id: str,
        targets: Sequence[SandboxSpec],
        *,
        operations: Sequence[SandboxOperation] | None = None,
        source_generation: int | None = None,
        source_spec_hash: str | None = None,
    ) -> tuple[
        tuple[SandboxRecord, ...],
        tuple[SandboxForkRuntimeResult, ...],
    ]:
        records, results, _timings = self.fork_many_with_timings(
            source_sandbox_id,
            targets,
            operations=operations,
            source_generation=source_generation,
            source_spec_hash=source_spec_hash,
        )
        return records, results

    def fork_many_with_timings(
        self,
        source_sandbox_id: str,
        targets: Sequence[SandboxSpec],
        *,
        operations: Sequence[SandboxOperation] | None = None,
        source_generation: int | None = None,
        source_spec_hash: str | None = None,
    ) -> tuple[
        tuple[SandboxRecord, ...],
        tuple[SandboxForkRuntimeResult, ...],
        dict[str, Any],
    ]:
        """Atomically plan and restore a same-instant local fan-out.

        Every destination intent is durable before the checkpoint is taken.
        All children share one deterministic, immutable checkpoint artifact;
        final state is committed atomically only after every restore succeeds.
        """

        started = time.monotonic()
        phases: dict[str, int] = {}
        if not SANDBOX_ID_RE.fullmatch(source_sandbox_id):
            raise ValueError("invalid source sandbox id")
        requested_targets = tuple(targets)
        if not requested_targets:
            raise ValueError("fork fan-out requires at least one target")
        if len(requested_targets) > MAX_FORK_FANOUT:
            raise ValueError(f"fork fan-out cannot exceed {MAX_FORK_FANOUT} targets")
        target_ids = [target.id for target in requested_targets]
        if len(set(target_ids)) != len(target_ids):
            raise ValueError("fork fan-out target ids must be unique")
        for target in requested_targets:
            target.validate()
        if operations is None:
            requested_operations = tuple(
                SandboxOperation.legacy_create(target) for target in requested_targets
            )
        else:
            requested_operations = tuple(operations)
            if len(requested_operations) != len(requested_targets):
                raise ValueError("fork fan-out requires one operation per target")
        for target, operation in zip(
            requested_targets, requested_operations, strict=True
        ):
            operation.validate_spec(target)

        with (
            self.lifecycle.exclusive(source_sandbox_id, *target_ids),
            _sandbox_fork_runtime_lock(self.store.path),
        ):
            phase = time.monotonic()
            with _sandbox_create_lock(self.store.path, target_ids[0]):
                state = self.store.load_state()
                if not state.drain.admission_open:
                    raise SandboxAdmissionClosedError(
                        f"sandbox fork admission is closed while drain token "
                        f"{state.drain.token!r} is active"
                    )
                source = state.records.get(source_sandbox_id)
                if source is None or source.state != "running":
                    raise ValueError(
                        f"running source sandbox not found: {source_sandbox_id}"
                    )
                if (
                    source_generation is not None
                    and source.generation != source_generation
                ):
                    raise SandboxStaleOperationError(
                        f"source generation changed before fork: {source_sandbox_id}"
                    )
                actual_source_hash = source.spec_hash or sandbox_spec_fingerprint(
                    source.spec
                )
                if source_spec_hash and actual_source_hash != source_spec_hash:
                    raise SandboxStaleOperationError(
                        f"source spec changed before fork: {source_sandbox_id}"
                    )
                unfinished_descendants = tuple(
                    record
                    for record in state.records.values()
                    if record.state == "restoring"
                    and record.source_sandbox_id == source_sandbox_id
                )
                if unfinished_descendants:
                    lineages = {
                        (record.checkpoint_id, record.fork_nonce)
                        for record in unfinished_descendants
                    }
                    requested_by_id = {
                        target.id: operation
                        for target, operation in zip(
                            requested_targets,
                            requested_operations,
                            strict=True,
                        )
                    }
                    adopts_unfinished_lineage = len(lineages) == 1 and all(
                        (existing := state.records.get(target_id)) is not None
                        and existing.creation_kind == "restore"
                        and existing.source_sandbox_id == source_sandbox_id
                        and existing.source_generation == source.generation
                        and existing.generation == operation.generation
                        and existing.operation_id == operation.operation_id
                        and existing.spec_hash == operation.spec_hash
                        and (existing.checkpoint_id, existing.fork_nonce) in lineages
                        for target_id, operation in requested_by_id.items()
                    )
                    if not adopts_unfinished_lineage:
                        raise SandboxBusyError(
                            "source sandbox has an unfinished fork lineage; only "
                            "an exact replay or subset can run until it resolves: "
                            f"{source_sandbox_id}"
                        )
                requested_checkpoint_id = fanout_checkpoint_id(
                    source_sandbox_id,
                    source.generation,
                    tuple(zip(requested_targets, requested_operations, strict=True)),
                )
                existing_targets = [
                    state.records.get(target.id) for target in requested_targets
                ]
                existing_checkpoint_ids = {
                    record.checkpoint_id
                    for record in existing_targets
                    if record is not None and record.checkpoint_id
                }
                existing_fork_nonces = {
                    record.fork_nonce
                    for record in existing_targets
                    if record is not None and record.fork_nonce
                }
                # A retry may intentionally address only the unfinished
                # subset of a previously persisted fan-out.  Adopt its shared
                # artifact only when every requested target already exists;
                # never append a new target after the memory instant was taken.
                if (
                    all(record is not None for record in existing_targets)
                    and len(existing_checkpoint_ids) == 1
                ):
                    checkpoint_id = next(iter(existing_checkpoint_ids))
                    if len(existing_fork_nonces) != 1:
                        raise SandboxConflictError(
                            "persisted fork fan-out has no unique readiness nonce"
                        )
                    fork_nonce = next(iter(existing_fork_nonces))
                else:
                    checkpoint_id = requested_checkpoint_id
                    fork_nonce = secrets.token_hex(32)
                planned_records: list[SandboxRecord] = []
                effective_targets: list[SandboxSpec] = []
                replaying: list[bool] = []
                state_changed = False
                for requested, operation in zip(
                    requested_targets, requested_operations, strict=True
                ):
                    validate_fork_compatibility(source.spec, requested)
                    tombstone = state.tombstones.get(requested.id)
                    if (
                        tombstone is not None
                        and operation.generation <= tombstone.generation
                    ):
                        raise SandboxStaleOperationError(
                            f"fork target generation {operation.generation} is fenced by "
                            f"tombstone generation {tombstone.generation}: {requested.id}"
                        )

                    existing = state.records.get(requested.id)
                    replaying.append(existing is not None)
                    if existing is not None:
                        if operation.generation != existing.generation:
                            error_type = (
                                SandboxStaleOperationError
                                if operation.generation < existing.generation
                                else SandboxConflictError
                            )
                            raise error_type(
                                f"fork target generation conflicts with live generation "
                                f"{existing.generation}: {requested.id}"
                            )
                        if (
                            existing.operation_id != operation.operation_id
                            or existing.spec_hash != operation.spec_hash
                            or existing.creation_kind != "restore"
                            or existing.source_sandbox_id != source_sandbox_id
                            or existing.source_generation != source.generation
                            or existing.checkpoint_id != checkpoint_id
                            or existing.fork_nonce != fork_nonce
                        ):
                            raise SandboxConflictError(
                                f"fork target already exists with another fan-out "
                                f"operation: {requested.id}"
                            )
                        if existing.state not in {"restoring", "running"}:
                            raise SandboxConflictError(
                                f"fork target is in incompatible state "
                                f"{existing.state}: {requested.id}"
                            )
                        planned = existing
                        effective = existing.spec
                    else:
                        self._require_available_capacity(requested, state.records)
                        effective = self._assign_ssh_port(requested, state.records)
                        now = utc_now()
                        planned = SandboxRecord(
                            spec=effective,
                            container_name=self.runtime.container_name(effective.id),
                            state="restoring",
                            created_at=now,
                            updated_at=now,
                            generation=operation.generation,
                            operation_id=operation.operation_id,
                            spec_hash=operation.spec_hash,
                            creation_kind="restore",
                            source_sandbox_id=source_sandbox_id,
                            source_generation=source.generation,
                            checkpoint_id=checkpoint_id,
                            fork_nonce=fork_nonce,
                        )
                        state.records[effective.id] = planned
                        state_changed = True
                    planned_records.append(planned)
                    effective_targets.append(effective)
                if state_changed:
                    self.store.save_state(state.records, state.tombstones)
                phases["persist_intent_ms"] = _elapsed_ms(phase)

            runtime_results: list[SandboxForkRuntimeResult | None] = [
                None for _target in effective_targets
            ]
            recovery_indexes: list[int] = []
            for index, record in enumerate(planned_records):
                if record.state == "running":
                    runtime_results[index] = SandboxForkRuntimeResult(
                        checkpoint_id=checkpoint_id,
                        commands=(),
                    )
                else:
                    recovery_indexes.append(index)

            # Inspect every replay candidate in bounded parallel before
            # running any workload hook. One shared wall-clock allowance keeps
            # 64 slow Docker inspections from multiplying the request time.
            inspection_deadline = (
                time.monotonic() + FORK_RECOVERY_INSPECTION_ALLOWANCE_SECONDS
            )

            def inspect_recovery_candidate(index: int) -> bool:
                target = effective_targets[index]
                operation = requested_operations[index]
                budget = _ForkSetupBudget.start(
                    max(0.0, inspection_deadline - time.monotonic())
                )
                identity = self.runtime.managed_container_identity(
                    target.id,
                    _fork_setup_budget=budget,
                )
                return (
                    identity is not None
                    and self._runtime_identity_matches(
                        identity,
                        operation=operation,
                        spec=target,
                    )
                    and self.runtime.managed_container_running(
                        target.id,
                        _fork_setup_budget=budget,
                    )
                )

            phase = time.monotonic()
            inspected = self._run_bounded_fork_tasks(
                recovery_indexes,
                inspect_recovery_candidate,
            )
            phases["recover_inspect_ms"] = _elapsed_ms(phase)
            ready_indexes = [index for index in recovery_indexes if inspected[index]]
            pending_indexes = [
                index for index in recovery_indexes if not inspected[index]
            ]

            def confirm_recovered_target(index: int) -> SandboxForkRuntimeResult:
                target = effective_targets[index]
                self.runtime.wait_fork_ready(
                    target,
                    checkpoint_id=checkpoint_id,
                    fork_nonce=fork_nonce,
                )
                return SandboxForkRuntimeResult(
                    checkpoint_id=checkpoint_id,
                    commands=(),
                )

            if ready_indexes:
                phase = time.monotonic()
                recovered = self._run_bounded_fork_tasks(
                    ready_indexes,
                    confirm_recovered_target,
                )
                phases["recover_ready_ms"] = _elapsed_ms(phase)
                for index in ready_indexes:
                    runtime_results[index] = recovered[index]

            recovered_from_container = bool(ready_indexes)
            has_restored_sibling = (
                any(record.state == "running" for record in planned_records)
                or recovered_from_container
            )
            pending = [
                (effective_targets[index], requested_operations[index])
                for index in pending_indexes
            ]

            if pending:
                phase = time.monotonic()
                restored = self.runtime.fork_many(
                    source.spec,
                    pending,
                    source_generation=source.generation,
                    source_spec_hash=actual_source_hash,
                    source_operation_id=source.operation_id,
                    checkpoint_id=checkpoint_id,
                    fork_nonce=fork_nonce,
                    require_existing_checkpoint=has_restored_sibling,
                )
                phases["checkpoint_restore_ms"] = _elapsed_ms(phase)
                for index, result in zip(pending_indexes, restored, strict=True):
                    runtime_results[index] = result

            completed_results = tuple(
                result for result in runtime_results if result is not None
            )
            if len(completed_results) != len(planned_records):
                raise RuntimeError("fork fan-out produced incomplete runtime results")

            phase = time.monotonic()
            with _sandbox_create_lock(self.store.path, target_ids[0]):
                final_state = self.store.load_state()
                final_records: list[SandboxRecord] = []
                for planned, operation in zip(
                    planned_records, requested_operations, strict=True
                ):
                    current = final_state.records.get(planned.spec.id)
                    if (
                        current is None
                        or current.generation != operation.generation
                        or current.operation_id != operation.operation_id
                        or current.checkpoint_id != checkpoint_id
                        or current.fork_nonce != fork_nonce
                    ):
                        raise SandboxStaleOperationError(
                            f"fork target intent changed during restore: "
                            f"{planned.spec.id}"
                        )
                    if current.state == "restoring":
                        current = replace(
                            current,
                            state="running",
                            updated_at=utc_now(),
                        )
                        final_state.records[current.spec.id] = current
                    elif current.state != "running":
                        raise SandboxConflictError(
                            f"fork target is in incompatible state {current.state}: "
                            f"{current.spec.id}"
                        )
                    final_records.append(current)
                self.store.save_state(final_state.records, final_state.tombstones)
                checkpoint_in_use = any(
                    record.state == "restoring"
                    and record.checkpoint_id == checkpoint_id
                    for record in final_state.records.values()
                )
            phases["store_record_ms"] = _elapsed_ms(phase)

            # Runtime cleanup happens after the durable running-state commit.
            # Bound the whole best-effort phase by one wall-clock allowance;
            # a stuck helper may leak an immutable artifact, but cannot delay
            # or roll back successfully restored children.
            phase = time.monotonic()
            cleanup_deadline = phase + FORK_SETUP_CLEANUP_ALLOWANCE_SECONDS

            def clean_restored_target(index: int) -> None:
                remaining = max(0.0, cleanup_deadline - time.monotonic())
                if remaining <= 0:
                    return
                budget = _ForkSetupBudget.start(remaining)
                try:
                    self.runtime.cleanup_restored_checkpoint(
                        effective_targets[index].id,
                        _fork_setup_budget=budget,
                    )
                except Exception:
                    pass

            try:
                self._run_bounded_fork_tasks(
                    tuple(range(len(effective_targets))),
                    clean_restored_target,
                )
            except Exception:
                pass
            if not checkpoint_in_use:
                remaining = max(0.0, cleanup_deadline - time.monotonic())
                if remaining > 0:
                    try:
                        self.runtime.release_checkpoint(
                            checkpoint_id,
                            _fork_setup_budget=_ForkSetupBudget.start(remaining),
                        )
                    except Exception:
                        # The sealed artifact is operation-scoped and immutable.
                        # A cleanup failure is a bounded leak, not a reason to
                        # destroy successfully restored children.
                        pass
            phases["cleanup_ms"] = _elapsed_ms(phase)
            result_timings: dict[str, Any] = {
                "total_ms": _elapsed_ms(started),
                "phases": phases,
                "idempotent": all(replaying),
            }
            if all(record.state == "running" for record in planned_records):
                result_timings["recovered"] = "store"
            elif recovered_from_container:
                result_timings["recovered"] = "container"
            elif any(replaying):
                result_timings["recovered"] = "runtime"
            return tuple(final_records), completed_results, result_timings

    def _run_bounded_fork_tasks(
        self,
        indexes: Sequence[int],
        task: Callable[[int], Any],
    ) -> dict[int, Any]:
        """Run indexed fork work with restore-equivalent bounded scheduling."""

        requested_indexes = tuple(indexes)
        if not requested_indexes:
            return {}
        worker_count = min(
            self.runtime.fork_restore_parallelism,
            len(requested_indexes),
        )
        results: dict[int, Any] = {}
        failures: list[tuple[int, Exception]] = []
        next_offset = 0
        with ThreadPoolExecutor(
            max_workers=worker_count,
            thread_name_prefix="sandbox-fork-recovery",
        ) as pool:
            in_flight: dict[Future[Any], int] = {}

            def submit(index: int) -> None:
                in_flight[pool.submit(task, index)] = index

            while next_offset < worker_count:
                submit(requested_indexes[next_offset])
                next_offset += 1

            while in_flight:
                completed, _pending = wait(
                    tuple(in_flight),
                    return_when=FIRST_COMPLETED,
                )
                for future in sorted(completed, key=lambda item: in_flight[item]):
                    index = in_flight.pop(future)
                    try:
                        results[index] = future.result()
                    except Exception as exc:
                        failures.append((index, exc))
                while (
                    not failures
                    and next_offset < len(requested_indexes)
                    and len(in_flight) < worker_count
                ):
                    submit(requested_indexes[next_offset])
                    next_offset += 1

        if failures:
            _index, failure = min(failures, key=lambda item: item[0])
            raise failure
        if len(results) != len(requested_indexes):
            raise RuntimeError("bounded fork work produced incomplete results")
        return results

    @staticmethod
    def _runtime_identity_matches(
        identity: tuple[int, str, str],
        *,
        operation: SandboxOperation,
        spec: SandboxSpec,
    ) -> bool:
        generation, operation_id, spec_hash = identity
        valid_hashes = {operation.spec_hash}
        if operation.generation == 0 and not operation.operation_id:
            valid_hashes.update(sandbox_spec_fingerprints(spec))
        return (
            generation == operation.generation
            and operation_id == operation.operation_id
            and spec_hash in valid_hashes
        )

    def _require_available_capacity(
        self,
        spec: SandboxSpec,
        records: dict[str, SandboxRecord],
    ) -> None:
        capacity = self.effective_capacity
        if capacity is None:
            return
        allocated = ResourceQuantity()
        for record in records.values():
            if record.state in {"running", "planned", "restoring"}:
                allocated = allocated + record.spec.requested_resources()
        requested = spec.requested_resources()
        prospective = allocated + requested
        exhausted = []
        if capacity.vcpu > 0 and prospective.vcpu > capacity.vcpu:
            exhausted.append("vcpu")
        if capacity.memory_mb > 0 and prospective.memory_mb > capacity.memory_mb:
            exhausted.append("memory_mb")
        if capacity.disk_mb > 0 and prospective.disk_mb > capacity.disk_mb:
            exhausted.append("disk_mb")
        if exhausted:
            dimensions = ", ".join(exhausted)
            raise SandboxCapacityUnavailableError(
                f"insufficient node capacity for sandbox {spec.id}: exhausted "
                f"{dimensions}; requested={requested.to_dict()}, "
                f"allocated={allocated.to_dict()}, "
                f"effective_capacity={capacity.to_dict()}"
            )

    def delete(
        self,
        sandbox_id: str,
        *,
        generation: int = 0,
        operation_id: str = "",
    ) -> tuple[SandboxRecord | None, CommandResult]:
        # Termination is the hard lifecycle boundary. Existing attached exec,
        # SSH, and file operations hold shared leases, but they must not veto a
        # forced container removal. Mark the sandbox exclusive first so no new
        # activity can start, then let runtime deletion sever existing work.
        with self.lifecycle.exclusive(sandbox_id, allow_shared=True):
            return self._delete_uncoordinated(
                sandbox_id,
                generation=generation,
                operation_id=operation_id,
            )

    def _delete_uncoordinated(
        self,
        sandbox_id: str,
        *,
        generation: int = 0,
        operation_id: str = "",
    ) -> tuple[SandboxRecord | None, CommandResult]:
        if not SANDBOX_ID_RE.match(sandbox_id):
            raise ValueError("invalid sandbox id.")
        if generation < 0:
            raise ValueError("sandbox generation cannot be negative")
        operation_id = operation_id.strip()
        if generation > 0 and not operation_id:
            raise ValueError("operation_id is required for versioned delete")
        if operation_id and not OPERATION_ID_RE.match(operation_id):
            raise ValueError("operation_id contains unsupported characters")
        with _sandbox_create_lock(self.store.path, sandbox_id):
            state = self.store.load_state()
            record = state.records.get(sandbox_id)
            checkpoint_to_release = ""
            tombstone = state.tombstones.get(sandbox_id)
            if tombstone is not None:
                if generation < tombstone.generation:
                    raise SandboxStaleOperationError(
                        f"sandbox delete generation {generation} is older than tombstone "
                        f"generation {tombstone.generation}: {sandbox_id}"
                    )
                if generation == tombstone.generation:
                    if operation_id == tombstone.operation_id:
                        return None, CommandResult(argv=(), exit_code=0)
                    if tombstone.operation_id.startswith("ttl:"):
                        # TTL is an autonomous delete, not a competing caller
                        # operation.  A later explicit delete of the already
                        # absent same generation may adopt the tombstone and
                        # become the stable replay identity.
                        state.tombstones[sandbox_id] = replace(
                            tombstone,
                            operation_id=operation_id,
                            updated_at=utc_now(),
                        )
                        self.store.save_state(state.records, state.tombstones)
                        return None, CommandResult(argv=(), exit_code=0)
                    else:
                        raise SandboxConflictError(
                            f"sandbox generation {generation} was tombstoned by a "
                            f"different operation: {sandbox_id}"
                        )
            if record is not None:
                if any(
                    candidate.spec.id != sandbox_id
                    and candidate.state == "restoring"
                    and candidate.source_sandbox_id == sandbox_id
                    for candidate in state.records.values()
                ):
                    raise SandboxBusyError(
                        f"sandbox has an unfinished fork restore: {sandbox_id}"
                    )
                if (
                    record.state == "restoring"
                    and record.checkpoint_id
                    and self.runtime.checkpoint_artifact_state(record.checkpoint_id)
                    == "pending"
                ):
                    raise SandboxBusyError(
                        "sandbox restore has an ambiguous checkpoint operation: "
                        f"{sandbox_id}"
                    )
                if generation < record.generation:
                    raise SandboxStaleOperationError(
                        f"sandbox delete generation {generation} is older than live "
                        f"generation {record.generation}: {sandbox_id}"
                    )
                if generation > record.generation:
                    raise SandboxConflictError(
                        f"delete generation {generation} cannot remove live generation "
                        f"{record.generation}: {sandbox_id}"
                    )
                if record.state == "deleting":
                    if (
                        record.generation > 0
                        and record.delete_operation_id != operation_id
                    ):
                        raise SandboxConflictError(
                            f"sandbox generation {generation} is being deleted by a "
                            f"different operation: {sandbox_id}"
                        )
                else:
                    record = replace(
                        record,
                        state="deleting",
                        delete_operation_id=operation_id,
                        updated_at=utc_now(),
                    )
                    state.records[sandbox_id] = record
                    self.store.save_state(state.records, state.tombstones)
                # Docker deletion is idempotent.  If the process died after a
                # successful remove, replay observes the durable deleting
                # intent and completes the same operation/tombstone.
                result = self.runtime.delete(sandbox_id)
                if record.spec.forkable:
                    self.runtime.drop_application_checkpoint(
                        sandbox_id,
                        record.generation,
                        record.spec_hash,
                    )
                checkpoint_to_release = record.checkpoint_id
                spec_hash = record.spec_hash
                state.records.pop(sandbox_id, None)
            else:
                runtime_identity = self.runtime.managed_container_identity(sandbox_id)
                if runtime_identity is not None:
                    runtime_generation, _runtime_operation_id, runtime_spec_hash = (
                        runtime_identity
                    )
                    if generation < runtime_generation:
                        raise SandboxStaleOperationError(
                            f"sandbox delete generation {generation} is older than runtime "
                            f"generation {runtime_generation}: {sandbox_id}"
                        )
                    if generation > runtime_generation:
                        raise SandboxConflictError(
                            f"delete generation {generation} cannot remove runtime generation "
                            f"{runtime_generation}: {sandbox_id}"
                        )
                    result = self.runtime.delete(sandbox_id)
                    spec_hash = runtime_spec_hash
                    if re.fullmatch(r"[0-9a-f]{64}", runtime_spec_hash):
                        # The store may have been lost after Docker create.
                        # Dropping a derived absent application directory is
                        # idempotent, so this also safely covers non-forkable
                        # managed containers.
                        self.runtime.drop_application_checkpoint(
                            sandbox_id,
                            runtime_generation,
                            runtime_spec_hash,
                        )
                else:
                    result = CommandResult(argv=(), exit_code=0)
                    spec_hash = tombstone.spec_hash if tombstone is not None else ""
            state.tombstones[sandbox_id] = SandboxTombstone(
                sandbox_id=sandbox_id,
                generation=generation,
                operation_id=operation_id,
                spec_hash=spec_hash,
                updated_at=utc_now(),
            )
            self.store.save_state(state.records, state.tombstones)
            self._release_checkpoint_if_unused(
                checkpoint_to_release,
                state.records,
            )
            return record, result

    def active_count(self) -> int:
        return self.activity_snapshot().active_sandboxes

    def requested_resources(self) -> ResourceQuantity:
        snapshot = self.activity_snapshot()
        return snapshot.used_resources + snapshot.reserved_resources

    def configure_drain(
        self,
        token: str,
        draining: bool,
        *,
        active_build_count: Callable[[], int],
    ) -> NodeDrainSnapshot:
        token = token.strip()
        if not token or not OPERATION_ID_RE.match(token):
            raise ValueError("drain token contains unsupported characters")
        with _sandbox_create_lock(self.store.path, "configure-drain"):
            state = self.store.load_state()
            current = state.drain
            if draining:
                if current.draining:
                    if current.token != token:
                        raise SandboxConflictError(
                            f"node is already draining with token {current.token!r}"
                        )
                    return self._drain_snapshot_locked(state, active_build_count)
                next_revision = state.revision + 1
                build_count = max(0, active_build_count())
                idle = not state.records and build_count == 0
                drain = NodeDrainState(
                    draining=True,
                    token=token,
                    drain_activity_epoch=next_revision if idle else 0,
                    admission_open=False,
                )
            else:
                if current.draining:
                    if current.token != token:
                        raise SandboxConflictError(
                            f"drain token does not match active token {current.token!r}"
                        )
                elif current.token == token:
                    return self._drain_snapshot_locked(state, active_build_count)
                else:
                    raise SandboxConflictError("node is not draining with this token")
                drain = NodeDrainState(
                    draining=False,
                    # Retain the last token so an undrain retry is idempotent.
                    token=token,
                    drain_activity_epoch=0,
                    admission_open=True,
                )
            revision = self.store.save_state(
                state.records,
                state.tombstones,
                drain=drain,
            )
            state = SandboxStoreState(
                records=state.records,
                tombstones=state.tombstones,
                revision=revision,
                drain=drain,
            )
            return self._drain_snapshot_locked(state, active_build_count)

    def heartbeat_snapshot(
        self,
        *,
        active_build_count: Callable[[], int],
    ) -> NodeDrainSnapshot:
        with _sandbox_create_lock(self.store.path, "heartbeat-snapshot"):
            records, _expired, _revision = self._cleanup_expired_and_load()
            state = self.store.load_state()
            if state.records != records:
                # Both reads happen under the same exclusive lock; this is a
                # defensive assertion against future cleanup refactors.
                raise RuntimeError("sandbox state changed during heartbeat snapshot")
            return self._drain_snapshot_locked(state, active_build_count)

    def _drain_snapshot_locked(
        self,
        state: SandboxStoreState,
        active_build_count: Callable[[], int],
    ) -> NodeDrainSnapshot:
        build_count = max(0, active_build_count())
        activity = self._activity_from_records(state.records, state.revision)
        drain = state.drain
        if (
            drain.draining
            and not activity.records
            and build_count == 0
            and drain.drain_activity_epoch != activity.activity_revision
        ):
            next_revision = state.revision + 1
            drain = replace(drain, drain_activity_epoch=next_revision)
            revision = self.store.save_state(
                state.records,
                state.tombstones,
                drain=drain,
            )
            activity = replace(activity, activity_revision=revision)
        return NodeDrainSnapshot(
            activity=activity,
            drain=drain,
            active_image_builds=build_count,
        )

    def activity_snapshot(self) -> SandboxActivitySnapshot:
        """Return coherent activity fields from one cleanup/store snapshot."""
        with _sandbox_create_lock(self.store.path, "activity-snapshot"):
            records, _expired, revision = self._cleanup_expired_and_load()
            return self._activity_from_records(records, revision)

    @staticmethod
    def _activity_from_records(
        records: dict[str, SandboxRecord],
        revision: int,
    ) -> SandboxActivitySnapshot:
        running_records = tuple(
            records[sandbox_id]
            for sandbox_id in sorted(records)
            if records[sandbox_id].state in {"running", "deleting"}
        )
        reserved_records = tuple(
            records[sandbox_id]
            for sandbox_id in sorted(records)
            if records[sandbox_id].state in {"planned", "restoring"}
        )
        used = ResourceQuantity()
        for record in running_records:
            used = used + record.spec.requested_resources()
        reserved = ResourceQuantity()
        for record in reserved_records:
            reserved = reserved + record.spec.requested_resources()
        return SandboxActivitySnapshot(
            records=tuple(records[sandbox_id] for sandbox_id in sorted(records)),
            active_sandboxes=len(running_records),
            used_resources=used,
            reserved_resources=reserved,
            activity_revision=revision,
        )

    def snapshot(
        self,
        sandbox_id: str,
        target_image: str,
    ) -> CommandResult:
        with self.lifecycle.shared(sandbox_id):
            self._require_sandbox(sandbox_id)
            return self.runtime.snapshot(sandbox_id, target_image)

    def upload_file(
        self,
        sandbox_id: str,
        container_path: str,
        content: bytes,
    ) -> CommandResult:
        with self.lifecycle.shared(sandbox_id):
            record = self._require_sandbox(sandbox_id)
            validate_container_file_path("container_path", container_path)
            return self.runtime.write_file_to_container(
                sandbox_id,
                container_path,
                content,
                owner=record.spec.security.user,
            )

    def download_file(
        self,
        sandbox_id: str,
        container_path: str,
        *,
        max_bytes: int | None = None,
    ) -> tuple[bytes, CommandResult]:
        with self.lifecycle.shared(sandbox_id):
            self._require_sandbox(sandbox_id)
            validate_container_file_path("container_path", container_path)
            content, result = self.runtime.read_file_from_container(
                sandbox_id,
                container_path,
                max_bytes=max_bytes,
            )
            if max_bytes is not None and len(content) > max_bytes:
                raise SandboxFileTooLargeError(
                    f"sandbox file exceeds the {max_bytes} byte download limit"
                )
            return content, result

    def _require_sandbox(self, sandbox_id: str) -> SandboxRecord:
        return self.require_activity_sandbox(sandbox_id)

    def _assign_ssh_port(
        self,
        spec: SandboxSpec,
        records: dict[str, SandboxRecord],
    ) -> SandboxSpec:
        if not spec.ssh.enabled or spec.ssh.host_port is not None:
            return spec
        if self.ssh_port_range is None:
            raise ValueError(
                "ssh host_port is required when no ssh port range is configured."
            )
        start, end = self.ssh_port_range
        if not _valid_port(start) or not _valid_port(end) or start > end:
            raise ValueError("invalid ssh port range.")
        used = {
            record.spec.ssh.host_port
            for record in records.values()
            if record.spec.ssh.enabled and record.spec.ssh.host_port is not None
        }
        for port in range(start, end + 1):
            if port not in used:
                return replace(spec, ssh=replace(spec.ssh, host_port=port))
        raise ValueError("no free ssh ports available.")

    def cleanup_expired(self, now: datetime | None = None) -> list[SandboxRecord]:
        with _sandbox_create_lock(self.store.path, "cleanup-expired"):
            _records, expired, _revision = self._cleanup_expired_and_load(now)
            return expired

    def _cleanup_expired_and_load(
        self,
        now: datetime | None = None,
    ) -> tuple[dict[str, SandboxRecord], list[SandboxRecord], int]:
        state = self.store.load_state()
        records = state.records
        revision = state.revision
        expired = [
            record
            for record in records.values()
            if record.state in {"running", "planned", "restoring"}
            and record.is_expired(now)
            and not any(
                candidate.spec.id != record.spec.id
                and candidate.state == "restoring"
                and candidate.source_sandbox_id == record.spec.id
                for candidate in records.values()
            )
        ]
        if not expired:
            return records, [], revision
        deleted: list[SandboxRecord] = []
        checkpoint_ids: set[str] = set()
        for record in expired:
            if record.state == "restoring" and record.checkpoint_id:
                try:
                    if (
                        self.runtime.checkpoint_artifact_state(record.checkpoint_id)
                        == "pending"
                    ):
                        continue
                except RuntimeError:
                    continue
            try:
                # TTL expiration is termination, not a cooperative lifecycle
                # transition. Active exec, SSH, and file operations must not
                # keep an expired sandbox (and its node) alive indefinitely.
                with self.lifecycle.exclusive(record.spec.id, allow_shared=True):
                    self.runtime.delete(record.spec.id)
                    if record.spec.forkable:
                        self.runtime.drop_application_checkpoint(
                            record.spec.id,
                            record.generation,
                            record.spec_hash,
                        )
            except (RuntimeError, SandboxBusyError):
                # Preserve both the record and its resource accounting.  A
                # later cleanup pass can safely retry the transient failure.
                continue
            records.pop(record.spec.id, None)
            if record.checkpoint_id:
                checkpoint_ids.add(record.checkpoint_id)
            previous = state.tombstones.get(record.spec.id)
            if previous is None or record.generation >= previous.generation:
                state.tombstones[record.spec.id] = SandboxTombstone(
                    sandbox_id=record.spec.id,
                    generation=record.generation,
                    operation_id=f"ttl:{record.operation_id or 'legacy'}",
                    spec_hash=record.spec_hash,
                    updated_at=utc_now(),
                )
            deleted.append(record)
        if deleted:
            revision = self.store.save_state(records, state.tombstones)
            for checkpoint_id in checkpoint_ids:
                self._release_checkpoint_if_unused(checkpoint_id, records)
        return records, deleted, revision

    def _release_checkpoint_if_unused(
        self,
        checkpoint_id: str,
        records: dict[str, SandboxRecord],
    ) -> None:
        """Drop an artifact only after its last restoring intent is gone."""

        if not checkpoint_id or any(
            record.state == "restoring" and record.checkpoint_id == checkpoint_id
            for record in records.values()
        ):
            return
        try:
            self.runtime.release_checkpoint(checkpoint_id)
        except RuntimeError:
            # A sealed checkpoint is immutable and operation-scoped.  Failed
            # cleanup is a bounded leak and is safe to retry during recovery.
            pass


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


def _format_float(value: float) -> str:
    if value.is_integer():
        return str(int(value))
    return str(value)


def _elapsed_ms(started: float) -> int:
    return max(0, int((time.monotonic() - started) * 1000))


def sandbox_spec_fingerprint(spec: SandboxSpec) -> str:
    raw = json.dumps(spec.to_dict(), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def sandbox_spec_fingerprints(spec: SandboxSpec) -> set[str]:
    raw = spec.to_dict()
    variants = [raw]
    if not spec.forkable:
        without_forkable = dict(raw)
        without_forkable.pop("forkable", None)
        variants.append(without_forkable)
    if spec.fork_protocol == SandboxForkProtocolSpec():
        for candidate in tuple(variants):
            without_protocol = dict(candidate)
            without_protocol.pop("fork_protocol", None)
            variants.append(without_protocol)
    if spec.profile == "container" and spec.linux_host == SandboxLinuxHostSpec():
        for candidate in tuple(variants):
            legacy_raw = dict(candidate)
            legacy_raw.pop("profile", None)
            legacy_raw.pop("linux_host", None)
            variants.append(legacy_raw)
    return {
        hashlib.sha256(
            json.dumps(candidate, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        for candidate in variants
    }


def sandbox_specs_match(existing: SandboxSpec, requested: SandboxSpec) -> bool:
    normalized_requested = requested
    if (
        existing.ssh.enabled
        and requested.ssh.enabled
        and requested.ssh.host_port is None
        and existing.ssh.host_port is not None
    ):
        normalized_requested = replace(
            requested,
            ssh=replace(requested.ssh, host_port=existing.ssh.host_port),
        )
    return existing.to_dict() == normalized_requested.to_dict()


def sandbox_fork_target(source: SandboxSpec, raw: object) -> SandboxSpec:
    """Build a restore-compatible child spec from a fork request payload."""

    if not isinstance(raw, dict):
        raise ValueError("fork payload must be a JSON object")
    target_raw = raw.get("sandbox", raw.get("target", raw))
    if not isinstance(target_raw, dict):
        raise ValueError("fork sandbox must be a JSON object")
    if target_raw.get("image"):
        target = SandboxSpec.from_dict(target_raw)
    else:
        allowed = {
            "id",
            "env",
            "labels",
            "ttl_seconds",
            "memory_mb",
            "cpus",
            "disk_mb",
        }
        unsupported = sorted(set(target_raw) - allowed)
        if unsupported:
            raise ValueError(
                "fork overrides contain unsupported fields: " + ", ".join(unsupported)
            )
        target_id = str(target_raw.get("id") or "").strip()
        env = dict(source.env)
        raw_env = target_raw.get("env")
        if raw_env is not None:
            if not isinstance(raw_env, dict):
                raise ValueError("fork env must be a JSON object")
            env.update({str(key): str(value) for key, value in raw_env.items()})
        labels = dict(source.labels)
        raw_labels = target_raw.get("labels")
        if raw_labels is not None:
            if not isinstance(raw_labels, dict):
                raise ValueError("fork labels must be a JSON object")
            labels.update({str(key): str(value) for key, value in raw_labels.items()})
        target = replace(
            source,
            id=target_id,
            env=env,
            labels=labels,
            ttl_seconds=(
                int(target_raw["ttl_seconds"])
                if target_raw.get("ttl_seconds") is not None
                else source.ttl_seconds
            ),
            memory_mb=(
                int(target_raw["memory_mb"])
                if target_raw.get("memory_mb") is not None
                else source.memory_mb
            ),
            cpus=(
                float(target_raw["cpus"])
                if target_raw.get("cpus") is not None
                else source.cpus
            ),
            disk_mb=(
                int(target_raw["disk_mb"])
                if target_raw.get("disk_mb") is not None
                else source.disk_mb
            ),
            ssh=(
                replace(source.ssh, host_port=None)
                if source.ssh.enabled
                else source.ssh
            ),
        )
    target.validate()
    validate_fork_compatibility(source, target)
    return target


def validate_fork_compatibility(source: SandboxSpec, target: SandboxSpec) -> None:
    if not source.forkable or not target.forkable:
        raise ValueError("source and target sandboxes must opt in with forkable=true")
    if source.id == target.id:
        raise ValueError("fork target id must differ from source id")
    immutable_pairs = {
        "image": (source.image, target.image),
        "profile": (source.profile, target.profile),
        "command": (source.command, target.command),
        "working_dir": (source.working_dir, target.working_dir),
        "network": (source.network, target.network),
        "fork_protocol": (source.fork_protocol, target.fork_protocol),
        # disk_mb changes either Docker's rootfs quota or the size option of
        # the quota-backed workspace tmpfs.  The latter is an OCI mount-option
        # change and runsc correctly rejects it during restore.
        "disk_mb": (source.disk_mb, target.disk_mb),
        "security": (source.security, target.security),
        "filesystem": (source.filesystem, target.filesystem),
        "linux_host": (source.linux_host, target.linux_host),
        "ssh.enabled": (source.ssh.enabled, target.ssh.enabled),
        "ssh.user": (source.ssh.user, target.ssh.user),
        "ssh.host": (source.ssh.host, target.ssh.host),
        "ssh.container_port": (
            source.ssh.container_port,
            target.ssh.container_port,
        ),
        "ssh.authorized_keys": (
            source.ssh.authorized_keys,
            target.ssh.authorized_keys,
        ),
    }
    changed = [
        name for name, values in immutable_pairs.items() if values[0] != values[1]
    ]
    if changed:
        raise ValueError(
            "fork target changes restore-incompatible fields: " + ", ".join(changed)
        )


def validate_checkpoint_id(value: str) -> None:
    if re.fullmatch(r"[a-z0-9][a-z0-9_.-]{0,127}", value) is None:
        raise ValueError("checkpoint id contains unsupported characters")


def application_checkpoint_id(
    sandbox_id: str,
    generation: int,
    spec_hash: str,
) -> str:
    """Return a stable, generation-scoped helper identifier for runsc."""

    if SANDBOX_ID_RE.fullmatch(sandbox_id) is None:
        raise ValueError("invalid sandbox id.")
    if generation < 0:
        raise ValueError("sandbox generation cannot be negative")
    if re.fullmatch(r"[0-9a-f]{64}", spec_hash) is None:
        raise ValueError("sandbox spec hash must be a lowercase SHA-256 digest")
    generation_identity = hashlib.sha256(
        f"{generation}\0{spec_hash}".encode("ascii")
    ).hexdigest()[:32]
    return f"{sandbox_id}-{generation_identity}"


def validate_fork_nonce(value: str) -> None:
    if FORK_NONCE_RE.fullmatch(value) is None:
        raise ValueError("fork nonce must be a lowercase 64-hex value")


def fork_checkpoint_id(
    source_sandbox_id: str,
    source_generation: int,
    operation: SandboxOperation,
) -> str:
    raw = "\0".join(
        (
            source_sandbox_id,
            str(source_generation),
            str(operation.generation),
            operation.operation_id,
            operation.spec_hash,
        )
    )
    return "fork-" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:40]


def fanout_checkpoint_id(
    source_sandbox_id: str,
    source_generation: int,
    targets: Sequence[tuple[SandboxSpec, SandboxOperation]],
) -> str:
    """Return a stable artifact id for one same-instant fork request.

    Preserve the historical single-child identity so the existing fork API
    remains replay-compatible.  Multi-child identity is order-independent and
    binds every target operation into the checkpoint lineage.
    """

    if not targets:
        raise ValueError("fork fan-out requires at least one target")
    if len(targets) == 1:
        return fork_checkpoint_id(
            source_sandbox_id,
            source_generation,
            targets[0][1],
        )
    entries = sorted(
        (
            target.id,
            str(operation.generation),
            operation.operation_id,
            operation.kind,
            operation.spec_hash,
        )
        for target, operation in targets
    )
    raw_parts = [source_sandbox_id, str(source_generation), "fanout-v1"]
    for entry in entries:
        raw_parts.extend(entry)
    raw = "\0".join(raw_parts)
    return "fork-set-" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:36]


def _valid_port(value: int) -> bool:
    return 1 <= value <= 65535


def _string_tuple(raw: object, *, default: tuple[str, ...]) -> tuple[str, ...]:
    if raw is None:
        return default
    if isinstance(raw, str):
        return (raw,) if raw else ()
    if isinstance(raw, list) and all(isinstance(item, str) for item in raw):
        return tuple(raw)
    raise ValueError("expected a string list.")


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


def validate_container_file_path(name: str, value: str) -> None:
    validate_container_path(name, value)
    if value == "/" or value.endswith("/"):
        raise ValueError(f"{name} must identify a file, not a directory.")
