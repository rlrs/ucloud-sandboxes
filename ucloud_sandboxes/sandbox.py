from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timedelta
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
from threading import Event, RLock, Thread, local
import time
from typing import Any, Callable, ContextManager, Iterator, Protocol

from .models import ResourceQuantity, parse_iso_datetime, utc_now


SANDBOX_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")
OPERATION_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
ENV_KEY_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
DEFAULT_CONTAINER_PREFIX = "ucloud-sandbox-"
SANDBOX_GENERATION_LABEL = "ucloud-sandboxes.generation"
SANDBOX_OPERATION_ID_LABEL = "ucloud-sandboxes.operation-id"
SANDBOX_SPEC_HASH_LABEL = "ucloud-sandboxes.spec-sha256"
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
            host_port=int(raw["host_port"]) if raw.get("host_port") is not None else None,
            container_port=(
                int(raw["container_port"]) if raw.get("container_port") is not None else 22
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
        elif isinstance(command, list) and all(isinstance(item, str) for item in command):
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


class CommandExecutor(Protocol):
    def run(self, argv: tuple[str, ...], *, input: bytes | None = None) -> CommandResult:
        ...


class SubprocessExecutor:
    def run(self, argv: tuple[str, ...], *, input: bytes | None = None) -> CommandResult:
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
                    if (
                        len(stdout) > max_stdout_bytes
                        and not stdout_overflow.is_set()
                    ):
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
        self.stdout_bytes = stdout_bytes if stdout_bytes is not None else stdout.encode()
        self.stderr_bytes = stderr_bytes if stderr_bytes is not None else stderr.encode()
        self.commands: list[tuple[str, ...]] = []
        self.inputs: list[bytes | None] = []

    def run(self, argv: tuple[str, ...], *, input: bytes | None = None) -> CommandResult:
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
        container_prefix: str = DEFAULT_CONTAINER_PREFIX,
        allow_storage_opt_quota: bool = False,
        allow_tmpfs_workspace: bool = False,
        dry_run: bool = False,
    ) -> None:
        self.executor = executor or SubprocessExecutor()
        self.docker_binary = docker_binary
        self.runtime_name = runtime_name
        self.container_prefix = container_prefix
        self.allow_storage_opt_quota = allow_storage_opt_quota
        self.allow_tmpfs_workspace = allow_tmpfs_workspace
        self.dry_run = dry_run
        self._operation_local = local()

    def container_name(self, sandbox_id: str) -> str:
        return f"{self.container_prefix}{sandbox_id}"

    def create(
        self,
        spec: SandboxSpec,
        operation: SandboxOperation | None = None,
    ) -> CommandResult:
        spec.validate()
        operation = operation or getattr(self._operation_local, "operation", None)
        return self._run(self.create_command(spec, operation=operation))

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

    def delete(self, sandbox_id: str) -> CommandResult:
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
        result = self.executor.run(argv)
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
    ) -> tuple[str, ...]:
        spec.validate()
        operation = operation or SandboxOperation.legacy_create(spec)
        argv: list[str] = [
            self.docker_binary,
            "run",
            "-d",
            "--name",
            self.container_name(spec.id),
            "--runtime",
            self.runtime_name,
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
        if spec.memory_mb is not None:
            argv.extend(["--memory", f"{spec.memory_mb}m"])
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
        for key in sorted(spec.env):
            argv.extend(["-e", f"{key}={spec.env[key]}"])
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
                raise ValueError("ssh host_port must be assigned before runtime create.")
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
        argv.append(spec.image)
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

    def _run(self, argv: tuple[str, ...], *, input: bytes | None = None) -> CommandResult:
        if self.dry_run:
            return CommandResult(argv=argv, exit_code=0)
        result = self.executor.run(argv, input=input)
        if result.exit_code != 0:
            raise RuntimeError(
                f"command failed with exit code {result.exit_code}: {' '.join(argv)}\n"
                f"{result.stderr}"
            )
        return result

    def is_container_name_conflict(self, error: BaseException) -> bool:
        message = str(error).lower()
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
        result = self.executor.run(argv)
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
            raise RuntimeError("docker inspect returned invalid container labels") from exc
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
            raise RuntimeError("runtime container has an invalid generation label") from exc
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
                records[sandbox_id].to_dict()
                for sandbox_id in sorted(records)
            ],
            "tombstones": [
                tombstones[sandbox_id].to_dict()
                for sandbox_id in sorted(tombstones)
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


class SandboxManager:
    def __init__(
        self,
        store: SandboxStore,
        runtime: DockerGvisorRuntime,
        *,
        ssh_port_range: tuple[int, int] | None = None,
        effective_capacity: ResourceQuantity | None = None,
    ) -> None:
        if effective_capacity is not None and not effective_capacity.is_valid:
            raise ValueError(
                "effective_capacity cannot contain negative or non-finite values"
            )
        self.store = store
        self.runtime = runtime
        self.ssh_port_range = ssh_port_range
        self.effective_capacity = effective_capacity

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
                    return existing, CommandResult(argv=(), exit_code=0), {
                        "total_ms": _elapsed_ms(started),
                        "phases": phases,
                        "idempotent": True,
                        "recovered": "store",
                    }
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
                    return record, CommandResult(argv=(), exit_code=0), {
                        "total_ms": _elapsed_ms(started),
                        "phases": phases,
                        "idempotent": True,
                        "recovered": "container",
                    }
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
                if not self.runtime.managed_container_matches(spec, operation=operation):
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
                return record, CommandResult(argv=(), exit_code=0), {
                    "total_ms": _elapsed_ms(started),
                    "phases": phases,
                    "idempotent": True,
                    "recovered": "container",
                }
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
            return record, result, {
                "total_ms": _elapsed_ms(started),
                "phases": phases,
                "idempotent": replaying_planned,
            }

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
            if record.state in {"running", "planned"}:
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
            if records[sandbox_id].state == "planned"
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
        return self.runtime.snapshot(sandbox_id, target_image)

    def upload_file(
        self,
        sandbox_id: str,
        container_path: str,
        content: bytes,
    ) -> CommandResult:
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
        record = self.get(sandbox_id)
        if record is None:
            raise ValueError(f"sandbox not found: {sandbox_id}")
        return record

    def _assign_ssh_port(
        self,
        spec: SandboxSpec,
        records: dict[str, SandboxRecord],
    ) -> SandboxSpec:
        if not spec.ssh.enabled or spec.ssh.host_port is not None:
            return spec
        if self.ssh_port_range is None:
            raise ValueError("ssh host_port is required when no ssh port range is configured.")
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
            if record.state in {"running", "planned"} and record.is_expired(now)
        ]
        if not expired:
            return records, [], revision
        deleted: list[SandboxRecord] = []
        for record in expired:
            try:
                self.runtime.delete(record.spec.id)
            except RuntimeError:
                # Preserve both the record and its resource accounting.  A
                # later cleanup pass can safely retry the transient failure.
                continue
            records.pop(record.spec.id, None)
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
        return records, deleted, revision


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
    fingerprints = {
        hashlib.sha256(
            json.dumps(raw, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
    }
    if spec.profile == "container" and spec.linux_host == SandboxLinuxHostSpec():
        legacy_raw = dict(raw)
        legacy_raw.pop("profile", None)
        legacy_raw.pop("linux_host", None)
        fingerprints.add(
            hashlib.sha256(
                json.dumps(
                    legacy_raw,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
            ).hexdigest()
        )
    return fingerprints


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
