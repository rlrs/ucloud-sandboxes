from __future__ import annotations

from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timedelta
import json
from pathlib import Path
import re
import subprocess
from tempfile import TemporaryDirectory
from threading import RLock
import time
from typing import Any, Protocol

from .models import ResourceQuantity, parse_iso_datetime, utc_now


SANDBOX_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")
ENV_KEY_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
DEFAULT_CONTAINER_PREFIX = "ucloud-sandbox-"
DEFAULT_SANDBOX_USER = "1000:1000"
DEFAULT_PIDS_LIMIT = 256
SECURITY_VALUE_RE = re.compile(r"^[A-Za-z0-9_.:@/-]+$")
CONTAINER_PATH_RE = re.compile(r"^/[A-Za-z0-9_./-]+$")
_SANDBOX_LOCKS_GUARD = RLock()
_SANDBOX_LOCKS: dict[Path, RLock] = {}


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
    labels: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "SandboxSpec":
        command = raw.get("command", ())
        if isinstance(command, str):
            command_items: tuple[str, ...] = (command,)
        elif isinstance(command, list) and all(isinstance(item, str) for item in command):
            command_items = tuple(command)
        else:
            command_items = ()
        env = raw.get("env") or {}
        labels = raw.get("labels") or {}
        return cls(
            id=str(raw.get("id") or ""),
            image=str(raw.get("image") or ""),
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
            security=SandboxSecuritySpec.from_dict(raw.get("security")),
            filesystem=SandboxFilesystemSpec.from_dict(raw.get("filesystem")),
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
        if self.network not in {"none", "bridge"}:
            raise ValueError("network must be either 'none' or 'bridge'.")
        self.ssh.validate()
        self.security.validate()
        self.filesystem.validate()
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

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "SandboxRecord":
        spec_raw = raw.get("spec")
        if not isinstance(spec_raw, dict):
            raise ValueError("sandbox record is missing spec.")
        created_at = parse_iso_datetime(raw.get("created_at"))
        updated_at = parse_iso_datetime(raw.get("updated_at"))
        if created_at is None or updated_at is None:
            raise ValueError("sandbox record has invalid timestamps.")
        return cls(
            spec=SandboxSpec.from_dict(spec_raw),
            container_name=str(raw.get("container_name") or ""),
            state=str(raw.get("state") or ""),
            created_at=created_at,
            updated_at=updated_at,
        )

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "spec": self.spec.to_dict(),
            "container_name": self.container_name,
            "state": self.state,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
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

    def container_name(self, sandbox_id: str) -> str:
        return f"{self.container_prefix}{sandbox_id}"

    def create(self, spec: SandboxSpec) -> CommandResult:
        spec.validate()
        return self._run(self.create_command(spec))

    def delete(self, sandbox_id: str) -> CommandResult:
        if not SANDBOX_ID_RE.match(sandbox_id):
            raise ValueError("invalid sandbox id.")
        return self._run(
            (
                self.docker_binary,
                "rm",
                "-f",
                self.container_name(sandbox_id),
            )
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
    ) -> tuple[bytes, CommandResult]:
        validate_container_file_path("container_path", container_path)
        result = self._run(
            self.exec_command(
                sandbox_id,
                ("sh", "-c", 'cat "${UCLOUD_SANDBOX_FILE:?}"'),
                env={"UCLOUD_SANDBOX_FILE": container_path},
                interactive=False,
                user="0",
            )
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

    def create_command(self, spec: SandboxSpec) -> tuple[str, ...]:
        spec.validate()
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
        for key in sorted(spec.labels):
            argv.extend(["--label", f"{key}={spec.labels[key]}"])
        argv.append(spec.image)
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


class SandboxStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = _sandbox_lock(path)

    def load(self) -> dict[str, SandboxRecord]:
        with self._lock:
            if not self.path.exists():
                return {}
            raw = json.loads(self.path.read_text(encoding="utf-8"))
            if not isinstance(raw, dict):
                raise ValueError("sandbox store must contain a JSON object.")
            items = raw.get("sandboxes", [])
            if not isinstance(items, list):
                raise ValueError("sandbox store must contain a sandboxes list.")
            records: dict[str, SandboxRecord] = {}
            for item in items:
                if not isinstance(item, dict):
                    continue
                record = SandboxRecord.from_dict(item)
                records[record.spec.id] = record
            return records

    def save(self, records: dict[str, SandboxRecord]) -> None:
        with self._lock:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = self.path.with_suffix(self.path.suffix + ".tmp")
            payload = {
                "sandboxes": [
                    records[sandbox_id].to_dict()
                    for sandbox_id in sorted(records)
                ]
            }
            tmp_path.write_text(
                json.dumps(payload, indent=2, sort_keys=True),
                encoding="utf-8",
            )
            tmp_path.replace(self.path)

    def upsert(self, record: SandboxRecord) -> dict[str, SandboxRecord]:
        with self._lock:
            records = self.load()
            records[record.spec.id] = record
            self.save(records)
            return records

    def delete(self, sandbox_id: str) -> SandboxRecord | None:
        with self._lock:
            records = self.load()
            record = records.pop(sandbox_id, None)
            self.save(records)
            return record


def _sandbox_lock(path: Path) -> RLock:
    key = path.resolve()
    with _SANDBOX_LOCKS_GUARD:
        lock = _SANDBOX_LOCKS.get(key)
        if lock is None:
            lock = RLock()
            _SANDBOX_LOCKS[key] = lock
        return lock


class SandboxManager:
    def __init__(
        self,
        store: SandboxStore,
        runtime: DockerGvisorRuntime,
        *,
        ssh_port_range: tuple[int, int] | None = None,
    ) -> None:
        self.store = store
        self.runtime = runtime
        self.ssh_port_range = ssh_port_range

    def list(self) -> list[SandboxRecord]:
        self.cleanup_expired()
        return list(self.store.load().values())

    def get(self, sandbox_id: str) -> SandboxRecord | None:
        self.cleanup_expired()
        return self.store.load().get(sandbox_id)

    def create(self, spec: SandboxSpec) -> tuple[SandboxRecord, CommandResult]:
        record, result, _timings = self.create_with_timings(spec)
        return record, result

    def create_with_timings(
        self,
        spec: SandboxSpec,
    ) -> tuple[SandboxRecord, CommandResult, dict[str, Any]]:
        started = time.monotonic()
        phases: dict[str, int] = {}
        phase = time.monotonic()
        self.cleanup_expired()
        phases["cleanup_expired_ms"] = _elapsed_ms(phase)
        phase = time.monotonic()
        records = self.store.load()
        phases["load_store_ms"] = _elapsed_ms(phase)
        if spec.id in records:
            raise ValueError(f"sandbox already exists: {spec.id}")
        phase = time.monotonic()
        spec = self._assign_ssh_port(spec, records)
        phases["assign_ssh_port_ms"] = _elapsed_ms(phase)
        phase = time.monotonic()
        spec.validate()
        phases["validate_spec_ms"] = _elapsed_ms(phase)
        phase = time.monotonic()
        result = self.runtime.create(spec)
        phases["docker_create_ms"] = _elapsed_ms(phase)
        phase = time.monotonic()
        now = utc_now()
        record = SandboxRecord(
            spec=spec,
            container_name=self.runtime.container_name(spec.id),
            state="planned" if self.runtime.dry_run else "running",
            created_at=now,
            updated_at=now,
        )
        self.store.upsert(record)
        phases["store_record_ms"] = _elapsed_ms(phase)
        return record, result, {
            "total_ms": _elapsed_ms(started),
            "phases": phases,
        }

    def delete(self, sandbox_id: str) -> tuple[SandboxRecord | None, CommandResult]:
        result = self.runtime.delete(sandbox_id)
        record = self.store.delete(sandbox_id)
        return record, result

    def active_count(self) -> int:
        return sum(1 for record in self.list() if record.state == "running")

    def requested_resources(self) -> ResourceQuantity:
        total = ResourceQuantity()
        for record in self.list():
            if record.state in {"running", "planned"}:
                total = total + record.spec.requested_resources()
        return total

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
    ) -> tuple[bytes, CommandResult]:
        self._require_sandbox(sandbox_id)
        validate_container_file_path("container_path", container_path)
        return self.runtime.read_file_from_container(sandbox_id, container_path)

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
        records = self.store.load()
        expired = [
            record
            for record in records.values()
            if record.state in {"running", "planned"} and record.is_expired(now)
        ]
        if not expired:
            return []
        for record in expired:
            self.runtime.delete(record.spec.id)
            records.pop(record.spec.id, None)
        self.store.save(records)
        return expired


def _format_float(value: float) -> str:
    if value.is_integer():
        return str(int(value))
    return str(value)


def _elapsed_ms(started: float) -> int:
    return max(0, int((time.monotonic() - started) * 1000))


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
