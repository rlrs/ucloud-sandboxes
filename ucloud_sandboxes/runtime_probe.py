from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
import fcntl
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
import stat
from threading import Lock
import time
from typing import Any

from .capabilities import GVISOR_LIVE_FORK_PROBE
from .sandbox import CommandExecutor, CommandResult, SubprocessExecutor
from .runsc_restore import (
    DEFAULT_RUNTIME_NAME as DEFAULT_FORK_RUNTIME_NAME,
    RESTORE_CHECKPOINT_ANNOTATION,
)


DEFAULT_CHECKPOINT_HELPER = "/usr/local/libexec/ucloud-sandbox-checkpoint"
DEFAULT_CHECKPOINT_ROOT = "/var/lib/ucloud-sandboxes/checkpoints"
_LIVE_FORK_CHECKPOINT_ID = "runtime-conformance-v1"
_LIVE_FORK_SPEC_HASH = "0" * 64
_LIVE_FORK_MEMORY_MB = 128
_LIVE_FORK_DISK_MB = 128
_LIVE_FORK_TMPFS_MB = 64
_LIVE_FORK_RUN_TMPFS_MB = 16
_LIVE_FORK_ARTIFACT_ID = "runtime-conformance-gvisor-live-fork-v1"
_LIVE_FORK_REPEAT_ARTIFACT_ID = f"{_LIVE_FORK_ARTIFACT_ID}-repeat"
_LIVE_FORK_SOURCE_NAME = "ucloud-fork-source-gvisor-live-fork-v1"
_LIVE_FORK_CHILD_NAME = "ucloud-fork-child-gvisor-live-fork-v1"
_LIVE_FORK_PEER_NAME = "ucloud-fork-peer-gvisor-live-fork-v1"
_LIVE_FORK_SOURCE_APPLICATION_ID = "probe-source-gvisor-live-fork-v1"
_LIVE_FORK_CHILD_APPLICATION_ID = "probe-child-gvisor-live-fork-v1"
_LIVE_FORK_BRIDGE_IP_FORMAT = (
    '{{with index .NetworkSettings.Networks "bridge"}}{{.IPAddress}}{{end}}'
)
_LIVE_FORK_EXEC_TAG = "ucloud-exec-origin-gvisor-live-fork-v1"
_LIVE_FORK_EXEC_LAUNCH = (
    "sh -c 'while :; do sleep 60; done' \"$UCLOUD_EXEC_TAG\" "
    "</dev/null >/dev/null 2>&1 &"
)
_LIVE_FORK_EXEC_SCAN = (
    "state=absent; "
    "for file in /proc/[0-9]*/cmdline; do "
    '[ -r "$file" ] || continue; '
    "command=\"$(tr '\\000' ' ' <\"$file\" 2>/dev/null || true)\"; "
    'case "$command" in *"$UCLOUD_EXEC_TAG"*) state=present; break;; esac; '
    "done; "
    'printf "UCLOUD_EXEC_ORIGIN=%s\\n" "$state"'
)
_LIVE_FORK_LOCK_PATH = "/tmp/.ucloud-sandboxes-gvisor-live-fork-v1.lock"
_LIVE_FORK_PROCESS_LOCK = Lock()
_LIVE_FORK_READY_RE = re.compile(
    r"^UCLOUD_FORK_READY=([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
    r"[0-9a-f]{4}-[0-9a-f]{12})\r?$",
    flags=re.MULTILINE,
)
_LIVE_FORK_RESTORED_RE = re.compile(
    r"^UCLOUD_FORK_RESTORED=([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
    r"[0-9a-f]{4}-[0-9a-f]{12})\r?$",
    flags=re.MULTILINE,
)
_LIVE_FORK_SOCKET_PORT = 45678
_LIVE_FORK_SOCKET_PORT_HEX = f"{_LIVE_FORK_SOCKET_PORT:04X}"
_LIVE_FORK_SOCKET_READY = "UCLOUD_FORK_SOCKET_READY=connected"
_LIVE_FORK_SOCKET_RESTORED = "UCLOUD_FORK_SOCKET_RESTORED=disconnected"
_LIVE_FORK_SOURCE_RESUME = "UCLOUD_FORK_CHECKPOINT_1=resume"
_LIVE_FORK_CHILD_RESTORE = "UCLOUD_FORK_CHECKPOINT_1=restore"
_LIVE_FORK_REPEAT_RESUME = "UCLOUD_FORK_CHECKPOINT_2=resume"
_LIVE_FORK_CHECKPOINT_ARMED = "UCLOUD_FORK_CHECKPOINT_ARMED_1=true"
_LIVE_FORK_REPEAT_ARMED = "UCLOUD_FORK_CHECKPOINT_ARMED_2=true"
_LIVE_FORK_ROOTFS_PATH = "/ucloud-fork-rootfs"
_LIVE_FORK_TMPFS_PATH = "/tmp/ucloud-fork-tmpfs"
_LIVE_FORK_RUN_TMPFS_PATH = "/run/ucloud-fork-run-tmpfs"
_LIVE_FORK_PROCESS = (
    'sentinel="$(cat /proc/sys/kernel/random/uuid)"; '
    'inherited_id="$UCLOUD_SANDBOX_ID"; '
    f'printf "%s" "$sentinel" >{_LIVE_FORK_TMPFS_PATH}; '
    f'printf "%s" "$sentinel" >{_LIVE_FORK_RUN_TMPFS_PATH}; '
    "exec 3</proc/gvisor/checkpoint; "
    "checkpoint_watch() { "
    "cycle=1; "
    'while [ "$cycle" -le 2 ]; do '
    'printf "UCLOUD_FORK_CHECKPOINT_ARMED_%s=true\\n" "$cycle"; '
    'outcome="$(cat <&3)"; '
    'printf "UCLOUD_FORK_CHECKPOINT_%s=%s\\n" "$cycle" "$outcome"; '
    "cycle=$((cycle + 1)); "
    'if [ "$cycle" -le 2 ]; then exec 3<&-; exec 3</proc/gvisor/checkpoint; fi; '
    "done; "
    "}; "
    "checkpoint_watch & "
    f'tail -f /dev/null | nc "$UCLOUD_FORK_PEER_IP" {_LIVE_FORK_SOCKET_PORT} '
    ">/dev/null 2>&1 & "
    "socket_ready=false; "
    "for attempt in 1 2 3 4 5 6 7 8 9 10; do "
    f"if grep -q ':{_LIVE_FORK_SOCKET_PORT_HEX} 01 ' /proc/net/tcp; "
    "then socket_ready=true; break; fi; sleep 0.1; done; "
    'if [ "$socket_ready" != true ]; then '
    'printf "UCLOUD_FORK_SOCKET_READY=failed\\n"; exit 42; fi; '
    f'printf "{_LIVE_FORK_SOCKET_READY}\\n"; '
    'printf "UCLOUD_FORK_READY=%s\\n" "$sentinel"; '
    "on_restore() { "
    "spec_id=\"$(tr '\\000' '\\n' </proc/gvisor/spec_environ | "
    "sed -n 's/^UCLOUD_SANDBOX_ID=//p' | head -n 1)\"; "
    "socket_state=disconnected; "
    f"if grep -q ':{_LIVE_FORK_SOCKET_PORT_HEX} 01 ' /proc/net/tcp; "
    "then socket_state=connected; fi; "
    'printf "UCLOUD_FORK_INHERITED_ID=%s\\n" "$inherited_id"; '
    'printf "UCLOUD_FORK_SPEC_ID=%s\\n" "$spec_id"; '
    'printf "UCLOUD_FORK_SOCKET_RESTORED=%s\\n" "$socket_state"; '
    f'printf "UCLOUD_FORK_ROOTFS_RESTORED=%s\\n" "$(cat {_LIVE_FORK_ROOTFS_PATH})"; '
    f'printf "UCLOUD_FORK_TMPFS_RESTORED=%s\\n" "$(cat {_LIVE_FORK_TMPFS_PATH})"; '
    f'printf "UCLOUD_FORK_RUN_TMPFS_RESTORED=%s\\n" "$(cat {_LIVE_FORK_RUN_TMPFS_PATH})"; '
    'printf "UCLOUD_FORK_RESTORED=%s\\n" "$sentinel"; '
    "}; "
    "trap on_restore USR1; "
    "sleep 0.1; "
    "while :; do sleep 1; done"
)


@dataclass(frozen=True)
class ProbeResult:
    name: str
    ok: bool
    command: tuple[str, ...]
    exit_code: int | None
    stdout: str = ""
    stderr: str = ""
    detail: str = ""
    skipped: bool = False
    required: bool = True
    runtime_fingerprint: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "ok": self.ok,
            "skipped": self.skipped,
            "required": self.required,
            "command": list(self.command),
            "exit_code": self.exit_code,
            "stdout": self.stdout,
            "stderr": self.stderr,
            "detail": self.detail,
            "runtime_fingerprint": self.runtime_fingerprint,
        }


@dataclass(frozen=True)
class RuntimeConformanceReport:
    docker_binary: str
    runtime_name: str
    image: str
    use_sudo: bool
    executed: bool
    results: tuple[ProbeResult, ...]

    @property
    def ok(self) -> bool:
        return all(
            not result.required or result.ok or result.skipped
            for result in self.results
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "docker_binary": self.docker_binary,
            "runtime_name": self.runtime_name,
            "image": self.image,
            "use_sudo": self.use_sudo,
            "executed": self.executed,
            "results": [result.to_dict() for result in self.results],
        }


class DockerRuntimeProbe:
    def __init__(
        self,
        *,
        executor: CommandExecutor | None = None,
        docker_binary: str = "docker",
        runtime_name: str = "runsc",
        fork_runtime_name: str = DEFAULT_FORK_RUNTIME_NAME,
        image: str = "busybox",
        use_sudo: bool = False,
        execute: bool = False,
        max_parallel_probes: int = 6,
        probe_live_fork: bool = False,
        checkpoint_helper: str = DEFAULT_CHECKPOINT_HELPER,
        checkpoint_root: str | Path = DEFAULT_CHECKPOINT_ROOT,
        live_fork_wait_seconds: float = 5.0,
    ) -> None:
        self.executor = executor or SubprocessExecutor()
        self.docker_binary = docker_binary
        self.runtime_name = runtime_name
        self.fork_runtime_name = fork_runtime_name
        self.image = image
        self.use_sudo = use_sudo
        self.execute = execute
        self.max_parallel_probes = max(1, int(max_parallel_probes))
        self.probe_live_fork = bool(probe_live_fork)
        self.checkpoint_helper = str(checkpoint_helper)
        self.checkpoint_root = Path(checkpoint_root)
        self.live_fork_wait_seconds = max(0.0, float(live_fork_wait_seconds))

    def run(self) -> RuntimeConformanceReport:
        # Run one container first. On a new VM this also performs Docker's
        # implicit pull of the small probe image, avoiding a thundering herd of
        # concurrent first-pull requests. Once the image and runsc runtime have
        # been proven, the independent conformance checks can run concurrently.
        first = self._probe_gvisor_kernel()
        remaining_probes = (
            self._probe_network_none_blocks,
            self._probe_memory_limit_visible,
            self._probe_mount_blocked,
            self._probe_non_root_supported,
            self._probe_storage_opt_quota_enforced,
            self._probe_tmpfs_quota_enforced,
        )
        if self.execute and self.max_parallel_probes > 1:
            with ThreadPoolExecutor(
                max_workers=min(self.max_parallel_probes, len(remaining_probes)),
                thread_name_prefix="runtime-conformance",
            ) as pool:
                remaining = tuple(pool.map(lambda probe: probe(), remaining_probes))
        else:
            remaining = tuple(probe() for probe in remaining_probes)
        optional = (self._probe_live_fork(),) if self.probe_live_fork else ()
        results = (first, *remaining, *optional)
        return RuntimeConformanceReport(
            docker_binary=self.docker_binary,
            runtime_name=self.runtime_name,
            image=self.image,
            use_sudo=self.use_sudo,
            executed=self.execute,
            results=results,
        )

    def _base_command(self, *extra: str) -> tuple[str, ...]:
        prefix = ("sudo",) if self.use_sudo else ()
        return prefix + (
            self.docker_binary,
            "run",
            "--rm",
            "--runtime",
            self.runtime_name,
            *extra,
            self.image,
        )

    def _docker_command(self, *extra: str) -> tuple[str, ...]:
        prefix = ("sudo",) if self.use_sudo else ()
        return prefix + (self.docker_binary, *extra)

    def _helper_command(self, *extra: str) -> tuple[str, ...]:
        prefix = ("sudo",) if self.use_sudo else ()
        return prefix + (self.checkpoint_helper, *extra)

    def _inspect_live_fork_runtime(self) -> tuple[str, CommandResult]:
        runtime_info = self.executor.run(
            self._docker_command(
                "info",
                "--format",
                "{{json .Runtimes}}",
            )
        )
        self._require_success(
            runtime_info,
            "could not inspect Docker runtime configuration",
        )
        try:
            configured_runtimes = json.loads(runtime_info.stdout)
            configured_runtime = configured_runtimes[self.runtime_name]
            runtime_path = str(configured_runtime["path"])
            runtime_args = configured_runtime.get("runtimeArgs", [])
            configured_fork_runtime = configured_runtimes[self.fork_runtime_name]
            fork_runtime_path = str(configured_fork_runtime["path"])
            fork_runtime_args = configured_fork_runtime.get("runtimeArgs", [])
        except (KeyError, TypeError, json.JSONDecodeError) as exc:
            raise _LiveForkProbeFailure(
                runtime_info,
                "Docker did not report the configured runsc runtime",
            ) from exc
        required_runtime_args = {
            "--allow-live-tcp-migration=false",
            "--net-disconnect-ok=true",
            "--allow-connected-on-save=false",
        }
        if (
            not runtime_path
            or not fork_runtime_path
            or not isinstance(runtime_args, list)
            or not isinstance(fork_runtime_args, list)
            or not required_runtime_args.issubset({str(item) for item in runtime_args})
            or {str(item) for item in fork_runtime_args}
            != {str(item) for item in runtime_args}
        ):
            raise _LiveForkProbeFailure(
                runtime_info,
                "runsc must explicitly disconnect live sockets before fork",
            )
        docker_version = self.executor.run(
            self._docker_command(
                "version",
                "--format",
                "{{.Server.Version}}",
            )
        )
        self._require_success(docker_version, "could not inspect Docker server version")
        runtime_version = self.executor.run((runtime_path, "--version"))
        self._require_success(runtime_version, "could not inspect runsc version")
        wrapper_version = self.executor.run(
            (fork_runtime_path, "--ucloud-wrapper-version")
        )
        self._require_success(
            wrapper_version,
            "could not inspect the raw runsc restore wrapper version",
        )
        identity = {
            "docker_server_version": docker_version.stdout.strip(),
            "runtime_name": self.runtime_name,
            "runtime_path": runtime_path,
            "runtime_args": [str(item) for item in runtime_args],
            "runtime_version": runtime_version.stdout.strip(),
            "fork_runtime_name": self.fork_runtime_name,
            "fork_runtime_path": fork_runtime_path,
            "fork_runtime_args": [str(item) for item in fork_runtime_args],
            "fork_runtime_version": wrapper_version.stdout.strip(),
        }
        fingerprint = hashlib.sha256(
            json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        return fingerprint, runtime_info

    def live_fork_runtime_fingerprint(self) -> str:
        """Return the current daemon/runsc identity or fail closed."""

        try:
            fingerprint, _result = self._inspect_live_fork_runtime()
        except _LiveForkProbeFailure as exc:
            raise RuntimeError(exc.detail) from exc
        return fingerprint

    @staticmethod
    def _acquire_live_fork_probe_lock() -> int:
        """Fail closed when another process is using the fixed probe artifacts."""

        if not _LIVE_FORK_PROCESS_LOCK.acquire(blocking=False):
            raise RuntimeError("another live fork conformance probe is already running")
        descriptor = -1
        try:
            descriptor = os.open(
                _LIVE_FORK_LOCK_PATH,
                os.O_RDWR
                | os.O_CREAT
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0),
                0o600,
            )
            info = os.fstat(descriptor)
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_uid != os.geteuid()
                or info.st_mode & 0o077
            ):
                raise RuntimeError("live fork probe lock is not a private regular file")
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return descriptor
        except BlockingIOError as exc:
            if descriptor >= 0:
                os.close(descriptor)
            _LIVE_FORK_PROCESS_LOCK.release()
            raise RuntimeError(
                "another live fork conformance probe is already running"
            ) from exc
        except BaseException:
            if descriptor >= 0:
                os.close(descriptor)
            _LIVE_FORK_PROCESS_LOCK.release()
            raise

    @staticmethod
    def _release_live_fork_probe_lock(descriptor: int) -> None:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)
            _LIVE_FORK_PROCESS_LOCK.release()

    def _run(self, name: str, argv: tuple[str, ...]) -> CommandResult | ProbeResult:
        if not self.execute:
            return ProbeResult(
                name=name,
                ok=True,
                skipped=True,
                command=argv,
                exit_code=None,
                detail="dry-run; re-run with --execute to run this probe",
            )
        return self.executor.run(argv)

    def _probe_gvisor_kernel(self) -> ProbeResult:
        name = "gvisor-kernel"
        argv = self._base_command("--network", "none") + ("uname", "-a")
        result = self._run(name, argv)
        if isinstance(result, ProbeResult):
            return result
        ok = result.exit_code == 0 and "gvisor" in result.stdout.lower()
        return self._result(
            name,
            result,
            ok,
            "container uname should report the gVisor kernel",
        )

    def _probe_network_none_blocks(self) -> ProbeResult:
        name = "network-none-blocks-outbound"
        argv = self._base_command("--network", "none") + (
            "wget",
            "-T",
            "2",
            "-O",
            "-",
            "http://1.1.1.1",
        )
        result = self._run(name, argv)
        if isinstance(result, ProbeResult):
            return result
        ok = result.exit_code != 0
        return self._result(
            name,
            result,
            ok,
            "network=none should block outbound traffic",
        )

    def _probe_memory_limit_visible(self) -> ProbeResult:
        name = "memory-limit-visible"
        argv = self._base_command("--network", "none", "--memory", "128m") + (
            "cat",
            "/proc/meminfo",
        )
        result = self._run(name, argv)
        if isinstance(result, ProbeResult):
            return result
        mem_total = _parse_memtotal_kb(result.stdout)
        ok = result.exit_code == 0 and mem_total is not None and mem_total <= 192 * 1024
        detail = (
            f"MemTotal={mem_total}kB; expected <= 196608kB"
            if mem_total is not None
            else "MemTotal not found"
        )
        return self._result(name, result, ok, detail)

    def _probe_mount_blocked(self) -> ProbeResult:
        name = "mount-blocked"
        argv = self._base_command("--network", "none") + (
            "mount",
            "-t",
            "tmpfs",
            "tmpfs",
            "/tmp",
        )
        result = self._run(name, argv)
        if isinstance(result, ProbeResult):
            return result
        ok = result.exit_code != 0
        return self._result(
            name,
            result,
            ok,
            "default sandbox root should not be able to mount filesystems",
        )

    def _probe_non_root_supported(self) -> ProbeResult:
        name = "non-root-supported"
        argv = self._base_command("--network", "none", "--user", "1000:1000") + ("id",)
        result = self._run(name, argv)
        if isinstance(result, ProbeResult):
            return result
        ok = result.exit_code == 0 and "uid=1000" in result.stdout
        return self._result(
            name,
            result,
            ok,
            "sandbox should support running as a numeric non-root user",
        )

    def _probe_tmpfs_quota_enforced(self) -> ProbeResult:
        name = "tmpfs-quota-enforced"
        argv = self._base_command(
            "--network",
            "none",
            "--tmpfs",
            "/tmp:rw,nosuid,nodev,size=16m",
        ) + (
            "sh",
            "-c",
            (
                "size=$(df -k /tmp | awk 'NR==2 {print $2}'); "
                'if [ -z "$size" ] || [ "$size" -gt 20480 ]; then '
                "echo tmpfs-not-isolated; exit 0; fi; "
                "dd if=/dev/zero of=/tmp/ucloud-tmpfs-probe bs=1M count=32; "
                "status=$?; rm -f /tmp/ucloud-tmpfs-probe; exit $status"
            ),
        )
        result = self._run(name, argv)
        if isinstance(result, ProbeResult):
            return result
        ok = result.exit_code != 0 and _looks_like_no_space(
            result.stdout, result.stderr
        )
        return self._result(
            name,
            result,
            ok,
            "writing 32MB should fail in a 16MB tmpfs",
        )

    def _probe_storage_opt_quota_enforced(self) -> ProbeResult:
        name = "storage-opt-quota-enforced"
        argv = self._base_command(
            "--network",
            "none",
            "--storage-opt",
            "size=16m",
        ) + (
            "sh",
            "-c",
            (
                "dd if=/dev/zero of=/ucloud-storage-probe bs=1M count=32; "
                "status=$?; rm -f /ucloud-storage-probe; exit $status"
            ),
        )
        result = self._run(name, argv)
        if isinstance(result, ProbeResult):
            return result
        ok = result.exit_code != 0 and _looks_like_no_space(
            result.stdout, result.stderr
        )
        return self._result(
            name,
            result,
            ok,
            "writing 32MB should fail with --storage-opt size=16m",
        )

    def _probe_live_fork(self) -> ProbeResult:
        """Prove raw runsc restore under a distinct Docker-owned container.

        This is deliberately optional: checkpoint support is an additional node
        capability, not a prerequisite for ordinary sandbox execution.  The
        sentinel is generated by the initial workload after it starts and
        emitted once so the probe can learn it. It is not present in the OCI
        spec or filesystem;
        seeing the same value after the child is signalled therefore proves
        process memory was restored instead of merely starting an equivalent
        container spec.  The source also holds a TCP session to a separate
        peer container so the probe can prove that restore disconnects
        external sockets without rejecting snapshot-local loopback state. The
        workload also reads the child's identity from /proc/gvisor/spec_environ
        after restore.
        """

        name = GVISOR_LIVE_FORK_PROBE
        artifact_id = _LIVE_FORK_ARTIFACT_ID
        repeat_artifact_id = _LIVE_FORK_REPEAT_ARTIFACT_ID
        source_name = _LIVE_FORK_SOURCE_NAME
        child_name = _LIVE_FORK_CHILD_NAME
        peer_name = _LIVE_FORK_PEER_NAME
        source_application_id = _LIVE_FORK_SOURCE_APPLICATION_ID
        child_application_id = _LIVE_FORK_CHILD_APPLICATION_ID
        checkpoint_dir = self.checkpoint_root / artifact_id / "pending"
        dry_run_command = self._helper_command(
            "prepare",
            artifact_id,
            "<source-container-id>",
            "<source-image-sha256>",
            _LIVE_FORK_SPEC_HASH,
            _LIVE_FORK_CHECKPOINT_ID,
            str(_LIVE_FORK_MEMORY_MB),
            str(_LIVE_FORK_DISK_MB),
            str(_LIVE_FORK_TMPFS_MB),
            str(_LIVE_FORK_RUN_TMPFS_MB),
        )
        if not self.execute:
            return ProbeResult(
                name=name,
                ok=True,
                skipped=True,
                required=False,
                command=dry_run_command,
                exit_code=None,
                detail=(
                    "dry-run; live fork probe creates a source and child, "
                    "checkpoints with --leave-running, stages through the "
                    "checkpoint helper, and verifies restored process memory"
                ),
            )

        source_id = ""
        child_id = ""
        prepare_attempted = False
        repeat_prepare_attempted = False
        stage_attempted = False
        source_application_prepared = False
        child_application_prepared = False
        runtime_fingerprint = ""
        lock_descriptor: int | None = None
        outcome: ProbeResult | None = None
        cleanup_failures: list[tuple[CommandResult, str]] = []
        last_command = dry_run_command

        try:
            lock_descriptor = self._acquire_live_fork_probe_lock()

            # Fence any daemon-side restore/checkpoint work before touching
            # the staged files it may still be reading or writing.
            for stale_container_name, role in (
                (child_name, "child"),
                (source_name, "source"),
                (peer_name, "socket peer"),
            ):
                stale_remove = self.executor.run(
                    self._docker_command(
                        "rm",
                        "--force",
                        "--volumes",
                        stale_container_name,
                    )
                )
                last_command = stale_remove.argv
                if stale_remove.exit_code != 0 and not _container_is_missing(
                    stale_remove
                ):
                    raise _LiveForkProbeFailure(
                        stale_remove,
                        f"could not remove stale live fork {role}",
                    )

            helper_state = self.executor.run(self._helper_command("list"))
            last_command = helper_state.argv
            self._require_success(
                helper_state,
                "checkpoint helper could not inspect stale probe state",
            )
            try:
                staged_targets = _fixed_probe_staged_targets(helper_state.stdout)
            except ValueError as exc:
                raise _LiveForkProbeFailure(
                    helper_state,
                    "checkpoint helper returned invalid stale probe state",
                ) from exc
            for target_container_id in staged_targets:
                stale_unstage = self.executor.run(
                    self._helper_command(
                        "unstage",
                        target_container_id,
                        _LIVE_FORK_CHECKPOINT_ID,
                    )
                )
                last_command = stale_unstage.argv
                self._require_success(
                    stale_unstage,
                    "checkpoint helper could not unstage the stale probe child",
                )

            for stale_application_id, role in (
                (child_application_id, "child"),
                (source_application_id, "source"),
            ):
                stale_app_drop = self.executor.run(
                    self._helper_command("app-drop", stale_application_id)
                )
                last_command = stale_app_drop.argv
                self._require_success(
                    stale_app_drop,
                    f"checkpoint helper could not drop the stale {role} application path",
                )

            for stale_artifact_id in (artifact_id, repeat_artifact_id):
                stale_drop = self.executor.run(
                    self._helper_command("drop", stale_artifact_id)
                )
                last_command = stale_drop.argv
                self._require_success(
                    stale_drop,
                    "checkpoint helper could not reset the fixed probe artifact",
                )

            runtime_fingerprint, runtime_info = self._inspect_live_fork_runtime()
            last_command = runtime_info.argv

            image_result = self.executor.run(
                self._docker_command(
                    "image",
                    "inspect",
                    "--format",
                    "{{.Id}}",
                    self.image,
                )
            )
            last_command = image_result.argv
            self._require_success(image_result, "could not inspect probe image")
            image_id = _parse_image_sha256(image_result.stdout)
            if image_id is None:
                raise _LiveForkProbeFailure(
                    image_result,
                    "probe image id must be a sha256 digest",
                )

            peer_result = self.executor.run(
                self._docker_command(
                    "run",
                    "--detach",
                    "--network",
                    "bridge",
                    "--name",
                    peer_name,
                    image_id,
                    "sh",
                    "-c",
                    (
                        f"tail -f /dev/null | nc -l -p {_LIVE_FORK_SOCKET_PORT} "
                        ">/dev/null 2>&1"
                    ),
                )
            )
            last_command = peer_result.argv
            self._require_success(
                peer_result,
                "could not start the external TCP peer used by the fork probe",
            )
            peer_network = self.executor.run(
                self._docker_command(
                    "inspect",
                    "--format",
                    _LIVE_FORK_BRIDGE_IP_FORMAT,
                    peer_name,
                )
            )
            last_command = peer_network.argv
            self._require_success(
                peer_network,
                "could not inspect the fork probe TCP peer bridge endpoint",
            )
            peer_bridge_ip = _parse_bridge_ipv4(peer_network.stdout)
            if peer_bridge_ip is None:
                raise _LiveForkProbeFailure(
                    peer_network,
                    "fork probe TCP peer has no valid Docker bridge address",
                )

            source_application = self.executor.run(
                self._helper_command("app-prepare", source_application_id)
            )
            last_command = source_application.argv
            self._require_success(
                source_application,
                "checkpoint helper could not prepare the source application path",
            )
            source_application_prepared = True
            source_result = self.executor.run(
                self._live_fork_container_command(
                    "run",
                    source_name,
                    image_id,
                    source_application_id,
                    str(peer_bridge_ip),
                )
            )
            last_command = source_result.argv
            self._require_success(source_result, "could not start source container")
            source_id = _parse_container_id(source_result.stdout) or ""
            if not source_id:
                raise _LiveForkProbeFailure(
                    source_result,
                    "source container did not return a 64-hex container id",
                )
            source_network = self.executor.run(
                self._docker_command(
                    "inspect",
                    "--format",
                    _LIVE_FORK_BRIDGE_IP_FORMAT,
                    source_name,
                )
            )
            last_command = source_network.argv
            self._require_success(
                source_network,
                "could not inspect the source bridge endpoint",
            )
            source_bridge_ip = _parse_bridge_ipv4(source_network.stdout)
            if source_bridge_ip is None:
                raise _LiveForkProbeFailure(
                    source_network,
                    "source container has no valid Docker bridge address",
                )

            source_logs, ready_match = self._wait_for_container_log(
                source_name,
                _LIVE_FORK_READY_RE,
            )
            last_command = source_logs.argv
            if ready_match is None:
                raise _LiveForkProbeFailure(
                    source_logs,
                    "source workload did not publish its in-memory sentinel",
                )
            sentinel = ready_match.group(1)
            if not _has_log_line(source_logs.stdout, _LIVE_FORK_CHECKPOINT_ARMED):
                raise _LiveForkProbeFailure(
                    source_logs,
                    "source workload did not arm passive checkpoint observation",
                )
            if not _has_log_line(source_logs.stdout, _LIVE_FORK_SOCKET_READY):
                raise _LiveForkProbeFailure(
                    source_logs,
                    "source did not establish the TCP session used to test teardown",
                )

            rootfs_setup = self.executor.run(
                self._docker_command(
                    "exec",
                    "--user",
                    "0",
                    source_name,
                    "sh",
                    "-c",
                    f"printf {sentinel} >{_LIVE_FORK_ROOTFS_PATH}",
                )
            )
            last_command = rootfs_setup.argv
            self._require_success(
                rootfs_setup,
                "could not initialize mutable rootfs state as the manager user",
            )

            exec_daemon = self.executor.run(
                self._live_fork_exec_origin_command(
                    source_name,
                    _LIVE_FORK_EXEC_LAUNCH,
                )
            )
            last_command = exec_daemon.argv
            self._require_success(
                exec_daemon,
                "could not launch the detached exec-origin probe process",
            )
            exec_before_save = self.executor.run(
                self._live_fork_exec_origin_command(
                    source_name,
                    _LIVE_FORK_EXEC_SCAN,
                )
            )
            last_command = exec_before_save.argv
            self._require_success(
                exec_before_save,
                "could not inspect the detached exec-origin probe process",
            )
            if (
                _read_log_value(exec_before_save.stdout, "UCLOUD_EXEC_ORIGIN")
                != "present"
            ):
                raise _LiveForkProbeFailure(
                    exec_before_save,
                    "detached exec-origin descendant was not alive before checkpoint",
                )

            prepare_result = self.executor.run(
                self._helper_command(
                    "prepare",
                    artifact_id,
                    source_id,
                    image_id,
                    _LIVE_FORK_SPEC_HASH,
                    _LIVE_FORK_CHECKPOINT_ID,
                    str(_LIVE_FORK_MEMORY_MB),
                    str(_LIVE_FORK_DISK_MB),
                    str(_LIVE_FORK_TMPFS_MB),
                    str(_LIVE_FORK_RUN_TMPFS_MB),
                )
            )
            last_command = prepare_result.argv
            self._require_success(
                prepare_result,
                "checkpoint helper could not prepare the artifact",
            )
            prepare_attempted = True

            checkpoint_result = self.executor.run(
                self._docker_command(
                    "checkpoint",
                    "create",
                    "--checkpoint-dir",
                    str(checkpoint_dir),
                    "--leave-running",
                    source_name,
                    _LIVE_FORK_CHECKPOINT_ID,
                )
            )
            last_command = checkpoint_result.argv
            self._require_success(
                checkpoint_result,
                "runsc could not checkpoint the source with --leave-running",
            )

            complete_result = self.executor.run(
                self._helper_command("complete", artifact_id)
            )
            last_command = complete_result.argv
            self._require_success(
                complete_result,
                "checkpoint helper could not record Docker completion",
            )

            seal_result = self.executor.run(self._helper_command("seal", artifact_id))
            last_command = seal_result.argv
            self._require_success(
                seal_result,
                "checkpoint helper could not seal the artifact",
            )

            source_resumed, source_resume_match = self._wait_for_container_log(
                source_name,
                re.compile(
                    rf"^{re.escape(_LIVE_FORK_SOURCE_RESUME)}\r?$", re.MULTILINE
                ),
            )
            last_command = source_resumed.argv
            if source_resume_match is None:
                raise _LiveForkProbeFailure(
                    source_resumed,
                    "source workload did not observe passive checkpoint resume",
                )
            source_exec_resumed = self.executor.run(
                self._live_fork_exec_origin_command(
                    source_name,
                    _LIVE_FORK_EXEC_SCAN,
                )
            )
            last_command = source_exec_resumed.argv
            self._require_success(
                source_exec_resumed,
                "could not inspect the resumed source exec-origin process",
            )
            if (
                _read_log_value(source_exec_resumed.stdout, "UCLOUD_EXEC_ORIGIN")
                != "present"
            ):
                raise _LiveForkProbeFailure(
                    source_exec_resumed,
                    "detached exec-origin descendant did not remain in the source",
                )
            source_rootfs_mutation = self.executor.run(
                self._docker_command(
                    "exec",
                    "--user",
                    "0",
                    source_name,
                    "sh",
                    "-c",
                    f"printf source-only >{_LIVE_FORK_ROOTFS_PATH}",
                )
            )
            last_command = source_rootfs_mutation.argv
            self._require_success(
                source_rootfs_mutation,
                "could not mutate the source rootfs after checkpoint",
            )
            source_tmpfs_mutation = self.executor.run(
                self._docker_command(
                    "exec",
                    "--user",
                    "1000:1000",
                    source_name,
                    "sh",
                    "-c",
                    (
                        f"printf source-only >{_LIVE_FORK_TMPFS_PATH}; "
                        f"printf source-only >{_LIVE_FORK_RUN_TMPFS_PATH}"
                    ),
                )
            )
            last_command = source_tmpfs_mutation.argv
            self._require_success(
                source_tmpfs_mutation,
                "could not mutate the source tmpfs files after checkpoint",
            )

            child_application = self.executor.run(
                self._helper_command("app-prepare", child_application_id)
            )
            last_command = child_application.argv
            self._require_success(
                child_application,
                "checkpoint helper could not prepare the child application path",
            )
            child_application_prepared = True
            child_result = self.executor.run(
                self._live_fork_container_command(
                    "create",
                    child_name,
                    image_id,
                    child_application_id,
                    str(peer_bridge_ip),
                )
            )
            last_command = child_result.argv
            self._require_success(child_result, "could not create child container")
            child_id = _parse_container_id(child_result.stdout) or ""
            if not child_id:
                raise _LiveForkProbeFailure(
                    child_result,
                    "child container did not return a 64-hex container id",
                )
            if child_id == source_id:
                raise _LiveForkProbeFailure(
                    child_result,
                    "source and child must have distinct container ids",
                )

            stage_attempted = True
            stage_result = self.executor.run(
                self._helper_command(
                    "stage",
                    artifact_id,
                    child_id,
                    _LIVE_FORK_CHECKPOINT_ID,
                )
            )
            last_command = stage_result.argv
            self._require_success(
                stage_result,
                "checkpoint helper could not stage the child checkpoint",
            )

            start_result = self.executor.run(
                self._docker_command("start", child_name)
            )
            last_command = start_result.argv
            self._require_success(
                start_result,
                "the raw runsc restore wrapper could not start the child",
            )

            child_network = self.executor.run(
                self._docker_command(
                    "inspect",
                    "--format",
                    _LIVE_FORK_BRIDGE_IP_FORMAT,
                    child_name,
                )
            )
            last_command = child_network.argv
            self._require_success(
                child_network,
                "could not inspect the restored child bridge endpoint",
            )
            child_bridge_ip = _parse_bridge_ipv4(child_network.stdout)
            if child_bridge_ip is None or child_bridge_ip == source_bridge_ip:
                raise _LiveForkProbeFailure(
                    child_network,
                    "restored child did not receive a distinct Docker bridge address",
                )
            child_network_inside = self.executor.run(
                self._docker_command("exec", child_name, "hostname", "-i")
            )
            last_command = child_network_inside.argv
            self._require_success(
                child_network_inside,
                "could not inspect bridge identity inside the restored child",
            )
            if not _reports_bridge_ipv4(
                child_network_inside.stdout,
                child_bridge_ip,
            ):
                raise _LiveForkProbeFailure(
                    child_network_inside,
                    "restored child netstack did not adopt its Docker bridge address",
                )

            child_exec_restored = self.executor.run(
                self._live_fork_exec_origin_command(
                    child_name,
                    _LIVE_FORK_EXEC_SCAN,
                )
            )
            last_command = child_exec_restored.argv
            self._require_success(
                child_exec_restored,
                "could not inspect restored child exec-origin processes",
            )
            if (
                _read_log_value(child_exec_restored.stdout, "UCLOUD_EXEC_ORIGIN")
                != "absent"
            ):
                raise _LiveForkProbeFailure(
                    child_exec_restored,
                    "restored child retained a detached OriginExec descendant",
                )

            for role, container_name in (
                ("source", source_name),
                ("child", child_name),
            ):
                uname_result = self.executor.run(
                    self._docker_command("exec", container_name, "uname", "-a")
                )
                last_command = uname_result.argv
                if (
                    uname_result.exit_code != 0
                    or "gvisor" not in uname_result.stdout.lower()
                ):
                    raise _LiveForkProbeFailure(
                        uname_result,
                        f"{role} container is not runnable under the gVisor kernel",
                    )

            signal_result = self.executor.run(
                self._docker_command("kill", "--signal", "USR1", child_name)
            )
            last_command = signal_result.argv
            self._require_success(
                signal_result,
                "could not signal the restored child workload",
            )
            child_logs, restored_match = self._wait_for_container_log(
                child_name,
                _LIVE_FORK_RESTORED_RE,
            )
            last_command = child_logs.argv
            if restored_match is None:
                raise _LiveForkProbeFailure(
                    child_logs,
                    "restored child did not publish its in-memory sentinel",
                )
            restored_sentinel = restored_match.group(1)
            if restored_sentinel != sentinel:
                raise _LiveForkProbeFailure(
                    child_logs,
                    "restored child sentinel did not match source process memory",
                )
            inherited_id = _read_log_value(
                child_logs.stdout,
                "UCLOUD_FORK_INHERITED_ID",
            )
            spec_id = _read_log_value(child_logs.stdout, "UCLOUD_FORK_SPEC_ID")
            if inherited_id != source_name or spec_id != child_name:
                raise _LiveForkProbeFailure(
                    child_logs,
                    "restored process did not observe the child identity through "
                    "/proc/gvisor/spec_environ",
                )
            if not _has_log_line(child_logs.stdout, _LIVE_FORK_SOCKET_RESTORED):
                raise _LiveForkProbeFailure(
                    child_logs,
                    "restored child retained the source's external TCP session",
                )
            if not _has_log_line(child_logs.stdout, _LIVE_FORK_CHILD_RESTORE):
                raise _LiveForkProbeFailure(
                    child_logs,
                    "restored child did not observe passive checkpoint restore",
                )
            for key in (
                "UCLOUD_FORK_ROOTFS_RESTORED",
                "UCLOUD_FORK_TMPFS_RESTORED",
                "UCLOUD_FORK_RUN_TMPFS_RESTORED",
            ):
                if _read_log_value(child_logs.stdout, key) != sentinel:
                    raise _LiveForkProbeFailure(
                        child_logs,
                        "restored child did not retain isolated rootfs/tmpfs state",
                    )

            # A runsc --leave-running checkpoint restores the source and may
            # change its host PID.  Prove Docker's shim can checkpoint that
            # logical source again; one successful fork is insufficient for a
            # reusable agent-branching primitive.
            repeat_armed, repeat_armed_match = self._wait_for_container_log(
                source_name,
                re.compile(rf"^{re.escape(_LIVE_FORK_REPEAT_ARMED)}\r?$", re.MULTILINE),
            )
            last_command = repeat_armed.argv
            if repeat_armed_match is None:
                raise _LiveForkProbeFailure(
                    repeat_armed,
                    "source workload did not re-arm passive checkpoint observation",
                )
            repeat_prepare = self.executor.run(
                self._helper_command(
                    "prepare",
                    repeat_artifact_id,
                    source_id,
                    image_id,
                    _LIVE_FORK_SPEC_HASH,
                    _LIVE_FORK_CHECKPOINT_ID,
                    str(_LIVE_FORK_MEMORY_MB),
                    str(_LIVE_FORK_DISK_MB),
                    str(_LIVE_FORK_TMPFS_MB),
                    str(_LIVE_FORK_RUN_TMPFS_MB),
                )
            )
            last_command = repeat_prepare.argv
            self._require_success(
                repeat_prepare,
                "checkpoint helper could not prepare the repeat artifact",
            )
            repeat_prepare_attempted = True
            repeat_checkpoint = self.executor.run(
                self._docker_command(
                    "checkpoint",
                    "create",
                    "--checkpoint-dir",
                    str(self.checkpoint_root / repeat_artifact_id / "pending"),
                    "--leave-running",
                    source_name,
                    _LIVE_FORK_CHECKPOINT_ID,
                )
            )
            last_command = repeat_checkpoint.argv
            self._require_success(
                repeat_checkpoint,
                "source could not be checkpointed again after resuming",
            )
            repeat_complete = self.executor.run(
                self._helper_command("complete", repeat_artifact_id)
            )
            last_command = repeat_complete.argv
            self._require_success(
                repeat_complete,
                "checkpoint helper could not record repeat Docker completion",
            )
            repeat_seal = self.executor.run(
                self._helper_command("seal", repeat_artifact_id)
            )
            last_command = repeat_seal.argv
            self._require_success(
                repeat_seal,
                "checkpoint helper could not seal the repeat artifact",
            )
            repeat_resumed, repeat_resume_match = self._wait_for_container_log(
                source_name,
                re.compile(
                    rf"^{re.escape(_LIVE_FORK_REPEAT_RESUME)}\r?$", re.MULTILINE
                ),
            )
            last_command = repeat_resumed.argv
            if repeat_resume_match is None:
                raise _LiveForkProbeFailure(
                    repeat_resumed,
                    "source workload did not observe the repeated checkpoint resume",
                )
            source_after_repeat = self.executor.run(
                self._docker_command("exec", source_name, "uname", "-a")
            )
            last_command = source_after_repeat.argv
            if (
                source_after_repeat.exit_code != 0
                or "gvisor" not in source_after_repeat.stdout.lower()
            ):
                raise _LiveForkProbeFailure(
                    source_after_repeat,
                    "source was not runnable after a repeated checkpoint",
                )

            outcome = ProbeResult(
                name=name,
                ok=True,
                required=False,
                command=child_logs.argv,
                exit_code=child_logs.exit_code,
                stdout=child_logs.stdout,
                stderr=child_logs.stderr,
                detail=(
                    "source remained runnable and a distinct gVisor child resumed "
                    "the source workload's in-memory sentinel, passive checkpoint "
                    "notification, isolated rootfs/tmpfs state, and child identity "
                    "through /proc/gvisor/spec_environ and a distinct Docker bridge "
                    "endpoint adopted inside its netstack while its external TCP "
                    "session was disconnected "
                    "and detached OriginExec descendants remained source-only; the "
                    "resumed source also passed a second passively observed checkpoint"
                ),
                runtime_fingerprint=runtime_fingerprint,
            )
        except _LiveForkProbeFailure as exc:
            outcome = ProbeResult(
                name=name,
                ok=False,
                required=False,
                command=exc.result.argv,
                exit_code=exc.result.exit_code,
                stdout=exc.result.stdout,
                stderr=exc.result.stderr,
                detail=exc.detail,
            )
        except Exception as exc:
            # This probe is optional and must never prevent the required runtime
            # checks from producing a conformance report.
            outcome = ProbeResult(
                name=name,
                ok=False,
                required=False,
                command=last_command,
                exit_code=None,
                stderr=str(exc),
                detail="live fork probe raised an unexpected error",
            )
        finally:
            checkpoint_cleanup_safe = lock_descriptor is not None
            if lock_descriptor is not None and stage_attempted and child_id:
                checkpoint_cleanup_safe = self._cleanup_step(
                    self._helper_command(
                        "unstage",
                        child_id,
                        _LIVE_FORK_CHECKPOINT_ID,
                    ),
                    "checkpoint helper could not unstage the child",
                    cleanup_failures,
                )
            if lock_descriptor is not None:
                child_stopped = self._cleanup_step(
                    self._docker_command("rm", "--force", "--volumes", child_name),
                    "could not remove live fork child",
                    cleanup_failures,
                    ignore_missing=True,
                )
                source_stopped = self._cleanup_step(
                    self._docker_command("rm", "--force", "--volumes", source_name),
                    "could not remove live fork source",
                    cleanup_failures,
                    ignore_missing=True,
                )
                peer_stopped = self._cleanup_step(
                    self._docker_command("rm", "--force", "--volumes", peer_name),
                    "could not remove live fork TCP peer",
                    cleanup_failures,
                    ignore_missing=True,
                )
                checkpoint_cleanup_safe = (
                    checkpoint_cleanup_safe
                    and child_stopped
                    and source_stopped
                    and peer_stopped
                )
            if checkpoint_cleanup_safe and child_application_prepared:
                self._cleanup_step(
                    self._helper_command("app-drop", child_application_id),
                    "checkpoint helper could not drop the child application path",
                    cleanup_failures,
                )
            if checkpoint_cleanup_safe and source_application_prepared:
                self._cleanup_step(
                    self._helper_command("app-drop", source_application_id),
                    "checkpoint helper could not drop the source application path",
                    cleanup_failures,
                )
            if checkpoint_cleanup_safe and prepare_attempted:
                self._cleanup_step(
                    self._helper_command("drop", artifact_id),
                    "checkpoint helper could not drop the artifact",
                    cleanup_failures,
                )
            if checkpoint_cleanup_safe and repeat_prepare_attempted:
                self._cleanup_step(
                    self._helper_command("drop", repeat_artifact_id),
                    "checkpoint helper could not drop the repeat artifact",
                    cleanup_failures,
                )
            if lock_descriptor is not None:
                self._release_live_fork_probe_lock(lock_descriptor)

        assert outcome is not None
        if cleanup_failures:
            failed_result, detail = cleanup_failures[0]
            cleanup_detail = "; ".join(item[1] for item in cleanup_failures)
            if outcome.ok:
                return ProbeResult(
                    name=name,
                    ok=False,
                    required=False,
                    command=failed_result.argv,
                    exit_code=failed_result.exit_code,
                    stdout=failed_result.stdout,
                    stderr=failed_result.stderr,
                    detail=cleanup_detail,
                )
            return ProbeResult(
                name=name,
                ok=False,
                required=False,
                command=outcome.command,
                exit_code=outcome.exit_code,
                stdout=outcome.stdout,
                stderr=outcome.stderr,
                detail=f"{outcome.detail}; cleanup: {cleanup_detail}",
            )
        return outcome

    def _live_fork_container_command(
        self,
        action: str,
        name: str,
        image: str,
        application_id: str,
        peer_ip: str,
    ) -> tuple[str, ...]:
        detach = ("--detach",) if action == "run" else ()
        application_path = self.checkpoint_root / "application" / application_id
        return self._docker_command(
            action,
            *detach,
            "--runtime",
            (
                self.fork_runtime_name
                if action == "create"
                else self.runtime_name
            ),
            "--network",
            "bridge",
            "--memory",
            f"{_LIVE_FORK_MEMORY_MB}m",
            "--storage-opt",
            f"size={_LIVE_FORK_DISK_MB}m",
            "--init",
            "--user",
            "1000:1000",
            "--security-opt",
            "no-new-privileges",
            "--cap-drop",
            "ALL",
            "--pids-limit",
            "256",
            "--tmpfs",
            f"/tmp:rw,nosuid,nodev,size={_LIVE_FORK_TMPFS_MB}m",
            "--tmpfs",
            f"/run:rw,nosuid,nodev,size={_LIVE_FORK_RUN_TMPFS_MB}m",
            "--annotation",
            f"dev.gvisor.internal.checkpoint.path={application_path}",
            "--annotation",
            "dev.gvisor.internal.checkpoint.resume=true",
            "--annotation",
            "dev.gvisor.internal.checkpoint.compression=none",
            *(
                (
                    "--annotation",
                    f"{RESTORE_CHECKPOINT_ANNOTATION}={_LIVE_FORK_CHECKPOINT_ID}",
                )
                if action == "create"
                else ()
            ),
            "--env",
            f"UCLOUD_SANDBOX_ID={name}",
            "--env",
            f"UCLOUD_FORK_PEER_IP={peer_ip}",
            "--name",
            name,
            image,
            "sh",
            "-c",
            _LIVE_FORK_PROCESS,
        )

    def _live_fork_exec_origin_command(
        self,
        container_name: str,
        script: str,
    ) -> tuple[str, ...]:
        return self._docker_command(
            "exec",
            "--env",
            f"UCLOUD_EXEC_TAG={_LIVE_FORK_EXEC_TAG}",
            container_name,
            "sh",
            "-c",
            script,
        )

    def _wait_for_container_log(
        self,
        container_name: str,
        pattern: re.Pattern[str],
    ) -> tuple[CommandResult, re.Match[str] | None]:
        deadline = time.monotonic() + self.live_fork_wait_seconds
        while True:
            result = self.executor.run(self._docker_command("logs", container_name))
            if result.exit_code == 0:
                match = pattern.search(result.stdout)
                if match is not None:
                    return result, match
            if time.monotonic() >= deadline:
                return result, None
            time.sleep(min(0.1, max(0.0, deadline - time.monotonic())))

    @staticmethod
    def _require_success(result: CommandResult, detail: str) -> None:
        if result.exit_code != 0:
            raise _LiveForkProbeFailure(result, detail)

    def _cleanup_step(
        self,
        argv: tuple[str, ...],
        detail: str,
        failures: list[tuple[CommandResult, str]],
        *,
        ignore_missing: bool = False,
    ) -> bool:
        try:
            result = self.executor.run(argv)
        except Exception as exc:
            failures.append((CommandResult(argv, -1, stderr=str(exc)), detail))
            return False
        if result.exit_code == 0:
            return True
        if ignore_missing and _container_is_missing(result):
            return True
        failures.append((result, detail))
        return False

    @staticmethod
    def _result(
        name: str,
        result: CommandResult,
        ok: bool,
        detail: str,
    ) -> ProbeResult:
        return ProbeResult(
            name=name,
            ok=ok,
            command=result.argv,
            exit_code=result.exit_code,
            stdout=result.stdout,
            stderr=result.stderr,
            detail=detail,
        )


def _parse_memtotal_kb(raw: str) -> int | None:
    match = re.search(r"^MemTotal:\s+(\d+)\s+kB", raw, flags=re.MULTILINE)
    return int(match.group(1)) if match else None


def _looks_like_no_space(stdout: str, stderr: str) -> bool:
    output = f"{stdout}\n{stderr}".lower()
    return "no space left" in output or "enospc" in output


def _parse_container_id(raw: str) -> str | None:
    value = raw.strip().splitlines()[-1].strip() if raw.strip() else ""
    return value if re.fullmatch(r"[0-9a-f]{64}", value) else None


def _parse_bridge_ipv4(raw: str) -> str | None:
    value = raw.strip()
    try:
        address = ipaddress.ip_address(value)
    except ValueError:
        return None
    if not isinstance(address, ipaddress.IPv4Address):
        return None
    if address.is_loopback or address.is_unspecified or address.is_link_local:
        return None
    return str(address)


def _reports_bridge_ipv4(raw: str, expected: str) -> bool:
    return expected in {
        address
        for token in raw.split()
        if (address := _parse_bridge_ipv4(token)) is not None
    }


def _container_is_missing(result: CommandResult) -> bool:
    output = f"{result.stdout}\n{result.stderr}".lower()
    return "no such container" in output


def _fixed_probe_staged_targets(raw: str) -> tuple[str, ...]:
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError("checkpoint helper list is not JSON") from exc
    if not isinstance(payload, dict) or not isinstance(payload.get("staged"), list):
        raise ValueError("checkpoint helper list has an invalid schema")
    targets: list[str] = []
    for item in payload["staged"]:
        if not isinstance(item, dict):
            raise ValueError("checkpoint helper staged entry is invalid")
        if (
            item.get("artifact_id") != _LIVE_FORK_ARTIFACT_ID
            or item.get("checkpoint_id") != _LIVE_FORK_CHECKPOINT_ID
        ):
            continue
        target_container_id = str(item.get("target_container_id") or "")
        if re.fullmatch(r"[0-9a-f]{64}", target_container_id) is None:
            raise ValueError("checkpoint helper staged target id is invalid")
        targets.append(target_container_id)
    return tuple(dict.fromkeys(targets))


def _parse_image_sha256(raw: str) -> str | None:
    value = raw.strip().splitlines()[-1].strip() if raw.strip() else ""
    if value.startswith("sha256:"):
        value = value.removeprefix("sha256:")
    if re.fullmatch(r"[0-9a-f]{64}", value) is None:
        return None
    return f"sha256:{value}"


def _has_log_line(raw: str, expected: str) -> bool:
    return any(line.rstrip("\r") == expected for line in raw.splitlines())


def _read_log_value(raw: str, key: str) -> str | None:
    prefix = f"{key}="
    values = [
        line.rstrip("\r").removeprefix(prefix)
        for line in raw.splitlines()
        if line.rstrip("\r").startswith(prefix)
    ]
    return values[-1] if values else None


class _LiveForkProbeFailure(Exception):
    def __init__(self, result: CommandResult, detail: str) -> None:
        super().__init__(detail)
        self.result = result
        self.detail = detail
