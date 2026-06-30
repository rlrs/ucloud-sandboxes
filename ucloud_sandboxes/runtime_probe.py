from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any

from .sandbox import CommandExecutor, CommandResult, SubprocessExecutor


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

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "ok": self.ok,
            "skipped": self.skipped,
            "command": list(self.command),
            "exit_code": self.exit_code,
            "stdout": self.stdout,
            "stderr": self.stderr,
            "detail": self.detail,
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
        return all(result.ok or result.skipped for result in self.results)

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
        image: str = "busybox",
        use_sudo: bool = False,
        execute: bool = False,
    ) -> None:
        self.executor = executor or SubprocessExecutor()
        self.docker_binary = docker_binary
        self.runtime_name = runtime_name
        self.image = image
        self.use_sudo = use_sudo
        self.execute = execute

    def run(self) -> RuntimeConformanceReport:
        results = (
            self._probe_gvisor_kernel(),
            self._probe_network_none_blocks(),
            self._probe_memory_limit_visible(),
            self._probe_mount_blocked(),
            self._probe_non_root_supported(),
            self._probe_storage_opt_quota_enforced(),
            self._probe_tmpfs_quota_enforced(),
        )
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
                "if [ -z \"$size\" ] || [ \"$size\" -gt 20480 ]; then "
                "echo tmpfs-not-isolated; exit 0; fi; "
                "dd if=/dev/zero of=/tmp/ucloud-tmpfs-probe bs=1M count=32; "
                "status=$?; rm -f /tmp/ucloud-tmpfs-probe; exit $status"
            ),
        )
        result = self._run(name, argv)
        if isinstance(result, ProbeResult):
            return result
        ok = result.exit_code != 0 and _looks_like_no_space(result.stdout, result.stderr)
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
        ok = result.exit_code != 0 and _looks_like_no_space(result.stdout, result.stderr)
        return self._result(
            name,
            result,
            ok,
            "writing 32MB should fail with --storage-opt size=16m",
        )

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
