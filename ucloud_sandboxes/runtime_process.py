"""Verify runsc process ownership before adopting a numeric process identity."""

import os
from pathlib import Path

from .hibernation import linux_process_start_time_ticks


class RuntimeProcessIdentityError(RuntimeError):
    pass


def _flag(argv: list[str], name: str) -> str:
    values = []
    for index, arg in enumerate(argv):
        if arg.startswith(name + "="):
            values.append(arg[len(name) + 1 :])
        elif arg == name and index + 1 < len(argv):
            values.append(argv[index + 1])
    if len(values) != 1:
        raise RuntimeProcessIdentityError(f"runtime identity lacks unique {name}")
    return values[0]


def _process_exited(process: Path) -> bool:
    try:
        raw = (process / "stat").read_text()
    except FileNotFoundError:
        return True
    fields = raw[raw.rfind(")") + 2 :].split()
    return bool(fields and fields[0] in {"Z", "X"})


def owned_runtime_process_ticks(
    pid: int,
    *,
    proc_root: Path,
    runsc: Path,
    runtime_root: Path,
    bundle: Path,
    container_id: str,
    role: str = "sandbox",
    expected_ticks: int | None = None,
) -> int:
    """Require executable, invocation and cgroup provenance, bracketed by ticks.

    This is an accidental stale-PID defence, not an attestation against a
    hostile host root process. Callers must still use a pidfd and revalidate
    after opening it to close the discovery/open race.
    """
    if type(pid) is not int or pid <= 1:
        raise RuntimeProcessIdentityError(
            "runtime process PID must be greater than one"
        )
    ticks = linux_process_start_time_ticks(pid, proc_root=proc_root)
    if expected_ticks is not None and ticks != expected_ticks:
        raise RuntimeProcessIdentityError("runtime process start time changed")
    process = proc_root / str(pid)
    try:
        with (process / "cmdline").open("rb") as stream:
            raw = stream.read(65537)
        if not raw:
            # A zombie cannot be signalled or exec into a different owner.
            if _process_exited(process):
                raise ProcessLookupError(pid)
            raise RuntimeProcessIdentityError("runtime process has no command line")
        if len(raw) > 65536 or not raw.endswith(b"\0"):
            raise RuntimeProcessIdentityError("runtime process command line is invalid")
        argv = [arg.decode("utf-8") for arg in raw[:-1].split(b"\0")]
        command = "boot" if role == "sandbox" else "gofer"
        if (
            argv[0] != "runsc-" + role
            or argv[-1] != container_id
            or argv.count(command) != 1
            or _flag(argv, "--root") != str(runtime_root)
            or _flag(argv, "--bundle") != str(bundle)
        ):
            raise RuntimeProcessIdentityError(
                "runtime process invocation has another owner"
            )
        binary = runsc.resolve(strict=True)
        trusted = [binary]
        if role == "sandbox":
            # The pinned distribution may execute its packaged sentry sidecar.
            trusted.append(binary.parent / "gvisor-bin" / "gvisor_sentry")
        if not any(
            candidate.is_file() and os.path.samefile(process / "exe", candidate)
            for candidate in trusted
        ):
            raise RuntimeProcessIdentityError(
                "runtime process executable is not trusted"
            )
        groups = (process / "cgroup").read_text().splitlines()
        if not any(
            len(fields := line.split(":", 2)) == 3
            and fields[2].rstrip("/").rsplit("/", 1)[-1] == container_id
            for line in groups
        ):
            raise RuntimeProcessIdentityError(
                "runtime process cgroup has another owner"
            )
        if linux_process_start_time_ticks(pid, proc_root=proc_root) != ticks:
            raise RuntimeProcessIdentityError(
                "runtime process changed during verification"
            )
    except ProcessLookupError:
        raise
    except (RuntimeProcessIdentityError, OSError, UnicodeError) as exc:
        if _process_exited(process):
            raise ProcessLookupError(pid) from exc
        raise RuntimeProcessIdentityError(
            "runtime process ownership is unavailable"
        ) from exc
    return ticks
