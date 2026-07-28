#!/usr/bin/env python3
"""Compare gVisor memfd+swap and XFS-backed application memory.

Run this as root on an otherwise idle cgroup-v2 benchmark host. It creates one
container at a time, pauses it, asks the kernel to reclaim its cgroup, and then
measures lightweight exec and checksum-verified page touches after resume.
"""

from __future__ import annotations

import argparse
import errno
import json
import os
from pathlib import Path
import re
import subprocess
import time
import uuid


MIB = 1024 * 1024
MEMORY_STAT_FIELDS = (
    "anon",
    "file",
    "shmem",
    "file_dirty",
    "file_writeback",
    "swapcached",
    "pgfault",
    "pgmajfault",
)


def command(argv: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(argv, check=check, text=True, capture_output=True)


def elapsed_command(argv: list[str]) -> tuple[float, str]:
    started = time.perf_counter()
    result = command(argv)
    return time.perf_counter() - started, result.stdout.strip()


def read_int(path: Path) -> int:
    value = path.read_text().strip()
    return -1 if value == "max" else int(value)


def memory_snapshot(cgroup: Path) -> dict[str, int]:
    stats: dict[str, int] = {}
    for line in (cgroup / "memory.stat").read_text().splitlines():
        name, value = line.split()
        if name in MEMORY_STAT_FIELDS:
            stats[name] = int(value)
    stats["current"] = read_int(cgroup / "memory.current")
    stats["swap_current"] = read_int(cgroup / "memory.swap.current")
    return stats


def block_snapshot(device: str) -> dict[str, int]:
    fields = [int(value) for value in Path(f"/sys/block/{device}/stat").read_text().split()]
    return {
        "reads": fields[0],
        "read_sectors": fields[2],
        "writes": fields[4],
        "write_sectors": fields[6],
    }


def subtract(after: dict[str, int], before: dict[str, int]) -> dict[str, int]:
    return {name: after[name] - before[name] for name in after}


def container_cgroup(name: str) -> Path:
    pid = int(command(["docker", "inspect", "-f", "{{.State.Pid}}", name]).stdout)
    for line in Path(f"/proc/{pid}/cgroup").read_text().splitlines():
        hierarchy, _, relative = line.partition("::")
        if hierarchy == "0":
            result = Path("/sys/fs/cgroup") / relative.lstrip("/")
            if not (result / "memory.reclaim").exists():
                raise RuntimeError(f"{result} has no memory.reclaim")
            return result
    raise RuntimeError(f"no cgroup-v2 membership for container pid {pid}")


def backing_snapshot(backing_dir: Path) -> dict[str, int | str] | None:
    files = list(backing_dir.glob("*.memory"))
    if not files:
        return None
    if len(files) != 1:
        raise RuntimeError(f"expected one backing file, found {files}")
    stat = files[0].stat()
    return {
        "path": str(files[0]),
        "size": stat.st_size,
        "allocated": stat.st_blocks * 512,
    }


def workload_command(name: str, request: str) -> tuple[float, dict[str, int | str]]:
    elapsed, output = elapsed_command(
        ["docker", "exec", name, "/workload", "client", request]
    )
    values: dict[str, int | str] = {"raw": output}
    for key, value in re.findall(r"([a-z_]+)=([0-9]+)", output):
        values[key] = int(value)
    if output.startswith("error "):
        raise RuntimeError(output)
    return elapsed, values


def reclaim(cgroup: Path, requested_bytes: int) -> dict[str, int | str | bool]:
    started = time.perf_counter()
    error = ""
    complete = True
    try:
        (cgroup / "memory.reclaim").write_text(f"{requested_bytes}\n")
    except OSError as exc:
        if exc.errno != errno.EAGAIN:
            raise
        complete = False
        error = os.strerror(exc.errno)
    return {
        "seconds": time.perf_counter() - started,
        "requested_bytes": requested_bytes,
        "complete": complete,
        "error": error,
    }


def flush_backing(backing: dict[str, int | str] | None) -> float:
    if backing is None:
        return 0.0
    started = time.perf_counter()
    descriptor = os.open(str(backing["path"]), os.O_RDWR | os.O_CLOEXEC)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return time.perf_counter() - started


def wait_ready(name: str, timeout: float = 30.0) -> dict[str, int | str]:
    deadline = time.monotonic() + timeout
    last_error = ""
    while time.monotonic() < deadline:
        try:
            _, result = workload_command(name, "ready")
            return result
        except subprocess.CalledProcessError as exc:
            last_error = exc.stderr.strip()
            time.sleep(0.05)
    raise RuntimeError(f"workload did not become ready: {last_error}")


def park_and_wake(
    name: str,
    cgroup: Path,
    backing_dir: Path,
    block_device: str,
    requested_bytes: int,
    label: str,
) -> dict[str, object]:
    before_memory = memory_snapshot(cgroup)
    before_block = block_snapshot(block_device)
    before_backing = backing_snapshot(backing_dir)
    pause_seconds, _ = elapsed_command(["docker", "pause", name])
    flush_seconds = flush_backing(before_backing)
    after_flush_memory = memory_snapshot(cgroup)
    after_flush_block = block_snapshot(block_device)
    reclaim_result = reclaim(cgroup, requested_bytes)
    parked_memory = memory_snapshot(cgroup)
    after_block = block_snapshot(block_device)
    parked_backing = backing_snapshot(backing_dir)

    unpause_seconds, _ = elapsed_command(["docker", "unpause", name])
    true_seconds, _ = elapsed_command(["docker", "exec", name, "true"])
    ls_seconds, _ = elapsed_command(["docker", "exec", name, "ls", "/"])
    scan_wall_seconds, scan = workload_command(name, "scan")
    after_scan_memory = memory_snapshot(cgroup)
    final_block = block_snapshot(block_device)
    return {
        "label": label,
        "pause_seconds": pause_seconds,
        "flush_seconds": flush_seconds,
        "reclaim": reclaim_result,
        "unpause_seconds": unpause_seconds,
        "true_seconds": true_seconds,
        "ls_seconds": ls_seconds,
        "scan_wall_seconds": scan_wall_seconds,
        "scan": scan,
        "memory_before": before_memory,
        "memory_after_flush": after_flush_memory,
        "memory_parked": parked_memory,
        "memory_after_scan": after_scan_memory,
        "block_during_flush": subtract(after_flush_block, before_block),
        "block_during_reclaim": subtract(after_block, after_flush_block),
        "block_during_wake": subtract(final_block, after_block),
        "backing_before": before_backing,
        "backing_parked": parked_backing,
    }


def benchmark_case(
    runtime: str,
    logical_mib: int,
    populated_mib: int,
    workload_binary: Path,
    backing_dir: Path,
    block_device: str,
) -> dict[str, object]:
    name = f"gvisor-memory-{uuid.uuid4().hex[:12]}"
    memory_mib = max(1024, logical_mib + 512)
    create_started = time.perf_counter()
    command(
        [
            "docker",
            "run",
            "-d",
            "--name",
            name,
            "--runtime",
            runtime,
            "--network",
            "none",
            "--memory",
            f"{memory_mib}m",
            "--mount",
            f"type=bind,src={workload_binary},dst=/workload,readonly",
            "alpine:3.22",
            "/workload",
            "server",
            str(logical_mib),
            str(populated_mib),
        ]
    )
    try:
        ready = wait_ready(name)
        create_seconds = time.perf_counter() - create_started
        cgroup = container_cgroup(name)
        hot_wall_seconds, hot_scan = workload_command(name, "scan")
        requested_bytes = memory_mib * MIB
        cold = park_and_wake(
            name,
            cgroup,
            backing_dir,
            block_device,
            requested_bytes,
            "initial",
        )
        clean_second = park_and_wake(
            name,
            cgroup,
            backing_dir,
            block_device,
            requested_bytes,
            "clean-second-park",
        )
        _, dirty = workload_command(name, "dirty:1")
        dirty_one_percent = park_and_wake(
            name,
            cgroup,
            backing_dir,
            block_device,
            requested_bytes,
            "dirty-one-percent",
        )
        return {
            "runtime": runtime,
            "logical_mib": logical_mib,
            "populated_mib": populated_mib,
            "memory_limit_mib": memory_mib,
            "create_seconds": create_seconds,
            "ready": ready,
            "hot_scan_wall_seconds": hot_wall_seconds,
            "hot_scan": hot_scan,
            "initial_park": cold,
            "clean_second_park": clean_second,
            "dirty_command": dirty,
            "dirty_one_percent_park": dirty_one_percent,
        }
    finally:
        command(["docker", "rm", "-f", name], check=False)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workload", type=Path, default=Path("/tmp/memory-workload"))
    parser.add_argument(
        "--backing-dir",
        type=Path,
        default=Path("/mnt/gvisor-xfs/application-memory"),
    )
    parser.add_argument("--block-device", default="vda")
    parser.add_argument(
        "--case",
        action="append",
        default=[],
        help="LOGICAL_MIB:POPULATED_MIB; repeat for multiple cases",
    )
    parser.add_argument(
        "--runtime",
        action="append",
        default=[],
        help="Docker runtime; defaults to runsc-memfd and runsc-xfs",
    )
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if os.geteuid() != 0:
        parser.error("run as root so memory.reclaim and Docker are accessible")
    cases = [
        tuple(map(int, value.split(":", 1)))
        for value in (args.case or ["256:256", "1024:1024", "4096:4096", "1024:1"])
    ]
    runtimes = args.runtime or ["runsc-memfd", "runsc-xfs"]
    document: dict[str, object] = {
        "schema": 1,
        "started_unix": time.time(),
        "host": {
            "uname": command(["uname", "-a"]).stdout.strip(),
            "memory_total_bytes": int(
                re.search(r"MemTotal:\s+(\d+)", Path("/proc/meminfo").read_text()).group(1)
            )
            * 1024,
            "block_device": args.block_device,
            "backing_dir": str(args.backing_dir),
            "runsc_sha256": command(
                ["sha256sum", "/usr/local/bin/runsc-disk"]
            ).stdout.split()[0],
            "runsc_version": command(
                ["/usr/local/bin/runsc-disk", "--version"]
            ).stdout.strip(),
        },
        "cases": [],
    }
    for logical_mib, populated_mib in cases:
        for runtime in runtimes:
            print(
                f"benchmarking {runtime} logical={logical_mib} MiB "
                f"populated={populated_mib} MiB",
                flush=True,
            )
            result = benchmark_case(
                runtime,
                logical_mib,
                populated_mib,
                args.workload.resolve(),
                args.backing_dir,
                args.block_device,
            )
            document["cases"].append(result)
            if args.output:
                args.output.write_text(json.dumps(document, indent=2) + "\n")
    document["finished_unix"] = time.time()
    encoded = json.dumps(document, indent=2) + "\n"
    if args.output:
        args.output.write_text(encoded)
    else:
        print(encoded)


if __name__ == "__main__":
    main()
