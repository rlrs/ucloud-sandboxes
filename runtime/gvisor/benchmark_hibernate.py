#!/usr/bin/env python3
"""Benchmark gVisor metadata-only hibernation on a dedicated Linux host."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import select
import signal
import subprocess
import time
from typing import Any


READY_RE = re.compile(
    r"ready logical_pages=(?P<logical>\d+) populated_pages=(?P<populated>\d+) "
    r"checksum=(?P<checksum>\d+)"
)
SCAN_RE = re.compile(
    r"scan ns=(?P<ns>\d+) checksum=(?P<checksum>\d+) "
    r"pages=(?P<pages>\d+) bytes=(?P<bytes>\d+)"
)


def checked(
    command: list[str], timeout: float = 30, *, capture: bool = True
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        command,
        text=True,
        stdout=subprocess.PIPE if capture else subprocess.DEVNULL,
        stderr=subprocess.PIPE if capture else subprocess.DEVNULL,
        timeout=timeout,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"command failed ({result.returncode}): {command!r}\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    return result


def best_effort(command: list[str], timeout: float = 30) -> None:
    subprocess.run(
        command,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        timeout=timeout,
    )


def exec_response(
    prefix: list[str], container_id: str, workload: str, request: str
) -> tuple[str, float]:
    started = time.monotonic()
    process = subprocess.Popen(
        prefix + ["exec", container_id, workload, "client", request],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )
    assert process.stdout is not None
    readable, _, _ = select.select([process.stdout], [], [], 10)
    if not readable:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        stdout, stderr = process.communicate()
        raise RuntimeError(
            f"timed out waiting for exec response: {request!r}; "
            f"stdout={stdout!r}, stderr={stderr!r}"
        )
    line = process.stdout.readline().strip()
    elapsed = time.monotonic() - started
    if not line:
        stdout, stderr = process.communicate()
        raise RuntimeError(
            f"exec produced no response for {request!r}: "
            f"stdout={stdout!r}, stderr={stderr!r}"
        )
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    process.wait()
    return line, elapsed


def file_stats(path: Path) -> dict[str, int]:
    info = path.stat()
    return {
        "logical_bytes": info.st_size,
        "allocated_bytes": info.st_blocks * 512,
    }


def process_rss(pid: int) -> int | None:
    try:
        lines = Path(f"/proc/{pid}/status").read_text().splitlines()
    except OSError:
        return None
    for line in lines:
        if line.startswith("VmRSS:"):
            return int(line.split()[1]) * 1024
    return None


def configure_bundle(
    template: dict[str, Any],
    config_path: Path,
    workload: str,
    logical_mib: int,
    populated_mib: int,
) -> None:
    config = json.loads(json.dumps(template))
    config["process"]["args"] = [
        workload,
        "server",
        str(logical_mib),
        str(populated_mib),
    ]
    config_path.write_text(json.dumps(config, indent=2) + "\n")


def parse_case(raw: str) -> tuple[int, int]:
    logical, populated = raw.split(":", 1)
    return int(logical), int(populated)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runsc", type=Path, required=True)
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--memory-dir", type=Path, required=True)
    parser.add_argument("--checkpoint-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--workload", default="/memory-workload")
    parser.add_argument(
        "--case",
        action="append",
        type=parse_case,
        help="LOGICAL_MIB:POPULATED_MIB (repeatable)",
    )
    parser.add_argument("--cycles", type=int, default=3)
    parser.add_argument(
        "--cold-cycle",
        type=int,
        default=1,
        help="zero-based cycle on which to sync and drop host page cache",
    )
    args = parser.parse_args()

    if os.geteuid() != 0:
        parser.error("benchmark must run as root")
    cases = args.case or [(256, 256), (1024, 1024), (4096, 4096), (4096, 1)]
    config_path = args.bundle / "config.json"
    template = json.loads(config_path.read_text())
    container_id = "hibernate-benchmark"
    memory_directory = "hibernate-benchmark.sandbox-1"
    template.setdefault("annotations", {})[
        "dev.gvisor.internal.application-memory-directory"
    ] = memory_directory
    (args.memory_dir / memory_directory).mkdir(
        mode=0o700,
        parents=True,
        exist_ok=True,
    )
    common = [
        str(args.runsc),
        f"--root={args.root}",
        f"--application-memory-file-dir={args.memory_dir}",
        "--network=none",
    ]
    state_prefix = [str(args.runsc), f"--root={args.root}"]

    runsc_bytes = args.runsc.read_bytes()
    output: dict[str, Any] = {
        "schema": 1,
        "created_at_unix": time.time(),
        "host": {
            "uname": " ".join(os.uname()),
            "runsc_sha256": hashlib.sha256(runsc_bytes).hexdigest(),
            "runsc_version": checked([str(args.runsc), "--version"]).stdout.strip(),
            "page_size": os.sysconf("SC_PAGE_SIZE"),
        },
        "cases": [],
    }

    for logical_mib, populated_mib in cases:
        best_effort(state_prefix + ["delete", "--force", container_id], timeout=10)
        configure_bundle(
            template,
            config_path,
            args.workload,
            logical_mib,
            populated_mib,
        )
        checked(
            common + ["create", f"--bundle={args.bundle}", container_id],
            capture=False,
        )
        checked(state_prefix + ["start", container_id], capture=False)

        ready_line = ""
        ready_seconds = 0.0
        for _ in range(300):
            try:
                ready_line, ready_seconds = exec_response(
                    state_prefix, container_id, args.workload, "ready"
                )
                break
            except RuntimeError:
                time.sleep(0.1)
        ready_match = READY_RE.fullmatch(ready_line)
        if ready_match is None:
            raise RuntimeError(f"invalid ready response: {ready_line!r}")

        case_result: dict[str, Any] = {
            "logical_mib": logical_mib,
            "populated_mib": populated_mib,
            "initial_ready_seconds": ready_seconds,
            "initial_ready": {
                key: int(value) for key, value in ready_match.groupdict().items()
            },
            "cycles": [],
        }

        for cycle in range(args.cycles):
            state = json.loads(checked(state_prefix + ["state", container_id]).stdout)
            pid = int(state["pid"])
            pre_park_rss = process_rss(pid)
            cycle_dir = (
                args.checkpoint_root
                / f"{logical_mib}-{populated_mib}-cycle-{cycle}"
            )
            cycle_dir.mkdir(mode=0o700, parents=True, exist_ok=False)

            started = time.monotonic()
            checked(
                common
                + [
                    "checkpoint",
                    "--hibernate",
                    f"--image-path={cycle_dir}",
                    container_id,
                ],
                timeout=60,
            )
            park_seconds = time.monotonic() - started
            artifacts = {
                path.name: file_stats(path)
                for path in sorted(cycle_dir.iterdir())
                if path.is_file()
            }
            parked_process_alive = Path(f"/proc/{pid}").exists()

            checked(
                state_prefix + ["delete", "--force", container_id],
                timeout=10,
                capture=False,
            )
            cache_drop_seconds = None
            cold = cycle == args.cold_cycle
            if cold:
                started = time.monotonic()
                os.sync()
                Path("/proc/sys/vm/drop_caches").write_text("3\n")
                cache_drop_seconds = time.monotonic() - started

            started = time.monotonic()
            checked(
                common
                + [
                    "restore",
                    "--detach",
                    "--background",
                    f"--image-path={cycle_dir}",
                    f"--bundle={args.bundle}",
                    container_id,
                ],
                timeout=60,
                capture=False,
            )
            restore_seconds = time.monotonic() - started
            restored_state = json.loads(
                checked(state_prefix + ["state", container_id]).stdout
            )
            ready_line, first_ready_seconds = exec_response(
                state_prefix, container_id, args.workload, "ready"
            )
            ready_match = READY_RE.fullmatch(ready_line)
            if ready_match is None:
                raise RuntimeError(f"invalid restored ready response: {ready_line!r}")
            scan_line, scan_response_seconds = exec_response(
                state_prefix, container_id, args.workload, "scan"
            )
            scan_match = SCAN_RE.fullmatch(scan_line)
            if scan_match is None:
                raise RuntimeError(f"invalid scan response: {scan_line!r}")

            case_result["cycles"].append(
                {
                    "cycle": cycle,
                    "cold": cold,
                    "park_seconds": park_seconds,
                    "parked_process_alive": parked_process_alive,
                    "pre_park_sentry_rss_bytes": pre_park_rss,
                    "artifacts": artifacts,
                    "cache_drop_seconds": cache_drop_seconds,
                    "restore_seconds": restore_seconds,
                    "restored_sentry_rss_bytes": process_rss(
                        int(restored_state["pid"])
                    ),
                    "first_ready_seconds": first_ready_seconds,
                    "ready": {
                        key: int(value)
                        for key, value in ready_match.groupdict().items()
                    },
                    "scan_response_seconds": scan_response_seconds,
                    "scan": {
                        key: int(value)
                        for key, value in scan_match.groupdict().items()
                    },
                }
            )
        checked(
            state_prefix + ["delete", "--force", container_id],
            timeout=10,
            capture=False,
        )
        output["cases"].append(case_result)
        args.output.write_text(json.dumps(output, indent=2) + "\n")

    print(json.dumps(output, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
