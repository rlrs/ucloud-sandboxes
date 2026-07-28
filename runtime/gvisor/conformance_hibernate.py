#!/usr/bin/env python3
"""Repeatedly hibernate non-trivial kernel/process state under runsc."""

from __future__ import annotations

import argparse
import atexit
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time


def run(
    command: list[str],
    *,
    timeout: float = 60,
    capture: bool = True,
) -> str:
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
    return result.stdout.strip() if result.stdout else ""


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runsc", type=Path, required=True)
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--memory-dir", type=Path, required=True)
    parser.add_argument("--checkpoint-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cycles", type=int, default=100)
    parser.add_argument(
        "--network",
        choices=("none", "sandbox"),
        default="sandbox",
        help="runsc network mode (sandbox is required for the TCP state test)",
    )
    parser.add_argument(
        "--rootfs-tmp",
        action="store_true",
        help="use a rootfs /tmp instead of mounting private tmpfs (diagnostics only)",
    )
    args = parser.parse_args()
    if os.geteuid() != 0:
        parser.error("conformance must run as root")

    config_path = args.bundle / "config.json"
    config = json.loads(config_path.read_text(encoding="utf-8"))
    config["process"]["args"] = ["/conformance-workload", "server"]
    config["root"]["readonly"] = False
    memory_directory = "hibernate-conformance.sandbox-1"
    config.setdefault("annotations", {})[
        "dev.gvisor.internal.application-memory-directory"
    ] = memory_directory
    (args.memory_dir / memory_directory).mkdir(
        mode=0o700,
        parents=True,
        exist_ok=True,
    )
    if args.rootfs_tmp:
        (args.bundle / "rootfs" / "tmp").mkdir(mode=0o1777, exist_ok=True)
    else:
        config["mounts"].append(
            {
                "destination": "/tmp",
                "type": "tmpfs",
                "source": "tmpfs",
                "options": ["nosuid", "nodev", "mode=1777", "size=67108864"],
            }
        )
    network_namespace: str | None = None
    if args.network == "sandbox":
        network_namespace = f"gvisor-conf-{os.getpid()}"
        run(["ip", "netns", "add", network_namespace])
        atexit.register(
            subprocess.run,
            ["ip", "netns", "del", network_namespace],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        run(
            [
                "ip",
                "netns",
                "exec",
                network_namespace,
                "ip",
                "link",
                "set",
                "lo",
                "up",
            ]
        )
        for namespace in config["linux"]["namespaces"]:
            if namespace["type"] == "network":
                namespace["path"] = f"/run/netns/{network_namespace}"
                break
        else:
            raise RuntimeError("OCI config does not declare a network namespace")
    config_path.write_text(json.dumps(config, indent=2) + "\n", encoding="utf-8")

    container_id = "hibernate-conformance"
    state = [str(args.runsc), f"--root={args.root}"]
    common = [
        *state,
        f"--application-memory-file-dir={args.memory_dir}",
        f"--network={args.network}",
    ]
    run(
        common + ["create", f"--bundle={args.bundle}", container_id],
        capture=False,
    )
    run(state + ["start", container_id], capture=False)

    samples: list[dict[str, object]] = []
    previous_generation = 0
    for cycle in range(args.cycles):
        before = run(
            state
            + ["exec", container_id, "/conformance-workload", "client"],
        )
        generation = int(before.split()[1]) >> 32
        if generation <= previous_generation:
            raise RuntimeError("condition-variable generation did not advance")
        previous_generation = generation

        cycle_dir = args.checkpoint_root / f"cycle-{cycle:04d}"
        cycle_dir.mkdir(mode=0o700, parents=True)
        started = time.monotonic()
        run(
            common
            + [
                "checkpoint",
                "--hibernate",
                f"--image-path={cycle_dir}",
                container_id,
            ]
        )
        park_ms = (time.monotonic() - started) * 1000
        run(state + ["delete", "--force", container_id], capture=False)

        started = time.monotonic()
        run(
            common
            + [
                "restore",
                "--detach",
                "--background",
                f"--image-path={cycle_dir}",
                f"--bundle={args.bundle}",
                container_id,
            ],
            capture=False,
        )
        restore_ms = (time.monotonic() - started) * 1000
        started = time.monotonic()
        after = run(
            state
            + ["exec", container_id, "/conformance-workload", "client"],
        )
        verify_ms = (time.monotonic() - started) * 1000
        restored_generation = int(after.split()[1]) >> 32
        if restored_generation <= generation:
            raise RuntimeError("restored condition-variable generation did not advance")
        previous_generation = restored_generation
        samples.append(
            {
                "cycle": cycle,
                "park_ms": park_ms,
                "restore_ms": restore_ms,
                "verify_ms": verify_ms,
                "response": after,
            }
        )

    run(state + ["delete", "--force", container_id], capture=False)
    payload = {
        "schema": 1,
        "cycles": args.cycles,
        "runsc_sha256": hashlib.sha256(args.runsc.read_bytes()).hexdigest(),
        "samples": samples,
    }
    args.output.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
