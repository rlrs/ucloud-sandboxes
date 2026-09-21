#!/usr/bin/env python3
"""Run identical guest probes on a gateway or a native Linux Docker reference."""

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import urllib.request
import uuid


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", choices=("gateway", "docker"), required=True)
    parser.add_argument("--docker-runtime", default="runc")
    parser.add_argument(
        "--image", required=True, help="Python-equipped image pinned by sha256 digest"
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--runtime-digest",
        required=True,
        help="Qualified runtime artifact SHA256 (or Docker/runc version identifier)",
    )
    parser.add_argument("--directory", default="/tmp")
    args = parser.parse_args()
    if not re.fullmatch(r"[^\s]+@sha256:[0-9a-f]{64}", args.image):
        parser.error("image must be pinned by sha256 digest")
    # Reserve evidence before provisioning; never overwrite earlier qualification.
    with args.output.open("x") as handle:
        handle.write("{}\n")
    probe = Path(__file__).with_name("guest_conformance_probe.py").read_text()
    argv = ["python3", "-I", "-c", probe, "--directory", args.directory]
    sandbox_id = "features-" + uuid.uuid4().hex[:16]
    evidence = {
        "schema_version": 1,
        "backend": args.backend,
        "docker_runtime": args.docker_runtime if args.backend == "docker" else None,
        "image": args.image,
        "runtime_identifier": args.runtime_digest,
        "probe_sha256": hashlib.sha256(probe.encode()).hexdigest(),
        "started_at": datetime.now(timezone.utc).isoformat(),
        "directory": args.directory,
        "memory_mb": 1024,
        "cpus": 1,
    }
    client = None
    created = False
    try:
        if args.backend == "docker":
            if platform.system() != "Linux":
                raise ValueError("native reference runner must execute on Linux")
            result = subprocess.run(
                [
                    "docker",
                    "run",
                    "--rm",
                    "--name",
                    sandbox_id,
                    "--runtime",
                    args.docker_runtime,
                    "--tmpfs",
                    "/tmp:rw,nosuid,nodev,mode=1777,size=64m",
                    "--tmpfs",
                    "/run:rw,nosuid,nodev,mode=755,size=16m",
                    "--shm-size",
                    "64m",
                    "--memory",
                    "1024m",
                    "--cpus",
                    "1",
                    "--network",
                    "none",
                    "--user",
                    "0:0",
                    "--entrypoint",
                    "python3",
                    args.image,
                    *argv[1:],
                ],
                capture_output=True,
                text=True,
                timeout=120,
            )
            stdout, exit_code = result.stdout, result.returncode
        else:
            from ucloud_sandboxes_sdk import SandboxClient

            base = os.environ["UCLOUD_SANDBOX_URL"].rstrip("/")
            token = os.environ["UCLOUD_SANDBOX_API_TOKEN"]
            client = SandboxClient(base, api_token=token, timeout_seconds=900)
            payload = {
                "id": sandbox_id,
                "image": args.image,
                "profile": "container",
                "command": ["python3", "-c", "import time; time.sleep(1800)"],
                "memory_mb": 1024,
                "disk_mb": 2048,
                "cpus": 1,
                "ttl_seconds": 1800,
                "security": {"user": "0:0", "cap_drop": [], "init": False},
            }
            request = urllib.request.Request(
                base + "/v1/sandboxes",
                data=json.dumps(payload).encode(),
                headers={
                    "Authorization": "Bearer " + token,
                    "Content-Type": "application/json",
                },
                method="POST",
            )
            # Cleanup is attempted even if create succeeds remotely but its response is lost.
            created = True
            with urllib.request.urlopen(request, timeout=900) as response:
                json.load(response)
            result = client.exec(sandbox_id, argv, working_dir="/", timeout_seconds=120)
            stdout, exit_code = result.stdout, result.exit_code
        evidence["probe"] = json.loads(stdout)
        evidence["exit_code"] = exit_code
        checks = evidence["probe"].get("results", {})
        expected = {
            "literal-paths-and-symlinks",
            "filesystem-xattrs",
            "filesystem-locks",
            "posix-acl",
            "unix-sockets",
            "process-signals",
        }
        passed = set(checks) == expected and all(
            row.get("status") == "passed" for row in checks.values()
        )
        evidence["status"] = "passed" if exit_code == 0 and passed else "failed"
    except Exception as exc:
        evidence["status"] = "error"
        evidence["error_type"] = type(exc).__name__
    finally:
        try:
            if args.backend == "docker":
                subprocess.run(
                    ["docker", "rm", "-f", sandbox_id], capture_output=True, timeout=30
                )
            elif created and client is not None:
                client.delete(sandbox_id)
            evidence["cleanup"] = "completed"
        except Exception as exc:
            evidence["cleanup"] = "failed"
            evidence["cleanup_error_type"] = type(exc).__name__
        args.output.write_text(json.dumps(evidence, indent=2) + "\n")
    return (
        0
        if evidence["status"] == "passed" and evidence["cleanup"] == "completed"
        else 1
    )


if __name__ == "__main__":
    raise SystemExit(main())
