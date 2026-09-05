#!/usr/bin/env python3
"""Qualify ACL/identity/lock persistence and paused handoff on a disposable node."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import struct
import subprocess
import time
from uuid import uuid4

from ucloud_sandboxes.gvisor_distribution import GVISOR_COMMIT, distribution_files


IMAGE = "python@sha256:9d2e5553305c7c7b0097999bb17187c69b921ccd6bc9d40e4bb5ebe652c00285"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runsc", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--cycles", type=int, default=10)
    args = parser.parse_args()
    if os.geteuid() != 0 or args.cycles < 1:
        parser.error("run as root on a disposable Linux node with positive cycles")
    runtime = args.runsc.resolve(strict=True)
    distribution_files(runtime, GVISOR_COMMIT)
    work = args.output.resolve()
    work.mkdir(mode=0o700, parents=True, exist_ok=False)
    bundle = work / "bundle"
    rootfs = bundle / "rootfs"
    rootfs.mkdir(parents=True)
    progress = work / "progress"
    progress.mkdir()
    memory = work / "memory"
    (memory / "compatibility").mkdir(parents=True, mode=0o700)
    identifier = "gvisor-compat-" + uuid4().hex[:12]
    state = [str(runtime), f"--root={work / 'state'}"]
    common = [*state, "--network=none", f"--application-memory-file-dir={memory}"]
    transcript = (work / "commands.log").open("w")

    def run(command, *, detached=False, cwd=None):
        transcript.write(json.dumps([str(part) for part in command]) + "\n")
        transcript.flush()
        result = subprocess.run(
            command,
            cwd=cwd,
            text=True,
            stdin=subprocess.DEVNULL,
            stdout=transcript if detached else subprocess.PIPE,
            stderr=transcript,
            timeout=180,
            check=True,
        )
        return result.stdout

    def counter():
        return struct.unpack("<Q", (progress / "counter").read_bytes())[0]

    def wait_progress(previous):
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if (progress / "counter").exists() and counter() > previous:
                return
            time.sleep(0.05)
        raise RuntimeError("guest did not resume execution")

    def verify():
        result = json.loads(
            run(
                state
                + [
                    "exec",
                    identifier,
                    "/usr/local/bin/python3",
                    "/compatibility.py",
                    "verify",
                ]
            )
        )
        if (
            result.get("acl") != "pass"
            or result.get("flock") != "pass"
            or result.get("identity") != {"uid": 1000, "gid": 1000, "groups": [42]}
        ):
            raise RuntimeError(f"guest verification failed: {result}")
        return result

    def assert_paused():
        status = json.loads(run(state + ["state", identifier]))
        if status["status"] != "paused":
            raise RuntimeError(f"runtime did not persist paused state: {status}")
        before = counter()
        time.sleep(0.3)
        if counter() != before:
            raise RuntimeError("guest executed before paused handoff was resumed")

    image_container = None
    samples = []
    try:
        run(["docker", "pull", IMAGE])
        image_container = run(["docker", "create", IMAGE]).strip()
        archive = work / "rootfs.tar"
        run(["docker", "export", "--output", str(archive), image_container])
        # This is a pinned, trusted qualification fixture, not the product image store.
        run(["tar", "--extract", "--file", str(archive), "--directory", str(rootfs)])
        archive.unlink()
        run([str(runtime), "spec"], cwd=bundle)
        config = json.loads((bundle / "config.json").read_text())
        config["root"]["readonly"] = False
        config["process"].update(
            args=["/usr/local/bin/python3", "/compatibility.py", "server"],
            cwd="/",
            terminal=False,
        )
        caps = [
            "CAP_CHOWN",
            "CAP_DAC_OVERRIDE",
            "CAP_FOWNER",
            "CAP_SETUID",
            "CAP_SETGID",
        ]
        config["process"]["capabilities"] = {
            key: caps
            for key in ("bounding", "effective", "inheritable", "permitted", "ambient")
        }
        config["mounts"] = [m for m in config["mounts"] if m["destination"] != "/tmp"]
        config["mounts"].extend(
            [
                {
                    "destination": "/tmp",
                    "type": "tmpfs",
                    "source": "tmpfs",
                    "options": ["mode=1777", "size=67108864", "nosuid", "nodev"],
                },
                {
                    "destination": "/handoff-probe",
                    "type": "bind",
                    "source": str(progress),
                    "options": ["bind", "rw"],
                },
            ]
        )
        config.setdefault("annotations", {})[
            "dev.gvisor.internal.application-memory-directory"
        ] = "compatibility"
        config["linux"]["cgroupsPath"] = "/" + identifier
        config["linux"]["resources"] = {
            "cpu": {"quota": 25000, "period": 100000},
            "memory": {"limit": 512 * 1024 * 1024},
        }
        (bundle / "config.json").write_text(json.dumps(config))
        fixture = (
            Path(__file__).resolve().parents[1]
            / "runtime/gvisor/compatibility_workload.py"
        )
        shutil.copyfile(fixture, rootfs / "compatibility.py")
        run(
            common + ["run", "--detach", f"--bundle={bundle}", identifier],
            detached=True,
        )
        wait_progress(0)
        initial = verify()
        # Exercise capture rollback while the original sentry is still alive.
        rollback = work / "rollback"
        run(
            common
            + ["checkpoint", "--hibernate", f"--image-path={rollback}", identifier]
        )
        assert_paused()
        before = counter()
        run(state + ["resume", identifier])
        wait_progress(before)
        verify()
        for cycle in range(args.cycles):
            checkpoint = work / f"checkpoint-{cycle}"
            run(
                common
                + [
                    "checkpoint",
                    "--hibernate",
                    f"--image-path={checkpoint}",
                    identifier,
                ]
            )
            assert_paused()
            run(state + ["delete", "--force", identifier])
            run(
                common
                + [
                    "restore",
                    "--detach",
                    "--background",
                    "--start-paused",
                    "--cpu-startup-burst",
                    f"--image-path={checkpoint}",
                    f"--bundle={bundle}",
                    identifier,
                ],
                detached=True,
            )
            assert_paused()
            cpu_max = (
                (Path("/sys/fs/cgroup") / identifier / "cpu.max").read_text().strip()
            )
            if cpu_max != "25000 100000":
                raise RuntimeError(f"CPU quota was not restored: {cpu_max}")
            before = counter()
            run(state + ["resume", identifier])
            wait_progress(before)
            samples.append({"cycle": cycle, "cpu_max": cpu_max, "state": verify()})
        payload = {
            "schema": 1,
            "gvisor_commit": GVISOR_COMMIT,
            "image": IMAGE,
            "initial": initial,
            "capture_rollback": "pass",
            "cycles": samples,
            "runsc_sha256": hashlib.sha256(runtime.read_bytes()).hexdigest(),
        }
        (work / "result.json").write_text(json.dumps(payload, indent=2) + "\n")
        print(json.dumps(payload))
    finally:
        run(state + ["delete", "--force", identifier])
        if image_container:
            run(["docker", "rm", image_container])
        # runsc owns a bind-mounted null network namespace beneath its state root.
        namespace = work / "state/null-netns"
        if (
            namespace.exists()
            and subprocess.run(["mountpoint", "-q", str(namespace)]).returncode == 0
        ):
            run(["umount", str(namespace)])
        transcript.close()


if __name__ == "__main__":
    main()
