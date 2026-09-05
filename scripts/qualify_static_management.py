#!/usr/bin/env python3
import argparse
import hashlib
import json
import subprocess
import sys
import tempfile
import shutil
import os
from pathlib import Path
from ucloud_sandboxes.sandbox import SandboxSpec
from ucloud_sandboxes.direct_oci import DirectOciConfigBuilder
from ucloud_sandboxes.image_rootfs import DockerImageConfig, MaterializedRootfs


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Qualify the branch OCI builder and static file helper in a shellless rootfs; run as root on Linux."
    )
    parser.add_argument("--runtime", type=Path, required=True)
    parser.add_argument("--helper", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if sys.platform != "linux" or os.geteuid() != 0:
        parser.error("requires a disposable Linux host and root")
    args.runtime = args.runtime.resolve()
    args.helper = args.helper.resolve()
    with args.output.open("x") as output:
        output.write("{}\n")
    root = Path(tempfile.mkdtemp(prefix="ucloud-static-conformance-"))
    bundle = root / "scratch-bundle"
    bundle.mkdir()
    fs = bundle / "rootfs"
    fs.mkdir()
    (fs / "etc").mkdir()
    spec = SandboxSpec.from_dict(
        {
            "id": "static-helper-probe",
            "image": "scratch-helper",
            "command": [
                "/.ucloud-job-init",
                "supervise",
                "--state-dir",
                "/workspace/state",
            ],
            "memory_mb": 256,
            "disk_mb": 256,
            "network": "none",
            "security": {
                "user": "1000:1000",
                "init": False,
                "supplementary_groups": ["42"],
            },
            "filesystem": {
                "management_helper": "static",
                "shm_mb": 128,
                "workspace_storage": "image",
            },
        }
    )
    builder = DirectOciConfigBuilder(managed_init_binary=args.helper)
    image = MaterializedRootfs(
        "scratch-helper",
        "sha256:" + hashlib.sha256((args.helper).read_bytes()).hexdigest(),
        "0" * 64,
        fs,
        DockerImageConfig(),
    )
    config = builder.build(spec, image)
    builder.prepare_workspace(fs, spec=spec)
    builder.install_managed_init(fs, enabled=True)
    (bundle / "config.json").write_text(json.dumps(config))
    prefix = [str(args.runtime), "--root", str(root / "runsc-state"), "--network=none"]
    name = "static-helper-probe"
    checks = {}
    execution_error = None
    cleanup_ok = False
    try:
        subprocess.run(
            [*prefix, "run", "--detach", "--bundle", str(bundle), name],
            check=True,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=(root / "scratch-start.log").open("w"),
            timeout=30,
        )

        def execute(*args, input=None):
            return subprocess.run(
                [*prefix, "exec", name, "/.ucloud-job-init", "files", *args],
                input=input,
                capture_output=True,
                timeout=20,
            )

        target = "/workspace/literal ü :,$file"
        w = execute("write", target, "16", input=b"hello literal")
        r = execute("read", target, "16")
        checks["shellless-files"] = (
            w.returncode == 0 and r.returncode == 0 and r.stdout == b"hello literal"
        )
        checks["readiness"] = execute("ready").returncode == 0
        status = execute("read", "/proc/self/status", "8192")
        checks["identity"] = (
            status.returncode == 0
            and b"Uid:\t1000\t1000\t1000\t1000" in status.stdout
            and b"Groups:\t42" in status.stdout
        )
        checks["root-write-denied"] = (
            execute("write", "/root-file", "16", input=b"denied").returncode != 0
        )
        checks["oversized-write-preserves-old"] = (
            execute("write", target, "2", input=b"too much").returncode != 0
            and execute("read", target, "16").stdout == b"hello literal"
        )
        checks["no-image-shell"] = not (fs / "bin").exists()
    except Exception as exc:
        execution_error = type(exc).__name__
    finally:
        try:
            deleted = subprocess.run(
                [*prefix, "delete", "--force", name], capture_output=True, timeout=30
            )
            cleanup_ok = deleted.returncode == 0
            # runsc retains a bind-mounted empty network namespace per state
            # root even after the last container is deleted.
            namespace = root / "runsc-state/null-netns"
            if cleanup_ok and os.path.ismount(namespace):
                unmounted = subprocess.run(
                    ["umount", str(namespace)], capture_output=True, timeout=10
                )
                cleanup_ok = unmounted.returncode == 0
            if cleanup_ok:
                shutil.rmtree(root)
        except Exception:
            cleanup_ok = False
    report = {
        "schema_version": 1,
        "checks": checks,
        "error_type": execution_error,
        "cleanup": "completed" if cleanup_ok else "failed",
        "runtime_sha256": hashlib.sha256((args.runtime).read_bytes()).hexdigest(),
        "helper_sha256": hashlib.sha256((args.helper).read_bytes()).hexdigest(),
        "spec": spec.to_dict(),
    }
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report))
    if not cleanup_ok:
        print("Cleanup needs attention: " + str(root), file=sys.stderr)
    return (
        0
        if checks and all(checks.values()) and execution_error is None and cleanup_ok
        else 1
    )


if __name__ == "__main__":
    raise SystemExit(main())
