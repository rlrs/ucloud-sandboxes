#!/usr/bin/env python3
"""Benchmark the AgentEnv-style direct-runsc Warden lifecycle on Linux."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import statistics
import time

from ucloud_sandboxes.direct_oci import DirectOciConfigBuilder
from ucloud_sandboxes.direct_warden import (
    DirectRunscWarden,
    DirectRunscWardenConfig,
    DirectSandbox,
    SubprocessCommandRunner,
)
from ucloud_sandboxes.hibernation import HibernationRuntimeFingerprint
from ucloud_sandboxes.image_rootfs import DockerRootfsStore, OverlayRootfsManager
from ucloud_sandboxes.sandbox import SandboxSecuritySpec, SandboxSpec


RUNSC_COMMIT = "9f653e577965df2ddd13875b5530cd2588661f1c"


class TimingCommandRunner:
    def __init__(self) -> None:
        self.delegate = SubprocessCommandRunner()
        self.events: list[dict[str, object]] = []

    def run(self, argv, *, timeout):
        started = time.monotonic()
        result = self.delegate.run(argv, timeout=timeout)
        elapsed_ms = (time.monotonic() - started) * 1000
        commands = {
            "checkpoint",
            "create",
            "delete",
            "exec",
            "restore",
            "start",
            "state",
        }
        command = next((item for item in argv if item in commands), str(argv[0]))
        self.events.append(
            {
                "command": command,
                "elapsed_ms": elapsed_ms,
                "returncode": result.returncode,
            }
        )
        return result


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while block := handle.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def tree_fingerprint(root: Path) -> str:
    digest = hashlib.sha256()
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root).as_posix().encode("utf-8")
        info = path.lstat()
        digest.update(relative)
        digest.update(b"\0")
        digest.update(str(info.st_mode).encode("ascii"))
        digest.update(b"\0")
        if path.is_file() and not path.is_symlink():
            digest.update(bytes.fromhex(sha256_file(path)))
    return digest.hexdigest()


def canonical_sha256(payload: object) -> str:
    return hashlib.sha256(
        json.dumps(
            payload,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
    ).hexdigest()


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    index = min(len(ordered) - 1, int(len(ordered) * fraction))
    return ordered[index]


def process_rss_bytes(pid: int) -> int:
    for line in Path(f"/proc/{pid}/status").read_text(encoding="ascii").splitlines():
        if line.startswith("VmRSS:"):
            return int(line.split()[1]) * 1024
    raise RuntimeError(f"VmRSS is absent for PID {pid}")


def process_cpu_max(pid: int) -> str:
    for line in Path(f"/proc/{pid}/cgroup").read_text(encoding="ascii").splitlines():
        hierarchy, controllers, relative = line.split(":", 2)
        if hierarchy == "0" and controllers == "":
            return (
                Path("/sys/fs/cgroup")
                .joinpath(relative.lstrip("/"), "cpu.max")
                .read_text(encoding="ascii")
                .strip()
            )
    raise RuntimeError(f"unified cgroup is absent for PID {pid}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runsc", type=Path, required=True)
    parser.add_argument("--bundle", type=Path)
    parser.add_argument("--image")
    parser.add_argument("--conformance-workload", type=Path)
    parser.add_argument(
        "--omit-resources",
        action="store_true",
        help="Diagnostic A/B: omit OCI Linux resource limits.",
    )
    parser.add_argument(
        "--foreground-restore",
        action="store_true",
        help="Diagnostic A/B: do not pass runsc restore --background.",
    )
    parser.add_argument(
        "--cpu-startup-burst",
        action="store_true",
        help="Pass the qualified custom restore CPU-startup-burst flag.",
    )
    parser.add_argument("--state-root", type=Path, required=True)
    parser.add_argument(
        "--quota-root",
        type=Path,
        help=(
            "Use the production split layout: runtime, bundles, journals, and "
            "image cache below state-root; overlay, memory, and artifacts below "
            "this unified quota root."
        ),
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cycles", type=int, default=100)
    parser.add_argument("--memory-mb", type=int, default=512)
    parser.add_argument("--disk-mb", type=int, default=512)
    parser.add_argument("--cpus", type=float)
    parser.add_argument(
        "--cpu-burst-us",
        type=int,
        help="Diagnostic OCI cpu.max.burst allowance in microseconds.",
    )
    parser.add_argument("--network", default="sandbox")
    parser.add_argument(
        "--memory-directory",
        default="hibernate-conformance.sandbox-1",
    )
    args = parser.parse_args()
    if os.geteuid() != 0:
        parser.error("benchmark must run as root")
    if args.cycles < 1:
        parser.error("--cycles must be positive")
    if args.memory_mb < 1 or args.disk_mb < 1:
        parser.error("--memory-mb and --disk-mb must be positive")
    if args.cpus is not None and args.cpus <= 0:
        parser.error("--cpus must be positive")
    if args.cpu_burst_us is not None and args.cpu_burst_us < 0:
        parser.error("--cpu-burst-us cannot be negative")
    if (args.bundle is None) == (args.image is None):
        parser.error("exactly one of --bundle or --image is required")

    overlays = None
    lease = None
    writable_root = (
        args.quota_root.resolve()
        if args.quota_root is not None
        else (args.state_root / "writable").resolve()
    )
    memory_root = (
        writable_root
        if args.quota_root is not None
        else (args.state_root / "memory").resolve()
    )
    artifact_root = (
        writable_root
        if args.quota_root is not None
        else (args.state_root / "artifacts").resolve()
    )
    tool_command = (
        ("/conformance-workload", "client")
        if args.conformance_workload is not None
        else ("/bin/true",)
    )
    initial_command = (
        ("/conformance-workload", "server")
        if args.conformance_workload is not None
        else ("/bin/sleep", "86400")
    )
    if args.bundle is None:
        image_store = DockerRootfsStore(
            (args.state_root / "image-cache").resolve()
        )
        image = image_store.materialize(args.image)
        config = DirectOciConfigBuilder().build(
            SandboxSpec(
                id="warden-benchmark",
                image=args.image,
                command=initial_command,
                memory_mb=args.memory_mb,
                cpus=args.cpus,
                disk_mb=args.disk_mb,
                network="none" if args.network == "none" else "bridge",
                parkable=True,
                security=SandboxSecuritySpec(
                    user="0:0",
                    cap_drop=(),
                    init=False,
                    no_new_privileges=False,
                ),
            ),
            image,
        )
        if args.cpu_burst_us is not None:
            config["linux"]["resources"]["cpu"]["burst"] = args.cpu_burst_us
        if args.omit_resources:
            config["linux"].pop("resources", None)
        overlays = OverlayRootfsManager(
            image_store,
            writable_root=writable_root,
            bundle_root=(args.state_root / "bundles").resolve(),
        )
        lease = overlays.prepare(
            sandbox_id="warden-benchmark",
            sandbox_generation=1,
            image_ref=args.image,
            config_template=config,
        )
        if args.conformance_workload is not None:
            target = lease.merged / "conformance-workload"
            shutil.copyfile(args.conformance_workload, target)
            target.chmod(0o755)
        bundle = lease.sandbox.bundle
        rootfs_sha256 = lease.image.rootfs_identity_sha256
    else:
        bundle = args.bundle
        rootfs_sha256 = tree_fingerprint(bundle / "rootfs")
    config_payload = json.loads(
        (bundle / "config.json").read_text(encoding="utf-8")
    )
    spec_sha256 = canonical_sha256(config_payload)
    runsc_sha256 = sha256_file(args.runsc)
    cpu_features_sha256 = hashlib.sha256(
        next(
            line
            for line in Path("/proc/cpuinfo").read_text(encoding="ascii").splitlines()
            if line.startswith("flags")
        ).encode("ascii")
    ).hexdigest()
    runtime = HibernationRuntimeFingerprint(
        runsc_sha256=runsc_sha256,
        runsc_commit=RUNSC_COMMIT,
        platform="systrap",
        architecture=os.uname().machine,
        page_size=os.sysconf("SC_PAGE_SIZE"),
        cpu_features_sha256=cpu_features_sha256,
        boot_config_sha256=canonical_sha256(
            {
                "network": args.network,
                "platform": "systrap",
                "restore_background": True,
                "restore_cpu_startup_burst": args.cpu_startup_burst,
            }
        ),
        rootfs_sha256=rootfs_sha256,
    )
    timing_runner = TimingCommandRunner()
    warden = DirectRunscWarden(
        DirectRunscWardenConfig(
            runsc=args.runsc.resolve(),
            runtime_root=(args.state_root / "runsc").resolve(),
            memory_root=memory_root,
            bundle_root=bundle.parent.resolve(),
            journal_root=(args.state_root / "journals").resolve(),
            artifact_root=artifact_root,
            runtime_fingerprint=runtime,
            network=args.network,
            readiness_command=("/bin/true",),
            restore_background=not args.foreground_restore,
            restore_cpu_startup_burst=args.cpu_startup_burst,
            remove_memory_directory_on_delete=args.quota_root is None,
        ),
        runner=timing_runner,
    )
    sandbox = (
        lease.sandbox
        if lease is not None
        else DirectSandbox(
            sandbox_id="warden-benchmark",
            sandbox_generation=1,
            container_id=hashlib.sha256(b"warden-benchmark:1").hexdigest(),
            spec_sha256=spec_sha256,
            rootfs_sha256=runtime.rootfs_sha256,
            bundle=bundle.resolve(),
            memory_directory=args.memory_directory,
        )
    )

    running = None
    create_ms = 0.0
    samples: list[dict[str, object]] = []
    try:
        created_started = time.monotonic()
        running = warden.create(sandbox, operation_id="create:1")
        create_ms = (time.monotonic() - created_started) * 1000
        for cycle in range(args.cycles):
            before = warden.exec(
                sandbox,
                tool_command,
            ).stdout.strip()
            rss = process_rss_bytes(int(running.sentry_pid))

            started = time.monotonic()
            parked = warden.park(sandbox, operation_id=f"park:{cycle + 1}")
            park_ms = (time.monotonic() - started) * 1000
            generation = warden.artifacts.generation_path(
                sandbox_id=sandbox.sandbox_id,
                sandbox_generation=sandbox.sandbox_generation,
                hibernation_generation=parked.hibernation_generation,
            )
            allocated_bytes = sum(
                path.stat().st_blocks * 512
                for path in generation.iterdir()
                if path.is_file()
            )
            if Path(f"/proc/{running.sentry_pid}").exists():
                raise RuntimeError("parked sandbox retained its old sentry")

            started = time.monotonic()
            command_start = len(timing_runner.events)
            running = warden.resume(
                sandbox,
                operation_id=f"wake:{cycle + 1}",
            )
            resume_ms = (time.monotonic() - started) * 1000
            restored_cpu_max = process_cpu_max(int(running.sentry_pid))
            resume_commands = timing_runner.events[command_start:]
            started = time.monotonic()
            after = warden.exec(
                sandbox,
                tool_command,
            ).stdout.strip()
            verify_ms = (time.monotonic() - started) * 1000
            samples.append(
                {
                    "allocated_artifact_bytes": allocated_bytes,
                    "before": before,
                    "cycle": cycle,
                    "park_ms": park_ms,
                    "pre_park_sentry_rss_bytes": rss,
                    "restored_cpu_max": restored_cpu_max,
                    "restored_sentry_pid": running.sentry_pid,
                    "resume_ms": resume_ms,
                    "resume_commands": resume_commands,
                    "resume_residual_ms": resume_ms
                    - sum(float(item["elapsed_ms"]) for item in resume_commands),
                    "verify_ms": verify_ms,
                    "after": after,
                }
            )
    finally:
        warden.delete(sandbox)
        if lease is not None:
            assert overlays is not None
            overlays.release(lease)

    park = [float(item["park_ms"]) for item in samples]
    resume = [float(item["resume_ms"]) for item in samples]
    verify = [float(item["verify_ms"]) for item in samples]
    payload = {
        "schema": 1,
        "cycles": args.cycles,
        "create_ms": create_ms,
        "workload": (
            "stateful-conformance-16mib"
            if args.conformance_workload is not None
            else "busybox-sleep"
        ),
        "host": {
            "runsc_sha256": runsc_sha256,
            "runsc_version": os.popen(f"{args.runsc} --version").read().strip(),
            "uname": " ".join(os.uname()),
        },
        "summary": {
            "park_p50_ms": statistics.median(park),
            "park_p95_ms": percentile(park, 0.95),
            "resume_p50_ms": statistics.median(resume),
            "resume_p95_ms": percentile(resume, 0.95),
            "verify_p50_ms": statistics.median(verify),
            "verify_p95_ms": percentile(verify, 0.95),
        },
        "samples": samples,
    }
    rendered = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    args.output.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
