#!/usr/bin/env python3
"""Benchmark bounded tool-triggered restores through the direct node owner."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
from pathlib import Path
import shutil
import statistics
import time

from ucloud_sandboxes.direct_node import (
    DirectNodeCapability,
    DirectNodeCoordinator,
)
from ucloud_sandboxes.direct_oci import DirectOciConfigBuilder
from ucloud_sandboxes.direct_warden import (
    DirectRunscWarden,
    DirectRunscWardenConfig,
)
from ucloud_sandboxes.hibernation import HibernationRuntimeFingerprint
from ucloud_sandboxes.image_rootfs import DockerRootfsStore, OverlayRootfsManager
from ucloud_sandboxes.sandbox import SandboxSecuritySpec, SandboxSpec


RUNSC_COMMIT = "9f653e577965df2ddd13875b5530cd2588661f1c"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while block := handle.read(1024 * 1024):
            digest.update(block)
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


async def wake_round(
    coordinator: DirectNodeCoordinator,
    sandboxes,
    tool_command: tuple[str, ...],
) -> tuple[float, list[float]]:
    async def one(item) -> float:
        started = time.monotonic()
        result = await coordinator.exec(item, tool_command)
        if result.returncode != 0:
            raise RuntimeError("tool-triggered exec failed")
        if tool_command[0] == "/conformance-workload" and not result.stdout.startswith(
            "ok "
        ):
            raise RuntimeError(f"conformance state verification failed: {result.stdout}")
        return (time.monotonic() - started) * 1000

    started = time.monotonic()
    latencies = await asyncio.gather(*(one(item) for item in sandboxes))
    return (time.monotonic() - started) * 1000, latencies


async def benchmark(args: argparse.Namespace) -> dict[str, object]:
    image_store = DockerRootfsStore((args.state_root / "image-cache").resolve())
    image = image_store.materialize(args.image)
    if args.config_template is None:
        template = DirectOciConfigBuilder().build(
            SandboxSpec(
                id="benchmark-template",
                image=args.image,
                command=("/bin/sleep", "86400"),
                memory_mb=512,
                disk_mb=512,
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
    else:
        template = json.loads(args.config_template.read_text(encoding="utf-8"))
    if args.conformance_workload is None:
        template["process"]["args"] = ["/bin/sleep", "86400"]
        tool_command = ("/bin/true",)
        workload_kind = "busybox-sleep"
    else:
        template["process"]["args"] = ["/conformance-workload", "server"]
        tool_command = ("/conformance-workload", "client")
        workload_kind = "stateful-conformance-16mib"
    template["process"]["terminal"] = False
    overlays = OverlayRootfsManager(
        image_store,
        writable_root=(args.state_root / "writable").resolve(),
        bundle_root=(args.state_root / "bundles").resolve(),
    )
    runsc_sha256 = sha256_file(args.runsc)
    boot_config_sha256 = canonical_sha256(
        {
            "network": args.network,
            "platform": "systrap",
            "restore_background": True,
            "restore_cpu_startup_burst": args.cpu_startup_burst,
        }
    )
    runtime = HibernationRuntimeFingerprint(
        runsc_sha256=runsc_sha256,
        runsc_commit=RUNSC_COMMIT,
        platform="systrap",
        architecture=os.uname().machine,
        page_size=os.sysconf("SC_PAGE_SIZE"),
        cpu_features_sha256=hashlib.sha256(
            next(
                line
                for line in Path("/proc/cpuinfo")
                .read_text(encoding="ascii")
                .splitlines()
                if line.startswith("flags")
            ).encode("ascii")
        ).hexdigest(),
        boot_config_sha256=boot_config_sha256,
        rootfs_sha256=image.rootfs_identity_sha256,
    )
    warden = DirectRunscWarden(
        DirectRunscWardenConfig(
            runsc=args.runsc.resolve(),
            runtime_root=(args.state_root / "runsc").resolve(),
            memory_root=(args.state_root / "memory").resolve(),
            bundle_root=(args.state_root / "bundles").resolve(),
            journal_root=(args.state_root / "journals").resolve(),
            artifact_root=(args.state_root / "artifacts").resolve(),
            runtime_fingerprint=runtime,
            network=args.network,
            readiness_command=("/bin/true",),
            restore_cpu_startup_burst=args.cpu_startup_burst,
        )
    )
    leases = [
        overlays.prepare(
            sandbox_id=f"burst-{index:04d}",
            sandbox_generation=1,
            image_ref=args.image,
            config_template=template,
        )
        for index in range(args.sandboxes)
    ]
    sandboxes = [lease.sandbox for lease in leases]
    if args.conformance_workload is not None:
        for lease in leases:
            target = lease.merged / "conformance-workload"
            shutil.copyfile(args.conformance_workload, target)
            target.chmod(0o755)
    samples: list[dict[str, object]] = []
    try:
        for index, item in enumerate(sandboxes):
            warden.create(item, operation_id=f"create:{index}")
            warden.park(item, operation_id=f"initial-park:{index}")

        for slots in args.restore_slots:
            coordinator = DirectNodeCoordinator(
                warden,
                DirectNodeCapability(
                    enabled=True,
                    max_concurrent_restores=slots,
                    allowed_runsc_sha256=runsc_sha256,
                    allowed_boot_config_sha256=boot_config_sha256,
                ),
            )
            for iteration in range(args.iterations):
                makespan_ms, latencies = await wake_round(
                    coordinator,
                    sandboxes,
                    tool_command,
                )
                samples.append(
                    {
                        "iteration": iteration,
                        "makespan_ms": makespan_ms,
                        "p50_ms": statistics.median(latencies),
                        "p95_ms": percentile(latencies, 0.95),
                        "p99_ms": percentile(latencies, 0.99),
                        "restore_slots": slots,
                    }
                )
                for index, item in enumerate(sandboxes):
                    await coordinator.park(
                        item,
                        operation_id=f"park:{slots}:{iteration}:{index}",
                    )
    finally:
        for item in sandboxes:
            try:
                record = warden.inspect(item)
                if record is not None:
                    if record.state.value not in {"running", "parked"}:
                        warden.reconcile(item)
                    warden.delete(item)
            except Exception as exc:
                print(f"cleanup failed for {item.sandbox_id}: {exc}", flush=True)
        for lease in leases:
            try:
                overlays.release(lease)
            except Exception as exc:
                print(
                    f"overlay cleanup failed for {lease.sandbox.sandbox_id}: {exc}",
                    flush=True,
                )

    return {
        "schema": 1,
        "image": args.image,
        "runsc_sha256": runsc_sha256,
        "sandboxes": args.sandboxes,
        "iterations": args.iterations,
        "workload": workload_kind,
        "samples": samples,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runsc", type=Path, required=True)
    parser.add_argument("--image", required=True)
    parser.add_argument("--config-template", type=Path)
    parser.add_argument("--state-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--sandboxes", type=int, default=24)
    parser.add_argument("--iterations", type=int, default=3)
    parser.add_argument("--restore-slots", type=int, nargs="+", default=[1, 4, 8, 24])
    parser.add_argument("--network", default="sandbox")
    parser.add_argument(
        "--cpu-startup-burst",
        action="store_true",
        help="Pass the qualified custom restore CPU-startup-burst flag.",
    )
    parser.add_argument("--conformance-workload", type=Path)
    args = parser.parse_args()
    if os.geteuid() != 0:
        parser.error("benchmark must run as root")
    if args.sandboxes < 1 or args.iterations < 1:
        parser.error("sandboxes and iterations must be positive")
    if any(item < 1 for item in args.restore_slots):
        parser.error("restore slots must be positive")
    payload = asyncio.run(benchmark(args))
    rendered = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    args.output.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
