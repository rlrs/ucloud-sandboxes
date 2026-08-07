#!/usr/bin/env python3
"""Measure resident and parked density for direct-runsc Warden sandboxes."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import statistics
import time

from ucloud_sandboxes.direct_warden import (
    DirectRunscWarden,
    DirectRunscWardenConfig,
)
from ucloud_sandboxes.hibernation import HibernationRuntimeFingerprint
from ucloud_sandboxes.image_rootfs import DockerOverlay2RootfsStore, OverlayRootfsManager


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


def process_rss_bytes(pid: int) -> int:
    for line in Path(f"/proc/{pid}/status").read_text(encoding="ascii").splitlines():
        if line.startswith("VmRSS:"):
            return int(line.split()[1]) * 1024
    raise RuntimeError(f"VmRSS is absent for PID {pid}")


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(len(ordered) * fraction))]


def summarize(values: list[float]) -> dict[str, float]:
    return {
        "p50": statistics.median(values),
        "p95": percentile(values, 0.95),
        "p99": percentile(values, 0.99),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runsc", type=Path, required=True)
    parser.add_argument("--image", required=True)
    parser.add_argument("--config-template", type=Path, required=True)
    parser.add_argument("--conformance-workload", type=Path, required=True)
    parser.add_argument("--state-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--sandboxes", type=int, default=256)
    parser.add_argument("--network", default="sandbox")
    args = parser.parse_args()
    if os.geteuid() != 0:
        parser.error("benchmark must run as root")
    if args.sandboxes < 1:
        parser.error("sandboxes must be positive")

    template = json.loads(args.config_template.read_text(encoding="utf-8"))
    template["process"]["args"] = ["/conformance-workload", "server"]
    template["process"]["terminal"] = False
    image_store = DockerOverlay2RootfsStore(
        (args.state_root / "image-cache").resolve()
    )
    image = image_store.materialize(args.image)
    overlays = OverlayRootfsManager(
        image_store,
        writable_root=(args.state_root / "writable").resolve(),
        bundle_root=(args.state_root / "bundles").resolve(),
    )
    runsc_sha256 = sha256_file(args.runsc)
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
        boot_config_sha256=canonical_sha256(
            {
                "network": args.network,
                "platform": "systrap",
                "restore_background": True,
            }
        ),
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
        )
    )
    leases = []
    create_ms: list[float] = []
    verify_ms: list[float] = []
    park_ms: list[float] = []
    sentry_rss: list[int] = []
    payload: dict[str, object] | None = None
    try:
        for index in range(args.sandboxes):
            lease = overlays.prepare(
                sandbox_id=f"density-{index:04d}",
                sandbox_generation=1,
                image_ref=args.image,
                config_template=template,
            )
            target = lease.merged / "conformance-workload"
            shutil.copyfile(args.conformance_workload, target)
            target.chmod(0o755)
            leases.append(lease)
            started = time.monotonic()
            record = warden.create(
                lease.sandbox,
                operation_id=f"create:{index}",
            )
            create_ms.append((time.monotonic() - started) * 1000)
            sentry_rss.append(process_rss_bytes(record.sentry_pid or 0))

        for lease in leases:
            started = time.monotonic()
            result = warden.exec(
                lease.sandbox,
                ("/conformance-workload", "client"),
            )
            verify_ms.append((time.monotonic() - started) * 1000)
            if result.returncode != 0 or not result.stdout.startswith("ok "):
                raise RuntimeError(
                    f"stateful verification failed: {lease.sandbox.sandbox_id}"
                )

        parked_pids: list[int] = []
        for index, lease in enumerate(leases):
            running = warden.inspect(lease.sandbox)
            if running is None or running.sentry_pid is None:
                raise RuntimeError("running sandbox has no sentry identity")
            parked_pids.append(running.sentry_pid)
            started = time.monotonic()
            warden.park(
                lease.sandbox,
                operation_id=f"park:{index}",
            )
            park_ms.append((time.monotonic() - started) * 1000)

        retained_pids = [pid for pid in parked_pids if Path(f"/proc/{pid}").exists()]
        artifact_files = [
            path
            for path in warden.config.artifact_root.rglob("*")
            if path.is_file()
        ]
        payload = {
            "schema": 1,
            "image": args.image,
            "runsc_sha256": runsc_sha256,
            "sandboxes": args.sandboxes,
            "workload": "stateful-conformance-16mib",
            "create_ms": summarize(create_ms),
            "verify_ms": summarize(verify_ms),
            "park_ms": summarize(park_ms),
            "running_sentry_rss_total_bytes": sum(sentry_rss),
            "running_sentry_rss_per_sandbox_bytes": summarize(
                [float(item) for item in sentry_rss]
            ),
            "parked_artifact_allocated_bytes": sum(
                path.stat().st_blocks * 512 for path in artifact_files
            ),
            "parked_artifact_logical_bytes": sum(
                path.stat().st_size for path in artifact_files
            ),
            "parked_retained_sentry_pids": retained_pids,
        }
    finally:
        for lease in leases:
            try:
                record = warden.inspect(lease.sandbox)
                if record is not None:
                    if record.state.value not in {"running", "parked"}:
                        warden.reconcile(lease.sandbox)
                    warden.delete(lease.sandbox)
            except Exception as exc:
                print(
                    f"backend cleanup failed for {lease.sandbox.sandbox_id}: {exc}",
                    flush=True,
                )
            try:
                overlays.release(lease)
            except Exception as exc:
                print(
                    f"overlay cleanup failed for {lease.sandbox.sandbox_id}: {exc}",
                    flush=True,
                )

    if payload is None:
        raise RuntimeError("density benchmark did not produce a result")
    rendered = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    args.output.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
