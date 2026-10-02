"""Node-side create pipeline micro-benchmark.

Real: DirectSandboxProvisioner, DirectSandboxRegistry (SQLite, synchronous=FULL,
on the given state directory), DirectNetworkManager with real ip/netns/veth and
real per-create iptables-save/sysctl host-rule checks. Fake: storage service,
overlay mounts and runsc (the provisioner test fakes of the measured tree).

Runs in a disposable user+net+mount namespace, so no host network state changes:

    unshare --user --map-root-user --net --mount --fork \\
        python bench_create_pipeline.py --code TREE --state DIR --out FILE.json
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import inspect
import json
import os
from pathlib import Path
import shutil
import statistics
import subprocess
import sys
import time


def percentile(values, q):
    ordered = sorted(values)
    if not ordered:
        return None
    index = min(len(ordered) - 1, max(0, round(q / 100 * (len(ordered) - 1))))
    return round(ordered[index], 2)


def summary(samples):
    keys = sorted({key for sample in samples for key in sample})
    return {
        key: {
            "p50": percentile([s.get(key, 0) for s in samples], 50),
            "p90": percentile([s.get(key, 0) for s in samples], 90),
            "p99": percentile([s.get(key, 0) for s in samples], 99),
            "mean": round(statistics.fmean(s.get(key, 0) for s in samples), 2),
        }
        for key in keys
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--code", type=Path, required=True)
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--pool", type=int, default=0)
    parser.add_argument("--sequential", type=int, default=100)
    parser.add_argument("--burst", type=int, default=32)
    parser.add_argument("--sustained", type=int, default=128)
    parser.add_argument("--tmpfs", action="store_true")
    args = parser.parse_args()
    # It remounts / and /run/netns and edits iptables: never in the host's
    # initial user namespace, where root would change the real node.
    assert os.geteuid() == 0, "run under unshare --map-root-user"
    assert Path("/proc/self/uid_map").read_text().split() != ["0", "0", "4294967295"], \
        "run under unshare --user --map-root-user, not as host root"

    def run(*argv):
        subprocess.run(argv, check=True, capture_output=True, timeout=10)

    run("mount", "--make-rprivate", "/")
    Path("/run/netns").mkdir(parents=True, exist_ok=True)
    run("mount", "-t", "tmpfs", "tmpfs", "/run/netns")
    run("ip", "link", "set", "lo", "up")

    sys.path.insert(0, str(args.code))
    from tests.test_direct_provisioner import FakeImageStore, FakeOverlays, FakeStorage, FakeWarden
    from ucloud_sandboxes import phase_timings
    from ucloud_sandboxes.direct_network import DirectNetworkManager
    from ucloud_sandboxes.direct_oci import DirectOciConfigBuilder
    from ucloud_sandboxes.direct_provisioner import DirectSandboxProvisioner
    from ucloud_sandboxes.direct_registry import DirectSandboxRegistry
    from ucloud_sandboxes.sandbox import SandboxSecuritySpec, SandboxSpec
    import threading

    class Storage(FakeStorage):
        guard = threading.Lock()

        def prepare_volume(self, owner, **kwargs):
            with self.guard:
                return super().prepare_volume(owner, **kwargs)

    if args.state.exists():
        shutil.rmtree(args.state)
    args.state.mkdir(parents=True)
    if args.tmpfs:
        # fsync is free here: isolates CPU, subprocess and kernel work.
        run("mount", "-t", "tmpfs", "-o", "mode=0700", "tmpfs", str(args.state))
    os.chmod(args.state, 0o700)
    images = FakeImageStore(args.state)
    overlays = FakeOverlays(images, args.state)
    storage = Storage(overlays.writable_root)
    warden = FakeWarden(args.state, storage)
    warden.config.network = "sandbox"
    warden.rootfs_lifecycle = overlays
    pooled = "pool_size" in inspect.signature(DirectNetworkManager).parameters
    network = DirectNetworkManager(
        args.state / "network-slots.json",
        **({"pool_size": args.pool} if pooled else {}),
    )
    registry = DirectSandboxRegistry(args.state / "registry.sqlite")
    provisioner = DirectSandboxProvisioner(
        registry=registry, overlays=overlays, oci=DirectOciConfigBuilder(network_mode="sandbox"),
        warden=warden, network_manager=network,
    )
    provisioner.start()

    def pool_full():
        if not pooled or args.pool == 0:
            return
        deadline = time.monotonic() + 120
        while len(network._pool_ready) < args.pool:
            if time.monotonic() > deadline:
                raise TimeoutError("pool did not fill")
            time.sleep(0.01)

    counter = iter(range(1_000_000))

    def create(_=None):
        name = f"bench-{next(counter):06d}"
        spec = SandboxSpec(id=name, image="image", memory_mb=1024, disk_mb=2048,
                           network="bridge", security=SandboxSecuritySpec(init=False))
        with phase_timings.recording() as phases:
            started = time.monotonic()
            provisioner.create(spec=spec, sandbox_generation=1, operation_id=f"create:{name}:1")
            total = (time.monotonic() - started) * 1000
        return {**{k: float(v) for k, v in phases.items()}, "total_ms": total}

    def batch(count, concurrency):
        revision = registry.activity_revision()
        started = time.monotonic()
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            samples = list(pool.map(create, range(count)))
        wall = time.monotonic() - started
        return {
            "creates": count,
            "concurrency": concurrency,
            "creates_per_second": round(count / wall, 1),
            "registry_commits_per_create": (registry.activity_revision() - revision) / count,
            "phases": summary(samples),
        }

    pool_full()
    for _ in range(5):
        create()
    results = {"code": str(args.code), "pool": args.pool if pooled else None}
    pool_full()
    results["sequential"] = batch(args.sequential, 1)
    pool_full()
    results["burst"] = batch(args.burst, args.burst)
    pool_full()
    results["sustained"] = batch(args.sustained, 32)

    host = []
    for _ in range(50):
        started = time.monotonic()
        network._ensure_host_rules()
        host.append((time.monotonic() - started) * 1000)
    parts = {}
    for name, argv in {
        "iptables_save_ms": ("iptables-save",),
        "sysctl_ms": ("sysctl", "-q", "-w", "net.ipv4.ip_forward=1"),
    }.items():
        samples = []
        for _ in range(50):
            started = time.monotonic()
            subprocess.run(argv, capture_output=True, check=True)
            samples.append((time.monotonic() - started) * 1000)
        parts[name] = percentile(samples, 50)
    results["host_rules_check_ms"] = {"p50": percentile(host, 50), "p99": percentile(host, 99), **parts}
    if pooled:
        network.stop_pool()
    args.out.write_text(json.dumps(results, indent=2, sort_keys=True))
    print(json.dumps(results, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
