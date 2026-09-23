"""Measure the optional live-wait tier inside a native product fixture.

Called after the fixture starts its isolated sandbox and before durable capture.
No global VM drop-caches, swap setting, or production inventory is touched.
The result is evidence, not an automatic feature activation decision.
"""

from dataclasses import asdict
from pathlib import Path
import time

from ucloud_sandboxes.resident_memory import (
    ResidentMemorySampler,
    ResidentMemoryReclaimer,
)
from ucloud_sandboxes.hibernation import HibernationState
from ucloud_sandboxes.resource_evidence import disk_rates, read_leaf_disks


def qualify_live_wait(
    warden,
    sandbox,
    check,
    *,
    rounds=8,
    target_bytes=64 * 1024 * 1024,
    compare_pause=False,
):
    sampler = ResidentMemorySampler(proc_root=warden.config.proc_root)
    original = warden.inspect(sandbox)
    key = (sandbox.sandbox_id, sandbox.sandbox_generation)
    import json

    cgroup_path = json.loads((sandbox.bundle / "config.json").read_text())["linux"][
        "cgroupsPath"
    ]

    def sample():
        value = sampler.sample(
            key,
            pid=original.sentry_pid,
            start_time_ticks=original.sentry_start_time_ticks,
            container_id=sandbox.container_id,
            expected_path=cgroup_path,
        )
        if value is None:
            raise RuntimeError("native live-wait cgroup ownership is not measurable")
        return value

    measurements = []
    for index in range(rounds):
        baseline_start = time.monotonic()
        check()
        baseline_seconds = time.monotonic() - baseline_start
        before = sample()
        disks_before = read_leaf_disks(Path("/proc"), Path("/sys")) or {}
        started = time.monotonic()
        mode = (
            ("resident", "quiesced", "quiesced", "resident")[index % 4]
            if compare_pause
            else "resident"
        )
        if mode == "quiesced":
            # Qualification-only comparison; product keeps one LIVE resident
            # phase because this pause adds no consistent reclaim advantage.
            warden._checked(*warden._state_prefix(), "pause", sandbox.container_id)
        quiet = warden.inspect_snapshot(sandbox)
        pause_seconds = time.monotonic() - started
        try:
            assert quiet.state == HibernationState.RUNNING
            assert quiet.hibernation_generation == original.hibernation_generation
            assert quiet.sentry_pid == original.sentry_pid
            paused = sample()
            result = ResidentMemoryReclaimer(sampler).reclaim(
                key,
                paused,
                target_bytes=target_bytes,
                is_current=lambda: warden.inspect_snapshot(sandbox) == quiet,
            )
            reclaimed = sample()
        finally:
            thaw_start = time.monotonic()
            if mode == "quiesced":
                warden._checked(*warden._state_prefix(), "resume", sandbox.container_id)
            awake = warden.inspect_snapshot(sandbox)
            thaw_seconds = time.monotonic() - thaw_start
        assert awake.state == HibernationState.RUNNING
        assert awake.hibernation_generation == original.hibernation_generation
        assert awake.sentry_pid == original.sentry_pid
        check_start = time.monotonic()
        check()
        check_seconds = time.monotonic() - check_start
        after = sample()
        elapsed = time.monotonic() - started
        disks_after = read_leaf_disks(Path("/proc"), Path("/sys")) or {}
        physical = [
            asdict(
                disk_rates(
                    identity,
                    name,
                    counters,
                    disks_before.get(identity, (None, None))[1],
                    elapsed,
                )
            )
            for identity, (name, counters) in disks_after.items()
        ]
        measurements.append(
            {
                "mode": mode,
                "before": asdict(before),
                "paused": asdict(paused),
                "reclaimed": asdict(reclaimed),
                "after_touch": asdict(after),
                "reclaim": asdict(result),
                "pause_seconds": pause_seconds,
                "thaw_seconds": thaw_seconds,
                "first_check_seconds": check_seconds,
                "baseline_check_seconds": baseline_seconds,
                "same_generation_and_pid": True,
                "physical_devices": physical,
                "measurement_seconds": elapsed,
            }
        )
    return {
        "cgroup_path": cgroup_path,
        "rounds": measurements,
        "writer_enabled": False,
        "note": "A live wait has no checkpoint portability or node-loss recovery.",
    }
