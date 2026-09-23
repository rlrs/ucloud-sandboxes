from ucloud_sandboxes.transition_admission import MemoryDemand
from dataclasses import asdict, replace
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

from tests.test_node_runtime import _WakeService
from ucloud_sandboxes.background_io import Pressure, PressureSampler
from ucloud_sandboxes.node_runtime import DirectNodeRuntime
from ucloud_sandboxes.models import ResidentWaitMetrics
from ucloud_sandboxes.resident_memory import ResidentMemorySample
from ucloud_sandboxes.resource_evidence import MemoryBackingCapacity
from ucloud_sandboxes.warm_park import WarmParkDeferred, WarmParkPolicy, decide_resident_wait

GIB = 1024**3


class ResidentMemoryBackingTests(TestCase):
    def test_backing_reasons_round_trip_without_accepting_arbitrary_reasons(self):
        metrics = ResidentWaitMetrics(1, 0, 0, 1, 0, "memory_backing_headroom")
        for reason in ("memory_backing_headroom", "memory_backing_unavailable"):
            expected = replace(metrics, reason=reason)
            self.assertEqual(ResidentWaitMetrics.from_dict(asdict(expected)), expected)
        self.assertIsNone(
            ResidentWaitMetrics.from_dict(asdict(replace(metrics, reason="unknown")))
        )

    def pressure(self, free=2 * GIB):
        return Pressure(
            0.2, 0, 0, 20 * GIB, MemoryBackingCapacity(95 * GIB, free, "mount")
        )

    def test_backing_exhaustion_reclaims_even_with_physical_memory_available(self):
        decision = decide_resident_wait(self.pressure(), MemoryDemand(0, 0))
        self.assertTrue(decision.memory_reclaim)
        self.assertTrue(decision.backing_reclaim)
        self.assertEqual(decision.reason, "memory_backing_headroom")
        self.assertEqual(decision.target_bytes, int(5.5 * GIB))
        self.assertFalse(
            decide_resident_wait(
                replace(self.pressure(), memory_backing=None), MemoryDemand(0, 0)
            ).reclaim
        )

    def test_backing_hysteresis_and_pending_demand_use_real_free_bytes(self):
        pressure = self.pressure(6 * GIB)
        self.assertFalse(decide_resident_wait(pressure, MemoryDemand(0, 0)).reclaim)
        self.assertTrue(decide_resident_wait(pressure, MemoryDemand(0, 0), memory_reclaim=True).reclaim)
        decision = decide_resident_wait(self.pressure(7 * GIB), MemoryDemand(4 * GIB, 4 * GIB))
        self.assertTrue(decision.backing_reclaim)
        self.assertEqual(decision.reason, "queued_demand")
        self.assertEqual(decision.target_bytes, int(4.5 * GIB))

    def test_file_restore_demand_does_not_create_a_tmpfs_deficit(self):
        pressure = self.pressure(7 * GIB)
        decision = decide_resident_wait(pressure, MemoryDemand(4 * GIB, 0))
        self.assertFalse(decision.reclaim)
        self.assertTrue(decide_resident_wait(
            pressure, MemoryDemand(4 * GIB, 4 * GIB)
        ).backing_reclaim)

    def test_tmpfs_deficit_skips_known_file_heaps_and_credits_ram_only(self):
        pressure = [self.pressure(20 * GIB)]
        policy = WarmParkPolicy(lambda: pressure[0])
        for key, ram in (('file', 0), ('ram', 6 * GIB)):
            with self.assertRaises(WarmParkDeferred):
                with policy.defer(key, memory_bytes=6 * GIB, ram_bytes=ram, blocking=False):
                    self.fail('healthy headroom must retain both backings')
        pressure[0] = self.pressure(2 * GIB)
        self.assertFalse(policy.ready('file', memory_bytes=6 * GIB, ram_bytes=0))
        self.assertTrue(policy.ready('ram', memory_bytes=6 * GIB, ram_bytes=6 * GIB))
        with policy.defer('ram', memory_bytes=6 * GIB, ram_bytes=6 * GIB, blocking=False):
            self.assertFalse(policy.ready('file', memory_bytes=6 * GIB, ram_bytes=0))

    def test_unknown_configured_backing_is_not_disabled_or_healthy(self):
        unknown = replace(self.pressure(), memory_backing=MemoryBackingCapacity())
        decision = decide_resident_wait(unknown, MemoryDemand(0, 0))
        self.assertTrue(decision.memory_reclaim)
        self.assertTrue(decision.backing_reclaim)
        self.assertEqual(decision.reason, "memory_backing_unavailable")
        self.assertGreater(decision.target_bytes, 0)

    def test_host_memory_still_limits_when_backing_has_space(self):
        pressure = replace(
            self.pressure(80 * GIB),
            memory_fraction=0.02,
            memory_available_bytes=2 * GIB,
        )
        decision = decide_resident_wait(pressure, MemoryDemand(0, 0))
        self.assertTrue(decision.memory_reclaim)
        self.assertFalse(decision.backing_reclaim)
        self.assertEqual(decision.reason, "memory_headroom")

    def test_io_pressure_limits_wave_but_cannot_stall_backing_reclaim(self):
        decision = decide_resident_wait(replace(self.pressure(), io_stall=90), MemoryDemand(0, 0))
        self.assertTrue(decision.reclaim)
        self.assertEqual(decision.target_bytes, 5 * GIB)

    def test_tmpfs_deficit_does_not_evict_unhelpful_clean_host_cache(self):
        policy = WarmParkPolicy(self.pressure)
        sample = ResidentMemorySample(GIB, 0, GIB, 0, 0, 0, "/cg/s", 1, 2, 100, 200, 0)
        key = ("agent", 1, "request")
        with policy.defer(key, memory_bytes=sample.current_bytes, blocking=False):
            self.assertEqual(policy.cache_reclaim_target(key, sample), 0)
            self.assertEqual(policy.snapshot()["cache_reclaim_attempts"], 0)
            self.assertGreater(policy.snapshot()["reclaim_target_bytes"], 0)

    def test_pressure_refresh_preserves_configured_unknown_after_sample_expiry(self):
        with TemporaryDirectory() as temporary:
            proc = Path(temporary)
            (proc / "meminfo").write_text(
                "MemTotal: 104857600 kB\nMemAvailable: 20971520 kB\n"
            )
            sampler = PressureSampler(proc, memory_backing_root=Path("/active-memory"))
            with (
                patch(
                    "ucloud_sandboxes.background_io.time.monotonic", return_value=100
                ),
                patch(
                    "ucloud_sandboxes.background_io.sample_memory_backing",
                    return_value=self.pressure().memory_backing,
                ) as read,
            ):
                first = sampler.sample()
                self.assertIs(sampler.sample(), first)
                read.assert_called_once_with(Path("/active-memory"), proc_root=proc)
            with (
                patch(
                    "ucloud_sandboxes.background_io.time.monotonic", return_value=101
                ),
                patch(
                    "ucloud_sandboxes.background_io.sample_memory_backing",
                    return_value=MemoryBackingCapacity(),
                ),
            ):
                newer = sampler.sample()
            self.assertEqual(newer.memory_backing, MemoryBackingCapacity())
            self.assertTrue(decide_resident_wait(newer, MemoryDemand(0, 0)).backing_reclaim)

    def test_runtime_binds_actual_application_memory_root_to_policy(self):
        service = _WakeService()
        service.warden = SimpleNamespace(
            config=SimpleNamespace(application_memory_root=Path("/active-memory"))
        )
        runtime = DirectNodeRuntime(service)
        with patch(
            "ucloud_sandboxes.background_io.sample_memory_backing",
            return_value=self.pressure().memory_backing,
        ) as sample:
            observed = runtime._warm_parks.pressure()
        sample.assert_called_once_with(Path("/active-memory"), proc_root=Path("/proc"))
        self.assertEqual(observed.memory_backing, self.pressure().memory_backing)
