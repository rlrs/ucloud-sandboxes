from dataclasses import replace
from contextlib import ExitStack
import os
import time
import unittest
from unittest.mock import patch

from tests.test_node_runtime import _WakeService
from ucloud_sandboxes.background_io import Pressure
from ucloud_sandboxes.node_runtime import DirectNodeRuntime
from ucloud_sandboxes.resident_memory import ResidentMemorySample, ResidentReclaimResult
from ucloud_sandboxes.warm_park import WarmParkPolicy, WarmParkDeferred
from ucloud_sandboxes.transition_admission import MemoryDemand

MIB = 1024**2


class ResidentCachePolicyTests(unittest.TestCase):
    def setUp(self):
        self.pressure = Pressure(0.01, 0, 0, 64 * MIB)
        self.policy = WarmParkPolicy(lambda: self.pressure)
        self.sample = ResidentMemorySample(
            1536 * MIB, 16 * MIB, 1500 * MIB, 0, 0, 0, "/cg/s", 1, 2, 100, 200, 0
        )
        self.key = ("agent", 1, "request")

    def observe_wait_then_sample(self, runtime):
        pressure, self.pressure = self.pressure, Pressure(.8, 0, 0, 8 * 1024**3)
        with self.assertRaises(WarmParkDeferred):
            runtime.park_with_activity_revision(
                "agent", operation_id="wait", generation=1, relay_request_id="request"
            )
        self.sample = replace(self.sample, sampled_at=time.monotonic())
        self.pressure = pressure

    def test_cache_probe_needs_real_deficit_and_avoids_io_pressure(self):
        with self.policy.defer(
            self.key, memory_bytes=self.sample.current_bytes, blocking=False
        ):
            self.pressure = Pressure(0.5, 0, 0, 4 * 1024**3)
            self.assertEqual(self.policy.cache_reclaim_target(self.key, self.sample), 0)
            self.pressure = Pressure(0.01, 0, 50, 64 * MIB)
            self.assertEqual(self.policy.cache_reclaim_target(self.key, self.sample), 0)
            self.pressure = Pressure(0.01, 0, 0, 64 * MIB)
            self.assertEqual(
                self.policy.cache_reclaim_target(self.key, self.sample), 256 * MIB
            )
            self.assertEqual(
                self.policy.snapshot()["projected_reclaim_bytes"], 256 * MIB
            )
            self.assertEqual(self.policy.cache_reclaim_target(self.key, self.sample), 0)

    def test_tmpfs_working_set_and_unknown_samples_do_not_get_fake_credit(self):
        with self.policy.defer(
            self.key, memory_bytes=self.sample.current_bytes, blocking=False
        ):
            self.assertEqual(self.policy.cache_reclaim_target(self.key, None), 0)
            self.assertEqual(
                self.policy.cache_reclaim_target(
                    self.key, replace(self.sample, shared_memory_bytes=1500 * MIB)
                ),
                0,
            )

    def test_application_file_reclaim_is_deficit_sized_and_includes_owned_writeback(self):
        dirty = replace(self.sample, dirty_bytes=1024 * MIB)
        with self.policy.defer(self.key, memory_bytes=dirty.current_bytes, ram_bytes=0,
                               blocking=False):
            target = self.policy.cache_reclaim_target(
                self.key, dirty, application_file_backed=True)
            self.assertGreater(target, 256 * MIB)
            self.assertLessEqual(target, dirty.file_bytes)
            self.assertLessEqual(target, self.policy.snapshot()["reclaim_target_bytes"])
            self.assertEqual(self.policy.cache_reclaim_target(
                self.key, dirty, application_file_backed=True), 0)

    def test_application_heap_refault_does_not_force_a_full_checkpoint(self):
        with self.policy.defer(self.key, memory_bytes=self.sample.current_bytes, ram_bytes=0,
                               blocking=False):
            self.policy.cache_reclaim_target(self.key, self.sample, application_file_backed=True)
            self.policy.record_cache_reclaim(self.key, self.sample,
                ResidentReclaimResult(512 * MIB, 512 * MIB, .2, 0, "target_reached"))
        self.policy.wake(self.key)
        next_key = ("agent", 1, "next-request")
        hot = replace(self.sample, refault_file_pages=512 * MIB // os.sysconf("SC_PAGE_SIZE"))
        with patch("ucloud_sandboxes.warm_park.time.monotonic", return_value=time.monotonic() + 1), \
                self.policy.defer(next_key, memory_bytes=hot.current_bytes, ram_bytes=0,
                                  blocking=False):
            self.assertGreater(self.policy.cache_reclaim_target(
                next_key, hot, application_file_backed=True), 256 * MIB)
        self.assertEqual(self.policy.snapshot()["cache_refault_backoffs"], 0)

    def test_reclaim_windows_stop_when_memory_or_storage_pressure_changes(self):
        with self.policy.defer(self.key, memory_bytes=self.sample.current_bytes,
                               ram_bytes=0, blocking=False):
            self.policy.cache_reclaim_target(self.key, self.sample,
                                             application_file_backed=True)
            self.assertTrue(self.policy.cache_reclaim_still_needed(self.key))
            self.pressure = Pressure(.5, 0, 0, 4 << 30)
            self.assertFalse(self.policy.cache_reclaim_still_needed(self.key))
            self.pressure = Pressure(.01, 0, 50, 64 * MIB)
            self.assertFalse(self.policy.cache_reclaim_still_needed(self.key))
            self.assertTrue(self.policy.cache_reclaim_still_needed(
                self.key, application_file_backed=True))

    def test_file_reclaim_under_storage_pressure_uses_existing_shared_byte_budget(self):
        # Even a burst of known heaps cannot all flush for one shared deficit.
        policy = WarmParkPolicy(lambda: Pressure(.05, 0, 50, 4 << 30),
                                demand=lambda: MemoryDemand(4 << 30, 0))
        targets = []
        with ExitStack() as stack:
            for index in range(32):
                key = (str(index), 1, "request")
                try:
                    stack.enter_context(policy.defer(key, memory_bytes=self.sample.current_bytes,
                                                      ram_bytes=0, blocking=False))
                except WarmParkDeferred:
                    continue
                targets.append(policy.cache_reclaim_target(key, self.sample,
                                                          application_file_backed=True))
            self.assertGreater(sum(targets), 0)
            self.assertLessEqual(sum(targets), policy.snapshot()["reclaim_target_bytes"])
            self.assertLess(len(targets), 32)
            self.assertEqual(sum(targets), policy.snapshot()["projected_reclaim_bytes"])

    def test_rapid_refault_backs_off_new_waits_then_allows_a_later_probe(self):
        with patch("ucloud_sandboxes.warm_park.time.monotonic", return_value=10):
            with self.policy.defer(
                self.key, memory_bytes=self.sample.current_bytes, blocking=False
            ):
                self.assertGreater(
                    self.policy.cache_reclaim_target(self.key, self.sample), 0
                )
                self.policy.record_cache_reclaim(
                    self.key,
                    self.sample,
                    ResidentReclaimResult(
                        256 * MIB, 256 * MIB, 0.1, 0, "target_reached"
                    ),
                )
            self.policy.wake(self.key)
        hot = replace(
            self.sample, refault_file_pages=256 * MIB // os.sysconf("SC_PAGE_SIZE")
        )
        next_key = ("agent", 1, "request2")
        with patch("ucloud_sandboxes.warm_park.time.monotonic", return_value=12):
            with self.policy.defer(
                next_key, memory_bytes=hot.current_bytes, blocking=False
            ):
                self.assertEqual(self.policy.cache_reclaim_target(next_key, hot), 0)
            self.policy.wake(next_key)
        with patch("ucloud_sandboxes.warm_park.time.monotonic", return_value=43):
            with self.policy.defer(
                ("agent", 1, "request3"), memory_bytes=hot.current_bytes, blocking=False
            ):
                self.assertGreater(
                    self.policy.cache_reclaim_target(("agent", 1, "request3"), hot), 0
                )
        self.assertEqual(self.policy.snapshot()["cache_refault_backoffs"], 1)

    def test_runtime_reclaims_without_lifecycle_lock_and_does_not_checkpoint(self):
        service = _WakeService()
        service.provisioner.registry.relay_wake_fence = lambda *_: False
        service.resident_memory_sample = lambda *_: self.sample
        runtime = DirectNodeRuntime(service)
        runtime._warm_parks = self.policy
        self.observe_wait_then_sample(runtime)
        called = []

        def reclaim(*args, **kw):
            called.append(kw["target_bytes"])
            self.assertTrue(runtime.lifecycle.is_idle("agent"))
            self.assertTrue(kw["is_wait_current"]())
            with runtime.lifecycle.exclusive("agent"):
                self.assertFalse(kw["is_wait_current"]())
            return ResidentReclaimResult(256 * MIB, 256 * MIB, 0.1, 0, "target_reached")

        service.reclaim_resident_wait = reclaim
        with self.assertRaises(WarmParkDeferred):
            runtime.park_with_activity_revision(
                "agent", operation_id="wait", generation=1, relay_request_id="request"
            )
        self.assertEqual(called, [256 * MIB])
        self.assertEqual(service.park_calls, [])
        self.assertEqual(self.policy.snapshot()["cache_reclaimed_bytes"], 256 * MIB)

    def test_unsupported_or_empty_reclaim_still_progresses_to_checkpoint(self):
        service = _WakeService()
        service.provisioner.registry.relay_wake_fence = lambda *_: False
        service.resident_memory_sample = lambda *_: self.sample
        service.reclaim_resident_wait = lambda *a, **kw: ResidentReclaimResult(
            0, 0, 0.1, 0, "unsupported"
        )
        runtime = DirectNodeRuntime(service)
        runtime._warm_parks = self.policy
        self.observe_wait_then_sample(runtime)
        record, _ = runtime.park_with_activity_revision(
            "agent", operation_id="wait", generation=1, relay_request_id="request"
        )
        self.assertEqual(record.state, "parked")
        self.assertEqual(service.park_calls, ["agent"])

    def test_wake_during_reclaim_cancels_the_checkpoint_fallback(self):
        from ucloud_sandboxes.sandbox import SandboxConflictError

        service = _WakeService()
        service.provisioner.registry.relay_wake_fence = lambda *_: False
        service.resident_memory_sample = lambda *_: self.sample
        runtime = DirectNodeRuntime(service)
        runtime._warm_parks = self.policy
        self.observe_wait_then_sample(runtime)

        def reclaim(*args, **kw):
            runtime._warm_parks.wake(self.key)
            self.assertFalse(kw["is_wait_current"]())
            return ResidentReclaimResult(0, 0, 0.1, 0, "superseded")

        service.reclaim_resident_wait = reclaim
        with self.assertRaises(SandboxConflictError):
            runtime.park_with_activity_revision(
                "agent", operation_id="wait", generation=1, relay_request_id="request"
            )
        self.assertEqual(service.park_calls, [])
