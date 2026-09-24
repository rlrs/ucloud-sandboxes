from unittest import TestCase
from unittest.mock import Mock, patch

from ucloud_sandboxes.background_io import Pressure
from ucloud_sandboxes.warm_park import WarmParkDeferred, WarmParkPolicy


class ResidentWaitRankingTests(TestCase):
    GIB = 1024**3

    def setUp(self):
        self.now = 0.0
        self.pressure = Pressure(0.8, 0, 0, 80 * self.GIB)
        self.clock = patch(
            "ucloud_sandboxes.warm_park.time.monotonic", side_effect=lambda: self.now
        )
        self.clock.start()
        self.addCleanup(self.clock.stop)
        self.policy = WarmParkPolicy(lambda: self.pressure)

    def train(self, sandbox, *, wait=100, park=2, wake=2, memory=None, generation=1):
        memory = memory or 4 * self.GIB
        start = self.now
        key = (sandbox, generation, "training")
        self.pressure = Pressure(0.04, 0, 0, 4 * self.GIB)
        with self.policy.defer(key, memory_bytes=memory, blocking=False):
            self.now = start + park
            self.policy.parked(key)
            self.policy.parked(key)  # An exact replay must not double-count cost.
        self.now = start + wait
        observation = self.policy.wake(key)
        self.now += wake
        self.policy.record_wake(key, observation)
        self.now += 1
        self.pressure = Pressure(0.8, 0, 0, 80 * self.GIB)
        return self.policy._history[(sandbox, generation)]

    def retain(self, sandbox, *, memory=None, generation=1, request="next"):
        key = (sandbox, generation, request)
        with self.assertRaises(WarmParkDeferred):
            with self.policy.defer(
                key, memory_bytes=memory or 4 * self.GIB, blocking=False
            ):
                self.fail("memory headroom must retain even an observed long wait")
        return key

    def pressure_on(self):
        self.pressure = Pressure(0.04, 0, 0, 4 * self.GIB)

    def test_response_burst_can_settle_without_checkpointing(self):
        from ucloud_sandboxes.transition_admission import MemoryDemand
        key = self.retain("agent")
        demand = [MemoryDemand(79 * self.GIB)]
        self.policy.demand = lambda: demand[0]
        self.assertFalse(self.policy.ready(key, memory_bytes=self.GIB))
        self.now += .9
        self.assertFalse(self.policy.ready(key, memory_bytes=self.GIB))
        demand[0] = MemoryDemand()
        self.assertFalse(self.policy.ready(key, memory_bytes=self.GIB))
        # A later independent burst gets its own grace, not the first timestamp.
        self.now += 10
        demand[0] = MemoryDemand(79 * self.GIB)
        self.assertFalse(self.policy.ready(key, memory_bytes=self.GIB))

    def test_persistent_forecast_probes_then_rechecks_instead_of_mass_parking(self):
        from ucloud_sandboxes.transition_admission import MemoryDemand
        first = self.retain("first", memory=self.GIB)
        self.now += .1
        second = self.retain("second", memory=self.GIB)
        demand = [MemoryDemand(85 * self.GIB)]
        self.policy.demand = lambda: demand[0]
        self.assertFalse(self.policy.ready(first, memory_bytes=self.GIB))
        self.now += 1
        with self.policy.defer(first, memory_bytes=self.GIB, blocking=False):
            self.assertTrue(self.policy.ready(first, memory_bytes=self.GIB))
            self.assertFalse(self.policy.ready(second, memory_bytes=self.GIB))
            self.policy.parked(first)
        self.now += .3
        with self.policy.defer(second, memory_bytes=self.GIB, blocking=False):
            demand[0] = MemoryDemand()
            self.assertFalse(self.policy.ready(second, memory_bytes=self.GIB))

    def test_physical_shortage_bypasses_forecast_grace_and_probe_serialization(self):
        from ucloud_sandboxes.transition_admission import MemoryDemand
        first = self.retain("first", memory=self.GIB)
        self.now += .1
        second = self.retain("second", memory=self.GIB)
        self.policy.demand = lambda: MemoryDemand(85 * self.GIB)
        self.assertFalse(self.policy.ready(first, memory_bytes=self.GIB))
        self.pressure = Pressure(.01, 0, 0, self.GIB)
        with self.policy.defer(first, memory_bytes=self.GIB, blocking=False):
            with self.policy.defer(second, memory_bytes=self.GIB, blocking=False):
                self.assertEqual(self.policy.snapshot()["checkpoint_inflight"], 2)

    def test_synchronized_small_heap_burst_does_not_launch_a_checkpoint_wave(self):
        from contextlib import ExitStack
        from ucloud_sandboxes.transition_admission import MemoryDemand
        # One worker's share of 256 agents on eight workers. Actual heaps fit;
        # simultaneous continuations temporarily reserve much larger bounds.
        keys = [self.retain(str(i), memory=self.GIB) for i in range(32)]
        incoming = [MemoryDemand(90 * self.GIB)]
        self.policy.demand = lambda: incoming[0]
        for key in keys:
            self.assertFalse(self.policy.ready(key, memory_bytes=self.GIB))
        self.now += 1
        with ExitStack() as stack:
            admitted = []
            for key in keys:
                try:
                    stack.enter_context(self.policy.defer(
                        key, memory_bytes=self.GIB, blocking=False))
                except WarmParkDeferred:
                    continue
                admitted.append(key)
            self.assertEqual(len(admitted), 1)
            incoming[0] = MemoryDemand(20 * self.GIB)
            # The admitted but not yet started capture is also canceled by the
            # runtime's final check when active agents return to safe waits.
            self.assertFalse(self.policy.ready(admitted[0], memory_bytes=self.GIB))
        self.assertEqual(self.policy.snapshot()["checkpoint_inflight"], 0)
        self.assertEqual(self.policy.snapshot()["checkpoints_completed"], 0)

    def test_backing_shortage_bypasses_forecast_grace(self):
        from dataclasses import replace
        from ucloud_sandboxes.resource_evidence import MemoryBackingCapacity
        from ucloud_sandboxes.transition_admission import MemoryDemand
        key = self.retain("agent")
        self.policy.demand = lambda: MemoryDemand(85 * self.GIB)
        self.assertFalse(self.policy.ready(key, memory_bytes=self.GIB))
        self.pressure = replace(self.pressure, memory_backing=MemoryBackingCapacity(
            total_bytes=90 * self.GIB, available_bytes=self.GIB))
        self.assertTrue(self.policy.ready(key, memory_bytes=self.GIB, ram_bytes=self.GIB))

    def test_memory_psi_with_abundant_headroom_does_not_hibernate(self):
        key = self.retain("agent")
        self.pressure = Pressure(.67, 12.52, 7.68, 67 * self.GIB)
        self.assertFalse(self.policy.ready(key, memory_bytes=self.GIB))
        self.assertEqual(self.policy.snapshot()["reclaim_target_bytes"], 0)

    def test_io_pressure_drains_reclaim_then_allows_progress(self):
        first = self.retain("first", memory=self.GIB)
        self.now += 1
        second = self.retain("second", memory=self.GIB)
        self.pressure = Pressure(0.01, 20, 50, self.GIB)
        with self.policy.defer(first, memory_bytes=self.GIB, blocking=False):
            self.assertTrue(self.policy.ready(first, memory_bytes=self.GIB))
            self.assertFalse(self.policy.ready(second, memory_bytes=self.GIB))
            with self.assertRaises(WarmParkDeferred):
                with self.policy.defer(second, memory_bytes=self.GIB, blocking=False):
                    self.fail("saturated storage admitted another capture")
            self.policy.parked(first)
        self.now += 5  # Allow the existing settle window to expire.
        with self.policy.defer(second, memory_bytes=self.GIB, blocking=False):
            self.assertEqual(self.policy.snapshot()["checkpoint_inflight"], 1)

    def test_storage_recovery_restores_byte_based_parallelism(self):
        first = self.retain("first", memory=self.GIB)
        self.now += 1
        second = self.retain("second", memory=self.GIB)
        self.pressure = Pressure(0.01, 20, 50, self.GIB)
        with self.policy.defer(first, memory_bytes=self.GIB, blocking=False):
            self.assertFalse(self.policy.ready(second, memory_bytes=self.GIB))
            self.pressure = Pressure(0.01, 0, 0, self.GIB)
            with self.policy.defer(second, memory_bytes=self.GIB, blocking=False):
                self.assertEqual(self.policy.snapshot()["checkpoint_inflight"], 2)

    def test_fresh_long_wait_precedes_old_near_response_wait(self):
        history = self.train("old")
        self.assertEqual(len(history.parks), 1)
        self.train("fresh")
        old = self.retain("old")
        self.now += 97
        fresh = self.retain("fresh")
        self.pressure_on()
        self.assertFalse(self.policy.ready(old, memory_bytes=4 * self.GIB))
        self.assertTrue(self.policy.ready(fresh, memory_bytes=4 * self.GIB))
        with self.policy.defer(fresh, memory_bytes=4 * self.GIB, blocking=False):
            self.assertFalse(self.policy.ready(old, memory_bytes=4 * self.GIB))
            self.assertEqual(
                self.policy.snapshot()["projected_reclaim_bytes"], 4 * self.GIB
            )

    def test_measured_transition_cost_breaks_same_remaining_wait_tie(self):
        self.train("expensive", park=30, wake=30)
        self.train("cheap", park=1, wake=1)
        expensive, cheap = self.retain("expensive"), self.retain("cheap")
        self.pressure_on()
        self.assertTrue(self.policy.ready(cheap, memory_bytes=4 * self.GIB))
        self.assertFalse(self.policy.ready(expensive, memory_bytes=4 * self.GIB))

    def test_larger_live_heap_cannot_reuse_tiny_checkpoint_cost(self):
        self.train("grew", park=1, wake=1, memory=self.GIB)
        self.train("stable", park=2, wake=2)
        grew, stable = self.retain("grew"), self.retain("stable")
        self.pressure_on()
        self.assertFalse(self.policy.ready(grew, memory_bytes=4 * self.GIB))
        self.assertTrue(self.policy.ready(stable, memory_bytes=4 * self.GIB))

    def test_missing_history_stays_fifo_and_deficit_always_progresses(self):
        first = self.retain("unknown1")
        self.now += 1
        second = self.retain("unknown2")
        self.pressure_on()
        with self.policy.defer(first, memory_bytes=4 * self.GIB, blocking=False):
            self.assertFalse(self.policy.ready(second, memory_bytes=4 * self.GIB))
        self.policy.forget(first)
        self.now += 2
        with self.policy.defer(second, memory_bytes=4 * self.GIB, blocking=False):
            self.policy.parked(second)

    def test_response_ready_prefers_other_wait_but_never_blocks_reclamation(self):
        ready = self.retain("ready")
        self.now += 1
        waiting = self.retain("waiting")
        self.policy.response_ready(ready)
        self.pressure_on()
        self.assertFalse(self.policy.ready(ready, memory_bytes=4 * self.GIB))
        self.assertTrue(self.policy.ready(waiting, memory_bytes=4 * self.GIB))
        self.policy.response_ready(waiting)
        # All responses ready is not permission to retain every live heap.
        with self.policy.defer(ready, memory_bytes=4 * self.GIB, blocking=False):
            self.policy.parked(ready)

    def test_observed_model_wait_excludes_our_admission_queue_delay(self):
        key = self.retain("agent")
        self.now = 20
        self.policy.response_ready(key)
        self.now = 80
        self.policy.response_ready(key)  # A transport retry cannot move arrival.
        self.policy.wake(key)
        self.assertEqual(tuple(self.policy._history[("agent", 1)].waits), (20,))
        self.assertNotIn(key, self.policy._response_ready_at)

    def test_unprofitable_wait_still_reclaims_when_it_is_the_only_choice(self):
        self.train("short", wait=6, park=5, wake=5)
        short = self.retain("short")
        self.pressure_on()
        with self.policy.defer(short, memory_bytes=4 * self.GIB, blocking=False):
            self.policy.parked(short)

    def test_generation_deletion_and_wake_failure_do_not_pollute_history(self):
        history = self.train("agent")
        self.assertEqual(list(history.waits), [100])  # Restore time is excluded.
        self.assertEqual(list(history.wakes), [(2, 4 * self.GIB)])
        key = self.retain("agent", generation=2)
        self.policy.forget(key)
        self.assertNotIn(("agent", 2), self.policy._history)
        key = self.retain("agent")
        self.pressure_on()
        with self.policy.defer(key, memory_bytes=4 * self.GIB, blocking=False):
            self.now += 2
            self.policy.parked(key)
        observation = self.policy.wake(key)
        # A restore failure never calls record_wake. Deletion then revokes any
        # late success observation rather than recreating an old incarnation.
        self.assertEqual(len(history.wakes), 1)
        self.policy.forget_incarnation("agent", 1)
        self.now += 10
        self.policy.record_wake(key, observation)
        self.assertNotIn(("agent", 1), self.policy._history)

    def test_observation_history_and_per_incarnation_samples_are_bounded(self):
        for index in range(40):
            self.train("same", wait=10 + index, park=1, wake=1)
        self.assertEqual(len(self.policy._history[("same", 1)].waits), 32)
        self.assertEqual(len(self.policy._history[("same", 1)].parks), 32)
        self.assertEqual(len(self.policy._history[("same", 1)].wakes), 32)
        for index in range(4100):
            key = self.retain(f"new-{index}")
            self.now += 1
            self.policy.wake(key)
        self.assertEqual(len(self.policy._history), 4096)

    def test_order_cache_does_not_bypass_cancellation_or_inflight_byte_credit(self):
        self.train("one")
        self.train("two")
        one, two = self.retain("one"), self.retain("two")
        self.pressure_on()
        self.assertTrue(self.policy.ready(one, memory_bytes=4 * self.GIB))
        self.policy.wake(one)
        with self.policy.defer(
            two, memory_bytes=4 * self.GIB, blocking=False
        ) as cancelled:
            self.policy.wake(two)
            self.assertTrue(cancelled.is_set())
        self.assertEqual(self.policy.snapshot()["projected_reclaim_bytes"], 0)

    def test_runtime_records_restore_cost_only_after_success(self):
        from tests.test_node_runtime import _WakeService
        from ucloud_sandboxes.node_runtime import DirectNodeRuntime

        service = _WakeService()
        service.provisioner.registry.relay_wake_fence = Mock(return_value=False)
        manager = DirectNodeRuntime(service)
        manager._warm_parks.record_wake = Mock()
        original = service.wake
        service.wake = Mock(side_effect=RuntimeError("restore failed"))
        with self.assertRaisesRegex(RuntimeError, "restore failed"):
            manager.wake_with_activity_revision(
                "agent", generation=1, operation_id="wake:r", relay_request_id="r"
            )
        manager._warm_parks.record_wake.assert_not_called()
        service.wake = original
        manager.wake_with_activity_revision(
            "agent", generation=1, operation_id="wake:r", relay_request_id="r"
        )
        manager._warm_parks.record_wake.assert_called_once()

    @staticmethod
    def hint(*, sequence=1, phase="model_wait", ttl=10, remaining=100):
        return dict(
            sequence=sequence,
            phase=phase,
            ttl_seconds=ttl,
            expected_remaining_wait_seconds=remaining,
            observed_at=1000.0,
            evaluated_at=1000.0,
            expires_at=1000.0 + ttl,
            registration_incarnation="a" * 64,
        )

    def test_phase_hint_changes_order_only_and_expires_inside_order_cache_interval(
        self,
    ):
        self.train("near")
        self.train("fresh")
        near = self.retain("near")
        self.now += 97
        fresh = self.retain("fresh")
        with patch("ucloud_sandboxes.warm_park.time.time", return_value=1000):
            self.policy.observe_phase(near, self.hint(ttl=0.1, remaining=300))
        self.assertFalse(self.policy.ready(near, memory_bytes=4 * self.GIB))
        self.pressure_on()
        self.assertTrue(self.policy.ready(near, memory_bytes=4 * self.GIB))
        self.assertFalse(self.policy.ready(fresh, memory_bytes=4 * self.GIB))
        self.now += 0.11
        self.assertFalse(self.policy.ready(near, memory_bytes=4 * self.GIB))
        self.assertTrue(self.policy.ready(fresh, memory_bytes=4 * self.GIB))

    def test_replayed_or_older_phase_does_not_extend_monotonic_lifetime(self):
        key = self.retain("agent")
        hint = self.hint(sequence=2)
        with patch("ucloud_sandboxes.warm_park.time.time", return_value=1000):
            self.policy.observe_phase(key, hint)
        self.now += 8
        with patch("ucloud_sandboxes.warm_park.time.time", return_value=900):
            self.policy.observe_phase(key, hint)
            self.policy.observe_phase(
                key, self.hint(sequence=1, ttl=300, remaining=300)
            )
        self.assertEqual(self.policy._advised_remaining(key, self.now), 92)
        self.now += 3
        self.assertIsNone(self.policy._advised_remaining(key, self.now))

    def test_new_non_wait_phase_clears_advice_and_generation_keys_do_not_share_it(self):
        key = self.retain("agent")
        with patch("ucloud_sandboxes.warm_park.time.time", return_value=1000):
            self.policy.observe_phase(key, self.hint())
            self.assertIsNone(
                self.policy._advised_remaining(("agent", 2, key[2]), self.now)
            )
            hint = self.hint(sequence=2, phase="tool")
            hint.pop("expected_remaining_wait_seconds")
            self.policy.observe_phase(key, hint)
        self.assertIsNone(self.policy._advised_remaining(key, self.now))
        self.policy.forget_incarnation("agent", 1)
        self.assertFalse(self.policy._phase_sequences)
