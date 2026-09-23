from pathlib import Path
from tempfile import TemporaryDirectory
import time
import unittest
from unittest.mock import patch

from ucloud_sandboxes.resident_memory import ResidentMemorySampler


class ResidentMemoryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name).resolve()
        self.proc = self.root / "proc"
        self.cg = self.root / "cgroup"
        self.container = "a" * 64
        self.path = self.cg / "ucloud-sandboxes" / self.container
        self.path.mkdir(parents=True)
        process = self.proc / "123"
        process.mkdir(parents=True)
        self.stat = process / "stat"
        self.stat.write_text("123 (sentry) S " + "0 " * 18 + "456 0")
        (process / "cgroup").write_text("0::/ucloud-sandboxes/" + self.container)
        (self.path / "memory.current").write_text("1000")
        (self.path / "memory.stat").write_text(
            "shmem 0\nanon 100\nfile 800\nfile_dirty 50\nfile_writeback 20\nworkingset_refault_file 7\n"
        )
        self.sampler = ResidentMemorySampler(cgroup_root=self.cg, proc_root=self.proc)

    def sample(self, **kwargs):
        return self.sampler.sample(
            ("s", 1),
            pid=123,
            start_time_ticks=456,
            container_id=self.container,
            **kwargs,
        )

    def test_samples_actual_charge_and_cost_counters_without_process_rss(self):
        sample = self.sample(expected_path="/ucloud-sandboxes/" + self.container)
        self.assertEqual(sample.current_bytes, 1000)
        self.assertEqual(sample.clean_file_bytes, 730)
        self.assertEqual(sample.refault_file_pages, 7)
        self.assertEqual(self.sampler.get(("s", 1)), sample)
        with patch(
            "ucloud_sandboxes.resident_memory.time.monotonic",
            return_value=time.monotonic() + 3,
        ):
            self.assertIsNone(self.sampler.get(("s", 1)))

    def test_pid_reuse_invalidates_observation_and_no_configured_limit_fallback(self):
        self.assertIsNotNone(self.sample())
        self.stat.write_text("123 (sentry) S " + "0 " * 18 + "999 0")
        self.assertIsNone(self.sample())
        self.assertIsNone(self.sampler.get(("s", 1)))

    def test_missing_or_stale_footprint_allows_only_one_capture_probe(self):
        from ucloud_sandboxes.background_io import Pressure
        from ucloud_sandboxes.warm_park import WarmParkDeferred, WarmParkPolicy

        for stale in (False, True):
            with self.subTest(stale=stale):
                sampled_at = time.monotonic()
                if stale:
                    self.assertIsNotNone(self.sample())
                else:
                    self.sampler.forget(("s", 1))
                with patch("time.monotonic", return_value=sampled_at + 4):
                    # Exercise the real cache age fence used by foreground and
                    # maintenance park requests, rather than inventing zero RSS.
                    self.assertIsNone(self.sampler.get(("s", 1)))
                    policy = WarmParkPolicy(
                        lambda: Pressure(.01, 20, 50, 1024**3)
                    )
                    with policy.defer("probe", memory_bytes=0, blocking=False):
                        for index in range(256):
                            with self.assertRaises(WarmParkDeferred):
                                with policy.defer(
                                    str(index), memory_bytes=0, blocking=False
                                ):
                                    self.fail("unknown samples must not fan out captures")
                        # Even a later measured candidate must wait until this
                        # unknown probe yields actual headroom evidence.
                        self.assertFalse(policy.ready("0", memory_bytes=1024**3))
                        self.assertEqual(policy.snapshot()["checkpoint_inflight"], 1)
                        self.assertEqual(policy.snapshot()["projected_reclaim_bytes"], 0)
                        policy.parked("probe")
                with patch("time.monotonic", return_value=sampled_at + 5):
                    with policy.defer("0", memory_bytes=0, blocking=False):
                        self.assertEqual(policy.snapshot()["checkpoint_inflight"], 1)

    def test_unknown_probe_waits_for_measured_capture_to_settle(self):
        from ucloud_sandboxes.background_io import Pressure
        from ucloud_sandboxes.warm_park import WarmParkDeferred, WarmParkPolicy

        policy = WarmParkPolicy(lambda: Pressure(.01, 20, 50, 1024**3))
        with policy.defer("measured", memory_bytes=1024**3, blocking=False):
            with self.assertRaises(WarmParkDeferred):
                with policy.defer("unknown", memory_bytes=0, blocking=False):
                    self.fail("unmeasured I/O must not join measured captures")

    def test_fresh_pre_wait_sample_cannot_price_a_later_heap(self):
        from tests.test_node_runtime import _WakeService
        from ucloud_sandboxes.background_io import Pressure
        from ucloud_sandboxes.node_runtime import DirectNodeRuntime
        from ucloud_sandboxes.warm_park import WarmParkDeferred, WarmParkPolicy

        now = time.monotonic()
        service = _WakeService()
        service.provisioner.registry.relay_wake_fence = lambda *_: False
        service.resident_memory_sample = lambda *_: self.sampler.get(("s", 1))
        runtime = DirectNodeRuntime(service)
        runtime._warm_parks = WarmParkPolicy(lambda: Pressure(.8, 0, 0, 8 * 1024**3))
        key = ('agent', 1, 'request')
        with patch('time.monotonic', return_value=now):
            self.assertIsNotNone(self.sample())
        with patch('time.monotonic', return_value=now + .1):
            with self.assertRaises(WarmParkDeferred):
                runtime.park_with_activity_revision(
                    'agent', generation=1, relay_request_id='request',
                    operation_id='park:request',
                )
            self.assertIsNotNone(self.sampler.get(("s", 1)))  # Cache still fresh.
            self.assertIsNone(runtime._resident_wait_memory_sample(key))
        with patch('time.monotonic', return_value=now + .2):
            (self.path / 'memory.current').write_text(str(1536 * 1024**2))
            self.sample()
            self.assertEqual(
                runtime._resident_wait_memory_sample(key).current_bytes,
                1536 * 1024**2,
            )

    def test_wrong_cgroup_and_incomplete_counters_stay_unknown(self):
        self.assertIsNone(self.sample(expected_path="/another/runtime"))
        (self.path / "memory.stat").write_text("shmem 0\nanon 100\nfile 800\n")
        self.assertIsNone(self.sample())
        self.sampler.retain(())
        self.assertIsNone(self.sampler.get(("s", 1)))

    def test_shared_parent_cgroup_cannot_be_misattributed_to_incarnation(self):
        (self.proc / "123" / "cgroup").write_text("0::/ucloud-sandboxes")
        self.assertIsNone(self.sample())

    def test_tmpfs_pages_are_not_counted_as_reclaimable_without_swap(self):
        counters = self.path / "memory.stat"
        counters.write_text(counters.read_text().replace("shmem 0", "shmem 800"))
        sample = self.sample()
        self.assertEqual(sample.clean_file_bytes, 0)
        from ucloud_sandboxes.resident_memory import ResidentMemoryReclaimer

        report = ResidentMemoryReclaimer(self.sampler).reclaim(
            ("s", 1), sample, target_bytes=900, is_current=lambda: True
        )
        self.assertEqual(report.reason, "no_reclaimable_cache")
        self.assertEqual(report.requested_bytes, 0)

    def test_reclaim_windows_report_actual_progress_and_cancel_on_wake(self):
        from ucloud_sandboxes.resident_memory import ResidentMemoryReclaimer

        (self.path / "memory.reclaim").touch()
        sample = self.sample()
        alive = True
        calls = []

        def write(fd, data):
            nonlocal alive
            calls.append(data)
            (self.path / "memory.current").write_text("900")
            alive = False
            return len(data)

        with patch("os.write", side_effect=write):
            report = ResidentMemoryReclaimer(self.sampler).reclaim(
                ("s", 1),
                sample,
                target_bytes=700,
                is_current=lambda: alive,
                window_bytes=200,
            )
        self.assertEqual(calls, [b"200 swappiness=0"])
        self.assertEqual(report.requested_bytes, 200)
        self.assertEqual(report.reclaimed_bytes, 100)
        self.assertEqual(report.reason, "superseded")

    def test_unsupported_reclaim_does_not_fall_back_to_anonymous_swap(self):
        import errno
        from ucloud_sandboxes.resident_memory import ResidentMemoryReclaimer

        (self.path / "memory.reclaim").touch()
        with patch(
            "os.write", side_effect=OSError(errno.EINVAL, "unsupported")
        ) as write:
            report = ResidentMemoryReclaimer(self.sampler).reclaim(
                ("s", 1), self.sample(), target_bytes=500, is_current=lambda: True
            )
        self.assertEqual(write.call_count, 1)
        self.assertEqual(report.reason, "unsupported")
        self.assertEqual(report.reclaimed_bytes, 0)

    def test_cgroup_replacement_cannot_receive_reclaim(self):
        from ucloud_sandboxes.resident_memory import ResidentMemoryReclaimer

        sample = self.sample()
        self.path.rename(self.path.with_name("old"))
        self.path.mkdir()
        (self.path / "memory.reclaim").touch()
        with patch("os.write") as write:
            report = ResidentMemoryReclaimer(self.sampler).reclaim(
                ("s", 1), sample, target_bytes=500, is_current=lambda: True
            )
        write.assert_not_called()
        self.assertEqual(report.reason, "cgroup_changed")
