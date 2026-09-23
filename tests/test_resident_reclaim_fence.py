from types import SimpleNamespace
from unittest.mock import patch
import unittest

from tests import test_managed_control_admission as fixture
from ucloud_sandboxes.hibernation import HibernationState
from ucloud_sandboxes.resident_memory import ResidentMemorySample, ResidentReclaimResult


class ResidentReclaimFenceTests(unittest.TestCase):
    def file_service(self):
        item = fixture.ManagedControlAdmissionTests()
        item.setUp()
        self.addCleanup(item.doCleanups)
        service = item.service
        item.registration.spec = SimpleNamespace(parkable=True)
        sample = ResidentMemorySample(1536 << 20, 16 << 20, 1500 << 20,
                                     384 << 20, 0, 0, "/cg/s", 1, 2, 100, 200, 0)
        lifecycle = SimpleNamespace(state=HibernationState.RUNNING,
                                    sentry_pid=100, sentry_start_time_ticks=200)
        service.warden.inspect_snapshot = lambda _: lifecycle
        service.warden.application_memory_mode = lambda *_: "file"
        service.warden.config.reflink_memory_restore = True
        service.resident_memory_sample = lambda *_: sample
        service.provisioner.registry.get = lambda _: item.current
        service.provisioner.registry.relay_wake_fence = lambda *_: False
        return service, sample

    def test_application_writeback_rechecks_wait_before_kernel_reclaim(self):
        service, sample = self.file_service()
        current = True

        def flush(_):
            nonlocal current
            with service._try_lock("s", 1) as available:
                self.assertTrue(available, "writeback inherited the owner lock")
            current = False
            return True

        service.warden.flush_reclaimable_memory = flush
        with patch("ucloud_sandboxes.direct_service.ResidentMemoryReclaimer.reclaim") as reclaim:
            result = service.reclaim_resident_wait("s", generation=1,
                relay_request_id="request", target_bytes=1024 << 20,
                is_wait_current=lambda: current)
        self.assertEqual(result.reason, "superseded")
        reclaim.assert_not_called()

    def test_application_writeback_refreshes_same_incarnation_before_reclaim(self):
        from dataclasses import replace
        service, sample = self.file_service()
        clean = replace(sample, dirty_bytes=0, sampled_at=1)
        flushed = []
        service.warden.flush_reclaimable_memory = lambda _: flushed.append(True) or True
        with patch.object(service._resident_memory, "sample", return_value=clean) as refresh, \
                patch("ucloud_sandboxes.direct_service.ResidentMemoryReclaimer.reclaim",
                      return_value=ResidentReclaimResult(1024 << 20, 1024 << 20, .1, 0,
                                                        "target_reached")) as reclaim:
            result = service.reclaim_resident_wait("s", generation=1,
                relay_request_id="request", target_bytes=1024 << 20,
                is_wait_current=lambda: True)
        self.assertEqual(flushed, [True])
        refresh.assert_called_once()
        self.assertIs(reclaim.call_args.args[1], clean)
        self.assertEqual(result.reclaimed_bytes, 1024 << 20)

    def test_application_writeback_cgroup_replacement_has_no_reclaim_credit(self):
        from dataclasses import replace
        service, sample = self.file_service()
        service.warden.flush_reclaimable_memory = lambda _: True
        with patch.object(service._resident_memory, "sample",
                          return_value=replace(sample, cgroup_inode=99)), \
                patch("ucloud_sandboxes.direct_service.ResidentMemoryReclaimer.reclaim") as reclaim:
            result = service.reclaim_resident_wait("s", generation=1,
                relay_request_id="request", target_bytes=1024 << 20,
                is_wait_current=lambda: True)
        self.assertEqual(result.reason, "cgroup_changed")
        reclaim.assert_not_called()

    def test_reclaim_uses_short_source_fences_and_stops_on_activity_or_wake(self):
        item = fixture.ManagedControlAdmissionTests()
        item.setUp()
        self.addCleanup(item.doCleanups)
        service = item.service
        item.registration.spec = SimpleNamespace(parkable=True)
        sample = SimpleNamespace(sentry_pid=100, sentry_start_time_ticks=200)
        lifecycle = SimpleNamespace(
            state=HibernationState.RUNNING, sentry_pid=100, sentry_start_time_ticks=200
        )
        service.warden.inspect_snapshot = lambda _: lifecycle
        service.warden.application_memory_mode = lambda *_: "ram"
        service.resident_memory_sample = lambda *_: sample
        service._resident_memory = object()
        waking = False
        service.provisioner = SimpleNamespace(
            registry=SimpleNamespace(
                get=lambda _: item.current, relay_wake_fence=lambda *_: waking
            )
        )

        def reclaim(*args, **kw):
            nonlocal waking
            with service._try_lock("s", 1) as available:
                self.assertTrue(available, "kernel reclaim inherited the owner lock")
            self.assertTrue(kw["is_current"]())
            service._last_activity[("s", 1)] = 42
            self.assertFalse(kw["is_current"]())
            service._last_activity.clear()
            waking = True
            self.assertFalse(kw["is_current"]())
            waking = False
            lifecycle.state = HibernationState.PARKED
            # A real journal is immutable and replaced on transition.
            service.warden.inspect_snapshot = lambda _: SimpleNamespace(
                state=HibernationState.PARKED
            )
            self.assertFalse(kw["is_current"]())
            return ResidentReclaimResult(0, 0, 0, 0, "superseded")

        with patch(
            "ucloud_sandboxes.direct_service.ResidentMemoryReclaimer.reclaim",
            side_effect=reclaim,
        ):
            result = service.reclaim_resident_wait(
                "s",
                generation=1,
                relay_request_id="request",
                target_bytes=1024,
                is_wait_current=lambda: True,
            )
        self.assertEqual(result.reason, "superseded")
