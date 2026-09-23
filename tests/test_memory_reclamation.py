from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Event
import unittest
from unittest.mock import patch

from ucloud_sandboxes.checkpoint_components import MemoryBackingRef
from ucloud_sandboxes.memory_backing import FilesystemTrim, MemoryBackingStore
from tests.test_split_memory_lifecycle import FakeQuota


class MemoryReclamationTests(unittest.TestCase):
    def test_shared_trim_covers_only_deletions_before_its_start(self):
        settling, begin = Event(), Event()
        joined = Event()
        class ObservedFuture(Future):
            def result(self, timeout=None):
                joined.set()
                return super().result(timeout)
        passes = []
        def settle(_):
            settling.set()
            self.assertTrue(begin.wait(3))
        def trim():
            passes.append(1)
        with patch("ucloud_sandboxes.memory_backing.time.sleep", settle), patch(
            "ucloud_sandboxes.memory_backing.Future", ObservedFuture
        ), ThreadPoolExecutor(2) as threads:
            barrier = FilesystemTrim(trim)
            first = threads.submit(barrier.release)
            self.assertTrue(settling.wait(3))
            second = threads.submit(barrier.release)
            self.assertTrue(joined.wait(3))
            begin.set()
            first.result(3)
            second.result(3)
        self.assertEqual(len(passes), 1)

        entered, finish = Event(), Event()
        next_entered, next_finish = Event(), Event()
        def two_passes():
            passes.append(1)
            current, release = (entered, finish) if len(passes) == 2 else (next_entered, next_finish)
            current.set()
            self.assertTrue(release.wait(3))
        barrier = FilesystemTrim(two_passes, settle_seconds=0)
        with ThreadPoolExecutor(2) as threads:
            first = threads.submit(barrier.release)
            self.assertTrue(entered.wait(3))
            second = threads.submit(barrier.release)
            finish.set()
            first.result(3)
            self.assertTrue(next_entered.wait(3))
            self.assertFalse(second.done())
            next_finish.set()
            second.result(3)
        self.assertEqual(len(passes), 3)

    def test_slow_physical_reclaim_retains_claim_without_blocking_other_allocations(self):
        entered, finish = Event(), Event()
        class SlowQuota(FakeQuota):
            def release(self, root, project_id):
                entered.set()
                if not finish.wait(3):
                    raise AssertionError("reclaim was not released")
                super().release(root, project_id)
        with TemporaryDirectory() as directory, ThreadPoolExecutor(1) as threads:
            root = Path(directory)
            store = MemoryBackingStore(root / "memory", root / "state.sqlite",
                                       hard_capacity_bytes=1000, quota=SlowQuota())
            first = MemoryBackingRef("first.sandbox-1", 100)
            second = MemoryBackingRef("second.sandbox-1", 900)
            store.prepare(first, sandbox_id="first", sandbox_generation=1)
            deletion = threads.submit(store.delete, first, sandbox_id="first", sandbox_generation=1)
            try:
                self.assertTrue(entered.wait(3))
                store.prepare(second, sandbox_id="second", sandbox_generation=1)
                store.require(second, sandbox_id="second", sandbox_generation=1)
                self.assertEqual(store.metrics()["memory_backing_hard_reserved_bytes"], 1000)
            finally:
                finish.set()
            deletion.result(3)
            self.assertEqual(store.metrics()["memory_backing_hard_reserved_bytes"], 900)

    def test_failed_trim_does_not_release_claim_and_can_be_retried(self):
        class FailingQuota(FakeQuota):
            def release(self, root, project_id):
                if self.fail:
                    raise OSError("injected trim failure")
                super().release(root, project_id)
        with TemporaryDirectory() as directory:
            root = Path(directory)
            quota = FailingQuota()
            store = MemoryBackingStore(root / "memory", root / "state.sqlite",
                                       hard_capacity_bytes=1000, quota=quota)
            ref = MemoryBackingRef("first.sandbox-1", 100)
            store.prepare(ref, sandbox_id="first", sandbox_generation=1)
            quota.fail = True
            with self.assertRaisesRegex(OSError, "trim failure"):
                store.delete(ref, sandbox_id="first", sandbox_generation=1)
            self.assertEqual(store.metrics()["memory_backing_hard_reserved_bytes"], 100)
            quota.fail = False
            store.delete(ref, sandbox_id="first", sandbox_generation=1)
            self.assertEqual(store.metrics()["memory_backing_hard_reserved_bytes"], 0)
