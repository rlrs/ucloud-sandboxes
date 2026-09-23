"""Physical cleanup is replayable maintenance, never wake authority."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path
import shutil
import sqlite3
from tempfile import TemporaryDirectory
from threading import Event
import unittest
from unittest.mock import patch

from tests.test_split_memory_lifecycle import FakeQuota
from ucloud_sandboxes.checkpoint_components import MemoryBackingRef
from ucloud_sandboxes.memory_backing import (
    MemoryBackingStore, MemoryBackingError, RetainedCheckpointRef, XfsMemoryQuota,
)


class RetiredMemoryMaintenanceTests(unittest.TestCase):
    def setUp(self):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name).resolve()
        self.quota = FakeQuota()
        self.store = MemoryBackingStore(self.root / "memory", self.root / "state.sqlite",
                                       hard_capacity_bytes=1 << 30, quota=self.quota)
        self.ref = MemoryBackingRef("s.sandbox-1", 64 << 20)
        self.owner = dict(sandbox_id="s", sandbox_generation=1)
        self.store.prepare(self.ref, **self.owner)
        self.store.configure_reflink_restore(True)

    def checkpoint(self, generation=1):
        checkpoint = RetainedCheckpointRef(self.ref, generation, "a" * 64)
        directory = self.store.root / self.ref.allocation_id / f"hibernate-{generation}"
        directory.mkdir(mode=0o700)
        path = directory / "application_memory.img"
        path.write_bytes(b"x" * 4096)
        path.chmod(0o600)
        size = max(4096, path.stat().st_blocks * 512)
        self.store.retain_checkpoint(self.ref, **self.owner, hibernation_generation=generation,
                                     manifest_sha256=checkpoint.manifest_sha256, allocated_bytes=size)
        shutil.rmtree(directory)
        return checkpoint

    def state(self, generation=1):
        with closing(sqlite3.connect(self.store.journal)) as connection:
            return connection.execute("SELECT project_id,state FROM retained_checkpoints "
                "WHERE allocation_id=? AND hibernation_generation=?",
                (self.ref.allocation_id, generation)).fetchone()

    def test_many_generations_share_one_batch_and_all_claims_outlive_physical_barrier(self):
        checkpoints = [self.checkpoint(index) for index in range(1, 9)]
        released = []
        def physical(_root, projects):
            self.assertEqual(len(projects), 8)
            self.assertEqual(released, [])
            self.assertTrue(all(self.state(index)[1] == "retiring" for index in range(1, 9)))
            self.store.require(self.ref, **self.owner)  # no owner mutation lock spans trim
        with patch.object(self.quota, "release_many", side_effect=physical) as release:
            self.assertEqual(self.store.release_retained_checkpoints(checkpoints,
                release_claim=released.append), 8)
        release.assert_called_once()
        self.assertEqual(released, checkpoints)
        self.assertTrue(all(self.state(index)[1] == "deleted" for index in range(1, 9)))

    def test_physical_failure_and_post_local_commit_crash_keep_replayable_claims(self):
        checkpoint = self.checkpoint()
        released = []
        with patch.object(self.quota, "release_many", side_effect=OSError("trim failed")):
            with self.assertRaisesRegex(OSError, "trim failed"):
                self.store.release_retained_checkpoints([checkpoint], release_claim=released.append)
        self.assertEqual(self.state()[1], "retiring")
        self.assertEqual(released, [])
        with self.assertRaisesRegex(MemoryBackingError, 'retained'):
            self.store.configure_reflink_restore(False)
        def crash(_):
            self.assertEqual(self.state()[1], "deleted")
            raise OSError("registry commit failed")
        with self.assertRaisesRegex(OSError, "registry commit failed"):
            self.store.release_retained_checkpoints([checkpoint], release_claim=crash)
        reopened = MemoryBackingStore(self.store.root, self.store.journal,
                                       hard_capacity_bytes=1 << 30, quota=self.quota)
        self.assertEqual(reopened.release_retained_checkpoints([checkpoint],
            release_claim=released.append), 1)
        self.assertEqual(released, [checkpoint])

    def test_reader_and_reappearing_source_keep_global_claim(self):
        checkpoint = self.checkpoint()
        released = []
        with self.store.read_lease(self.ref, **self.owner):
            self.assertEqual(self.store.release_retained_checkpoints([checkpoint],
                release_claim=released.append), 0)
        path = self.store.root / self.ref.allocation_id / "hibernate-1"
        with patch.object(self.quota, "release_many", side_effect=lambda *_: path.mkdir(mode=0o700)):
            self.assertEqual(self.store.release_retained_checkpoints([checkpoint],
                release_claim=released.append), 0)
        self.assertEqual(released, [])
        self.assertEqual(self.state()[1], "retiring")

    def test_old_batch_cannot_release_deleted_then_reimported_same_digest(self):
        checkpoint = self.checkpoint()
        old_project = self.state()[0]
        entered, finish = Event(), Event()
        released = []
        def physical(*_):
            entered.set()
            self.assertTrue(finish.wait(3))
        with ThreadPoolExecutor(1) as threads, patch.object(self.quota, "release_many", side_effect=physical):
            batch = threads.submit(self.store.release_retained_checkpoints, [checkpoint],
                                   release_claim=released.append)
            try:
                self.assertTrue(entered.wait(3))
                # Deletion's synchronous barrier may finish while the old
                # batch is outside locks. Reimport gets a fresh monotonic ID.
                self.assertTrue(self.store.release_retained_checkpoint(self.ref,
                    hibernation_generation=1, manifest_sha256=checkpoint.manifest_sha256))
                self.store.delete(self.ref, **self.owner)
                self.store.prepare(self.ref, **self.owner)
                self.checkpoint()
                self.assertNotEqual(self.state()[0], old_project)
            finally:
                finish.set()
            self.assertEqual(batch.result(3), 0)
        self.assertEqual(released, [])
        self.assertEqual(self.state()[1], "ready")

    def test_registry_only_claim_needs_absent_generation(self):
        checkpoint = RetainedCheckpointRef(self.ref, 3, "b" * 64)
        path = self.store.root / self.ref.allocation_id / "hibernate-3"
        path.mkdir(mode=0o700)
        released = []
        self.assertEqual(self.store.release_retained_checkpoints([checkpoint],
            release_claim=released.append), 0)
        path.rmdir()
        self.assertEqual(self.store.release_retained_checkpoints([checkpoint],
            release_claim=released.append), 1)

    def test_xfs_batch_performs_one_trim_before_all_project_releases(self):
        quota = XfsMemoryQuota()
        quota.filesystem_root = Path("/xfs")
        with patch("ucloud_sandboxes.memory_backing.subprocess.run") as command, \
                patch.object(quota._trim, "release") as trim:
            command.return_value.stdout = "/dev/loop0\n"
            quota.release_many(Path("/xfs/memory"), (101, 102, 103))
        trim.assert_called_once()
        self.assertEqual(command.call_count, 4)
