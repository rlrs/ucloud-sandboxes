"""Retention batches acknowledge only durable, owner-fenced quota transitions."""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
from threading import Barrier
import unittest

from ucloud_sandboxes.checkpoint_components import MemoryBackingRef
from ucloud_sandboxes.memory_backing import MemoryBackingStore, MemoryBackingError
from tests.test_split_memory_lifecycle import FakeQuota


class MemoryRetentionBatchTests(unittest.TestCase):
    def setUp(self):
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        self.quota = FakeQuota()
        self.store = MemoryBackingStore(root / 'memory', root / 'journal.sqlite',
                                       hard_capacity_bytes=1 << 24, quota=self.quota)
        self.refs = [MemoryBackingRef(f'sandbox-{i}.sandbox-1', 1 << 20) for i in range(2)]
        for i, ref in enumerate(self.refs):
            lease = self.store.prepare(ref, sandbox_id=f'sandbox-{i}', sandbox_generation=1)
            path = lease.path / 'hibernate-1'
            path.mkdir(mode=0o700)
            source = path / 'application_memory.img'
            source.write_bytes(b'memory')
            source.chmod(0o600)

    def retain(self, i):
        self.store.retain_checkpoint(self.refs[i], sandbox_id=f'sandbox-{i}',
            sandbox_generation=1, hibernation_generation=1,
            manifest_sha256='a' * 64, allocated_bytes=65536)

    def rows(self):
        with sqlite3.connect(self.store.journal) as conn:
            return conn.execute('SELECT project_id,state FROM retained_checkpoints ORDER BY allocation_id').fetchall()

    def test_concurrent_owners_share_commits_and_quota_sees_durable_prepare(self):
        barrier = Barrier(2)
        original = self.quota.retain_file

        def assign(path, project, size):
            # A separate connection must see the prepared claim BEFORE the
            # irreversible inode ownership change, even with a shared commit.
            self.assertIn((project, 'preparing'), self.rows())
            barrier.wait(timeout=5)
            original(path, project, size)

        self.quota.retain_file = assign
        self.store._write_batches.delay = .05
        with ThreadPoolExecutor(2) as pool:
            list(pool.map(self.retain, range(2)))
        rows = self.rows()
        self.assertEqual([state for _, state in rows], ['ready', 'ready'])
        self.assertEqual(len({project for project, _ in rows}), 2)
        self.assertEqual(self.store._write_batches.operations, 4)
        self.assertLess(self.store._write_batches.commits, 4)

    def test_commit_failures_preserve_recoverable_ownership(self):
        for failed_commit in (1, 2):
            with self.subTest(failed_commit=failed_commit):
                # A fresh owner is used for each failure point.
                i = failed_commit - 1
                self.store = MemoryBackingStore(self.store.root, self.store.journal,
                    hard_capacity_bytes=1 << 24, quota=self.quota)
                attempts = []
                journal = self.store.journal

                class FailingConnection(sqlite3.Connection):
                    def commit(conn):
                        attempts.append(True)
                        if len(attempts) == failed_commit:
                            raise sqlite3.OperationalError('injected commit failure')
                        return super().commit()

                def connect():
                    conn = sqlite3.connect(journal, factory=FailingConnection, check_same_thread=False)
                    conn.execute('PRAGMA synchronous=FULL')
                    return conn

                self.store._write_batches.connect = connect
                with self.assertRaisesRegex(sqlite3.OperationalError, 'injected'):
                    self.retain(i)
                rows = self.rows()
                if failed_commit == 1:
                    self.assertEqual(rows, [])
                    self.assertFalse(any(p.name == 'application_memory.img' for p in self.quota.projects))
                else:
                    self.assertEqual(rows[-1][1], 'preparing')
                self.store = MemoryBackingStore(self.store.root, journal,
                    hard_capacity_bytes=1 << 24, quota=self.quota)
                self.retain(i)
                self.assertTrue(all(state == 'ready' for _, state in self.rows()))

    def test_replaced_journal_fails_closed_before_assignment(self):
        self.store.journal.rename(self.store.journal.with_suffix('.old'))
        self.store.journal.touch()
        with self.assertRaisesRegex(MemoryBackingError, 'replaced'):
            self.store._write_batches.validate()
