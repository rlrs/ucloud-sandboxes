from concurrent.futures import ThreadPoolExecutor
from threading import Event
from contextlib import closing
from dataclasses import replace
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from tests import test_split_memory_lifecycle as fixtures
from ucloud_sandboxes.checkpoint_components import MemoryBackingRef
from ucloud_sandboxes.memory_backing import MemoryBackingError, MemoryBackingStore


class MemoryBackingModeTests(unittest.TestCase):
    def setUp(self):
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.ram = self.root / 'ram'
        self.ram.mkdir(mode=0o700)
        self.quota = fixtures.FakeQuota()
        self.ref = MemoryBackingRef('guest.sandbox-1', 4096)
        self.owner = dict(sandbox_id='guest', sandbox_generation=1)
        self.store = self.open_store()

    def open_store(self):
        with patch('ucloud_sandboxes.memory_backing.subprocess.run',
                   return_value=SimpleNamespace(stdout='tmpfs rw,noswap\n')):
            return MemoryBackingStore(self.root / 'disk', self.root / 'memory.sqlite',
                                      hard_capacity_bytes=8192, quota=self.quota,
                                      active_root=self.ram)

    def test_cached_mode_read_does_not_wait_for_allocator_io(self):
        self.store.prepare(self.ref, **self.owner)
        entered, release = Event(), Event()
        validate = self.quota.validate_project
        def blocked_validate(*args, **kwargs):
            entered.set()
            if not release.wait(3):
                raise TimeoutError('test failed to release allocator I/O')
            return validate(*args, **kwargs)
        with patch.object(self.quota, 'validate_project', side_effect=blocked_validate), \
                ThreadPoolExecutor(max_workers=2) as pool:
            allocation = pool.submit(self.store.require, self.ref, **self.owner)
            try:
                self.assertTrue(entered.wait(1))
                read = pool.submit(self.store.active_mode, 'guest', 1)
                self.assertEqual(read.result(1), 'ram')
            finally:
                release.set()
            allocation.result(2)

    def test_file_mode_is_published_only_after_commit(self):
        self.store.prepare(self.ref, **self.owner)
        entered, release = Event(), Event()
        class SlowCommit(sqlite3.Connection):
            def commit(self):
                entered.set()
                if not release.wait(3):
                    raise TimeoutError('test failed to release commit')
                return super().commit()
        def connect():
            return sqlite3.connect(self.store.journal, factory=SlowCommit, check_same_thread=False)
        self.store._write_batches.close()
        with patch.object(self.store._write_batches, 'connect', side_effect=connect), \
                ThreadPoolExecutor(max_workers=2) as pool:
            selection = pool.submit(self.store.prepare_file_restore, self.ref, **self.owner)
            try:
                self.assertTrue(entered.wait(1))
                read = pool.submit(self.store.active_mode, 'guest', 1)
                self.assertEqual(read.result(1), 'ram')
            finally:
                release.set()
            self.assertEqual(selection.result(2).active_mode, 'file')
        self.assertEqual(self.store.active_mode('guest', 1), 'file')

    def test_selection_survives_restart_without_reversing_reimport(self):
        lease = self.store.prepare(self.ref, **self.owner)
        self.assertEqual(lease.active_mode, 'ram')
        self.assertEqual(self.store.active_mode('guest', 1), 'ram')
        selected = self.store.prepare_file_restore(self.ref, **self.owner)
        self.assertEqual(selected.active_mode, 'file')
        self.assertEqual(selected.path, lease.path)
        restarted = self.open_store()
        self.assertEqual(restarted.active_mode('guest', 1), 'file')
        self.assertEqual(restarted.require(self.ref, **self.owner).active_mode, 'file')
        restarted.delete(self.ref, **self.owner)
        self.assertIsNone(restarted.active_mode('guest', 1))
        self.assertEqual(restarted.prepare(self.ref, **self.owner).active_mode, 'file')

    def test_still_present_ram_mapping_cannot_be_abandoned(self):
        self.store.prepare(self.ref, **self.owner)
        active = self.ram / self.ref.allocation_id / 'application_memory.active'
        active.write_bytes(b'live application state')
        with self.assertRaisesRegex(MemoryBackingError, 'live RAM'):
            self.store.prepare_file_restore(self.ref, **self.owner)
        self.assertEqual(active.read_bytes(), b'live application state')
        self.assertEqual(self.open_store().active_mode('guest', 1), 'ram')

    def test_missing_stale_and_changed_quota_owners_cannot_select_file(self):
        with self.assertRaisesRegex(MemoryBackingError, 'retained'):
            self.store.prepare_file_restore(self.ref, **self.owner)
        self.store.prepare(self.ref, **self.owner)
        for ref, owner in ((self.ref, dict(sandbox_id='guest', sandbox_generation=2)),
                           (MemoryBackingRef(self.ref.allocation_id, 8192), self.owner)):
            with self.assertRaisesRegex(MemoryBackingError, 'retained'):
                self.store.prepare_file_restore(ref, **owner)
        self.quota.projects.clear()
        with self.assertRaisesRegex(MemoryBackingError, 'project changed'):
            self.store.prepare_file_restore(self.ref, **self.owner)
        self.assertEqual(self.open_store().active_mode('guest', 1), 'ram')

    def test_limit_starts_small_and_moves_within_ceiling_and_capacity(self):
        ref = MemoryBackingRef('guest.sandbox-1', 6144)
        lease = self.store.prepare(ref, **self.owner, limit_bytes=1024)
        self.assertEqual(self.quota.limits[lease.project_id], 1024)
        self.assertEqual(self.store.metrics()['memory_backing_hard_reserved_bytes'], 1024)
        other = MemoryBackingRef('other.sandbox-1', 4096)
        self.store.prepare(other, sandbox_id='other', sandbox_generation=1, limit_bytes=4096)
        # 1024 + 4096 of 8192: raising to the ceiling would overcommit.
        with self.assertRaisesRegex(MemoryBackingError, 'capacity'):
            self.store.set_limit(ref, **self.owner, limit_bytes=6144)
        self.assertEqual(self.quota.limits[lease.project_id], 1024)
        self.assertEqual(self.store.set_limit(ref, **self.owner, limit_bytes=4096), 1024)
        self.assertEqual((self.quota.limits[lease.project_id], self.store.limit_bytes(ref)),
                         (4096, 4096))
        # Lowering changes the kernel limit before the journal charge.
        self.quota.fail = True
        with self.assertRaises(OSError):
            self.store.set_limit(ref, **self.owner, limit_bytes=512)
        self.assertEqual(self.store.limit_bytes(ref), 4096)
        self.quota.fail = False
        self.store.set_limit(ref, **self.owner, limit_bytes=512)
        self.assertEqual(self.store.metrics()['memory_backing_hard_reserved_bytes'], 4608)
        # A capture may exceed the identity ceiling; only capacity bounds it.
        with self.assertRaisesRegex(MemoryBackingError, 'capacity'):
            self.store.set_limit(ref, **self.owner, limit_bytes=8192)
        with self.assertRaisesRegex(MemoryBackingError, 'positive'):
            self.store.set_limit(ref, **self.owner, limit_bytes=0)
        with self.assertRaisesRegex(MemoryBackingError, 'retained'):
            self.store.set_limit(ref, sandbox_id='guest', sandbox_generation=2, limit_bytes=512)

    def test_allocated_bytes_counts_every_block_once(self):
        self.store.prepare(self.ref, **self.owner, limit_bytes=1024)
        generation = self.root / 'disk' / self.ref.allocation_id / 'hibernate-1'
        generation.mkdir()
        (generation / 'pages.img').write_bytes(b'x' * 10000)
        (generation / 'link.img').hardlink_to(generation / 'pages.img')
        allocated = self.store.allocated_bytes(self.ref)
        self.assertGreaterEqual(allocated, 10000)
        before = allocated
        (generation / 'more.img').write_bytes(b'y' * 5000)
        self.assertGreater(self.store.allocated_bytes(self.ref), before)

    def test_legacy_journal_migrates_worker_layout_transactionally(self):
        # An old six-column allocation is the actual input to the migration.
        journal = self.root / 'memory.sqlite'
        with closing(sqlite3.connect(journal)) as conn, conn:
            conn.execute('DROP TABLE allocations')
            conn.execute('CREATE TABLE allocations (allocation_id TEXT PRIMARY KEY, '
                         'sandbox_id TEXT, generation INTEGER, project_id INTEGER, '
                         'quota_bytes INTEGER, state TEXT)')
            conn.execute('INSERT INTO allocations VALUES (?,?,?,?,?,?)',
                         ('guest.sandbox-1', 'guest', 1, 600000, 4096, 'ready'))
            conn.execute('PRAGMA user_version=0')
        reopened = self.open_store()
        self.assertEqual(reopened.active_mode('guest', 1), 'ram')
        with closing(sqlite3.connect(journal)) as conn:
            self.assertEqual(conn.execute('PRAGMA user_version').fetchone()[0], 3)
            self.assertEqual(conn.execute('SELECT active_mode FROM allocations').fetchone()[0], 'ram')
            # Existing allocations keep their full ceiling as their limit.
            self.assertEqual(conn.execute('SELECT limit_bytes FROM allocations').fetchone()[0], 4096)
        self.assertEqual(self.open_store().active_mode('guest', 1), 'ram')

    def test_selection_failure_does_not_publish_file_mode(self):
        self.store.prepare(self.ref, **self.owner)
        real_connect = self.store._connect
        class FailedSelection(sqlite3.Connection):
            def commit(self):
                raise sqlite3.OperationalError('injected durability failure')
        def failed_connect():
            return sqlite3.connect(self.store.journal, factory=FailedSelection, check_same_thread=False)
        self.store._write_batches.close()
        with patch.object(self.store._write_batches, 'connect', side_effect=failed_connect):
            with self.assertRaisesRegex(sqlite3.OperationalError, 'durability'):
                self.store.prepare_file_restore(self.ref, **self.owner)
        self.assertEqual(self.store.active_mode('guest', 1), 'ram')
        with closing(real_connect()) as conn:
            self.assertEqual(conn.execute('SELECT active_mode FROM allocations').fetchone()[0], 'ram')

    def test_reader_cannot_be_disabled_until_owned_allocations_drain(self):
        self.store.configure_reflink_restore(True)
        self.store.prepare(self.ref, **self.owner)
        self.store.prepare_file_restore(self.ref, **self.owner)
        with self.assertRaisesRegex(MemoryBackingError, 'reader is required'):
            self.open_store().configure_reflink_restore(False)
        self.store.delete(self.ref, **self.owner)
        self.open_store().configure_reflink_restore(False)


class WardenFileModeTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.SplitLifecycleTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.fixture.split()
        self.fixture.warden.config = replace(self.fixture.warden.config, reflink_memory_restore=True)

    def test_restore_uses_owned_file_mode_and_keeps_runtime_identity_stable(self):
        f = self.fixture
        fingerprint = f.warden._runtime_fingerprint(f.sandbox)
        parked = f.warden.park(f.sandbox, operation_id='park-file')
        # The dedicated retention tests exercise the registry/project handoff.
        with patch.object(f.warden, '_retain_restore_source'):
            f.warden.resume(f.sandbox, operation_id='wake-file')
        command = next(cmd for cmd in f.runner.commands if 'restore' in cmd)
        self.assertIn('--application-memory-reflink-restore=true', command)
        self.assertNotIn('--application-memory-ram-backing=true', command)
        self.assertIn(f'--application-memory-file-dir={f.config.memory_root}', command)
        self.assertEqual(f.warden._runtime_fingerprint(f.sandbox), fingerprint)
        self.assertEqual(f.warden.application_memory_mode(f.sandbox.sandbox_id, 1), 'file')
        self.assertGreater(parked.hibernation_generation, 0)

    def test_running_runtime_cannot_enter_restore_selection(self):
        f = self.fixture
        with patch.object(f.warden.memory_backing, 'prepare_file_restore') as select:
            with self.assertRaisesRegex(RuntimeError, 'expected parked'):
                f.warden.resume(f.sandbox, operation_id='invalid-wake')
            select.assert_not_called()

    def test_flush_retains_allocation_but_does_not_hold_lifecycle_lock(self):
        import fcntl
        import os
        f = self.fixture
        active = f.warden._active_memory_root(f.sandbox) / 'application_memory.active'
        active.write_bytes(b'dirty file backed heap')
        active.chmod(0o600)
        def check_flush(fd):
            self.assertEqual(os.fstat(fd).st_ino, active.stat().st_ino)
            lock = f.config.runtime_root / 'warden-locks' / '.sandbox-1.sandbox-1.warden.lock'
            descriptor = os.open(lock, os.O_RDWR)
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            finally:
                os.close(descriptor)
            with self.assertRaisesRegex(MemoryBackingError, 'readers'):
                with f.warden.memory_backing._allocation_lock(f.sandbox.memory, exclusive=True):
                    self.fail('flush lost its backing capacity lease')
        with patch('ucloud_sandboxes.direct_warden.os.fdatasync', side_effect=check_flush, create=True):
            self.assertTrue(f.warden.flush_reclaimable_memory(f.sandbox))

    def test_flush_rejects_a_replaced_active_file(self):
        f = self.fixture
        active = f.warden._active_memory_root(f.sandbox) / 'application_memory.active'
        active.write_bytes(b'old')
        active.chmod(0o600)
        def replace_file(fd):
            active.unlink()
            active.write_bytes(b'new')
        with patch('ucloud_sandboxes.direct_warden.os.fdatasync', side_effect=replace_file, create=True):
            self.assertFalse(f.warden.flush_reclaimable_memory(f.sandbox))
