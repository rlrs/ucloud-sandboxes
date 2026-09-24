"""Real ownership journals with injected XFS/native boundaries.

Kernel FICLONE/quota behavior has a separate Linux qualification; these tests
exercise ordering, failure recovery, and exact durable capacity ownership.
"""
from dataclasses import replace
from contextlib import closing
from concurrent.futures import ThreadPoolExecutor
import os
import sqlite3
from threading import Event, Thread
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from ucloud_sandboxes.direct_registry import (
    DirectSandboxRegistry, DirectRegistryCapacityUnavailable, DirectRegistryConflictError,
)
from ucloud_sandboxes.direct_runtime import build_direct_runtime_service
from ucloud_sandboxes.direct_service import DirectSandboxService
from ucloud_sandboxes.direct_warden import DirectRunscWarden, DirectWardenError
from ucloud_sandboxes.hibernation import HibernationState
from ucloud_sandboxes.memory_backing import MemoryBackingStore, MemoryBackingError
from ucloud_sandboxes.sandbox import SandboxSpec
from tests import test_direct_warden as fixtures
from tests import test_split_memory_lifecycle as split_fixtures


class RetentionQuota(split_fixtures.FakeQuota):
    def __init__(self):
        super().__init__()
        self.events = []
        self.failure = None
        self.before_assignment = lambda: None
        self.retained_projects = set()
        self.fail_release = False

    def retain_file(self, path, project_id, quota_bytes):
        self.before_assignment()
        self.events.append(('before', project_id, quota_bytes))
        if self.failure == 'before':
            raise OSError('injected before assignment')
        self.projects[path] = (project_id, quota_bytes)
        self.retained_projects.add(project_id)
        self.events.append(('assigned', project_id, quota_bytes))
        if self.failure == 'after':
            raise OSError('injected after assignment')

    def release(self, root, project_id):
        self.events.append(('release', project_id))
        if project_id in self.retained_projects and self.fail_release:
            raise OSError('injected retention trim failure')
        super().release(root, project_id)


class ReflinkRetentionTests(unittest.TestCase):
    def setUp(self):
        fixtures.DirectRunscWardenTests.setUp(self)
        self.addCleanup(self.temporary.cleanup)
        self.spec = SandboxSpec(id=self.sandbox.sandbox_id, image='image', memory_mb=4, disk_mb=16,
                                parkable=True)
        self.base = self.spec.requested_resources().disk_mb
        self.registry = DirectSandboxRegistry(self.root / 'direct-registry.sqlite', hard_disk_capacity_mb=self.base+100)
        planned = self.registry.plan(spec=self.spec, sandbox_generation=1, operation_id='create:1',
                                     runtime_compatibility_sha256='b'*64, split_memory_backing=True)
        self.sandbox = replace(self.sandbox, spec_sha256=planned.spec_sha256)
        split_fixtures.SplitLifecycleTests.split(self, planned.memory_reference)
        self.quota = RetentionQuota()
        self.quota.projects.update(self.warden.memory_backing.quota.projects)
        self.warden.memory_backing.quota = self.quota
        quota = self.registry.commit_quota(self.spec.id, expected_revision=planned.revision,
            project_id=1, total_mb=self.base, quota_path=self.config.memory_root / self.memory_directory)
        rootfs = self.registry.commit_rootfs(self.spec.id, expected_revision=quota.revision,
            image_id='sha256:'+'e'*64, sandbox=self.sandbox)
        self.registry.commit_owned(self.spec.id, expected_revision=rootfs.revision)
        self.config = replace(self.config, reflink_memory_restore=True)
        self.warden.config = self.config
        self.warden.memory_capacity = self.registry
        self.warden.memory_backing.configure_reflink_restore(True)
        self.runner.identity_config = self.config
        self.warden.park(self.sandbox, operation_id='park:1')
        self.source = self.warden.artifacts.generation_path(sandbox_id=self.spec.id,
            sandbox_generation=1, hibernation_generation=1) / 'application_memory.img'
        self.source_bytes = self.source.read_bytes()
        self.source_identity = (self.source.stat().st_dev, self.source.stat().st_ino)
        self.base_project = self.quota.projects[self.config.memory_root / self.memory_directory]
        self.quota.before_assignment = self.assert_reserved

    def assert_reserved(self):
        self.assertGreater(self.registry.reflink_overlap_bytes(), 0)
        self.assertEqual(len(self.registry.list_reflink_overlaps(self.spec.id, 1)), 1)
        self.assertEqual(self.quota.projects[self.config.memory_root / self.memory_directory], self.base_project)

    def assert_source(self):
        self.assertEqual(self.source.read_bytes(), self.source_bytes)
        self.assertEqual((self.source.stat().st_dev, self.source.stat().st_ino), self.source_identity)

    def reopen(self):
        old = self.warden.memory_backing
        store = MemoryBackingStore(old.root, old.journal, hard_capacity_bytes=old.hard_capacity_bytes,
                                   quota=self.quota)
        self.registry = DirectSandboxRegistry(self.registry.path, hard_disk_capacity_mb=self.base+100)
        self.warden = DirectRunscWarden(self.config, runner=self.runner, fencer=self.fencer,
            storage=self.storage, rootfs_lifecycle=self.rootfs, memory_backing=store,
            memory_capacity=self.registry)

    def test_capacity_denial_precedes_project_assignment_and_candidate(self):
        self.registry.hard_disk_capacity_mb = self.base
        with self.assertRaises(DirectRegistryCapacityUnavailable):
            self.warden.resume(self.sandbox, operation_id='wake:1')
        self.assertEqual(self.quota.events, [])
        self.assertEqual(self.registry.reflink_overlap_bytes(), 0)
        self.assert_source()
        self.assertEqual(self.warden.inspect(self.sandbox).state, HibernationState.PARKED)
        self.assertFalse(any('restore' in command for command in self.runner.commands))

    def test_assignment_failures_recover_same_source_and_claim_after_restart(self):
        for failure in ('before', 'after'):
            with self.subTest(failure=failure):
                self.quota.failure = failure
                with self.assertRaisesRegex(OSError, 'assignment'):
                    self.warden.resume(self.sandbox, operation_id='wake:' + failure)
                self.assert_source()
                self.assert_reserved()
                with closing(sqlite3.connect(self.warden.memory_backing.journal)) as conn:
                    self.assertEqual(conn.execute('SELECT state FROM retained_checkpoints').fetchone()[0], 'preparing')
                self.reopen()
        self.quota.failure = None
        running = self.warden.resume(self.sandbox, operation_id='wake:success')
        self.assertEqual(running.state, HibernationState.RUNNING)
        self.assert_reserved()
        self.warden.reconcile_retired_memory_capacity()
        self.assertEqual(self.registry.reflink_overlap_bytes(), 0)
        self.assertFalse(self.source.exists())
        assigned = {event[1] for event in self.quota.events if event[0] == 'assigned'}
        self.assertEqual(len(assigned), 1)
        self.assertEqual(self.quota.projects[self.config.memory_root / self.memory_directory], self.base_project)

    def test_failed_candidate_retries_without_consuming_immutable_source(self):
        self.runner.fail_readiness = True
        with self.assertRaises(DirectWardenError):
            self.warden.resume(self.sandbox, operation_id='wake:failed')
        self.assert_source()
        self.assert_reserved()
        self.assertEqual(self.warden.inspect(self.sandbox).state, HibernationState.PARKED)
        self.reopen()
        self.runner.fail_readiness = False
        self.assertEqual(self.warden.resume(self.sandbox, operation_id='wake:retry').state, HibernationState.RUNNING)
        self.assert_reserved()
        self.warden.reconcile_retired_memory_capacity()
        self.assertEqual(self.registry.reflink_overlap_bytes(), 0)

    def test_running_recovery_releases_claim_after_artifact_was_already_removed(self):
        self.quota.fail_release = True
        running = self.warden.resume(self.sandbox, operation_id='wake:1')
        self.assertEqual(running.state, HibernationState.RUNNING)
        self.assertTrue(self.source.parent.exists())
        self.assert_reserved()
        with self.assertRaisesRegex(OSError, 'trim failure'):
            self.warden.reconcile_retired_memory_capacity()
        self.assertFalse(self.source.parent.exists())
        self.reopen()
        self.quota.fail_release = False
        self.assertEqual(self.warden.reconcile(self.sandbox).state, HibernationState.RUNNING)
        self.assert_reserved()
        self.warden.reconcile_retired_memory_capacity()
        self.assertEqual(self.registry.reflink_overlap_bytes(), 0)
        self.warden.reconcile(self.sandbox)
        self.assertEqual(self.registry.list_reflink_overlaps(self.spec.id, 1), ())

    def test_slow_physical_release_holds_neither_wake_nor_lifecycle_lock(self):
        running = self.warden.resume(self.sandbox, operation_id='wake:1')
        self.assertEqual(running.state, HibernationState.RUNNING)
        self.assertFalse(any(event[0] == 'release' for event in self.quota.events))
        self.assert_reserved()
        entered, finish = Event(), Event()
        def trim(*_):
            entered.set()
            self.assertTrue(finish.wait(3))
        with ThreadPoolExecutor(1) as threads, patch.object(self.quota, 'release_many', side_effect=trim):
            cleanup = threads.submit(self.warden.reconcile_retired_memory_capacity)
            try:
                self.assertTrue(entered.wait(3))
                # The ordinary foreground journal fence is available while
                # physical cleanup blocks; running reconciliation cannot trim.
                with self.warden._locked(self.sandbox):
                    self.assertEqual(self.warden._journal(self.sandbox).load().state,
                                     HibernationState.RUNNING)
                self.assertEqual(self.warden.reconcile(self.sandbox).state, HibernationState.RUNNING)
                self.assert_reserved()
            finally:
                finish.set()
            self.assertEqual(cleanup.result(3), 1)
        self.assertEqual(self.registry.reflink_overlap_bytes(), 0)

    def test_slow_old_generation_unlink_does_not_block_reconcile_capture_or_wake(self):
        self.warden.resume(self.sandbox, operation_id='wake:1')
        self.assert_source()  # Wake did not unlink the immutable generation.
        old_directory_inode = self.source.parent.stat().st_ino
        entered, finish = Event(), Event()
        unlink = os.unlink

        def slow_unlink(name, *, dir_fd=None):
            if (name == 'application_memory.img' and dir_fd is not None
                    and os.fstat(dir_fd).st_ino == old_directory_inode):
                entered.set()
                self.assertTrue(finish.wait(5))
            return unlink(name, dir_fd=dir_fd)

        self.quota.before_assignment = lambda: None
        with ThreadPoolExecutor(2) as threads, patch('ucloud_sandboxes.hibernation.os.unlink', side_effect=slow_unlink):
            cleanup = threads.submit(self.warden.reconcile_retired_memory_capacity)
            try:
                self.assertTrue(entered.wait(2))
                self.assertEqual(self.warden.reconcile(self.sandbox).state, HibernationState.RUNNING)
                parked = threads.submit(self.warden.park, self.sandbox, operation_id='park:2').result(2)
                self.assertEqual(parked.hibernation_generation, 2)
                # A later restore has independent source files and remains
                # runnable while old-generation fsync/unlink is stalled.
                running = threads.submit(self.warden.resume, self.sandbox, operation_id='wake:2').result(2)
                self.assertEqual(running.state, HibernationState.RUNNING)
                newer = self.source.parent.parent / 'hibernate-2' / 'application_memory.img'
                self.assertTrue(newer.exists())
                self.assertEqual(len(self.registry.list_reflink_overlaps()), 2)
            finally:
                finish.set()
            self.assertEqual(cleanup.result(2), 1)
        self.assertTrue(newer.exists())
        self.assertEqual(len(self.registry.list_reflink_overlaps()), 1)
        self.warden.reconcile_retired_memory_capacity()
        self.assertEqual(self.registry.reflink_overlap_bytes(), 0)

    def test_current_failed_restore_source_cannot_be_retired(self):
        self.runner.fail_readiness = True
        with self.assertRaises(DirectWardenError):
            self.warden.resume(self.sandbox, operation_id='wake:failed')
        self.assertEqual(self.warden.reconcile_retired_memory_capacity(), 0)
        self.assert_source()
        self.assert_reserved()

    def test_repeated_wakes_reuse_retirement_lock_and_report_preparation(self):
        self.quota.before_assignment = lambda: None
        for generation in range(1, 4):
            timings = {}
            self.warden.resume(self.sandbox, operation_id=f'wake:{generation}', timings=timings)
            preparation = ('prepare_workspace', 'validate_checkpoint', 'join_network_prepare',
                           'prepare_memory', 'retain_source')
            for phase in preparation:
                self.assertGreaterEqual(timings[phase], 0)
            self.assertLessEqual(sum(timings[phase] for phase in preparation),
                                 timings['validate_artifact'])
            self.assertEqual(self.warden.reconcile_retired_memory_capacity(), 1)
            self.assertEqual(len(list(self.warden.artifacts.root.glob('.retire-*.lock'))), 1)
            if generation < 3:
                self.warden.park(self.sandbox, operation_id=f'park:{generation + 1}')

    def test_retirement_crash_after_payload_unlink_resumes_from_authenticated_manifest(self):
        self.warden.resume(self.sandbox, operation_id='wake:1')
        unlink = os.unlink

        def crash(name, *, dir_fd=None):
            result = unlink(name, dir_fd=dir_fd)
            if name == 'application_memory.img':
                raise OSError('injected retirement crash after unlink')
            return result

        with patch('ucloud_sandboxes.hibernation.os.unlink', side_effect=crash):
            with self.assertRaisesRegex(OSError, 'retirement crash'):
                self.warden.reconcile_retired_memory_capacity()
        self.assertFalse(self.source.exists())
        self.assertTrue((self.source.parent / self.warden.artifacts.MANIFEST_NAME).exists())
        self.assert_reserved()
        self.reopen()
        self.assertEqual(self.warden.reconcile(self.sandbox).state, HibernationState.RUNNING)
        self.assertEqual(self.warden.reconcile_retired_memory_capacity(), 1)
        self.assertEqual(self.registry.reflink_overlap_bytes(), 0)

    def test_retirement_wrong_manifest_cannot_unlink_source(self):
        self.warden.resume(self.sandbox, operation_id='wake:1')
        claim = self.registry.list_reflink_overlaps()[0]
        with patch.object(self.registry, 'list_reflink_overlaps', return_value=(replace(claim, manifest_sha256='0'*64),)):
            with self.assertRaisesRegex(Exception, 'identity changed'):
                self.warden.reconcile_retired_memory_capacity()
        self.assert_source()
        self.assert_reserved()

    def test_existing_service_reconciliation_loop_finishes_retirement(self):
        self.warden.resume(self.sandbox, operation_id='wake:maintenance')
        service = DirectSandboxService(SimpleNamespace(warden=self.warden, registry=self.registry,
            image_cache_reconciliation_pending=False), deletion_reconcile_interval_seconds=.001)
        reconcile = self.warden.reconcile_retired_memory_capacity
        def once():
            try:
                return reconcile()
            finally:
                service._stop_event.set()
        with patch.object(self.warden, 'reconcile_retired_memory_capacity', side_effect=once) as cleanup:
            thread = Thread(target=service._deletion_reconciliation_loop)
            thread.start()
            thread.join(3)
            self.assertFalse(thread.is_alive())
            cleanup.assert_called_once()
        self.assertEqual(self.registry.reflink_overlap_bytes(), 0)

    def test_delete_releases_only_after_owned_files_are_deleted(self):
        with patch.object(self.warden.memory_backing, 'retain_checkpoint', side_effect=OSError('before local journal')):
            with self.assertRaises(OSError):
                self.warden.resume(self.sandbox, operation_id='wake:1')
        self.assert_reserved()
        self.warden.delete(self.sandbox)
        with self.assertRaisesRegex(DirectWardenError, 'still exists'):
            self.warden.release_deleted_memory_capacity(self.sandbox)
        self.assert_reserved()
        self.warden.memory_backing.delete(self.sandbox.memory, sandbox_id=self.spec.id, sandbox_generation=1)
        self.warden.release_deleted_memory_capacity(self.sandbox)
        self.assertEqual(self.registry.reflink_overlap_bytes(), 0)

    def test_open_source_reader_keeps_physical_claim_after_unlink(self):
        store = self.warden.memory_backing
        with store.read_lease(self.sandbox.memory, sandbox_id=self.spec.id, sandbox_generation=1):
            with self.source.open('rb') as reader:
                running = self.warden.resume(self.sandbox, operation_id='wake:1')
                self.assertEqual(running.state, HibernationState.RUNNING)
                self.assertTrue(self.source.exists())
                self.assertEqual(reader.read(), self.source_bytes)
                self.assert_reserved()
                self.assertEqual(self.warden.reconcile_retired_memory_capacity(), 0)
                self.assertFalse(self.source.exists())
        self.warden.reconcile_retired_memory_capacity()
        self.assertEqual(self.registry.reflink_overlap_bytes(), 0)

    def test_retention_retry_rejects_a_replaced_source_inode(self):
        self.quota.failure = 'after'
        with self.assertRaises(OSError):
            self.warden.resume(self.sandbox, operation_id='wake:1')
        claim = self.registry.list_reflink_overlaps(self.spec.id, 1)[0]
        original = self.source.with_suffix('.original')
        self.source.rename(original)
        self.source.write_bytes(self.source_bytes)
        self.source.chmod(0o600)
        with self.assertRaisesRegex(MemoryBackingError, 'identity conflicts'):
            self.warden.memory_backing.retain_checkpoint(self.sandbox.memory,
                sandbox_id=self.spec.id, sandbox_generation=1, hibernation_generation=1,
                manifest_sha256=claim.manifest_sha256, allocated_bytes=claim.allocated_bytes)
        self.assert_reserved()

    def test_feature_disable_is_fenced_until_deleted_owner_retention_drains(self):
        self.quota.failure = 'after'
        with self.assertRaises(OSError):
            self.warden.resume(self.sandbox, operation_id='wake:1')
        self.warden.delete(self.sandbox)
        self.warden.memory_backing.delete(self.sandbox.memory, sandbox_id=self.spec.id, sandbox_generation=1)
        # Simulate restart after allocation deletion, before ancillary project
        # and global capacity cleanup. Disabling the reader cannot strand it.
        with self.assertRaisesRegex(MemoryBackingError, 'reader is required'):
            self.warden.memory_backing.configure_reflink_restore(False)
        self.quota.failure = None
        self.warden.release_deleted_memory_capacity(self.sandbox)
        self.warden.memory_backing.configure_reflink_restore(False)

    def test_factory_cannot_disable_reader_with_registry_only_orphan_claim(self):
        with patch.object(self.warden.memory_backing, 'retain_checkpoint', side_effect=OSError('before local row')):
            with self.assertRaises(OSError):
                self.warden.resume(self.sandbox, operation_id='wake:1')
        self.warden.delete(self.sandbox)
        self.warden.memory_backing.delete(self.sandbox.memory, sandbox_id=self.spec.id, sandbox_generation=1)
        self.assertGreater(self.registry.reflink_overlap_bytes(), 0)
        with (patch('ucloud_sandboxes.direct_runtime.installed_sidecar_fingerprints', return_value={}),
              patch('ucloud_sandboxes.direct_runtime._cpu_features_sha256', return_value='a'*64),
              patch('ucloud_sandboxes.direct_runtime.StorageNativeNodeClient.wait_ready', return_value={'metrics': {}}),
              patch('ucloud_sandboxes.direct_runtime.DirectRunscWarden') as constructor):
            with self.assertRaisesRegex(ValueError, 'overlap capacity drains'):
                build_direct_runtime_service(state_root=self.root, volume_mount_root=self.config.memory_root,
                    runsc=self.config.runsc, runsc_commit='a'*40, init_binary=self.root/'init',
                    storage_native_socket=self.root/'storage.sock')
            constructor.assert_not_called()

    def test_same_parked_incarnation_reimport_gets_fresh_retention_project(self):
        self.quota.failure = 'after'
        with self.assertRaises(OSError):
            self.warden.resume(self.sandbox, operation_id='wake:failed')
        old_claim = self.registry.list_reflink_overlaps(self.spec.id, 1)[0]
        old_project = next(iter(self.quota.retained_projects))
        deleting = self.registry.begin_delete(self.spec.id, expected_revision=self.registry.get(self.spec.id).revision)
        self.warden.delete(self.sandbox)
        store = self.warden.memory_backing
        store.delete(self.sandbox.memory, sandbox_id=self.spec.id, sandbox_generation=1)
        self.warden.release_deleted_memory_capacity(self.sandbox)
        self.registry.commit_deleted(self.spec.id, sandbox_generation=1, expected_revision=deleting.revision)
        self.assertEqual(self.registry.reflink_overlap_bytes(), 0)

        # The migration protocol permits this exact sandbox generation to
        # return with a fresh migration ID; no guest execution happened away.
        imported = self.registry.plan_import(spec=self.spec, sandbox_generation=1,
            operation_id='import:back', runtime_compatibility_sha256='b'*64,
            migration_id='migration:back', migration_sha256='c'*64, split_memory_backing=True)
        quota = self.registry.commit_import_quota(self.spec.id, expected_revision=imported.revision,
            project_id=2, total_mb=self.base, quota_path=self.config.memory_root/self.memory_directory)
        rootfs = self.registry.commit_import_rootfs(self.spec.id, expected_revision=quota.revision,
            image_id='sha256:'+'e'*64, sandbox=self.sandbox)
        ready = self.registry.commit_import_ready(self.spec.id, expected_revision=rootfs.revision,
            migration_id='migration:back', migration_sha256='c'*64)
        self.registry.activate_import(self.spec.id, expected_revision=ready.revision,
            migration_id='migration:back', migration_sha256='c'*64)
        store.prepare(self.sandbox.memory, sandbox_id=self.spec.id, sandbox_generation=1)
        self.base_project = self.quota.projects[self.config.memory_root/self.memory_directory]
        self.source.parent.mkdir(mode=0o700)
        self.source.write_bytes(b'reimported-memory')
        self.source.chmod(0o600)
        size = max(4096, self.source.stat().st_blocks*512)
        digest = 'c'*64
        self.registry.reserve_reflink_overlap(self.spec.id, 1, 1, size, manifest_sha256=digest)
        self.quota.failure = None
        arguments = dict(sandbox_id=self.spec.id, sandbox_generation=1, hibernation_generation=1,
                         manifest_sha256=digest, allocated_bytes=size)
        store.retain_checkpoint(self.sandbox.memory, **arguments)
        self.reopen()
        self.warden.memory_backing.retain_checkpoint(self.sandbox.memory, **arguments)
        new_projects = self.quota.retained_projects - {old_project}
        self.assertEqual(len(new_projects), 1)
        self.assertNotEqual(next(iter(new_projects)), self.base_project[0])
        with self.assertRaises(DirectRegistryConflictError):
            self.registry.release_reflink_overlap(self.spec.id, 1, 1,
                manifest_sha256=old_claim.manifest_sha256)
        self.assertEqual(self.registry.reflink_overlap_bytes(), size)
        self.assertEqual(self.source.read_bytes(), b'reimported-memory')
