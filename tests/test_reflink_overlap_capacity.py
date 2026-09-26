from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory
import sqlite3
from threading import Barrier
import unittest

from ucloud_sandboxes.direct_registry import (
    DirectRegistryCapacityUnavailable, DirectRegistryConflictError,
    DirectSandboxRegistry,
)
from ucloud_sandboxes.direct_warden import DirectSandbox
from tests.test_direct_registry import DirectRegistryTests

MIB = 1024**2
DIGEST = 'a' * 64


class ReflinkOverlapCapacityTests(unittest.TestCase):
    def setUp(self):
        self.directory = TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name).resolve()
        self.spec = DirectRegistryTests().spec('one')
        self.base = self.spec.requested_resources().disk_mb
        self.registry = DirectSandboxRegistry(self.root / 'registry.sqlite', hard_disk_capacity_mb=self.base + 100)
        self._owned('one')

    def _plan(self, name):
        return self.registry.plan(spec=DirectRegistryTests().spec(name), sandbox_generation=1,
                                  operation_id='create:' + name, runtime_compatibility_sha256='b' * 64)

    def _owned(self, name):
        planned = self._plan(name)
        quota = self.registry.commit_quota(name, expected_revision=planned.revision,
            project_id=200000, total_mb=self.base, quota_path=self.root / name)
        rootfs = self.registry.commit_rootfs(name, expected_revision=quota.revision,
            image_id='sha256:' + 'e' * 64,
            sandbox=DirectSandbox(sandbox_id=name, sandbox_generation=1, container_id='f' * 64,
                spec_sha256=quota.spec_sha256, rootfs_sha256='d' * 64,
                bundle=self.root / 'bundles' / name, memory_directory=name + '.1'))
        return self.registry.commit_owned(name, expected_revision=rootfs.revision)

    def reserve(self, generation=1, size=40*MIB, registry=None, digest=DIGEST):
        (registry or self.registry).reserve_reflink_overlap('one', 1, generation, size, manifest_sha256=digest)

    def test_exact_retry_restart_and_digest_fenced_release(self):
        self.reserve()
        self.reserve()
        reopened = DirectSandboxRegistry(self.registry.path, hard_disk_capacity_mb=self.base + 100)
        self.reserve(registry=reopened)
        claims = reopened.list_reflink_overlaps('one', 1)
        self.assertEqual(len(claims), 1)
        self.assertEqual((claims[0].allocated_bytes, claims[0].manifest_sha256), (40*MIB, DIGEST))
        for size, digest in ((41*MIB, DIGEST), (40*MIB, 'b'*64)):
            with self.assertRaises(DirectRegistryConflictError):
                self.reserve(size=size, digest=digest, registry=reopened)
        with self.assertRaises(DirectRegistryConflictError):
            reopened.release_reflink_overlap('one', 1, 1, manifest_sha256='b'*64)
        reopened.release_reflink_overlap('one', 2, 1, manifest_sha256=DIGEST)
        self.assertEqual(reopened.reflink_overlap_bytes(), 40*MIB)
        reopened.release_reflink_overlap('one', 1, 1, manifest_sha256=DIGEST)
        reopened.release_reflink_overlap('one', 1, 1, manifest_sha256=DIGEST)
        self.assertEqual(reopened.reflink_overlap_bytes(), 0)

    def test_atomic_concurrent_overlap_claims_share_physical_budget(self):
        barrier = Barrier(2)
        def reserve(index):
            barrier.wait()
            try:
                self.reserve(index, 60*MIB)
                return True
            except DirectRegistryCapacityUnavailable:
                return False
        with ThreadPoolExecutor(2) as pool:
            results = list(pool.map(reserve, (1, 2)))
        self.assertEqual(sorted(results), [False, True])
        self.assertEqual(self.registry.reflink_overlap_bytes(), 60*MIB)

    def test_overlap_and_new_plan_cannot_double_spend_disk(self):
        self.registry.hard_disk_capacity_mb = 2*self.base
        barrier = Barrier(2)
        def run(overlap):
            barrier.wait()
            try:
                self.reserve(size=self.base*MIB) if overlap else self._plan('two')
                return True
            except DirectRegistryConflictError:
                return False
        with ThreadPoolExecutor(2) as pool:
            results = list(pool.map(run, (True, False)))
        self.assertEqual(sorted(results), [False, True])

    def test_deletion_requires_explicit_physical_reconciliation(self):
        self.reserve()
        owned = self.registry.get('one')
        deleting = self.registry.begin_delete('one', expected_revision=owned.revision)
        with self.assertRaisesRegex(DirectRegistryConflictError, 'unreconciled'):
            self.registry.commit_deleted('one', sandbox_generation=1, expected_revision=deleting.revision)
        self.assertEqual(len(self.registry.list_reflink_overlaps('one', 1)), 1)
        self.registry.release_reflink_overlap('one', 1, 1, manifest_sha256=DIGEST)
        self.registry.commit_deleted('one', sandbox_generation=1, expected_revision=deleting.revision)
        self.assertIsNone(self.registry.get('one'))

    def test_import_uses_same_retained_overlap_budget(self):
        self.registry.hard_disk_capacity_mb = 2*self.base
        self.reserve(size=1)
        with self.assertRaisesRegex(DirectRegistryConflictError, 'capacity exhausted'):
            self.registry.plan_import(spec=DirectRegistryTests().spec('imported'), sandbox_generation=2,
                operation_id='import:two', runtime_compatibility_sha256='b'*64,
                migration_id='migration:two', migration_sha256='c'*64)
        self.assertIsNone(self.registry.get('imported'))

    def test_no_claim_for_wrong_incarnation_unbounded_capacity_or_invalid_size(self):
        with self.assertRaises(DirectRegistryConflictError):
            self.registry.reserve_reflink_overlap('one', 2, 1, MIB, manifest_sha256=DIGEST)
        for size in (-1, True, 2**63):
            with self.assertRaises(ValueError):
                self.reserve(size=size)
        self.registry.hard_disk_capacity_mb = 0
        with self.assertRaises(DirectRegistryCapacityUnavailable):
            self.reserve()
        self.assertEqual(self.registry.reflink_overlap_bytes(), 0)

    def test_version5_upgrade_preserves_ownership_and_fences_older_readers(self):
        original = self.registry.get('one')
        with closing(sqlite3.connect(self.registry.path)) as conn:
            conn.execute('DROP TABLE reflink_overlaps')
            conn.execute('DROP TABLE workspace_capacity')
            conn.execute('DROP TABLE registration_disk')
            conn.execute('PRAGMA user_version=5')
        reopened = DirectSandboxRegistry(self.registry.path, hard_disk_capacity_mb=self.base+100)
        self.assertEqual(reopened.get('one'), original)
        self.reserve(registry=reopened)
        with closing(sqlite3.connect(self.registry.path)) as conn:
            self.assertEqual(conn.execute('PRAGMA user_version').fetchone()[0], 9)
