from datetime import datetime, timedelta, timezone
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from ucloud_sandboxes.control_plane import _persist_registry_image_protection
from ucloud_sandboxes.managed_registry import (
    RegistryImageLeaseNotFound, RegistryUsageStateError, RegistryUsageStore,
)

DIGEST = 'sha256:' + 'a' * 64


class RegistryUsageHotpathTests(unittest.TestCase):
    def test_gateway_protection_uses_scoped_reads_and_preserves_renewal_policy(self):
        with TemporaryDirectory() as raw:
            store = RegistryUsageStore(Path(raw) / 'registry.sqlite')
            now = datetime(2026, 9, 21, tzinfo=timezone.utc)
            image = 'registry.example/repo:tag@' + DIGEST

            def protect(owner, seconds=0, *, persistent=False, touch=False, ref=image):
                return _persist_registry_image_protection(
                    store, ref, owner, touch=touch, persistent=persistent,
                    now=now + timedelta(seconds=seconds), ttl_seconds=10,
                )

            with patch.object(store, '_snapshot_unlocked', side_effect=AssertionError('whole registry scan')):
                self.assertTrue(protect('permanent', persistent=True, touch=True))
                self.assertTrue(protect('transient'))
                first = store.get_lease('repo', 'tag', 'transient', now=now)
                with patch.object(store, '_transaction', side_effect=AssertionError('unnecessary write')):
                    self.assertTrue(protect('permanent', 1000))
                    self.assertTrue(protect('transient', 4))
                self.assertTrue(protect('transient', 6))
                renewed = store.get_lease('repo', 'tag', 'transient', now=now)
                self.assertEqual(renewed.acquired_at, first.acquired_at)
                self.assertEqual(renewed.expires_at, (now + timedelta(seconds=16)).isoformat())
                with self.assertRaisesRegex(ValueError, 'immutable'):
                    protect('permanent', ref=image.replace(DIGEST, 'sha256:' + 'b' * 64))
                self.assertIsNone(store.get_lease('repo', 'tag', 'transient', now=now + timedelta(seconds=16)))
                self.assertTrue(protect('transient', 16))
                self.assertEqual(store.get_lease('repo', 'tag', 'transient', now=now + timedelta(seconds=16)).acquired_at,
                                 (now + timedelta(seconds=16)).isoformat())
            self.assertEqual(len(store.snapshot(now=now).records), 2)

    def test_scoped_lookup_reads_committed_identity_and_rejects_corruption(self):
        with TemporaryDirectory() as raw:
            store = RegistryUsageStore(Path(raw) / 'registry.sqlite')
            expected = store.acquire_reference('repo', 'tag', 'owner', digest=DIGEST)
            with sqlite3.connect(store.path) as writer:
                writer.execute('BEGIN IMMEDIATE')
                writer.execute('DELETE FROM registry_leases')
                self.assertEqual(store.get_lease('repo', 'tag', 'owner'), expected)
                writer.commit()
            self.assertIsNone(store.get_lease('repo', 'tag', 'owner'))
            store.acquire_reference('repo', 'tag', 'owner', digest=DIGEST)
            with sqlite3.connect(store.path) as writer:
                writer.execute("UPDATE registry_leases SET digest = 'invalid'")
            with self.assertRaises(ValueError):
                store.get_lease('repo', 'tag', 'owner')
            store.path.unlink()
            with self.assertRaises(RegistryUsageStateError):
                store.get_lease('repo', 'tag', 'owner')
            self.assertFalse(store.path.exists())

    def test_scoped_mutations_preserve_lease_and_generation_fences(self):
        with TemporaryDirectory() as raw:
            store = RegistryUsageStore(Path(raw) / 'registry.sqlite')
            now = datetime(2026, 9, 21, tzinfo=timezone.utc)
            first = store.acquire_lease('repo', 'tag', 'owner', digest=DIGEST, ttl_seconds=10, now=now)
            before = store.snapshot(now=now).generation
            with patch.object(store, '_snapshot_unlocked', side_effect=AssertionError('whole registry scan')):
                store.touch_image('registry.example/repo:tag', when=now)
                renewed = store.renew_lease('repo', 'tag', 'owner', digest=DIGEST, ttl_seconds=20,
                                            now=now + timedelta(seconds=5))
                self.assertEqual(renewed.acquired_at, first.acquired_at)
                with self.assertRaisesRegex(ValueError, 'immutable'):
                    store.renew_lease('repo', 'tag', 'owner', digest='sha256:' + 'b' * 64,
                                      ttl_seconds=20, now=now + timedelta(seconds=6))
                with self.assertRaises(RegistryImageLeaseNotFound):
                    store.renew_lease('repo', 'tag', 'owner', digest=DIGEST, ttl_seconds=20,
                                      now=now + timedelta(seconds=30))
            after = store.snapshot(now=now + timedelta(seconds=30))
            self.assertFalse(after.leases)
            self.assertGreater(after.generation, before)
            self.assertEqual(len(after.records), 1)

    def test_health_is_read_only_and_does_not_contend_with_reserved_writer(self):
        with TemporaryDirectory() as raw:
            store = RegistryUsageStore(Path(raw) / 'registry.sqlite')
            with sqlite3.connect(store.path) as writer:
                writer.execute('BEGIN IMMEDIATE')
                with patch.object(store, '_transaction', side_effect=AssertionError('writer health check')):
                    store.check_readable()
                writer.rollback()
            store.path.unlink()
            with self.assertRaises(RegistryUsageStateError):
                store.check_readable()
            self.assertFalse(store.path.exists())

    def test_health_rejects_broken_schema(self):
        with TemporaryDirectory() as raw:
            store = RegistryUsageStore(Path(raw) / 'registry.sqlite')
            with sqlite3.connect(store.path) as db:
                db.execute('DROP TABLE registry_leases')
            with self.assertRaises(RegistryUsageStateError):
                store.check_readable()

    def test_maintenance_still_prunes_unrelated_expired_leases(self):
        with TemporaryDirectory() as raw:
            store = RegistryUsageStore(Path(raw) / 'registry.sqlite')
            now = datetime(2026, 9, 21, tzinfo=timezone.utc)
            store.acquire_lease('repo', 'tag', 'expired', digest=DIGEST, ttl_seconds=1, now=now)
            store.acquire_reference('repo', 'tag', 'permanent', digest=DIGEST, now=now)
            store.touch_image('registry.example/repo:tag', when=now + timedelta(seconds=2))
            snapshot = store.snapshot(now=now + timedelta(seconds=2))
            self.assertEqual(set(snapshot.leases), {('repo', 'tag', 'permanent')})
