"""A published, unmounted workspace stops consuming the shared disk ledger.

The storage daemon stops charging a published workspace. The registry must
agree, or the gateway sees headroom that worker admission refuses; every mount
must re-reserve before it touches local disk, fenced against late publishers.
"""

from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory
import sqlite3
import threading
import unittest
from unittest.mock import MagicMock

from ucloud_sandboxes.direct_registry import (
    DirectRegistryCapacityUnavailable,
    DirectRegistryConflictError,
    DirectSandboxRegistry,
)
from ucloud_sandboxes.direct_warden import DirectRunscWarden, DirectSandbox
from ucloud_sandboxes.storage_native_daemon import StorageVolumeState
from tests.test_direct_registry import DirectRegistryTests

MIB = 1024**2


class PublishedWorkspaceCapacityTests(unittest.TestCase):
    def setUp(self):
        self.directory = TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name).resolve()
        self.spec = DirectRegistryTests().spec("one")
        self.claim = self.spec.requested_resources().disk_mb
        self.workspace = self.spec.disk_mb
        # Exactly one full claim fits.
        self.registry = DirectSandboxRegistry(
            self.root / "registry.sqlite", hard_disk_capacity_mb=self.claim
        )
        self._owned("one")

    def _plan(self, name):
        return self.registry.plan(
            spec=DirectRegistryTests().spec(name), sandbox_generation=1,
            operation_id="create:" + name, runtime_compatibility_sha256="b" * 64,
        )

    def _owned(self, name):
        planned = self._plan(name)
        quota = self.registry.commit_quota(
            name, expected_revision=planned.revision, project_id=200000,
            total_mb=self.claim, quota_path=self.root / name,
        )
        rootfs = self.registry.commit_rootfs(
            name, expected_revision=quota.revision, image_id="sha256:" + "e" * 64,
            sandbox=DirectSandbox(
                sandbox_id=name, sandbox_generation=1, container_id="f" * 64,
                spec_sha256=quota.spec_sha256, rootfs_sha256="d" * 64,
                bundle=self.root / "bundles" / name, memory_directory=name + ".1",
            ),
        )
        return self.registry.commit_owned(name, expected_revision=rootfs.revision)

    def _release(self, epoch=0):
        return self.registry.release_published_workspace(
            "one", 1, workspace_mb=self.workspace, expected_mount_epoch=epoch
        )

    def test_release_frees_workspace_and_mount_recharges_it(self):
        self.assertTrue(self._release())
        # The freed workspace alone does not fit another full claim here, but a
        # capacity that is exactly workspace-short now admits one.
        self.registry.hard_disk_capacity_mb = self.claim + self.claim - self.workspace
        self._owned("two")
        with self.assertRaises(DirectRegistryCapacityUnavailable):
            self.registry.reserve_workspace_for_mount("one", 1)
        # A refused remount keeps the workspace released: it stays parked.
        self.assertEqual(self.registry.workspace_mount_epoch("one", 1), 0)
        self.registry.hard_disk_capacity_mb = 2 * self.claim
        self.registry.reserve_workspace_for_mount("one", 1)
        self.assertEqual(self.registry.workspace_mount_epoch("one", 1), 1)
        with self.assertRaises(DirectRegistryCapacityUnavailable):
            self._plan("three")

    def test_late_publisher_cannot_uncharge_a_remounted_workspace(self):
        epoch = self.registry.workspace_mount_epoch("one", 1)
        self.registry.reserve_workspace_for_mount("one", 1)  # a wake won the race
        self.assertFalse(self._release(epoch))
        self.registry.hard_disk_capacity_mb = self.claim + self.claim - self.workspace
        with self.assertRaises(DirectRegistryCapacityUnavailable):
            self._plan("two")
        # A publication of the next park uses the current epoch.
        self.assertTrue(self._release(self.registry.workspace_mount_epoch("one", 1)))
        self._owned("two")

    def test_identity_is_fenced_and_delete_forgets_release(self):
        with self.assertRaises(DirectRegistryConflictError):
            self.registry.release_published_workspace(
                "one", 2, workspace_mb=self.workspace, expected_mount_epoch=0
            )
        with self.assertRaises(ValueError):
            self.registry.release_published_workspace(
                "one", 1, workspace_mb=0, expected_mount_epoch=0
            )
        self.assertTrue(self._release())
        with closing(sqlite3.connect(self.registry.path)) as conn:
            self.assertEqual(
                conn.execute("SELECT released_mb FROM workspace_capacity").fetchall(),
                [(self.workspace,)],
            )

    def test_refused_create_is_retryable_capacity(self):
        with self.assertRaises(DirectRegistryCapacityUnavailable):
            self._plan("two")

    def test_version6_registry_upgrades_without_releases(self):
        with closing(sqlite3.connect(self.registry.path)) as conn:
            conn.execute("DROP TABLE workspace_capacity")
            conn.execute("DROP TABLE registration_disk")
            conn.execute("PRAGMA user_version=6")
        reopened = DirectSandboxRegistry(self.registry.path, hard_disk_capacity_mb=self.claim)
        self.assertEqual(reopened.workspace_mount_epoch("one", 1), 0)
        with closing(sqlite3.connect(self.registry.path)) as conn:
            self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 9)
        self.assertTrue(reopened.release_published_workspace(
            "one", 1, workspace_mb=self.workspace, expected_mount_epoch=0))


class WardenWorkspaceCapacityWiringTests(unittest.TestCase):
    """The Warden re-reserves before every mount and releases on publication."""

    def warden(self, capacity):
        warden = object.__new__(DirectRunscWarden)
        warden._claims_guard = threading.Lock()
        warden._workspace_mounts = {}
        warden.disk_capacity = capacity
        warden.storage = MagicMock()
        warden._storage_owner = MagicMock(return_value="owner")
        warden._validate_storage_record = MagicMock()
        return warden

    def sandbox(self, memory=True):
        sandbox = MagicMock(sandbox_id="one", sandbox_generation=1)
        sandbox.memory = object() if memory else None
        return sandbox

    def test_mount_reserves_before_storage_and_refusal_prevents_mount(self):
        capacity = MagicMock()
        order = []
        capacity.reserve_workspace_for_mount.side_effect = lambda *a: order.append("reserve")
        warden = self.warden(capacity)
        warden.storage.ensure_mounted.side_effect = lambda *a, **k: order.append("mount") or MagicMock()
        warden.ensure_workspace_mounted(self.sandbox(), operation_id="op")
        self.assertEqual(order, ["reserve", "mount"])
        capacity.reserve_workspace_for_mount.side_effect = DirectRegistryCapacityUnavailable("full")
        warden.storage.ensure_mounted.reset_mock()
        with self.assertRaises(DirectRegistryCapacityUnavailable):
            warden.ensure_workspace_mounted(self.sandbox(), operation_id="op")
        warden.storage.ensure_mounted.assert_not_called()

    def test_non_split_sandboxes_bypass_the_workspace_ledger(self):
        capacity = MagicMock()
        warden = self.warden(capacity)
        warden.ensure_workspace_mounted(self.sandbox(memory=False), operation_id="op")
        capacity.reserve_workspace_for_mount.assert_not_called()

    def test_release_uses_published_virtual_size_and_captured_epoch(self):
        capacity = MagicMock()
        warden = self.warden(capacity)
        record = MagicMock(state=StorageVolumeState.PUBLISHED, virtual_size=4096 * MIB + 1)
        warden._release_published_workspace_capacity(self.sandbox(), record, 3)
        capacity.release_published_workspace.assert_called_once_with(
            "one", 1, workspace_mb=4097, expected_mount_epoch=3
        )


if __name__ == "__main__":
    unittest.main()
