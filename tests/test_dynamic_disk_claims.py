"""Dynamic disk claims: charge what a sandbox has demonstrated, never more
than the node physically has.

Workspace grants and park-time checkpoint space move a registration's claim;
fixed (legacy and upgraded) registrations keep their lifetime claim.
"""

from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory
import sqlite3
import unittest

from ucloud_sandboxes.direct_registry import (
    DirectRegistryCapacityUnavailable,
    DirectSandboxRegistry,
    DiskClaim,
)
from ucloud_sandboxes.direct_warden import DirectSandbox
from ucloud_sandboxes.sandbox import SandboxSpec


def _spec(name):
    return SandboxSpec(id=name, image="registry/image@sha256:" + "a" * 64,
                       memory_mb=1024, disk_mb=4096, parkable=True)


class DynamicDiskClaimTests(unittest.TestCase):
    def setUp(self):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name).resolve()
        self.registry = DirectSandboxRegistry(self.root / "registry.sqlite",
                                              hard_disk_capacity_mb=4000)

    def _plan(self, name, claim=DiskClaim(1024, 64), split=True):
        return self.registry.plan(
            spec=_spec(name), sandbox_generation=1, operation_id="create:" + name,
            runtime_compatibility_sha256="b" * 64, split_memory_backing=split,
            initial_claim=claim,
        )

    def _owned(self, name, claim=DiskClaim(1024, 64)):
        planned = self._plan(name, claim)
        quota = self.registry.commit_quota(
            name, expected_revision=planned.revision, project_id=200000,
            total_mb=planned.spec.requested_resources().disk_mb, quota_path=self.root / name,
        )
        incarnation = f"{name}.sandbox-1"
        rootfs = self.registry.commit_rootfs(
            name, expected_revision=quota.revision, image_id="sha256:" + "e" * 64,
            sandbox=DirectSandbox(
                sandbox_id=name, sandbox_generation=1, container_id="f" * 64,
                spec_sha256=quota.spec_sha256, rootfs_sha256="d" * 64,
                bundle=self.root / "bundles" / name, memory_directory=incarnation,
                workspace_directory="workspace-" + incarnation,
                memory=planned.memory_reference,
            ),
        )
        return self.registry.commit_owned(name, expected_revision=rootfs.revision)

    def _reserved_mb(self):
        with self.registry._transaction(write=False) as connection:
            return self.registry._reserved_disk_bytes(connection) // 1024**2

    def test_plans_charge_the_initial_claim_not_the_maximum(self):
        # The 7,232 MiB maximum would not fit twice; two initial claims do.
        self._plan("one")
        self._plan("two")
        self.assertEqual(self._reserved_mb(), 2 * 1088)
        self.assertEqual(self.registry.disk_claim("one", 1), DiskClaim(1024, 64))
        self.assertEqual(self.registry.disk_claims_mb(), {("one", 1): 1088, ("two", 1): 1088})
        with self.assertRaises(DirectRegistryCapacityUnavailable):
            self._plan("three", DiskClaim(2000, 64))
        with self.assertRaises(ValueError):
            self._plan("legacy", split=False)

    def test_admitted_growth_refuses_without_headroom_and_recorded_bytes_always_land(self):
        self._owned("one")
        self._owned("two")
        grown = self.registry.update_disk_claim("one", 1, workspace_mb=2048, require_capacity=True)
        self.assertEqual(grown, DiskClaim(2048, 64))
        with self.assertRaises(DirectRegistryCapacityUnavailable):
            self.registry.update_disk_claim("two", 1, workspace_mb=2048, require_capacity=True)
        self.assertEqual(self.registry.disk_claim("two", 1), DiskClaim(1024, 64))
        # A committed checkpoint exists whether or not it fits: record it.
        self.registry.update_disk_claim("two", 1, memory_mb=900)
        self.assertEqual(self._reserved_mb(), 2048 + 64 + 1024 + 900)
        # Shrinking never needs headroom.
        self.registry.update_disk_claim("two", 1, memory_mb=64, require_capacity=True)
        self.assertEqual(self._reserved_mb(), 2048 + 64 + 1024 + 64)

    def test_published_workspace_releases_the_dynamic_grant_and_remount_recharges_it(self):
        self._owned("one")
        self.registry.update_disk_claim("one", 1, workspace_mb=1500)
        # The caller passes the ceiling; the dynamic row releases its own claim.
        self.assertTrue(self.registry.release_published_workspace(
            "one", 1, workspace_mb=4096, expected_mount_epoch=0))
        self.assertEqual(self._reserved_mb(), 64)
        self.assertEqual(self.registry.disk_claims_mb(), {("one", 1): 64})
        # While published, workspace updates do not charge; remount does.
        self.registry.update_disk_claim("one", 1, workspace_mb=1024, require_capacity=True)
        self.assertEqual(self._reserved_mb(), 64)
        self.registry.hard_disk_capacity_mb = 1000
        with self.assertRaises(DirectRegistryCapacityUnavailable):
            self.registry.reserve_workspace_for_mount("one", 1)
        self.registry.hard_disk_capacity_mb = 4000
        self.registry.reserve_workspace_for_mount("one", 1)
        self.assertEqual(self._reserved_mb(), 1024 + 64)

    def test_record_writes_do_not_reset_a_dynamic_claim(self):
        self._owned("one")
        self.registry.update_disk_claim("one", 1, workspace_mb=3000, memory_mb=10)
        record = self.registry.get("one")
        self.registry.begin_delete("one", expected_revision=record.revision)
        self.assertEqual(self.registry.disk_claim("one", 1), DiskClaim(3000, 10))

    def test_fixed_claims_ignore_updates(self):
        self.registry.hard_disk_capacity_mb = 10**6
        self.registry.plan(spec=_spec("legacy"), sandbox_generation=1, operation_id="create:legacy",
                           runtime_compatibility_sha256="b" * 64, split_memory_backing=True)
        self.assertIsNone(self.registry.disk_claim("legacy", 1))
        self.assertIsNone(self.registry.update_disk_claim("legacy", 1, workspace_mb=1))
        self.assertEqual(self._reserved_mb(), _spec("legacy").requested_resources().disk_mb)

    def test_adopting_a_published_fixed_claim_keeps_the_total(self):
        self.registry.hard_disk_capacity_mb = 10**6
        planned = self._plan("one", claim=None)
        quota = self.registry.commit_quota(
            "one", expected_revision=planned.revision, project_id=200000,
            total_mb=planned.spec.requested_resources().disk_mb, quota_path=self.root / "one")
        incarnation = "one.sandbox-1"
        rootfs = self.registry.commit_rootfs(
            "one", expected_revision=quota.revision, image_id="sha256:" + "e" * 64,
            sandbox=DirectSandbox(
                sandbox_id="one", sandbox_generation=1, container_id="f" * 64,
                spec_sha256=quota.spec_sha256, rootfs_sha256="d" * 64,
                bundle=self.root / "bundles" / "one", memory_directory=incarnation,
                workspace_directory="workspace-" + incarnation, memory=planned.memory_reference))
        self.registry.commit_owned("one", expected_revision=rootfs.revision)
        self.assertTrue(self.registry.release_published_workspace(
            "one", 1, workspace_mb=4096, expected_mount_epoch=0))
        before = self._reserved_mb()
        adopted = self.registry.update_disk_claim("one", 1, adopt=True)
        self.assertEqual(adopted.workspace_mb, 4096)
        self.assertEqual(self._reserved_mb(), before)
        self.registry.reserve_workspace_for_mount("one", 1)
        self.assertEqual(self._reserved_mb(), before + 4096)

    def test_version8_upgrade_keeps_each_registration_fixed(self):
        self.registry.hard_disk_capacity_mb = 10**6
        self._plan("one")
        self.registry.plan(spec=_spec("two"), sandbox_generation=1, operation_id="create:two",
                           runtime_compatibility_sha256="b" * 64, split_memory_backing=True)
        with closing(sqlite3.connect(self.registry.path)) as conn, conn:
            conn.execute("DROP TABLE registration_disk")
            conn.execute(
                "CREATE TABLE registration_disk (\n"
                "            sandbox_id TEXT PRIMARY KEY,\n"
                "            sandbox_generation INTEGER NOT NULL CHECK (sandbox_generation > 0),\n"
                "            reserved_mb INTEGER NOT NULL CHECK (reserved_mb >= 0)\n"
                "        ) STRICT"
            )
            conn.execute("PRAGMA user_version=8")
        reopened = DirectSandboxRegistry(self.registry.path, hard_disk_capacity_mb=10**6)
        # Upgraded registrations were created with full-size backing.
        full = _spec("one").requested_resources().disk_mb
        self.assertEqual(reopened.disk_claims_mb(), {("one", 1): full, ("two", 1): full})
        self.assertIsNone(reopened.disk_claim("one", 1))
        with closing(sqlite3.connect(self.registry.path)) as conn:
            self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 9)


class GatewayInitialClaimTests(unittest.TestCase):
    def _heartbeat(self, grant=1024, idle=64):
        from dataclasses import replace
        from tests.test_wake_capacity import WakeCapacityTests
        heartbeat = WakeCapacityTests().heartbeat(active=0)
        return replace(heartbeat, runtime_metrics=replace(
            heartbeat.runtime_metrics, storage_workspace_grant_mb=grant,
            storage_memory_idle_claim_mb=idle))

    def _route(self, parkable=True, state="creating"):
        from tests import test_control_plane as fixtures
        from ucloud_sandboxes.models import ResourceQuantity
        spec = _spec("burst")
        return fixtures._sandbox_route(
            sandbox_id="burst", node_id="node", job_id="job", node_url="http://node:8090",
            state=state, resources=spec.requested_resources(),
            spec={**spec.to_dict(), "parkable": parkable},
        )

    def test_in_flight_create_is_charged_the_advertised_initial_claim(self):
        from ucloud_sandboxes.placement_accounting import _node_reserved_route_resources
        full = _spec("burst").requested_resources().disk_mb
        charged = _node_reserved_route_resources(self._heartbeat(), [self._route()])
        self.assertEqual(charged.disk_mb, 1024 + 64)
        # File-backed memory keeps the formula memory component.
        charged = _node_reserved_route_resources(self._heartbeat(idle=0), [self._route()])
        self.assertEqual(charged.disk_mb, 1024 + full - 4096)
        # Older workers, and non-parkable sandboxes, keep the full claim.
        for heartbeat, route in ((self._heartbeat(0, 0), self._route()),
                                 (self._heartbeat(), self._route(parkable=False))):
            self.assertEqual(_node_reserved_route_resources(heartbeat, [route]).disk_mb, full)


if __name__ == "__main__":
    unittest.main()
