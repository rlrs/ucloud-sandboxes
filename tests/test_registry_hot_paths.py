"""Registry capacity checks and snapshots stay cheap at node density.

Capacity checks run under SQLite's node-wide writer lock and snapshots run in
one-second loops; neither may decode every registration's JSON.
"""

from contextlib import closing
import sqlite3
import unittest
import unittest.mock
from unittest.mock import patch

from ucloud_sandboxes.direct_registry import DirectRegistryError, DirectSandboxRegistry
from tests import test_published_workspace_capacity as published

TEST_TIER = "contract"


class RegistryHotPathTests(unittest.TestCase):
    def setUp(self):
        fixture = published.PublishedWorkspaceCapacityTests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        self.fixture = fixture
        self.registry = fixture.registry
        self.workspace = fixture.workspace
        self._release, self._owned = fixture._release, fixture._owned
        self.registry.hard_disk_capacity_mb = 10 * fixture.claim
        fixture._plan("planned")  # a claim before its quota is committed

    def _json_reserved_mb(self):
        # The pre-ledger computation, straight from each record.
        return sum(
            record.quota_total_mb
            if record.quota_total_mb is not None
            else record.spec.requested_resources().disk_mb
            for record in self.registry.snapshot().records
        )

    def _ledger_reserved_mb(self):
        with self.registry._transaction(write=False) as connection:
            return self.registry._reserved_disk_bytes(connection) // 1024**2

    def test_ledger_matches_records_through_lifecycle(self):
        self.assertEqual(self._ledger_reserved_mb(), self._json_reserved_mb())
        self.assertTrue(self._release())
        self.assertEqual(
            self._ledger_reserved_mb(), self._json_reserved_mb() - self.workspace
        )
        self.registry.reserve_workspace_for_mount("one", 1)
        self.assertEqual(self._ledger_reserved_mb(), self._json_reserved_mb())

    def test_version7_upgrade_backfills_the_ledger(self):
        expected = self._ledger_reserved_mb()
        with closing(sqlite3.connect(self.registry.path)) as conn:
            conn.execute("DROP TABLE registration_disk")
            conn.execute("PRAGMA user_version=7")
        reopened = DirectSandboxRegistry(
            self.registry.path, hard_disk_capacity_mb=self.registry.hard_disk_capacity_mb
        )
        with reopened._transaction(write=False) as connection:
            self.assertEqual(reopened._reserved_disk_bytes(connection) // 1024**2, expected)
        with closing(sqlite3.connect(self.registry.path)) as conn:
            self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 9)

    def _owner(self):
        owner = DirectSandboxRegistry(
            self.registry.path, hard_disk_capacity_mb=self.registry.hard_disk_capacity_mb, owner=True
        )
        self.addCleanup(owner.close)
        return owner

    def test_owner_rebuilds_decode_only_changed_rows(self):
        owner = self._owner()
        first = owner.snapshot()
        decode = patch.object(
            DirectSandboxRegistry, "_decode", wraps=DirectSandboxRegistry._decode
        )
        with decode as calls:
            self.assertIs(owner.snapshot(), first)
            calls.assert_not_called()
        # Foreign commits: the owner rereads every row and decodes only "two".
        self.assertTrue(self._release())  # bumps activity, not the record
        self._owned("two")
        owner._index_checked_at = float("-inf")  # The recheck interval passed.
        with decode as calls:
            second = owner.snapshot()
        self.assertEqual(calls.call_count, 1)
        self.assertIn("two", second.by_sandbox_id)
        self.assertIs(second.get("one"), first.get("one"))

    def test_owner_reads_use_no_connection_and_see_external_commits(self):
        owner = self._owner()
        first = owner.get("one")
        owner.disk_claims_mb()
        idle = patch.multiple(
            owner, _borrow=unittest.mock.DEFAULT, _refresh_owned_index=unittest.mock.DEFAULT,
            _check_file=unittest.mock.DEFAULT,
        )
        owner._index_checked_at = float("inf")  # A slow host must not fall due for the recheck.
        with idle as never:
            for _ in range(3):
                self.assertIs(owner.get("one"), first)
                self.assertIsNone(owner.get("absent"))
                self.assertEqual(owner.activity_revision(), self.registry.activity_revision())
                self.assertEqual(owner.snapshot().records, self.registry.snapshot().records)
                self.assertTrue(owner.references_image(first.image_id))
                self.assertEqual(owner.disk_claims_mb(), self.registry.disk_claims_mb())
        for mock in never.values():
            mock.assert_not_called()
        external = DirectSandboxRegistry(self.registry.path)
        deleting = external.begin_delete("one", expected_revision=first.revision)
        # The owner's next write proves its index against the file first.
        decode = patch.object(
            DirectSandboxRegistry, "_decode", wraps=DirectSandboxRegistry._decode
        )
        with decode as calls:
            owner.commit_deleted("one", sandbox_generation=1, expected_revision=deleting.revision)
        self.assertEqual(calls.call_count, 1)
        self.assertIsNone(owner.get("one"))
        self.assertEqual(owner.activity_revision(), external.activity_revision())
        self.assertEqual(owner.disk_claims_mb(), external.disk_claims_mb())

    def test_changed_schema_or_metadata_falls_back_to_validation(self):
        self.registry.get("one")
        with closing(sqlite3.connect(self.registry.path)) as conn:
            conn.execute("PRAGMA ignore_check_constraints = ON")
            conn.execute("UPDATE registry_metadata SET activity_revision = -1")
            conn.commit()
        with self.assertRaisesRegex(DirectRegistryError, "metadata"):
            self.registry.get("one")
        with closing(sqlite3.connect(self.registry.path)) as conn:
            conn.execute("DROP TABLE registry_metadata")
        with self.assertRaisesRegex(DirectRegistryError, "schema"):
            self.registry.get("one")


if __name__ == "__main__":
    unittest.main()
