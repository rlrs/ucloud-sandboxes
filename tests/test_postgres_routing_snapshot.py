"""Routing read snapshots must keep GC's completeness proof and roots together."""

import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch
from uuid import uuid4

from tests.test_routing import sandbox_route

DSN = os.environ.get("UCLOUD_TEST_POSTGRES_DSN")


@unittest.skipUnless(DSN, "requires isolated PostgreSQL")
class PostgresRoutingSnapshotTests(unittest.TestCase):
    def setUp(self):
        from ucloud_sandboxes.shared_control.routing_repository import (
            PostgresRoutingStore,
        )

        self.directory = TemporaryDirectory()
        self.schema = "ucloud_routing_test_" + uuid4().hex
        self.store = PostgresRoutingStore(
            Path(self.directory.name) / "routes",
            dsn=DSN,
            schema=self.schema,
        )
        self.store.migrate()

    def tearDown(self):
        import psycopg
        from psycopg import sql

        self.store.close()
        with psycopg.connect(DSN, autocommit=True) as conn:
            conn.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(self.schema))
            )
        self.directory.cleanup()

    def test_gc_roots_use_the_snapshot_that_passed_completeness(self):
        from ucloud_sandboxes.shared_control.routing_repository import _Connection

        first = {"root": "already-published"}
        later = {"root": "not-in-the-checked-snapshot"}
        self.store.upsert_sandbox(
            sandbox_route(
                sandbox_id="original",
                node_id="node",
                job_id="job",
                node_url="http://worker",
                storage_snapshot=first,
            )
        )
        original_execute = _Connection.execute
        inserted = False

        def interleaved_execute(adapter, query, parameters=()):
            nonlocal inserted
            result = original_execute(adapter, query, parameters)
            if "LEFT JOIN sandbox_storage_dependencies" in query and not inserted:
                inserted = True
                # Another connection publishes a new route after the complete
                # snapshot was checked. Its roots must wait for the next scan.
                self.store.upsert_sandbox(
                    sandbox_route(
                        sandbox_id="concurrent",
                        node_id="node",
                        job_id="job",
                        node_url="http://worker",
                        storage_snapshot=later,
                    )
                )
                with self.store.pool.connection() as writer:
                    writer.execute(
                        "DELETE FROM sandbox_storage_dependencies WHERE sandbox_id='concurrent'"
                    )
            return result

        with patch.object(_Connection, "execute", interleaved_execute):
            self.assertEqual(
                self.store.storage_snapshot_dependencies_readonly(
                    require_complete=True
                ),
                [first],
            )
        self.assertTrue(inserted)
        with self.assertRaisesRegex(ValueError, "cannot GC"):
            self.store.storage_snapshot_dependencies_readonly(require_complete=True)

    def test_admission_scope_preserves_incoming_capacity(self):
        from tests.test_routing_migration_scope import assert_scoped_migrations

        assert_scoped_migrations(self, self.store)

    def test_plain_reader_is_read_only_and_returns_connection_cleanly(self):
        with self.store._connect() as conn:
            self.assertEqual(
                conn.execute("SHOW transaction_isolation").fetchone()[0],
                "repeatable read",
            )
            self.assertEqual(
                conn.execute("SHOW transaction_read_only").fetchone()[0], "on"
            )
            conn.execute("BEGIN")  # Canonical SQLite reader's explicit snapshot marker.
        # A later write must not inherit read-only transaction settings.
        self.store.upsert_sandbox(
            sandbox_route(
                sandbox_id="after-read",
                node_id="node",
                job_id="job",
                node_url="http://worker",
            )
        )
        self.assertIsNotNone(self.store.get_sandbox("after-read"))
