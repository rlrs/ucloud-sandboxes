"""Exact-owner reads avoid transaction setup without weakening placement snapshots."""

from contextlib import contextmanager
from dataclasses import replace
import os
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch
from uuid import uuid4

from tests.test_routing import sandbox_route

DSN = os.environ.get("UCLOUD_TEST_POSTGRES_DSN")


@unittest.skipUnless(DSN, "requires isolated PostgreSQL")
class SingleStatementRoutingTests(unittest.TestCase):
    def setUp(self):
        from ucloud_sandboxes.shared_control.routing_repository import PostgresRoutingStore

        self.temp = TemporaryDirectory()
        self.path = Path(self.temp.name) / "routes"
        self.schema = "ucloud_routing_single_" + uuid4().hex
        self.store = PostgresRoutingStore(self.path, dsn=DSN, schema=self.schema)
        self.store.migrate()
        self.route = self.store.upsert_sandbox(
            sandbox_route(
                sandbox_id="single",
                node_id="n",
                job_id="j",
                node_url="http://node",
                state="running",
            )
        )

    def tearDown(self):
        import psycopg
        from psycopg import sql

        self.store.close()
        with psycopg.connect(DSN, autocommit=True) as conn:
            conn.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(self.schema))
            )
        self.temp.cleanup()

    def test_exact_read_issues_one_statement_and_no_transaction_setup(self):
        statements = []
        transactions = []
        original = self.store.pool.connection

        class ObservedConnection:
            def __init__(self, conn):
                self.conn = conn

            def execute(self, query, parameters=()):
                statements.append(query)
                return self.conn.execute(query, parameters)

            def transaction(self):
                transactions.append(True)
                return self.conn.transaction()

        @contextmanager
        def observed():
            with original() as conn:
                yield ObservedConnection(conn)

        with patch.object(self.store.pool, "connection", observed):
            self.assertEqual(self.store.get_sandbox("single"), self.route)
        self.assertEqual(len(statements), 1)
        self.assertTrue(statements[0].strip().startswith("SELECT"))
        self.assertEqual(transactions, [])

        from ucloud_sandboxes.routing import ExecRoute

        exec_route = ExecRoute(
            session_id="exec-single", sandbox_id="single", node_id="n",
            job_id="j", node_url="http://node",
        )
        self.store.upsert_exec(exec_route)
        statements.clear()
        with patch.object(self.store.pool, "connection", observed):
            stored = self.store.get_exec("exec-single")
            self.assertIsNone(self.store.get_exec("absent"))
        self.assertEqual(stored.sandbox_id, "single")
        self.assertEqual(len(statements), 2)  # One per exec poll lookup.
        self.assertEqual(transactions, [])

    def test_nested_read_reuses_uncommitted_transaction_and_snapshot(self):
        with self.store._transaction() as conn:
            conn.execute(
                "UPDATE sandboxes SET state=? WHERE sandbox_id=?", ("parked", "single")
            )
            with patch.object(
                self.store.pool,
                "connection",
                side_effect=AssertionError("nested read borrowed another connection"),
            ):
                self.assertEqual(self.store.get_sandbox("single").state, "parked")
        self.assertEqual(self.store.get_sandbox("single").state, "parked")

    def test_placement_snapshot_survives_concurrent_commit(self):
        with self.store._transaction():
            before = self.store.get_sandbox("single")
            with self.store.pool.connection() as other:
                other.execute(
                    "UPDATE sandboxes SET activity_epoch=activity_epoch+1 WHERE sandbox_id=%s",
                    ("single",),
                )
            self.assertEqual(self.store.get_sandbox("single"), before)
        self.assertEqual(
            self.store.get_sandbox("single").activity_epoch, before.activity_epoch + 1
        )

    def test_external_read_observes_delete_and_new_generation(self):
        self.store.delete_sandbox("single")
        self.assertIsNone(self.store.get_sandbox("single"))
        newer = replace(
            self.route,
            generation=self.route.generation + 1,
            create_operation_id="create-new",
        )
        self.store.upsert_sandbox(newer)
        self.assertEqual(self.store.get_sandbox("single").generation, newer.generation)

    def test_replaced_authority_descriptor_fails_before_read(self):
        self.path.write_text("descriptor")
        self.store.bind_authority()
        replacement = self.path.with_suffix(".replacement")
        replacement.write_text("changed")
        replacement.replace(self.path)
        # Replacement is detected by the next once-a-second recheck.
        self.store.get_sandbox("single")
        self.store._authority_checked_at = float("-inf")
        with self.assertRaisesRegex(sqlite3.DatabaseError, "descriptor was replaced"):
            self.store.get_sandbox("single")

    def test_query_failure_is_mapped_and_does_not_poison_pool(self):
        with self.assertRaisesRegex(sqlite3.DatabaseError, "routing read failed"):
            self.store._fetchone_readonly("SELECT * FROM nonexistent_read_table")
        self.assertEqual(self.store.get_sandbox("single"), self.route)

    def test_multiquery_reader_retains_coherent_snapshot(self):
        with self.store._connect() as conn:
            before = conn.execute(
                "SELECT activity_epoch FROM sandboxes WHERE sandbox_id=?", ("single",)
            ).fetchone()[0]
            with self.store.pool.connection() as other:
                other.execute(
                    "UPDATE sandboxes SET activity_epoch=activity_epoch+1 WHERE sandbox_id=%s",
                    ("single",),
                )
            self.assertEqual(
                conn.execute(
                    "SELECT activity_epoch FROM sandboxes WHERE sandbox_id=?",
                    ("single",),
                ).fetchone()[0],
                before,
            )
        self.assertEqual(self.store.get_sandbox("single").activity_epoch, before + 1)
