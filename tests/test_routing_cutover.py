import os
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
import unittest
from uuid import uuid4

from tests.test_routing import sandbox_route
from ucloud_sandboxes.routing import (
    RoutingStore,
    open_routing_store,
    SandboxRouteAllocation,
)
from ucloud_sandboxes.models import ResourceQuantity

DSN = os.environ.get("UCLOUD_TEST_POSTGRES_DSN")


@unittest.skipUnless(DSN, "requires isolated PostgreSQL")
class RoutingCutoverTests(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "routes.sqlite"
        self.dsn = Path(self.temp.name) / "postgres.dsn"
        self.dsn.write_text(DSN)
        self.schema = "ucloud_routing_cutover_" + uuid4().hex
        self.old = RoutingStore(self.path)

    def tearDown(self):
        import psycopg
        from psycopg import sql
        from ucloud_sandboxes.routing import _POSTGRES_ROUTING_STORES

        for key, store in list(_POSTGRES_ROUTING_STORES.items()):
            if key[1] == self.path.resolve():
                store.close()
                del _POSTGRES_ROUTING_STORES[key]
        with psycopg.connect(DSN, autocommit=True) as conn:
            conn.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                    sql.Identifier(self.schema)
                )
            )

    def test_idle_cutover_preserves_generation_and_fences_old_authority(self):
        from ucloud_sandboxes.shared_control.routing_cutover import cutover

        route = sandbox_route(
            sandbox_id="reused",
            node_id="n",
            job_id="j",
            node_url="http://n",
            generation=7,
        )
        self.old.upsert_sandbox(route)
        self.old.delete_sandbox("reused")
        self.old.upsert_pending("queued", ResourceQuantity(memory_mb=1024))
        receipt = cutover(self.path, dsn_file=self.dsn, schema=self.schema)
        self.assertTrue(Path(receipt["backup"]).exists())
        self.assertEqual(receipt["tables"]["sandbox_generation_hwm"]["rows"], 1)
        store = open_routing_store(self.path)
        self.assertIs(open_routing_store(self.path), store)
        self.assertTrue(store.distributed)
        self.assertIsNotNone(store.get_pending("queued"))
        new, _ = store.allocate_sandbox_create_with_pending(
            SandboxRouteAllocation(
                sandbox_id="reused",
                node_id="n",
                job_id="j",
                node_url="http://n",
                resources=route.resources,
                spec=route.spec,
            ),
            spec_hash=route.spec_hash,
        )
        self.assertEqual(new.generation, 8)
        with self.assertRaises(sqlite3.DatabaseError):
            RoutingStore(self.path)
        with self.assertRaises(sqlite3.DatabaseError):
            self.old.upsert_pending("stale", ResourceQuantity(memory_mb=1))

    def test_active_fleet_refuses_cutover(self):
        from ucloud_sandboxes.shared_control.routing_cutover import cutover

        self.old.upsert_sandbox(
            sandbox_route(
                sandbox_id="active", node_id="n", job_id="j", node_url="http://n"
            )
        )
        with self.assertRaisesRegex(ValueError, "idle fleet"):
            cutover(self.path, dsn_file=self.dsn, schema=self.schema)
        self.assertFalse(open_routing_store(self.path).distributed)
        self.assertIsNotNone(self.old.get_sandbox("active"))
