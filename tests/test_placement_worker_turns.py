"""Same-worker placement turns must not hold pooled connections while queued."""

from concurrent.futures import ThreadPoolExecutor
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Event
from time import monotonic, sleep
import unittest
from uuid import uuid4


DSN = os.environ.get("UCLOUD_TEST_POSTGRES_DSN")


@unittest.skipUnless(DSN, "requires isolated PostgreSQL")
class PlacementWorkerTurnTests(unittest.TestCase):
    def setUp(self):
        from ucloud_sandboxes.shared_control.routing_repository import PostgresRoutingStore

        self.temp = TemporaryDirectory()
        self.schema = "ucloud_routing_turns_" + uuid4().hex
        self.store = PostgresRoutingStore(
            Path(self.temp.name) / "routes",
            dsn=DSN,
            schema=self.schema,
            timeout_seconds=3,
            max_connections=2,
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
        self.temp.cleanup()

    def test_queued_same_worker_turns_leave_connections_for_other_workers(self):
        holding, release = Event(), Event()
        order = []

        def hold():
            holding.set()
            release.wait(10)
            order.append("holder")

        def queued(index):
            return lambda: order.append(index)

        with ThreadPoolExecutor(5) as pool:
            holder = pool.submit(self.store.run_placement, hold, worker_id="hot")
            self.assertTrue(holding.wait(5))
            waiters = [
                pool.submit(self.store.run_placement, queued(i), worker_id="hot")
                for i in range(3)
            ]
            sleep(0.2)  # Let every waiter queue behind the hot worker's turn.
            began = monotonic()
            self.assertEqual(
                self.store.run_placement(lambda: "cold", worker_id="cold"), "cold"
            )
            self.assertIsNone(self.store.get_sandbox_readonly("absent"))
            self.assertLess(monotonic() - began, 1)
            self.assertEqual(order, [])
            release.set()
            holder.result(5)
            for waiter in waiters:
                waiter.result(5)
        self.assertEqual(order[0], "holder")
        self.assertEqual(sorted(order[1:]), [0, 1, 2])
        self.assertEqual(self.store._worker_turns, {})

    def test_heartbeat_reconcile_locks_worker_rows_only_at_commit(self):
        import psycopg
        from unittest.mock import patch
        from tests.test_routing import sandbox_route
        from ucloud_sandboxes.models import SandboxInventoryEntry, utc_now

        route = self.store.upsert_sandbox(sandbox_route(
            sandbox_id="resident", node_id="node-1", job_id="job-1",
            node_url="http://node-1:8090", state="running",
            node_epoch="boot-1", activity_epoch=10,
        ))
        observation = SandboxInventoryEntry(
            sandbox_id=route.sandbox_id, generation=route.generation,
            operation_id=route.create_operation_id, spec_hash=route.spec_hash,
            state="running", resources=route.resources,
        )
        probes = []
        absence_scan = self.store._sandbox_routes_for_node_url_unlocked

        def probe(conn, *args, **kwargs):
            # Mid-body: an overlapping placement must be able to lock both the
            # worker revision row and the resident's route row.
            with psycopg.connect(DSN, autocommit=True) as other:
                other.execute(
                    psycopg.sql.SQL("SET search_path TO {}").format(
                        psycopg.sql.Identifier(self.schema)))
                other.execute("SET lock_timeout = '200ms'")
                with other.transaction():
                    probes.append(other.execute(
                        "SELECT identity FROM worker_capacity_revisions"
                        " WHERE identity='job:job-1' FOR UPDATE").fetchall())
                    probes.append(other.execute(
                        "SELECT sandbox_id FROM sandboxes WHERE sandbox_id='resident'"
                        " FOR UPDATE").fetchall())
            return absence_scan(conn, *args, **kwargs)

        with patch.object(self.store, "_sandbox_routes_for_node_url_unlocked", side_effect=probe):
            self.store.reconcile_sandboxes_for_node(
                route.node_url, [observation], node_id=route.node_id,
                job_id=route.job_id, reported_sandbox_ids={route.sandbox_id},
                observed_at=utc_now().isoformat(), node_epoch="boot-1",
                activity_epoch=12,
            )
        self.assertEqual(probes, [[("job:job-1",)], [("resident",)]])
        # The accepted inventory still advanced the revision and watermark.
        with self.store.pool.connection() as conn:
            self.assertEqual(conn.execute(
                "SELECT revision FROM worker_capacity_revisions WHERE identity='job:job-1'"
            ).fetchone()["revision"], 2)
        self.assertEqual(self.store.get_sandbox_readonly("resident").activity_epoch, 12)


if __name__ == "__main__":
    unittest.main()
