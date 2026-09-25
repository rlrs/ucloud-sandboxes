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


if __name__ == "__main__":
    unittest.main()
