"""Real PostgreSQL races for exact worker capacity revision fences."""

from concurrent.futures import ThreadPoolExecutor
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Barrier, Event
import unittest
from uuid import uuid4

from tests.test_routing import sandbox_route
from ucloud_sandboxes.models import ResourceQuantity
from ucloud_sandboxes.routing import SandboxRouteAllocation, SandboxRouteConflictError
from ucloud_sandboxes.shared_control.routing_repository import PostgresRoutingStore

DSN = os.environ.get("UCLOUD_TEST_POSTGRES_DSN")


@unittest.skipUnless(DSN, "requires isolated PostgreSQL")
class PlacementCapacityConcurrencyTests(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.schema = "ucloud_routing_capacity_" + uuid4().hex
        self.store = PostgresRoutingStore(
            Path(self.temp.name) / "routes",
            dsn=DSN,
            schema=self.schema,
            max_connections=16,
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

    def allocate(self, sandbox_id, identity, *, spec_hash="a" * 64):
        node, job, url = identity
        return self.store.allocate_sandbox_create_with_pending(
            SandboxRouteAllocation(
                sandbox_id=sandbox_id,
                node_id=node,
                job_id=job,
                node_url=url,
                resources=ResourceQuantity(memory_mb=512),
                spec={"id": sandbox_id},
            ),
            spec_hash=spec_hash,
        )[0]

    def reserve(
        self, sandbox_id, identity, *, barrier=None, limit=1, spec_hash="a" * 64
    ):
        first = True

        def attempt():
            nonlocal first
            node, job, url = identity
            residents = self.store._sandbox_route_rows_readonly(
                node_identity=(node, job, url.rstrip("/"), url.rstrip("/") + "/"),
            )
            if first and barrier is not None:
                first = False
                barrier.wait(timeout=5)
            if len(residents) >= limit:
                return None
            return self.allocate(sandbox_id, identity, spec_hash=spec_hash)

        return self.store.run_placement(attempt)

    def race(self, identities, *, sandbox_ids=None, hashes=None):
        barrier = Barrier(len(identities))
        sandbox_ids = sandbox_ids or [
            "sandbox-" + str(i) for i in range(len(identities))
        ]
        hashes = hashes or ["a" * 64] * len(identities)
        with ThreadPoolExecutor(len(identities)) as executor:
            futures = [
                executor.submit(
                    self.reserve, sid, identity, barrier=barrier, spec_hash=hashed
                )
                for sid, identity, hashed in zip(sandbox_ids, identities, hashes)
            ]
            return [future.result(timeout=10) for future in futures]

    def test_same_worker_concurrent_empty_snapshot_does_not_overbook(self):
        results = self.race([("n", "j", "http://worker")] * 8)
        self.assertEqual(sum(route is not None for route in results), 1)
        self.assertGreater(self.store.serialization_retries, 0)

    def test_unrelated_workers_have_no_false_serialization_retries(self):
        identities = [
            ("n" + str(i), "j" + str(i), "http://worker-" + str(i)) for i in range(12)
        ]
        results = self.race(identities)
        self.assertTrue(all(route is not None for route in results))
        self.assertEqual(self.store.serialization_retries, 0)

    def test_overlapping_node_identity_prevents_overbooking(self):
        results = self.race(
            [("shared-node", "j1", "http://one"), ("shared-node", "j2", "http://two")]
        )
        self.assertEqual(sum(route is not None for route in results), 1)

    def test_overlapping_job_identity_prevents_overbooking(self):
        results = self.race(
            [("n1", "shared-job", "http://one"), ("n2", "shared-job", "http://two")]
        )
        self.assertEqual(sum(route is not None for route in results), 1)

    def test_normalized_url_identity_prevents_overbooking(self):
        results = self.race([("n1", "j1", "http://same/"), ("n2", "j2", "http://same")])
        self.assertEqual(sum(route is not None for route in results), 1)

    def test_same_sandbox_different_workers_has_one_generation_and_owner(self):
        results = self.race(
            [("n1", "j1", "http://one"), ("n2", "j2", "http://two")],
            sandbox_ids=["same-sandbox", "same-sandbox"],
        )
        self.assertEqual({route.generation for route in results}, {1})
        self.assertEqual(len({route.job_id for route in results}), 1)
        self.assertEqual(len({route.create_operation_id for route in results}), 1)

    def test_different_spec_same_sandbox_cannot_overwrite_winner(self):
        barrier = Barrier(2)
        with ThreadPoolExecutor(2) as executor:
            futures = [
                executor.submit(
                    self.reserve,
                    "same-sandbox",
                    ("n" + str(i), "j" + str(i), "http://n" + str(i)),
                    barrier=barrier,
                    spec_hash=str(i + 1) * 64,
                )
                for i in range(2)
            ]
            routes, errors = [], []
            for future in futures:
                try:
                    routes.append(future.result(timeout=10))
                except SandboxRouteConflictError as exc:
                    errors.append(exc)
        self.assertEqual(len(routes), 1)
        self.assertEqual(len(errors), 1)
        self.assertEqual(
            self.store.get_sandbox("same-sandbox").spec_hash, routes[0].spec_hash
        )

    def test_migration_revision_invalidates_source_and_destination_snapshots(self):
        source = sandbox_route(
            sandbox_id="migrating",
            node_id="source-node",
            job_id="source-job",
            node_url="http://source",
            state="parked",
        )
        self.store.upsert_sandbox(source)
        snapshots_ready = Barrier(3)
        migration_done = Event()
        attempts = {"source": 0, "destination": 0}

        def admission(side):
            identity = (side + "-node", side + "-job", "http://" + side)

            def transaction():
                attempts[side] += 1
                self.store._sandbox_route_rows_readonly(
                    node_identity=(*identity, identity[2] + "/"),
                )
                if attempts[side] == 1:
                    snapshots_ready.wait(timeout=5)
                    self.assertTrue(migration_done.wait(timeout=5))
                return self.allocate("new-" + side, identity)

            return self.store.run_placement(transaction)

        with ThreadPoolExecutor(2) as executor:
            futures = [executor.submit(admission, side) for side in attempts]
            snapshots_ready.wait(timeout=5)
            try:
                self.store.begin_sandbox_migration(
                    source,
                    migration_id="migration-test",
                    destination_node_id="destination-node",
                    destination_job_id="destination-job",
                    destination_node_url="http://destination",
                )
            finally:
                migration_done.set()
            for future in futures:
                self.assertIsNotNone(future.result(timeout=10))
        self.assertGreaterEqual(attempts["source"], 2)
        self.assertGreaterEqual(attempts["destination"], 2)
