"""C3.2 group create: ``/v1/sandboxes:batch`` on power-of-k placement.

Tier: contract. A group packs onto one worker while it fits, overflows to the
next, and attaches its image once per worker. A definitely rejected member is
re-planned elsewhere; what no worker takes is pending demand with a retryable
answer. Members stay ordinary sandboxes, and a member deleted since is never
recreated by a repeated request. The multi-row intents keep the single
create's fences on SQLite and PostgreSQL, and a deleted group refuses them.
"""
from dataclasses import replace
import os
from pathlib import Path
import random
from tempfile import TemporaryDirectory
import unittest
from uuid import uuid4

from tests.harness import LocalFleet
from tests.harness.assembly import fixed_metrics
from tests.test_placement_choice import SHAPE, FakeClock, heartbeat
from ucloud_sandboxes.models import ResourceQuantity
from ucloud_sandboxes.placement_choice import FleetView, InflightOverlay, PlacementRequest, PowerOfKChooser
from ucloud_sandboxes.routing import RoutingStore, SandboxGroup, SandboxGroupDeletedError, SandboxRouteAllocation

TEST_TIER = "contract"
DSN = os.environ.get("UCLOUD_TEST_POSTGRES_DSN")
SPEC = {"image": "harness/base:1", "cpus": 1, "memory_mb": 256, "disk_mb": 1024, "network": "none"}
HASH = "a" * 64


def post_group(fleet, group_id, count, **changes):
    return fleet.request("POST", "/v1/sandboxes:batch", token="sandbox", payload={
        "group_id": group_id, "count": count, "spec": {**SPEC, **changes}})


def placed(fleet, response):
    """Members per node id, and each node's image pulls and create POSTs."""
    by_node = {}
    for member in response.json()["sandboxes"]:
        by_node.setdefault(fleet.route(member["id"]).node_id, []).append(member["id"])
    calls = {node.node_id: (node.requests.count(("POST", "/v1/images/pull")),
                            node.requests.count(("POST", "/v1/sandboxes"))) for node in fleet.nodes}
    return by_node, calls


class GroupFleetTests(unittest.TestCase):
    def test_a_group_packs_onto_one_worker_and_its_members_stay_ordinary(self):
        for postgres in (False, True):
            with self.subTest(postgres=postgres), LocalFleet(
                    nodes=2, create_placement="power_of_k", postgres=postgres) as fleet:
                created = post_group(fleet, "g", 4)
                self.assertEqual(created.status, 201, created.body)
                self.assertEqual(created.json()["counts"], {"running": 4})
                by_node, calls = placed(fleet, created)
                self.assertEqual([len(members) for members in by_node.values()], [4])
                node = next(iter(by_node))
                # One attach for the group; the other worker saw nothing.
                self.assertEqual(calls, {item.node_id: (1, 4) if item.node_id == node else (0, 0)
                                         for item in fleet.nodes})
                self.assertEqual(fleet.exec("g-0001", ["cat", "/opt/greeting"]).stdout, "hello from the image\n")
                self.assertEqual(fleet.delete("g-0002").status, 200)
                listed = fleet.request("GET", "/v1/sandboxes:batch/g", token="sandbox").json()["sandboxes"]
                self.assertEqual([item["status"] for item in listed], ["running", "running", "deleted", "running"])
                # A repeat reports the group and never recreates a deleted member.
                repeated = post_group(fleet, "g", 4)
                self.assertEqual((repeated.status, repeated.json()["counts"]), (200, {"running": 3, "deleted": 1}))
                self.assertIsNone(fleet.route("g-0002"))
                self.assertEqual(post_group(fleet, "g", 5).json()["error_code"], "sandbox_group_conflict")
                deleted = fleet.request("DELETE", "/v1/sandboxes:batch/g", token="sandbox")
                self.assertEqual(deleted.status, 200, deleted.body)
                self.assertEqual([fleet.route(f"g-{index:04d}") for index in range(4)], [None] * 4)
                self.assertEqual(post_group(fleet, "g", 4).json()["error_code"], "sandbox_group_deleted")
                self.assertEqual(fleet.request("GET", "/v1/sandboxes:batch/none", token="sandbox").status, 404)
                for payload in ({"group_id": "-g", "count": 1, "spec": SPEC}, {"group_id": "g", "count": 0, "spec": SPEC},
                                {"group_id": "g", "count": 1, "spec": {**SPEC, "id": "x"}}):
                    self.assertEqual(fleet.request("POST", "/v1/sandboxes:batch", payload=payload,
                                                   token="sandbox").status, 400)

    def test_overflow_spills_to_the_next_worker_with_one_attach_each(self):
        with LocalFleet(nodes=3, create_placement="power_of_k") as fleet:
            fleet.gateway.RequestHandlerClass.services.groups.budget = 2
            created = post_group(fleet, "spill", 5)
            self.assertEqual(created.status, 201, created.body)
            by_node, calls = placed(fleet, created)
            self.assertEqual(sorted(map(len, by_node.values())), [1, 2, 2])
            self.assertEqual(sorted(calls.values()), [(1, 1), (1, 2), (1, 2)])

    def test_a_definite_reject_replans_the_members_on_another_worker(self):
        with LocalFleet(nodes=2, create_placement="power_of_k", admission_wait_seconds=1) as fleet:
            first, second = fleet.nodes
            second.sample_metrics = lambda: replace(fixed_metrics(), cpu_percent=90.0)  # Ranks behind.
            fleet.heartbeat()
            # The first runs out of memory headroom after its last heartbeat.
            first.sample_metrics = lambda: replace(fixed_metrics(), memory_available_mb=100)
            created = post_group(fleet, "moved", 3)
            self.assertEqual(created.status, 201, created.body)
            for index in range(3):
                route = fleet.route(f"moved-{index:04d}")
                self.assertEqual((route.job_id, route.generation), (second.job_id, 2))
                self.assertIsNone(first.registration(route.sandbox_id))
            self.assertEqual(first.requests.count(("POST", "/v1/sandboxes")), 3)

    def test_leftovers_become_pending_and_a_repeat_places_them(self):
        with LocalFleet(create_placement="power_of_k") as fleet:
            node = fleet.nodes[0]
            fleet.create("seed")  # Its image is cached: the creates themselves are refused.
            fleet.heartbeat()
            self.assertEqual(node.drain("solo").status, 200)  # The gateway does not know.
            refused = post_group(fleet, "later", 2)
            self.assertEqual(refused.status, 503, refused.body)
            self.assertEqual((refused.json()["error_code"], refused.json()["retryable"], refused.json()["counts"]),
                             ("node_admission_closed", True, {"pending": 2}))
            self.assertEqual(refused.headers["X-UCloud-Sandbox-Retryable"], "true")
            self.assertEqual(refused.headers["X-UCloud-Group-Delivered"], "0")  # The queue requeues it.
            pending = fleet.gateway.RequestHandlerClass.routing_store.load().pending
            self.assertEqual({pending[f"later-{i:04d}"].failure_reason for i in range(2)}, {"node_admission_closed"})
            self.assertIsNone(fleet.route("later-0000"))
            node.drain("solo", draining=False)
            fleet.heartbeat()
            placed_now = post_group(fleet, "later", 2)
            self.assertEqual((placed_now.status, placed_now.json()["counts"]), (201, {"running": 2}))

    def test_ranked_placement_refuses_groups(self):
        with LocalFleet() as fleet:
            refused = post_group(fleet, "g", 2)
            self.assertEqual((refused.status, refused.json()["error_code"]),
                             (501, "sandbox_group_create_unavailable"))


class GroupPlanTests(unittest.TestCase):
    def chooser(self, k):
        return PowerOfKChooser(InflightOverlay(clock=FakeClock()), rng=random.Random(3),
                               target_creates_per_node=8, api_processes=1, k=k)

    def test_a_resident_worker_joins_the_sample(self):
        view = FleetView(tuple(heartbeat(f"n{index}", cpu=50.0 if index == 7 else 10.0) for index in range(10)))
        for _ in range(20):
            chosen = self.chooser(1).choose(view, PlacementRequest(SHAPE), include_job_ids=frozenset({"job-n7"}))
            self.assertIn("job-n7", [node.job_id for node in chosen])
            self.assertEqual(len(chosen), 2)

    def test_spread_caps_each_worker_at_an_even_share(self):
        view = FleetView(tuple(heartbeat(f"n{index}") for index in range(4)))
        chooser = self.chooser(4)
        self.assertEqual([item.count for item in chooser.plan_group(view, PlacementRequest(SHAPE), 8, per_node_budget=8)],
                         [8])
        self.assertEqual([item.count for item in chooser.plan_group(
            view, PlacementRequest(SHAPE), 7, per_node_budget=8, policy="spread")], [2, 2, 2, 1])


class GroupIntentFences:
    store: RoutingStore

    def allocation(self, node, sandbox_id):
        return SandboxRouteAllocation(
            sandbox_id=sandbox_id, node_id=node, job_id="job-" + node, node_url=f"http://{node}:8090",
            resources=ResourceQuantity(memory_mb=256), spec={"id": sandbox_id}, node_epoch="e",
        )

    def reserve(self, node, *members, spec_hash=HASH):
        return self.store.reserve_create_intents(
            [self.allocation(node, member) for member in members], spec_hashes=[spec_hash] * len(members),
            operation_ids=[f"create-{uuid4().hex}" for _ in members], group_id="g")

    def test_group_intents_keep_the_incarnation_fences(self):
        group = self.store.ensure_sandbox_group(SandboxGroup("g", HASH, {"image": "i"}, 2))
        # The first record wins: a retry reads the stored request.
        self.assertEqual(self.store.ensure_sandbox_group(SandboxGroup("g", "b" * 64, {}, 3)), group)
        first, second = (route for route, _pending in self.reserve("a", "g-0000", "g-0001"))
        self.assertEqual((first.generation, second.job_id), (1, "job-a"))
        # Another spec on a member's id spares the rest of the transaction.
        self.assertEqual(self.reserve("b", "g-0000", "g-x", spec_hash="b" * 64)[0], None)
        self.assertIsNotNone(self.store.get_sandbox("g-x"))
        moved, = self.store.retarget_create_intents([(second, self.allocation("b", "g-0001"), "create-m")],
                                                    group_id="g")
        self.assertEqual((moved.generation, moved.job_id), (2, "job-b"))
        confirmed = self.store.confirm_creates(
            [replace(first, state="running"), replace(second, state="running")], group_id="g")
        self.assertEqual((confirmed[0].state, confirmed[1]), ("running", None))
        self.assertEqual(self.store.sandbox_group("g").placed, frozenset({"g-0000"}))
        self.assertEqual(self.store.delete_sandbox_group("g").state, "deleted")
        with self.assertRaises(SandboxGroupDeletedError):
            self.reserve("a", "g-0002")
        with self.assertRaises(SandboxGroupDeletedError):
            self.store.retarget_create_intents([(moved, self.allocation("a", "g-0001"), "x")], group_id="g")


class SqliteGroupFenceTests(GroupIntentFences, unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.store = RoutingStore(Path(self.temp.name) / "routes.sqlite")

    def tearDown(self):
        self.temp.cleanup()


@unittest.skipUnless(DSN, "requires real PostgreSQL (UCLOUD_TEST_POSTGRES_DSN)")
class PostgresGroupFenceTests(GroupIntentFences, unittest.TestCase):
    def setUp(self):
        from ucloud_sandboxes.shared_control.routing_repository import PostgresRoutingStore

        self.temp = TemporaryDirectory()
        self.schema = "ucloud_routing_group_" + uuid4().hex
        self.store = PostgresRoutingStore(Path(self.temp.name) / "routes", dsn=DSN, schema=self.schema,
                                          max_connections=4)
        self.store.migrate()
        self.store.migrate()  # The additive statements are idempotent.

    def tearDown(self):
        import psycopg
        from psycopg import sql

        self.store.close()
        with psycopg.connect(DSN, autocommit=True) as conn:
            conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(self.schema)))
        self.temp.cleanup()

    def test_a_group_command_claim_authorizes_member_intents(self):
        self.store.ensure_sandbox_group(SandboxGroup("g", HASH, {}, 1))
        command, claim, body = uuid4(), uuid4(), b'{"group_id":"g"}'
        with self.store.pool.connection() as conn:
            conn.execute(
                """INSERT INTO gateway_commands(command_id, command_key, kind, sandbox_id, path, headers, body,
                state, deadline, claim_token, claim_until) VALUES (%s, %s, 'group', 'g', '/v1/sandboxes:batch',
                '{}', %s, 'running', now() + interval '1 minute', %s, now() + interval '1 minute')""",
                (command, "c" * 64, body, claim))
        with self.store.command_execution(str(command), str(claim), "/v1/sandboxes:batch", body):
            route = self.reserve("a", "g-0000")[0][0]
            self.assertIsNotNone(self.store.delete_sandbox_if_current(
                "g-0000", generation=route.generation, create_operation_id=route.create_operation_id))
        with self.store.pool.connection() as conn:
            self.assertIsNone(conn.execute("SELECT generation FROM gateway_commands").fetchone()["generation"])


if __name__ == "__main__":
    unittest.main()
