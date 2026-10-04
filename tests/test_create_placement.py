"""C4.3 phase 1: power-of-k creates behind ``gateway_create_placement``.

Tier: contract. Two gateways over one routing and heartbeat state place
creates in both modes. Power-of-k moves a definitely rejected create to the
next candidate as a fresh incarnation, keeps an ambiguous one where it is,
and queues demand once every candidate rejects. A worker bounds its
admission wait by the gateway's header. The route intent keeps today's
generation, operation and spec-hash fences on SQLite and PostgreSQL.
"""
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import json
import os
from pathlib import Path
import random
from tempfile import TemporaryDirectory
from threading import Barrier
import time
from types import SimpleNamespace
import unittest
from uuid import uuid4

from tests import test_direct_provisioner as direct_fixtures
from tests.harness import LocalFleet
from tests.harness.assembly import fixed_metrics
from tests.test_placement_choice import heartbeat
from ucloud_sandboxes.admission import ADMISSION_WAIT_HEADER, CREATE_ADMISSION_WAIT, parse_admission_wait
from ucloud_sandboxes.capabilities import ENVIRONMENT_RAFS_CAPABILITY, ENVIRONMENT_ROOT_CAPABILITY
from ucloud_sandboxes.config import DeploymentConfig
from ucloud_sandboxes.direct_service import DirectSandboxService
from ucloud_sandboxes.gateway.create import CreatePlacement
from ucloud_sandboxes.gateway.node_rpc import ProxiedResponse
from ucloud_sandboxes.gateway.registry_refs import RegistryReferences
from ucloud_sandboxes.models import ResourceQuantity
from ucloud_sandboxes.routing import RoutingStore, SandboxRouteAllocation, SandboxRouteConflictError
from ucloud_sandboxes.sandbox import SandboxSpec, SandboxStartupBusyError
from ucloud_sandboxes.telemetry import Telemetry

TEST_TIER = "contract"
DSN = os.environ.get("UCLOUD_TEST_POSTGRES_DSN")
SPEC = {"image": "harness/base:1", "cpus": 1, "memory_mb": 256, "disk_mb": 1024, "network": "none"}
ROOT = "sha256:" + "7" * 64
HASH = "a" * 64


class TwoGatewayTests(unittest.TestCase):
    def test_both_modes_place_creates_from_either_gateway(self):
        for mode in ("ranked", "power_of_k"):
            for postgres in (False, True):
                with self.subTest(mode=mode, postgres=postgres), LocalFleet(
                        nodes=2, gateways=2, create_placement=mode, postgres=postgres) as fleet:
                    names = [f"{mode}-{index}" for index in range(6)]
                    for index, name in enumerate(names):
                        response = fleet.request("POST", "/v1/sandboxes", payload={"id": name, **SPEC},
                                                 token="sandbox", gateway=index % 2)
                        self.assertEqual(response.status, 201, response.body)
                    for name in names:
                        route = fleet.route(name)
                        self.assertEqual((route.generation, route.state), (1, "running"))
                        self.assertIsNotNone(fleet.node_for(name).registration(name))
                    retried = fleet.request("POST", "/v1/sandboxes", payload={"id": names[0], **SPEC},
                                            token="sandbox", gateway=1)
                    self.assertEqual((retried.status, retried.json()["recovered"]), (200, True))
                    self.assertEqual(fleet.exec(names[1], ["cat", "/opt/greeting"]).stdout,
                                     "hello from the image\n")
                    self.assertEqual(fleet.delete(names[2]).status, 200)
                    self.assertIsNone(fleet.route(names[2]))

    def test_a_stale_view_sends_a_deferred_create_to_the_next_candidate(self):
        with LocalFleet(nodes=2, create_placement="power_of_k") as fleet:
            first, second = fleet.nodes
            # The second ranks behind; CPU alone defers nothing at the worker.
            second.sample_metrics = lambda: replace(fixed_metrics(), cpu_percent=90.0)
            fleet.heartbeat()
            # The first runs out of memory headroom after its last heartbeat.
            first.sample_metrics = lambda: replace(fixed_metrics(), memory_available_mb=100)
            started = time.monotonic()
            created = fleet.create("moved")
            # It waited the short admission wait there, not the full 30 s.
            self.assertLess(time.monotonic() - started, 10)
            route = fleet.route("moved")
            self.assertEqual((route.job_id, route.generation), (second.job_id, 2))
            self.assertEqual((created["generation"], created["operation_id"]),
                             (2, route.create_operation_id))
            self.assertIn(("POST", "/v1/sandboxes"), first.requests)
            self.assertIsNone(first.registration("moved"))

    def test_every_candidate_rejecting_queues_retryable_demand(self):
        with LocalFleet(create_placement="power_of_k") as fleet:
            node = fleet.nodes[0]
            fleet.create("seed")  # Its image is cached: the create itself is refused.
            fleet.heartbeat()
            self.assertEqual(node.drain("solo").status, 200)  # The gateway does not know.
            refused = fleet.request("POST", "/v1/sandboxes", payload={"id": "refused", **SPEC}, token="sandbox")
            self.assertEqual(refused.status, 503, refused.body)
            self.assertEqual((refused.json()["error_code"], refused.json()["retryable"]),
                             ("node_admission_closed", True))
            self.assertEqual(refused.headers["X-UCloud-Sandbox-Retryable"], "true")
            self.assertIsNone(fleet.route("refused"))
            pending = fleet.gateway.RequestHandlerClass.routing_store.load().pending
            self.assertEqual((pending["refused"].failure_reason, pending["refused"].generation),
                             ("node_admission_closed", 1))

    def test_a_dispatched_root_needs_a_capable_worker(self):
        with LocalFleet(create_placement="power_of_k") as fleet:
            handler = fleet.gateway.RequestHandlerClass
            mapped = {"harness/base:1": ROOT}
            handler.dispatch_environment_roots = True
            handler.services.registry_refs.dependency_resolver = SimpleNamespace(root=mapped.get)
            pinned = fleet.request("POST", "/v1/sandboxes", payload={"id": "pinned", **SPEC}, token="sandbox")
            self.assertEqual((pinned.status, pinned.json()["error_code"]), (503, "no_ready_node"), pinned.body)
            self.assertIsNone(fleet.route("pinned"))
            mapped.clear()
            self.assertEqual(fleet.create("plain")["state"], "running")
            self.assertNotIn("environment_root", fleet.route("plain").spec)


class CreateLoopTests(unittest.TestCase):
    """The outcome handling against scripted workers and a real route store."""

    def setUp(self):
        self.temp = TemporaryDirectory()
        self.store = RoutingStore(Path(self.temp.name) / "routes.sqlite")
        self.nodes = [heartbeat("a", cpu=10.0), heartbeat("b", cpu=50.0)]
        self.placement = CreatePlacement(
            self.store, SimpleNamespace(load_heartbeats=lambda shared=False: {
                node.job_id: node for node in self.nodes}),
            heartbeat_ttl_seconds=120, metrics_store=None, telemetry=Telemetry.disabled("test"),
            registry_refs=RegistryReferences(registry_url=None, registry_worker_url=None, usage_store=None,
                                             deployment_id="test", dependency_resolver=None),
            target_creates_per_node=8, api_processes=1, rng=random.Random(0),
        )

    def tearDown(self):
        self.temp.cleanup()

    def run_create(self, answers, **spec):
        calls, written = [], []

        def proxy(node_url, path, *, method, body=None, timeout_seconds=0, extra_headers=None):
            calls.append((node_url.split("//")[1].split(":")[0], extra_headers))
            answer = answers[calls[-1][0]]
            return answer(body) if callable(answer) else answer

        ex = SimpleNamespace(
            _ensure_image_for_create=lambda *_args: None, _proxy_request=proxy,
            _send_existing_sandbox_response=lambda *_args, **_kwargs: False,
            _send_proxied_response=lambda response, **_kwargs: written.append(response.status),
            _write_json=lambda payload, *, status=200, headers=None: written.append((status, payload)),
            _write_no_ready_node=lambda _demand, code: written.append((503, code)),
            _write_create_in_progress_response=lambda _id: written.append("in progress"),
            _retry_sandbox_create_on_assigned_node=lambda *_args: written.append("retry"),
            _confirm_sandbox_observation=lambda route, confirm: confirm(route),
        )
        root = SimpleNamespace(set_attribute=lambda *_args: None, status="ok")
        self.placement.create(ex, SandboxSpec.from_dict({"id": "s", **SPEC, **spec}), root)
        return calls, written

    def test_an_accepted_create_is_confirmed_and_stays_charged(self):
        calls, written = self.run_create({"a": accepted})
        self.assertEqual((calls, written), ([("a", {ADMISSION_WAIT_HEADER: "1"})], [201]))
        route = self.store.get_sandbox("s")
        self.assertEqual((route.job_id, route.generation, route.state), ("job-a", 1, "running"))
        # Charged until a heartbeat reports the incarnation.
        self.assertEqual(self.placement.overlay.reservation_count(), 1)

    def test_a_definite_reject_moves_the_intent_to_the_next_candidate(self):
        calls, written = self.run_create({"a": refused("node_active_admission_deferred"), "b": accepted})
        # The last candidate waits the worker's full admission time.
        self.assertEqual((calls, written), ([("a", {ADMISSION_WAIT_HEADER: "1"}), ("b", None)], [201]))
        route = self.store.get_sandbox("s")
        self.assertEqual((route.job_id, route.generation, route.state), ("job-b", 2, "running"))
        self.assertEqual(self.placement.overlay.reservation_count(), 1)

    def test_an_ambiguous_answer_keeps_the_route_on_its_worker(self):
        calls, written = self.run_create({"a": ProxiedResponse(504, {}, b"{}")})
        self.assertEqual((calls, written), ([("a", {ADMISSION_WAIT_HEADER: "1"})], [504]))
        route = self.store.get_sandbox("s")
        self.assertEqual((route.job_id, route.generation, route.state), ("job-a", 1, "creating"))
        self.assertEqual(self.placement.overlay.reservation_count(), 1)

    def test_every_rejection_queues_demand_and_releases_the_charge(self):
        calls, written = self.run_create({node: refused("node_admission_closed") for node in "ab"})
        self.assertEqual(([node for node, _ in calls], written), (["a", "b"], [(503, "node_admission_closed")]))
        self.assertIsNone(self.store.get_sandbox("s"))
        pending = self.store.load().pending["s"]
        self.assertEqual((pending.failure_reason, pending.generation), ("node_admission_closed", 2))
        self.assertEqual(self.placement.overlay.reservation_count(), 0)

    def test_a_dispatched_root_samples_only_capable_workers(self):
        self.nodes[1] = replace(self.nodes[1], capabilities=(
            *self.nodes[1].capabilities, ENVIRONMENT_ROOT_CAPABILITY, ENVIRONMENT_RAFS_CAPABILITY))
        calls, written = self.run_create({"b": accepted}, environment_root=ROOT)
        self.assertEqual((calls, written), ([("b", None)], [201]))
        self.nodes.pop()
        self.store.delete_sandbox("s")
        self.assertEqual(self.run_create({}, environment_root=ROOT), ([], [(503, "no_ready_node")]))


def accepted(body):
    spec = json.loads(body)
    operation = spec.pop("_ucloud_operation")
    return ProxiedResponse(201, {}, json.dumps({"sandbox": {
        "spec": spec, "generation": operation["generation"], "operation_id": operation["operation_id"],
        "spec_hash": operation["spec_hash"], "state": "running", "node_epoch": "epoch-1", "activity_epoch": 1,
    }}).encode())


def refused(code):
    return ProxiedResponse(503, {}, json.dumps({"error": "no", "error_code": code, "retryable": True}).encode())


class AdmissionWaitTests(unittest.TestCase):
    def test_the_header_parses_or_is_ignored(self):
        for raw, parsed in (("1", 1.0), (" 0.5 ", 0.5), ("0", 0.0), (None, None), ("", None),
                            ("-1", None), ("nan", None), ("inf", None), ("soon", None)):
            self.assertEqual(parse_admission_wait(raw), parsed, raw)

    def test_it_bounds_the_startup_slot_wait_to_the_nodes_own(self):
        with TemporaryDirectory() as directory:
            provisioner, *_ = direct_fixtures.DirectProvisionerTests().make(Path(directory).resolve())
            service = DirectSandboxService(provisioner, max_concurrent_startups=1)
            self.assertTrue(service._startup_slots.acquire(blocking=False))
            token = CREATE_ADMISSION_WAIT.set(0.05)
            try:
                started = time.monotonic()
                with self.assertRaises(SandboxStartupBusyError):
                    with service.startup_admission():
                        pass
                self.assertLess(time.monotonic() - started, 5)
                service.admission_wait_seconds = 0.01  # The header never extends it.
                CREATE_ADMISSION_WAIT.set(60.0)
                with self.assertRaises(SandboxStartupBusyError):
                    with service.startup_admission():
                        pass
            finally:
                CREATE_ADMISSION_WAIT.reset(token)
                service._startup_slots.release()

    def test_the_switch_defaults_to_ranked_and_round_trips(self):
        raw = DeploymentConfig.default(scope_id="project").to_dict()
        self.assertNotIn("gateway_create_placement", raw)
        self.assertEqual(DeploymentConfig.from_dict(raw).gateway_create_placement, "ranked")
        raw["gateway_create_placement"] = "power_of_k"
        self.assertEqual(DeploymentConfig.from_dict(raw).to_dict()["gateway_create_placement"], "power_of_k")
        raw["gateway_create_placement"] = "random"
        with self.assertRaises(ValueError):
            DeploymentConfig.from_dict(raw)


class IntentFences:
    store: RoutingStore

    def allocation(self, node, sandbox_id="s"):
        return SandboxRouteAllocation(
            sandbox_id=sandbox_id, node_id=node, job_id="job-" + node, node_url=f"http://{node}:8090",
            resources=ResourceQuantity(memory_mb=256), spec={"id": sandbox_id}, node_epoch="e",
        )

    def reserve(self, node, operation, sandbox_id="s", spec_hash=HASH):
        return self.store.reserve_create_intent(
            self.allocation(node, sandbox_id), spec_hash=spec_hash, create_operation_id=operation)[0]

    def test_reserve_retarget_and_confirm_keep_the_incarnation_fences(self):
        route = self.reserve("a", "create-1")
        self.assertEqual((route.generation, route.job_id, route.state), (1, "job-a", "creating"))
        self.assertEqual(self.reserve("b", "create-2").create_operation_id, "create-1")
        with self.assertRaises(SandboxRouteConflictError):
            self.reserve("b", "create-2", spec_hash="b" * 64)
        moved = self.store.retarget_create_intent(route, self.allocation("b"), create_operation_id="create-3")
        self.assertEqual((moved.generation, moved.job_id, moved.create_operation_id), (2, "job-b", "create-3"))
        # The old incarnation can neither move again nor confirm late.
        self.assertIsNone(self.store.retarget_create_intent(route, self.allocation("c"), create_operation_id="x"))
        self.assertIsNone(self.store.confirm_create(replace(route, state="running")))
        confirmed = self.store.confirm_create(replace(moved, state="running", activity_epoch=3))
        self.assertEqual((confirmed.state, confirmed.job_id), ("running", "job-b"))
        # A confirmed sandbox is no longer a create to move.
        self.assertIsNone(self.store.retarget_create_intent(confirmed, self.allocation("a"), create_operation_id="x"))
        deleting = self.store.prepare_sandbox_delete("s")
        self.assertIsNone(self.store.confirm_create(replace(deleting, state="running", activity_epoch=4)))
        self.store.delete_sandbox_if_current("s", generation=deleting.generation)
        self.assertEqual(self.reserve("a", "create-6").generation, 3)


class SqliteIntentFenceTests(IntentFences, unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.store = RoutingStore(Path(self.temp.name) / "routes.sqlite")

    def tearDown(self):
        self.temp.cleanup()


@unittest.skipUnless(DSN, "requires real PostgreSQL (UCLOUD_TEST_POSTGRES_DSN)")
class PostgresIntentFenceTests(IntentFences, unittest.TestCase):
    def setUp(self):
        from ucloud_sandboxes.shared_control.routing_repository import PostgresRoutingStore

        self.temp = TemporaryDirectory()
        self.schema = "ucloud_routing_intent_" + uuid4().hex
        self.store = PostgresRoutingStore(Path(self.temp.name) / "routes", dsn=DSN, schema=self.schema,
                                          max_connections=16)
        self.store.migrate()

    def tearDown(self):
        import psycopg
        from psycopg import sql

        self.store.close()
        with psycopg.connect(DSN, autocommit=True) as conn:
            conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(self.schema)))
        self.temp.cleanup()

    def race(self, count, attempt):
        barrier = Barrier(count)

        def run(index):
            barrier.wait(timeout=5)
            return attempt(index)

        with ThreadPoolExecutor(count) as pool:
            return list(pool.map(run, range(count)))

    def test_concurrent_intents_for_one_sandbox_share_one_route(self):
        routes = self.race(8, lambda index: self.reserve("abcdefgh"[index], f"create-{index}"))
        self.assertEqual(len({(route.generation, route.create_operation_id) for route in routes}), 1)
        self.assertEqual(self.store.get_sandbox("s"), routes[0])

    def test_one_workers_creates_wait_for_row_locks_instead_of_aborting(self):
        routes = self.race(12, lambda index: self.reserve("a", f"create-{index}", sandbox_id=f"s{index}"))
        self.race(12, lambda index: self.store.confirm_create(replace(routes[index], state="running")))
        self.assertEqual({route.state for route in self.store.sandbox_routes_readonly()}, {"running"})
        self.assertEqual(self.store.serialization_retries, 0)


if __name__ == "__main__":
    unittest.main()
