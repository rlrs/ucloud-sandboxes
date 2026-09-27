from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Lock
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

from ucloud_sandboxes import cli, control_plane
from ucloud_sandboxes.autoscaler_state import AutoscalerStateStore
from ucloud_sandboxes.capabilities import (
    RUNTIME_COMPATIBILITY_CAPABILITY_PREFIX,
    RUNTIME_CPU_CAPABILITY_PREFIX,
)
from ucloud_sandboxes.config import DeploymentConfig
from ucloud_sandboxes.control_state import SOFT_DRAIN, ControlStateStore
from ucloud_sandboxes.deployment import package_version
from ucloud_sandboxes.models import (
    NodeHeartbeat,
    NodeRuntimeMetrics,
    ResourceQuantity,
    SandboxPlacementRequest,
    ScaleDecision,
    ScalePolicy,
    is_soft_drained,
    utc_now,
)
from ucloud_sandboxes.policy import evaluate_scale, plan_soft_drain
from ucloud_sandboxes.routing import RoutingStore
from tests.test_cli import (
    autoscaler_args, owned_heartbeat, owned_node_job, reconcile, save_heartbeats,
    temporary_root, ucloud_config, write_jobs,
)
from tests.test_control_plane import _portable_snapshot, _sandbox_route, build_heartbeat
from tests.test_policy import demand, node


CAPACITY = ResourceQuantity(vcpu=32, memory_mb=98_304, disk_mb=1_000_000)


def worker(job_id, *, active=3, used_memory_mb=8_000, drained=False, idle_since=None):
    now = utc_now()
    result = node(
        job_id,
        active=active,
        total_resources=CAPACITY,
        idle_since=idle_since,
        runtime_metrics=NodeRuntimeMetrics(
            collected_at=now,
            memory_total_mb=CAPACITY.memory_mb,
            memory_available_mb=CAPACITY.memory_mb - used_memory_mb,
        ),
    )
    return drained_node(result) if drained else result


def drained_node(value, since="2026-09-27T12:00:00+00:00"):
    labels = {**value.heartbeat.labels, SOFT_DRAIN: since}
    return replace(value, heartbeat=replace(value.heartbeat, labels=labels))


def portable_route(sandbox_id, **values):
    snapshot = _portable_snapshot(sandbox_id)
    fields = {
        "sandbox_id": sandbox_id,
        "node_id": "node-300",
        "job_id": "300",
        "node_url": "http://node-300:8090",
        "resources": snapshot.manifest.spec.requested_resources(),
        "spec": snapshot.manifest.spec.to_dict(),
        "state": "parked",
        "storage_schema": "storage-native-v1",
        "snapshot_manifest_digest": snapshot.publication.manifest_digest,
        "snapshot_repository": snapshot.publication.repository,
        "snapshot_tag": snapshot.publication.tag,
        "storage_snapshot": snapshot.to_dict(),
    }
    fields.update(values)
    return _sandbox_route(**fields)


class SoftDrainControlStateTests(unittest.TestCase):
    def heartbeat(self, **values):
        fields = {
            "node_id": "node-300",
            "job_id": "300",
            "deployment_id": "prod",
            "updated_at": utc_now(),
            "received_at": utc_now(),
            "active_sandboxes": 2,
            "node_epoch": "epoch-1",
            "activity_epoch": 1,
            "inventory_complete": True,
        }
        fields.update(values)
        return NodeHeartbeat(**fields)

    def test_label_survives_receipts_and_quarantine_recovery(self):
        with TemporaryDirectory() as raw:
            store = ControlStateStore(Path(raw) / "control.sqlite")
            store.upsert_heartbeat(self.heartbeat())
            self.assertFalse(store.clear_soft_drain("300"))
            self.assertFalse(store.set_soft_drain("missing", "t0"))
            self.assertTrue(store.set_soft_drain("300", "t0"))
            self.assertFalse(store.set_soft_drain("300", "t0"))

            # A worker cannot set or clear the controller-owned label.
            store.receive_heartbeat(
                self.heartbeat(activity_epoch=2, labels={SOFT_DRAIN: "forged"})
            )
            store.upsert_heartbeat(self.heartbeat(activity_epoch=3))
            stored = store.get_heartbeat("300")
            self.assertEqual(stored.labels[SOFT_DRAIN], "t0")
            # Soft drain does not close admission; only placement ranks it.
            self.assertTrue(stored.admission_open)
            self.assertFalse(stored.draining)
            self.assertTrue(store.load_heartbeats()["300"].admission_open)

            store.quarantine_node("300", "heartbeat_continuity_unverified")
            self.assertTrue(
                store.recover_quarantined_node(
                    self.heartbeat(activity_epoch=4, received_at=utc_now())
                )
            )
            recovered = store.get_heartbeat("300")
            self.assertTrue(recovered.admission_open)
            self.assertEqual(recovered.labels, {SOFT_DRAIN: "t0"})

            self.assertTrue(store.clear_soft_drain("300"))
            self.assertFalse(is_soft_drained(store.get_heartbeat("300")))


class SoftDrainSelectionTests(unittest.TestCase):
    def plan(self, nodes, **values):
        values.setdefault("required_resources", ResourceQuantity())
        return plan_soft_drain(nodes, values.pop("policy", ScalePolicy()), utc_now(), **values)

    def test_selects_fewest_sandboxes_then_newest_surplus_worker(self):
        nodes = [worker("100", active=10), worker("200"), worker("300")]
        decision = evaluate_scale(nodes, demand(), ScalePolicy(), now=utc_now())
        self.assertEqual(decision.soft_drain_job_id, "300")
        self.assertTrue(decision.soft_drain_selected)
        self.assertEqual(decision.stops, ())
        self.assertIn("soft-drain: selected 300", decision.reasons[-1])
        self.assertEqual(
            self.plan([worker("100"), worker("200", active=1), worker("300")]).job_id,
            "200",
        )

    def test_only_drains_a_worker_whose_parks_can_move_to_a_cpu_twin(self):
        def with_cpu(value, cpu):
            heartbeat = value.heartbeat
            return replace(value, heartbeat=replace(heartbeat, capabilities=(
                *heartbeat.capabilities, RUNTIME_CPU_CAPABILITY_PREFIX + cpu * 64,
            )))
        now = utc_now()
        mismatched = [with_cpu(worker("100", active=5), "a"), with_cpu(worker("200", active=2), "b")]
        plan = plan_soft_drain(mismatched, ScalePolicy(max_nodes=3), now,
                               required_resources=ResourceQuantity(vcpu=1, memory_mb=1024, disk_mb=1024))
        self.assertEqual(plan.job_id, "")
        twins = [*mismatched, with_cpu(worker("300", active=4), "b")]
        plan = plan_soft_drain(twins, ScalePolicy(max_nodes=3), now,
                               required_resources=ResourceQuantity(vcpu=1, memory_mb=1024, disk_mb=1024))
        self.assertEqual(plan.job_id, "200")
        # A selection that lost its twin is released.
        lonely = [mismatched[0], drained_node(mismatched[1])]
        plan = plan_soft_drain(lonely, ScalePolicy(max_nodes=3), now,
                               required_resources=ResourceQuantity(vcpu=1, memory_mb=1024, disk_mb=1024))
        self.assertEqual((plan.job_id, plan.clear_job_ids), ("", ("200",)))

    def test_requires_two_busy_workers_surplus_and_no_pending_demand(self):
        idle = worker("300", active=0, idle_since=utc_now())
        for nodes, pending in (
            ([worker("100")], 0),
            ([worker("100"), idle], 0),
            ([worker("100", used_memory_mb=60_000), worker("200", used_memory_mb=60_000)], 0),
            ([worker("100"), worker("200")], 1),
        ):
            with self.subTest(nodes=[item.job_id for item in nodes], pending=pending):
                decision = evaluate_scale(
                    nodes,
                    demand(
                        pending_count=pending,
                        placement_requests=(
                            SandboxPlacementRequest(ResourceQuantity(1, 1024, 2048)),
                        ) if pending else (),
                    ),
                    ScalePolicy(),
                    now=utc_now(),
                )
                self.assertEqual(decision.soft_drain_job_id, "")
                self.assertEqual(decision.creates, 0)
        self.assertEqual(
            self.plan([worker("100"), worker("200")], policy=ScalePolicy(min_nodes=2)).job_id,
            "",
        )
        self.assertEqual(
            self.plan(
                [worker("100"), worker("200")],
                policy=ScalePolicy(drain_on_park_enabled=False),
            ).job_id,
            "",
        )

    def test_keeps_one_selection_and_clears_it_when_surplus_disappears(self):
        kept = evaluate_scale(
            [worker("100"), worker("200", drained=True), worker("300", drained=True)],
            demand(),
            ScalePolicy(),
            now=utc_now(),
        )
        self.assertEqual(kept.soft_drain_job_id, "200")
        self.assertFalse(kept.soft_drain_selected)
        self.assertEqual(kept.soft_drain_clear_job_ids, ("300",))

        heavy = evaluate_scale(
            [
                worker("100", used_memory_mb=60_000),
                worker("200", used_memory_mb=60_000, drained=True),
            ],
            demand(),
            ScalePolicy(),
            now=utc_now(),
        )
        self.assertEqual(heavy.soft_drain_job_id, "")
        self.assertEqual(heavy.soft_drain_clear_job_ids, ("200",))
        self.assertEqual(heavy.creates, 0)

        disabled = self.plan(
            [worker("100"), worker("200", drained=True)],
            policy=ScalePolicy(drain_on_park_enabled=False),
        )
        self.assertEqual(disabled.clear_job_ids, ("200",))

    def test_reopens_drained_worker_for_demand_instead_of_creating(self):
        full = replace(
            worker("100"),
            heartbeat=replace(
                worker("100").heartbeat,
                used_resources=ResourceQuantity(disk_mb=CAPACITY.disk_mb - 1024),
            ),
        )
        request = SandboxPlacementRequest(ResourceQuantity(1, 1024, 10_240))
        decision = evaluate_scale(
            [full, worker("200", drained=True)],
            demand(pending_count=1, placement_requests=(request,)),
            ScalePolicy(),
            now=utc_now(),
        )
        self.assertEqual(decision.creates, 0)
        self.assertEqual(decision.soft_drain_job_id, "")
        self.assertEqual(decision.soft_drain_clear_job_ids, ("200",))
        # Pending demand the drained worker could serve also reopens it.
        self.assertEqual(
            self.plan(
                [worker("100"), worker("200", drained=True)],
                pending_count=1,
                placement_requests=(SandboxPlacementRequest(ResourceQuantity(1, 1024, 2048)),),
            ).clear_job_ids,
            ("200",),
        )

    def test_emptied_worker_stops_after_short_grace(self):
        now = utc_now()
        policy = ScalePolicy(scale_down_idle_seconds=300)
        idle = worker("300", active=0, idle_since=now - timedelta(seconds=90))
        plain = evaluate_scale([worker("100"), idle], demand(), policy, now=now)
        self.assertEqual(plain.stops, ())
        drained = evaluate_scale(
            [worker("100"), drained_node(idle)], demand(), policy, now=now
        )
        self.assertEqual(drained.stops, ("300",))
        recent = drained_node(
            worker("300", active=0, idle_since=now - timedelta(seconds=30))
        )
        self.assertEqual(
            evaluate_scale([worker("100"), recent], demand(), policy, now=now).stops, ()
        )


class SoftDrainMoverTests(unittest.TestCase):
    def apply(self, store, routes, *, post, execute=True, blocked=(), policy=None):
        decision = ScaleDecision(
            actions=(), ready_nodes=2, provisioning_nodes=0, total_nodes=2, reasons=(),
            soft_drain_job_id="300", soft_drain_selected=True,
            soft_drain_clear_job_ids=("200",),
        )
        nodes = [
            replace(worker(job_id), heartbeat=store.get_heartbeat(job_id))
            for job_id in ("200", "300")
        ]
        with patch.object(cli, "_post_gateway_sandbox_migration", side_effect=post):
            return cli._apply_soft_drain(
                decision,
                nodes,
                control_state=store,
                policy=policy or ScalePolicy(drain_on_park_moves_per_cycle=2),
                route_reservations={"300": tuple(routes)},
                blocked_job_ids=set(blocked),
                pending_wake_sandbox_ids={"waking-soon"},
                gateway_url="http://127.0.0.1:8080",
                bearer_token="gateway-secret",
                execute=execute,
                assert_provider_fence=lambda: None,
            )

    def store(self, root):
        store = ControlStateStore(root / "control.sqlite")
        for job_id in ("200", "300"):
            store.upsert_heartbeat(replace(worker(job_id).heartbeat, deployment_id="prod"))
        store.set_soft_drain("200", "t0")
        return store

    def test_moves_attached_parks_within_budget_even_before_publication_is_seen(self):
        calls, lock = [], Lock()

        def post(gateway_url, sandbox_id, **kwargs):
            with lock:
                calls.append((gateway_url, sandbox_id, kwargs))
            return {"migration": {"phase": "complete", "destination_job_id": "100"}}

        routes = [
            _sandbox_route(sandbox_id="running", node_id="node-300", job_id="300",
                           node_url="http://node-300:8090", state="running"),
            portable_route("unpublished", snapshot_tag=""),
            portable_route("detaching", worker_state="detaching"),
            portable_route("deleting", delete_operation_id="delete-1"),
            portable_route("waking-soon"),
            portable_route("first", generation=3),
            portable_route("second"),
            portable_route("third"),
        ]
        with TemporaryDirectory() as raw:
            store = self.store(Path(raw))
            result, moves = self.apply(store, routes, post=post)
            self.assertTrue(is_soft_drained(store.get_heartbeat("300")))
            self.assertFalse(is_soft_drained(store.get_heartbeat("200")))
        self.assertEqual(result["jobId"], "300")
        self.assertTrue(result["selected"])
        # The gateway publishes an unseen park before moving it.
        self.assertEqual(sorted(call[1] for call in calls), ["first", "unpublished"])
        for gateway_url, _sandbox_id, kwargs in calls:
            self.assertEqual(gateway_url, "http://127.0.0.1:8080")
            self.assertEqual(kwargs["bearer_token"], "gateway-secret")
            self.assertTrue(kwargs["soft_drain"])
        self.assertEqual(
            [move["migrationId"] for move in moves],
            ["drain-300-unpublished-1", "drain-300-first-3"],
        )
        self.assertTrue(all(move["requestSucceeded"] for move in moves))
        self.assertEqual(moves[1]["destinationJobId"], "100")

    def test_no_destination_skips_and_dry_run_or_blocked_worker_does_nothing(self):
        def unavailable(gateway_url, sandbox_id, **_kwargs):
            raise HTTPError(gateway_url, 503, "Service Unavailable", None, None)

        with TemporaryDirectory() as raw:
            store = self.store(Path(raw))
            _, moves = self.apply(store, [portable_route("one")], post=unavailable)
            self.assertEqual(len(moves), 1)
            self.assertTrue(moves[0]["skipped"])
            self.assertFalse(moves[0]["requestSucceeded"])

        def forbidden(*_args, **_kwargs):
            raise AssertionError("no move expected")

        with TemporaryDirectory() as raw:
            store = self.store(Path(raw))
            result, moves = self.apply(
                store, [portable_route("one")], post=forbidden, execute=False
            )
            self.assertEqual((result["applied"], moves), (False, []))
            self.assertFalse(is_soft_drained(store.get_heartbeat("300")))
            self.assertTrue(is_soft_drained(store.get_heartbeat("200")))
            result, moves = self.apply(
                store, [portable_route("one")], post=forbidden, blocked={"300"}
            )
            self.assertEqual((result["jobId"], moves), ("", []))
            self.assertFalse(is_soft_drained(store.get_heartbeat("300")))


class SoftDrainCycleTests(unittest.TestCase):
    def test_cycle_selects_surplus_worker_and_moves_its_parks(self):
        newer = owned_node_job(agent_version=True)
        newer = {**newer, "id": "owned-2", "specification": {
            **newer["specification"], "name": "ucloud-sandbox-node-owned-2"}}

        def heartbeat(job_id):
            return owned_heartbeat(
                job_id=job_id,
                node_id="node-" + job_id,
                node_url=f"http://node-{job_id}:8090",
                total_resources=CAPACITY,
                resources_known=True,
                runtime_metrics=NodeRuntimeMetrics(
                    collected_at=utc_now(),
                    memory_total_mb=CAPACITY.memory_mb,
                    memory_available_mb=CAPACITY.memory_mb - 8_000,
                ),
            )

        busy = tuple(
            _sandbox_route(sandbox_id=f"busy-{index}", node_id="node-owned",
                           job_id="owned", node_url="http://node-owned:8090",
                           state="running")
            for index in range(3)
        )
        parks = tuple(
            portable_route(f"park-{index}", node_id="node-owned-2", job_id="owned-2",
                           node_url="http://node-owned-2:8090")
            for index in range(2)
        )
        calls, lock = [], Lock()

        def post(_gateway_url, sandbox_id, **kwargs):
            with lock:
                calls.append((sandbox_id, kwargs["migration_id"]))
            return {"migration": {"phase": "complete", "destination_job_id": "owned"}}

        with temporary_root() as root:
            jobs_file = write_jobs(root, owned_node_job(agent_version=True), newer)
            heartbeat_file = root / "control-state.sqlite"
            save_heartbeats(
                heartbeat_file,
                {job_id: heartbeat(job_id) for job_id in ("owned", "owned-2")},
            )
            config = ucloud_config(
                project_id="project-1",
                deployment_id="prod-a",
                ucloud_session_file=str(root / "session.json"),
                data_root=str(root),
            )
            state = AutoscalerStateStore(root / "autoscaler-state.sqlite")
            with patch.object(cli, "_post_gateway_sandbox_migration", side_effect=post):
                result = reconcile(
                    config,
                    autoscaler_args(jobs_file, heartbeat_file),
                    state,
                    route_reservations={"owned": busy, "owned-2": parks},
                )
            stored = ControlStateStore(config.control_state_file()).load_heartbeats()

        self.assertEqual(result["softDrain"]["jobId"], "owned-2")
        self.assertTrue(result["softDrain"]["selected"])
        self.assertTrue(is_soft_drained(stored["owned-2"]))
        self.assertFalse(is_soft_drained(stored["owned"]))
        self.assertEqual(result["stopJobIds"], [])
        self.assertEqual(
            sorted(calls),
            [("park-0", "drain-owned-2-park-0-1"), ("park-1", "drain-owned-2-park-1-1")],
        )
        self.assertTrue(
            all(move["requestSucceeded"] for move in result["softDrainMoveResults"])
        )


class SoftDrainGatewayTests(unittest.TestCase):
    def heartbeat(self, job, *, drained=False, image=""):
        heartbeat = NodeHeartbeat(
            job_id=job,
            node_id="node-" + job,
            node_url="http://node-" + job + ":8090",
            deployment_id="test-deployment",
            updated_at=utc_now(),
            agent_version=package_version(),
            active_sandboxes=1,
            inventory_complete=True,
            resources_known=True,
            capabilities=(
                "sandbox",
                "disk-quota",
                "storage-native-v1",
                "sandbox-migrate-storage-native-v1",
                "hibernate-local-v2",
                RUNTIME_COMPATIBILITY_CAPABILITY_PREFIX
                + _portable_snapshot("parked").manifest.runtime.node_compatibility_sha256,
            ),
            total_resources=ResourceQuantity(vcpu=32, memory_mb=98304, disk_mb=100000),
            cached_images=(image,) if image else (),
            cached_images_known=True,
            runtime_metrics=NodeRuntimeMetrics(
                collected_at=utc_now(),
                cpu_count=32,
                cpu_percent=10,
                memory_total_mb=98304,
                memory_available_mb=90000,
                memory_psi_full_avg10=0,
                storage_hard_capacity_mb=100000,
                storage_max_concurrent_operations=8,
            ),
        )
        if drained:
            heartbeat = replace(heartbeat, labels={SOFT_DRAIN: "t0"})
        return heartbeat

    def handler(self, root, *destinations):
        routing = RoutingStore(root / "routes.sqlite")
        route = routing.upsert_sandbox(
            portable_route(
                "parked", node_id="node-300", job_id="300",
                node_url="http://node-300:8090",
            )
        )
        store = ControlStateStore(root / "control.sqlite")
        store.upsert_heartbeat(self.heartbeat("300"))
        for destination in destinations:
            store.upsert_heartbeat(destination)
            if is_soft_drained(destination):
                store.set_soft_drain(destination.job_id, "t0")

        class Handler(control_plane.ControlPlaneHandler):
            pass

        handler = object.__new__(Handler)
        handler.store = store
        handler.routing_store = routing
        handler.heartbeat_ttl_seconds = 120
        handler.wake_consolidation_policy = ScalePolicy()
        return handler, route

    def test_migration_destination_never_soft_drained_except_wake_fallback(self):
        with TemporaryDirectory() as raw:
            image = portable_route("parked").spec["image"]
            handler, route = self.handler(
                Path(raw),
                self.heartbeat("100", drained=True, image=image),
                self.heartbeat("150"),
            )
            chosen = handler._select_migration_destination(route, requested_node_id="")
            self.assertEqual(chosen.job_id, "150")
        with TemporaryDirectory() as raw:
            handler, route = self.handler(Path(raw), self.heartbeat("100", drained=True))
            self.assertIsNone(
                handler._select_migration_destination(route, requested_node_id="")
            )
            wake = handler._select_migration_destination(
                route, requested_node_id="", require_active_resources=True,
            )
            self.assertEqual(wake.job_id, "100")

    def test_soft_drain_move_without_destination_records_no_demand(self):
        with TemporaryDirectory() as raw:
            handler, route = self.handler(Path(raw), self.heartbeat("100", drained=True))
            outcomes = []
            handler._read_json_body = lambda: {"migration_id": "m-1", "soft_drain": True}
            handler._atomic_placement = lambda operation, **_kwargs: operation()
            handler._write_wake_unavailable = outcomes.append
            handler._migrate_sandbox_on_node(route.sandbox_id)
            self.assertEqual(outcomes[0].error_code, "migration_destination_unavailable")
            self.assertEqual(handler.routing_store.pending_sandboxes(), [])
            self.assertEqual(handler.routing_store.sandbox_migrations(), [])

    def test_soft_drain_move_publishes_a_park_the_gateway_has_not_seen(self):
        with TemporaryDirectory() as raw:
            handler, _route = self.handler(Path(raw), self.heartbeat("100", drained=True))
            unseen = handler.routing_store.upsert_sandbox(portable_route(
                "unseen", node_id="node-300", job_id="300",
                node_url="http://node-300:8090", snapshot_tag="",
            ))
            outcomes, asked = [], []

            def publish(route):
                asked.append(route.sandbox_id)
                return None, "sandbox woke before publication"

            handler._publish_route_for_detach = publish
            handler._read_json_body = lambda: {"migration_id": "m-2", "soft_drain": True}
            handler._atomic_placement = lambda operation, **_kwargs: operation()
            handler._write_wake_unavailable = outcomes.append
            handler._migrate_sandbox_on_node(unseen.sandbox_id)
            self.assertEqual(asked, ["unseen"])
            self.assertEqual(outcomes[0].error_code, "migration_source_not_parked")
            self.assertEqual(handler.routing_store.sandbox_migrations(), [])

    def test_destinations_with_other_cpu_features_are_never_chosen(self):
        snapshot_cpu = _portable_snapshot("parked").manifest.runtime.cpu_features_sha256
        with TemporaryDirectory() as raw:
            other = self.heartbeat("100")
            other = replace(other, capabilities=(
                *other.capabilities, RUNTIME_CPU_CAPABILITY_PREFIX + "f" * 64,
            ))
            same = self.heartbeat("150")
            same = replace(same, capabilities=(
                *same.capabilities, RUNTIME_CPU_CAPABILITY_PREFIX + snapshot_cpu,
            ))
            handler, route = self.handler(Path(raw), other, same)
            self.assertEqual(
                handler._select_migration_destination(route, requested_node_id="").job_id, "150",
            )
        with TemporaryDirectory() as raw:
            handler, route = self.handler(Path(raw), other)
            self.assertIsNone(handler._select_migration_destination(route, requested_node_id=""))
        with TemporaryDirectory() as raw:
            # A worker that predates the capability stays eligible.
            handler, route = self.handler(Path(raw), self.heartbeat("120"))
            self.assertEqual(
                handler._select_migration_destination(route, requested_node_id="").job_id, "120",
            )

    def test_failed_drain_move_is_rolled_back_so_the_source_can_wake(self):
        with TemporaryDirectory() as raw:
            handler, route = self.handler(Path(raw), self.heartbeat("150"))
            written, aborted = [], []
            handler._read_json_body = lambda: {"migration_id": "m-3", "soft_drain": True}
            handler._atomic_placement = lambda operation, **_kwargs: operation()
            handler._prepare_and_advance_sandbox_migration = lambda migration, **_kwargs: replace(
                migration, phase="prepared",
                error="storage-native snapshot does not match the required runtime",
            )

            def abort(migration):
                aborted.append(migration.migration_id)
                return replace(migration, phase="complete", error=""), ""

            handler._abort_sandbox_migration = abort
            handler._write_json = lambda payload, status=200: written.append((status, payload))
            handler._migrate_sandbox_on_node(route.sandbox_id)
            self.assertEqual(aborted, ["m-3"])
            self.assertEqual(written[0][0], 503)
            self.assertTrue(written[0][1]["aborted"])

    def test_create_placement_prefers_undrained_but_falls_back(self):
        drained = replace(
            build_heartbeat(node_id="a", job_id="job-a", node_url="http://a",
                            total_resources=ResourceQuantity(16, 16384, 1_000_000)),
            labels={SOFT_DRAIN: "t0"},
        )
        other = build_heartbeat(node_id="b", job_id="job-b", node_url="http://b",
                                total_resources=ResourceQuantity(16, 16384, 1_000_000))

        def selector(heartbeats):
            handler = object.__new__(control_plane.ControlPlaneHandler)
            handler.telemetry = None
            handler.registry_layer_cache = None
            handler.create_target_concurrency_per_node = 4
            handler.inflight_create_placements = control_plane.InflightCreatePlacements()
            handler._placement_routes = lambda: []
            handler._ready_sandbox_heartbeats = lambda **_kwargs: list(heartbeats)
            handler._nodes_with_image = lambda *_args, **_kwargs: {"a"}
            return handler

        requested = ResourceQuantity(1, 1024, 4096)
        with patch.object(control_plane, "_node_can_fit_available", return_value=True):
            self.assertEqual(selector([drained, other])._select_node(requested).node_id, "b")
            self.assertEqual(selector([drained])._select_node(requested).node_id, "a")


class SoftDrainConfigTests(unittest.TestCase):
    def test_policy_fields_round_trip_and_default_for_older_configs(self):
        config = DeploymentConfig.default()
        config = replace(
            config,
            policy=replace(
                config.policy, drain_on_park_enabled=False, drain_on_park_moves_per_cycle=7
            ),
        )
        self.assertEqual(DeploymentConfig.from_dict(config.to_dict()), config)
        raw = DeploymentConfig.default().to_dict()
        raw["policy"].pop("drain_on_park_enabled")
        raw["policy"].pop("drain_on_park_moves_per_cycle")
        policy = DeploymentConfig.from_dict(raw).policy
        self.assertTrue(policy.drain_on_park_enabled)
        self.assertEqual(policy.drain_on_park_moves_per_cycle, 4)
        raw["policy"]["drain_on_park_moves_per_cycle"] = -1
        with self.assertRaises(ValueError):
            DeploymentConfig.from_dict(raw)


if __name__ == "__main__":
    unittest.main()
