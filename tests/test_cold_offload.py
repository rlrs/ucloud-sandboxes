from dataclasses import replace
from datetime import timedelta
from types import SimpleNamespace
import unittest

from ucloud_sandboxes.capabilities import STORAGE_NATIVE_CAPABILITY, STORAGE_NATIVE_DETACH_CAPABILITY
from ucloud_sandboxes.cold_offload import plan_cold_offload
from ucloud_sandboxes.models import NodeHeartbeat, NodeRuntimeMetrics, ResourceQuantity, utc_now
from ucloud_sandboxes.routing import ProgramRequestState, SandboxRoute
from tests.test_control_plane import _portable_snapshot


class ColdOffloadTests(unittest.TestCase):
    def setUp(self):
        self.now = utc_now()
        self.heartbeat = NodeHeartbeat(
            node_id='node', job_id='job', node_url='http://node', updated_at=self.now, active_sandboxes=1,
            runtime_metrics=NodeRuntimeMetrics(collected_at=self.now,
                storage_hard_capacity_mb=100_000, storage_hard_reserved_mb=95_000),
            capabilities=(STORAGE_NATIVE_CAPABILITY, STORAGE_NATIVE_DETACH_CAPABILITY),
        )
        self.node = SimpleNamespace(is_schedulable=True, job_id='job', heartbeat=self.heartbeat)

    def route(self, name, *, claim=10_000, restore_mb=100, age=600, **kwargs):
        snapshot = _portable_snapshot(name)
        publication = replace(snapshot.publication,
            layers=(replace(snapshot.publication.layers[0], size=restore_mb*1024**2),))
        snapshot = replace(snapshot, publication=publication)
        return SandboxRoute(
            sandbox_id=name, node_id='node', job_id='job', node_url='http://node',
            resources=ResourceQuantity(disk_mb=claim), spec=snapshot.manifest.spec.to_dict(),
            state='parked', generation=1, create_operation_id=snapshot.manifest.create_operation_id,
            spec_hash=snapshot.manifest.spec_sha256, storage_schema=STORAGE_NATIVE_CAPABILITY,
            snapshot_manifest_digest=publication.manifest_digest,
            snapshot_repository=publication.repository, snapshot_tag=publication.tag,
            storage_snapshot=snapshot.to_dict(), updated_at=(self.now-timedelta(seconds=age)).isoformat(),
            **kwargs,
        )

    def plan(self, routes, requests=(), **kwargs):
        return plan_cold_offload([self.node], routes, requests, limit=8, now=self.now, **kwargs)

    def test_rank_released_claim_against_restore_cost_and_stop_at_headroom_target(self):
        expensive = self.route('expensive', restore_mb=1000)
        cheap = self.route('cheap', restore_mb=10)
        next_best = self.route('next', restore_mb=100)
        plans = self.plan([expensive, next_best, cheap])
        self.assertEqual([item.route.sandbox_id for item in plans], ['cheap', 'next'])
        self.assertEqual(sum(item.route.resources.disk_mb for item in plans), 20_000)
        self.assertEqual(plans[0].restore_bytes, 10*1024**2)
        # Advisory projection did not mutate any route or heartbeat claim.
        self.assertEqual(cheap.worker_state, 'attached')
        self.assertEqual(self.node.heartbeat.runtime_metrics.storage_hard_reserved_mb, 95_000)

    def test_active_model_wait_and_ready_wake_are_not_cold(self):
        waiting, ready, pending, cold = [self.route(name) for name in ('waiting', 'ready', 'pending', 'cold')]
        requests = [ProgramRequestState(name, 'rollout', name, 1, state, ResourceQuantity())
                    for name, state in [('waiting', 'model_wait'), ('ready', 'ready_to_wake')]]
        self.assertEqual([item.route.sandbox_id for item in self.plan(
            [waiting, ready, pending, cold], requests, pending_wake_sandbox_ids={'pending'})], ['cold'])

    def test_unknown_or_recovered_capacity_does_not_offload(self):
        route = self.route('cold')
        for metrics in (None, NodeRuntimeMetrics(collected_at=self.now),
                        NodeRuntimeMetrics(collected_at=self.now,
                            storage_hard_capacity_mb=100_000, storage_hard_reserved_mb=80_000)):
            self.node.heartbeat = replace(self.heartbeat, runtime_metrics=metrics)
            self.assertEqual(self.plan([route]), ())

    def test_interrupted_detach_retries_even_after_worker_releases_claim(self):
        route = self.route('cold', worker_state='detaching')
        self.node.heartbeat = replace(self.heartbeat, runtime_metrics=None)
        (candidate,) = self.plan([route])
        self.assertEqual(candidate.reason, 'resume_detach')
        self.assertEqual(self.plan([replace(route, worker_state='detached')]), ())

    def test_unpublished_deleting_stale_and_draining_nodes_are_not_candidates(self):
        route = self.route('cold')
        self.assertEqual(self.plan([replace(route, snapshot_manifest_digest='')]), ())
        self.assertEqual(self.plan([replace(route, delete_operation_id='delete:cold')]), ())
        self.assertEqual(self.plan([route], excluded_job_ids={'job'}), ())
        self.node.is_schedulable = False
        self.assertEqual(self.plan([route]), ())
        self.node.is_schedulable = True
        self.node.heartbeat = replace(self.heartbeat, capabilities=())
        self.assertEqual(self.plan([route]), ())

    def test_equal_cost_prefers_older_wait_and_respects_shared_maintenance_budget(self):
        newer, older = self.route('newer', age=60), self.route('older', age=3600)
        plan = plan_cold_offload([self.node], [newer, older], (), limit=1, now=self.now)
        self.assertEqual(plan[0].route.sandbox_id, 'older')
        self.assertEqual(plan_cold_offload([self.node], [older], (), limit=0), ())

    def test_large_pending_disk_request_can_trigger_below_utilization_threshold(self):
        self.node.heartbeat = replace(self.heartbeat, runtime_metrics=replace(
            self.heartbeat.runtime_metrics, storage_hard_reserved_mb=60_000))
        routes = [self.route('a'), self.route('b'), self.route('c')]
        self.assertEqual(self.plan(routes), ())
        plans = self.plan(routes, pending_disk_mb=55_000)
        self.assertEqual(len(plans), 2)
        # No amount of offload makes an oversized request fit this node.
        self.assertEqual(self.plan(routes, pending_disk_mb=100_001), ())

    def test_conditional_detach_checks_wake_and_exact_snapshot_under_writer_fence(self):
        from pathlib import Path
        from tempfile import TemporaryDirectory
        from ucloud_sandboxes.routing import RoutingStore, wake_pending_demand_id
        with TemporaryDirectory() as root:
            store = RoutingStore(Path(root) / 'routing.sqlite')
            route = store.upsert_sandbox(self.route('cold'))
            self.assertIsNone(store.begin_sandbox_detach(replace(route, node_epoch='stale'), require_cold=True))
            self.assertIsNone(store.begin_sandbox_detach(
                replace(route, snapshot_manifest_digest='sha256:'+'1'*64), require_cold=True))
            store.upsert_pending(wake_pending_demand_id(route.sandbox_id), route.resources)
            self.assertIsNone(store.begin_sandbox_detach(route, require_cold=True))
            store.clear_pending(wake_pending_demand_id(route.sandbox_id))
            store.upsert_program_request_transition_with_change(route, request_id='request',
                rollout_id='rollout', state='ready_to_wake')
            self.assertIsNone(store.begin_sandbox_detach(route, require_cold=True))
            store.upsert_program_request_transition_with_change(route, request_id='request',
                rollout_id='rollout', state='terminal')
            detached = store.begin_sandbox_detach(route, require_cold=True)
            self.assertEqual(detached.worker_state, 'detaching')
            # An interrupted committed intent is still finishable after restart;
            # no route claim disappears until complete_sandbox_detach.
            reopened = RoutingStore(store.path)
            self.assertEqual(reopened.get_sandbox_readonly(route.sandbox_id).worker_state, 'detaching')
            self.assertEqual(reopened.get_sandbox_readonly(route.sandbox_id).resources.disk_mb, route.resources.disk_mb)
            self.assertEqual(reopened.complete_sandbox_detach(detached).worker_state, 'detached')

    def test_gateway_revalidates_new_model_request_before_autonomous_eviction(self):
        from pathlib import Path
        from tempfile import TemporaryDirectory
        from unittest.mock import Mock
        from ucloud_sandboxes.control_plane import ControlPlaneHandler
        from ucloud_sandboxes.routing import RoutingStore, cold_offload_fence
        with TemporaryDirectory() as root:
            store = RoutingStore(Path(root) / 'routing.sqlite')
            route = store.upsert_sandbox(self.route('cold'))
            handler = object.__new__(ControlPlaneHandler)
            handler.routing_store = store
            handler._read_json_body = lambda: {'if_cold': cold_offload_fence(route)}
            handler._ensure_registry_route_reference = lambda *_, **__: store.upsert_program_request_transition_with_change(
                route, request_id='arrived-after-selection', rollout_id='rollout', state='model_wait')
            handler._finish_sandbox_detach = Mock()
            responses = []
            handler._write_json = lambda payload, **kwargs: responses.append((payload, kwargs))
            handler._detach_sandbox_from_worker(route.sandbox_id)
            self.assertEqual(responses[0][1]['status'], 409)
            handler._finish_sandbox_detach.assert_not_called()
            self.assertEqual(store.get_sandbox_readonly(route.sandbox_id).worker_state, 'attached')

    def test_admission_offload_requires_enough_cold_claims_within_expense_budget(self):
        self.node.heartbeat = replace(self.heartbeat, runtime_metrics=replace(
            self.heartbeat.runtime_metrics, storage_hard_reserved_mb=60_000))
        route = self.route('only', claim=10_000)
        self.assertEqual(self.plan([route], pending_disk_mb=55_000), ())
        other = self.route('other', claim=10_000)
        self.assertEqual(plan_cold_offload([self.node], [route, other], (),
            pending_disk_mb=55_000, limit=1), ())
