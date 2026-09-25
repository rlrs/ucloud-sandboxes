from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import Mock, patch
import unittest

from tests.test_control_plane import _gateway_server, _sandbox_route, build_heartbeat
from ucloud_sandboxes import control_plane
from ucloud_sandboxes.models import ResourceQuantity


class WakeOwnerAdmissionTests(unittest.TestCase):
    def test_shadow_observes_admission_view_without_another_inventory_read(self):
        with TemporaryDirectory() as temp:
            server = _gateway_server(Path(temp))
            try:
                handler = object.__new__(server.RequestHandlerClass)
                route = _sandbox_route(
                    sandbox_id="local", node_id="owner", job_id="job-owner",
                    node_url="http://owner:8090", state="parked",
                    resources=ResourceQuantity(vcpu=1, memory_mb=1024, disk_mb=2048),
                    spec={"parkable": True, "managed_process": True},
                )
                handler.routing_store.upsert_sandbox(route)
                handler.store.upsert_heartbeat(build_heartbeat(
                    node_id=route.node_id, job_id=route.job_id, node_url=route.node_url,
                    capabilities=("sandbox", "disk-quota"),
                    total_resources=ResourceQuantity(vcpu=32, memory_mb=98304, disk_mb=100000),
                ))
                with (
                    patch.object(handler, "_placement_routes_for_node", wraps=handler._placement_routes_for_node) as reads,
                    patch.object(handler, "_record_program_wake_shadow_plan") as observe,
                ):
                    handler._prepare_program_lifecycle(route, "wake", {
                        "request_id": "request-local", "rollout_id": "rollout-local",
                    }, defer_local_shadow=True)
                    observe.assert_not_called()
                    placed, _ = handler._prepare_wake_placement(route)
                    self.assertEqual(placed.state, "waking")
                    handler._flush_program_wake_shadow()
                    handler._flush_program_wake_shadow()
                    reads.assert_called_once()
                    observe.assert_called_once()
                    # Telemetry describes the capacity decision before its
                    # reservation, even though the durable route is now waking.
                    self.assertEqual(observe.call_args.args[2][0].state, "parked")
                    self.assertEqual(handler.routing_store.get_sandbox_readonly(route.sandbox_id).state, "waking")
                    handler._prepare_program_lifecycle(route, "wake", {
                        "request_id": "request-race", "rollout_id": "rollout-local",
                    }, defer_local_shadow=True)
                    # Another request already reserved this wake. Admission
                    # returns before building a view, so observation reads a
                    # fresh owner snapshot rather than reusing the old one.
                    handler._prepare_wake_placement(route)
                    handler._flush_program_wake_shadow()
                    self.assertEqual(reads.call_count, 2)
                    self.assertEqual(observe.call_count, 2)
                    self.assertEqual(observe.call_args.args[2][0].state, "waking")
                    handler._prepare_program_lifecycle(route, "park", {})
                    self.assertIsNone(handler._program_wake_owner_view)
                    self.assertIsNone(handler._deferred_program_wake_shadow)
            finally:
                server.server_close()

    def test_unpublished_local_wake_does_not_read_unrelated_fleet_state(self):
        with TemporaryDirectory() as temp:
            server = _gateway_server(Path(temp))
            try:
                handler = object.__new__(server.RequestHandlerClass)
                route = _sandbox_route(
                    sandbox_id="local", node_id="owner", job_id="job-owner",
                    node_url="http://owner:8090", state="parked",
                    resources=ResourceQuantity(vcpu=2, memory_mb=1024, disk_mb=2048),
                    spec={"parkable": True, "managed_process": True},
                )
                handler.routing_store.upsert_sandbox(route)
                handler.store.upsert_heartbeat(build_heartbeat(
                    node_id="owner", job_id="job-owner", node_url=route.node_url,
                    capabilities=("sandbox", "disk-quota"),
                    total_resources=ResourceQuantity(vcpu=32, memory_mb=98304, disk_mb=100000),
                ))
                with (
                    patch.object(handler.routing_store, "sandbox_routes_readonly",
                                 side_effect=AssertionError("whole fleet route read")),
                    patch.object(handler.routing_store, "placement_routes_readonly",
                                 side_effect=AssertionError("whole fleet route read")),
                    patch.object(handler.store, "load_heartbeats",
                                 side_effect=AssertionError("whole fleet heartbeat read")),
                ):
                    handler._prepare_program_lifecycle(route, "wake", {
                        "request_id": "request-local", "rollout_id": "rollout-local",
                    })
                    placed, moved = handler._prepare_wake_placement(route)
                    self.assertEqual(placed.state, "waking")
                    self.assertFalse(moved)
                    # A later read observes new admission; this is not a TTL cache.
                    observed = handler._placement_routes_for_node(
                        handler.store.get_heartbeat("job-owner")
                    )
                    self.assertEqual(observed[0].state, "waking")
            finally:
                server.server_close()

    def test_placement_reread_rejects_recreated_incarnation_before_worker_dispatch(self):
        for operation in ("_prepare_wake_placement",):
            with self.subTest(operation=operation), TemporaryDirectory() as temp:
                server = _gateway_server(Path(temp))
                try:
                    handler = object.__new__(server.RequestHandlerClass)
                    previous = _sandbox_route(sandbox_id="recreated", state="parked",
                                              node_id="node", job_id="job", node_url="http://node")
                    handler.routing_store.upsert_sandbox(previous)
                    handler.routing_store.delete_sandbox(previous.sandbox_id)
                    replacement = handler.routing_store.upsert_sandbox(replace(
                        previous, generation=previous.generation + 1,
                        create_operation_id="replacement", state="running",
                    ))
                    with patch.object(handler, "_write_json") as reply, patch.object(
                        handler, "_proxy_request", side_effect=AssertionError("replacement received stale wake"),
                    ):
                        self.assertIsNone(getattr(handler, operation)(previous))
                    self.assertEqual(reply.call_args.kwargs["status"], 503)
                    self.assertTrue(reply.call_args.args[0]["retryable"])
                    self.assertEqual(handler.routing_store.get_sandbox_readonly(previous.sandbox_id), replacement)
                finally:
                    server.server_close()

    def test_owner_view_preserves_incoming_migration_reservations(self):
        owner = build_heartbeat(node_id="owner", job_id="job-owner", node_url="http://owner")
        local = _sandbox_route(
            sandbox_id="local", node_id="owner", job_id="job-owner",
            node_url="http://owner", state="parked",
            resources=ResourceQuantity(vcpu=2, memory_mb=1024, disk_mb=2048),
        )
        incoming = replace(local, sandbox_id="incoming", spec={"id": "incoming"},
                           node_id="other", job_id="job-other", node_url="http://other")
        for phase in ("planned", "imported", "routed", "activated"):
            for identity in (
                ("owner", "foreign-job", "http://foreign"),
                ("foreign-node", "job-owner", "http://foreign"),
                ("foreign-node", "foreign-job", "http://owner/"),
            ):
                with self.subTest(phase=phase, identity=identity):
                    migration = SimpleNamespace(
                        migration_id="move-in", sandbox_id="incoming", phase=phase,
                        destination_node_id=identity[0], destination_job_id=identity[1],
                        destination_node_url=identity[2],
                    )
                    handler = object.__new__(control_plane.ControlPlaneHandler)
                    handler.routing_store = Mock()
                    handler.routing_store.placement_routes_readonly.return_value = [local, incoming]
                    handler.routing_store.sandbox_routes_matching_node_identity.return_value = [local]
                    handler.routing_store.sandbox_migrations.return_value = [migration]
                    handler.routing_store.get_sandbox_readonly.return_value = incoming
                    full = handler._placement_routes()
                    scoped = handler._placement_routes_for_node(owner)
                    expected = [r for r in full if control_plane._route_targets_node(r, owner)]
                    self.assertEqual(scoped, expected)
                    reservation = scoped[-1]
                    self.assertEqual(reservation.resources.memory_mb, 1024)
                    self.assertEqual(reservation.resources.disk_mb,
                                     0 if phase in {"routed", "activated"} else 2048)
                    handler.routing_store.get_sandbox_readonly.assert_called_once_with("incoming")


class DurableRelayGatewayTests(unittest.TestCase):
    def test_fenced_payload_requires_capable_owner_and_stale_park_is_skipped(self):
        import json
        from ucloud_sandboxes.capabilities import RELAY_WAKE_FENCE_CAPABILITY
        with TemporaryDirectory() as raw:
            server=_gateway_server(Path(raw))
            try:
                handler=object.__new__(server.RequestHandlerClass)
                handler._write_json=Mock()
                route=_sandbox_route(sandbox_id='agent',node_id='worker',job_id='job',node_url='http://worker',
                                     state='running',spec={'parkable':True,'managed_process':True})
                handler.routing_store.upsert_sandbox(route)
                owner=build_heartbeat(node_id='worker',job_id='job',node_url='http://worker',capabilities=('sandbox',))
                handler.store.upsert_heartbeat(owner)
                payload={'operation_id':'relay-park:r','request_id':'r','rollout_id':'rollout','generation':route.generation,'durable_lifecycle':True}
                self.assertIsNone(handler._lifecycle_proxy_body(route,'park',payload))
                self.assertEqual(handler._write_json.call_args.kwargs['status'],503)
                handler.store.upsert_heartbeat(replace(owner,capabilities=('sandbox',RELAY_WAKE_FENCE_CAPABILITY)))
                forwarded=json.loads(handler._lifecycle_proxy_body(route,'park',payload))
                self.assertEqual(forwarded,{'operation_id':'relay-park:r','generation':route.generation,'relay_request_id':'r'})
                handler._record_program_request_transition(route,payload,state='ready_to_wake')
                self.assertIsNone(handler._parse_lifecycle_request(route,'park',json.dumps(payload).encode()))
                self.assertEqual(handler._write_json.call_args.args[0],{'skipped':True,'reason':'model_result_ready'})
            finally:
                server.server_close()
