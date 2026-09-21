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
                    handler.routing_store.sandbox_routes_readonly.return_value = [local, incoming]
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
