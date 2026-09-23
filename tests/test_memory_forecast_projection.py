from dataclasses import replace
import unittest

from tests.test_control_plane import build_heartbeat, _sandbox_route
from ucloud_sandboxes.cli import apply_route_reservations_to_heartbeats
from ucloud_sandboxes.models import ResourceQuantity, SandboxInventoryEntry


class MemoryForecastProjectionTests(unittest.TestCase):
    def setUp(self):
        self.heartbeat = build_heartbeat(node_id="node", job_id="job",
            node_url="http://node:8090", total_resources=ResourceQuantity(32, 98304, 1000000),
            used_resources=ResourceQuantity())

    def route(self, name, state):
        return _sandbox_route(sandbox_id=name, node_id="node", job_id="job",
            node_url="http://node:8090", resources=ResourceQuantity(1, 2048, 4096),
            state=state, spec={"image": "image"})

    def project(self, routes, heartbeat=None):
        return apply_route_reservations_to_heartbeats(
            {"job": heartbeat or self.heartbeat}, {"job": tuple(routes)},
        )["job"]

    def test_many_parked_owners_do_not_permanently_reserve_memory(self):
        routes = [self.route(str(i), "parked") for i in range(512)]
        self.assertEqual(self.project(routes).reserved_resources.memory_mb, 0)

    def test_new_creates_and_wakes_forecast_startup_ram(self):
        routes = [self.route("create", "creating"), self.route("wake", "waking"),
                  self.route("park", "parked"), self.route("run", "running")]
        self.assertEqual(self.project(routes).reserved_resources.memory_mb, 4096)

    def test_exact_inventory_and_worker_reservation_are_not_added_twice(self):
        route = self.route("create", "creating")
        entry = SandboxInventoryEntry(route.sandbox_id, route.generation,
            route.create_operation_id, route.spec_hash, route.state, route.resources)
        heartbeat = replace(self.heartbeat, inventory=(entry,),
            reserved_resources=ResourceQuantity(memory_mb=2048))
        projected = self.project([route], heartbeat)
        self.assertEqual(projected.reserved_resources.memory_mb, 2048)
        self.assertEqual(self.project([route], projected).reserved_resources.memory_mb, 2048)

    def test_wake_forecast_does_not_wait_for_next_inventory_state(self):
        route = self.route("wake", "waking")
        entry = SandboxInventoryEntry(route.sandbox_id, route.generation,
            route.create_operation_id, route.spec_hash, "parked", route.resources)
        self.assertEqual(self.project([route], replace(self.heartbeat,
            inventory=(entry,))).reserved_resources.memory_mb, 2048)
