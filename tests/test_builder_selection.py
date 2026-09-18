import json
import unittest
from dataclasses import replace
from unittest.mock import Mock

from ucloud_sandboxes.control_plane import ControlPlaneHandler, ProxiedResponse
from ucloud_sandboxes.deployment import package_version
from ucloud_sandboxes.models import NodeHeartbeat, ResourceQuantity, utc_now
from ucloud_sandboxes.registry import heartbeat_to_dict


class BuilderSelectionTests(unittest.TestCase):
    def setUp(self):
        self.busy = NodeHeartbeat(
            node_id="a-busy",
            job_id="1",
            updated_at=utc_now(),
            active_sandboxes=0,
            active_image_builds=4,
            node_url="http://busy",
            agent_version=package_version(),
            deployment_id="test-deployment",
            capabilities=("image-build",),
            total_resources=ResourceQuantity(vcpu=16, memory_mb=49152, disk_mb=204800),
            physical_disk_free_mb=60000,
        )
        self.idle = replace(
            self.busy,
            node_id="b-idle",
            job_id="2",
            node_url="http://idle",
            active_image_builds=0,
            physical_disk_free_mb=240000,
        )
        self.handler = object.__new__(ControlPlaneHandler)
        self.handler._ready_heartbeats = Mock(return_value=[self.busy, self.idle])
        self.handler._proxy_request = Mock(side_effect=self.probe)

    def probe(self, url, path, **kwargs):
        if path == "/v1/heartbeat":
            node = self.busy if url == self.busy.node_url else self.idle
            return self.response(200, {"heartbeat": heartbeat_to_dict(node)})
        return self.response(404)

    @staticmethod
    def response(status, payload=None):
        return ProxiedResponse(status, {}, json.dumps(payload or {}).encode())

    def test_new_build_uses_idle_peer_with_equal_nominal_capacity(self):
        self.assertEqual(self.handler._select_builder_node(image_id="new"), self.idle)
        self.assertEqual(self.handler._proxy_request.call_count, 4)

    def test_retry_stays_with_busy_active_owner(self):
        self.handler._proxy_request.side_effect = None
        self.handler._proxy_request.return_value = self.response(
            200, {"build": {"status": "running"}}
        )
        self.assertEqual(
            self.handler._select_builder_node(image_id="existing"), self.busy
        )

    def test_unknown_owner_does_not_dispatch_duplicate_build(self):
        self.handler._proxy_request.side_effect = None
        for response in (self.response(503), self.response(200, {"build": {}})):
            with self.subTest(response=response.status):
                self.handler._proxy_request.return_value = response
                self.assertIsNone(
                    self.handler._select_builder_node(image_id="existing")
                )

    def test_completed_build_does_not_pin_new_build_to_busy_node(self):
        self.handler._proxy_request.side_effect = [
            self.response(200, {"build": {"status": "succeeded"}}),
            self.response(404),
            self.probe(self.busy.node_url, "/v1/heartbeat"),
            self.probe(self.idle.node_url, "/v1/heartbeat"),
        ]
        self.assertEqual(self.handler._select_builder_node(image_id="again"), self.idle)

    def test_single_builder_needs_no_owner_probe(self):
        self.handler._ready_heartbeats.return_value = [self.busy]
        self.assertEqual(self.handler._select_builder_node(image_id="only"), self.busy)
        self.handler._proxy_request.assert_not_called()

    def test_burst_uses_live_load_instead_of_stale_periodic_heartbeat(self):
        stale = replace(self.busy, active_image_builds=0, physical_disk_free_mb=999999)
        self.handler._ready_heartbeats.return_value = [stale, self.idle]
        self.assertEqual(self.handler._select_builder_node(image_id="burst"), self.idle)

    def test_live_draining_or_restarted_builder_is_not_selected(self):
        for changed in (
            replace(self.idle, draining=True),
            replace(self.idle, node_epoch="new"),
        ):
            with self.subTest(node=changed):
                self.handler._proxy_request.side_effect = [
                    self.response(404),
                    self.response(404),
                    self.probe(self.busy.node_url, "/v1/heartbeat"),
                    self.response(200, {"heartbeat": heartbeat_to_dict(changed)}),
                ]
                self.assertEqual(
                    self.handler._select_builder_node(image_id="new"), self.busy
                )
