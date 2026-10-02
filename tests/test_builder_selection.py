import json
import unittest
from dataclasses import replace
from unittest.mock import Mock, patch
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from collections import Counter
from ucloud_sandboxes import control_plane

from ucloud_sandboxes.build_admission import BUILD_ADMISSION_CAPACITY_LABEL
from ucloud_sandboxes.control_plane import ControlPlaneHandler, ProxiedResponse
from ucloud_sandboxes.deployment import package_version
from ucloud_sandboxes.images import DEFAULT_MAX_ACTIVE_IMAGE_BUILDS
from ucloud_sandboxes.models import NodeHeartbeat, ResourceQuantity, utc_now
from ucloud_sandboxes.registry import heartbeat_to_dict
from tests.gateway_support import gateway_services


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
        self.handler.services = gateway_services()
        self.handler.services.fleet.ready_heartbeats = Mock(return_value=[self.busy, self.idle])
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

    def test_terminal_build_keeps_its_owner_until_cleanup_releases_admission(self):
        for status in ("succeeded", "failed"):
            for phase in ("preparing_solving", "finishing"):
                with self.subTest(status=status, phase=phase):
                    self.handler._proxy_request.reset_mock()
                    self.handler._proxy_request.side_effect = None
                    self.handler._proxy_request.return_value = self.response(
                        200, {"build": {"status": status, "admission_phase": phase}})
                    self.assertEqual(
                        self.handler._select_builder_node(image_id="existing"), self.busy)
                    self.assertEqual(self.handler._proxy_request.call_count, 1)

    def test_completed_build_does_not_pin_new_build_to_busy_node(self):
        self.handler._proxy_request.side_effect = [
            self.response(200, {"build": {"status": "succeeded"}}),
            self.response(404),
            self.probe(self.busy.node_url, "/v1/heartbeat"),
            self.probe(self.idle.node_url, "/v1/heartbeat"),
        ]
        self.assertEqual(self.handler._select_builder_node(image_id="again"), self.idle)

    def test_single_full_builder_leaves_new_work_pending(self):
        self.handler.services.fleet.ready_heartbeats.return_value = [self.busy]
        self.assertIsNone(self.handler._select_builder_node(image_id="only"))
        self.assertEqual(self.handler._proxy_request.call_count, 2)

    def test_single_full_builder_still_receives_existing_build_retry(self):
        self.handler.services.fleet.ready_heartbeats.return_value = [self.busy]
        self.handler._proxy_request.side_effect = None
        self.handler._proxy_request.return_value = self.response(200, {"build": {"status": "running"}})
        self.assertEqual(self.handler._select_builder_node(image_id="existing"), self.busy)

    def test_all_full_builders_leave_work_unassigned_until_a_peer_frees(self):
        self.idle = replace(self.idle, active_image_builds=4)
        self.handler.services.fleet.ready_heartbeats.return_value = [self.busy, self.idle]
        with (
            patch.dict(control_plane._BUILDER_DISPATCH_COUNTS, {}, clear=True),
            patch.dict(control_plane._BUILDER_DISPATCH_INFLIGHT, {}, clear=True),
        ):
            self.assertIsNone(self.handler._select_builder_node(image_id="waiting", reserve=True))
            self.assertEqual(control_plane._BUILDER_DISPATCH_COUNTS, {})
            # The periodic heartbeat remains full. The retry sees the newly
            # free live slot and has no previous queued owner to pin it.
            self.idle = replace(self.idle, active_image_builds=3)
            selected = self.handler._select_builder_node(image_id="waiting", reserve=True)
            self.assertEqual(selected.job_id, self.idle.job_id)
            self.assertEqual(control_plane._BUILDER_DISPATCH_INFLIGHT, {self.idle.job_id: 1})

    def test_burst_uses_live_load_instead_of_stale_periodic_heartbeat(self):
        stale = replace(self.busy, active_image_builds=0, physical_disk_free_mb=999999)
        self.handler.services.fleet.ready_heartbeats.return_value = [stale, self.idle]
        self.assertEqual(self.handler._select_builder_node(image_id="burst"), self.idle)

    def test_live_capacity_can_reopen_or_close_a_periodically_full_builder(self):
        stale = replace(self.busy, labels={BUILD_ADMISSION_CAPACITY_LABEL: "4"})
        self.handler.services.fleet.ready_heartbeats.return_value = [stale]
        self.busy = replace(self.busy, labels={BUILD_ADMISSION_CAPACITY_LABEL: "6"})
        self.assertEqual(self.handler._select_builder_node(image_id="new"), self.busy)
        for capacity in ("4", "0", "bad"):
            with self.subTest(capacity=capacity):
                self.busy = replace(self.busy, labels={BUILD_ADMISSION_CAPACITY_LABEL: capacity})
                self.assertIsNone(self.handler._select_builder_node(image_id="new"))

    def test_legacy_live_sample_clears_stale_capacity_and_preserves_other_labels(self):
        stale = replace(self.busy, labels={
            BUILD_ADMISSION_CAPACITY_LABEL: "6", "controller-owned": "keep",
        })
        self.handler.services.fleet.ready_heartbeats.return_value = [stale]
        # The live legacy node is full at four; the periodic six-slot hint
        # must not survive refresh and dispatch another build to it.
        self.assertIsNone(self.handler._select_builder_node(image_id="new"))
        self.busy = replace(self.busy, active_image_builds=3)
        selected = self.handler._select_builder_node(image_id="new")
        self.assertEqual(selected.labels, {"controller-owned": "keep"})
        self.assertEqual(stale.labels[BUILD_ADMISSION_CAPACITY_LABEL], "6")

    def test_closed_capacity_still_replays_the_active_owner(self):
        for capacity in ("0", "bad"):
            with self.subTest(capacity=capacity):
                owner = replace(self.busy, labels={BUILD_ADMISSION_CAPACITY_LABEL: capacity})
                self.handler.services.fleet.ready_heartbeats.return_value = [owner]
                self.handler._proxy_request.side_effect = None
                self.handler._proxy_request.return_value = self.response(
                    200, {"build": {"status": "running"}},
                )
                self.handler._proxy_request.reset_mock()
                self.assertEqual(self.handler._select_builder_node(image_id="existing"), owner)
                self.assertEqual(self.handler._proxy_request.call_count, 1)

    def test_simultaneous_samples_honor_each_nodes_capacity_and_existing_load(self):
        nodes = [
            replace(self.idle, node_id="small", job_id="small", node_url="http://small",
                    active_image_builds=1, labels={BUILD_ADMISSION_CAPACITY_LABEL: "2"}),
            replace(self.idle, node_id="large", job_id="large", node_url="http://large",
                    active_image_builds=4, labels={BUILD_ADMISSION_CAPACITY_LABEL: "6"}),
        ]
        sampled = Barrier(8)
        self.handler.services.fleet.ready_heartbeats.return_value = nodes

        def probe(url, path, **kwargs):
            current = next(node for node in nodes if node.node_url == url)
            if current == nodes[-1]:
                sampled.wait(5)
            return self.response(200, {"heartbeat": heartbeat_to_dict(current)})

        self.handler._proxy_request.side_effect = probe
        with (
            patch.dict(control_plane._BUILDER_DISPATCH_COUNTS, {}, clear=True),
            patch.dict(control_plane._BUILDER_DISPATCH_INFLIGHT, {}, clear=True),
        ):
            def dispatch(_):
                selected = self.handler._select_builder_node(reserve=True)
                if selected is None:
                    return None
                # Completion must not erase a reservation for a concurrent
                # selector still holding the same old live sample.
                with control_plane._BUILDER_DISPATCH_GUARD:
                    control_plane._BUILDER_DISPATCH_INFLIGHT[selected.job_id] -= 1
                return selected.job_id

            with ThreadPoolExecutor(max_workers=8) as pool:
                selected = list(pool.map(dispatch, range(8)))
            self.assertEqual(Counter(selected), {"small": 1, "large": 2, None: 5})
            self.assertTrue(all(v == 0 for v in control_plane._BUILDER_DISPATCH_INFLIGHT.values()))

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
                self.assertIsNone(self.handler._select_builder_node(image_id="new"))

    def test_simultaneous_live_samples_reserve_distinct_builder_capacity(self):
        nodes = [
            replace(self.idle, node_id=f"node-{i}", job_id=f"job-{i}", node_url=f"http://node-{i}")
            for i in range(4)
        ]
        sampled = Barrier(20)
        self.handler.services.fleet.ready_heartbeats.return_value = nodes

        def probe(url, path, **kwargs):
            current = next(h for h in nodes if h.node_url == url)
            if current == nodes[-1]:
                sampled.wait(5)
            return self.response(200, {"heartbeat": heartbeat_to_dict(current)})

        self.handler._proxy_request.side_effect = probe
        with (
            patch.dict(control_plane._BUILDER_DISPATCH_COUNTS, {}, clear=True),
            patch.dict(control_plane._BUILDER_DISPATCH_INFLIGHT, {}, clear=True),
        ):
            def dispatch(_):
                chosen = self.handler._select_builder_node(reserve=True)
                if chosen is None:
                    return None
                # A response may finish before another stale sample chooses.
                with control_plane._BUILDER_DISPATCH_GUARD:
                    control_plane._BUILDER_DISPATCH_INFLIGHT[chosen.job_id] -= 1
                return chosen.job_id

            with ThreadPoolExecutor(max_workers=20) as pool:
                selected = list(pool.map(dispatch, range(20)))
            self.assertEqual(Counter(selected), {**{h.job_id: 4 for h in nodes}, None: 4})
            self.assertTrue(all(v == 0 for v in control_plane._BUILDER_DISPATCH_INFLIGHT.values()))

    def test_selection_counts_dispatch_still_waiting_for_builder_acceptance(self):
        self.handler.services.fleet.ready_heartbeats.return_value = [self.idle, replace(self.idle, job_id="3", node_id="c-idle", node_url="http://third")]
        self.handler._proxy_request.side_effect = lambda url, path, **kw: self.response(
            200, {"heartbeat": heartbeat_to_dict(next(h for h in self.handler.services.fleet.ready_heartbeats() if h.node_url == url))},
        )
        # Builds pack onto a builder until its slots are full; dispatches it
        # has not acknowledged yet count toward that.
        full = DEFAULT_MAX_ACTIVE_IMAGE_BUILDS
        with (
            patch.dict(control_plane._BUILDER_DISPATCH_COUNTS, {"2": full}, clear=True),
            patch.dict(control_plane._BUILDER_DISPATCH_INFLIGHT, {"2": full}, clear=True),
        ):
            self.assertEqual(self.handler._select_builder_node(reserve=True).job_id, "3")
