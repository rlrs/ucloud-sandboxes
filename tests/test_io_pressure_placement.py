from collections import Counter
from dataclasses import replace
import json
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
import unittest

from tests.test_control_plane import build_heartbeat, _sandbox_route
from ucloud_sandboxes import control_plane
from ucloud_sandboxes.control_state import ControlStateStore
from ucloud_sandboxes.deployment import package_version
from ucloud_sandboxes.models import NodeRuntimeMetrics, ResourceQuantity, utc_now
from ucloud_sandboxes.program_scheduler import node_pressure_score
from ucloud_sandboxes.runtime_metrics import sample_node_runtime_metrics


class IoPressurePlacementTests(unittest.TestCase):
    def test_mapped_guest_memory_guides_placement_without_blocking_admission(self):
        quiet = self.heartbeat("quiet")
        busy = replace(quiet, node_id="busy", job_id="busy", node_url="http://busy:8090",
                       runtime_metrics=replace(quiet.runtime_metrics, memory_working_set_mb=88000))
        handler = object.__new__(control_plane.ControlPlaneHandler)
        handler._placement_routes = lambda: []
        handler._ready_sandbox_heartbeats = lambda: [busy, quiet]
        handler._nodes_with_image = lambda *_args, **_kwargs: {"busy", "quiet"}
        handler.registry_layer_cache = None
        handler.create_target_concurrency_per_node = 4
        requested = ResourceQuantity(1, 1024, 4096)
        self.assertEqual(handler._select_node(requested, image="image").node_id, "quiet")
        handler._ready_sandbox_heartbeats = lambda: [busy]
        self.assertEqual(handler._select_node(requested, image="image").node_id, "busy")

    def heartbeat(self, node, *, io=0, memory=0, used_disk=0):
        return build_heartbeat(
            node_id=node, job_id=node, node_url=f"http://{node}:8090",
            agent_version=package_version(), capabilities=("sandbox", "disk-quota"),
            total_resources=ResourceQuantity(32, 98304, 1_000_000),
            used_resources=ResourceQuantity(disk_mb=used_disk),
            cached_images=("image",),
            runtime_metrics=NodeRuntimeMetrics(
                collected_at=utc_now(), cpu_percent=10, memory_percent=10,
                memory_total_mb=98304, memory_available_mb=88000,
                io_psi_some_avg10=io, memory_psi_some_avg10=memory,
            ),
        )

    def test_placement_uses_stall_headroom_and_inflight_reservations(self):
        busy = self.heartbeat("busy", io=75, used_disk=500000)
        quiet = self.heartbeat("quiet")
        handler = object.__new__(control_plane.ControlPlaneHandler)
        routes = []
        handler._placement_routes = lambda: routes
        handler._ready_sandbox_heartbeats = lambda: [busy, quiet]
        handler._nodes_with_image = lambda *_args, **_kwargs: {"busy", "quiet"}
        handler.registry_layer_cache = None
        handler.create_target_concurrency_per_node = 4
        requested = ResourceQuantity(1, 1024, 4096)
        chosen = []
        for index in range(8):
            node = handler._select_node(requested, image="image")
            chosen.append(node.node_id)
            routes.append(_sandbox_route(
                sandbox_id=str(index), node_id=node.node_id, job_id=node.job_id,
                node_url=node.node_url, resources=requested, state="creating",
                spec={"image": "image"},
            ))
        self.assertEqual(chosen[0], "quiet")
        counts = Counter(chosen)
        self.assertGreater(counts["quiet"], counts["busy"])
        self.assertGreater(counts["busy"], 0)
        # Even extreme I/O PSI only affects ranking; it cannot close admission.
        handler._ready_sandbox_heartbeats = lambda: [self.heartbeat("busy", io=99)]
        self.assertIsNotNone(handler._select_node(requested, image="image"))

    def test_completed_creates_remain_visible_to_load_balancing(self):
        handler = object.__new__(control_plane.ControlPlaneHandler)
        routes = []
        nodes = [self.heartbeat(str(n), io=40 if n == 3 else 0) for n in range(4)]
        handler._placement_routes = lambda: routes
        handler._ready_sandbox_heartbeats = lambda: nodes
        handler._nodes_with_image = lambda *_args, **_kwargs: {n.node_id for n in nodes}
        handler.registry_layer_cache = None
        handler.create_target_concurrency_per_node = 4
        for index in range(256):
            node = handler._select_node(ResourceQuantity(1, 1024, 4096), image="image")
            routes.append(_sandbox_route(
                sandbox_id=str(index), node_id=node.node_id, job_id=node.job_id,
                node_url=node.node_url, state="running",
                resources=ResourceQuantity(1, 1024, 4096), spec={"image": "image"},
            ))
        counts = Counter(r.node_id for r in routes)
        # Pressure can favor the quieter peers without letting a stale sample
        # strand most of a ready worker's capacity for the entire burst.
        self.assertEqual(len(counts), 4)
        self.assertLess(max(counts.values()) - min(counts.values()), 16)
        self.assertLess(counts['3'], counts['0'])

    def test_partial_memory_reclaim_is_a_ranking_signal(self):
        self.assertGreater(node_pressure_score(self.heartbeat("reclaim", memory=60)),
                           node_pressure_score(self.heartbeat("quiet")))

    def test_image_cache_affinity_does_not_hide_idle_worker(self):
        busy = self.heartbeat("busy", io=75)
        quiet = replace(self.heartbeat("quiet"), cached_images=())
        handler = object.__new__(control_plane.ControlPlaneHandler)
        handler._placement_routes = lambda: []
        handler._ready_sandbox_heartbeats = lambda: [busy, quiet]
        handler._nodes_with_image = lambda *_args, **_kwargs: {"busy"}
        handler.registry_layer_cache = None
        handler.create_target_concurrency_per_node = 4
        requested = ResourceQuantity(1, 1024, 4096)
        self.assertEqual(handler._select_node(requested, image="image").node_id, "quiet")
        # Retain locality when both nodes have comparable pressure/headroom.
        busy = self.heartbeat("busy")
        self.assertEqual(handler._select_node(requested, image="image").node_id, "busy")

    def test_io_psi_sampling_and_unavailable_signal(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "pressure").mkdir()
            (root / "pressure" / "io").write_text(
                "some avg10=36.50 avg60=20 total=1\nfull avg10=11.25 avg60=10 total=1\n"
            )
            sampled = sample_node_runtime_metrics(proc_root=root, sample_seconds=0)
            self.assertEqual(sampled.io_psi_some_avg10, 36.5)
            self.assertEqual(sampled.io_psi_full_avg10, 11.25)
            (root / "pressure" / "io").unlink()
            self.assertIsNone(sample_node_runtime_metrics(proc_root=root, sample_seconds=0).io_psi_some_avg10)

    def test_legacy_database_rows_and_heartbeat_payloads_remain_readable(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "control.sqlite"
            store = ControlStateStore(path)
            heartbeat = self.heartbeat("worker")
            store.upsert_heartbeat(replace(heartbeat, runtime_metrics=replace(
                heartbeat.runtime_metrics, storage_ublk_max_devices=128,
            )))
            with sqlite3.connect(path) as connection:
                row = connection.execute("SELECT payload FROM control_records").fetchone()[0]
                raw = json.loads(row)
                raw["runtime_metrics"].pop("io_psi_some_avg10")
                raw["runtime_metrics"].pop("io_psi_full_avg10")
                connection.execute("UPDATE control_records SET payload = ?",
                                   (json.dumps(raw, sort_keys=True, separators=(",", ":")),))
            current = store.get_heartbeat("worker")
            self.assertIsNone(current.runtime_metrics.io_psi_some_avg10)
            self.assertEqual(current.runtime_metrics.storage_ublk_max_devices, 128)
            for value in (True, "busy", float("nan")):
                raw["runtime_metrics"]["io_psi_some_avg10"] = value
                self.assertIsNone(NodeRuntimeMetrics.from_dict(raw["runtime_metrics"]))
