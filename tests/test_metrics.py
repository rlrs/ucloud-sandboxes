from datetime import timedelta
from tempfile import TemporaryDirectory
from pathlib import Path
import unittest

from ucloud_sandboxes.agent import build_heartbeat
from ucloud_sandboxes.metrics import MetricsStore, build_metrics_snapshot
from ucloud_sandboxes.models import NodeRuntimeMetrics, ResourceQuantity, utc_now
from ucloud_sandboxes.routing import (
    ExecRoute,
    PendingImageBuildDemand,
    PendingSandboxDemand,
    PreparedBuilderDemand,
    PreparedCapacityDemand,
    RoutingState,
    SandboxRoute,
)


class MetricsTests(unittest.TestCase):
    def test_builds_dashboard_snapshot_from_heartbeats_routes_and_events(self) -> None:
        now = utc_now()
        heartbeat = build_heartbeat(
            job_id="job-1",
            node_id="node-1",
            active_sandboxes=1,
            node_url="http://node-1:8090",
            capabilities=("sandbox", "image-cache", "disk-quota"),
            total_resources=ResourceQuantity(vcpu=4, memory_mb=8192, disk_mb=100_000),
            used_resources=ResourceQuantity(vcpu=1, memory_mb=2048, disk_mb=10_000),
            runtime_metrics=NodeRuntimeMetrics(
                collected_at=now,
                cpu_percent=20.0,
                cpu_vcpu=0.8,
                cpu_count=4,
                memory_total_mb=8192,
                memory_used_mb=3072,
                memory_available_mb=5120,
                memory_percent=37.5,
                load_average_1m=0.5,
                load_average_5m=0.4,
                load_average_15m=0.3,
            ),
            now=now,
        )
        routing = RoutingState(
            sandboxes={
                "active-one": SandboxRoute(
                    sandbox_id="active-one",
                    node_id="node-1",
                    job_id="job-1",
                    node_url="http://node-1:8090",
                    resources=ResourceQuantity(vcpu=1, memory_mb=512, disk_mb=1024),
                    created_at=now.isoformat(),
                ),
                "stale-one": SandboxRoute(
                    sandbox_id="stale-one",
                    node_id="stale-node",
                    job_id="stale-job",
                    node_url="http://stale-node:8090",
                    resources=ResourceQuantity(vcpu=1, memory_mb=512, disk_mb=1024),
                    created_at=now.isoformat(),
                )
            },
            exec_sessions={
                "exec-1": ExecRoute(
                    session_id="exec-1",
                    sandbox_id="active-one",
                    node_id="node-1",
                    job_id="job-1",
                    node_url="http://node-1:8090",
                )
            },
            pending={
                "pending-one": PendingSandboxDemand(
                    sandbox_id="pending-one",
                    resources=ResourceQuantity(vcpu=2, memory_mb=4096, disk_mb=2048),
                    created_at=(now - timedelta(seconds=30)).isoformat(),
                    updated_at=now.isoformat(),
                    attempts=2,
                )
            },
            image_builds={
                "image-1": PendingImageBuildDemand(
                    image_id="image-1",
                    tag="registry.example/image:latest",
                    created_at=(now - timedelta(seconds=60)).isoformat(),
                    updated_at=now.isoformat(),
                )
            },
            prepared={
                "prep-1": PreparedCapacityDemand(
                    prepare_id="prep-1",
                    resources=ResourceQuantity(vcpu=1, memory_mb=2048, disk_mb=1024),
                    count=4,
                    created_at=(now - timedelta(seconds=15)).isoformat(),
                    updated_at=now.isoformat(),
                    expires_at=(now + timedelta(seconds=600)).isoformat(),
                )
            },
            prepared_builders={
                "builder-prep-1": PreparedBuilderDemand(
                    prepare_id="builder-prep-1",
                    count=1,
                    created_at=(now - timedelta(seconds=10)).isoformat(),
                    updated_at=now.isoformat(),
                    expires_at=(now + timedelta(seconds=600)).isoformat(),
                )
            },
        )

        with TemporaryDirectory() as raw_dir:
            store = MetricsStore(Path(raw_dir) / "metrics.jsonl")
            store.append(
                "sandbox_scheduled",
                {
                    "sandbox_id": "active-one",
                    "scale_up_wait_ms": 12_000,
                    "had_pending_demand": True,
                },
            )
            snapshot = build_metrics_snapshot(
                {"job-1": heartbeat},
                routing,
                store.load_events(),
                heartbeat_ttl_seconds=120,
            )

        self.assertEqual(snapshot["nodes"]["fresh"], 1)
        self.assertEqual(snapshot["nodes"]["samples"], 0)
        self.assertEqual(snapshot["nodes"]["items"][0]["actual_usage"]["cpu_percent"], 20.0)
        self.assertEqual(snapshot["resources"]["sandbox"]["actual_usage"]["cpu_vcpu"], 0.8)
        self.assertEqual(snapshot["resources"]["sandbox"]["load"]["vcpu"], 0.25)
        self.assertEqual(snapshot["sandboxes"]["running"], 1)
        self.assertEqual(snapshot["sandboxes"]["active_routes"], 2)
        self.assertEqual(snapshot["sandboxes"]["routes_on_fresh_nodes"], 1)
        self.assertEqual(snapshot["sandboxes"]["provisional_running_routes"], 0)
        self.assertEqual(snapshot["sandboxes"]["stale_routes"], 1)
        self.assertEqual(snapshot["sandboxes"]["pending"], 1)
        self.assertEqual(snapshot["sandboxes"]["pending_resources"]["vcpu"], 2.0)
        self.assertEqual(snapshot["sandboxes"]["pending_attempts"], 2)
        self.assertEqual(snapshot["capacity"]["prepared"], 1)
        self.assertEqual(snapshot["capacity"]["prepared_sandboxes"], 4)
        self.assertEqual(snapshot["capacity"]["prepared_resources"]["vcpu"], 4.0)
        self.assertEqual(snapshot["exec"]["sessions"], 1)
        self.assertEqual(snapshot["images"]["pending_builds"], 1)
        self.assertEqual(snapshot["builders"]["prepared_builders"], 1)
        self.assertEqual(snapshot["builders"]["items"][0]["prepare_id"], "builder-prep-1")
        self.assertEqual(snapshot["scale_up"]["samples"], 1)
        self.assertEqual(snapshot["scale_up"]["last_ms"], 12_000)

    def test_recent_route_on_fresh_node_counts_as_provisional_running(self) -> None:
        now = utc_now()
        heartbeat = build_heartbeat(
            job_id="job-1",
            node_id="node-1",
            active_sandboxes=0,
            node_url="http://node-1:8090",
            capabilities=("sandbox", "image-cache", "disk-quota"),
            now=now,
        )
        routing = RoutingState(
            sandboxes={
                "new-one": SandboxRoute(
                    sandbox_id="new-one",
                    node_id="node-1",
                    job_id="job-1",
                    node_url="http://node-1:8090",
                    resources=ResourceQuantity(vcpu=1, memory_mb=512, disk_mb=1024),
                    created_at=(now + timedelta(seconds=1)).isoformat(),
                )
            },
            exec_sessions={},
            pending={},
            image_builds={},
            prepared={},
            prepared_builders={},
        )

        snapshot = build_metrics_snapshot(
            {"job-1": heartbeat},
            routing,
            [],
            heartbeat_ttl_seconds=120,
        )

        self.assertEqual(snapshot["sandboxes"]["running"], 1)
        self.assertEqual(snapshot["sandboxes"]["provisional_running_routes"], 1)
        self.assertEqual(snapshot["sandboxes"]["stale_routes"], 0)

    def test_includes_recent_node_metric_samples(self) -> None:
        now = utc_now()
        heartbeat = build_heartbeat(
            job_id="job-1",
            node_id="node-1",
            active_sandboxes=2,
            capabilities=("sandbox", "image-cache", "disk-quota"),
            total_resources=ResourceQuantity(vcpu=8, memory_mb=16384, disk_mb=100_000),
            used_resources=ResourceQuantity(vcpu=3, memory_mb=4096, disk_mb=25_000),
            runtime_metrics=NodeRuntimeMetrics(
                collected_at=now,
                cpu_percent=37.5,
                cpu_vcpu=3.0,
                cpu_count=8,
                memory_total_mb=16384,
                memory_used_mb=4096,
                memory_available_mb=12288,
                memory_percent=25.0,
            ),
            now=now,
        )

        with TemporaryDirectory() as raw_dir:
            store = MetricsStore(Path(raw_dir) / "metrics.jsonl")
            from ucloud_sandboxes.metrics import record_node_heartbeat

            record_node_heartbeat(store, heartbeat)
            snapshot = build_metrics_snapshot(
                {"job-1": heartbeat},
                None,
                store.load_events(),
                heartbeat_ttl_seconds=120,
            )

        self.assertEqual(snapshot["nodes"]["samples"], 1)
        sample = snapshot["nodes"]["recent_samples"][0]
        self.assertEqual(sample["kind"], "node_heartbeat")
        self.assertEqual(sample["data"]["node_id"], "node-1")
        self.assertEqual(sample["data"]["active_sandboxes"], 2)
        self.assertEqual(sample["data"]["load"]["vcpu"], 0.375)
        self.assertEqual(sample["data"]["actual_usage"]["cpu_vcpu"], 3.0)
        self.assertEqual(sample["data"]["actual_usage"]["memory_percent"], 25.0)

    def test_builds_vm_lifecycle_summary(self) -> None:
        now = utc_now()
        with TemporaryDirectory() as raw_dir:
            store = MetricsStore(Path(raw_dir) / "metrics.jsonl")
            store.append(
                "vm_submitted",
                {
                    "job_id": "job-1",
                    "role": "sandbox",
                    "node_id": "node-1",
                    "product_id": "cpu-amd-zen5-16-vcpu",
                },
                timestamp=(now - timedelta(seconds=120)).isoformat(),
            )
            store.append(
                "vm_observed",
                {
                    "job_id": "job-1",
                    "role": "sandbox",
                    "state": "RUNNING",
                    "created_at": (now - timedelta(seconds=119)).isoformat(),
                    "started_at": (now - timedelta(seconds=90)).isoformat(),
                },
                timestamp=(now - timedelta(seconds=89)).isoformat(),
            )
            store.append(
                "vm_init_attempt",
                {
                    "job_id": "job-1",
                    "node_id": "node-1",
                    "role": "sandbox",
                    "status": "succeeded",
                    "attempts": 1,
                    "started_at": (now - timedelta(seconds=80)).isoformat(),
                    "finished_at": (now - timedelta(seconds=20)).isoformat(),
                    "duration_ms": 60_000,
                    "stage_duration_ms": 1000,
                    "run_duration_ms": 59_000,
                    "returncode": 0,
                },
                timestamp=(now - timedelta(seconds=20)).isoformat(),
            )
            store.append(
                "node_heartbeat",
                {
                    "job_id": "job-1",
                    "node_id": "node-1",
                    "heartbeat_updated_at": (now - timedelta(seconds=15)).isoformat(),
                },
                timestamp=(now - timedelta(seconds=15)).isoformat(),
            )
            store.append(
                "sandbox_scheduled",
                {
                    "job_id": "job-1",
                    "sandbox_id": "sandbox-1",
                    "scale_up_wait_ms": 112_000,
                },
                timestamp=(now - timedelta(seconds=8)).isoformat(),
            )

            snapshot = build_metrics_snapshot(
                {},
                None,
                store.load_events(),
                heartbeat_ttl_seconds=120,
            )

        lifecycle = snapshot["vm_lifecycle"]
        self.assertEqual(lifecycle["samples"], 1)
        item = lifecycle["items"][0]
        self.assertEqual(item["job_id"], "job-1")
        self.assertEqual(item["role"], "sandbox")
        self.assertEqual(item["submit_to_running_ms"], 30_000)
        self.assertEqual(item["running_to_first_heartbeat_ms"], 75_000)
        self.assertEqual(item["first_heartbeat_to_first_sandbox_ms"], 7_000)
        self.assertEqual(item["last_successful_init_duration_ms"], 60_000)
        self.assertEqual(item["first_sandbox_scale_up_wait_ms"], 112_000)


if __name__ == "__main__":
    unittest.main()
