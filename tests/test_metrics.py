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
        self.assertEqual(snapshot["sandboxes"]["active_routes"], 1)
        self.assertEqual(snapshot["sandboxes"]["pending"], 1)
        self.assertEqual(snapshot["sandboxes"]["pending_resources"]["vcpu"], 2.0)
        self.assertEqual(snapshot["sandboxes"]["pending_attempts"], 2)
        self.assertEqual(snapshot["capacity"]["prepared"], 1)
        self.assertEqual(snapshot["capacity"]["prepared_sandboxes"], 4)
        self.assertEqual(snapshot["capacity"]["prepared_resources"]["vcpu"], 4.0)
        self.assertEqual(snapshot["exec"]["sessions"], 1)
        self.assertEqual(snapshot["images"]["pending_builds"], 1)
        self.assertEqual(snapshot["scale_up"]["samples"], 1)
        self.assertEqual(snapshot["scale_up"]["last_ms"], 12_000)

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


if __name__ == "__main__":
    unittest.main()
