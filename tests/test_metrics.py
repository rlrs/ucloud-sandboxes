from datetime import timedelta
from tempfile import TemporaryDirectory
from pathlib import Path
import sqlite3
import unittest

from ucloud_sandboxes.agent import build_heartbeat
from ucloud_sandboxes.metrics import (
    GatewayBusyTraceSampler,
    MetricEvent,
    MetricsStore,
    build_live_scale_signals,
    build_metrics_snapshot,
    record_autoscaler_cycle,
    record_trace_span,
)
from ucloud_sandboxes.models import (
    NodeRuntimeMetrics,
    ResourceQuantity,
    ScalePolicy,
    utc_now,
)
from ucloud_sandboxes.routing import (
    ExecRoute,
    PendingImageBuildDemand,
    PendingSandboxDemand,
    ProgramRequestState,
    PreparedBuilderDemand,
    PreparedCapacityDemand,
    RoutingState,
    SandboxRoute,
)


class MetricsTests(unittest.TestCase):
    def test_sqlite_store_filters_indexed_recent_events(self) -> None:
        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "metrics.sqlite"
            store = MetricsStore(path)
            store.append("heartbeat", {"index": 1})
            store.append("trace", {"index": 2})
            store.append("heartbeat", {"index": 3})

            events = store.load_events(
                max_events=10,
                kinds=("heartbeat",),
                since_seconds=60,
            )
            mode = path.stat().st_mode & 0o777

        self.assertEqual([event.data["index"] for event in events], [1, 3])
        self.assertEqual(mode, 0o600)

    def test_sqlite_writer_collision_does_not_fail_product_path(self) -> None:
        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "metrics.sqlite"
            store = MetricsStore(path)
            blocker = sqlite3.connect(path)
            blocker.execute("BEGIN IMMEDIATE")
            store.append("heartbeat", {"index": 1})
            blocker.rollback()
            blocker.close()

            store.append("heartbeat", {"index": 2})
            events = store.load_events()

        self.assertEqual(
            [event.kind for event in events],
            ["metrics_dropped_events", "heartbeat"],
        )
        self.assertEqual(events[0].data["count"], 1)
        self.assertEqual(events[1].data["index"], 2)

    def test_builds_live_pressure_and_provisioning_signals(self) -> None:
        now = utc_now()
        heartbeat_data = {
            "job_id": "job-1",
            "active_workloads": 1,
            "actual_usage": {
                "cpu_percent": 82.0,
                "memory_percent": 70.0,
                "memory_psi_full_avg10": 0.0,
                "storage_active_operations": 1,
                "storage_waiting_operations": 0,
                "storage_max_concurrent_operations": 8,
            },
        }
        events = [
            MetricEvent(
                timestamp=(now - timedelta(seconds=70)).isoformat(),
                kind="vm_submitted",
                data={"job_id": "job-1", "role": "sandbox"},
            ),
            *[
                MetricEvent(
                    timestamp=(now - timedelta(seconds=offset)).isoformat(),
                    kind="node_heartbeat",
                    data=heartbeat_data,
                )
                for offset in (20, 10, 1)
            ],
            MetricEvent(
                timestamp=now.isoformat(),
                kind="sandbox_scheduled",
                data={
                    "job_id": "job-1",
                    "scale_up_wait_ms": 72_000,
                },
            ),
        ]

        signals = build_live_scale_signals(events, ScalePolicy())

        self.assertEqual(signals.pressure_samples, 3)
        self.assertEqual(signals.cpu_utilization, 0.82)
        self.assertEqual(signals.provisioning_samples, 1)
        self.assertGreaterEqual(signals.provisioning_p95_seconds or 0, 49)
        self.assertEqual(signals.scale_up_wait_p95_seconds, 72.0)

    def test_builds_gateway_create_pressure_signal(self) -> None:
        now = utc_now()
        events = [
            MetricEvent(
                timestamp=(now - timedelta(seconds=offset)).isoformat(),
                kind="trace_span",
                data={
                    "name": "gateway.sandbox_create",
                    "attributes": {
                        "outcome": "gateway_busy",
                        "aggregated_rejections": rejections,
                        "max_concurrent_sandbox_creates": 32,
                    },
                },
            )
            for offset, rejections in ((10, 4), (1, 7))
        ]

        signals = build_live_scale_signals(events, ScalePolicy())

        self.assertEqual(signals.create_pressure_samples, 2)
        self.assertEqual(signals.sandbox_create_rejections, 11)
        self.assertEqual(signals.sandbox_create_limit, 32)
        self.assertLessEqual(signals.latest_create_pressure_age_seconds or 0, 2)

    def test_rootfs_export_queue_is_live_pressure(self) -> None:
        now = utc_now()
        events = [
            MetricEvent(
                timestamp=(now - timedelta(seconds=offset)).isoformat(),
                kind="node_heartbeat",
                data={
                    "active_workloads": 4,
                    "actual_usage": {
                        "cpu_percent": 2,
                        "memory_percent": 3,
                        "rootfs_export_active_operations": 4,
                        "rootfs_export_waiting_operations": 8,
                        "rootfs_export_max_concurrent_operations": 4,
                    },
                },
            )
            for offset in (20, 10, 1)
        ]

        signals = build_live_scale_signals(events, ScalePolicy())

        self.assertEqual(signals.pressure_samples, 3)
        self.assertEqual(signals.rootfs_export_queue_utilization, 1.0)

    def test_busy_rootfs_slots_without_waiters_are_not_pressure(self) -> None:
        now = utc_now()
        events = [
            MetricEvent(
                timestamp=(now - timedelta(seconds=offset)).isoformat(),
                kind="node_heartbeat",
                data={
                    "active_workloads": 3,
                    "actual_usage": {
                        "cpu_percent": 2,
                        "memory_percent": 3,
                        "rootfs_export_active_operations": 3,
                        "rootfs_export_waiting_operations": 0,
                        "rootfs_export_max_concurrent_operations": 4,
                    },
                },
            )
            for offset in (20, 10, 1)
        ]

        signals = build_live_scale_signals(events, ScalePolicy())

        self.assertEqual(signals.pressure_samples, 0)
        self.assertIsNone(signals.rootfs_export_queue_utilization)

    def test_snapshot_uses_precomputed_exec_session_count(self) -> None:
        snapshot = build_metrics_snapshot(
            {},
            RoutingState({}, {}, {}, {}),
            [],
            heartbeat_ttl_seconds=120,
            exec_session_count=2_000,
        )

        self.assertEqual(snapshot["exec"]["sessions"], 2_000)

    def test_snapshot_exposes_aging_first_program_wake_queue(self) -> None:
        now = utc_now()
        requests = [
            ProgramRequestState(
                request_id="newer",
                rollout_id="rollout-2",
                sandbox_id="sandbox-2",
                sandbox_generation=1,
                state="ready_to_wake",
                resources=ResourceQuantity(vcpu=2, memory_mb=2048, disk_mb=4096),
                accepted_at=(now - timedelta(seconds=30)).isoformat(),
                response_ready_at=(now - timedelta(seconds=5)).isoformat(),
                updated_at=(now - timedelta(seconds=5)).isoformat(),
            ),
            ProgramRequestState(
                request_id="older",
                rollout_id="rollout-1",
                sandbox_id="sandbox-1",
                sandbox_generation=1,
                state="ready_to_wake",
                resources=ResourceQuantity(vcpu=1, memory_mb=1024, disk_mb=2048),
                accepted_at=(now - timedelta(seconds=60)).isoformat(),
                response_ready_at=(now - timedelta(seconds=20)).isoformat(),
                wake_completed_at=(now - timedelta(seconds=10)).isoformat(),
                updated_at=(now - timedelta(seconds=10)).isoformat(),
            ),
            ProgramRequestState(
                request_id="waiting",
                rollout_id="rollout-3",
                sandbox_id="sandbox-3",
                sandbox_generation=1,
                state="model_wait",
                resources=ResourceQuantity(vcpu=4, memory_mb=8192, disk_mb=16384),
                accepted_at=(now - timedelta(seconds=40)).isoformat(),
                updated_at=(now - timedelta(seconds=40)).isoformat(),
            ),
        ]

        snapshot = build_metrics_snapshot(
            {},
            RoutingState({}, {}, {}, {}),
            [],
            heartbeat_ttl_seconds=120,
            program_requests=requests,
        )

        programs = snapshot["programs"]
        self.assertEqual(programs["states"]["model_wait"], 1)
        self.assertEqual(programs["states"]["ready_to_wake"], 2)
        self.assertEqual(
            programs["shadow_wake_queue"][0]["request_id"],
            "older",
        )
        self.assertEqual(
            programs["resources"]["ready_to_wake"]["memory_mb"],
            3072,
        )
        self.assertEqual(programs["response_to_wake_p95_ms"], 10_000)

    def test_gateway_busy_traces_are_aggregated_between_samples(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = MetricsStore(Path(raw_dir) / "metrics.jsonl")
            sampler = GatewayBusyTraceSampler(store, min_interval_seconds=60)

            emitted = [
                sampler.record(
                    trace_id=f"busy-{index}",
                    max_concurrent_sandbox_creates=32,
                )
                for index in range(100)
            ]
            events = store.load_events()

        self.assertEqual(emitted.count(True), 1)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].kind, "trace_span")
        self.assertEqual(events[0].data["attributes"]["outcome"], "gateway_busy")
        self.assertEqual(
            events[0].data["attributes"]["max_concurrent_sandbox_creates"],
            32,
        )

    def test_metrics_state_file_is_owner_only(self) -> None:
        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "metrics.jsonl"
            store = MetricsStore(path)

            store.append("sensitive", {"token": "redacted-by-operator-policy"})

            self.assertEqual(path.stat().st_mode & 0o777, 0o600)

    def test_metrics_store_rotates_and_bounds_output(self) -> None:
        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "metrics.jsonl"
            store = MetricsStore(
                path,
                max_bytes=220,
                max_files=2,
                max_event_bytes=200,
            )

            for index in range(12):
                store.append("bounded", {"index": index, "padding": "x" * 24})

            retained = store.load_events(max_events=100)
            segments = [
                candidate
                for candidate in path.parent.glob("metrics.jsonl.*")
                if candidate.name.removeprefix("metrics.jsonl.").isdigit()
            ]

            self.assertLessEqual(len(segments), 2)
            self.assertTrue(retained)
            self.assertEqual(retained[-1].data["index"], 11)
            self.assertTrue(all(candidate.stat().st_size <= 220 for candidate in segments))
            self.assertLessEqual(path.stat().st_size, 220)

    def test_metrics_store_replaces_oversized_event_with_marker(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = MetricsStore(
                Path(raw_dir) / "metrics.jsonl",
                max_bytes=512,
                max_event_bytes=160,
            )

            event = store.append("oversized", {"payload": "x" * 1024})
            loaded = store.load_events()

            self.assertTrue(event.data["metrics_payload_truncated"])
            self.assertGreater(event.data["original_bytes"], 160)
            self.assertEqual(loaded, [event])

    def test_load_events_returns_recent_tail(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = MetricsStore(Path(raw_dir) / "metrics.jsonl")
            for index in range(20):
                store.append("event", {"index": index})

            events = store.load_events(max_events=5)

        self.assertEqual(
            [event.data["index"] for event in events], [15, 16, 17, 18, 19]
        )

    def test_autoscaler_cycle_records_build_warm_fields(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = MetricsStore(Path(raw_dir) / "metrics.jsonl")
            record_autoscaler_cycle(
                store,
                cycle=7,
                result={
                    "decision": {"reasons": ["disk headroom below demand"]},
                    "builderDecision": {"reasons": ["no pending builds"]},
                    "pendingImageBuilds": 1,
                    "activeImageBuilds": 2,
                    "preparedBuilderCount": 0,
                    "buildWarmSandboxResources": {
                        "vcpu": 16.0,
                        "memory_mb": 32768,
                        "disk_mb": 204800,
                    },
                },
            )

            event = store.load_events()[0]

        self.assertEqual(event.kind, "autoscaler_cycle")
        self.assertEqual(event.data["pending_image_builds"], 1)
        self.assertEqual(event.data["active_image_builds"], 2)
        self.assertEqual(event.data["build_warm_sandbox_resources"]["vcpu"], 16.0)
        self.assertEqual(event.data["reasons"], ["disk headroom below demand"])
        self.assertEqual(event.data["builder_reasons"], ["no pending builds"])

    def test_autoscaler_cycle_bounds_wake_plan_and_exposes_policy(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = MetricsStore(Path(raw_dir) / "metrics.jsonl")
            record_autoscaler_cycle(
                store,
                cycle=8,
                result={
                    "decision": {},
                    "programWakePlan": {
                        "mode": "action",
                        "queued": 300,
                        "placed": 150,
                        "unplaced_count": 150,
                        "placements": [
                            {"request_id": f"placed-{index}"}
                            for index in range(150)
                        ],
                        "unplaced": [
                            {"request_id": f"unplaced-{index}"}
                            for index in range(150)
                        ],
                    },
                    "effectivePolicy": {
                        "program_aware_autoscaling_enabled": True,
                        "model_wait_capacity_weight": 0.25,
                    },
                },
            )

            event = store.load_events()[0]

        wake_plan = event.data["program_wake_plan"]
        self.assertEqual(len(wake_plan["placements"]), 100)
        self.assertEqual(len(wake_plan["unplaced"]), 100)
        self.assertEqual(wake_plan["placements_truncated"], 50)
        self.assertEqual(wake_plan["unplaced_truncated"], 50)
        self.assertTrue(
            event.data["effective_policy"][
                "program_aware_autoscaling_enabled"
            ]
        )

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
                ),
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
                ),
                "failed-image": PendingSandboxDemand(
                    sandbox_id="failed-image",
                    resources=ResourceQuantity(
                        vcpu=8,
                        memory_mb=16_384,
                        disk_mb=32_768,
                    ),
                    created_at=(now - timedelta(seconds=45)).isoformat(),
                    updated_at=now.isoformat(),
                    attempts=3,
                    failure_reason="image_pull_http_503",
                ),
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
        self.assertEqual(snapshot["nodes"]["sandbox_ready"], 1)
        self.assertEqual(snapshot["nodes"]["sandbox_draining"], 0)
        self.assertEqual(snapshot["nodes"]["sandbox_admission_closed"], 0)
        self.assertEqual(snapshot["nodes"]["samples"], 0)
        self.assertEqual(
            snapshot["nodes"]["items"][0]["actual_usage"]["cpu_percent"], 20.0
        )
        self.assertEqual(
            snapshot["resources"]["sandbox"]["actual_usage"]["cpu_vcpu"], 0.8
        )
        self.assertEqual(snapshot["resources"]["sandbox"]["load"]["vcpu"], 0.25)
        self.assertEqual(snapshot["sandboxes"]["running"], 1)
        self.assertEqual(snapshot["sandboxes"]["active_routes"], 2)
        self.assertEqual(snapshot["sandboxes"]["states"], {"unknown": 2})
        self.assertEqual(snapshot["sandboxes"]["routes_on_fresh_nodes"], 1)
        self.assertEqual(snapshot["sandboxes"]["provisional_running_routes"], 0)
        self.assertEqual(snapshot["sandboxes"]["stale_routes"], 1)
        self.assertEqual(snapshot["sandboxes"]["pending"], 1)
        self.assertEqual(snapshot["sandboxes"]["pending_resources"]["vcpu"], 2.0)
        self.assertEqual(snapshot["sandboxes"]["pending_attempts"], 2)
        self.assertEqual(snapshot["sandboxes"]["suppressed_pending"], 1)
        self.assertEqual(
            snapshot["sandboxes"]["suppressed_pending_resources"]["vcpu"],
            8.0,
        )
        self.assertEqual(snapshot["sandboxes"]["suppressed_pending_attempts"], 3)
        self.assertEqual(snapshot["capacity"]["prepared"], 1)
        self.assertEqual(snapshot["capacity"]["prepared_sandboxes"], 4)
        self.assertEqual(snapshot["capacity"]["prepared_resources"]["vcpu"], 4.0)
        self.assertEqual(snapshot["exec"]["sessions"], 1)
        self.assertEqual(snapshot["images"]["pending_builds"], 1)
        self.assertEqual(snapshot["builders"]["prepared_builders"], 1)
        self.assertEqual(
            snapshot["builders"]["items"][0]["prepare_id"], "builder-prep-1"
        )
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
        self.assertEqual(item["running_to_first_init_attempt_ms"], 10_000)
        self.assertEqual(item["running_to_first_heartbeat_ms"], 75_000)
        self.assertEqual(item["first_init_attempt_to_first_heartbeat_ms"], 65_000)
        self.assertEqual(item["first_heartbeat_to_first_sandbox_ms"], 7_000)
        self.assertEqual(item["last_successful_init_duration_ms"], 60_000)
        self.assertEqual(item["last_successful_package_stage_ms"], 1_000)
        self.assertEqual(item["last_successful_remote_init_ms"], 59_000)
        self.assertEqual(item["first_sandbox_scale_up_wait_ms"], 112_000)

    def test_builds_trace_summary_from_spans(self) -> None:
        now = utc_now()
        with TemporaryDirectory() as raw_dir:
            store = MetricsStore(Path(raw_dir) / "metrics.jsonl")
            record_trace_span(
                store,
                trace_id="trace-1",
                span_id="root",
                name="gateway.sandbox_create",
                started_at=(now - timedelta(seconds=2)).isoformat(),
                finished_at=now.isoformat(),
                duration_ms=2000,
                attributes={"sandbox_id": "sandbox-1"},
            )
            record_trace_span(
                store,
                trace_id="trace-1",
                span_id="node",
                parent_span_id="root",
                name="gateway.sandbox_proxy_create",
                started_at=(now - timedelta(seconds=1)).isoformat(),
                finished_at=now.isoformat(),
                duration_ms=1000,
                attributes={"node_timings": {"total_ms": 900}},
            )

            snapshot = build_metrics_snapshot(
                {},
                None,
                store.load_events(),
                heartbeat_ttl_seconds=120,
            )

        traces = snapshot["traces"]
        self.assertEqual(traces["span_count"], 2)
        self.assertEqual(traces["recent"][0]["trace_id"], "trace-1")
        self.assertEqual(traces["recent"][0]["duration_ms"], 2000)
        self.assertEqual(
            traces["recent"][0]["spans"][1]["name"], "gateway.sandbox_proxy_create"
        )


if __name__ == "__main__":
    unittest.main()
