from datetime import timedelta
from tempfile import TemporaryDirectory
from pathlib import Path
import json
import sqlite3
import unittest
from unittest.mock import patch

from ucloud_sandboxes.agent import build_heartbeat as _build_heartbeat
from ucloud_sandboxes.metrics import (
    GatewayBusySampler,
    MetricEvent,
    MetricsStore,
    build_live_scale_signals,
    build_metrics_snapshot,
    record_autoscaler_cycle,
)
from ucloud_sandboxes.models import (
    NodeRuntimeMetrics,
    ResourceQuantity,
    ScalePolicy,
    utc_now,
)
from ucloud_sandboxes.routing import (
    PendingSandboxDemand,
    ProgramRequestState,
    RoutingState,
    SandboxRoute,
)


def build_heartbeat(**kwargs):
    kwargs.setdefault("deployment_id", "test-deployment")
    return _build_heartbeat(**kwargs)


def sandbox_route(**values: object) -> SandboxRoute:
    values.setdefault("resources", ResourceQuantity())
    values.setdefault("spec", {"id": values.get("sandbox_id")})
    values.setdefault("state", "unknown")
    values.setdefault("generation", 1)
    values.setdefault("create_operation_id", "create-test-route")
    values.setdefault("spec_hash", "a" * 64)
    return SandboxRoute(**values)  # type: ignore[arg-type]


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

    def test_sqlite_store_enforces_logical_and_physical_byte_budget(self) -> None:
        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "metrics.sqlite"
            max_bytes = 128 * 1024
            store = MetricsStore(
                path,
                max_bytes=max_bytes,
                max_event_bytes=8 * 1024,
                max_events=10_000,
            )
            for index in range(200):
                store.append(
                    "bounded",
                    {"index": index, "padding": "x" * 2048},
                )

            events = store.load_events(max_events=10_000)
            physical_bytes = sum(
                candidate.stat().st_size
                for candidate in (path, path.with_name(path.name + "-wal"))
                if candidate.exists()
            )

        self.assertTrue(events)
        self.assertEqual(events[-1].data["index"], 199)
        self.assertLess(len(events), 200)
        self.assertLessEqual(physical_bytes, max_bytes)

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

    def test_image_materialization_queue_is_live_pressure(self) -> None:
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
                        "image_materialization_active_operations": 4,
                        "image_materialization_waiting_operations": 8,
                        "image_materialization_max_concurrent_operations": 4,
                    },
                },
            )
            for offset in (20, 10, 1)
        ]

        signals = build_live_scale_signals(events, ScalePolicy())

        self.assertEqual(signals.pressure_samples, 3)
        self.assertEqual(signals.image_materialization_queue_utilization, 1.0)

    def test_snapshot_publication_queue_is_diagnostic_not_scale_pressure(
        self,
    ) -> None:
        now = utc_now()
        events = [
            MetricEvent(
                timestamp=(now - timedelta(seconds=offset)).isoformat(),
                kind="node_heartbeat",
                data={
                    "active_workloads": 0,
                    "actual_usage": {
                        "storage_publication_active": 2,
                        "storage_publication_waiting": 20,
                        "storage_publication_limit": 2,
                    },
                },
            )
            for offset in (20, 10, 1)
        ]

        signals = build_live_scale_signals(events, ScalePolicy())

        self.assertEqual(signals.pressure_samples, 0)
        self.assertIsNone(signals.storage_queue_utilization)

    def test_healthy_live_observations_remain_visible_without_pressure(self) -> None:
        now = utc_now()
        events = [
            MetricEvent(
                timestamp=(now - timedelta(seconds=offset)).isoformat(),
                kind="node_heartbeat",
                data={
                    "active_workloads": 2,
                    "actual_usage": {
                        "cpu_percent": 20,
                        "memory_percent": 30,
                        "storage_active_operations": 0,
                        "storage_waiting_operations": 0,
                    },
                },
            )
            for offset in (20, 10, 1)
        ]

        signals = build_live_scale_signals(events, ScalePolicy())

        self.assertEqual(signals.observation_samples, 3)
        self.assertEqual(signals.latest_observation_age_seconds, 1)
        self.assertEqual(signals.pressure_samples, 0)
        self.assertEqual(signals.cpu_utilization, 0.2)
        self.assertEqual(signals.memory_utilization, 0.3)

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

    def test_gateway_busy_signals_are_aggregated_between_samples(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = MetricsStore(Path(raw_dir) / "metrics.sqlite")
            sampler = GatewayBusySampler(store, min_interval_seconds=60)

            with patch(
                "ucloud_sandboxes.metrics.time.monotonic",
                side_effect=[0.0, *([1.0] * 99), 61.0],
            ):
                emitted = [
                    sampler.record(
                        max_concurrent_sandbox_creates=32,
                    )
                    for _index in range(100)
                ]
                emitted.append(sampler.record(max_concurrent_sandbox_creates=32))
            events = store.load_events()

        self.assertEqual(emitted.count(True), 2)
        self.assertEqual(len(events), 2)
        self.assertEqual(events[0].kind, "sandbox_create_busy")
        self.assertEqual(events[0].data["outcome"], "gateway_busy")
        self.assertEqual(
            events[0].data["max_concurrent_sandbox_creates"],
            32,
        )
        self.assertEqual(events[0].data["aggregated_rejections"], 1)
        self.assertEqual(events[1].data["aggregated_rejections"], 100)

    def test_metrics_store_replaces_oversized_event_with_marker(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = MetricsStore(
                Path(raw_dir) / "metrics.sqlite",
                max_bytes=64 * 1024,
                max_event_bytes=160,
            )

            event = store.append("oversized", {"payload": "x" * 1024})
            loaded = store.load_events()

            self.assertTrue(event.data["metrics_payload_truncated"])
            self.assertGreater(event.data["original_bytes"], 160)
            self.assertEqual(loaded, [event])

    def test_autoscaler_cycle_bounds_wake_plan_and_exposes_policy(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = MetricsStore(Path(raw_dir) / "metrics.sqlite")
            record_autoscaler_cycle(
                store,
                cycle=8,
                result={
                    "decision": {},
                    "execute": True,
                    "controllerLockHeld": True,
                    "blockedCreateRoles": ["sandbox"],
                    "createdJobIds": ["created-1"],
                    "requestedStopJobIds": ["requested-1", "blocked-1"],
                    "stopJobIds": ["requested-1"],
                    "blockedStopJobIds": ["blocked-1"],
                    "blocked_storage_native_detach_stop_job_ids": ["blocked-1"],
                    "drainingJobIds": ["requested-1"],
                    "cancelingDrainJobIds": ["canceling-1"],
                    "canceledDrainJobIds": ["canceled-1"],
                    "drainReadyStopJobIds": ["ready-1"],
                    "unreachableReadyStopJobIds": ["unreachable-1"],
                    "destructive_stop_job_ids": ["destructive-1"],
                    "definitelyTerminatedJobIds": ["terminated-1"],
                    "drainResults": [
                        {
                            "jobId": "requested-1",
                            "role": "sandbox",
                            "action": "drain",
                            "requestSucceeded": True,
                            "heartbeatReady": False,
                            "cancellationAcknowledged": False,
                            "error": "",
                            "token": "must-not-be-persisted",
                        }
                    ],
                    "storage_native_detach_results": [
                        {
                            "job_id": "requested-1",
                            "sandbox_id": "sandbox-1",
                            "request_succeeded": False,
                            "error": "registry unavailable",
                            "sandbox": {"secret": "must-not-be-persisted"},
                        }
                    ],
                    "pending_delete_results": [
                        {
                            "job_id": "requested-1",
                            "sandbox_id": "sandbox-delete-1",
                            "delete_operation_id": "delete-operation-1",
                            "request_succeeded": True,
                            "error": "",
                            "deleted": {"secret": "must-not-be-persisted"},
                        }
                    ],
                    "orphanedMigrationReconciles": [
                        {
                            "migration_id": "migration-orphaned-1",
                            "sandbox_id": "sandbox-orphaned-1",
                            "phase": "complete",
                            "error": "sandbox route is absent",
                            "storage_snapshot": {"secret": "must-not-be-persisted"},
                        }
                    ],
                    "providerOperationResults": [
                        {
                            "operationId": "operation-1",
                            "kind": "stop",
                            "role": "sandbox",
                            "state": "accepted",
                            "jobIds": ["terminated-1"],
                            "source": "planned",
                            "error": "",
                            "request": {"secret": "must-not-be-persisted"},
                        }
                    ],
                    "programWakePlan": {
                        "mode": "action",
                        "queued": 300,
                        "placed": 150,
                        "unplaced_count": 150,
                        "placements": [
                            {"request_id": f"placed-{index}"} for index in range(150)
                        ],
                        "unplaced": [
                            {"request_id": f"unplaced-{index}"} for index in range(150)
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
            event.data["effective_policy"]["program_aware_autoscaling_enabled"]
        )
        execution = event.data["execution"]
        self.assertTrue(execution["controller_lock_held"])
        self.assertEqual(execution["blocked_create_roles"], ["sandbox"])
        scale_down = execution["scale_down"]
        self.assertEqual(scale_down["blocked_job_ids"], ["blocked-1"])
        self.assertEqual(scale_down["draining_job_ids"], ["requested-1"])
        self.assertEqual(scale_down["terminated_job_ids"], ["terminated-1"])
        self.assertFalse(scale_down["drain_attempts"][0]["heartbeat_ready"])
        self.assertEqual(
            scale_down["storage_detach_attempts"][0]["error"],
            "registry unavailable",
        )
        self.assertTrue(
            scale_down["pending_delete_attempts"][0]["request_succeeded"]
        )
        self.assertEqual(
            scale_down["pending_delete_attempts"][0]["delete_operation_id"],
            "delete-operation-1",
        )
        self.assertEqual(
            scale_down["orphaned_migration_reconciles"][0]["migration_id"],
            "migration-orphaned-1",
        )
        self.assertEqual(
            execution["provider_operations"][0]["job_ids"],
            ["terminated-1"],
        )
        self.assertNotIn("must-not-be-persisted", json.dumps(event.data))

    def test_snapshot_aggregates_live_routes_and_schedulable_demand(self) -> None:
        now = utc_now()
        heartbeat = build_heartbeat(
            job_id="job-1",
            node_id="node-1",
            active_sandboxes=1,
            node_url="http://node-1:8090",
            capabilities=("sandbox", "image-cache", "disk-quota"),
            total_resources=ResourceQuantity(vcpu=4, memory_mb=8192, disk_mb=100_000),
            used_resources=ResourceQuantity(vcpu=1, memory_mb=2048, disk_mb=10_000),
            now=now,
        )
        routing = RoutingState(
            sandboxes={
                "active": sandbox_route(
                    sandbox_id="active",
                    node_id="node-1",
                    job_id="job-1",
                    node_url="http://node-1:8090",
                    created_at=now.isoformat(),
                )
            },
            exec_sessions={},
            pending={
                "schedulable": PendingSandboxDemand(
                    sandbox_id="schedulable",
                    resources=ResourceQuantity(vcpu=2, memory_mb=4096),
                    created_at=now.isoformat(),
                    updated_at=now.isoformat(),
                ),
                "suppressed": PendingSandboxDemand(
                    sandbox_id="suppressed",
                    resources=ResourceQuantity(vcpu=8, memory_mb=16_384),
                    created_at=now.isoformat(),
                    updated_at=now.isoformat(),
                    failure_reason="image_pull_http_503",
                ),
            },
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

        self.assertEqual(
            (
                snapshot["nodes"]["fresh"],
                snapshot["nodes"]["sandbox_ready"],
                snapshot["sandboxes"]["running"],
                snapshot["sandboxes"]["routes_on_fresh_nodes"],
            ),
            (1, 1, 1, 1),
        )
        self.assertEqual(snapshot["sandboxes"]["pending"], 1)
        self.assertEqual(snapshot["sandboxes"]["pending_resources"]["vcpu"], 2.0)
        self.assertEqual(snapshot["sandboxes"]["suppressed_pending"], 1)
        self.assertEqual(
            snapshot["sandboxes"]["suppressed_pending_resources"]["vcpu"],
            8.0,
        )

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
                "new-one": sandbox_route(
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

    def test_portable_parked_route_is_not_stale_after_source_node_loss(self) -> None:
        routing = RoutingState(
            sandboxes={
                "portable": sandbox_route(
                    sandbox_id="portable",
                    node_id="lost-node",
                    job_id="lost-job",
                    node_url="http://lost-node:8090",
                    state="parked",
                    storage_schema="storage-native-v1",
                    snapshot_manifest_digest="sha256:" + "a" * 64,
                    snapshot_repository="snapshots",
                    snapshot_tag="portable",
                    storage_snapshot={"schema": "storage-native-v1"},
                )
            },
            exec_sessions={},
            pending={},
            image_builds={},
            prepared={},
            prepared_builders={},
        )

        snapshot = build_metrics_snapshot({}, routing, [], heartbeat_ttl_seconds=120)

        self.assertEqual(snapshot["sandboxes"]["active_routes"], 1)
        self.assertEqual(snapshot["sandboxes"]["portable_parked_routes"], 1)
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
            store = MetricsStore(Path(raw_dir) / "metrics.sqlite")
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
        node = snapshot["nodes"]["items"][0]
        self.assertEqual(
            sample["data"],
            {key: node[key] for key in sample["data"]},
        )

    def test_builds_vm_lifecycle_summary(self) -> None:
        now = utc_now()
        with TemporaryDirectory() as raw_dir:
            store = MetricsStore(Path(raw_dir) / "metrics.sqlite")
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

    def test_metrics_snapshot_does_not_embed_trace_storage(self) -> None:
        snapshot = build_metrics_snapshot(
            {},
            None,
            [],
            heartbeat_ttl_seconds=120,
        )

        self.assertNotIn("traces", snapshot)


if __name__ == "__main__":
    unittest.main()
