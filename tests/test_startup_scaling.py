"""Scale for sustained work, without treating HTTP retries as VM demand."""

from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from tests.test_policy import node
from tests import test_control_plane as gateway_fixtures
from ucloud_sandboxes.cli import vm_init_options_for_job
from ucloud_sandboxes.config import DeploymentConfig
from ucloud_sandboxes.vm_init import render_vm_init_script
from ucloud_sandboxes.models import (
    LiveScaleSignals,
    NodeRuntimeMetrics,
    ResourceQuantity,
    SandboxDemand,
    SandboxPlacementRequest,
    ScalePolicy,
    utc_now,
)
from ucloud_sandboxes.policy import _nodes_for_unplaced_requests, evaluate_scale
from ucloud_sandboxes.routing import RoutingStore, sandbox_demand_from_routing_state


class StartupScalingTests(unittest.TestCase):
    def test_batched_placement_preserves_fragmented_disk_and_reusable_memory(self):
        policy = replace(
            self.policy,
            default_node_resources=ResourceQuantity(vcpu=4, memory_mb=4096, disk_mb=100),
        )
        requests = (
            SandboxPlacementRequest(
                resources=ResourceQuantity(vcpu=2, memory_mb=1024, disk_mb=30), count=6
            ),
            SandboxPlacementRequest(
                resources=ResourceQuantity(vcpu=2, memory_mb=1024, disk_mb=10), count=2
            ),
            SandboxPlacementRequest(
                resources=ResourceQuantity(vcpu=2, memory_mb=1024), count=1_000_000_000
            ),
        )
        self.assertEqual(
            _nodes_for_unplaced_requests(
                [], requests, policy, now=utc_now(), oldest_pending_seconds=0
            ),
            2,
        )

    def test_large_reservation_is_durable_and_policy_bounds_scale_out(self):
        with TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            gateway = gateway_fixtures._gateway_server(
                root, routing_file=root / "routes.sqlite"
            )
            fixture = gateway_fixtures.ControlPlaneTests()
            with gateway_fixtures._running_server(gateway) as base:
                for count in (256, 512, 1_000_000_000):
                    result = fixture._json_request(
                        base + "/v1/capacity/prepare",
                        method="POST",
                        payload={
                            "id": "large-reservation", "count": count,
                            "cpus": 2, "memory_mb": 1024, "disk_mb": 5184,
                        },
                    )
                    self.assertEqual(result["prepare"]["count"], count)
                    store = RoutingStore(root / "routes.sqlite")
                    self.assertEqual(store.prepared_capacity()[0].count, count)
                    # Planner effort must follow fleet capacity, not the
                    # caller's potentially enormous future workload count.
                    from ucloud_sandboxes import policy as policy_module
                    with patch.object(
                        policy_module, "dynamic_request_fits",
                        wraps=policy_module.dynamic_request_fits,
                    ) as fits:
                        decision = evaluate_scale([], store.pending_demand(), self.policy)
                    self.assertLess(fits.call_count, 100)
                    self.assertLessEqual(decision.creates, self.policy.max_create_per_cycle)
                    self.assertGreater(decision.creates, 0)

    def setUp(self):
        self.resources = ResourceQuantity(vcpu=32, memory_mb=98304, disk_mb=1_449_984)
        self.shape = ResourceQuantity(vcpu=2, memory_mb=1024, disk_mb=5184)
        self.policy = ScalePolicy(
            default_node_resources=self.resources,
            max_nodes=6,
            max_create_per_cycle=4,
            create_pressure_max_headroom_nodes=1,
        )
        # This queue already fits RAM. These tests isolate sustained startup
        # headroom, while test_memory_forecast_policy covers large cold bursts.
        self.low_memory = NodeRuntimeMetrics(collected_at=utc_now(),
            memory_total_mb=self.resources.memory_mb,
            memory_available_mb=self.resources.memory_mb - 8192,
            memory_working_set_mb=8192)
        self.ready = node("busy", active=8, total_resources=self.resources,
                          runtime_metrics=self.low_memory)
        self.signals = LiveScaleSignals(
            observation_samples=2,
            latest_observation_age_seconds=1,
            cpu_utilization=0.30,
        )
        self.demand = SandboxDemand(
            pending_count=8,
            oldest_capacity_pending_seconds=30,
            oldest_pending_seconds=30,
            placement_requests=(
                SandboxPlacementRequest(resources=self.shape, count=8),
            ),
        )

    def test_worker_bootstrap_uses_scheduler_startup_budget(self):
        config = DeploymentConfig.default()
        config = replace(
            config, policy=replace(config.policy, create_target_concurrency_per_node=3)
        )
        with patch(
            "ucloud_sandboxes.cli.read_bearer_token_source", return_value="test-token"
        ):
            options = vm_init_options_for_job(
                config,
                self.ready.job,
                "sandbox",
                package_spec="/tmp/package.tar.gz",
                package_sha256="a" * 64,
            )
        self.assertEqual(options.direct_max_concurrent_startups, 3)
        script = render_vm_init_script(options)
        self.assertIn("UCLOUD_DIRECT_MAX_CONCURRENT_STARTUPS=3", script)
        self.assertIn(
            "--max-concurrent-startups ${UCLOUD_DIRECT_MAX_CONCURRENT_STARTUPS}", script
        )
        self.assertIn(
            "UCLOUD_DIRECT_MAX_CONCURRENT_STARTUPS=$UCLOUD_DIRECT_MAX_CONCURRENT_STARTUPS",
            script,
        )

    def test_old_capacity_queue_adds_headroom_below_cpu_threshold(self):
        result = evaluate_scale(
            [self.ready], self.demand, self.policy, live_signals=self.signals
        )
        self.assertEqual(result.resource_deficit, ResourceQuantity())
        self.assertFalse(result.pressure_scale_up)
        self.assertTrue(result.create_pressure_scale_up)
        self.assertEqual(result.creates, 1)
        self.assertIn(
            "8 capacity request(s) queued for 30s", " ".join(result.reasons)
        )

    def test_short_stale_disabled_or_idle_queues_do_not_buy_headroom(self):
        for demand, policy, signals, ready in (
            (
                replace(self.demand, oldest_capacity_pending_seconds=29),
                self.policy,
                self.signals,
                self.ready,
            ),
            (
                self.demand,
                self.policy,
                replace(self.signals, latest_observation_age_seconds=100),
                self.ready,
            ),
            (
                self.demand,
                replace(self.policy, create_pressure_enabled=False),
                self.signals,
                self.ready,
            ),
            (
                self.demand,
                replace(self.policy, create_pressure_max_headroom_nodes=0),
                self.signals,
                self.ready,
            ),
            (
                self.demand,
                self.policy,
                self.signals,
                node("idle", total_resources=self.resources),
            ),
            (
                replace(
                    self.demand,
                    oldest_capacity_pending_seconds=0,
                    oldest_pending_seconds=600,
                ),
                self.policy,
                self.signals,
                self.ready,
            ),
        ):
            with self.subTest(
                demand=demand, policy=policy, signals=signals, ready=ready.job.id
            ):
                self.assertEqual(
                    evaluate_scale(
                        [ready], demand, policy, live_signals=signals
                    ).creates,
                    0,
                )

    def test_provisioning_credit_and_continued_busy_backlog(self):
        starting = node("starting",state="IN_QUEUE",fresh=False,
                        heartbeat_present=False,total_resources=self.resources)
        busy = node("busy-2",active=8,total_resources=self.resources,
                    runtime_metrics=self.low_memory)
        for extra, expected in ((starting,0),(busy,1)):
            with self.subTest(extra=extra.job.id):
                decision = evaluate_scale([self.ready,extra],self.demand,self.policy,
                                          live_signals=self.signals)
                self.assertEqual(decision.resource_deficit,ResourceQuantity())
                self.assertEqual(decision.creates,expected)
        # The newly requested headroom is credited on the next cycle while it
        # boots; the same queue cannot purchase it a second time.
        decision = evaluate_scale([self.ready,busy,starting],self.demand,self.policy,
                                  live_signals=self.signals)
        self.assertEqual(decision.creates,0)

    def test_preparations_and_noncapacity_errors_do_not_age_capacity_queue(self):
        now = utc_now()
        with TemporaryDirectory() as directory:
            store = RoutingStore(Path(directory) / "routes.sqlite")
            with patch(
                "ucloud_sandboxes.routing.utc_now",
                return_value=now - timedelta(seconds=90),
            ):
                store.upsert_prepared_capacity(
                    "warm", self.shape, count=1, ttl_seconds=600
                )
                store.upsert_pending(
                    "bad-image", self.shape, failure_reason="image_pull_http_500"
                )
            with patch(
                "ucloud_sandboxes.routing.utc_now",
                return_value=now - timedelta(seconds=5),
            ):
                store.upsert_pending("fresh", self.shape)
            with patch("ucloud_sandboxes.routing.utc_now", return_value=now):
                indexed = store.pending_demand()
                snapshot = sandbox_demand_from_routing_state(store.load(), now=now)
            self.assertEqual(indexed, snapshot)
            self.assertEqual(indexed.oldest_pending_seconds, 90)
            self.assertEqual(indexed.oldest_capacity_pending_seconds, 5)
            self.assertEqual(indexed.pending_count, 1)
            self.assertEqual(indexed.suppressed_pending_count, 1)
