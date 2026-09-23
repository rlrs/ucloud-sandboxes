from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from time import monotonic
import unittest

from ucloud_sandboxes import control_plane
from ucloud_sandboxes.config import DeploymentConfig
from ucloud_sandboxes.capabilities import RUNTIME_COMPATIBILITY_CAPABILITY_PREFIX
from ucloud_sandboxes.consolidation import can_consolidate_wake
from ucloud_sandboxes.control_state import ControlStateStore
from ucloud_sandboxes.metrics import MetricsStore
from ucloud_sandboxes.models import (
    NodeHeartbeat,
    NodeRuntimeMetrics,
    ResourceQuantity,
    ScalePolicy,
    utc_now,
)
from ucloud_sandboxes.routing import RoutingStore
from ucloud_sandboxes.deployment import package_version
from tests.test_control_plane import _portable_snapshot, _sandbox_route, _prepare_wake_route


class ConsolidationTests(unittest.TestCase):
    def setUp(self):
        self.now = utc_now()
        self.policy = replace(ScalePolicy(), parked_wake_consolidation_enabled=True)
        self.shape = ResourceQuantity(vcpu=2, memory_mb=1024, disk_mb=2048)
        self.source = self.node("200", active=1)
        self.destination = self.node("100", active=3)

    def node(self, job, *, active):
        return NodeHeartbeat(
            job_id=job,
            node_id="node-" + job,
            node_url="http://node-" + job + ":8090",
            deployment_id="test-deployment",
            updated_at=self.now,
            agent_version=package_version(),
            active_sandboxes=active,
            inventory_complete=True,
            resources_known=True,
            capabilities=(
                "sandbox",
                "disk-quota",
                "storage-native-v1",
                "sandbox-migrate-storage-native-v1",
                "hibernate-local-v2",
            ),
            total_resources=ResourceQuantity(vcpu=32, memory_mb=98304, disk_mb=100000),
            runtime_metrics=NodeRuntimeMetrics(
                collected_at=self.now,
                cpu_count=32,
                cpu_percent=10,
                memory_total_mb=98304,
                memory_available_mb=90000,
                memory_psi_full_avg10=0,
                storage_hard_capacity_mb=100000,
                storage_max_concurrent_operations=8,
            ),
        )

    def test_monotonic_packing_and_headroom(self):
        self.assertTrue(
            can_consolidate_wake(
                self.source, self.destination, self.shape, self.policy, now=self.now
            )
        )
        for destination in (
            replace(self.destination, job_id="300"),
            replace(self.destination, active_sandboxes=0),
            replace(self.destination, draining=True),
            replace(self.destination, inventory_complete=False),
            replace(self.destination, updated_at=self.now - timedelta(seconds=31)),
            replace(self.destination, active_sandbox_creates=1),
        ):
            with self.subTest(destination=destination):
                self.assertFalse(
                    can_consolidate_wake(
                        self.source, destination, self.shape, self.policy, now=self.now
                    )
                )
        self.assertFalse(
            can_consolidate_wake(
                self.source, self.destination, self.shape, ScalePolicy(), now=self.now
            )
        )
        self.assertFalse(
            can_consolidate_wake(
                replace(self.source, active_sandboxes=4),
                self.destination,
                self.shape,
                self.policy,
                now=self.now,
            )
        )

    def test_unknown_stale_and_pressure_metrics_prevent_optional_moves(self):
        for changes in (
            {"cpu_percent": None},
            {"cpu_percent": float("nan")},
            {"cpu_percent": 65},
            {"memory_available_mb": 10000},
            {"memory_psi_full_avg10": 5},
            {"memory_psi_full_avg10": None},
            {"storage_error_volumes": 1},
            {"storage_waiting_operations": 1},
            {"storage_active_operations": 6},
            {"collected_at": self.now - timedelta(seconds=31)},
        ):
            with self.subTest(changes=changes):
                dest = replace(
                    self.destination,
                    runtime_metrics=replace(
                        self.destination.runtime_metrics, **changes
                    ),
                )
                self.assertFalse(
                    can_consolidate_wake(
                        self.source, dest, self.shape, self.policy, now=self.now
                    )
                )
        src = replace(
            self.source,
            runtime_metrics=replace(self.source.runtime_metrics, cpu_percent=36),
        )
        self.assertFalse(
            can_consolidate_wake(
                src, self.destination, self.shape, self.policy, now=self.now
            )
        )

    def test_optional_packing_does_not_move_to_worse_io_or_reclaim_pressure(self):
        for signal in (
            "io_psi_some_avg10",
            "io_psi_full_avg10",
            "memory_psi_some_avg10",
        ):
            source = replace(
                self.source,
                runtime_metrics=replace(
                    self.source.runtime_metrics,
                    **{signal: 5},
                ),
            )
            busy = replace(
                self.destination,
                runtime_metrics=replace(
                    self.destination.runtime_metrics,
                    **{signal: 36},
                ),
            )
            quiet = replace(
                self.destination,
                runtime_metrics=replace(
                    self.destination.runtime_metrics,
                    **{signal: 2},
                ),
            )
            self.assertFalse(
                can_consolidate_wake(
                    source,
                    busy,
                    self.shape,
                    self.policy,
                    now=self.now,
                )
            )
            self.assertTrue(
                can_consolidate_wake(
                    source,
                    quiet,
                    self.shape,
                    self.policy,
                    now=self.now,
                )
            )

    def test_file_backed_resident_memory_is_not_free_consolidation_capacity(self):
        destination = replace(
            self.destination,
            runtime_metrics=replace(
                self.destination.runtime_metrics,
                memory_working_set_mb=87_000,
                memory_available_mb=90_000,
            ),
        )
        self.assertFalse(can_consolidate_wake(
            self.source, destination, self.shape, self.policy, now=self.now,
        ))

    def test_similarly_io_pressured_nodes_do_not_consolidate(self):
        source = replace(
            self.source,
            runtime_metrics=replace(
                self.source.runtime_metrics, io_psi_full_avg10=50,
            ),
        )
        for destination_psi in (50, 40, self.policy.max_io_psi_full_avg10, float("nan")):
            with self.subTest(destination_psi=destination_psi):
                destination = replace(
                    self.destination,
                    runtime_metrics=replace(
                        self.destination.runtime_metrics,
                        io_psi_full_avg10=destination_psi,
                    ),
                )
                self.assertFalse(can_consolidate_wake(
                    source, destination, self.shape, self.policy, now=self.now,
                ))
        healthy_destination = replace(
            self.destination,
            runtime_metrics=replace(
                self.destination.runtime_metrics, io_psi_full_avg10=2,
            ),
        )
        self.assertTrue(can_consolidate_wake(
            source, healthy_destination, self.shape, self.policy, now=self.now,
        ))

    def setup_handler(self, root):
        snapshot = _portable_snapshot("parked")
        self.destination = replace(
            self.destination,
            capabilities=(
                *(
                    cap
                    for cap in self.destination.capabilities
                    if not cap.startswith(RUNTIME_COMPATIBILITY_CAPABILITY_PREFIX)
                ),
                RUNTIME_COMPATIBILITY_CAPABILITY_PREFIX
                + snapshot.manifest.runtime.node_compatibility_sha256,
            ),
        )
        routing = RoutingStore(root / "routes.sqlite")
        route = routing.upsert_sandbox(
            _sandbox_route(
                sandbox_id="parked",
                node_id=self.source.node_id,
                job_id=self.source.job_id,
                node_url=self.source.node_url,
                resources=snapshot.manifest.spec.requested_resources(),
                spec=snapshot.manifest.spec.to_dict(),
                state="parked",
                storage_schema="storage-native-v1",
                snapshot_manifest_digest=snapshot.publication.manifest_digest,
                snapshot_repository=snapshot.publication.repository,
                snapshot_tag=snapshot.publication.tag,
                storage_snapshot=snapshot.to_dict(),
            )
        )
        store = ControlStateStore(root / "control.sqlite")
        store.upsert_heartbeat(self.source)
        store.upsert_heartbeat(
            replace(
                self.destination,
                cached_images=(route.spec["image"],),
                cached_images_known=True,
            )
        )

        class Handler(control_plane.ControlPlaneHandler):
            pass

        handler = object.__new__(Handler)
        handler.store = store
        handler.routing_store = routing
        handler.heartbeat_ttl_seconds = 120
        handler.wake_consolidation_policy = self.policy
        handler.metrics_store = MetricsStore(root / "metrics.sqlite")
        handler._write_json = lambda *a, **kw: None
        handler._prepare_and_advance_sandbox_migration = lambda migration, **kw: migration
        return handler, route

    def test_gateway_reserves_once_and_retry_resumes_even_when_local_fits(self):
        with TemporaryDirectory() as raw:
            handler, route = self.setup_handler(Path(raw))
            self.assertIsNone(_prepare_wake_route(handler, route))
            migrations = handler.routing_store.sandbox_migrations(active_only=True)
            self.assertEqual(len(migrations), 1)
            self.assertEqual(migrations[0].destination_job_id, self.destination.job_id)
            self.assertTrue(migrations[0].migration_id.startswith("consolidate-wake-"))
            self.assertIsNone(_prepare_wake_route(handler, route))
            self.assertEqual(
                handler.routing_store.sandbox_migrations(active_only=True), migrations
            )
            self.assertIsNone(
                handler._select_migration_destination(
                    route,
                    requested_node_id="",
                    require_active_resources=True,
                    consolidation_source=self.source,
                )
            )
            self.assertGreater(type(handler).wake_consolidation_next_at, monotonic())

    def test_gateway_falls_back_to_local_without_safe_consolidation(self):
        for condition in (
            "disabled",
            "cold",
            "busy",
            "cooldown",
            "disk",
            "unpublished",
            "inflight",
        ):
            with self.subTest(condition=condition), TemporaryDirectory() as raw:
                handler, route = self.setup_handler(Path(raw))
                if condition == "disabled":
                    handler.wake_consolidation_policy = ScalePolicy()
                elif condition == "cold":
                    handler.store.upsert_heartbeat(self.destination)
                elif condition == "busy":
                    handler.store.upsert_heartbeat(
                        replace(
                            self.destination,
                            runtime_metrics=replace(
                                self.destination.runtime_metrics, cpu_percent=95
                            ),
                        )
                    )
                elif condition == "cooldown":
                    type(handler).wake_consolidation_next_at = monotonic() + 60
                elif condition == "disk":
                    dest = replace(
                        self.destination,
                        cached_images=(route.spec["image"],),
                        used_resources=ResourceQuantity(disk_mb=99999),
                    )
                    handler.store.upsert_heartbeat(dest)
                elif condition == "inflight":
                    handler.routing_store.upsert_sandbox(
                        _sandbox_route(
                            sandbox_id="other-wake",
                            node_id=self.destination.node_id,
                            job_id=self.destination.job_id,
                            node_url=self.destination.node_url,
                            spec={"id": "other-wake", "image": route.spec["image"]},
                            state="waking",
                        )
                    )
                elif condition == "unpublished":
                    route = handler.routing_store.upsert_sandbox(
                        replace(route, snapshot_manifest_digest="", storage_snapshot={})
                    )
                selected = _prepare_wake_route(handler, route)
                self.assertEqual(selected.job_id, self.source.job_id)
                self.assertEqual(selected.state, "waking")
                self.assertEqual(handler.routing_store.sandbox_migrations(), [])

    def test_legacy_config_keeps_consolidation_disabled(self):
        raw = DeploymentConfig.default().to_dict()
        raw["policy"].pop("parked_wake_consolidation_enabled")
        self.assertFalse(
            DeploymentConfig.from_dict(raw).policy.parked_wake_consolidation_enabled
        )
