from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from tempfile import TemporaryDirectory
from pathlib import Path
import json
import unittest

from ucloud_sandboxes.models import ResourceQuantity, utc_now
from ucloud_sandboxes.routing import (
    ExecRoute,
    PENDING_DEMAND_TTL_SECONDS,
    PendingImageBuildDemand,
    PendingSandboxDemand,
    PreparedBuilderDemand,
    PreparedCapacityDemand,
    RoutingState,
    RoutingStore,
    SandboxRoute,
)


class RoutingStoreTests(unittest.TestCase):
    def test_concurrent_writes_preserve_valid_state(self) -> None:
        with TemporaryDirectory() as raw_dir:
            route_file = Path(raw_dir) / "routes.json"

            def write(index: int) -> None:
                store = RoutingStore(route_file)
                store.upsert_pending(
                    f"pending-{index}",
                    ResourceQuantity(vcpu=1.0, memory_mb=1024, disk_mb=2048),
                )
                store.upsert_sandbox(
                    SandboxRoute(
                        sandbox_id=f"sandbox-{index}",
                        node_id="node-1",
                        job_id="job-1",
                        node_url="http://node-1:8090",
                        resources=ResourceQuantity(
                            vcpu=1.0, memory_mb=1024, disk_mb=2048
                        ),
                    )
                )

            with ThreadPoolExecutor(max_workers=16) as executor:
                list(executor.map(write, range(32)))

            state = RoutingStore(route_file).load()

        self.assertEqual(len(state.pending), 32)
        self.assertEqual(len(state.sandboxes), 32)
        self.assertEqual(
            state.pending["pending-0"].resources,
            ResourceQuantity(vcpu=1.0, memory_mb=1024, disk_mb=2048),
        )

    def test_reconcile_sandboxes_for_node_removes_missing_node_routes(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = RoutingStore(Path(raw_dir) / "routes.sqlite")
            now = utc_now()
            old = (now - timedelta(seconds=60)).isoformat()
            store.save(
                RoutingState(
                    sandboxes={
                        "stale-one": SandboxRoute(
                            sandbox_id="stale-one",
                            node_id="node-1",
                            job_id="job-1",
                            node_url="http://node-1:8090",
                            resources=ResourceQuantity(
                                vcpu=1, memory_mb=512, disk_mb=1024
                            ),
                            created_at=old,
                            updated_at=old,
                        ),
                        "other-node": SandboxRoute(
                            sandbox_id="other-node",
                            node_id="node-2",
                            job_id="job-2",
                            node_url="http://node-2:8090",
                            resources=ResourceQuantity(
                                vcpu=1, memory_mb=512, disk_mb=1024
                            ),
                            created_at=old,
                            updated_at=old,
                        ),
                    },
                    exec_sessions={
                        "exec-stale": ExecRoute(
                            session_id="exec-stale",
                            sandbox_id="stale-one",
                            node_id="node-1",
                            job_id="job-1",
                            node_url="http://node-1:8090",
                            created_at=old,
                            updated_at=old,
                        )
                    },
                    pending={
                        "stale-one": PendingSandboxDemand(
                            sandbox_id="stale-one",
                            resources=ResourceQuantity(
                                vcpu=1, memory_mb=512, disk_mb=1024
                            ),
                            created_at=old,
                            updated_at=old,
                        )
                    },
                    image_builds={},
                )
            )

            store.reconcile_sandboxes_for_node(
                "http://node-1:8090",
                [
                    SandboxRoute(
                        sandbox_id="active-one",
                        node_id="node-1",
                        job_id="job-1",
                        node_url="http://node-1:8090",
                        resources=ResourceQuantity(vcpu=1, memory_mb=512, disk_mb=1024),
                    )
                ],
                observed_at=now.isoformat(),
            )
            state = store.load()

        self.assertNotIn("stale-one", state.sandboxes)
        self.assertNotIn("stale-one", state.pending)
        self.assertNotIn("exec-stale", state.exec_sessions)
        self.assertIn("active-one", state.sandboxes)
        self.assertIn("other-node", state.sandboxes)

    def test_reconcile_sandboxes_for_node_keeps_newer_routes(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = RoutingStore(Path(raw_dir) / "routes.sqlite")
            now = utc_now()
            future = (now + timedelta(seconds=5)).isoformat()
            store.save(
                RoutingState(
                    sandboxes={
                        "new-after-list-started": SandboxRoute(
                            sandbox_id="new-after-list-started",
                            node_id="node-1",
                            job_id="job-1",
                            node_url="http://node-1:8090",
                            resources=ResourceQuantity(
                                vcpu=1, memory_mb=512, disk_mb=1024
                            ),
                            created_at=future,
                            updated_at=future,
                        )
                    },
                    exec_sessions={},
                    pending={},
                    image_builds={},
                )
            )

            store.reconcile_sandboxes_for_node(
                "http://node-1:8090",
                [],
                observed_at=now.isoformat(),
            )
            state = store.load()

        self.assertIn("new-after-list-started", state.sandboxes)

    def test_legacy_json_file_is_moved_aside(self) -> None:
        with TemporaryDirectory() as raw_dir:
            route_file = Path(raw_dir) / "routes.json"
            route_file.write_text(
                json.dumps(
                    {
                        "sandboxes": [],
                        "exec_sessions": [],
                        "pending": [],
                        "image_builds": [],
                    }
                ),
                encoding="utf-8",
            )

            state = RoutingStore(route_file).load()
            backups = list(Path(raw_dir).glob("routes.json.legacy-*"))

        self.assertEqual(state.pending, {})
        self.assertEqual(len(backups), 1)

    def test_prepared_capacity_signal_contributes_until_consumed_or_deleted(
        self,
    ) -> None:
        with TemporaryDirectory() as raw_dir:
            store = RoutingStore(Path(raw_dir) / "routes.sqlite")

            prepared = store.upsert_prepared_capacity(
                "prep-1",
                ResourceQuantity(vcpu=1, memory_mb=512, disk_mb=1024),
                count=4,
                ttl_seconds=600,
            )
            demand = store.pending_demand()
            consumed = store.consume_prepared_capacity()
            demand_after_consume = store.pending_demand()
            store.upsert_prepared_capacity(
                "prep-2",
                ResourceQuantity(vcpu=1, memory_mb=512, disk_mb=1024),
                count=2,
                ttl_seconds=600,
            )
            deleted = store.delete_prepared_capacity("prep-2")
            demand_after_delete = store.pending_demand()

        self.assertEqual(prepared.total_resources.vcpu, 4.0)
        self.assertEqual(demand.pending_resources, ResourceQuantity())
        self.assertEqual(
            demand.prepared_resources,
            ResourceQuantity(vcpu=4, memory_mb=2048, disk_mb=4096),
        )
        self.assertEqual(demand.desired_resources, demand.prepared_resources)
        self.assertEqual([item.prepare_id for item in consumed], ["prep-1"])
        self.assertEqual(demand_after_consume.prepared_resources, ResourceQuantity())
        self.assertEqual(deleted.prepare_id if deleted else None, "prep-2")
        self.assertEqual(demand_after_delete.prepared_resources, ResourceQuantity())

    def test_prepared_builder_signal_contributes_until_consumed_or_deleted(
        self,
    ) -> None:
        with TemporaryDirectory() as raw_dir:
            store = RoutingStore(Path(raw_dir) / "routes.sqlite")

            prepared = store.upsert_prepared_builder(
                "builder-prep-1",
                count=2,
                ttl_seconds=600,
            )
            listed = store.prepared_builders()
            count = store.prepared_builder_count()
            consumed = store.consume_prepared_builders()
            count_after_consume = store.prepared_builder_count()
            store.upsert_prepared_builder(
                "builder-prep-2",
                count=1,
                ttl_seconds=600,
            )
            deleted = store.delete_prepared_builder("builder-prep-2")
            count_after_delete = store.prepared_builder_count()

        self.assertEqual(prepared.count, 2)
        self.assertEqual([item.prepare_id for item in listed], ["builder-prep-1"])
        self.assertEqual(count, 2)
        self.assertEqual([item.prepare_id for item in consumed], ["builder-prep-1"])
        self.assertEqual(count_after_consume, 0)
        self.assertEqual(deleted.prepare_id if deleted else None, "builder-prep-2")
        self.assertEqual(count_after_delete, 0)

    def test_pending_image_build_signal_contributes_until_consumed_or_deleted(
        self,
    ) -> None:
        with TemporaryDirectory() as raw_dir:
            store = RoutingStore(Path(raw_dir) / "routes.sqlite")

            store.upsert_pending_image_build(
                "custom",
                "registry.example.org/custom:latest",
            )
            count = store.pending_image_build_count()
            consumed = store.consume_pending_image_builds()
            count_after_consume = store.pending_image_build_count()
            store.upsert_pending_image_build(
                "custom-2",
                "registry.example.org/custom-2:latest",
            )
            store.clear_pending_image_build("custom-2")
            count_after_delete = store.pending_image_build_count()

        self.assertEqual(count, 1)
        self.assertEqual([item.image_id for item in consumed], ["custom"])
        self.assertEqual(count_after_consume, 0)
        self.assertEqual(count_after_delete, 0)

    def test_sqlite_store_refreshes_signals_consumed_by_another_process(self) -> None:
        with TemporaryDirectory() as raw_dir:
            route_file = Path(raw_dir) / "routes.sqlite"
            gateway_store = RoutingStore(route_file)
            autoscaler_store = RoutingStore(route_file)

            gateway_store.upsert_pending(
                "sandbox-1",
                ResourceQuantity(vcpu=1, memory_mb=1024, disk_mb=2048),
            )
            gateway_store.upsert_pending_image_build(
                "custom",
                "registry.example.org/custom:latest",
            )
            gateway_store.upsert_prepared_capacity(
                "prep-1",
                ResourceQuantity(vcpu=2, memory_mb=2048, disk_mb=4096),
                count=1,
                ttl_seconds=600,
            )
            gateway_store.upsert_prepared_builder(
                "builder-prep-1",
                count=1,
                ttl_seconds=600,
            )

            self.assertEqual(autoscaler_store.pending_image_build_count(), 1)
            self.assertEqual(autoscaler_store.prepared_builder_count(), 1)
            autoscaler_store.consume_pending_demand()
            autoscaler_store.consume_pending_image_builds()
            autoscaler_store.consume_prepared_capacity()
            autoscaler_store.consume_prepared_builders()

            gateway_demand = gateway_store.pending_demand()
            gateway_pending_images = gateway_store.pending_image_build_count()
            gateway_prepared_builders = gateway_store.prepared_builder_count()

        self.assertEqual(gateway_demand.pending_resources, ResourceQuantity())
        self.assertEqual(gateway_demand.prepared_resources, ResourceQuantity())
        self.assertEqual(gateway_pending_images, 0)
        self.assertEqual(gateway_prepared_builders, 0)

    def test_expired_prepared_capacity_is_pruned_from_demand(self) -> None:
        now = utc_now()
        with TemporaryDirectory() as raw_dir:
            store = RoutingStore(Path(raw_dir) / "routes.sqlite")
            store.save(
                RoutingState(
                    sandboxes={},
                    exec_sessions={},
                    pending={},
                    image_builds={},
                    prepared={
                        "expired": PreparedCapacityDemand(
                            prepare_id="expired",
                            resources=ResourceQuantity(vcpu=2, memory_mb=1024),
                            count=2,
                            created_at=(now - timedelta(seconds=30)).isoformat(),
                            updated_at=(now - timedelta(seconds=30)).isoformat(),
                            expires_at=(now - timedelta(seconds=1)).isoformat(),
                        )
                    },
                )
            )

            demand = store.pending_demand()
            state = store.load()

        self.assertEqual(demand.prepared_resources, ResourceQuantity())
        self.assertEqual(state.prepared, {})

    def test_expired_prepared_builder_is_pruned(self) -> None:
        now = utc_now()
        with TemporaryDirectory() as raw_dir:
            store = RoutingStore(Path(raw_dir) / "routes.sqlite")
            store.save(
                RoutingState(
                    sandboxes={},
                    exec_sessions={},
                    pending={},
                    image_builds={},
                    prepared_builders={
                        "expired": PreparedBuilderDemand(
                            prepare_id="expired",
                            count=1,
                            created_at=(now - timedelta(seconds=30)).isoformat(),
                            updated_at=(now - timedelta(seconds=30)).isoformat(),
                            expires_at=(now - timedelta(seconds=1)).isoformat(),
                        )
                    },
                )
            )

            count = store.prepared_builder_count()
            state = store.load()

        self.assertEqual(count, 0)
        self.assertEqual(state.prepared_builders, {})

    def test_expired_pending_demand_is_pruned_from_demand(self) -> None:
        now = utc_now()
        with TemporaryDirectory() as raw_dir:
            store = RoutingStore(Path(raw_dir) / "routes.sqlite")
            store.save(
                RoutingState(
                    sandboxes={},
                    exec_sessions={},
                    pending={
                        "expired": PendingSandboxDemand(
                            sandbox_id="expired",
                            resources=ResourceQuantity(vcpu=1, memory_mb=512),
                            created_at=(
                                now - timedelta(seconds=PENDING_DEMAND_TTL_SECONDS + 30)
                            ).isoformat(),
                            updated_at=(
                                now - timedelta(seconds=PENDING_DEMAND_TTL_SECONDS + 1)
                            ).isoformat(),
                        )
                    },
                    image_builds={},
                    prepared={},
                )
            )

            demand = store.pending_demand()
            state = store.load()

        self.assertEqual(demand.pending_resources, ResourceQuantity())
        self.assertEqual(state.pending, {})

    def test_expired_pending_image_build_is_pruned(self) -> None:
        now = utc_now()
        with TemporaryDirectory() as raw_dir:
            store = RoutingStore(Path(raw_dir) / "routes.sqlite")
            store.save(
                RoutingState(
                    sandboxes={},
                    exec_sessions={},
                    pending={},
                    image_builds={
                        "expired": PendingImageBuildDemand(
                            image_id="expired",
                            tag="registry.example.org/expired:latest",
                            created_at=(
                                now - timedelta(seconds=PENDING_DEMAND_TTL_SECONDS + 30)
                            ).isoformat(),
                            updated_at=(
                                now - timedelta(seconds=PENDING_DEMAND_TTL_SECONDS + 1)
                            ).isoformat(),
                        )
                    },
                    prepared={},
                )
            )

            count = store.pending_image_build_count()
            state = store.load()

        self.assertEqual(count, 0)
        self.assertEqual(state.image_builds, {})

    def test_consuming_pending_demand_clears_active_pending_signals(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = RoutingStore(Path(raw_dir) / "routes.sqlite")
            store.upsert_pending(
                "pending-one",
                ResourceQuantity(vcpu=1, memory_mb=512, disk_mb=1024),
            )

            consumed = store.consume_pending_demand()
            demand = store.pending_demand()

        self.assertEqual([item.sandbox_id for item in consumed], ["pending-one"])
        self.assertEqual(demand.pending_resources, ResourceQuantity())


if __name__ == "__main__":
    unittest.main()
