from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from tempfile import TemporaryDirectory
from pathlib import Path
import json
from threading import Event
import unittest

from ucloud_sandboxes.models import ResourceQuantity, utc_now
from ucloud_sandboxes.routing import (
    ExecRoute,
    PENDING_DEMAND_TTL_SECONDS,
    PendingImageBuildDemand,
    PendingImageWarmup,
    PendingSandboxDemand,
    PreparedBuilderDemand,
    PreparedCapacityDemand,
    RoutingState,
    RoutingStore,
    SandboxRoute,
    SandboxRouteConflictError,
)


class RoutingStoreTests(unittest.TestCase):
    def test_routing_database_is_owner_only(self) -> None:
        with TemporaryDirectory() as raw_dir:
            route_file = Path(raw_dir) / "routes.sqlite"
            RoutingStore(route_file).load()

            self.assertEqual(route_file.stat().st_mode & 0o777, 0o600)

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

    def test_sandbox_routes_persist_cached_spec_and_state(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = RoutingStore(Path(raw_dir) / "routes.sqlite")
            spec = {
                "id": "cached-one",
                "image": "busybox",
                "labels": {"run": "r1"},
                "resources": {"vcpu": 1.0, "memory_mb": 512, "disk_mb": 1024},
            }
            store.upsert_sandbox(
                SandboxRoute(
                    sandbox_id="cached-one",
                    node_id="node-1",
                    job_id="job-1",
                    node_url="http://node-1:8090",
                    resources=ResourceQuantity(vcpu=1.0, memory_mb=512, disk_mb=1024),
                    spec=spec,
                    state="creating",
                )
            )

            route = store.get_sandbox_readonly("cached-one")
            routes = store.sandbox_routes_readonly()
            store.upsert_sandbox(
                SandboxRoute(
                    sandbox_id="cached-one",
                    node_id="node-1",
                    job_id="job-1",
                    node_url="http://node-1:8090",
                    resources=ResourceQuantity(vcpu=1.0, memory_mb=512, disk_mb=1024),
                )
            )
            preserved = store.get_sandbox_readonly("cached-one")

        self.assertIsNotNone(route)
        assert route is not None
        self.assertEqual(route.spec, spec)
        self.assertEqual(route.state, "creating")
        self.assertEqual([item.sandbox_id for item in routes], ["cached-one"])
        self.assertEqual(routes[0].spec["image"], "busybox")
        self.assertIsNotNone(preserved)
        assert preserved is not None
        self.assertEqual(preserved.spec, spec)
        self.assertEqual(preserved.state, "creating")

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
                            state="running",
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

    def test_delete_sandboxes_for_jobs_removes_routes_and_dependents(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = RoutingStore(Path(raw_dir) / "routes.sqlite")
            now = utc_now().isoformat()
            store.save(
                RoutingState(
                    sandboxes={
                        "remove-me": SandboxRoute(
                            sandbox_id="remove-me",
                            node_id="node-1",
                            job_id="job-1",
                            node_url="http://node-1:8090",
                            resources=ResourceQuantity(
                                vcpu=1, memory_mb=512, disk_mb=1024
                            ),
                            created_at=now,
                            updated_at=now,
                        ),
                        "keep-me": SandboxRoute(
                            sandbox_id="keep-me",
                            node_id="node-2",
                            job_id="job-2",
                            node_url="http://node-2:8090",
                            resources=ResourceQuantity(
                                vcpu=1, memory_mb=512, disk_mb=1024
                            ),
                            created_at=now,
                            updated_at=now,
                        ),
                    },
                    exec_sessions={
                        "exec-remove": ExecRoute(
                            session_id="exec-remove",
                            sandbox_id="remove-me",
                            node_id="node-1",
                            job_id="job-1",
                            node_url="http://node-1:8090",
                            created_at=now,
                            updated_at=now,
                        ),
                        "exec-keep": ExecRoute(
                            session_id="exec-keep",
                            sandbox_id="keep-me",
                            node_id="node-2",
                            job_id="job-2",
                            node_url="http://node-2:8090",
                            created_at=now,
                            updated_at=now,
                        ),
                    },
                    pending={
                        "remove-me": PendingSandboxDemand(
                            sandbox_id="remove-me",
                            resources=ResourceQuantity(
                                vcpu=1, memory_mb=512, disk_mb=1024
                            ),
                            created_at=now,
                            updated_at=now,
                        )
                    },
                    image_builds={},
                )
            )

            removed = store.delete_sandboxes_for_jobs(["job-1"])
            state = store.load()

        self.assertEqual([route.sandbox_id for route in removed], ["remove-me"])
        self.assertNotIn("remove-me", state.sandboxes)
        self.assertNotIn("remove-me", state.pending)
        self.assertNotIn("exec-remove", state.exec_sessions)
        self.assertIn("keep-me", state.sandboxes)
        self.assertIn("exec-keep", state.exec_sessions)

    def test_readonly_sandbox_queries_return_current_routes(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = RoutingStore(Path(raw_dir) / "routes.sqlite")
            route = SandboxRoute(
                sandbox_id="readonly-one",
                node_id="node-1",
                job_id="job-1",
                node_url="http://node-1:8090",
                resources=ResourceQuantity(vcpu=1, memory_mb=512, disk_mb=1024),
            )
            store.upsert_sandbox(route)

            fetched = store.get_sandbox_readonly("readonly-one")
            routes = store.sandbox_routes_readonly()

        self.assertIsNotNone(fetched)
        self.assertEqual(fetched.sandbox_id, "readonly-one")
        self.assertEqual([item.sandbox_id for item in routes], ["readonly-one"])

    def test_delete_stale_sandboxes_removes_missing_jobs_after_grace(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = RoutingStore(Path(raw_dir) / "routes.sqlite")
            now = utc_now()
            old = (now - timedelta(seconds=600)).isoformat()
            recent = (now - timedelta(seconds=30)).isoformat()
            store.save(
                RoutingState(
                    sandboxes={
                        "old-missing": SandboxRoute(
                            sandbox_id="old-missing",
                            node_id="old-node",
                            job_id="old-job",
                            node_url="http://old-node:8090",
                            created_at=old,
                            updated_at=old,
                        ),
                        "recent-missing": SandboxRoute(
                            sandbox_id="recent-missing",
                            node_id="recent-node",
                            job_id="recent-job",
                            node_url="http://recent-node:8090",
                            created_at=recent,
                            updated_at=recent,
                        ),
                        "active-job": SandboxRoute(
                            sandbox_id="active-job",
                            node_id="active-node",
                            job_id="active-job",
                            node_url="http://active-node:8090",
                            created_at=old,
                            updated_at=old,
                        ),
                        "fresh-node": SandboxRoute(
                            sandbox_id="fresh-node",
                            node_id="fresh-node",
                            job_id="missing-job",
                            node_url="http://fresh-node:8090",
                            created_at=old,
                            updated_at=old,
                        ),
                    },
                    exec_sessions={
                        "exec-old": ExecRoute(
                            session_id="exec-old",
                            sandbox_id="old-missing",
                            node_id="old-node",
                            job_id="old-job",
                            node_url="http://old-node:8090",
                            created_at=old,
                            updated_at=old,
                        )
                    },
                    pending={
                        "old-missing": PendingSandboxDemand(
                            sandbox_id="old-missing",
                            resources=ResourceQuantity(vcpu=1, memory_mb=512),
                            created_at=old,
                            updated_at=old,
                        )
                    },
                    image_builds={},
                )
            )

            removed = store.delete_stale_sandboxes(
                active_job_ids=["active-job"],
                active_node_ids=["fresh-node"],
                older_than=now - timedelta(seconds=120),
            )
            state = store.load()

        self.assertEqual([route.sandbox_id for route in removed], ["old-missing"])
        self.assertNotIn("old-missing", state.sandboxes)
        self.assertNotIn("old-missing", state.pending)
        self.assertNotIn("exec-old", state.exec_sessions)
        self.assertIn("recent-missing", state.sandboxes)
        self.assertIn("active-job", state.sandboxes)
        self.assertIn("fresh-node", state.sandboxes)

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

    def test_image_warmup_survives_prepared_capacity_consumption(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = RoutingStore(Path(raw_dir) / "routes.sqlite")
            store.upsert_prepared_capacity(
                "prep-1",
                ResourceQuantity(vcpu=1, memory_mb=512, disk_mb=1024),
                count=4,
                ttl_seconds=600,
                image="registry.example.org/image:latest",
            )
            warmup = store.upsert_image_warmup(
                "prep-1",
                "registry.example.org/image:latest",
                ResourceQuantity(vcpu=1, memory_mb=512, disk_mb=1024),
                count=4,
                ttl_seconds=600,
            )
            consumed = store.consume_prepared_capacity()
            warmups_after_consume = store.image_warmups()
            marked = store.mark_image_warmup_node("prep-1", "node-1")
            deleted = store.delete_image_warmup("prep-1")
            warmups_after_delete = store.image_warmups()

        self.assertEqual(warmup.warmup_id, "prep-1")
        self.assertEqual([item.prepare_id for item in consumed], ["prep-1"])
        self.assertEqual([item.warmup_id for item in warmups_after_consume], ["prep-1"])
        self.assertEqual(marked.warmed_node_ids, ("node-1",))
        self.assertEqual(deleted.warmed_node_ids, ("node-1",))
        self.assertEqual(warmups_after_delete, [])

    def test_image_warmup_mark_ignores_stale_image_completion(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = RoutingStore(Path(raw_dir) / "routes.sqlite")
            store.upsert_image_warmup(
                "prep-1",
                "registry.example.org/old:latest",
                ResourceQuantity(vcpu=1, memory_mb=512),
                count=1,
                ttl_seconds=600,
            )
            store.upsert_image_warmup(
                "prep-1",
                "registry.example.org/new:latest",
                ResourceQuantity(vcpu=1, memory_mb=512),
                count=1,
                ttl_seconds=600,
            )

            stale_mark = store.mark_image_warmup_node(
                "prep-1",
                "node-1",
                expected_image="registry.example.org/old:latest",
            )
            current_mark = store.mark_image_warmup_node(
                "prep-1",
                "node-1",
                expected_image="registry.example.org/new:latest",
            )

        self.assertIsNone(stale_mark)
        self.assertEqual(current_mark.warmed_node_ids, ("node-1",))

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

    def test_expired_image_warmup_is_pruned(self) -> None:
        now = utc_now()
        with TemporaryDirectory() as raw_dir:
            store = RoutingStore(Path(raw_dir) / "routes.sqlite")
            store.save(
                RoutingState(
                    sandboxes={},
                    exec_sessions={},
                    pending={},
                    image_builds={},
                    image_warmups={
                        "expired": PendingImageWarmup(
                            warmup_id="expired",
                            image="registry.example.org/expired:latest",
                            resources=ResourceQuantity(vcpu=1, memory_mb=512),
                            count=1,
                            created_at=(now - timedelta(seconds=30)).isoformat(),
                            updated_at=(now - timedelta(seconds=30)).isoformat(),
                            expires_at=(now - timedelta(seconds=1)).isoformat(),
                        )
                    },
                )
            )

            warmups = store.image_warmups()
            state = store.load()

        self.assertEqual(warmups, [])
        self.assertEqual(state.image_warmups, {})

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

    def test_repeated_pending_signal_for_same_sandbox_does_not_multiply_demand(
        self,
    ) -> None:
        with TemporaryDirectory() as raw_dir:
            store = RoutingStore(Path(raw_dir) / "routes.sqlite")
            resources = ResourceQuantity(vcpu=1, memory_mb=512, disk_mb=1024)

            store.upsert_pending("pending-one", resources)
            store.upsert_pending("pending-one", resources)
            demand = store.pending_demand()
            pending = store.pending_sandboxes()

        self.assertEqual(demand.pending_resources, resources)
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0].attempts, 2)

    def test_failed_create_pending_demand_preserves_incarnation_identity(self) -> None:
        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "routes.sqlite"
            store = RoutingStore(path)
            resources = ResourceQuantity(vcpu=1, memory_mb=512, disk_mb=1024)

            store.upsert_pending(
                "pending-one",
                resources,
                generation=4,
                operation_id="create-4",
                spec_hash="sha256:spec-4",
                failure_reason="image_pull_http_502",
            )
            store.upsert_pending(
                "pending-one",
                resources,
                generation=4,
                operation_id="create-4",
                spec_hash="sha256:spec-4",
                failure_reason="image_pull_http_503",
            )
            replay = RoutingStore(path).get_pending("pending-one")
            assert replay is not None
            self.assertEqual(replay.attempts, 2)
            self.assertEqual(replay.generation, 4)
            self.assertEqual(replay.operation_id, "create-4")
            self.assertEqual(replay.spec_hash, "sha256:spec-4")
            self.assertEqual(replay.failure_reason, "image_pull_http_503")

            store.upsert_pending(
                "pending-one",
                resources,
                generation=5,
                operation_id="create-5",
                spec_hash="sha256:spec-5",
                failure_reason="registry_lease_unavailable",
            )
            replacement = RoutingStore(path).get_pending("pending-one")

        assert replacement is not None
        self.assertEqual(replacement.attempts, 1)
        self.assertEqual(replacement.generation, 5)
        self.assertEqual(replacement.operation_id, "create-5")

    def test_snapshot_consume_does_not_delete_refreshed_signals(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = RoutingStore(Path(raw_dir) / "routes.sqlite")
            store.upsert_pending(
                "pending-one",
                ResourceQuantity(vcpu=1, memory_mb=512, disk_mb=1024),
            )
            store.upsert_pending_image_build("image-one", "registry/image:old")
            store.upsert_prepared_capacity(
                "prep-one",
                ResourceQuantity(vcpu=1, memory_mb=512, disk_mb=1024),
                count=1,
                ttl_seconds=600,
            )
            store.upsert_prepared_builder(
                "builder-one",
                count=1,
                ttl_seconds=600,
            )
            snapshot = store.load()

            store.upsert_pending(
                "pending-one",
                ResourceQuantity(vcpu=1, memory_mb=512, disk_mb=1024),
            )
            store.upsert_pending_image_build("image-one", "registry/image:new")
            store.upsert_prepared_capacity(
                "prep-one",
                ResourceQuantity(vcpu=1, memory_mb=512, disk_mb=1024),
                count=2,
                ttl_seconds=600,
            )
            store.upsert_prepared_builder(
                "builder-one",
                count=2,
                ttl_seconds=600,
            )

            consumed_pending = store.consume_pending_demand(
                snapshot.pending.values()
            )
            consumed_images = store.consume_pending_image_builds(
                snapshot.image_builds.values()
            )
            consumed_prepared = store.consume_prepared_capacity(
                snapshot.prepared.values()
            )
            consumed_builders = store.consume_prepared_builders(
                snapshot.prepared_builders.values()
            )
            remaining = store.load()

        self.assertEqual(consumed_pending, [])
        self.assertEqual(consumed_images, [])
        self.assertEqual(consumed_prepared, [])
        self.assertEqual(consumed_builders, [])
        self.assertEqual(remaining.pending["pending-one"].attempts, 2)
        self.assertEqual(remaining.image_builds["image-one"].attempts, 2)
        self.assertEqual(remaining.image_builds["image-one"].tag, "registry/image:new")
        self.assertEqual(remaining.prepared["prep-one"].count, 2)
        self.assertEqual(remaining.prepared_builders["builder-one"].count, 2)

    def test_generation_high_water_survives_delete_and_reopen(self) -> None:
        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "routes.sqlite"
            base = SandboxRoute(
                sandbox_id="versioned-one",
                node_id="node-1",
                job_id="job-1",
                node_url="http://node-1:8090",
                resources=ResourceQuantity(vcpu=1, memory_mb=512, disk_mb=1024),
                spec={"id": "versioned-one", "image": "busybox"},
            )
            first_store = RoutingStore(path)
            first = first_store.allocate_sandbox_create(base, spec_hash="hash-1")
            removed = first_store.delete_sandbox_if_current(
                first.sandbox_id,
                generation=first.generation,
                create_operation_id=first.create_operation_id,
            )
            second = RoutingStore(path).allocate_sandbox_create(
                base,
                spec_hash="hash-2",
            )

        self.assertIsNotNone(removed)
        self.assertEqual(first.generation, 1)
        self.assertEqual(second.generation, 2)
        self.assertNotEqual(first.create_operation_id, second.create_operation_id)

    def test_stale_inventory_cannot_overwrite_or_delete_newer_generation(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = RoutingStore(Path(raw_dir) / "routes.sqlite")
            current = SandboxRoute(
                sandbox_id="versioned-one",
                node_id="node-1",
                job_id="job-1",
                node_url="http://node-1:8090",
                resources=ResourceQuantity(vcpu=2, memory_mb=1024, disk_mb=2048),
                spec={"id": "versioned-one", "image": "busybox"},
                state="running",
                generation=2,
                create_operation_id="create-2",
                spec_hash="hash-2",
                node_epoch="epoch-1",
                activity_epoch=5,
            )
            store.upsert_sandbox(current)
            store.reconcile_sandboxes_for_node(
                current.node_url,
                [
                    SandboxRoute(
                        sandbox_id=current.sandbox_id,
                        node_id=current.node_id,
                        job_id=current.job_id,
                        node_url=current.node_url,
                        state="running",
                        generation=1,
                        create_operation_id="create-1",
                        spec_hash="hash-1",
                        node_epoch="epoch-1",
                        activity_epoch=4,
                    )
                ],
                observed_at=utc_now().isoformat(),
                node_epoch="epoch-1",
                activity_epoch=4,
                inventory_complete=True,
            )
            after_stale_entry = store.get_sandbox_readonly(current.sandbox_id)
            store.reconcile_sandboxes_for_node(
                current.node_url,
                [],
                observed_at=utc_now().isoformat(),
                node_epoch="epoch-1",
                activity_epoch=4,
                inventory_complete=True,
            )
            after_stale_absence = store.get_sandbox_readonly(current.sandbox_id)

        for route in (after_stale_entry, after_stale_absence):
            self.assertIsNotNone(route)
            assert route is not None
            self.assertEqual(route.generation, 2)
            self.assertEqual(route.create_operation_id, "create-2")
            self.assertEqual(route.spec_hash, "hash-2")
            self.assertEqual(route.resources, current.resources)

    def test_same_generation_update_requires_exact_nonempty_identity(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = RoutingStore(Path(raw_dir) / "routes.sqlite")
            current = SandboxRoute(
                sandbox_id="versioned-one",
                node_id="node-1",
                job_id="job-1",
                node_url="http://node-1:8090",
                state="running",
                generation=4,
                create_operation_id="create-4",
                spec_hash="hash-4",
                node_epoch="epoch-1",
                activity_epoch=8,
            )
            store.upsert_sandbox(current)

            for create_operation_id, spec_hash in (
                ("", "hash-4"),
                ("create-4", ""),
                ("different", "hash-4"),
                ("create-4", "different"),
            ):
                result = store.upsert_sandbox(
                    SandboxRoute(
                        **{
                            **current.__dict__,
                            "state": "stopped",
                            "create_operation_id": create_operation_id,
                            "spec_hash": spec_hash,
                        }
                    )
                )
                self.assertEqual(result.state, "running")

            stored = store.get_sandbox_readonly(current.sandbox_id)

        self.assertIsNotNone(stored)
        assert stored is not None
        self.assertEqual(stored.state, "running")
        self.assertEqual(stored.create_operation_id, "create-4")
        self.assertEqual(stored.spec_hash, "hash-4")

    def test_exact_identity_adopts_new_node_epoch_then_allows_absence(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = RoutingStore(Path(raw_dir) / "routes.sqlite")
            first = store.allocate_sandbox_create(
                SandboxRoute(
                    sandbox_id="survived-restart",
                    node_id="node-1",
                    job_id="job-1",
                    node_url="http://node-1:8090",
                    spec={"id": "survived-restart", "image": "busybox"},
                ),
                spec_hash="hash-1",
                create_operation_id="create-1",
            )
            store.upsert_sandbox(
                SandboxRoute(
                    **{
                        **first.__dict__,
                        "state": "running",
                        "node_epoch": "epoch-before-restart",
                        "activity_epoch": 100,
                    }
                )
            )
            adopted_at = utc_now()

            store.reconcile_sandboxes_for_node(
                first.node_url,
                [
                    SandboxRoute(
                        sandbox_id=first.sandbox_id,
                        node_id=first.node_id,
                        job_id=first.job_id,
                        node_url=first.node_url,
                        state="running",
                        generation=first.generation,
                        create_operation_id=first.create_operation_id,
                        spec_hash=first.spec_hash,
                        node_epoch="epoch-after-restart",
                        activity_epoch=1,
                    )
                ],
                observed_at=adopted_at.isoformat(),
                node_epoch="epoch-after-restart",
                activity_epoch=1,
                inventory_complete=True,
            )
            adopted = store.get_sandbox_readonly(first.sandbox_id)
            store.reconcile_sandboxes_for_node(
                first.node_url,
                [],
                observed_at=(adopted_at + timedelta(seconds=1)).isoformat(),
                node_epoch="epoch-after-restart",
                activity_epoch=1,
                inventory_complete=True,
            )
            removed = store.get_sandbox_readonly(first.sandbox_id)

        self.assertIsNotNone(adopted)
        assert adopted is not None
        self.assertEqual(adopted.node_epoch, "epoch-after-restart")
        self.assertEqual(adopted.activity_epoch, 1)
        self.assertIsNone(removed)

    def test_reconcile_transaction_cannot_delete_concurrent_new_incarnation(self) -> None:
        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "routes.sqlite"
            reconciling_store = RoutingStore(path)
            writer_store = RoutingStore(path)
            first = writer_store.allocate_sandbox_create(
                SandboxRoute(
                    sandbox_id="reused-id",
                    node_id="node-1",
                    job_id="job-1",
                    node_url="http://node-1:8090",
                    spec={"id": "reused-id", "image": "busybox"},
                ),
                spec_hash="hash-1",
                create_operation_id="create-1",
            )
            writer_store.upsert_sandbox(
                SandboxRoute(**{**first.__dict__, "state": "running"})
            )
            snapshot_reached = Event()
            allow_reconcile_to_continue = Event()
            original_load = reconciling_store._load_unlocked

            def pause_at_snapshot(conn):
                snapshot = original_load(conn)
                snapshot_reached.set()
                self.assertTrue(allow_reconcile_to_continue.wait(timeout=5))
                return snapshot

            reconciling_store._load_unlocked = pause_at_snapshot

            def replace_incarnation() -> SandboxRoute:
                writer_store.delete_sandbox_if_current(
                    "reused-id",
                    generation=1,
                    create_operation_id="create-1",
                )
                return writer_store.allocate_sandbox_create(
                    SandboxRoute(
                        sandbox_id="reused-id",
                        node_id="node-1",
                        job_id="job-1",
                        node_url="http://node-1:8090",
                        spec={"id": "reused-id", "image": "python"},
                    ),
                    spec_hash="hash-2",
                    create_operation_id="create-2",
                )

            observed_at = (utc_now() + timedelta(seconds=1)).isoformat()
            with ThreadPoolExecutor(max_workers=2) as executor:
                reconciliation = executor.submit(
                    reconciling_store.reconcile_sandboxes_for_node,
                    first.node_url,
                    [],
                    observed_at=observed_at,
                    node_epoch="",
                    activity_epoch=0,
                    inventory_complete=True,
                )
                self.assertTrue(snapshot_reached.wait(timeout=5))
                replacement = executor.submit(replace_incarnation)
                allow_reconcile_to_continue.set()
                reconciliation.result(timeout=5)
                replacement.result(timeout=5)
            stored = RoutingStore(path).get_sandbox_readonly(first.sandbox_id)

        self.assertIsNotNone(stored)
        assert stored is not None
        self.assertEqual(stored.generation, 2)
        self.assertEqual(stored.create_operation_id, "create-2")
        self.assertEqual(stored.spec_hash, "hash-2")

    def test_concurrent_different_spec_allocation_rejects_loser_atomically(self) -> None:
        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "routes.sqlite"
            first_store = RoutingStore(path)
            second_store = RoutingStore(path)

            def allocate(store: RoutingStore, image: str, spec_hash: str):
                return store.allocate_sandbox_create(
                    SandboxRoute(
                        sandbox_id="same-id",
                        node_id="node-1",
                        job_id="job-1",
                        node_url="http://node-1:8090",
                        spec={"id": "same-id", "image": image},
                    ),
                    spec_hash=spec_hash,
                )

            with ThreadPoolExecutor(max_workers=2) as executor:
                futures = [
                    executor.submit(allocate, first_store, "busybox", "hash-a"),
                    executor.submit(allocate, second_store, "python", "hash-b"),
                ]
                results: list[SandboxRoute] = []
                conflicts = 0
                for future in futures:
                    try:
                        results.append(future.result())
                    except SandboxRouteConflictError:
                        conflicts += 1
            stored = RoutingStore(path).get_sandbox_readonly("same-id")

        self.assertEqual(len(results), 1)
        self.assertEqual(conflicts, 1)
        self.assertIsNotNone(stored)
        assert stored is not None
        self.assertEqual(stored.create_operation_id, results[0].create_operation_id)
        self.assertEqual(stored.spec_hash, results[0].spec_hash)


if __name__ == "__main__":
    unittest.main()
