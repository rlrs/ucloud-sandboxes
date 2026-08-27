from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import replace
from datetime import datetime, timedelta
from tempfile import TemporaryDirectory
from pathlib import Path
import json
import sqlite3
from threading import Event
import unittest
from unittest.mock import patch

from ucloud_sandboxes.models import SandboxInventoryEntry, ResourceQuantity, utc_now
from ucloud_sandboxes.managed_process import ManagedProcessRecord
from ucloud_sandboxes.routing import (
    ExecRoute,
    PENDING_DEMAND_TTL_SECONDS,
    PendingSandboxDemand,
    RoutingState,
    RoutingStore,
    SandboxRoute,
    SandboxRouteAllocation,
    SandboxRouteConflictError,
    is_portable_parked_route,
    sandbox_demand_from_routing_state,
)


def sandbox_route(**values: object) -> SandboxRoute:
    values.setdefault("resources", ResourceQuantity())
    values.setdefault("spec", {"id": values.get("sandbox_id")})
    values.setdefault("state", "unknown")
    values.setdefault("generation", 1)
    values.setdefault("create_operation_id", "create-test-route")
    values.setdefault("spec_hash", "a" * 64)
    return SandboxRoute(**values)  # type: ignore[arg-type]


def sandbox_allocation(**values: object) -> SandboxRouteAllocation:
    values.setdefault("resources", ResourceQuantity())
    values.setdefault("spec", {"id": values.get("sandbox_id")})
    return SandboxRouteAllocation(**values)  # type: ignore[arg-type]


@contextmanager
def routing_store() -> Iterator[RoutingStore]:
    with TemporaryDirectory() as raw_dir:
        yield RoutingStore(Path(raw_dir) / "routes.sqlite")


def allocate_sandbox_create(
    store: RoutingStore,
    allocation: SandboxRouteAllocation,
    *,
    spec_hash: str,
    create_operation_id: str | None = None,
) -> SandboxRoute:
    stored, _pending = store.allocate_sandbox_create_with_pending(
        allocation,
        spec_hash=spec_hash,
        create_operation_id=create_operation_id,
    )
    return stored


def set_sandbox_state(
    store: RoutingStore,
    route: SandboxRoute,
    state: str,
) -> SandboxRoute:
    stored = store.set_sandbox_state_if_current(
        route,
        expected_states={route.state},
        state=state,
    )
    assert stored is not None
    return stored


def move_sandbox_with_journal(
    store: RoutingStore,
    source: SandboxRoute,
    *,
    destination_node_id: str,
    destination_job_id: str,
    destination_node_url: str,
) -> SandboxRoute:
    migration = store.begin_sandbox_migration(
        source,
        migration_id=f"migration-{source.sandbox_id}",
        destination_node_id=destination_node_id,
        destination_job_id=destination_job_id,
        destination_node_url=destination_node_url,
    )
    for expected, phase in (({"planned"}, "prepared"), ({"prepared"}, "staged")):
        advanced = store.advance_sandbox_migration(
            migration.migration_id,
            expected_phases=expected,
            phase=phase,
        )
        assert advanced is not None
    routed = store.route_sandbox_migration(migration.migration_id)
    assert routed is not None
    return routed[1]


def seed_routing_state(store: RoutingStore, state: RoutingState) -> None:
    """Seed fixtures through the same transactional APIs used in production."""

    def timestamp(raw: str) -> datetime:
        return datetime.fromisoformat(raw) if raw else utc_now()

    for route in state.sandboxes.values():
        with patch(
            "ucloud_sandboxes.routing.utc_now",
            return_value=timestamp(route.updated_at),
        ):
            store.upsert_sandbox(route)
    for route in state.exec_sessions.values():
        with patch(
            "ucloud_sandboxes.routing.utc_now",
            return_value=timestamp(route.updated_at),
        ):
            store.upsert_exec(route)
    for item in state.pending.values():
        with patch(
            "ucloud_sandboxes.routing.utc_now",
            return_value=timestamp(item.updated_at),
        ):
            store.upsert_pending(
                item.sandbox_id,
                item.resources,
                generation=item.generation,
                operation_id=item.operation_id,
                spec_hash=item.spec_hash,
                failure_reason=item.failure_reason,
            )


class RoutingStoreTests(unittest.TestCase):
    def test_managed_primary_state_survives_route_handoff_and_is_generation_fenced(
        self,
    ) -> None:
        with routing_store() as store:
            route = store.upsert_sandbox(
                sandbox_route(
                    sandbox_id="managed-one",
                    node_id="node-1",
                    job_id="vm-1",
                    node_url="http://node-1:8090",
                    state="running",
                    generation=1,
                    create_operation_id="create-managed-one",
                    spec_hash="b" * 64,
                )
            )
            running = ManagedProcessRecord(
                sandbox_id="managed-one",
                sandbox_generation=route.generation,
                job_id="rollout-1",
                spec_sha256="a" * 64,
                state="running",
                pid=42,
                sequence=2,
                updated_at="2026-08-03T00:00:00+00:00",
            )
            store.upsert_managed_process(route, running)

            moved = move_sandbox_with_journal(
                store,
                route,
                destination_node_id="node-2",
                destination_job_id="vm-2",
                destination_node_url="http://node-2:8090",
            )
            cached = store.get_managed_process("managed-one", "rollout-1")

            self.assertEqual(moved.generation, route.generation)
            self.assertEqual(cached, running)
            self.assertIsNone(
                store.get_managed_process(
                    "managed-one",
                    "rollout-1",
                    sandbox_generation=route.generation + 1,
                )
            )
            with self.assertRaises(SandboxRouteConflictError):
                store.upsert_managed_process(
                    replace(moved, generation=moved.generation + 1),
                    running,
                )

    def test_program_request_lifecycle_is_monotonic_durable_and_terminal(self) -> None:
        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "routes.sqlite"
            store = RoutingStore(path)
            route = store.upsert_sandbox(
                sandbox_route(
                    sandbox_id="sandbox-1",
                    node_id="node-1",
                    job_id="job-1",
                    node_url="http://node-1:8090",
                    resources=ResourceQuantity(memory_mb=4096, disk_mb=8192),
                    state="parked",
                )
            )
            transition = store.upsert_program_request_transition_with_change
            transition(
                route,
                request_id="request-1",
                rollout_id="rollout-1",
                state="model_wait",
                transition_at="2026-07-31T10:00:00+00:00",
            )
            ready, changed = transition(
                route,
                request_id="request-1",
                rollout_id="rollout-1",
                state="ready_to_wake",
                transition_at="2026-07-31T10:00:10+00:00",
                last_error="restore failed",
            )
            duplicate, duplicate_changed = transition(
                route,
                request_id="request-1",
                rollout_id="rollout-1",
                state="ready_to_wake",
                transition_at="2026-07-31T10:00:11+00:00",
                last_error="restore failed",
            )
            regressed, regressed_changed = transition(
                route,
                request_id="request-1",
                rollout_id="rollout-1",
                state="model_wait",
                transition_at="2026-07-31T10:00:12+00:00",
            )
            acting, acting_changed = transition(
                route,
                request_id="request-1",
                rollout_id="rollout-1",
                state="acting",
                transition_at="2026-07-31T10:00:13+00:00",
                clear_error=True,
            )
            durable = RoutingStore(path).program_requests_readonly()[0]
            store.delete_sandbox_if_current(
                route.sandbox_id,
                generation=route.generation,
            )
            active_after_delete = store.program_requests_readonly()
            terminal = store.program_requests_readonly(include_terminal=True)[0]

        self.assertTrue(changed)
        self.assertEqual((duplicate, regressed), (ready, ready))
        self.assertEqual((duplicate_changed, regressed_changed), (False, False))
        self.assertTrue(acting_changed)
        self.assertEqual(durable, acting)
        self.assertEqual(durable.resources.disk_mb, 8192)
        self.assertEqual(durable.last_error, "")
        self.assertEqual(active_after_delete, [])
        self.assertEqual(terminal.state, "terminal")

    def test_program_request_identity_is_generation_fenced(self) -> None:
        with routing_store() as store:
            route = store.upsert_sandbox(
                sandbox_route(
                    sandbox_id="sandbox-1",
                    node_id="node-1",
                    job_id="job-1",
                    node_url="http://node-1:8090",
                    state="parked",
                )
            )
            store.upsert_program_request_transition_with_change(
                route,
                request_id="request-1",
                rollout_id="rollout-1",
                state="model_wait",
            )

            with self.assertRaises(SandboxRouteConflictError):
                store.upsert_program_request_transition_with_change(
                    replace(route, generation=route.generation + 1),
                    request_id="request-2",
                    rollout_id="rollout-1",
                    state="model_wait",
                )
            with self.assertRaises(SandboxRouteConflictError):
                store.upsert_program_request_transition_with_change(
                    route,
                    request_id="request-1",
                    rollout_id="rollout-2",
                    state="ready_to_wake",
                )

    def test_migration_pending_shape_excludes_source_job(self) -> None:
        with routing_store() as store:
            store.upsert_sandbox(
                sandbox_route(
                    sandbox_id="sandbox-1",
                    node_id="node-1",
                    job_id="job-1",
                    node_url="http://node-1:8090",
                    state="parked",
                )
            )
            resources = ResourceQuantity(disk_mb=8192)
            store.upsert_pending("__migration__:sandbox-1", resources)

            demand = sandbox_demand_from_routing_state(store.load())
            indexed_demand = store.pending_demand()

        self.assertEqual(demand.placement_requests[0].resources, resources)
        self.assertEqual(
            demand.placement_requests[0].excluded_job_ids,
            ("job-1",),
        )
        self.assertEqual(indexed_demand.placement_requests, demand.placement_requests)

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
                    sandbox_route(
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

    def test_migration_journal_and_route_switch_commit_atomically(self) -> None:
        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "routes.sqlite"
            store = RoutingStore(path)
            source = allocate_sandbox_create(
                store,
                sandbox_allocation(
                    sandbox_id="journaled-move",
                    node_id="source-node",
                    job_id="source-job",
                    node_url="http://source:8090",
                    resources=ResourceQuantity(memory_mb=1024, disk_mb=4096),
                    spec={"id": "journaled-move", "image": "busybox"},
                ),
                spec_hash="a" * 64,
                create_operation_id="create-operation",
            )
            source = set_sandbox_state(store, source, "parked")
            migration = store.begin_sandbox_migration(
                source,
                migration_id="migration-one",
                destination_node_id="destination-node",
                destination_job_id="destination-job",
                destination_node_url="http://destination:8090",
            )
            migration = store.advance_sandbox_migration(
                migration.migration_id,
                expected_phases={"planned"},
                phase="prepared",
                storage_schema="storage-native-v1",
                snapshot_sha256="a" * 64,
                storage_snapshot={
                    "publication": {
                        "manifest_digest": "sha256:" + "b" * 64,
                        "repository": "snapshots",
                        "tag": "migration-one",
                    }
                },
            )
            assert migration is not None
            migration = store.advance_sandbox_migration(
                migration.migration_id,
                expected_phases={"prepared"},
                phase="staged",
            )
            assert migration is not None

            routed = store.route_sandbox_migration(migration.migration_id)
            replayed = RoutingStore(path).route_sandbox_migration(
                migration.migration_id
            )

        self.assertIsNotNone(routed)
        self.assertIsNotNone(replayed)
        assert routed is not None and replayed is not None
        self.assertEqual(routed[0].phase, "routed")
        self.assertEqual(routed[1].node_id, "destination-node")
        self.assertEqual(replayed, routed)

    def test_route_delete_terminalizes_active_migration(self) -> None:
        with routing_store() as store:
            source = allocate_sandbox_create(
                store,
                sandbox_allocation(
                    sandbox_id="deleted-mid-migration",
                    node_id="source-node",
                    job_id="source-job",
                    node_url="http://source:8090",
                    resources=ResourceQuantity(disk_mb=4096),
                    spec={"id": "deleted-mid-migration", "image": "busybox"},
                ),
                spec_hash="a" * 64,
                create_operation_id="create-operation",
            )
            source = set_sandbox_state(store, source, "parked")
            migration = store.begin_sandbox_migration(
                source,
                migration_id="migration-deleted-route",
                destination_node_id="destination-node",
                destination_job_id="destination-job",
                destination_node_url="http://destination:8090",
            )
            deleting = store.prepare_sandbox_delete(source.sandbox_id)
            assert deleting is not None

            removed = store.delete_sandbox_if_current(
                deleting.sandbox_id,
                generation=deleting.generation,
                delete_operation_id=deleting.delete_operation_id,
            )
            final_migration = store.get_sandbox_migration(migration.migration_id)

        self.assertIsNotNone(removed)
        self.assertIsNotNone(final_migration)
        assert final_migration is not None
        self.assertEqual(final_migration.phase, "complete")
        self.assertEqual(
            final_migration.error,
            "sandbox deleted before migration completed",
        )

    def test_orphaned_migration_reconciliation_is_bounded(self) -> None:
        with routing_store() as store:
            migration_ids: list[str] = []
            for index in range(2):
                sandbox_id = f"legacy-orphan-{index}"
                source = allocate_sandbox_create(
                    store,
                    sandbox_allocation(
                        sandbox_id=sandbox_id,
                        node_id="source-node",
                        job_id="source-job",
                        node_url="http://source:8090",
                        resources=ResourceQuantity(disk_mb=4096),
                        spec={"id": sandbox_id, "image": "busybox"},
                    ),
                    spec_hash="a" * 64,
                    create_operation_id=f"create-operation-{index}",
                )
                source = set_sandbox_state(store, source, "parked")
                migration = store.begin_sandbox_migration(
                    source,
                    migration_id=f"legacy-orphan-migration-{index}",
                    destination_node_id="destination-node",
                    destination_job_id="destination-job",
                    destination_node_url="http://destination:8090",
                )
                migration_ids.append(migration.migration_id)
            # Model state written by a pre-reconciliation release, which could
            # remove the route without terminalizing its migration journal.
            with store._transaction() as connection:
                connection.execute("DELETE FROM sandboxes")

            first = store.terminalize_orphaned_sandbox_migrations(max_count=1)
            remaining = store.sandbox_migrations(active_only=True)
            second = store.terminalize_orphaned_sandbox_migrations(max_count=1)

        self.assertEqual([item.migration_id for item in first], migration_ids[:1])
        self.assertEqual([item.migration_id for item in remaining], migration_ids[1:])
        self.assertEqual([item.migration_id for item in second], migration_ids[1:])
        self.assertTrue(all(item.phase == "complete" for item in (*first, *second)))
        self.assertTrue(
            all(item.error == "sandbox route is absent" for item in (*first, *second))
        )

    def test_wake_completion_atomically_marks_destination_waking(self) -> None:
        with routing_store() as store:
            source = allocate_sandbox_create(
                store,
                sandbox_allocation(
                    sandbox_id="wake-move",
                    node_id="source-node",
                    job_id="source-job",
                    node_url="http://source:8090",
                    resources=ResourceQuantity(
                        vcpu=2,
                        memory_mb=4096,
                        disk_mb=8192,
                    ),
                    spec={"id": "wake-move", "image": "busybox"},
                ),
                spec_hash="a" * 64,
                create_operation_id="create-operation",
            )
            source = set_sandbox_state(store, source, "parked")
            migration = store.begin_sandbox_migration(
                source,
                migration_id="wake-migration",
                destination_node_id="destination-node",
                destination_job_id="destination-job",
                destination_node_url="http://destination:8090",
            )
            migration = store.advance_sandbox_migration(
                migration.migration_id,
                expected_phases={"planned"},
                phase="prepared",
                storage_schema="storage-native-v1",
                snapshot_sha256="a" * 64,
                storage_snapshot={
                    "publication": {
                        "manifest_digest": "sha256:" + "b" * 64,
                        "repository": "snapshots",
                        "tag": "wake-migration",
                    }
                },
            )
            assert migration is not None
            migration = store.advance_sandbox_migration(
                migration.migration_id,
                expected_phases={"prepared"},
                phase="staged",
            )
            assert migration is not None
            routed = store.route_sandbox_migration(migration.migration_id)
            assert routed is not None
            activated = store.advance_sandbox_migration(
                migration.migration_id,
                expected_phases={"routed"},
                phase="activated",
            )
            assert activated is not None

            completed = store.complete_sandbox_migration(
                migration.migration_id,
                wake_destination=True,
            )
            destination = store.get_sandbox("wake-move")

        self.assertIsNotNone(completed)
        self.assertEqual(completed.phase, "complete")
        self.assertIsNotNone(destination)
        self.assertEqual(destination.state, "waking")
        self.assertEqual(destination.node_id, "destination-node")

    def test_reconcile_sandboxes_for_node_removes_missing_node_routes(self) -> None:
        with routing_store() as store:
            now = utc_now()
            old = (now - timedelta(seconds=60)).isoformat()
            seed_routing_state(
                store,
                RoutingState(
                    sandboxes={
                        "stale-one": sandbox_route(
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
                        "other-node": sandbox_route(
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
                ),
            )

            with patch.object(
                store,
                "_load_unlocked",
                side_effect=AssertionError("reconcile must not load all routes"),
            ):
                store.reconcile_sandboxes_for_node(
                    "http://node-1:8090",
                    [
                        SandboxInventoryEntry(
                            sandbox_id="active-one",
                            resources=ResourceQuantity(
                                vcpu=1,
                                memory_mb=512,
                                disk_mb=1024,
                            ),
                            state="running",
                            generation=1,
                            operation_id="create-active-one",
                            spec_hash="a" * 64,
                        )
                    ],
                    node_id="node-1",
                    job_id="job-1",
                    reported_sandbox_ids={"active-one"},
                    observed_at=now.isoformat(),
                )
            state = store.load()

        self.assertNotIn("stale-one", state.sandboxes)
        self.assertNotIn("stale-one", state.pending)
        self.assertNotIn("exec-stale", state.exec_sessions)
        self.assertNotIn("active-one", state.sandboxes)
        self.assertIn("other-node", state.sandboxes)

    def test_reconcile_sandboxes_for_node_keeps_newer_routes(self) -> None:
        with routing_store() as store:
            now = utc_now()
            future = (now + timedelta(seconds=5)).isoformat()
            seed_routing_state(
                store,
                RoutingState(
                    sandboxes={
                        "new-after-list-started": sandbox_route(
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
                ),
            )

            store.reconcile_sandboxes_for_node(
                "http://node-1:8090",
                [],
                node_id="node-1",
                job_id="job-1",
                reported_sandbox_ids=set(),
                observed_at=now.isoformat(),
            )
            state = store.load()

        self.assertIn("new-after-list-started", state.sandboxes)

    def test_reconcile_inventory_cannot_advance_route_generation(self) -> None:
        with routing_store() as store:
            existing = store.upsert_sandbox(
                sandbox_route(
                    sandbox_id="sandbox-1",
                    node_id="node-1",
                    job_id="job-1",
                    node_url="http://node-1:8090",
                    state="running",
                    generation=1,
                    create_operation_id="create-1",
                    spec_hash="a" * 64,
                )
            )

            store.reconcile_sandboxes_for_node(
                existing.node_url,
                [
                    SandboxInventoryEntry(
                        sandbox_id=existing.sandbox_id,
                        generation=2,
                        operation_id="unplanned-create-2",
                        spec_hash="b" * 64,
                        state="parked",
                    )
                ],
                node_id=existing.node_id,
                job_id=existing.job_id,
                reported_sandbox_ids={existing.sandbox_id},
                observed_at=utc_now().isoformat(),
                node_epoch="boot-1",
                activity_epoch=100,
            )
            stored = store.get_sandbox("sandbox-1")

        assert stored is not None
        self.assertEqual(stored.generation, 1)
        self.assertEqual(stored.create_operation_id, "create-1")
        self.assertEqual(stored.state, "running")

    def test_reconcile_inventory_promotes_completed_background_publication(
        self,
    ) -> None:
        from tests.test_control_plane import _portable_snapshot

        snapshot = _portable_snapshot(
            "sandbox-1",
            create_operation_id="create-1",
        )
        with routing_store() as store:
            existing = store.upsert_sandbox(
                sandbox_route(
                    sandbox_id="sandbox-1",
                    node_id="node-1",
                    job_id="job-1",
                    node_url="http://node-1:8090",
                    state="parked",
                    generation=1,
                    create_operation_id="create-1",
                    resources=snapshot.manifest.spec.requested_resources(),
                    spec=snapshot.manifest.spec.to_dict(),
                    spec_hash=snapshot.manifest.spec_sha256,
                )
            )
            removed, stale_snapshots = store.reconcile_sandboxes_for_node(
                existing.node_url,
                [
                    SandboxInventoryEntry(
                        sandbox_id=existing.sandbox_id,
                        generation=existing.generation,
                        operation_id=existing.create_operation_id,
                        spec_hash=existing.spec_hash,
                        state="parked",
                        storage_schema="storage-native-v1",
                        snapshot_manifest_digest=(
                            snapshot.publication.manifest_digest
                        ),
                        snapshot_repository=snapshot.publication.repository,
                        snapshot_tag=snapshot.publication.tag,
                        storage_snapshot=snapshot.to_dict(),
                    )
                ],
                node_id=existing.node_id,
                job_id=existing.job_id,
                reported_sandbox_ids={existing.sandbox_id},
                observed_at=utc_now().isoformat(),
            )
            published = store.get_sandbox_readonly(existing.sandbox_id)

            assert published is not None
            removed_after_wake, stale_after_wake = store.reconcile_sandboxes_for_node(
                existing.node_url,
                [
                    SandboxInventoryEntry(
                        sandbox_id=existing.sandbox_id,
                        generation=existing.generation,
                        operation_id=existing.create_operation_id,
                        spec_hash=existing.spec_hash,
                        state="running",
                    )
                ],
                node_id=existing.node_id,
                job_id=existing.job_id,
                reported_sandbox_ids={existing.sandbox_id},
                observed_at=utc_now().isoformat(),
            )

        self.assertEqual(removed, [])
        self.assertEqual(stale_snapshots, [])
        self.assertEqual(published.snapshot_tag, snapshot.publication.tag)
        self.assertEqual(published.storage_snapshot, snapshot.to_dict())
        self.assertEqual(removed_after_wake, [])
        self.assertEqual(stale_after_wake, [published])

    def test_reconcile_inventory_rejects_snapshot_for_another_incarnation(
        self,
    ) -> None:
        from tests.test_control_plane import _portable_snapshot

        owned = _portable_snapshot(
            "sandbox-1",
            create_operation_id="create-1",
        )
        foreign = _portable_snapshot(
            "sandbox-1",
            generation=2,
            create_operation_id="create-2",
        )
        with routing_store() as store:
            existing = store.upsert_sandbox(
                sandbox_route(
                    sandbox_id="sandbox-1",
                    node_id="node-1",
                    job_id="job-1",
                    node_url="http://node-1:8090",
                    state="parked",
                    generation=1,
                    create_operation_id="create-1",
                    resources=owned.manifest.spec.requested_resources(),
                    spec=owned.manifest.spec.to_dict(),
                    spec_hash=owned.manifest.spec_sha256,
                )
            )

            store.reconcile_sandboxes_for_node(
                existing.node_url,
                [
                    SandboxInventoryEntry(
                        sandbox_id=existing.sandbox_id,
                        generation=existing.generation,
                        operation_id=existing.create_operation_id,
                        spec_hash=existing.spec_hash,
                        state="parked",
                        storage_schema="storage-native-v1",
                        snapshot_manifest_digest=(
                            foreign.publication.manifest_digest
                        ),
                        snapshot_repository=foreign.publication.repository,
                        snapshot_tag=foreign.publication.tag,
                        storage_snapshot=foreign.to_dict(),
                    )
                ],
                node_id=existing.node_id,
                job_id=existing.job_id,
                reported_sandbox_ids={existing.sandbox_id},
                observed_at=utc_now().isoformat(),
            )
            stored = store.get_sandbox_readonly(existing.sandbox_id)

        assert stored is not None
        self.assertEqual(stored.state, "parked")
        self.assertEqual(stored.storage_snapshot, {})
        self.assertFalse(is_portable_parked_route(stored))

    def test_transient_inventory_cannot_regress_stable_lifecycle_state(self) -> None:
        with routing_store() as store:
            current = store.upsert_sandbox(
                sandbox_route(
                    sandbox_id="sandbox-1",
                    node_id="node-1",
                    job_id="job-1",
                    node_url="http://node-1:8090",
                    state="running",
                    generation=1,
                    create_operation_id="create-1",
                    spec_hash="a" * 64,
                    node_epoch="epoch-1",
                    activity_epoch=1,
                )
            )

            store.reconcile_sandboxes_for_node(
                current.node_url,
                [
                    SandboxInventoryEntry(
                        sandbox_id=current.sandbox_id,
                        generation=current.generation,
                        operation_id=current.create_operation_id,
                        spec_hash=current.spec_hash,
                        state="restoring",
                    )
                ],
                node_id=current.node_id,
                job_id=current.job_id,
                reported_sandbox_ids={current.sandbox_id},
                observed_at=utc_now().isoformat(),
                node_epoch=current.node_epoch,
                activity_epoch=current.activity_epoch + 1,
                inventory_complete=True,
            )
            stored = store.get_sandbox_readonly(current.sandbox_id)

        assert stored is not None
        self.assertEqual(stored.state, "running")
        self.assertEqual(stored.activity_epoch, current.activity_epoch)

    def test_delete_sandboxes_for_jobs_removes_routes_and_dependents(self) -> None:
        with routing_store() as store:
            now = utc_now().isoformat()
            seed_routing_state(
                store,
                RoutingState(
                    sandboxes={
                        "remove-me": sandbox_route(
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
                        "keep-me": sandbox_route(
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
                ),
            )

            removed = store.delete_sandboxes_for_jobs(["job-1"])
            state = store.load()
            removed_exec = store.get_exec("exec-remove")
            kept_exec = store.get_exec("exec-keep")

        self.assertEqual([route.sandbox_id for route in removed], ["remove-me"])
        self.assertNotIn("remove-me", state.sandboxes)
        self.assertNotIn("remove-me", state.pending)
        self.assertNotIn("exec-remove", state.exec_sessions)
        self.assertIsNone(removed_exec)
        self.assertIn("keep-me", state.sandboxes)
        self.assertIn("exec-keep", state.exec_sessions)
        self.assertIsNotNone(kept_exec)

    def test_node_loss_keeps_only_fully_published_parked_route(self) -> None:
        with routing_store() as store:
            live = store.upsert_sandbox(
                sandbox_route(
                    sandbox_id="live-lost",
                    node_id="lost-node",
                    job_id="lost-job",
                    node_url="http://lost-node:8090",
                    state="running",
                    generation=1,
                    create_operation_id="create-live",
                    spec_hash="a" * 64,
                )
            )
            portable = store.upsert_sandbox(
                sandbox_route(
                    sandbox_id="portable-parked",
                    node_id="lost-node",
                    job_id="lost-job",
                    node_url="http://lost-node:8090",
                    state="parked",
                    generation=2,
                    create_operation_id="create-parked",
                    spec_hash="b" * 64,
                    storage_schema="storage-native-v1",
                    snapshot_manifest_digest="sha256:" + "c" * 64,
                    snapshot_repository="snapshots",
                    snapshot_tag="sandbox-2",
                    storage_snapshot={"schema": "storage-native-v1"},
                )
            )
            local_park = store.upsert_sandbox(
                sandbox_route(
                    sandbox_id="local-parked",
                    node_id="lost-node",
                    job_id="lost-job",
                    node_url="http://lost-node:8090",
                    state="parked",
                    generation=3,
                    create_operation_id="create-local",
                    spec_hash="d" * 64,
                )
            )
            for route in (live, portable, local_park):
                store.upsert_exec(
                    ExecRoute(
                        session_id=f"exec-{route.sandbox_id}",
                        sandbox_id=route.sandbox_id,
                        node_id=route.node_id,
                        job_id=route.job_id,
                        node_url=route.node_url,
                    )
                )

            removed = store.delete_sandboxes_for_jobs_with_error(
                ["lost-job"],
                terminal_error="node_lost",
            )
            state = store.load()

        self.assertTrue(is_portable_parked_route(portable))
        self.assertEqual(
            [route.sandbox_id for route in removed],
            ["live-lost", "local-parked"],
        )
        self.assertEqual(set(state.sandboxes), {"portable-parked"})
        self.assertEqual(
            state.sandboxes["portable-parked"].worker_state,
            "detached",
        )
        self.assertEqual(state.exec_sessions, {})

    def test_worker_detach_is_generation_fenced_and_idempotent(self) -> None:
        with routing_store() as store:
            route = store.upsert_sandbox(
                sandbox_route(
                    sandbox_id="portable-parked",
                    node_id="node-1",
                    job_id="job-1",
                    node_url="http://node-1:8090",
                    state="parked",
                    storage_schema="storage-native-v1",
                    snapshot_manifest_digest="sha256:" + "c" * 64,
                    snapshot_repository="snapshots",
                    snapshot_tag="sandbox-1",
                    storage_snapshot={"schema": "storage-native-v1"},
                )
            )
            fenced = store.begin_sandbox_detach(route)
            replayed_fence = store.begin_sandbox_detach(route)
            stale = replace(route, generation=route.generation + 1)
            stale_completion = store.complete_sandbox_detach(stale)
            assert fenced is not None
            completed = store.complete_sandbox_detach(fenced)
            replayed_completion = store.complete_sandbox_detach(fenced)

        assert replayed_fence is not None
        assert completed is not None
        assert replayed_completion is not None
        self.assertEqual(fenced.worker_state, "detaching")
        self.assertEqual(replayed_fence.worker_state, "detaching")
        self.assertIsNone(stale_completion)
        self.assertEqual(completed.worker_state, "detached")
        self.assertEqual(replayed_completion.worker_state, "detached")

    def test_delete_stale_sandboxes_removes_missing_jobs_after_grace(self) -> None:
        with routing_store() as store:
            now = utc_now()
            old = (now - timedelta(seconds=600)).isoformat()
            recent = (now - timedelta(seconds=30)).isoformat()
            seed_routing_state(
                store,
                RoutingState(
                    sandboxes={
                        "old-missing": sandbox_route(
                            sandbox_id="old-missing",
                            node_id="old-node",
                            job_id="old-job",
                            node_url="http://old-node:8090",
                            created_at=old,
                            updated_at=old,
                        ),
                        "recent-missing": sandbox_route(
                            sandbox_id="recent-missing",
                            node_id="recent-node",
                            job_id="recent-job",
                            node_url="http://recent-node:8090",
                            created_at=recent,
                            updated_at=recent,
                        ),
                        "active-job": sandbox_route(
                            sandbox_id="active-job",
                            node_id="active-node",
                            job_id="active-job",
                            node_url="http://active-node:8090",
                            created_at=old,
                            updated_at=old,
                        ),
                        "fresh-node": sandbox_route(
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
                ),
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

    def test_non_sqlite_state_fails_closed(self) -> None:
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

            with self.assertRaisesRegex(sqlite3.DatabaseError, "not a SQLite"):
                RoutingStore(route_file)
            self.assertTrue(route_file.exists())
            self.assertEqual(
                json.loads(route_file.read_text(encoding="utf-8"))["sandboxes"],
                [],
            )

    def test_unversioned_sqlite_state_fails_closed(self) -> None:
        with TemporaryDirectory() as raw_dir:
            route_file = Path(raw_dir) / "routes.sqlite"
            with sqlite3.connect(route_file) as conn:
                conn.execute("CREATE TABLE obsolete_state (value TEXT)")

            with self.assertRaisesRegex(
                sqlite3.DatabaseError,
                "unsupported routing schema version 0",
            ):
                RoutingStore(route_file)

    def test_route_claims_image_specific_capacity_before_generic_capacity(
        self,
    ) -> None:
        resources = ResourceQuantity(vcpu=1, memory_mb=512, disk_mb=1024)
        image = "registry.example.org/workload:latest"
        with routing_store() as store:
            store.upsert_prepared_capacity(
                "generic-older",
                resources,
                count=1,
                ttl_seconds=600,
            )
            store.upsert_prepared_capacity(
                "image-newer",
                resources,
                count=1,
                ttl_seconds=600,
                image=image,
            )
            allocation = sandbox_allocation(
                sandbox_id="sandbox-1",
                node_id="node-1",
                job_id="job-1",
                node_url="http://node-1:8090",
                resources=resources,
                spec={"id": "sandbox-1", "image": image},
            )
            before = store.pending_demand()
            first = allocate_sandbox_create(store, allocation, spec_hash="1" * 64)
            replay = allocate_sandbox_create(store, allocation, spec_hash="1" * 64)

            remaining = store.prepared_capacity()
            store.delete_prepared_capacity("generic-older")
            after_delete = store.pending_demand()

        self.assertEqual(before.prepared_resources, resources + resources)
        self.assertEqual(first, replay)
        self.assertEqual(
            [item.prepare_id for item in remaining],
            ["generic-older"],
        )
        self.assertEqual(after_delete.prepared_resources, ResourceQuantity())

    def test_image_warmup_survives_automatic_prepared_capacity_claim(self) -> None:
        with routing_store() as store:
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
            allocate_sandbox_create(
                store,
                sandbox_allocation(
                    sandbox_id="sandbox-1",
                    node_id="node-1",
                    job_id="job-1",
                    node_url="http://node-1:8090",
                    resources=ResourceQuantity(vcpu=1, memory_mb=512, disk_mb=1024),
                    spec={
                        "id": "sandbox-1",
                        "image": "registry.example.org/image:latest",
                    },
                ),
                spec_hash="1" * 64,
            )
            remaining = store.prepared_capacity()
            warmups_after_claim = store.image_warmups()
            store.upsert_image_warmup(
                "prep-1",
                "registry.example.org/replacement:latest",
                ResourceQuantity(vcpu=1, memory_mb=512, disk_mb=1024),
                count=4,
                ttl_seconds=600,
            )
            stale = store.mark_image_warmup_node(
                "prep-1",
                "node-1",
                expected_image="registry.example.org/image:latest",
            )
            marked = store.mark_image_warmup_node(
                "prep-1",
                "node-1",
                expected_image="registry.example.org/replacement:latest",
            )
            deleted = store.delete_image_warmup("prep-1")
            warmups_after_delete = store.image_warmups()

        self.assertEqual(warmup.warmup_id, "prep-1")
        self.assertEqual(
            [(item.prepare_id, item.count) for item in remaining], [("prep-1", 3)]
        )
        self.assertEqual([item.warmup_id for item in warmups_after_claim], ["prep-1"])
        self.assertIsNone(stale)
        self.assertEqual(marked.warmed_node_ids, ("node-1",))
        self.assertEqual(deleted.warmed_node_ids, ("node-1",))
        self.assertEqual(warmups_after_delete, [])

    def test_transient_capacity_signals_are_consumable(self) -> None:
        cases = (
            (
                "builder",
                lambda store: store.upsert_prepared_builder(
                    "signal", count=2, ttl_seconds=600
                ),
                lambda store: store.prepared_builder_count(),
                lambda store: store.consume_prepared_builders(),
            ),
            (
                "image",
                lambda store: store.upsert_pending_image_build(
                    "signal", "registry.example.org/custom:latest"
                ),
                lambda store: store.pending_image_build_count(),
                lambda store: store.consume_pending_image_builds(),
            ),
        )
        for name, create, count, consume in cases:
            with self.subTest(signal=name), routing_store() as store:
                create(store)
                self.assertEqual(count(store), 2 if name == "builder" else 1)
                self.assertEqual(len(consume(store)), 1)
                self.assertEqual(count(store), 0)

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
            autoscaler_store.delete_prepared_capacity("prep-1")
            autoscaler_store.consume_prepared_builders()

            gateway_demand = gateway_store.pending_demand()
            gateway_pending_images = gateway_store.pending_image_build_count()
            gateway_prepared_builders = gateway_store.prepared_builder_count()

        self.assertEqual(gateway_demand.pending_resources, ResourceQuantity())
        self.assertEqual(gateway_demand.prepared_resources, ResourceQuantity())
        self.assertEqual(gateway_pending_images, 0)
        self.assertEqual(gateway_prepared_builders, 0)

    def test_expired_signals_are_pruned_by_their_public_read_paths(self) -> None:
        now = utc_now()
        resources = ResourceQuantity(vcpu=1, memory_mb=512)
        cases = (
            (
                "prepared capacity",
                2,
                lambda store: store.upsert_prepared_capacity(
                    "expired", resources, count=2, ttl_seconds=1
                ),
                lambda store: store.pending_demand().prepared_resources,
                ResourceQuantity(),
                "prepared",
            ),
            (
                "prepared builder",
                2,
                lambda store: store.upsert_prepared_builder(
                    "expired", count=1, ttl_seconds=1
                ),
                lambda store: store.prepared_builder_count(),
                0,
                "prepared_builders",
            ),
            (
                "pending sandbox",
                PENDING_DEMAND_TTL_SECONDS + 1,
                lambda store: store.upsert_pending("expired", resources),
                lambda store: store.pending_demand().pending_resources,
                ResourceQuantity(),
                "pending",
            ),
            (
                "pending image build",
                PENDING_DEMAND_TTL_SECONDS + 1,
                lambda store: store.upsert_pending_image_build(
                    "expired", "registry.example.org/expired:latest"
                ),
                lambda store: store.pending_image_build_count(),
                0,
                "image_builds",
            ),
            (
                "image warmup",
                2,
                lambda store: store.upsert_image_warmup(
                    "expired",
                    "registry.example.org/expired:latest",
                    resources,
                    count=1,
                    ttl_seconds=1,
                ),
                lambda store: store.image_warmups(),
                [],
                "image_warmups",
            ),
        )

        for name, age_seconds, create, observe, expected, collection in cases:
            with self.subTest(signal=name), routing_store() as store:
                with patch(
                    "ucloud_sandboxes.routing.utc_now",
                    return_value=now - timedelta(seconds=age_seconds),
                ):
                    create(store)
                self.assertEqual(observe(store), expected)
                self.assertEqual(getattr(store.load(), collection), {})

    def test_allocation_returns_and_consumes_pending_demand_atomically(self) -> None:
        resources = ResourceQuantity(vcpu=1, memory_mb=512, disk_mb=1024)
        with routing_store() as store:
            _first, first_demand = store.upsert_pending_with_demand(
                "pending-one", resources
            )
            repeated, repeated_demand = store.upsert_pending_with_demand(
                "pending-one", resources
            )
            route, pending = store.allocate_sandbox_create_with_pending(
                sandbox_allocation(
                    sandbox_id="pending-one",
                    node_id="node-1",
                    job_id="job-1",
                    node_url="http://node-1:8090",
                    resources=resources,
                    spec={"id": "pending-one", "image": "busybox"},
                ),
                spec_hash="1" * 64,
            )
            after = store.get_pending("pending-one")

        self.assertEqual(first_demand.pending_resources, resources)
        self.assertEqual(repeated_demand.pending_resources, resources)
        self.assertEqual(repeated.attempts, 2)
        self.assertEqual(route.sandbox_id, "pending-one")
        self.assertEqual(pending, repeated)
        self.assertIsNone(after)

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
                spec_hash="4" * 64,
                failure_reason="image_pull_http_502",
            )
            store.upsert_pending(
                "pending-one",
                resources,
                generation=4,
                operation_id="create-4",
                spec_hash="4" * 64,
                failure_reason="image_pull_http_503",
            )
            replay = RoutingStore(path).get_pending("pending-one")
            assert replay is not None
            self.assertEqual(replay.attempts, 2)
            self.assertEqual(replay.generation, 4)
            self.assertEqual(replay.operation_id, "create-4")
            self.assertEqual(replay.spec_hash, "4" * 64)
            self.assertEqual(replay.failure_reason, "image_pull_http_503")

            store.upsert_pending(
                "pending-one",
                resources,
                generation=5,
                operation_id="create-5",
                spec_hash="5" * 64,
                failure_reason="registry_lease_unavailable",
            )
            replacement = RoutingStore(path).get_pending("pending-one")

        assert replacement is not None
        self.assertEqual(replacement.attempts, 1)
        self.assertEqual(replacement.generation, 5)
        self.assertEqual(replacement.operation_id, "create-5")

    def test_post_placement_failures_do_not_request_fleet_capacity(self) -> None:
        with routing_store() as store:
            capacity = ResourceQuantity(vcpu=1, memory_mb=512, disk_mb=1024)
            failed = ResourceQuantity(vcpu=4, memory_mb=8192, disk_mb=33_856)
            store.upsert_pending("no-node", capacity)
            store.upsert_pending(
                "pull-failed",
                failed,
                failure_reason="image_pull_http_503",
            )
            store.upsert_pending(
                "lease-failed",
                failed,
                failure_reason="registry_lease_unavailable",
            )
            store.upsert_pending(
                "publication-pending",
                failed,
                failure_reason="wake_snapshot_publication_pending",
            )

            demand = store.pending_demand()

        self.assertEqual(demand.pending_resources, capacity)
        self.assertEqual(demand.pending_count, 1)
        self.assertEqual(
            demand.suppressed_pending_resources,
            failed + failed + failed,
        )
        self.assertEqual(demand.suppressed_pending_count, 3)
        self.assertEqual(len(demand.placement_requests), 1)

    def test_snapshot_consume_does_not_delete_refreshed_signals(self) -> None:
        with routing_store() as store:
            store.upsert_pending(
                "pending-one",
                ResourceQuantity(vcpu=1, memory_mb=512, disk_mb=1024),
            )
            store.upsert_pending_image_build("image-one", "registry/image:old")
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
            store.upsert_prepared_builder(
                "builder-one",
                count=2,
                ttl_seconds=600,
            )

            consumed_pending = store.consume_pending_demand(snapshot.pending.values())
            consumed_images = store.consume_pending_image_builds(
                snapshot.image_builds.values()
            )
            consumed_builders = store.consume_prepared_builders(
                snapshot.prepared_builders.values()
            )
            remaining = store.load()

        self.assertEqual(consumed_pending, [])
        self.assertEqual(consumed_images, [])
        self.assertEqual(consumed_builders, [])
        self.assertEqual(remaining.pending["pending-one"].attempts, 2)
        self.assertEqual(remaining.image_builds["image-one"].attempts, 2)
        self.assertEqual(remaining.image_builds["image-one"].tag, "registry/image:new")
        self.assertEqual(remaining.prepared_builders["builder-one"].count, 2)

    def test_stale_inventory_cannot_overwrite_or_delete_newer_generation(self) -> None:
        with routing_store() as store:
            current = sandbox_route(
                sandbox_id="versioned-one",
                node_id="node-1",
                job_id="job-1",
                node_url="http://node-1:8090",
                resources=ResourceQuantity(vcpu=2, memory_mb=1024, disk_mb=2048),
                spec={"id": "versioned-one", "image": "busybox"},
                state="running",
                generation=2,
                create_operation_id="create-2",
                spec_hash="2" * 64,
                node_epoch="epoch-1",
                activity_epoch=5,
            )
            store.upsert_sandbox(current)
            store.reconcile_sandboxes_for_node(
                current.node_url,
                [
                    SandboxInventoryEntry(
                        sandbox_id=current.sandbox_id,
                        state="running",
                        generation=1,
                        operation_id="create-1",
                        spec_hash="1" * 64,
                    )
                ],
                node_id=current.node_id,
                job_id=current.job_id,
                reported_sandbox_ids={current.sandbox_id},
                observed_at=utc_now().isoformat(),
                node_epoch="epoch-1",
                activity_epoch=4,
                inventory_complete=True,
            )
            after_stale_entry = store.get_sandbox_readonly(current.sandbox_id)
            store.reconcile_sandboxes_for_node(
                current.node_url,
                [],
                node_id=current.node_id,
                job_id=current.job_id,
                reported_sandbox_ids=set(),
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
            self.assertEqual(route.spec_hash, "2" * 64)
            self.assertEqual(route.resources, current.resources)

    def test_same_generation_update_requires_exact_nonempty_identity(self) -> None:
        with routing_store() as store:
            current = sandbox_route(
                sandbox_id="versioned-one",
                node_id="node-1",
                job_id="job-1",
                node_url="http://node-1:8090",
                state="running",
                generation=4,
                create_operation_id="create-4",
                spec_hash="4" * 64,
                node_epoch="epoch-1",
                activity_epoch=8,
            )
            store.upsert_sandbox(current)

            for create_operation_id, spec_hash in (("", "4" * 64), ("create-4", "")):
                with self.assertRaises(ValueError):
                    store.upsert_sandbox(
                        sandbox_route(
                            **{
                                **current.__dict__,
                                "state": "stopped",
                                "create_operation_id": create_operation_id,
                                "spec_hash": spec_hash,
                            }
                        )
                    )
            for create_operation_id, spec_hash in (
                ("different", "4" * 64),
                ("create-4", "d" * 64),
            ):
                result = store.upsert_sandbox(
                    sandbox_route(
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
        self.assertEqual(stored.spec_hash, "4" * 64)

    def test_exact_identity_adopts_new_node_epoch_then_allows_absence(self) -> None:
        with routing_store() as store:
            first = allocate_sandbox_create(
                store,
                sandbox_allocation(
                    sandbox_id="survived-restart",
                    node_id="node-1",
                    job_id="job-1",
                    node_url="http://node-1:8090",
                    spec={"id": "survived-restart", "image": "busybox"},
                ),
                spec_hash="1" * 64,
                create_operation_id="create-1",
            )
            store.upsert_sandbox(
                sandbox_route(
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
                    SandboxInventoryEntry(
                        sandbox_id=first.sandbox_id,
                        state="running",
                        generation=first.generation,
                        operation_id=first.create_operation_id,
                        spec_hash=first.spec_hash,
                    )
                ],
                node_id=first.node_id,
                job_id=first.job_id,
                reported_sandbox_ids={first.sandbox_id},
                observed_at=adopted_at.isoformat(),
                node_epoch="epoch-after-restart",
                activity_epoch=1,
                inventory_complete=True,
            )
            adopted = store.get_sandbox_readonly(first.sandbox_id)
            store.reconcile_sandboxes_for_node(
                first.node_url,
                [],
                node_id=first.node_id,
                job_id=first.job_id,
                reported_sandbox_ids=set(),
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

    def test_reconcile_transaction_cannot_delete_concurrent_new_incarnation(
        self,
    ) -> None:
        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "routes.sqlite"
            reconciling_store = RoutingStore(path)
            writer_store = RoutingStore(path)
            first = allocate_sandbox_create(
                writer_store,
                sandbox_allocation(
                    sandbox_id="reused-id",
                    node_id="node-1",
                    job_id="job-1",
                    node_url="http://node-1:8090",
                    spec={"id": "reused-id", "image": "busybox"},
                ),
                spec_hash="1" * 64,
                create_operation_id="create-1",
            )
            writer_store.upsert_sandbox(
                sandbox_route(**{**first.__dict__, "state": "running"})
            )
            snapshot_reached = Event()
            allow_reconcile_to_continue = Event()
            original_scoped_load = (
                reconciling_store._sandbox_routes_for_node_url_unlocked
            )
            loaded_node_urls: list[str] = []

            def pause_at_snapshot(conn, node_url):
                loaded_node_urls.append(node_url)
                snapshot = original_scoped_load(conn, node_url)
                snapshot_reached.set()
                self.assertTrue(allow_reconcile_to_continue.wait(timeout=5))
                return snapshot

            reconciling_store._sandbox_routes_for_node_url_unlocked = pause_at_snapshot

            def replace_incarnation() -> SandboxRoute:
                writer_store.delete_sandbox_if_current(
                    "reused-id",
                    generation=1,
                    create_operation_id="create-1",
                )
                return allocate_sandbox_create(
                    writer_store,
                    sandbox_allocation(
                        sandbox_id="reused-id",
                        node_id="node-1",
                        job_id="job-1",
                        node_url="http://node-1:8090",
                        spec={"id": "reused-id", "image": "python"},
                    ),
                    spec_hash="2" * 64,
                    create_operation_id="create-2",
                )

            observed_at = (utc_now() + timedelta(seconds=1)).isoformat()
            with ThreadPoolExecutor(max_workers=2) as executor:
                reconciliation = executor.submit(
                    reconciling_store.reconcile_sandboxes_for_node,
                    first.node_url,
                    [],
                    node_id=first.node_id,
                    job_id=first.job_id,
                    reported_sandbox_ids=set(),
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
        self.assertEqual(stored.spec_hash, "2" * 64)
        self.assertEqual(loaded_node_urls, [first.node_url])

    def test_concurrent_different_spec_allocation_rejects_loser_atomically(
        self,
    ) -> None:
        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "routes.sqlite"
            first_store = RoutingStore(path)
            second_store = RoutingStore(path)

            def allocate(store: RoutingStore, image: str, spec_hash: str):
                return allocate_sandbox_create(
                    store,
                    sandbox_allocation(
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
                    executor.submit(allocate, first_store, "busybox", "a" * 64),
                    executor.submit(allocate, second_store, "python", "b" * 64),
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
