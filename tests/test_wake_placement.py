from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from tests.test_control_plane import _portable_snapshot, _sandbox_route, build_heartbeat
from ucloud_sandboxes.control_state import ControlStateStore
from ucloud_sandboxes.models import ResourceQuantity
from ucloud_sandboxes.routing import RoutingStore, wake_pending_demand_id
from ucloud_sandboxes.wake_admission import WakeAdmission
from ucloud_sandboxes.wake_placement import (
    BlockedOwnerRefresh,
    WakePlaced,
    WakePlacement,
    WakePlacementPorts,
    WakePlacementStopped,
    WakeUnavailable,
)


class WakePlacementTests(unittest.TestCase):
    """Real durable routes and typed worker observations, without an HTTP handler."""

    def setUp(self):
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        self.routes = RoutingStore(root / "routes.sqlite")
        self.control = ControlStateStore(root / "control.sqlite")
        self.owner = build_heartbeat(
            node_id="source",
            job_id="source-job",
            node_url="http://source",
            capabilities=("sandbox", "disk-quota"),
        )
        self.control.upsert_heartbeat(self.owner)
        self.destination = replace(
            self.owner,
            node_id="destination",
            job_id="destination-job",
            node_url="http://destination",
        )
        self.control.upsert_heartbeat(self.destination)
        self.route = self.routes.upsert_sandbox(
            _sandbox_route(
                sandbox_id="waiting",
                node_id=self.owner.node_id,
                job_id=self.owner.job_id,
                node_url=self.owner.node_url,
                state="parked",
                resources=ResourceQuantity(1, 1024, 4096),
            )
        )
        self.local_capacity = True
        self.destination_capacity = False
        self.lock_depth = 0
        self.calls = []
        self.admission = WakeAdmission(
            self.routes,
            read_owner=self.control.get_heartbeat,
            read_placement=lambda owner: self.routes.sandbox_routes_readonly(),
            can_admit=lambda *_: self.local_capacity,
            heartbeat_ttl_seconds=120,
            consolidation_enabled=False,
        )
        self.ports = WakePlacementPorts(
            reservation=self.reservation,
            owner=self.control.get_heartbeat,
            occupants=lambda _: self.routes.sandbox_routes_readonly(),
            destination=lambda *_args, **_kwargs: (
                self.destination if self.destination_capacity else None
            ),
            reserve_local=lambda _: None,
            finish_detach=self.unexpected,
            advance_migration=self.advance,
            refresh_capacity=lambda _: False,
            publish=self.unexpected,
            decode_publication=lambda _route, payload: payload["candidate"],
            observe_owner=lambda *_: None,
            observe_consolidation=lambda *_: None,
        )

    @contextmanager
    def reservation(self):
        self.lock_depth += 1
        try:
            yield
        finally:
            self.lock_depth -= 1

    def unexpected(self, *_args, **_kwargs):
        self.fail("unexpected worker operation")

    def advance(self, migration, **_kwargs):
        self.assertEqual(self.lock_depth, 0)
        self.calls.append(("advance", migration.migration_id))
        return migration

    def service(self, **ports):
        return WakePlacement(self.routes, self.admission, replace(self.ports, **ports))

    def portable(self):
        snapshot = _portable_snapshot(self.route.sandbox_id)
        return replace(
            self.route,
            storage_schema=snapshot.schema,
            snapshot_repository=snapshot.reference.repository,
            snapshot_tag=snapshot.reference.tag,
            snapshot_manifest_digest=snapshot.reference.manifest_digest,
            storage_snapshot=snapshot.to_dict(),
        )

    def test_local_wake_reserves_without_publication_or_migration(self):
        result = self.service().place(self.route)
        self.assertIsInstance(result, WakePlaced)
        self.assertEqual(result.route.state, "waking")
        self.assertFalse(result.owner_changed)
        self.assertEqual(
            self.routes.get_sandbox_readonly(self.route.sandbox_id), result.route
        )
        self.assertEqual(self.calls, [])

    def test_refresh_runs_outside_reservation_then_reuses_actual_headroom(self):
        self.local_capacity = False

        def refresh(_route):
            self.assertEqual(self.lock_depth, 0)
            self.local_capacity = True
            return True

        result = self.service(refresh_capacity=refresh).place(self.route)
        self.assertIsInstance(result, WakePlaced)
        self.assertEqual(result.route.state, "waking")
        self.assertEqual(self.calls, [])

    def test_full_fleet_keeps_demand_without_uploading_unusable_checkpoint(self):
        self.local_capacity = False
        result = self.service().place(self.route)
        self.assertIsInstance(result, WakeUnavailable)
        self.assertEqual(result.error_code, "wake_destination_unavailable")
        self.assertIsNotNone(
            self.routes.get_pending(wake_pending_demand_id(self.route.sandbox_id))
        )
        self.assertEqual(
            self.routes.get_sandbox_readonly(self.route.sandbox_id).state, "parked"
        )
        self.assertEqual(self.calls, [])

    def test_publication_then_migration_uses_one_existing_durable_reservation(self):
        self.local_capacity = False
        self.destination_capacity = True

        def publish(_route):
            self.assertEqual(self.lock_depth, 0)
            self.calls.append(("publish", self.route.sandbox_id))
            return {"candidate": self.portable()}

        service = self.service(publish=publish)
        first = service.place(self.route)
        self.assertIsInstance(first, WakeUnavailable)
        self.assertIsNotNone(first.migration)
        self.assertEqual(first.migration.phase, "planned")
        second = service.place(self.route)
        self.assertEqual(second.migration.migration_id, first.migration.migration_id)
        self.assertEqual(
            [call[0] for call in self.calls], ["publish", "advance", "advance"]
        )
        self.assertEqual(len(self.routes.sandbox_migrations(active_only=True)), 1)
        self.assertIsNone(
            self.routes.get_pending(wake_pending_demand_id(self.route.sandbox_id))
        )

    def test_publication_cannot_overwrite_concurrent_wake_or_replacement(self):
        for replacement in (False, True):
            with self.subTest(replacement=replacement):
                self.routes.upsert_sandbox(self.route)
                candidate = self.portable()
                current = replace(self.route, state="running")
                if replacement:
                    self.routes.delete_sandbox(self.route.sandbox_id)
                    current = replace(
                        current,
                        generation=current.generation + 1,
                        create_operation_id="new",
                    )
                current = self.routes.upsert_sandbox(current)
                self.assertIsNone(
                    self.service().accept_publication(
                        self.route, {"candidate": candidate}
                    )
                )
                self.assertEqual(
                    self.routes.get_sandbox_readonly(self.route.sandbox_id), current
                )

    def test_inflight_publication_cannot_restore_old_capture_after_wake_repark(self):
        self.local_capacity = False
        self.destination_capacity = True
        self.route = self.routes.upsert_sandbox(
            replace(self.route, node_epoch="boot", activity_epoch=1)
        )
        candidate = self.portable()
        current = []

        def publish(_route):
            self.routes.upsert_sandbox(
                replace(self.route, state="running", activity_epoch=2)
            )
            current.append(
                self.routes.upsert_sandbox(
                    replace(self.route, state="parked", activity_epoch=3)
                )
            )
            return {"candidate": candidate}

        result = self.service(publish=publish).place(self.route)
        self.assertIsInstance(result, WakeUnavailable)
        self.assertEqual(result.error_code, "snapshot_publication_pending")
        self.assertEqual(
            self.routes.get_sandbox_readonly(self.route.sandbox_id), current[0]
        )
        self.assertEqual(self.calls, [])

    def test_repark_after_publication_commit_defers_without_second_upload(self):
        self.local_capacity = False
        self.destination_capacity = True
        self.route = self.routes.upsert_sandbox(
            replace(self.route, node_epoch="boot", activity_epoch=1)
        )
        publishes = []

        def publish(_route):
            publishes.append(True)
            return {"candidate": self.portable()}

        def reserve_local(route):
            if route.snapshot_manifest_digest:
                self.routes.upsert_sandbox(
                    replace(self.route, state="running", activity_epoch=2)
                )
                self.routes.upsert_sandbox(
                    replace(self.route, state="parked", activity_epoch=3)
                )
            return None

        result = self.service(publish=publish, reserve_local=reserve_local).place(
            self.route
        )
        self.assertIsInstance(result, WakeUnavailable)
        self.assertEqual(result.error_code, "snapshot_publication_pending")
        self.assertEqual(len(publishes), 1)
        self.assertEqual(self.calls, [])
        self.assertEqual(
            self.routes.get_sandbox_readonly(self.route.sandbox_id).activity_epoch, 3
        )

    def test_completed_migration_reserves_destination_and_reports_owner_change(self):
        self.local_capacity = False
        self.destination_capacity = True
        portable = self.routes.upsert_sandbox(self.portable())

        def complete(migration, **kwargs):
            self.assertEqual(self.lock_depth, 0)
            self.routes.advance_sandbox_migration(
                migration.migration_id, expected_phases={"planned"}, phase="staged"
            )
            self.routes.route_sandbox_migration(migration.migration_id)
            self.routes.advance_sandbox_migration(
                migration.migration_id, expected_phases={"routed"}, phase="activated"
            )
            return self.routes.complete_sandbox_migration(
                migration.migration_id, wake_destination=kwargs["wake_on_complete"]
            )

        result = self.service(advance_migration=complete).place(portable)
        self.assertIsInstance(result, WakePlaced)
        self.assertTrue(result.owner_changed)
        self.assertEqual(result.route.node_id, self.destination.node_id)
        self.assertEqual(result.route.state, "waking")

    def test_migration_completion_does_not_wake_recreated_id(self):
        self.local_capacity = False
        self.destination_capacity = True
        portable = self.routes.upsert_sandbox(self.portable())
        replacements = []

        def complete(migration, **_kwargs):
            self.assertEqual(self.lock_depth, 0)
            self.routes.delete_sandbox(portable.sandbox_id)
            replacements.append(
                self.routes.upsert_sandbox(
                    replace(
                        portable,
                        generation=portable.generation + 1,
                        create_operation_id="replacement",
                    )
                )
            )
            return replace(migration, phase="complete")

        result = self.service(advance_migration=complete).place(portable)
        self.assertIsInstance(result, WakeUnavailable)
        self.assertIn("route changed", result.message)
        self.assertEqual(
            self.routes.get_sandbox_readonly(portable.sandbox_id), replacements[0]
        )
        self.assertEqual(replacements[0].state, "parked")

    def test_completed_migration_replay_cannot_reserve_recreated_destination(self):
        migration = self.routes.begin_sandbox_migration(
            self.route,
            migration_id="completed",
            destination_node_id=self.destination.node_id,
            destination_job_id=self.destination.job_id,
            destination_node_url=self.destination.node_url,
        )
        self.routes.advance_sandbox_migration(
            migration.migration_id, expected_phases={"planned"}, phase="staged"
        )
        _, destination = self.routes.route_sandbox_migration(migration.migration_id)
        self.routes.advance_sandbox_migration(
            migration.migration_id, expected_phases={"routed"}, phase="activated"
        )
        self.assertEqual(
            self.routes.complete_sandbox_migration(migration.migration_id).phase,
            "complete",
        )
        self.routes.delete_sandbox(destination.sandbox_id)
        replacement = self.routes.upsert_sandbox(
            replace(
                destination,
                generation=destination.generation + 1,
                create_operation_id="replacement",
            )
        )
        self.routes.complete_sandbox_migration(
            migration.migration_id, wake_destination=True
        )
        self.assertEqual(
            self.routes.get_sandbox_readonly(destination.sandbox_id), replacement
        )
        self.assertEqual(replacement.state, "parked")

    def test_direct_reservation_revalidates_incarnation_before_commit(self):
        self.routes.delete_sandbox(self.route.sandbox_id)
        replacement = self.routes.upsert_sandbox(
            replace(
                self.route,
                generation=self.route.generation + 1,
                create_operation_id="replacement",
            )
        )
        with self.assertRaises(WakePlacementStopped):
            self.service().reserve(self.route)
        self.assertEqual(
            self.routes.get_sandbox_readonly(self.route.sandbox_id), replacement
        )

    def test_stale_detach_does_not_dispatch_to_recreated_id(self):
        detaching = replace(self.portable(), worker_state="detaching")
        self.routes.delete_sandbox(self.route.sandbox_id)
        replacement = self.routes.upsert_sandbox(
            replace(
                self.route,
                generation=self.route.generation + 1,
                create_operation_id="replacement",
            )
        )
        result = self.service().place(detaching)
        self.assertIsInstance(result, WakeUnavailable)
        self.assertEqual(
            self.routes.get_sandbox_readonly(self.route.sandbox_id), replacement
        )

    def test_migration_preparation_returns_domain_failure(self):
        self.local_capacity = False
        self.destination_capacity = True
        portable = self.routes.upsert_sandbox(self.portable())

        def prepare(_migration, **_kwargs):
            raise WakePlacementStopped(
                WakeUnavailable(
                    "image preparation pending", details={"image": "example"}
                )
            )

        result = self.service(advance_migration=prepare).place(portable)
        self.assertEqual(
            result,
            WakeUnavailable("image preparation pending", details={"image": "example"}),
        )
        self.assertEqual(len(self.routes.sandbox_migrations(active_only=True)), 1)

    def test_refresh_rejects_rebooted_worker_evidence(self):
        self.local_capacity = False
        changed_boot = replace(self.owner, node_epoch="other-boot")
        changed = BlockedOwnerRefresh.refresh(
            self.route,
            routes=self.routes,
            admission=self.admission,
            read_worker=lambda _: changed_boot,
            receive=self.unexpected,
        )
        self.assertFalse(changed)
