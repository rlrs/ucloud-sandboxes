"""Wake reservation contracts exercised without an HTTP handler."""

from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
import sqlite3
import unittest
from unittest.mock import patch

from tests.test_control_plane import _sandbox_route
from ucloud_sandboxes.models import NodeHeartbeat, ResourceQuantity, utc_now
from ucloud_sandboxes.routing import RoutingStore, wake_pending_demand_id
from ucloud_sandboxes.wake_admission import WakeAdmission


class WakeAdmissionTests(unittest.TestCase):
    def setUp(self):
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.store = RoutingStore(Path(temporary.name) / "routes.sqlite")
        self.owner = NodeHeartbeat(
            node_id="node",
            job_id="job",
            node_url="http://node",
            deployment_id="test",
            updated_at=utc_now(),
            active_sandboxes=0,
            capabilities=("sandbox",),
        )
        self.routes = [
            self.store.upsert_sandbox(
                _sandbox_route(
                    sandbox_id=f"sandbox-{index}",
                    node_id="node",
                    job_id="job",
                    node_url="http://node",
                    state="parked",
                    resources=ResourceQuantity(vcpu=1, memory_mb=1024, disk_mb=2048),
                    spec={"parkable": True, "managed_process": True},
                )
            )
            for index in range(3)
        ]
        self.reads = []
        self.demands = []
        self.before_commit = None
        self.admission = WakeAdmission(
            self.store,
            read_owner=self.read_owner,
            read_placement=self.read_placement,
            can_admit=self.can_admit,
            heartbeat_ttl_seconds=30,
            consolidation_enabled=False,
        )

    def read_owner(self, job_id):
        self.reads.append(("owner", job_id))
        return self.owner

    def read_placement(self, owner):
        self.reads.append(("placement", owner.job_id))
        return [
            self.store.get_sandbox_readonly(route.sandbox_id) for route in self.routes
        ]

    def can_admit(self, owner, occupants, resources):
        active_mb = sum(
            route.resources.memory_mb for route in occupants if route.state == "waking"
        )
        self.demands.append(active_mb)
        if self.before_commit is not None:
            callback, self.before_commit = self.before_commit, None
            callback()
        return active_mb + resources.memory_mb <= 2048

    def test_duplicate_wakes_share_reservation_and_project_capacity_once(self):
        results = self.admission.reserve_batch(
            [self.routes[0], self.routes[0], *self.routes[1:]]
        )
        self.assertEqual(
            [row.route.state if row.route else None for row in results],
            ["waking", "waking", "waking", None],
        )
        self.assertEqual(self.demands, [0, 1024, 2048])
        self.assertEqual(self.reads, [("owner", "job"), ("placement", "job")])
        self.assertEqual(results[0].route, results[1].route)
        self.assertEqual(results[0].owner_view.occupants[0].state, "parked")
        self.assertEqual(results[2].owner_view.occupants[0].state, "waking")
        self.assertEqual(
            [
                self.store.get_sandbox_readonly(row.sandbox_id).state
                for row in self.routes
            ],
            ["waking", "waking", "parked"],
        )

    def test_owner_state_and_request_identity_must_still_be_eligible(self):
        for changes in (
            {"draining": True},
            {"admission_open": False},
            {"capabilities": ()},
            {"updated_at": utc_now() - timedelta(seconds=60)},
        ):
            with self.subTest(changes=changes):
                original = self.owner
                self.owner = replace(original, **changes)
                self.assertIsNone(
                    self.admission.reserve_batch([self.routes[0]])[0].route
                )
                self.owner = original
        for changes in (
            {"generation": 2},
            {"node_id": "other"},
            {"create_operation_id": "other"},
            {"spec_hash": "b" * 64},
        ):
            with self.subTest(changes=changes):
                self.assertIsNone(
                    self.admission.reserve_batch([replace(self.routes[0], **changes)])[
                        0
                    ].route
                )
        self.assertEqual(self.demands, [])

    def test_active_migration_excludes_local_reservation_without_consuming_capacity(
        self,
    ):
        route = self.routes[0]
        self.store.begin_sandbox_migration(
            route,
            migration_id="migration",
            destination_node_id="other-node",
            destination_job_id="other-job",
            destination_node_url="http://other",
        )
        results = self.admission.reserve_batch(self.routes)
        self.assertIsNone(results[0].route)
        self.assertEqual([row.route.state for row in results[1:]], ["waking", "waking"])
        self.assertEqual(self.demands, [0, 1024])
        self.assertEqual(
            self.store.get_sandbox_readonly(route.sandbox_id).state, "parked"
        )

    def test_unrelated_migration_is_not_decoded_for_local_admission(self):
        from ucloud_sandboxes import routing

        other = self.store.upsert_sandbox(
            replace(
                self.routes[0],
                sandbox_id="unrelated",
                spec={**self.routes[0].spec, "id": "unrelated"},
            )
        )
        self.store.begin_sandbox_migration(
            other,
            migration_id="other-migration",
            destination_node_id="other",
            destination_job_id="other",
            destination_node_url="http://other",
        )
        with patch(
            "ucloud_sandboxes.routing._sandbox_migration_from_row",
            wraps=routing._sandbox_migration_from_row,
        ) as decode:
            result = self.admission.reserve_batch([self.routes[0]])
            self.assertEqual(result[0].route.state, "waking")
            self.assertEqual(decode.call_count, 0)

    def test_delete_racing_inventory_read_is_fenced_by_atomic_commit(self):
        route = self.routes[0]
        self.store.upsert_pending_with_demand(
            wake_pending_demand_id(route.sandbox_id), route.resources
        )
        self.before_commit = lambda: self.store.upsert_sandbox(
            replace(route, state="deleting", delete_operation_id="delete")
        )
        result = self.admission.reserve_batch([route])[0]
        self.assertIsNone(result.route)
        self.assertEqual(
            self.store.get_sandbox_readonly(route.sandbox_id).state, "deleting"
        )
        with self.store._connect() as connection:
            self.assertEqual(
                connection.execute("SELECT count(*) FROM pending").fetchone()[0], 1
            )

    def test_failed_batch_commit_leaves_all_routes_and_demand_unchanged(self):
        for route in self.routes:
            self.store.upsert_pending_with_demand(
                wake_pending_demand_id(route.sandbox_id), route.resources
            )
        with self.store._connect() as connection:
            connection.execute("""CREATE TRIGGER fail_batch BEFORE UPDATE ON sandboxes
                WHEN NEW.sandbox_id='sandbox-1' AND NEW.state='waking'
                BEGIN SELECT RAISE(ABORT, 'injected batch failure'); END""")
        with self.assertRaises(sqlite3.IntegrityError):
            self.admission.reserve_batch(self.routes)
        self.assertEqual(
            [
                self.store.get_sandbox_readonly(row.sandbox_id).state
                for row in self.routes
            ],
            ["parked"] * 3,
        )
        with self.store._connect() as connection:
            self.assertEqual(
                connection.execute("SELECT count(*) FROM pending").fetchone()[0], 3
            )

    def test_single_reservation_reobserves_completed_wake_and_rejects_deleted_route(
        self,
    ):
        route = self.routes[0]
        waking = self.admission.reserve_current(route)
        self.assertEqual(waking.state, "waking")
        self.assertEqual(self.admission.reserve_current(route), waking)
        self.store.upsert_sandbox(
            replace(waking, state="deleting", delete_operation_id="delete")
        )
        self.assertIsNone(self.admission.reserve_current(route))

    def test_single_reservation_cannot_return_replacement_incarnation_after_cas_loss(
        self,
    ):
        route = self.routes[0]
        for changes in (
            {"generation": route.generation + 1},
            {"create_operation_id": "replacement"},
            {"spec_hash": "b" * 64},
        ):
            with self.subTest(changes=changes):
                replacement = replace(route, state="running", **changes)
                self.store.delete_sandbox(route.sandbox_id)
                replacement = self.store.upsert_sandbox(replacement)
                self.assertIsNone(self.admission.reserve_current(route))
                self.assertEqual(
                    self.store.get_sandbox_readonly(route.sandbox_id), replacement
                )
        deleting = replace(route, state="running", delete_operation_id="delete")
        self.store.delete_sandbox(route.sandbox_id)
        self.store.upsert_sandbox(deleting)
        self.assertIsNone(self.admission.reserve_current(route))
