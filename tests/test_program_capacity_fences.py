"""Program observations must not serialize unrelated work on their worker."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Event
import unittest
from uuid import uuid4

from tests.test_routing import sandbox_route
from ucloud_sandboxes.shared_control.routing_repository import PostgresRoutingStore

DSN = os.environ.get("UCLOUD_TEST_POSTGRES_DSN")


@unittest.skipUnless(DSN, "requires isolated PostgreSQL")
class ProgramCapacityFenceTests(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.schema = "ucloud_routing_program_" + uuid4().hex
        self.store = PostgresRoutingStore(
            Path(self.temp.name) / "routes", dsn=DSN, schema=self.schema
        )
        self.store.migrate()
        self.route = sandbox_route(
            sandbox_id="s",
            node_id="n",
            job_id="j",
            node_url="http://node",
            state="running",
            node_epoch="epoch",
            activity_epoch=1,
        )
        self.store.upsert_sandbox(self.route)
        self.route = self.store.get_sandbox("s")

    def tearDown(self):
        import psycopg
        from psycopg import sql

        self.store.close()
        with psycopg.connect(DSN, autocommit=True) as conn:
            conn.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(self.schema))
            )
        self.temp.cleanup()

    def revisions(self):
        with self.store.pool.connection() as conn:
            return {
                r["identity"]: r["revision"]
                for r in conn.execute("SELECT * FROM worker_capacity_revisions")
            }

    def transition(self, state, **kwargs):
        return self.store.upsert_program_request_transition_with_change(
            self.route,
            request_id="request",
            rollout_id="rollout",
            state=state,
            **kwargs,
        )

    def test_only_active_membership_edges_advance_worker_revision(self):
        before = self.revisions()
        self.transition("model_wait")
        active = self.revisions()
        self.assertNotEqual(before, active)
        for state in ("ready_to_wake", "waking", "acting"):
            with self.subTest(state=state):
                self.assertTrue(self.transition(state)[1])
                self.assertEqual(self.revisions(), active)
                self.assertFalse(self.transition(state)[1])
                self.assertEqual(self.revisions(), active)
        self.transition("terminal")
        terminal = self.revisions()
        self.assertNotEqual(active, terminal)
        # Late observations cannot resurrect a terminal request.
        self.transition("model_wait", last_error="late delivery")
        self.assertEqual(self.revisions(), terminal)

    def test_initial_terminal_observation_does_not_occupy_capacity(self):
        before = self.revisions()
        self.assertTrue(self.transition("terminal")[1])
        self.assertEqual(before, self.revisions())

    def test_noop_and_active_metadata_progress_do_not_wait_on_worker_fence(self):
        self.transition("model_wait")
        with ThreadPoolExecutor(1) as executor:
            with self.store.pool.connection() as conn:
                with conn.transaction():
                    conn.execute(
                        "SELECT * FROM worker_capacity_revisions WHERE identity='job:j' FOR UPDATE"
                    ).fetchall()
                    future = executor.submit(
                        lambda: (
                            self.transition("model_wait"),
                            self.transition("ready_to_wake", last_error="observed"),
                            self.store.confirm_sandbox_wake(
                                self.route,
                                node_epoch=self.route.node_epoch,
                                activity_epoch=self.route.activity_epoch + 1,
                            ),
                        )
                    )
                    # Transaction scope releases the lock before executor join,
                    # including on timeout from the previous implementation.
                    replay, progress, confirmed = future.result(timeout=2)
            self.assertFalse(replay[1])
            self.assertTrue(progress[1])
            self.assertIsNotNone(confirmed[0])

    def test_running_confirmation_freshness_does_not_advance_revision(self):
        before = self.revisions()
        result, _ = self.store.confirm_sandbox_wake(
            self.route,
            node_epoch=self.route.node_epoch,
            activity_epoch=self.route.activity_epoch + 1,
        )
        self.assertIsNotNone(result)
        self.assertEqual(before, self.revisions())
        self.assertTrue(result.updated_at)
        self.store.upsert_sandbox(
            replace(result, updated_at="2099-01-01T00:00:00+00:00")
        )
        self.assertEqual(before, self.revisions())

    def test_lifecycle_identity_and_state_changes_remain_fenced(self):
        before = self.revisions()
        result, _ = self.store.confirm_sandbox_wake(
            self.route, node_epoch=self.route.node_epoch, activity_epoch=2
        )
        self.assertIsNotNone(result)
        self.assertEqual(before, self.revisions())
        parked = self.store.set_sandbox_state_if_current(
            result, expected_states={"running"}, state="parked"
        )
        self.assertNotEqual(before, self.revisions())
        before = self.revisions()
        self.store.set_sandbox_state_if_current(
            parked,
            expected_states={"parked"},
            state="parked",
            node_epoch=parked.node_epoch,
            activity_epoch=parked.activity_epoch + 1,
        )
        self.assertNotEqual(before, self.revisions())

    def test_new_active_request_fences_current_owner_after_route_handoff(self):
        # Seed the result of a completed same-generation handoff. The receipt
        # still names the source owner, as can happen during relay delivery.
        destination = replace(
            self.route, node_id="new-n", job_id="new-j", node_url="http://new"
        )
        self.store.run_placement(
            lambda: self.store._write_sandbox(self.store._current.get(), destination)
        )
        before = self.revisions()
        self.transition("model_wait")
        after = self.revisions()
        self.assertEqual(before["job:j"], after["job:j"])
        self.assertGreater(after["job:new-j"], before["job:new-j"])

    def test_new_active_request_invalidates_cold_detach_snapshot(self):
        parked = replace(
            self.route,
            state="parked",
            storage_schema="storage-native-v1",
            storage_snapshot={"checkpoint": "opaque"},
            snapshot_manifest_digest="sha256:" + "a" * 64,
            snapshot_repository="snapshots",
            snapshot_tag="test",
        )
        self.store.upsert_sandbox(parked)
        self.route = self.store.get_sandbox("s")
        ready, release = Event(), Event()
        original = self.store._write_sandbox

        def delayed(conn, route):
            if route.worker_state == "detaching" and not ready.is_set():
                ready.set()
                self.assertTrue(release.wait(timeout=5))
            return original(conn, route)

        self.store._write_sandbox = delayed
        with ThreadPoolExecutor(1) as executor:
            future = executor.submit(
                self.store.run_placement,
                lambda: self.store.begin_sandbox_detach(self.route, require_cold=True),
            )
            try:
                self.assertTrue(ready.wait(timeout=5))
                self.transition("model_wait")
            finally:
                release.set()
            self.assertIsNone(future.result(timeout=5))
        self.assertEqual(self.store.get_sandbox("s").worker_state, "attached")
        self.assertGreater(self.store.serialization_retries, 0)
