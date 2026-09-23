"""Lifecycle authority contracts without an HTTP handler or mocked routing store."""

from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from tests.test_control_plane import _portable_snapshot, _sandbox_route
from ucloud_sandboxes.lifecycle_commit import (
    InvalidLifecycleReceipt,
    LifecycleCommitter,
    LifecycleRouteChanged,
    SnapshotReferences,
)
from ucloud_sandboxes.models import NodeHeartbeat, utc_now
from ucloud_sandboxes.routing import RoutingStore


class LifecycleCommitTests(unittest.TestCase):
    def setUp(self):
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.routes = RoutingStore(Path(self.tmp.name) / "routes.sqlite")
        self.snapshot = _portable_snapshot("sandbox")
        self.route = self.routes.upsert_sandbox(
            _sandbox_route(
                sandbox_id="sandbox",
                node_id="node",
                job_id="job",
                node_url="http://node",
                spec=self.snapshot.manifest.spec.to_dict(),
                resources=self.snapshot.manifest.spec.requested_resources(),
                state="running",
                node_epoch="boot",
                activity_epoch=5,
            )
        )
        self.heartbeat = NodeHeartbeat(
            node_id="node",
            job_id="job",
            node_url="http://node",
            deployment_id="test",
            updated_at=utc_now(),
            active_sandboxes=1,
            node_epoch="boot",
            activity_epoch=5,
        )
        self.protected = set()
        self.events = []
        self.domain = LifecycleCommitter(
            self.routes,
            heartbeat=lambda _: self.heartbeat,
            snapshots=SnapshotReferences(protect=self.protect, release=self.release),
        )

    def protect(self, route):
        self.events.append(("protect", route.snapshot_manifest_digest))
        self.protected.add(route.snapshot_manifest_digest)

    def release(self, route, *, keep_route):
        self.events.append(("release", route.snapshot_manifest_digest))
        if (
            keep_route is None
            or route.snapshot_manifest_digest != keep_route.snapshot_manifest_digest
        ):
            self.protected.discard(route.snapshot_manifest_digest)

    def receipt(self, *, snapshot=True, activity=6):
        payload = {"node_epoch": "boot", "activity_epoch": activity}
        if snapshot:
            ref = self.snapshot.reference
            payload.update(
                storage_schema=self.snapshot.schema,
                snapshot_sha256=self.snapshot.sha256,
                snapshot_manifest_digest=ref.manifest_digest,
                snapshot_repository=ref.repository,
                snapshot_tag=ref.tag,
                storage_snapshot=self.snapshot.to_dict(),
            )
        return payload

    def test_park_then_wake_commits_program_and_releases_old_checkpoint(self):
        parked = self.domain.park(self.route, self.receipt()).route
        self.assertEqual(parked.state, "parked")
        self.assertEqual(self.events[0][0], "protect")
        self.assertEqual(self.protected, {self.snapshot.reference.manifest_digest})
        outcome = self.domain.wake(
            parked,
            self.receipt(snapshot=False, activity=7),
            program_transition={
                "request_id": "request",
                "rollout_id": "rollout",
                "state": "acting",
                "clear_error": True,
            },
        )
        self.assertEqual(outcome.route.state, "running")
        self.assertEqual(outcome.route.storage_snapshot, {})
        program, changed = outcome.program_transition
        self.assertTrue(changed)
        self.assertEqual(program.state, "acting")
        self.assertEqual(self.routes.program_request_readonly("request"), program)
        self.assertFalse(self.protected)

    def test_invalid_worker_proof_cannot_acquire_references_or_commit(self):
        for change in (
            {"node_epoch": "another-boot"},
            {"activity_epoch": True},
            {"activity_epoch": 5},
            {"activity_epoch": -1},
        ):
            with self.subTest(change=change):
                with self.assertRaises(InvalidLifecycleReceipt):
                    self.domain.park(self.route, {**self.receipt(), **change})
                self.assertEqual(
                    self.routes.get_sandbox_readonly("sandbox"), self.route
                )
                self.assertFalse(self.events)
        self.heartbeat = replace(self.heartbeat, activity_epoch=9)
        with self.assertRaisesRegex(InvalidLifecycleReceipt, "predates"):
            self.domain.wake(self.route, self.receipt(activity=8))

    def test_resident_wait_cannot_promote_a_portable_checkpoint(self):
        with self.assertRaisesRegex(InvalidLifecycleReceipt, "resident wait"):
            self.domain.park(
                self.route, {**self.receipt(), "sandbox": {"state": "running"}}
            )
        self.assertFalse(self.protected)
        resident = self.domain.park(
            self.route,
            {**self.receipt(snapshot=False), "sandbox": {"state": "running"}},
        ).route
        self.assertEqual(resident.state, "running")
        self.assertFalse(resident.storage_snapshot)

    def test_route_race_compensates_only_uncommitted_candidate(self):
        def protect_and_delete(route):
            self.protect(route)
            self.routes.prepare_sandbox_delete("sandbox")

        self.domain.snapshots = SnapshotReferences(protect_and_delete, self.release)
        with self.assertRaises(LifecycleRouteChanged):
            self.domain.park(self.route, self.receipt())
        self.assertFalse(self.protected)
        self.assertTrue(self.routes.get_sandbox_readonly("sandbox").delete_operation_id)

    def test_ambiguous_commit_keeps_exact_durable_reference(self):
        original = self.routes.set_sandbox_state_if_current
        for committed in (False, True):
            with self.subTest(committed=committed):

                def ambiguous(*args, **kwargs):
                    if committed:
                        original(*args, **kwargs)
                    raise OSError("commit acknowledgement lost")

                with patch.object(
                    self.routes, "set_sandbox_state_if_current", side_effect=ambiguous
                ):
                    with self.assertRaisesRegex(OSError, "acknowledgement"):
                        self.domain.park(self.route, self.receipt())
                self.assertEqual(bool(self.protected), committed)

    def test_failed_readback_preserves_uncertain_protection(self):
        with (
            patch.object(
                self.routes,
                "set_sandbox_state_if_current",
                side_effect=OSError("uncertain commit"),
            ),
            patch.object(
                self.routes,
                "get_sandbox_readonly",
                side_effect=OSError("read unavailable"),
            ),
        ):
            with self.assertRaisesRegex(OSError, "read unavailable"):
                self.domain.park(self.route, self.receipt())
        self.assertEqual(self.protected, {self.snapshot.reference.manifest_digest})

    def test_failed_partial_protection_does_not_commit(self):
        def protect_then_fail(route):
            self.protect(route)
            raise OSError("reference write failed")

        self.domain.snapshots = SnapshotReferences(protect_then_fail, self.release)
        with self.assertRaisesRegex(OSError, "reference write failed"):
            self.domain.park(self.route, self.receipt())
        self.assertEqual(self.routes.get_sandbox_readonly("sandbox"), self.route)
        self.assertFalse(self.protected)
