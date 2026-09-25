"""Admission reads only incoming reservations and the requested wake journals."""

from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from tests.test_routing import sandbox_route
from ucloud_sandboxes import routing
from ucloud_sandboxes.control_plane import ControlPlaneHandler
from ucloud_sandboxes.models import ResourceQuantity
from ucloud_sandboxes.routing import RoutingStore


def assert_scoped_migrations(test, store):
    for suffix in ("one", "two"):
        source = store.upsert_sandbox(
            sandbox_route(
                sandbox_id=suffix,
                node_id="source",
                job_id="source-job",
                node_url="http://source",
                state="parked",
                resources=ResourceQuantity(vcpu=2, memory_mb=1024, disk_mb=4096),
            )
        )
        store.begin_sandbox_migration(
            source,
            migration_id="move-" + suffix,
            destination_node_id="dest-" + suffix,
            destination_job_id="dest-job-" + suffix,
            destination_node_url="http://dest-" + suffix,
        )
    with patch(
        "ucloud_sandboxes.routing._sandbox_migration_from_row",
        wraps=routing._sandbox_migration_from_row,
    ) as decode:
        selected = store.sandbox_migrations(
            active_only=True, sandbox_ids=["one", "absent"]
        )
        test.assertEqual([row.sandbox_id for row in selected], ["one"])
        test.assertEqual(decode.call_count, 1)
        test.assertEqual(store.sandbox_migrations(active_only=True, sandbox_ids=[]), [])
        test.assertEqual(decode.call_count, 1)
        # Match every identity alias supported by existing route accounting.
        for identity in [
            ("dest-one", "", ""),
            ("", "dest-job-one", ""),
            ("", "", "http://dest-one/"),
        ]:
            selected = store.sandbox_migrations(
                active_only=True, destination_identity=identity
            )
            test.assertEqual([row.sandbox_id for row in selected], ["one"])
        test.assertEqual(decode.call_count, 4)
    handler = SimpleNamespace(routing_store=store)
    heartbeat = SimpleNamespace(
        node_id="dest-one", job_id="dest-job-one", node_url="http://dest-one"
    )
    with patch(
        "ucloud_sandboxes.routing._sandbox_migration_from_row",
        wraps=routing._sandbox_migration_from_row,
    ) as decode:
        occupants = ControlPlaneHandler._placement_routes_for_node(handler, heartbeat)
        test.assertEqual(decode.call_count, 1)
        test.assertEqual(len(occupants), 1)
        test.assertEqual(occupants[0].reservation_id, "move-one")
        test.assertEqual(
            occupants[0].resources,
            ResourceQuantity(vcpu=2, memory_mb=1024, disk_mb=4096),
        )


class RoutingMigrationScopeTests(unittest.TestCase):
    def test_admission_scope_preserves_incoming_capacity(self):
        with TemporaryDirectory() as directory:
            assert_scoped_migrations(
                self, RoutingStore(Path(directory) / "routes.sqlite")
            )
