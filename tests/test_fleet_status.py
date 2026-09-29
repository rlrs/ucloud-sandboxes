"""Compact fleet reads preserve lifecycle truth without full user specs."""

from concurrent.futures import Future, ThreadPoolExecutor
from copy import deepcopy
from dataclasses import replace
from datetime import timedelta
import json
import os
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
from threading import Event, RLock
import unittest
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.request import urlopen
from uuid import uuid4

from tests.test_control_plane import _gateway_server, _running_server
from tests.test_registry import build_heartbeat
from ucloud_sandboxes.control_plane import ControlPlaneHandler, _sandbox_list_bytes
from ucloud_sandboxes.control_state import ControlStateStore
from ucloud_sandboxes.fleet_reader import FleetResponseRenderer, FleetSnapshotReader, status_ids
from ucloud_sandboxes.models import ResourceQuantity, SandboxInventoryEntry, utc_now
from ucloud_sandboxes.registry import heartbeat_to_dict
from ucloud_sandboxes.routing import RoutingStore, SandboxRoute


def route(sandbox_id="agent", **kwargs):
    return SandboxRoute(
        sandbox_id=sandbox_id, node_id="node", job_id="job", node_url="http://node:8090",
        resources=ResourceQuantity(1, 1024, 4096),
        spec={"id": sandbox_id, "image": "python", "labels": {"opaque": "x" * 4096}},
        state="parked", generation=1, create_operation_id="create", spec_hash="a" * 64,
        **kwargs,
    )


class StatusProjectionContracts:
    def setUp(self):
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        self.control = ControlStateStore(root / "control.sqlite")
        self.routes = self.make_routes(root / "routes.sqlite")
        self.now = utc_now()
        self.route = route(created_at=(self.now - timedelta(seconds=10)).isoformat(),
                           updated_at=(self.now - timedelta(seconds=5)).isoformat())
        self.routes.upsert_sandbox(self.route)
        self.heartbeat = replace(build_heartbeat(job_id="job"), node_id="node",
            node_url="http://node:8090", active_sandboxes=0, updated_at=self.now,
            inventory_complete=True, inventory=(SandboxInventoryEntry(
                sandbox_id="agent", state="parked", generation=1,
                operation_id="create", spec_hash="a" * 64,
                storage_schema="storage-native-v1",
                snapshot_manifest_digest="sha256:" + "b" * 64,
                snapshot_repository="snapshots", snapshot_tag="agent",
                storage_snapshot={"layers": [{"opaque": "must remain intact"}]},
            ),))
        self.control.upsert_heartbeat(self.heartbeat)
        self.renderer = FleetResponseRenderer(status_only=True)

    def read(self, ids=()):
        return json.loads(_sandbox_list_bytes(self.control, self.routes, 120,
            renderer=self.renderer, status_only=True, sandbox_ids=status_ids(ids)))

    def assert_projection(self):
        full = json.loads(_sandbox_list_bytes(self.control, self.routes, 120))["sandboxes"]
        compact = self.read()["sandboxes"]
        self.assertEqual(len(full), len(compact))
        for detailed, status in zip(full, compact):
            for key in ("id", "state", "cached_state", "node", "created_at", "updated_at"):
                self.assertEqual(status[key], detailed[key])
            self.assertEqual(status["spec"], {"id": detailed["spec"]["id"]})
            self.assertEqual(status["generation"], self.routes.get_sandbox(status["id"]).generation)
            self.assertEqual(set(status), {"id", "spec", "state", "cached_state", "node",
                                           "created_at", "updated_at", "generation"})
        return compact

    def test_projection_matches_full_and_omits_large_spec_before_decoding(self):
        shared = self.control.load_heartbeats(shared=True)["job"]
        original = deepcopy(heartbeat_to_dict(shared))
        self.assertEqual(self.assert_projection()[0]["state"], "parked")
        rows = self.routes.sandbox_status_rows_readonly()
        self.assertNotIn("spec_json", rows[0].keys())
        self.assertNotIn("resources_json", rows[0].keys())
        self.assertLess(len(json.dumps([dict(row) for row in rows])), 4096)
        self.assertEqual(heartbeat_to_dict(shared), original)

    def test_filter_is_exact_sorted_parameterized_and_unknown_ids_are_absent(self):
        hostile = "x') OR 1=1 --"
        self.routes.upsert_sandbox(route(hostile))
        self.routes.upsert_sandbox(route("z-last"))
        self.assertEqual([r["id"] for r in self.read(["z-last", "agent", "agent"])["sandboxes"]],
                         ["agent", "z-last"])
        self.assertEqual([r["id"] for r in self.read([hostile])["sandboxes"]], [hostile])
        self.assertEqual(self.read(["missing"])["sandboxes"], [])
        self.assertEqual(len(self.read()["sandboxes"]), 3)

    def test_inventory_quarantine_expiry_generation_and_deletion_stay_fresh(self):
        self.assert_projection()
        writer = ControlStateStore(self.control.path)
        writer.upsert_heartbeat(replace(self.heartbeat, inventory=(), updated_at=utc_now()))
        self.assertEqual(self.assert_projection()[0]["state"], "unknown")
        writer.quarantine_node("job", "untrusted absence")
        self.assertEqual(self.assert_projection()[0]["state"], "parked")
        with (patch("ucloud_sandboxes.models.utc_now", return_value=self.now + timedelta(seconds=121)),
              patch("ucloud_sandboxes.control_plane.utc_now", return_value=self.now + timedelta(seconds=121)),
              patch("ucloud_sandboxes.exec_routing.utc_now", return_value=self.now + timedelta(seconds=121))):
            self.assertEqual(self.assert_projection()[0]["state"], "unknown")
        self.routes.delete_sandbox("agent")
        self.assertEqual(self.read()["sandboxes"], [])
        self.routes.upsert_sandbox(replace(self.route, generation=2, create_operation_id="new-create"))
        self.assertEqual(self.assert_projection()[0]["generation"], 2)

    def test_portable_detached_and_invalid_lifecycle_fail_closed(self):
        detached = replace(self.route, worker_state="detached", storage_schema="storage-native-v1",
            snapshot_manifest_digest="sha256:" + "b" * 64, snapshot_repository="snapshots",
            snapshot_tag="agent", storage_snapshot={"version": 1, "opaque": ["proof"]})
        self.routes.upsert_sandbox(detached)
        self.assertEqual(self.assert_projection()[0]["state"], "parked")
        self.assertFalse(self.read()["sandboxes"][0]["node"]["attached"])
        with self.routes._transaction() as db:
            db.execute("UPDATE sandboxes SET storage_snapshot_json='{}' WHERE sandbox_id=?", ("agent",))
        with self.assertRaises(sqlite3.DatabaseError):
            self.read()


class FleetStatusTests(StatusProjectionContracts, unittest.TestCase):
    make_routes = staticmethod(RoutingStore)

    def test_isolated_reader_separates_projection_filter_and_full_caches(self):
        self.routes.upsert_sandbox(route("other"))
        reader = FleetSnapshotReader(self.control.path, self.routes.path, 120)
        self.addCleanup(reader.close)
        full = json.loads(reader.read())
        status = json.loads(reader.read_status(["agent"]))
        self.assertEqual([r["id"] for r in status["sandboxes"]], ["agent"])
        self.assertEqual(json.loads(reader.read()), full)
        self.assertEqual(len(json.loads(reader.read_status())["sandboxes"]), 2)
        self.routes.delete_sandbox("agent")
        self.assertEqual(json.loads(reader.read_status(["agent"]))["sandboxes"], [])
        child = reader._process
        child.terminate()
        child.join(2)
        self.assertEqual(len(json.loads(reader.read_status())["sandboxes"]), 1)
        self.assertIsNot(reader._process, child)

    def test_bounds_are_checked_before_sending_to_child(self):
        reader = FleetSnapshotReader(self.control.path, self.routes.path, 120)
        self.addCleanup(reader.close)
        for invalid in ([""], ["x"] * 257, ["x" * 513], ["a\0b"],
                        [str(i) + "\U0001f600" * 500 for i in range(256)]):
            with self.subTest(invalid=repr(invalid)[:40]), self.assertRaises(ValueError):
                reader.read_status(invalid)
            self.assertIsNone(reader._process)

    def test_http_default_status_filter_and_validation(self):
        server = _gateway_server(Path(self.tmp.name) / "http", isolate_fleet_reads=True)
        server.RequestHandlerClass.routing_store.upsert_sandbox(self.route)
        with _running_server(server) as url:
            def fetch(query):
                with urlopen(url + "/v1/sandboxes" + query, timeout=10) as response:
                    return json.load(response)
            full = fetch("")
            status = fetch("?view=status&id=agent")
            self.assertNotIn("view", full)
            self.assertIn("labels", full["sandboxes"][0]["spec"])
            self.assertEqual(status["view"], "status")
            self.assertFalse(status["refresh_supported"])
            self.assertEqual(status["sandboxes"][0]["spec"], {"id": "agent"})
            self.assertEqual(fetch("?view=status&id=missing")["sandboxes"], [])
            self.assertEqual(fetch("?view=full"), full)
            for query in ("?view=bad", "?view=status&view=full", "?view=status&id=",
                          "?id=agent", "?view=status&refresh=true", "?view=status&id=" + "x" * 513):
                with self.subTest(query=query[:80]), self.assertRaises(HTTPError) as exc:
                    fetch(query)
                self.assertEqual(exc.exception.code, 400)

    def test_concurrent_filters_do_not_cross_contaminate_and_errors_are_not_retained(self):
        entered, release, joined = Event(), Event(), Event()
        calls = []

        class ObservedFuture(Future):
            def result(self, *args, **kwargs):
                if not self.done():
                    joined.set()
                return super().result(*args, **kwargs)

        class Reader:
            def read_status(self, ids):
                calls.append(ids)
                if ids == ("a",):
                    entered.set()
                    if not release.wait(3):
                        raise TimeoutError("test reader not released")
                return json.dumps(ids).encode()

        class Handler(ControlPlaneHandler):
            fleet_response_lock = RLock()
            fleet_status_futures = {}
            fleet_snapshot_reader = Reader()

            def _write_bytes(self, body, content_type):
                self.response = body

        def read(ids):
            handler = object.__new__(Handler)
            handler._list_sandbox_statuses(ids)
            return handler.response

        with (patch("ucloud_sandboxes.control_plane.Future", ObservedFuture),
              ThreadPoolExecutor(max_workers=2) as pool):
            first = pool.submit(read, ("a",))
            self.assertTrue(entered.wait(3))
            try:
                self.assertEqual(pool.submit(read, ("b",)).result(2), b'["b"]')
                same = pool.submit(read, ("a",))
                self.assertTrue(joined.wait(2))
                self.assertEqual(calls, [("a",), ("b",)])
            finally:
                release.set()
            self.assertEqual(first.result(2), b'["a"]')
            self.assertEqual(same.result(2), b'["a"]')
        self.assertEqual(Handler.fleet_status_futures, {})
        with patch.object(Handler.fleet_snapshot_reader, "read_status", side_effect=ValueError("failed")):
            with self.assertRaisesRegex(ValueError, "failed"):
                read(("a",))
        self.assertEqual(Handler.fleet_status_futures, {})
        self.assertEqual(read(("a",)), b'["a"]')


@unittest.skipUnless(os.environ.get("UCLOUD_TEST_POSTGRES_DSN"), "requires isolated PostgreSQL")
class PostgresFleetStatusTests(StatusProjectionContracts, unittest.TestCase):
    def make_routes(self, path):
        import psycopg
        from psycopg import sql
        from ucloud_sandboxes.shared_control.routing_repository import PostgresRoutingStore
        dsn = os.environ["UCLOUD_TEST_POSTGRES_DSN"]
        schema = "ucloud_routing_status_test_" + uuid4().hex
        store = PostgresRoutingStore(path, dsn=dsn, schema=schema)
        store.migrate()

        def cleanup():
            store.close()
            with psycopg.connect(dsn, autocommit=True) as connection:
                connection.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
        self.addCleanup(cleanup)
        return store
