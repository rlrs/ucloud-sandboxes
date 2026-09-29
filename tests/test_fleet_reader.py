import json
import sqlite3
from copy import deepcopy
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from tests.test_registry import build_heartbeat
from ucloud_sandboxes.control_state import ControlStateStore
from ucloud_sandboxes.control_plane import _sandbox_list_bytes
from ucloud_sandboxes.fleet_reader import FleetResponseRenderer, FleetSnapshotReader
from ucloud_sandboxes.models import ResourceQuantity, SandboxInventoryEntry, utc_now
from ucloud_sandboxes.registry import heartbeat_to_dict
from ucloud_sandboxes.routing import RoutingStore, SandboxRoute


class FleetReaderTests(unittest.TestCase):
    def test_render_preserves_shared_inputs_and_rechecks_external_inventory(self):
        with TemporaryDirectory() as raw:
            root = Path(raw)
            control = ControlStateStore(root / 'control.sqlite')
            writer = ControlStateStore(control.path)
            routes = RoutingStore(root / 'routes.sqlite')
            entry = SandboxInventoryEntry(
                sandbox_id='agent', generation=1, operation_id='create',
                spec_hash='a' * 64, state='parked',
                storage_schema='storage-native-v1',
                snapshot_manifest_digest='sha256:' + 'b' * 64,
                snapshot_repository='snapshots', snapshot_tag='agent',
                storage_snapshot={'version': 1, 'layers': [{'digest': 'original'}]},
                storage_dependency={'layers': [{'digest': 'base', 'parents': ['root']}]},
            )
            heartbeat = replace(
                build_heartbeat(job_id='job'), node_id='node',
                node_url='http://node:8090', updated_at=utc_now(),
                active_sandboxes=0, inventory=(entry,), inventory_complete=True,
                labels={'pool': 'workers'},
            )
            writer.upsert_heartbeat(heartbeat)
            route_time = (utc_now() - timedelta(seconds=10)).isoformat()
            routes.upsert_sandbox(SandboxRoute(
                sandbox_id='agent', node_id='node', job_id='job',
                node_url='http://node:8090', resources=ResourceQuantity(),
                spec={'id': 'agent', 'image': 'python', 'labels': {'owner': 'original'}},
                state='parked', generation=1, create_operation_id='create',
                spec_hash='a' * 64, created_at=route_time, updated_at=route_time,
            ))
            cached = control.load_heartbeats(shared=True)['job']
            original = deepcopy(heartbeat_to_dict(cached))
            renderer = FleetResponseRenderer()

            def render():
                return json.loads(_sandbox_list_bytes(control, routes, 120, renderer=renderer))

            first = render()
            # Zero active processes must not hide a sandbox still in complete
            # inventory. The renderer must preserve these shared descriptors.
            self.assertEqual(first['sandboxes'][0]['state'], 'parked')
            first['sandboxes'][0]['spec']['labels']['owner'] = 'client mutation'
            self.assertEqual(render()['sandboxes'][0]['spec']['labels']['owner'], 'original')
            self.assertEqual(heartbeat_to_dict(cached), original)

            # A separate database connection changes inventory without changing
            # the route. Shared reads still observe that durable invalidation.
            writer.upsert_heartbeat(replace(heartbeat, inventory=(), updated_at=utc_now()))
            self.assertEqual(render()['sandboxes'][0]['state'], 'unknown')
            writer.quarantine_node('job', 'untrusted inventory')
            self.assertEqual(render()['sandboxes'][0]['state'], 'parked')
            self.assertEqual(heartbeat_to_dict(cached), original)
            with sqlite3.connect(control.path) as db:
                db.execute("UPDATE control_records SET payload='broken' WHERE namespace='heartbeat'")
            with self.assertRaises(ValueError):
                render()

    def test_reads_fresh_external_changes_and_recovers_dead_child(self):
        with TemporaryDirectory() as raw:
            root = Path(raw)
            control = ControlStateStore(root / 'control.sqlite')
            routes = RoutingStore(root / 'routes.sqlite')
            heartbeat = replace(build_heartbeat(job_id='job'), node_id='node',
                                node_url='http://node:8090', active_sandboxes=1,
                                updated_at=utc_now())
            control.upsert_heartbeat(heartbeat)
            route = SandboxRoute(sandbox_id='agent', node_id='node', job_id='job',
                node_url='http://node:8090', resources=ResourceQuantity(),
                spec={'id':'agent', 'image':'python'}, state='running', generation=1,
                create_operation_id='create', spec_hash='a'*64)
            routes.upsert_sandbox(route)
            reader = FleetSnapshotReader(control.path, routes.path, 120)
            try:
                self.assertEqual(json.loads(reader.read()), json.loads(_sandbox_list_bytes(control, routes, 120)))
                routes.upsert_sandbox(replace(route, state='parked'))
                self.assertEqual(json.loads(reader.read())['sandboxes'][0]['cached_state'], 'parked')
                first = reader._process
                first.terminate()
                first.join(2)
                routes.delete_sandbox('agent')
                self.assertEqual(json.loads(reader.read())['sandboxes'], [])
                self.assertIsNot(reader._process, first)
                with sqlite3.connect(control.path) as db:
                    db.execute("UPDATE control_records SET payload='broken' WHERE namespace='heartbeat'")
                with self.assertRaisesRegex(ValueError, 'unreadable'):
                    reader.read()
            finally:
                process = reader._process
                reader.close()
            self.assertIsNone(reader._process)
            self.assertTrue(process._closed)
            with self.assertRaisesRegex(RuntimeError, 'closed'):
                reader.read()

    def test_gateway_uses_and_closes_isolated_reader(self):
        from urllib.request import urlopen
        from tests.test_control_plane import _gateway_server, _running_server
        with TemporaryDirectory() as raw:
            server = _gateway_server(Path(raw), isolate_fleet_reads=True)
            reader = server.RequestHandlerClass.fleet_snapshot_reader
            with _running_server(server) as url:
                with urlopen(url + '/v1/sandboxes', timeout=5) as response:
                    self.assertEqual(json.load(response)['sandboxes'], [])
                self.assertIsNotNone(reader._process)
            self.assertIsNone(reader._process)
            self.assertTrue(reader._closed)

    def test_missing_or_replaced_database_cannot_be_recreated_on_child_restart(self):
        for replaced in (False, True):
            with self.subTest(replaced=replaced), TemporaryDirectory() as raw:
                root = Path(raw)
                control = ControlStateStore(root / 'control.sqlite')
                routes = RoutingStore(root / 'routes.sqlite')
                reader = FleetSnapshotReader(control.path, routes.path, 120)
                try:
                    reader.read()
                    reader._process.terminate()
                    reader._process.join(2)
                    control.path.rename(root / 'original.sqlite')
                    if replaced:
                        # Retain the original inode so the replacement cannot
                        # accidentally reuse it during this test.
                        ControlStateStore(control.path)
                    with self.assertRaises((RuntimeError, ValueError)):
                        reader.read()
                    if not replaced:
                        self.assertFalse(control.path.exists())
                finally:
                    reader.close()
