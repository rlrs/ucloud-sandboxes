import json
import sqlite3
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from tests.test_registry import build_heartbeat
from ucloud_sandboxes.control_state import ControlStateStore
from ucloud_sandboxes.control_plane import _sandbox_list_bytes
from ucloud_sandboxes.fleet_reader import FleetSnapshotReader
from ucloud_sandboxes.models import ResourceQuantity, utc_now
from ucloud_sandboxes.routing import RoutingStore, SandboxRoute


class FleetReaderTests(unittest.TestCase):
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
