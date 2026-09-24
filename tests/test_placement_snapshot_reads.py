from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
import unittest

from tests.test_control_plane import build_heartbeat, _sandbox_route
from ucloud_sandboxes.control_state import ControlStateStore
from ucloud_sandboxes.routing import RoutingStore


class PlacementSnapshotReadsTests(unittest.TestCase):
    def test_owner_aliases_are_ordered_deduplicated_and_fresh(self):
        with TemporaryDirectory() as directory:
            store = RoutingStore(Path(directory) / 'routing.sqlite')
            for sandbox_id, node, job, url in (
                ('d', 'other', 'other', 'http://other'),
                ('c', 'other', 'other', 'http://owner/'),
                ('b', 'other', 'job', 'http://other'),
                ('a', 'node', 'job', 'http://owner'),
            ):
                store.upsert_sandbox(_sandbox_route(
                    sandbox_id=sandbox_id, node_id=node, job_id=job, node_url=url,
                ))
            def read():
                return store.sandbox_routes_matching_node_identity(
                    node_id=' node ', job_id=' job ', node_url=' http://owner/ ',
                )
            self.assertEqual([r.sandbox_id for r in read()], ['a', 'b', 'c'])
            # External writes must be visible, without cache invalidation.
            with sqlite3.connect(store.path) as connection:
                connection.execute("UPDATE sandboxes SET state='waking' WHERE sandbox_id='b'")
            self.assertEqual(read()[1].state, 'waking')
            with sqlite3.connect(store.path) as connection:
                connection.execute("UPDATE sandboxes SET spec_json='[]' WHERE sandbox_id='b'")
            with self.assertRaisesRegex(sqlite3.DatabaseError, 'invalid sandbox route'):
                read()

    def test_heartbeat_snapshot_is_one_select_and_keeps_quarantine_and_copy_fences(self):
        with TemporaryDirectory() as directory:
            store = ControlStateStore(Path(directory) / 'control.sqlite')
            heartbeat = build_heartbeat(node_id='node', job_id='job', node_url='http://node')
            store.upsert_heartbeat(heartbeat)
            statements = []
            with store._connection() as connection:
                connection.set_trace_callback(statements.append)
            result = store.load_heartbeats()
            with store._connection() as connection:
                connection.set_trace_callback(None)
            self.assertEqual(len(statements), 1)
            self.assertTrue(statements[0].startswith('SELECT'))
            result['job'].labels['caller'] = 'mutated'
            self.assertNotIn('caller', store.load_heartbeats()['job'].labels)
            store.quarantine_node('job', 'test')
            self.assertFalse(store.load_heartbeats()['job'].admission_open)
            with sqlite3.connect(store.path) as connection:
                connection.execute("UPDATE control_records SET payload='{}' WHERE namespace='heartbeat'")
            with self.assertRaises(ValueError):
                store.load_heartbeats()
