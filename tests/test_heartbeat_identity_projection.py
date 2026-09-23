from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
import unittest
from tests.test_control_plane import _sandbox_route
from ucloud_sandboxes.routing import RoutingStore


class HeartbeatIdentityProjectionTests(unittest.TestCase):
    def test_all_matching_owner_components_remain_visible_without_payload_decode(self):
        with TemporaryDirectory() as temp:
            store = RoutingStore(Path(temp) / 'routes.sqlite')
            base = _sandbox_route(sandbox_id='s', node_id='owner', job_id='job',
                                  node_url='http://worker', state='running')
            tuples = [('owner', 'job', 'http://worker'),
                      ('owner', 'wrong-job', 'http://elsewhere'),
                      ('wrong-node', 'job', 'http://elsewhere'),
                      ('wrong-node', 'wrong-job', 'http://worker/'),
                      ('unrelated', 'unrelated', 'http://unrelated')]
            for i, (node, job, url) in enumerate(tuples * 3):
                sid = 's' + str(i)
                store.upsert_sandbox(replace(base, sandbox_id=sid, node_id=node,
                                            job_id=job, node_url=url,
                                            spec={'id': sid, 'env': {'LARGE': 'x' * 65536}}))
            with patch('ucloud_sandboxes.routing._sandbox_route_from_row',
                       side_effect=AssertionError('unnecessary full route decoding')):
                self.assertEqual(store.assigned_node_identities(
                    node_id='owner', job_id='job', node_url='http://worker/'), sorted(tuples[:4]))
            # An external writer's identity change is visible on the next read.
            other = RoutingStore(store.path)
            other.upsert_sandbox(replace(base, sandbox_id='fresh', spec={'id': 'fresh'},
                                        node_id='new-conflict'))
            self.assertIn(('new-conflict', 'job', 'http://worker'),
                          store.assigned_node_identities(node_id='owner', job_id='job', node_url='http://worker'))
