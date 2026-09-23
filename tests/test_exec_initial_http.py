"""Actual gateway/node HTTP round trips, with a real short host process."""
from http.client import HTTPConnection
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from tests.test_control_plane import _gateway_server, _running_server, _seed_gateway_node
from tests.test_sandbox_exec import FakeSandboxManager
from ucloud_sandboxes.http_server import HighBacklogThreadingHTTPServer
from ucloud_sandboxes.node_agent import NodeAgentHandler
from ucloud_sandboxes.sandbox_exec import ExecSessionManager
from ucloud_sandboxes.telemetry import Telemetry


class InitialExecHttpTests(unittest.TestCase):
    def test_gateway_preserves_query_output_and_durable_exec_route(self):
        class Handler(NodeAgentHandler):
            def _check_node_control_authorized(self):
                return True
        Handler.exec_manager = ExecSessionManager(FakeSandboxManager())
        Handler.manager = SimpleNamespace(consume_exec_start_timings=lambda: {})
        Handler.telemetry = Telemetry.disabled('initial-exec-test')
        Handler.node_control_bearer_token = ''
        Handler.sandboxes_enabled = True
        with TemporaryDirectory() as tmp:
            node = HighBacklogThreadingHTTPServer(('127.0.0.1', 0), Handler)
            with _running_server(node) as url:
                root = Path(tmp)
                heartbeats, routes = _seed_gateway_node(root, node_url=url, sandbox_id='one')
                gateway = _gateway_server(root, heartbeat_file=heartbeats, routing_file=routes)
                with _running_server(gateway):
                    connection = HTTPConnection(*gateway.server_address, timeout=5)
                    try:
                        for query in ('', '?initial_wait_seconds=0.05'):
                            body = json.dumps({'command': ['/bin/sh', '-c', 'printf marker'], 'env': {}, 'working_dir': None, 'stdin': False, 'tty': False})
                            connection.request('POST', '/v1/sandboxes/one/exec' + query, body=body, headers={'Content-Type': 'application/json'})
                            response = connection.getresponse(); payload = json.load(response)
                            self.assertEqual(response.status, 201, payload)
                            self.assertEqual('events' in payload, bool(query))
                            session_id = payload['session']['id']
                            from ucloud_sandboxes.routing import RoutingStore
                            self.assertEqual(RoutingStore(routes).get_exec(session_id).sandbox_id, 'one')
                            initial = payload.get('events', [])
                            after = initial[-1]['sequence'] if initial else 0
                            connection.request('GET', f'/v1/exec/{session_id}/events?after={after}&wait_seconds=1')
                            response = connection.getresponse(); rest = json.load(response)
                            self.assertEqual(response.status, 200, rest)
                            output = ''.join(e['data'] for e in initial + rest['events'] if e['stream'] == 'stdout')
                            self.assertEqual(output, 'marker')
                    finally:
                        connection.close()
