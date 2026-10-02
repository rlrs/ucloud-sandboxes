"""Actual gateway/node HTTP round trips, with a real short host process."""

from http.client import HTTPConnection
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from tests.test_control_plane import (
    _gateway_server,
    _running_server,
    _seed_gateway_node,
)
from tests.test_sandbox_exec import FakeSandboxManager
from ucloud_sandboxes.exec_session_routes import EXEC_SESSION_PREFIX_HEADER
from ucloud_sandboxes.http_server import HighBacklogThreadingHTTPServer
from ucloud_sandboxes.node_agent import NodeAgentHandler
from ucloud_sandboxes.routing import RoutingStore
from ucloud_sandboxes.sandbox_exec import ExecSessionManager
from ucloud_sandboxes.telemetry import Telemetry

TEST_TIER = "contract"


class InitialExecHttpTests(unittest.TestCase):
    def _round_trip(self, *, honors_session_prefix: bool) -> list[tuple[str, object]]:
        class Handler(NodeAgentHandler):
            def _check_node_control_authorized(self):
                return True

            def _start_exec(self, path, query=""):
                if not honors_session_prefix:
                    # An older worker ignores the signed route prefix.
                    del self.headers[EXEC_SESSION_PREFIX_HEADER]
                return super()._start_exec(path, query)

        Handler.exec_manager = ExecSessionManager(FakeSandboxManager())
        Handler.manager = SimpleNamespace(consume_exec_start_timings=lambda: {})
        Handler.telemetry = Telemetry.disabled("initial-exec-test")
        Handler.node_control_bearer_token = ""
        Handler.sandboxes_enabled = True
        observed: list[tuple[str, object]] = []
        with TemporaryDirectory() as tmp:
            node = HighBacklogThreadingHTTPServer(("127.0.0.1", 0), Handler)
            with _running_server(node) as url:
                root = Path(tmp)
                heartbeats, routes = _seed_gateway_node(
                    root, node_url=url, sandbox_id="one"
                )
                gateway = _gateway_server(
                    root, heartbeat_file=heartbeats, routing_file=routes
                )
                with _running_server(gateway):
                    connection = HTTPConnection(*gateway.server_address, timeout=5)
                    try:
                        for query in ("", "?initial_wait_seconds=0.05"):
                            body = json.dumps(
                                {
                                    "command": ["/bin/sh", "-c", "printf marker"],
                                    "env": {},
                                    "working_dir": None,
                                    "stdin": False,
                                    "tty": False,
                                }
                            )
                            connection.request(
                                "POST",
                                "/v1/sandboxes/one/exec" + query,
                                body=body,
                                headers={"Content-Type": "application/json"},
                            )
                            response = connection.getresponse()
                            payload = json.load(response)
                            self.assertEqual(response.status, 201, payload)
                            self.assertEqual("events" in payload, bool(query))
                            session_id = payload["session"]["id"]
                            observed.append(
                                (session_id, RoutingStore(routes).get_exec(session_id))
                            )
                            initial = payload.get("events", [])
                            after = initial[-1]["sequence"] if initial else 0
                            connection.request(
                                "GET",
                                f"/v1/exec/{session_id}/events?after={after}&wait_seconds=1",
                            )
                            response = connection.getresponse()
                            rest = json.load(response)
                            self.assertEqual(response.status, 200, rest)
                            output = "".join(
                                e["data"]
                                for e in initial + rest["events"]
                                if e["stream"] == "stdout"
                            )
                            self.assertEqual(output, "marker")
                    finally:
                        connection.close()
        return observed

    def test_signed_sessions_route_without_a_durable_exec_row(self):
        for session_id, durable in self._round_trip(honors_session_prefix=True):
            self.assertTrue(session_id.startswith("xr1."), session_id)
            self.assertIsNone(durable)

    def test_unsigned_worker_sessions_keep_the_durable_exec_route(self):
        for session_id, durable in self._round_trip(honors_session_prefix=False):
            self.assertTrue(session_id.startswith("exec-"), session_id)
            self.assertEqual(durable.sandbox_id, "one")
