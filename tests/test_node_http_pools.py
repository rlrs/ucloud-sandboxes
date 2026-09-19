from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from threading import Event, Thread
from urllib import error, request
import unittest
from unittest.mock import Mock, patch

import urllib3
from urllib3.exceptions import EmptyPoolError

from ucloud_sandboxes import control_plane


class NodeHttpPoolTests(unittest.TestCase):
    def test_live_event_poll_cannot_starve_file_and_control_requests(self):
        entered, release = Event(), Event()
        failures = []

        class Node(BaseHTTPRequestHandler):
            def do_GET(self):
                entered.set()
                release.wait(5)
                self.reply()

            def do_POST(self):
                self.rfile.read(int(self.headers["Content-Length"]))
                self.reply()

            def reply(self):
                self.send_response(200)
                self.send_header("Content-Length", "2")
                self.end_headers()
                self.wfile.write(b"{}")

            def log_message(self, *_args):
                pass

        node = ThreadingHTTPServer(("127.0.0.1", 0), Node)
        server = Thread(target=node.serve_forever, daemon=True)
        server.start()
        url = f"http://127.0.0.1:{node.server_port}"
        poll_pool = urllib3.PoolManager(maxsize=1, block=True, retries=False)
        control_pool = urllib3.PoolManager(maxsize=1, block=True, retries=False)

        def poll():
            try:
                with control_plane._open_node_request(
                    request.Request(url + "/v1/exec/one/events?wait_seconds=30"),
                    timeout=5, authenticated=True,
                ) as response:
                    response.read()
            except BaseException as exc:
                failures.append(exc)

        reader = Thread(target=poll)
        try:
            with (
                patch.object(control_plane, "_NODE_EXEC_EVENT_HTTP_POOL", poll_pool),
                patch.object(control_plane, "_NODE_HTTP_POOL", control_pool),
            ):
                reader.start()
                self.assertTrue(entered.wait(2))
                with control_plane._open_node_request(
                    request.Request(url + "/v1/sandboxes/one/exec", data=b"{}"),
                    timeout=1, authenticated=True,
                ) as response:
                    self.assertEqual(response.status, 200)
                    response.read()
                self.assertFalse(release.is_set())
                release.set()
                reader.join(3)
        finally:
            release.set()
            reader.join(3)
            poll_pool.clear()
            control_pool.clear()
            node.shutdown()
            node.server_close()
            server.join(2)
        self.assertFalse(failures)

    def test_pool_admission_failure_is_safe_but_transport_timeout_remains_ambiguous(self):
        pool = Mock()
        pool.request.side_effect = EmptyPoolError(None, "pool exhausted")
        with patch.object(control_plane, "_NODE_HTTP_POOL", pool):
            with self.assertRaises(error.URLError) as raised:
                control_plane._open_node_request(
                    request.Request("http://node/v1/sandboxes/one/exec", data=b"{}"),
                    timeout=1, authenticated=True,
                )
        response = control_plane._node_transport_error_response(raised.exception.reason)
        self.assertEqual(response.status, 503)
        self.assertTrue(json.loads(response.body)["retryable"])
        self.assertEqual(json.loads(response.body)["error_code"], "http_request_capacity_exhausted")
        self.assertEqual(response.headers["Retry-After"], "1")
        self.assertEqual(response.transport_error_kind, "")
        ambiguous = control_plane._node_transport_error_response(TimeoutError("timed out"))
        self.assertEqual(ambiguous.status, 504)
        self.assertNotIn("error_code", json.loads(ambiguous.body))
