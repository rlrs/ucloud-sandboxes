from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler
import json
import socket
from threading import Event, Thread
import unittest

from ucloud_sandboxes.http_server import HighBacklogThreadingHTTPServer


class _NoopHandler:
    def __init__(self, request, client_address, server) -> None:
        del request, client_address, server


class _BlockingHandler(BaseHTTPRequestHandler):
    started = Event()
    release = Event()

    def do_GET(self) -> None:
        self.started.set()
        self.release.wait(timeout=5)
        self.send_response(200)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, _format: str, *args: object) -> None:
        del args


class HttpServerTests(unittest.TestCase):
    def test_accepted_clients_get_read_timeout_and_limits_are_validated(self) -> None:
        with self.assertRaisesRegex(ValueError, "timeout"):
            HighBacklogThreadingHTTPServer(
                ("127.0.0.1", 0),
                _NoopHandler,
                client_socket_timeout_seconds=0,
            )
        with self.assertRaisesRegex(ValueError, "threads"):
            HighBacklogThreadingHTTPServer(
                ("127.0.0.1", 0),
                _NoopHandler,
                max_request_threads=0,
            )

        server = HighBacklogThreadingHTTPServer(
            ("127.0.0.1", 0),
            _NoopHandler,
            client_socket_timeout_seconds=1.25,
            max_request_threads=1,
        )
        client = socket.create_connection(server.server_address)
        accepted = None
        try:
            accepted, _address = server.get_request()
            self.assertEqual(accepted.gettimeout(), 1.25)
            self.assertEqual(server.max_request_threads, 1)
        finally:
            client.close()
            if accepted is not None:
                accepted.close()
            server.server_close()

    def test_thread_capacity_returns_retryable_json_instead_of_disconnect(self) -> None:
        _BlockingHandler.started.clear()
        _BlockingHandler.release.clear()
        server = HighBacklogThreadingHTTPServer(
            ("127.0.0.1", 0),
            _BlockingHandler,
            max_request_threads=1,
        )
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        first = HTTPConnection(*server.server_address, timeout=5)
        second = HTTPConnection(*server.server_address, timeout=5)
        try:
            first.request("GET", "/hold")
            self.assertTrue(_BlockingHandler.started.wait(timeout=2))

            second.request(
                "POST",
                "/v1/sandboxes",
                body=b"{}",
                headers={"Content-Type": "application/json"},
            )
            response = second.getresponse()
            body = json.loads(response.read().decode("utf-8"))

            self.assertEqual(response.status, 503)
            self.assertEqual(response.getheader("Content-Type"), "application/json")
            self.assertEqual(response.getheader("Retry-After"), "1")
            self.assertEqual(
                response.getheader("X-UCloud-Sandbox-Retryable"), "true"
            )
            self.assertTrue(body["retryable"])
        finally:
            _BlockingHandler.release.set()
            try:
                first.getresponse().read()
            except OSError:
                pass
            first.close()
            second.close()
            server.shutdown()
            server.server_close()


if __name__ == "__main__":
    unittest.main()
