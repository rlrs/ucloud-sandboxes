from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler
import json
import socket
from threading import Event, Thread
import unittest

from ucloud_sandboxes.http_server import HighBacklogThreadingHTTPServer
from ucloud_sandboxes.http_server import JsonHttpHandler, RequestBodyTooLargeError


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


class _JsonHandler(JsonHttpHandler):
    max_json_body_bytes = 4

    def do_POST(self) -> None:
        try:
            payload = self._read_json_body()
        except RequestBodyTooLargeError as exc:
            self._write_json({"error": str(exc)}, status=413)
            return
        except ValueError as exc:
            self._write_json({"error": str(exc)}, status=400)
            return
        self._write_json(
            {"payload": payload},
            headers={
                "Content-Length": "999",
                "Content-Type": "text/plain",
                "X-Test": "preserved",
            },
        )


class _EarlyRejectingJsonHandler(JsonHttpHandler):
    def do_POST(self) -> None:
        self._write_json({"retryable": True}, status=503)


class _NoKeepAliveJsonHandler(JsonHttpHandler):
    allow_http_keep_alive = False

    def do_GET(self) -> None:
        self._write_json({"ok": True})


class HttpServerTests(unittest.TestCase):
    def test_handler_can_close_idle_reverse_proxy_connections(self) -> None:
        server = HighBacklogThreadingHTTPServer(
            ("127.0.0.1", 0),
            _NoKeepAliveJsonHandler,
        )
        thread = Thread(
            target=server.serve_forever,
            kwargs={"poll_interval": 0.01},
            daemon=True,
        )
        thread.start()
        connection = HTTPConnection(*server.server_address, timeout=5)
        try:
            connection.request("GET", "/healthz")
            response = connection.getresponse()
            self.assertEqual(response.status, 200)
            self.assertEqual(response.getheader("Connection"), "close")
            self.assertEqual(json.loads(response.read()), {"ok": True})
            self.assertTrue(response.isclosed())
        finally:
            connection.close()
            server.shutdown()
            thread.join(timeout=1)
            server.server_close()

    def test_json_handler_owns_bounded_framing_and_response_headers(self) -> None:
        server = HighBacklogThreadingHTTPServer(
            ("127.0.0.1", 0),
            _JsonHandler,
        )
        thread = Thread(
            target=server.serve_forever,
            kwargs={"poll_interval": 0.01},
            daemon=True,
        )
        thread.start()
        try:

            def post(headers: dict[str, str], body: bytes = b""):
                connection = HTTPConnection(*server.server_address, timeout=5)
                connection.putrequest("POST", "/")
                for key, value in headers.items():
                    connection.putheader(key, value)
                connection.endheaders(body)
                response = connection.getresponse()
                payload = json.loads(response.read())
                connection.close()
                return response, payload

            for headers, status in (
                ({"Transfer-Encoding": "chunked", "Content-Length": "0"}, 400),
                ({}, 400),
                ({"Content-Length": "invalid"}, 400),
                ({"Content-Length": "-1"}, 400),
                ({"Content-Length": "5"}, 413),
            ):
                with self.subTest(headers=headers):
                    response, _payload = post(headers)
                    self.assertEqual(response.status, status)

            response, payload = post({"Content-Length": "2"}, b"{}")
            self.assertEqual(response.status, 200)
            self.assertEqual(payload, {"payload": {}})
            self.assertEqual(response.getheader("Connection"), "close")
            self.assertEqual(response.getheader("Content-Type"), "application/json")
            self.assertEqual(response.getheader("X-Test"), "preserved")
            self.assertEqual(
                int(response.getheader("Content-Length", "0")),
                len(json.dumps(payload, separators=(",", ":")).encode()),
            )
        finally:
            server.shutdown()
            thread.join(timeout=1)
            server.server_close()

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

    def test_early_body_rejection_closes_connection_before_body_is_read(self) -> None:
        server = HighBacklogThreadingHTTPServer(
            ("127.0.0.1", 0),
            _EarlyRejectingJsonHandler,
        )
        thread = Thread(
            target=server.serve_forever,
            kwargs={"poll_interval": 0.01},
            daemon=True,
        )
        thread.start()
        connection = HTTPConnection(*server.server_address, timeout=5)
        try:
            connection.request(
                "POST",
                "/v1/sandboxes",
                body=b'{"id":"unread"}',
                headers={"Content-Type": "application/json"},
            )
            response = connection.getresponse()
            self.assertEqual(response.status, 503)
            self.assertEqual(response.getheader("Connection"), "close")
            response.read()
        finally:
            connection.close()
            server.shutdown()
            thread.join(timeout=1)
            server.server_close()

    def test_thread_capacity_returns_retryable_json_instead_of_disconnect(self) -> None:
        _BlockingHandler.started.clear()
        _BlockingHandler.release.clear()
        server = HighBacklogThreadingHTTPServer(
            ("127.0.0.1", 0),
            _BlockingHandler,
            max_request_threads=1,
        )
        thread = Thread(
            target=server.serve_forever,
            kwargs={"poll_interval": 0.01},
            daemon=True,
        )
        thread.start()
        first = HTTPConnection(*server.server_address, timeout=5)
        second = HTTPConnection(*server.server_address, timeout=5)
        try:
            first.request("GET", "/hold")
            self.assertTrue(_BlockingHandler.started.wait(timeout=2))

            second.request(
                "POST",
                "/v1/sandboxes",
                headers={"Content-Length": "0"},
            )
            response = second.getresponse()
            body = json.loads(response.read().decode("utf-8"))

            self.assertEqual(response.status, 503)
            self.assertEqual(response.getheader("Content-Type"), "application/json")
            self.assertEqual(response.getheader("Retry-After"), "1")
            self.assertEqual(response.getheader("X-UCloud-Sandbox-Retryable"), "true")
            self.assertTrue(body["retryable"])
            self.assertEqual(
                body["error_code"],
                "http_request_capacity_exhausted",
            )
        finally:
            _BlockingHandler.release.set()
            try:
                first.getresponse().read()
            except OSError:
                pass
            first.close()
            second.close()
            server.shutdown()
            thread.join(timeout=1)
            server.server_close()


if __name__ == "__main__":
    unittest.main()
