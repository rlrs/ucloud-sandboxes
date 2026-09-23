from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
import gzip
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import socket
from threading import Event, Thread
import unittest
from urllib.error import URLError
from ucloud_sandboxes.node_http_async import AsyncNodeHttpPool


@contextmanager
def server(handler):
    node = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    node.daemon_threads = True
    thread = Thread(
        target=node.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True
    )
    thread.start()
    try:
        yield f"http://127.0.0.1:{node.server_port}"
    finally:
        node.shutdown()
        node.server_close()
        thread.join(2)


class Node(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_args):
        pass

    def reply(self, body=b"{}", status=200, headers=None):
        self.send_response(status)
        self.send_header("Content-Length", str(len(body)))
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)


class AsyncNodeHttpTests(unittest.TestCase):
    def setUp(self):
        self.pool = AsyncNodeHttpPool(control_connections=1, poll_connections=1)
        self.addCleanup(self.pool.close)

    def call(self, url, *, method="GET", body=None, limit=1024, poll=False, timeout=1):
        return self.pool.request(
            method,
            url,
            headers={"Authorization": "Bearer test-token"},
            body=body,
            timeout=timeout,
            connect_timeout=timeout,
            response_limit=limit,
            event_poll=poll,
        )

    def test_redirect_is_not_followed_and_compressed_bytes_are_unchanged(self):
        compressed = gzip.compress(b"binary\0body")

        class Handler(Node):
            def do_GET(inner):
                if inner.path == "/redirect":
                    inner.reply(status=307, headers={"Location": "/must-not-follow"})
                else:
                    self.assertEqual(inner.path, "/gzip")
                    inner.reply(compressed, headers={"Content-Encoding": "gzip"})

        with server(Handler) as url:
            with self.call(url + "/redirect") as response:
                self.assertEqual(response.status, 307)
                self.assertEqual(response.headers["Location"], "/must-not-follow")
            with self.call(url + "/gzip") as response:
                self.assertEqual(response.read(), compressed)
                self.assertEqual(response.headers["Content-Encoding"], "gzip")

    def test_long_poll_does_not_starve_control_and_response_memory_is_bounded(self):
        entered, release = Event(), Event()
        self.addCleanup(release.set)

        class Handler(Node):
            def do_GET(inner):
                if inner.path == "/poll":
                    entered.set()
                    release.wait(3)
                inner.reply(b"x" * 8192)

        with server(Handler) as url, ThreadPoolExecutor(1) as executor:
            future = executor.submit(self.call, url + "/poll", poll=True, timeout=4)
            self.assertTrue(entered.wait(1))
            with self.call(url + "/control", limit=20) as response:
                self.assertEqual(len(response.read()), 21)
            release.set()
            future.result().close()

    def test_truncated_or_dropped_mutations_and_reads_are_never_replayed(self):
        calls = []

        class Handler(Node):
            def do_GET(inner):
                calls.append(inner.command)
                inner.rfile.read(int(inner.headers.get("Content-Length", "0")))
                inner.connection.shutdown(socket.SHUT_RDWR)
                inner.connection.close()

            do_POST = do_GET
            do_PUT = do_GET
            do_DELETE = do_GET

        with server(Handler) as url:
            for method in ("GET", "POST", "PUT", "DELETE"):
                with self.subTest(method=method), self.assertRaises(URLError):
                    self.call(
                        url, method=method, body=b"payload" if method != "GET" else None
                    )
        self.assertEqual(calls, ["GET", "POST", "PUT", "DELETE"])

        class Truncated(Node):
            def do_GET(inner):
                inner.send_response(200)
                inner.send_header("Content-Length", "5")
                inner.end_headers()
                inner.wfile.write(b"x")
                inner.wfile.flush()
                inner.close_connection = True

        with server(Truncated) as url:
            with self.assertRaises(URLError):
                self.call(url)

    def test_connection_admission_timeout_is_proven_pre_dispatch(self):
        from urllib3.exceptions import EmptyPoolError

        entered, release = Event(), Event()
        calls = []

        class Handler(Node):
            def do_POST(inner):
                calls.append(inner.path)
                inner.rfile.read(int(inner.headers.get("Content-Length", "0")))
                entered.set()
                release.wait(3)
                inner.reply()

        with server(Handler) as url, ThreadPoolExecutor(1) as executor:
            future = executor.submit(
                self.call, url + "/first", method="POST", body=b"first", timeout=4
            )
            self.assertTrue(entered.wait(1))
            with self.assertRaises(URLError) as caught:
                self.call(url + "/second", method="POST", body=b"second", timeout=0.02)
            self.assertIsInstance(caught.exception.reason, EmptyPoolError)
            release.set()
            future.result().close()
        self.assertEqual(calls, ["/first"])

    def test_timeout_and_shutdown_fail_waiters_without_hanging(self):
        entered, release = Event(), Event()

        class Handler(Node):
            def do_GET(inner):
                entered.set()
                release.wait(3)
                inner.close_connection = True

        with server(Handler) as url, ThreadPoolExecutor(1) as executor:
            with self.assertRaises(URLError):
                self.call(url, timeout=0.02)
            future = executor.submit(self.call, url, timeout=5)
            self.assertTrue(entered.wait(1))
            self.pool.close()
            release.set()
            with self.assertRaises(URLError):
                future.result(timeout=2)
            with self.assertRaises(URLError):
                self.call(url)
