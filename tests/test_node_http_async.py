from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
import asyncio
import gzip
import inspect
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import socket
from threading import Event, Thread, current_thread
import unittest
from unittest.mock import patch
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

    def test_blocked_enqueue_does_not_block_loop_completions_or_race_shutdown(self):
        loop = self.pool._start()
        active, cleaned = Event(), Event()
        async def waiting():
            active.set()
            try:
                await asyncio.Event().wait()
            finally:
                cleaned.set()
        existing = self.pool.submit(waiting())
        self.assertTrue(active.wait(1))
        entered, release, closed = Event(), Event(), Event()
        self.addCleanup(release.set)
        original = loop.call_soon_threadsafe
        def enqueue(*args, **kwargs):
            if current_thread().name == "blocked-submitter":
                entered.set()
                if not release.wait(2):
                    raise AssertionError("test did not release enqueue")
            return original(*args, **kwargs)
        owned = waiting()
        results, errors = [], []
        def submit():
            try:
                results.append(self.pool.submit(owned))
            except BaseException as exc:
                errors.append(exc)
        def close():
            self.pool.close()
            closed.set()
        with patch.object(loop, "call_soon_threadsafe", side_effect=enqueue):
            submitter = Thread(target=submit, name="blocked-submitter")
            submitter.start()
            self.assertTrue(entered.wait(1))
            unlocked = self.pool._guard.acquire(blocking=False)
            self.assertTrue(unlocked, "enqueue syscall must not own the shared guard")
            if unlocked:
                self.pool._guard.release()
            existing.cancel()
            self.assertTrue(cleaned.wait(1), "loop cleanup stalled behind enqueue")
            closer = Thread(target=close)
            closer.start()
            self.assertFalse(closed.wait(.02))
            release.set()
            submitter.join(2)
            closer.join(2)
        self.assertEqual(errors, [])
        self.assertTrue(closed.is_set())
        self.assertFalse(self.pool._thread.is_alive())
        self.assertTrue(results[0].done())
        self.assertEqual(inspect.getcoroutinestate(owned), inspect.CORO_CLOSED)
        self.assertEqual(self.pool._pending, set())

    def test_callback_handoff_finishes_before_racing_loop_stop(self):
        loop = self.pool._start()
        entered, release, delivered = Event(), Event(), Event()
        self.addCleanup(release.set)
        original = loop.call_soon_threadsafe
        def enqueue(*args, **kwargs):
            if current_thread().name == "callback-submitter":
                entered.set()
                if not release.wait(2):
                    raise AssertionError("test did not release callback")
            return original(*args, **kwargs)
        with patch.object(loop, "call_soon_threadsafe", side_effect=enqueue):
            sender = Thread(target=lambda: self.pool.call_soon(delivered.set), name="callback-submitter")
            sender.start()
            self.assertTrue(entered.wait(1))
            closer = Thread(target=self.pool.close)
            closer.start()
            release.set()
            sender.join(2)
            closer.join(2)
        self.assertTrue(delivered.is_set())
        self.assertFalse(self.pool._thread.is_alive())

    def test_completed_callback_can_close_pool_on_owning_loop(self):
        loop = self.pool._start()
        ready, closed = Event(), Event()
        gate = asyncio.Event()
        async def work():
            ready.set()
            await gate.wait()
            return 42
        future = self.pool.submit(work())
        self.assertTrue(ready.wait(1))
        failures = []
        def completed(_future):
            try:
                self.pool.close()
            except BaseException as exc:
                failures.append(exc)
            finally:
                closed.set()
        future.add_done_callback(completed)
        loop.call_soon_threadsafe(gate.set)
        self.assertEqual(future.result(timeout=2), 42)
        self.assertTrue(closed.wait(1))
        self.pool._thread.join(2)
        self.assertEqual(failures, [])
        self.assertFalse(self.pool._thread.is_alive())

    def test_cancel_before_loop_dispatch_releases_coroutine_on_shutdown(self):
        entered, release = Event(), Event()
        self.addCleanup(release.set)
        def block_loop():
            entered.set()
            release.wait(2)
        self.pool.call_soon(block_loop)
        self.assertTrue(entered.wait(1))
        async def work():
            await asyncio.Event().wait()
        owned = work()
        future = self.pool.submit(owned)
        self.assertTrue(future.cancel())
        release.set()
        self.pool.close()
        self.assertEqual(inspect.getcoroutinestate(owned), inspect.CORO_CLOSED)
        self.assertEqual(self.pool._pending, set())
        self.assertFalse(self.pool._thread.is_alive())

    def test_rejected_and_failed_submissions_close_unstarted_coroutine(self):
        async def work():
            return 42
        loop = self.pool._start()
        owned = work()
        with patch.object(loop, "call_soon_threadsafe", side_effect=RuntimeError("injected enqueue failure")):
            with self.assertRaisesRegex(RuntimeError, "injected enqueue failure"):
                self.pool.submit(owned)
        self.assertEqual(inspect.getcoroutinestate(owned), inspect.CORO_CLOSED)
        self.assertEqual(self.pool._enqueuing, 0)
        self.pool.close()
        owned = work()
        with self.assertRaises(URLError):
            self.pool.submit(owned)
        self.assertEqual(inspect.getcoroutinestate(owned), inspect.CORO_CLOSED)

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
