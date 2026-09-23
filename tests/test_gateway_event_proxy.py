from contextlib import contextmanager, ExitStack
from dataclasses import replace
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Condition, Event, Thread
import time
import unittest
from unittest.mock import patch

from ucloud_sandboxes.control_plane import build_server
from ucloud_sandboxes.gateway_response_proxy import encode_response
from ucloud_sandboxes.models import NodeHeartbeat, ResourceQuantity, utc_now
from ucloud_sandboxes.routing import ExecRoute, SandboxRoute


@contextmanager
def running(server):
    thread = Thread(
        target=server.serve_forever, kwargs={"poll_interval": 0.005}, daemon=True
    )
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        thread.join(3)


class AsyncGatewayEventTests(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.release = Event()
        self.entered = Condition()
        self.calls = []
        self.uploads = []
        self.upload_status = 200
        self.drop_upload = False
        parent = self

        class Node(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *_args):
                pass

            def do_PUT(self):
                body = self.rfile.read(int(self.headers['Content-Length']))
                with parent.entered:
                    parent.uploads.append((body, dict(self.headers)))
                    parent.calls.append((self.path, self.headers.get('Authorization')))
                    parent.entered.notify_all()
                parent.release.wait(3)
                if parent.drop_upload:
                    self.close_connection = True
                    return
                response = json.dumps({'size': len(body)}).encode()
                self.send_response(parent.upload_status)
                self.send_header('Content-Length', str(len(response)))
                self.end_headers()
                try:
                    self.wfile.write(response)
                except OSError:
                    pass

            def do_GET(self):
                with parent.entered:
                    parent.calls.append((self.path, self.headers.get("Authorization")))
                    parent.entered.notify_all()
                parent.release.wait(3)
                body = b'{"events":[],"final":true}'
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                try:
                    self.wfile.write(body)
                except OSError:
                    pass

        self.node = ThreadingHTTPServer(("127.0.0.1", 0), Node)
        self.node.daemon_threads = True
        self.stack.enter_context(running(self.node))
        self.addCleanup(self.release.set)
        self.gateway = build_server(
            "127.0.0.1",
            0,
            self.root / "control.sqlite",
            gateway_bearer_token="operator",
            sandbox_api_token="public",
            heartbeat_bearer_token="heartbeat",
            node_control_bearer_token="node-private",
            deployment_id="test",
            routing_file=self.root / "routing.sqlite",
            image_file=self.root / "images.json",
            metrics_file=self.root / "metrics.sqlite",
            max_http_request_threads=2,
        )
        self.stack.enter_context(running(self.gateway))
        handler = self.gateway.RequestHandlerClass
        self.heartbeat = NodeHeartbeat(
            node_id="node",
            job_id="job",
            updated_at=utc_now(),
            received_at=utc_now(),
            node_url=f"http://127.0.0.1:{self.node.server_port}",
            active_sandboxes=1,
            deployment_id="test",
            total_resources=ResourceQuantity(vcpu=32, memory_mb=98304),
        )
        handler.store.receive_heartbeat(self.heartbeat)
        handler.routing_store.upsert_sandbox(SandboxRoute(
            sandbox_id='s', node_id='node', job_id='job', node_url=self.heartbeat.node_url,
            resources=ResourceQuantity(vcpu=1, memory_mb=1024), spec={'id': 's'},
            state='running', generation=7, create_operation_id='test-create', spec_hash='a' * 64,
        ))
        for index in range(8):
            handler.routing_store.upsert_exec(
                ExecRoute(
                    session_id=f"e{index}",
                    sandbox_id="s",
                    node_id="node",
                    job_id="job",
                    node_url=self.heartbeat.node_url,
                )
            )

    def poll(self, path="/v1/exec/e0/events", *, token="public", body=None):
        connection = HTTPConnection("127.0.0.1", self.gateway.server_port, timeout=3)
        self.addCleanup(connection.close)
        connection.request(
            "GET", path, body=body, headers={"Authorization": "Bearer " + token}
        )
        return connection

    def wait_calls(self, count):
        with self.entered:
            self.assertTrue(
                self.entered.wait_for(lambda: len(self.calls) >= count, timeout=2)
            )

    def upload(self, body=b'tool', *, token='public', sandbox='s'):
        connection = HTTPConnection('127.0.0.1', self.gateway.server_port, timeout=3)
        self.addCleanup(connection.close)
        connection.request('PUT', f'/v1/sandboxes/{sandbox}/files?path=/tool', body=body,
                           headers={'Authorization': 'Bearer ' + token,
                                    'X-UCloud-Sandbox-Generation': '999'})
        return connection

    def wait_upload_release(self):
        memory = self.gateway.RequestHandlerClass.upload_memory_limiter
        deadline = time.monotonic() + 1
        while time.monotonic() < deadline:
            if memory.acquire(blocking=False, weight=memory.capacity):
                memory.release(weight=memory.capacity)
                return
            time.sleep(.005)
        self.fail('upload body ownership leaked')

    def test_eight_blocked_uploads_release_two_handlers_and_hold_body_budget(self):
        connections = []
        body = b'x' * 65536
        for index in range(8):
            connections.append(self.upload(body))
            self.wait_calls(index + 1)
        response = self.poll('/healthz').getresponse()
        self.assertEqual(response.status, 200)
        response.read()
        memory = self.gateway.RequestHandlerClass.upload_memory_limiter
        responses = self.gateway.RequestHandlerClass.async_responses
        with responses._guard:
            receipts = tuple(responses._pending)
        self.assertEqual(len(receipts), 8)
        self.assertTrue(all(not receipt.cancel() for receipt in receipts))
        self.assertTrue(memory.acquire(blocking=False, weight=memory.capacity - 8 * len(body)))
        self.assertFalse(memory.acquire(blocking=False))
        memory.release(weight=memory.capacity - 8 * len(body))
        for payload, headers in self.uploads:
            self.assertEqual(payload, body)
            self.assertEqual(headers['Authorization'], 'Bearer node-private')
            self.assertEqual({key.lower(): value for key, value in headers.items()}
                             ['x-ucloud-sandbox-generation'], '7')
        self.release.set()
        for connection in connections:
            response = connection.getresponse()
            self.assertEqual(response.status, 200)
            self.assertEqual(json.loads(response.read())['size'], len(body))
        self.wait_upload_release()

    def test_upload_rejections_and_shutdown_release_body_ownership(self):
        for kwargs, status in (({'token': 'wrong'}, 401), ({'sandbox': 'missing'}, 404)):
            response = self.upload(**kwargs).getresponse()
            self.assertEqual(response.status, status)
            response.read()
        self.assertFalse(self.uploads)
        self.wait_upload_release()
        connection = self.upload()
        self.wait_calls(1)
        self.gateway.RequestHandlerClass.async_responses.close()
        with self.assertRaises((OSError, ConnectionError)):
            connection.getresponse()
        self.wait_upload_release()

    def test_upload_disconnect_and_worker_rejection_are_not_replayed(self):
        self.upload_status = 409
        self.release.set()
        response = self.upload().getresponse()
        self.assertEqual(response.status, 409)
        response.read()
        self.wait_upload_release()
        self.assertEqual(len(self.uploads), 1)
        self.drop_upload = True
        response = self.upload().getresponse()
        self.assertEqual(response.status, 502)
        response.read()
        self.wait_upload_release()
        self.assertEqual(len(self.uploads), 2)

    def test_client_disconnected_upload_finishes_once_and_releases_budget(self):
        connection = self.upload()
        self.wait_calls(1)
        connection.close()
        self.release.set()
        self.wait_upload_release()
        self.assertEqual(len(self.uploads), 1)

    def test_sleeping_polls_release_http_workers_and_keep_health_responsive(self):
        connections = []
        for index in range(8):
            connections.append(self.poll(f"/v1/exec/e{index}/events?wait=1"))
            self.wait_calls(index + 1)
        # Eight waiting requests exceed the two-thread HTTP admission pool.
        connection = self.poll("/healthz")
        response = connection.getresponse()
        self.assertEqual(response.status, 200)
        response.read()
        self.assertEqual(
            len(self.gateway.RequestHandlerClass.async_responses._pending), 8
        )
        self.release.set()
        for connection in connections:
            response = connection.getresponse()
            self.assertEqual(response.status, 200)
            self.assertEqual(response.headers["Connection"], "close")
            self.assertTrue(json.loads(response.read())["final"])
        self.assertEqual({token for _, token in self.calls}, {"Bearer node-private"})

    def test_auth_body_and_missing_route_rejections_never_dispatch(self):
        for kwargs, status in (
            ({"token": "wrong"}, 401),
            ({"body": b"unexpected"}, 400),
            ({"path": "/v1/exec/missing/events"}, 404),
        ):
            with self.subTest(kwargs=kwargs):
                response = self.poll(**kwargs).getresponse()
                self.assertEqual(response.status, status)
                response.read()
        self.assertEqual(self.calls, [])

    def test_stale_worker_has_same_retry_boundary(self):
        from datetime import timedelta

        self.gateway.RequestHandlerClass.store.receive_heartbeat(
            replace(
                self.heartbeat,
                received_at=utc_now() - timedelta(seconds=500),
                updated_at=utc_now() - timedelta(seconds=500),
            )
        )
        # receive_heartbeat records receipt itself; exercise the canonical
        # resolver with a stale read, preserving real routing and HTTP layers.
        with patch.object(
            self.gateway.RequestHandlerClass.store,
            "get_heartbeat",
            return_value=replace(
                self.heartbeat,
                updated_at=utc_now() - timedelta(seconds=500),
                received_at=utc_now() - timedelta(seconds=500),
            ),
        ):
            response = self.poll().getresponse()
            self.assertEqual(response.status, 503)
            self.assertEqual(
                json.loads(response.read())["error_code"], "sandbox_worker_unreachable"
            )
        self.assertEqual(self.calls, [])

    def test_shutdown_cancels_owned_responses_without_waiting_for_worker(self):
        connection = self.poll()
        self.wait_calls(1)
        started = time.monotonic()
        self.gateway.RequestHandlerClass.async_responses.close()
        self.assertLess(time.monotonic() - started, 1)
        with self.assertRaises((OSError, ConnectionError)):
            connection.getresponse()
        self.release.set()

    def test_partial_writes_are_completed_and_headers_cannot_inject(self):
        encoded = encode_response(
            200, {"Content-Type": "application/json", "Content-Length": "99"}, b"{}", {}
        )
        self.assertIn(b"Content-Length: 2\r\n", encoded)
        with self.assertRaises(ValueError):
            encode_response(200, {"x": "unsafe\r\ninjected: yes"}, b"", {})


class AsyncSocketOwnershipTests(unittest.TestCase):
    def test_shutdown_during_failed_handoff_releases_registered_socket(self):
        from urllib.error import URLError
        from unittest.mock import Mock
        from ucloud_sandboxes.gateway_response_proxy import AsyncGatewayResponses

        entered, cancel_queued, release = Event(), Event(), Event()

        class Pool:
            calls = 0

            def call_soon(self, callback):
                self.calls += 1
                if self.calls == 1:
                    entered.set()
                    if not release.wait(3):
                        raise AssertionError("handoff was not released")
                    raise URLError("pool closed during submission")
                cancel_queued.set()
                callback()

        responder = AsyncGatewayResponses(pool=Pool(), response_policy=None,
            response_limit=1024, timeout=1, connect_timeout=1)
        owned = Mock()
        lease_release = Mock()
        errors = []

        def start():
            try:
                responder.start(owned, url="http://node/events", headers={},
                                trace_headers={}, telemetry=None, method="PUT",
                                body=b"tool", event_poll=False, release=lease_release)
            except URLError:
                pass
            except BaseException as exc:
                errors.append(exc)

        def close():
            try:
                responder.close()
            except BaseException as exc:
                errors.append(exc)

        starter = Thread(target=start)
        closer = Thread(target=close)
        starter.start()
        try:
            self.assertTrue(entered.wait(1))
            closer.start()
            self.assertTrue(cancel_queued.wait(1), "handoff retained registry lock")
        finally:
            release.set()
            starter.join(3)
            if closer.ident is not None:
                closer.join(3)
        self.assertFalse(starter.is_alive())
        self.assertFalse(closer.is_alive())
        self.assertFalse(errors)
        owned.close.assert_called_once()
        lease_release.assert_called_once()
        self.assertFalse(responder._pending)

    def test_shutdown_removes_write_registration_before_closing_or_reusing_fd(self):
        import asyncio
        import socket
        from threading import current_thread
        from ucloud_sandboxes.gateway_response_proxy import AsyncGatewayResponses
        from ucloud_sandboxes.node_http_async import (
            AsyncNodeHttpPool,
            BufferedNodeResponse,
        )
        from ucloud_sandboxes.telemetry import Telemetry

        pool = AsyncNodeHttpPool()
        self.addCleanup(pool.close)
        sender, receiver = socket.socketpair()
        self.addCleanup(sender.close)
        self.addCleanup(receiver.close)
        sender.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
        closes, owned_fds = [], []
        body = b"x" * (2 * 1024 * 1024)

        async def upstream(*_args, **_kwargs):
            return BufferedNodeResponse(200, {}, body)

        pool.request_async = upstream

        class Tracked:
            def __init__(self, sock):
                self.sock = sock
                owned_fds.append(sock.fileno())

            def __getattr__(self, name):
                return getattr(self.sock, name)


            def close(self):
                loop = asyncio.get_running_loop()
                try:
                    loop._selector.get_key(self.sock.fileno())
                    registered = True
                except KeyError:
                    registered = False
                closes.append((current_thread().name, registered, self.sock.fileno()))
                self.sock.close()

        responses = AsyncGatewayResponses(
            response_policy=lambda r, _error: (r.status, r.headers, r.read()),
            response_limit=len(body),
            timeout=3,
            connect_timeout=1,
            pool=pool,
        )
        completion = responses.start(
            Tracked(sender),
            url="http://unused",
            headers={},
            trace_headers={},
            telemetry=Telemetry.disabled("test"),
        )
        # sock_sendall's first short write installs a writer without requiring
        # a BlockingIOError; query on the owning loop until it is registered.
        registered = Event()

        def observe():
            try:
                pool._loop._selector.get_key(owned_fds[0])
                registered.set()
            except KeyError:
                pass

        deadline = time.monotonic() + 1
        while not registered.is_set() and time.monotonic() < deadline:
            pool.call_soon(observe)
            time.sleep(0.005)
        self.assertTrue(registered.is_set(), "test must reach socket write backpressure")
        self.assertFalse(
            completion.done(), "slow downstream should retain an async write"
        )
        responses.close()
        self.assertTrue(completion.done())
        self.assertEqual(len(closes), 1)
        self.assertEqual(closes[0][:2], ("node-http-io", False))
        self.assertEqual(responses._pending, {})
        # A subsequent descriptor/transfer on the same loop must survive late
        # cleanup from the cancelled write, including ordinary fd-number reuse.
        new_sender, new_receiver = socket.socketpair()
        self.addCleanup(new_sender.close)
        self.addCleanup(new_receiver.close)
        new_sender.setblocking(False)
        sent = pool.submit(pool._loop.sock_sendall(new_sender, b"still-alive"))
        self.assertEqual(new_receiver.recv(64), b"still-alive")
        sent.result(timeout=1)

    def test_close_before_response_task_starts_still_closes_on_owner_loop(self):
        import socket
        from threading import current_thread
        from unittest.mock import Mock
        from ucloud_sandboxes.gateway_response_proxy import AsyncGatewayResponses
        from ucloud_sandboxes.node_http_async import AsyncNodeHttpPool
        from ucloud_sandboxes.telemetry import Telemetry

        pool = AsyncNodeHttpPool()
        self.addCleanup(pool.close)
        entered, release = Event(), Event()
        pool.call_soon(lambda: (entered.set(), release.wait(2)))
        self.assertTrue(entered.wait(1))
        sender, receiver = socket.socketpair()
        self.addCleanup(sender.close)
        self.addCleanup(receiver.close)
        closes = []
        lease_release = Mock()

        class Tracked:
            def __init__(self):
                self.sock = sender

            def setblocking(self, value):
                self.sock.setblocking(value)

            def close(self):
                closes.append(current_thread().name)
                self.sock.close()

        responses = AsyncGatewayResponses(
            response_policy=None,
            response_limit=100,
            timeout=1,
            connect_timeout=1,
            pool=pool,
        )
        completion = responses.start(
            Tracked(),
            url="unused",
            headers={},
            trace_headers={},
            telemetry=Telemetry.disabled("test"),
            method="PUT", body=b"tool", event_poll=False, release=lease_release,
        )
        closer = Thread(target=responses.close)
        closer.start()
        release.set()
        closer.join(2)
        self.assertFalse(closer.is_alive())
        self.assertTrue(completion.done())
        self.assertEqual(closes, ["node-http-io"])
        lease_release.assert_called_once()
