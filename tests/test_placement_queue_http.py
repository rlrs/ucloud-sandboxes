"""Real HTTP qualification of the durable public-to-placement handoff."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Event, Thread
from time import monotonic
from urllib.request import Request, urlopen
from unittest import TestCase, skipUnless
from unittest.mock import patch
from uuid import uuid4

from tests.test_control_plane import _gateway_server, _running_server, build_heartbeat
from ucloud_sandboxes.agent import post_heartbeat
from ucloud_sandboxes.models import ResourceQuantity
from ucloud_sandboxes.shared_control.placement_queue import (
    PlacementQueue,
    PlacementQueueWorker,
)
from ucloud_sandboxes.shared_control.routing_repository import PostgresRoutingStore

DSN = os.environ.get("UCLOUD_TEST_POSTGRES_DSN")


@skipUnless(DSN, "requires isolated PostgreSQL")
class PlacementQueueHTTPTests(TestCase):
    def setUp(self):
        self.directory = TemporaryDirectory()
        self.root = Path(self.directory.name)
        self.schema = "ucloud_routing_http_" + uuid4().hex
        self.routing_file = self.root / "routes.sqlite"
        self.routing = PostgresRoutingStore(
            self.routing_file, dsn=DSN, schema=self.schema
        )
        self.routing.migrate()
        self.patch = patch(
            "ucloud_sandboxes.control_plane.open_routing_store",
            return_value=self.routing,
        )
        self.patch.start()
        self.created = Event()
        self.release = Event()
        self.woken = Event()
        self.create_calls = []
        self.wake_calls = []
        fixture = self

        class Node(BaseHTTPRequestHandler):
            def log_message(self, *_args):
                pass

            def do_POST(self):
                payload = json.loads(
                    self.rfile.read(int(self.headers["Content-Length"]))
                )
                if self.path == "/v1/sandboxes":
                    operation = payload.pop("_ucloud_operation")
                    fixture.create_calls.append(operation)
                    fixture.created.set()
                    if not fixture.release.wait(10):
                        self.send_error(504)
                        return
                    body = {
                        "sandbox": {"spec": payload, "state": "running", **operation},
                        "exit_code": 0,
                    }
                    status = 201
                elif self.path.endswith("/wake"):
                    fixture.wake_calls.append(payload)
                    fixture.woken.set()
                    body = {"ok": True, "node_epoch": "boot-1", "activity_epoch": 30}
                    status = 200
                else:
                    self.send_error(404)
                    return
                raw = json.dumps(body).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

        self.node = ThreadingHTTPServer(("127.0.0.1", 0), Node)

    def tearDown(self):
        import psycopg
        from psycopg import sql

        self.release.set()
        self.patch.stop()
        self.routing.close()
        with psycopg.connect(DSN, autocommit=True) as conn:
            conn.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(self.schema))
            )
        self.directory.cleanup()

    @staticmethod
    def request(url, payload=None):
        data = None if payload is None else json.dumps(payload).encode()
        with urlopen(
            Request(url, data=data, headers={"Content-Type": "application/json"}),
            timeout=12,
        ) as response:
            return response.status, json.loads(response.read())

    @contextmanager
    def pipeline(self, *, create_concurrency=1):
        with _running_server(self.node) as node_url:
            public = _gateway_server(
                self.root, routing_file=self.routing_file, queue_placement=True
            )
            self.public_queue = public.RequestHandlerClass.placement_queue
            private = _gateway_server(
                self.root, routing_file=self.routing_file, placement_worker=True
            )
            with (
                _running_server(public) as public_url,
                _running_server(private) as private_url,
            ):
                heartbeat = build_heartbeat(
                    job_id="job-1",
                    node_id="node-1",
                    node_url=node_url,
                    node_epoch="boot-1",
                    activity_epoch=1,
                    capabilities=("sandbox", "image-cache", "disk-quota"),
                    cached_images=("busybox",),
                    total_resources=ResourceQuantity(
                        vcpu=16, memory_mb=16384, disk_mb=65536
                    ),
                )
                self.assertEqual(
                    post_heartbeat(
                        public_url + "/v1/nodes/heartbeat", heartbeat
                    ).status,
                    200,
                )
                stop = Event()
                worker = PlacementQueueWorker(
                    PlacementQueue(DSN, "test-deployment", schema=self.schema),
                    origin=private_url,
                    token="test-gateway-secret",
                    create_concurrency=create_concurrency,
                )
                errors = []

                def run():
                    try:
                        asyncio.run(worker.run(stop))
                    except BaseException as exc:
                        errors.append(exc)

                thread = Thread(target=run, daemon=True)
                thread.start()
                try:
                    yield public_url
                finally:
                    self.release.set()
                    stop.set()
                    thread.join(15)
                    self.assertFalse(
                        thread.is_alive(), "placement worker failed to stop"
                    )
                    if errors:
                        raise errors[0]

    @staticmethod
    def spec(name):
        return {
            "id": name,
            "image": "busybox",
            "cpus": 1,
            "memory_mb": 512,
            "disk_mb": 1024,
        }

    def test_slow_create_durably_reserves_route_without_blocking_public_requests(self):
        with self.pipeline() as public, ThreadPoolExecutor(1) as requests:
            try:
                result = requests.submit(
                    self.request, public + "/v1/sandboxes", self.spec("slow-one")
                )
                self.assertTrue(
                    self.created.wait(5), "queued create did not reach worker"
                )
                self.assertFalse(result.done())
                route = self.routing.get_sandbox("slow-one")
                self.assertIsNotNone(route)
                self.assertEqual(route.state, "creating")
                began = monotonic()
                self.assertEqual(self.request(public + "/healthz")[0], 200)
                self.assertEqual(self.request(public + "/v1/sandboxes")[0], 200)
                self.assertLess(
                    monotonic() - began, 1, "slow create blocked public reads"
                )
            finally:
                self.release.set()
            status, body = result.result(8)
            self.assertEqual(status, 201)
            self.assertEqual(body["sandbox"]["spec"]["id"], "slow-one")
            self.assertEqual(self.routing.get_sandbox("slow-one").state, "running")
            self.assertEqual(len(self.create_calls), 1)
            with self.routing.pool.connection() as conn:
                command = conn.execute(
                    "SELECT state,generation FROM gateway_commands"
                ).fetchone()
            self.assertEqual(command["state"], "done")
            self.assertEqual(command["generation"], route.generation)

    def test_wake_lane_progresses_while_create_lane_is_occupied(self):
        with self.pipeline() as public, ThreadPoolExecutor(1) as requests:
            self.release.set()
            self.request(public + "/v1/sandboxes", self.spec("warm-one"))
            route = self.routing.get_sandbox("warm-one")
            self.routing.upsert_sandbox(
                replace(route, node_epoch="boot-1", activity_epoch=20)
            )
            self.release.clear()
            self.created.clear()
            try:
                create = requests.submit(
                    self.request, public + "/v1/sandboxes", self.spec("blocked-one")
                )
                self.assertTrue(self.created.wait(5))
                identity = {
                    "generation": route.generation,
                    "operation_id": "wake-http-1",
                }
                status, body = self.request(
                    public + "/v1/sandboxes/warm-one/wake", identity
                )
                self.assertEqual(status, 200)
                self.assertTrue(body["ok"])
                self.assertTrue(self.woken.is_set())
                self.assertFalse(
                    create.done(), "wake test did not overlap a blocked create"
                )
                self.assertEqual(self.wake_calls[0]["generation"], route.generation)
                self.assertEqual(
                    self.wake_calls[0]["operation_id"], identity["operation_id"]
                )
            finally:
                self.release.set()
            self.assertEqual(create.result(8)[0], 201)

    def test_client_disconnect_does_not_cancel_durable_create(self):
        import socket
        from time import sleep
        from urllib.parse import urlsplit

        with self.pipeline() as public:
            target = urlsplit(public)
            raw = json.dumps(self.spec("disconnected-one")).encode()
            connection = socket.create_connection(
                (target.hostname, target.port), timeout=5
            )
            try:
                connection.sendall(
                    b"POST /v1/sandboxes HTTP/1.1\r\nHost: localhost\r\n"
                    b"Content-Type: application/json\r\nContent-Length: "
                    + str(len(raw)).encode()
                    + b"\r\nConnection: close\r\n\r\n"
                    + raw
                )
                self.assertTrue(self.created.wait(5))
            finally:
                connection.close()
                self.release.set()
            deadline = monotonic() + 5
            while monotonic() < deadline:
                with self.routing.pool.connection() as conn:
                    command = conn.execute(
                        "SELECT state,result_status FROM gateway_commands WHERE sandbox_id=%s",
                        ("disconnected-one",),
                    ).fetchone()
                if command and command["state"] == "done":
                    break
                sleep(0.02)
            self.assertEqual(command["state"], "done")
            self.assertEqual(command["result_status"], 201)
            self.assertEqual(
                self.routing.get_sandbox("disconnected-one").state, "running"
            )
            self.assertEqual(len(self.create_calls), 1)

    def test_duplicate_public_creates_share_dispatch_and_both_receive_result(self):
        from time import sleep

        with self.pipeline() as public, ThreadPoolExecutor(2) as requests:
            try:
                first = requests.submit(
                    self.request, public + "/v1/sandboxes", self.spec("duplicate-one")
                )
                self.assertTrue(self.created.wait(5))
                # Key order differs but request intent is identical.
                second = requests.submit(
                    self.request,
                    public + "/v1/sandboxes",
                    dict(reversed(list(self.spec("duplicate-one").items()))),
                )
                deadline = monotonic() + 5
                while monotonic() < deadline:
                    counts = [
                        len(group)
                        for group in tuple(self.public_queue.waiters.values())
                    ]
                    if counts == [2]:
                        break
                    sleep(0.01)
                self.assertEqual(
                    counts, [2], "duplicate callers did not share one command"
                )
                with self.routing.pool.connection() as conn:
                    self.assertEqual(
                        conn.execute(
                            "SELECT COUNT(*) AS n FROM gateway_commands"
                        ).fetchone()["n"],
                        1,
                    )
                self.assertEqual(len(self.create_calls), 1)
            finally:
                self.release.set()
            self.assertEqual(first.result(8), second.result(8))
            self.assertEqual(first.result()[0], 201)
            self.assertEqual(len(self.create_calls), 1)

    def test_coalescing_keeps_original_deadline_body_and_allows_new_completed_command(
        self,
    ):
        async def run():
            queue = PlacementQueue(DSN, "test-deployment", schema=self.schema)
            await queue.open()
            try:
                spec = self.spec("coalesced")
                original = json.dumps(spec).encode()
                first = await queue.submit(
                    "create",
                    spec["id"],
                    "/v1/sandboxes",
                    {},
                    original,
                    timeout_seconds=30,
                )
                duplicate = await queue.submit(
                    "create",
                    spec["id"],
                    "/v1/sandboxes",
                    {},
                    json.dumps(spec, sort_keys=True).encode(),
                    timeout_seconds=600,
                )
                self.assertEqual(first, duplicate)
                claims = await queue.claim("create", 2)
                self.assertEqual(len(claims), 1)
                self.assertEqual(bytes(claims[0]["body"]), original)
                self.assertTrue(await queue.complete(claims[0], 201, {}, b"{}"))
                next_command = await queue.submit(
                    "create", spec["id"], "/v1/sandboxes", {}, original
                )
                self.assertNotEqual(first.command_id, next_command.command_id)
                different_reference = await queue.submit(
                    "create",
                    spec["id"],
                    "/v1/sandboxes",
                    {"X-UCloud-Image-Reference-Kind": "managed"},
                    original,
                )
                self.assertNotEqual(
                    next_command.command_id, different_reference.command_id
                )
            finally:
                await queue.close()

        asyncio.run(run())
