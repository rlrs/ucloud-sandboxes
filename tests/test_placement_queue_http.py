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
from urllib.error import HTTPError
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
        self.execution_finished = Event()
        self.create_calls = []
        self.wake_calls = []
        self.reject_first_node = False
        self.rejected = []
        fixture = self

        class Node(BaseHTTPRequestHandler):
            def log_message(self, *_args):
                pass

            def do_DELETE(self):
                raw = b'{"ok":true}'
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

            def do_POST(self):
                payload = json.loads(
                    self.rfile.read(int(self.headers["Content-Length"]))
                )
                if self.path == "/v1/sandboxes":
                    operation = payload.pop("_ucloud_operation")
                    if fixture.reject_first_node and self.server is fixture.node:
                        fixture.rejected.append(operation)
                        raw = json.dumps(
                            {
                                "error": "direct node admission is closed",
                                "error_code": "node_admission_closed",
                                "retryable": True,
                            }
                        ).encode()
                        self.send_response(503)
                        self.send_header("Content-Type", "application/json")
                        self.send_header("Content-Length", str(len(raw)))
                        self.end_headers()
                        self.wfile.write(raw)
                        return
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

        self.node_handler = Node
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
            self.public_queue = public.RequestHandlerClass.placement_queue.client
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
                execute = worker.execute

                async def observed_execute(session, command):
                    try:
                        return await execute(session, command)
                    finally:
                        self.execution_finished.set()

                worker.execute = observed_execute
                self.queue_worker = worker
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
            # A parked owner needs placement, so this wake uses the queue lane.
            route = self.routing.upsert_sandbox(
                replace(route, node_epoch="boot-1", activity_epoch=20, state="parked")
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

    def wake_commands(self):
        with self.routing.pool.connection() as conn:
            return conn.execute(
                "SELECT sandbox_id,state FROM gateway_commands WHERE kind='wake'"
            ).fetchall()

    def running_route(self, public, name):
        self.release.set()
        self.request(public + "/v1/sandboxes", self.spec(name))
        route = self.routing.get_sandbox(name)
        return self.routing.upsert_sandbox(
            replace(route, node_epoch="boot-1", activity_epoch=20)
        )

    def test_warm_wake_bypasses_durable_queue(self):
        with self.pipeline() as public:
            route = self.running_route(public, "resident-one")
            status, body = self.request(
                public + "/v1/sandboxes/resident-one/wake",
                {"generation": route.generation, "operation_id": "wake-warm-1"},
            )
            self.assertEqual(status, 200)
            self.assertTrue(body["ok"])
            self.assertEqual(self.wake_calls[0]["operation_id"], "wake-warm-1")
            self.assertEqual(self.wake_calls[0]["generation"], route.generation)
            self.assertEqual(self.wake_commands(), [])

    def test_stale_generation_warm_wake_is_left_to_durable_queue(self):
        with self.pipeline() as public:
            route = self.running_route(public, "stale-one")
            with self.assertRaises(HTTPError) as rejected:
                self.request(
                    public + "/v1/sandboxes/stale-one/wake",
                    {"generation": route.generation + 1, "operation_id": "wake-stale"},
                )
            self.assertEqual(rejected.exception.code, 409)
            rejected.exception.close()
            self.assertEqual(self.wake_calls, [])
            self.assertEqual(
                [(row["sandbox_id"], row["state"]) for row in self.wake_commands()],
                [("stale-one", "done")],
            )

    def test_parked_wake_still_uses_durable_queue(self):
        with self.pipeline() as public:
            route = self.running_route(public, "parked-one")
            route = self.routing.upsert_sandbox(replace(route, state="parked"))
            status, _body = self.request(
                public + "/v1/sandboxes/parked-one/wake",
                {"generation": route.generation, "operation_id": "wake-parked-1"},
            )
            self.assertEqual(status, 200)
            self.assertEqual(self.wake_calls[0]["operation_id"], "wake-parked-1")
            self.assertEqual(
                [(row["sandbox_id"], row["state"]) for row in self.wake_commands()],
                [("parked-one", "done")],
            )

    def test_route_parked_after_warm_check_returns_to_durable_queue(self):
        from ucloud_sandboxes.control_plane import ControlPlaneHandler

        with self.pipeline() as public:
            route = self.running_route(public, "racing-one")
            parked = self.routing.upsert_sandbox(replace(route, state="parked"))
            # The warm read observed "running"; admission then sees "parked".
            with patch.object(ControlPlaneHandler, "_warm_wake_route", return_value=True):
                status, _body = self.request(
                    public + "/v1/sandboxes/racing-one/wake",
                    {"generation": parked.generation, "operation_id": "wake-race-1"},
                )
            self.assertEqual(status, 200)
            self.assertEqual(
                [(row["sandbox_id"], row["state"]) for row in self.wake_commands()],
                [("racing-one", "done")],
            )

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

    def test_closed_admission_reselects_alternate_worker_with_new_generation(self):
        self.reject_first_node = True
        self.release.set()
        alternate = ThreadingHTTPServer(("127.0.0.1", 0), self.node_handler)
        with self.pipeline() as public, _running_server(alternate) as alternate_url:
            heartbeat = build_heartbeat(
                job_id="job-2",
                node_id="node-2",
                node_url=alternate_url,
                node_epoch="boot-2",
                activity_epoch=1,
                capabilities=("sandbox", "image-cache", "disk-quota"),
                cached_images=("busybox",),
                total_resources=ResourceQuantity(
                    vcpu=16, memory_mb=16384, disk_mb=65536
                ),
            )
            self.assertEqual(
                post_heartbeat(public + "/v1/nodes/heartbeat", heartbeat).status, 200
            )
            status, body = self.request(
                public + "/v1/sandboxes", self.spec("reselected")
            )
            self.assertEqual(status, 201)
            self.assertEqual(len(self.rejected), 1)
            self.assertEqual(len(self.create_calls), 1)
            route = self.routing.get_sandbox("reselected")
            self.assertEqual(route.job_id, "job-2")
            self.assertGreater(route.generation, self.rejected[0]["generation"])
            self.assertEqual(body["sandbox"]["generation"], route.generation)
            self.assertIsNone(self.routing.get_pending("reselected"))

    def delete(self, public, sandbox_id):
        with urlopen(
            Request(public + "/v1/sandboxes/" + sandbox_id, method="DELETE"), timeout=5
        ) as response:
            self.assertEqual(response.status, 200)
            return json.loads(response.read())

    def wait_for_command(self, sandbox_id):
        from time import sleep

        deadline = monotonic() + 5
        while monotonic() < deadline:
            with self.routing.pool.connection() as conn:
                row = conn.execute(
                    "SELECT * FROM gateway_commands WHERE sandbox_id=%s", (sandbox_id,)
                ).fetchone()
            if row:
                return row
            sleep(0.01)
        self.fail("public request was not durably queued")

    def test_delete_queued_unbound_create_prevents_future_dispatch(self):
        from urllib.error import HTTPError

        with (
            self.pipeline(create_concurrency=0) as public,
            ThreadPoolExecutor(1) as requests,
        ):
            result = requests.submit(
                self.request, public + "/v1/sandboxes", self.spec("delete-queued")
            )
            command = self.wait_for_command("delete-queued")
            self.assertIsNone(command["generation"])
            self.assertEqual(command["state"], "queued")
            self.delete(public, "delete-queued")
            self.queue_worker.budgets["create"] = 1
            self.release.set()
            with self.assertRaises(HTTPError) as cancelled:
                result.result(5)
            self.assertIn(cancelled.exception.code, (409, 410))
            cancelled.exception.close()
            with self.routing.pool.connection() as conn:
                after = conn.execute(
                    "SELECT state FROM gateway_commands WHERE command_id=%s",
                    (command["command_id"],),
                ).fetchone()
            self.assertEqual(after["state"], "done")
            self.assertIsNone(self.routing.get_sandbox("delete-queued"))
            self.assertIsNone(self.routing.get_pending("delete-queued"))
            self.assertFalse(self.create_calls)

    def test_delete_bound_create_does_not_authorize_replay_as_new_incarnation(self):
        from urllib.error import HTTPError

        with self.pipeline() as public, ThreadPoolExecutor(1) as requests:
            try:
                result = requests.submit(
                    self.request, public + "/v1/sandboxes", self.spec("delete-bound")
                )
                self.assertTrue(self.created.wait(5))
                generation = self.routing.get_sandbox("delete-bound").generation
                self.delete(public, "delete-bound")
            finally:
                self.release.set()
            with self.assertRaises(HTTPError) as cancelled:
                result.result(5)
            self.assertIn(cancelled.exception.code, (409, 410))
            cancelled.exception.close()
            self.assertTrue(
                self.execution_finished.wait(5),
                "cancelled in-flight create did not finish",
            )
            self.assertIsNone(self.routing.get_sandbox("delete-bound"))
            self.assertEqual(
                [call["generation"] for call in self.create_calls], [generation]
            )
            with self.routing.pool.connection() as conn:
                row = conn.execute(
                    "SELECT state,generation FROM gateway_commands WHERE sandbox_id=%s",
                    ("delete-bound",),
                ).fetchone()
            self.assertEqual(row["state"], "done")
            self.assertEqual(row["generation"], generation)
