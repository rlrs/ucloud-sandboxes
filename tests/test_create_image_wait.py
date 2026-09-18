import json
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Event
from unittest.mock import Mock, patch
from urllib.request import Request

from ucloud_sandboxes import control_plane as cp
from tests import test_control_plane as helpers
from ucloud_sandboxes.routing import RoutingStore
from ucloud_sandboxes.models import ResourceQuantity
from tests.test_control_plane import (
    _gateway_server,
    _running_server,
    _temporary_root,
    build_heartbeat,
    post_heartbeat,
)


class CreateImageWaitTests(unittest.TestCase):
    _json_request = helpers.ControlPlaneTests._json_request

    def test_shared_pull_has_bounded_wait_and_bounded_background_capacity(self):
        tasks = cp.CreateImagePullTasks()
        release = Event()
        finished = Event()
        result = cp.ProxiedResponse(201, {}, b"{}")

        def pull():
            release.wait(5)
            finished.set()
            return result

        pull = Mock(side_effect=pull)
        with (
            patch.object(cp, "SANDBOX_IMAGE_WAIT_SECONDS", 0.02),
            patch.object(cp, "MAX_BACKGROUND_CREATE_IMAGE_PULLS", 1),
        ):
            try:
                with ThreadPoolExecutor(max_workers=12) as executor:
                    responses = list(
                        executor.map(lambda _: tasks.run(("same",), pull), range(12))
                    )
                self.assertEqual(pull.call_count, 1)
                self.assertTrue(
                    all(r.status == 503 and r.json()["retryable"] for r in responses)
                )
                self.assertEqual(tasks.run(("other",), pull).status, 503)
                self.assertEqual(pull.call_count, 1)
                release.set()
                self.assertTrue(finished.wait(1))
                self.assertIs(tasks.run(("same",), pull), result)
                self.assertEqual(pull.call_count, 1)
                self.assertIs(tasks.run(("other",), pull), result)
            finally:
                release.set()

    def test_long_pull_read_timeout_does_not_allow_long_connection_or_pool_wait(self):
        with patch.object(cp._NODE_HTTP_POOL, "request") as send:
            cp._open_node_request(
                Request("http://worker/v1/images/pull", data=b"{}"),
                timeout=1800,
                authenticated=True,
            )
        options = send.call_args.kwargs
        self.assertEqual(options["pool_timeout"], 5)
        self.assertEqual(options["timeout"].connect_timeout, 5)
        self.assertEqual(options["timeout"].read_timeout, 1800)

    def test_task_failure_is_propagated_and_does_not_leak_capacity(self):
        tasks = cp.CreateImagePullTasks()
        with self.assertRaisesRegex(RuntimeError, "lease unavailable"):
            tasks.run(("bad",), Mock(side_effect=RuntimeError("lease unavailable")))
        self.assertEqual(tasks.tasks, {})
        self.assertIsNone(tasks.run(("good",), lambda: None))

    def test_cold_create_retries_keep_identity_and_do_not_dispatch_before_pull(self):
        started, release = Event(), Event()
        creates = []
        pulls = []
        image_ready = Event()

        class Node(BaseHTTPRequestHandler):
            def log_message(self, *_args):
                pass

            def reply(self, payload, status=200):
                body = json.dumps(payload).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self):
                if self.path == "/v1/images":
                    self.reply(
                        {
                            "images": [
                                {"id": "busybox:latest", "tag": "busybox:latest"}
                            ]
                            if image_ready.is_set()
                            else []
                        }
                    )
                else:
                    self.reply({"error": "not found"}, 404)

            def do_POST(self):
                raw = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                if self.path == "/v1/images/pull":
                    pulls.append(raw)
                    started.set()
                    release.wait(5)
                    image_ready.set()
                    self.reply(
                        {"image": {"id": "busybox:latest", "tag": "busybox:latest"}},
                        201,
                    )
                else:
                    creates.append(raw)
                    operation = raw.pop("_ucloud_operation")
                    self.reply(
                        {
                            "sandbox": {
                                "spec": raw,
                                "state": "running",
                                "generation": operation["generation"],
                                "operation_id": operation["operation_id"],
                                "spec_hash": operation["spec_hash"],
                            }
                        },
                        201,
                    )

        with (
            _temporary_root() as root,
            patch.object(cp, "SANDBOX_IMAGE_WAIT_SECONDS", 0.03),
        ):
            node = ThreadingHTTPServer(("127.0.0.1", 0), Node)
            with _running_server(node) as node_url:
                gateway = _gateway_server(
                    root,
                    routing_file=root / "routes.sqlite",
                    max_concurrent_sandbox_creates=1,
                )
                with _running_server(gateway) as base:
                    try:
                        post_heartbeat(
                            f"{base}/v1/nodes/heartbeat",
                            build_heartbeat(
                                job_id="job-1",
                                node_id="node-1",
                                node_url=node_url,
                                capabilities=("sandbox", "image-cache", "disk-quota"),
                                cached_images=(),
                                total_resources=ResourceQuantity(
                                    vcpu=4, memory_mb=4096, disk_mb=8192
                                ),
                            ),
                        )
                        spec = {
                            "id": "cold",
                            "image": "busybox:latest",
                            "cpus": 1,
                            "memory_mb": 512,
                            "disk_mb": 1024,
                        }
                        for _ in range(3):
                            before = time.monotonic()
                            response = self._json_request(
                                f"{base}/v1/sandboxes",
                                method="POST",
                                payload=spec,
                                allow_error=True,
                            )
                            self.assertLess(time.monotonic() - before, 1)
                            self.assertEqual(response["status"], 503)
                            self.assertEqual(
                                response["body"]["error_code"], "image_warmup_pending"
                            )
                            self.assertEqual(response["headers"]["Retry-After"], "2")
                            route = RoutingStore(root / "routes.sqlite").get_sandbox(
                                "cold"
                            )
                            if _ == 0:
                                identity = (
                                    route.generation,
                                    route.create_operation_id,
                                    route.job_id,
                                )
                            self.assertEqual(
                                (
                                    route.generation,
                                    route.create_operation_id,
                                    route.job_id,
                                ),
                                identity,
                            )
                        self.assertTrue(started.is_set())
                        self.assertEqual(len(pulls), 1)
                        self.assertEqual(creates, [])
                        self.assertTrue(self._json_request(f"{base}/healthz")["ok"])
                        limiter = gateway.RequestHandlerClass.sandbox_create_limiter
                        self.assertTrue(limiter.acquire(blocking=False))
                        limiter.release()
                        release.set()
                        self.assertTrue(image_ready.wait(1))
                        response = self._json_request(
                            f"{base}/v1/sandboxes",
                            method="POST",
                            payload=spec,
                            allow_error=True,
                        )
                        self.assertIn("sandbox", response, response)
                        self.assertEqual(response["sandbox"]["generation"], identity[0])
                        self.assertEqual(
                            response["sandbox"]["operation_id"], identity[1]
                        )
                        self.assertEqual(len(creates), 1)
                        self.assertEqual(len(pulls), 1)
                    finally:
                        release.set()
