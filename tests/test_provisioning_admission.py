from contextlib import ExitStack
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import unittest

from tests import test_control_plane as helpers
from ucloud_sandboxes.agent import post_heartbeat
from ucloud_sandboxes.models import ResourceQuantity
from ucloud_sandboxes.routing import RoutingStore


class ProvisioningAdmissionTests(unittest.TestCase):
    _request = helpers.ControlPlaneTests._json_request

    def test_drain_during_cold_image_pull_reselects_or_keeps_retryable_demand(self):
        for alternate in (False, True):
            with self.subTest(alternate=alternate), helpers._temporary_root() as root, ExitStack() as stack:
                class Node(BaseHTTPRequestHandler):
                    def log_message(self, *_args):
                        pass

                    def send_json(self, payload, status=200):
                        body = json.dumps(payload).encode()
                        self.send_response(status)
                        self.send_header("Content-Type", "application/json")
                        self.send_header("Content-Length", str(len(body)))
                        self.end_headers()
                        self.wfile.write(body)

                    def do_GET(self):
                        self.send_json({"images": [], "sandboxes": []})

                    def do_POST(self):
                        raw = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                        self.server.calls.append(self.path)
                        if self.path == "/v1/images/pull":
                            if self.server.closed_admission:
                                self.send_json({"error": "direct node admission is closed",
                                                "error_code": "node_admission_closed", "retryable": True}, 503)
                            else:
                                self.send_json({"image": {"id": "base", "tag": "busybox"}})
                            return
                        operation = raw.pop("_ucloud_operation")
                        self.send_json({"sandbox": {"spec": raw, "state": "running",
                            "generation": operation["generation"], "operation_id": operation["operation_id"],
                            "spec_hash": operation["spec_hash"]}}, 201)

                routing_file = root / "routes.sqlite"
                gateway = helpers._gateway_server(root, routing_file=routing_file)
                base = stack.enter_context(helpers._running_server(gateway))
                nodes = []
                for n in range(2 if alternate else 1):
                    node = ThreadingHTTPServer(("127.0.0.1", 0), Node)
                    node.closed_admission = n == 0
                    node.calls = []
                    nodes.append(node)
                    url = stack.enter_context(helpers._running_server(node))
                    post_heartbeat(f"{base}/v1/nodes/heartbeat", helpers.build_heartbeat(
                        job_id=f"job-{n}", node_id=f"node-{n}", node_url=url,
                        capabilities=("sandbox", "image-cache", "disk-quota"), cached_images=(),
                        total_resources=ResourceQuantity(vcpu=4, memory_mb=4096, disk_mb=8192),
                    ))
                result = self._request(f"{base}/v1/sandboxes", method="POST", allow_error=True,
                    payload={"id": "cold-drain", "image": "busybox", "cpus": 1, "memory_mb": 512, "disk_mb": 1024})
                store = RoutingStore(routing_file)
                self.assertEqual(nodes[0].calls, ["/v1/images/pull"])
                if alternate:
                    self.assertEqual(result["sandbox"]["state"], "running")
                    self.assertEqual(store.get_sandbox("cold-drain").job_id, "job-1")
                    self.assertIsNone(store.get_pending("cold-drain"))
                    self.assertEqual(nodes[1].calls, ["/v1/images/pull", "/v1/sandboxes"])
                else:
                    self.assertEqual(result["status"], 503)
                    self.assertTrue(result["body"]["retryable"])
                    self.assertEqual(result["body"]["error_code"], "node_admission_closed")
                    self.assertIsNone(store.get_sandbox("cold-drain"))
                    self.assertEqual(store.get_pending("cold-drain").failure_reason, "node_admission_closed")

    def test_cold_builder_submission_keeps_demand_and_marks_safe_retry(self):
        with helpers._temporary_root() as root:
            gateway = helpers._gateway_server(root, routing_file=root / "routes.sqlite")
            context = helpers._store_build_context(gateway, helpers._tar_gz_context({"Dockerfile": b"FROM scratch\n"}))
            with helpers._running_server(gateway) as base:
                result = self._request(f"{base}/v1/images/build", method="POST", allow_error=True,
                    payload={"id": "cold-build", "tag": "example/cold-build", "wait": False, **context})
            self.assertEqual(result["status"], 503)
            self.assertEqual(result["body"]["error_code"], "builder_not_ready")
            self.assertTrue(result["body"]["retryable"])
            self.assertEqual(result["headers"]["Retry-After"], "2")
            self.assertEqual(RoutingStore(root / "routes.sqlite").pending_image_build_count(), 1)
