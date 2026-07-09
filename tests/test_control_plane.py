from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from tempfile import TemporaryDirectory
from http.client import HTTPConnection
from threading import Event, Lock, Thread
from time import sleep
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import sqlite3
from urllib import error, request
from urllib.parse import quote
import unittest
from unittest.mock import patch

from ucloud_sandboxes.agent import build_heartbeat, post_heartbeat
from ucloud_sandboxes import control_plane
from ucloud_sandboxes.control_plane import (
    IMAGE_BUILD_PROXY_TIMEOUT_SECONDS,
    IMAGE_PULL_PROXY_TIMEOUT_SECONDS,
    build_server,
)
from ucloud_sandboxes.deployment import package_version
from ucloud_sandboxes.http_server import DEFAULT_HTTP_REQUEST_QUEUE_SIZE
from ucloud_sandboxes.images import DockerImageRuntime, ImageRecord, ImageStore
from ucloud_sandboxes.models import NodeHeartbeat, ResourceQuantity, utc_now
from ucloud_sandboxes.node_agent import build_node_agent_server
from ucloud_sandboxes.routing import RoutingStore, SandboxRoute
from ucloud_sandboxes.sandbox import CommandResult, DockerGvisorRuntime


class CountingPullRuntime(DockerImageRuntime):
    def __init__(self) -> None:
        super().__init__(dry_run=True)
        self._lock = Lock()
        self.pulls: list[str] = []

    def pull(self, image: str) -> CommandResult:
        sleep(0.05)
        with self._lock:
            self.pulls.append(image)
        return CommandResult(argv=("docker", "pull", image), exit_code=0)


class FileRuntime(DockerGvisorRuntime):
    def __init__(self) -> None:
        super().__init__(dry_run=True, allow_storage_opt_quota=True)
        self.files: dict[tuple[str, str], bytes] = {}

    def write_file_to_container(
        self,
        sandbox_id: str,
        container_path: str,
        content: bytes,
        *,
        owner: str | None = None,
    ) -> CommandResult:
        result = super().write_file_to_container(
            sandbox_id,
            container_path,
            content,
            owner=owner,
        )
        self.files[(sandbox_id, container_path)] = content
        return result

    def read_file_from_container(
        self,
        sandbox_id: str,
        container_path: str,
    ) -> tuple[bytes, CommandResult]:
        _, result = super().read_file_from_container(sandbox_id, container_path)
        return self.files[(sandbox_id, container_path)], result


class ControlPlaneTests(unittest.TestCase):
    def test_gateway_server_uses_high_listen_backlog(self) -> None:
        with TemporaryDirectory() as raw_dir:
            server = build_server(
                "127.0.0.1",
                0,
                Path(raw_dir) / "heartbeats.json",
            )
            try:
                self.assertGreaterEqual(
                    server.request_queue_size,
                    DEFAULT_HTTP_REQUEST_QUEUE_SIZE,
                )
            finally:
                server.server_close()

    def test_accepts_heartbeat_and_lists_nodes(self) -> None:
        with TemporaryDirectory() as raw_dir:
            heartbeat_file = Path(raw_dir) / "heartbeats.json"
            server = build_server(
                "127.0.0.1",
                0,
                heartbeat_file,
                metrics_file=Path(raw_dir) / "metrics.jsonl",
            )
            thread = Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address
                heartbeat = build_heartbeat(
                    job_id="job-1",
                    node_id="node-1",
                    active_sandboxes=1,
                )

                result = post_heartbeat(
                    f"http://{host}:{port}/v1/nodes/heartbeat",
                    heartbeat,
                )

                self.assertEqual(result.status, 200)
                with request.urlopen(
                    f"http://{host}:{port}/v1/nodes", timeout=5
                ) as response:
                    payload = json.loads(response.read().decode("utf-8"))
                metrics = self._json_request(f"http://{host}:{port}/v1/metrics")
            finally:
                server.shutdown()
                server.server_close()

            self.assertEqual(len(payload["nodes"]), 1)
            self.assertEqual(payload["nodes"][0]["job_id"], "job-1")
            self.assertEqual(metrics["nodes"]["samples"], 1)
            self.assertEqual(
                metrics["nodes"]["recent_samples"][0]["data"]["job_id"],
                "job-1",
            )
            self.assertTrue(heartbeat_file.exists())

    def test_metrics_include_registry_summary_when_configured(self) -> None:
        class RegistryHandler(BaseHTTPRequestHandler):
            calls: list[str] = []

            def do_GET(self) -> None:
                path = self.path.split("?", 1)[0]
                type(self).calls.append(path)
                if path == "/v2/_catalog":
                    self._write_json({"repositories": ["prime/base", "prime/mini-swe"]})
                    return
                if path == "/v2/prime/base/tags/list":
                    self._write_json({"name": "prime/base", "tags": ["py311"]})
                    return
                if path == "/v2/prime/mini-swe/tags/list":
                    self._write_json(
                        {"name": "prime/mini-swe", "tags": ["mswe-2.2.8", "latest"]}
                    )
                    return
                self.send_response(404)
                self.end_headers()

            def log_message(self, format: str, *args: object) -> None:
                del format, args

            def _write_json(self, payload: dict[str, object]) -> None:
                body = json.dumps(payload).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        with TemporaryDirectory() as raw_dir:
            registry = ThreadingHTTPServer(("127.0.0.1", 0), RegistryHandler)
            registry_thread = Thread(target=registry.serve_forever, daemon=True)
            registry_thread.start()
            try:
                registry_host, registry_port = registry.server_address
                gateway = build_server(
                    "127.0.0.1",
                    0,
                    Path(raw_dir) / "heartbeats.json",
                    metrics_file=Path(raw_dir) / "metrics.jsonl",
                    registry_url=f"http://{registry_host}:{registry_port}",
                )
                gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
                gateway_thread.start()
                try:
                    host, port = gateway.server_address
                    metrics = self._json_request(f"http://{host}:{port}/v1/metrics")
                    cached_metrics = self._json_request(
                        f"http://{host}:{port}/v1/metrics"
                    )
                    calls_after_cached_metrics = list(RegistryHandler.calls)
                    refreshed_metrics = self._json_request(
                        f"http://{host}:{port}/v1/metrics?refresh_registry=true"
                    )
                    direct = self._json_request(f"http://{host}:{port}/v1/registry")
                finally:
                    gateway.shutdown()
                    gateway.server_close()
            finally:
                registry.shutdown()
                registry.server_close()

        self.assertTrue(metrics["registry"]["ok"])
        self.assertEqual(metrics["registry"]["repository_count"], 2)
        self.assertEqual(metrics["registry"]["scanned_tag_count"], 3)
        self.assertEqual(metrics["registry"]["visible_tag_count"], 3)
        self.assertEqual(
            metrics["registry"]["repositories"][1]["visible_tag_count"],
            2,
        )
        self.assertEqual(
            direct["registry"]["repositories"][1]["repository"],
            "prime/mini-swe",
        )
        self.assertFalse(metrics["registry"]["cached"])
        self.assertTrue(cached_metrics["registry"]["cached"])
        self.assertFalse(refreshed_metrics["registry"]["cached"])
        self.assertEqual(cached_metrics["registry"]["repository_count"], 2)
        self.assertEqual(calls_after_cached_metrics.count("/v2/_catalog"), 1)
        self.assertEqual(RegistryHandler.calls.count("/v2/_catalog"), 3)

    def test_gateway_hides_stale_private_registry_image_records(self) -> None:
        class MissingManifestRegistryHandler(BaseHTTPRequestHandler):
            def do_HEAD(self) -> None:
                if self.path.startswith("/v2/prime-rl/missing/manifests/latest"):
                    self.send_response(404)
                    self.end_headers()
                    return
                self.send_response(200)
                self.send_header("Docker-Content-Digest", "sha256:ok")
                self.end_headers()

            def log_message(self, format: str, *args: object) -> None:
                del format, args

        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            image_file = raw_path / "images.json"
            now = utc_now()
            ImageStore(image_file).upsert(
                ImageRecord(
                    id="missing",
                    tag="ucloud-sandbox-registry:5000/prime-rl/missing:latest",
                    source="build:/tmp/missing",
                    state="available",
                    created_at=now,
                    updated_at=now,
                    pushed=True,
                )
            )
            registry = ThreadingHTTPServer(
                ("127.0.0.1", 0),
                MissingManifestRegistryHandler,
            )
            registry_thread = Thread(target=registry.serve_forever, daemon=True)
            registry_thread.start()
            try:
                registry_host, registry_port = registry.server_address
                gateway = build_server(
                    "127.0.0.1",
                    0,
                    raw_path / "heartbeats.json",
                    routing_file=raw_path / "routes.sqlite",
                    image_file=image_file,
                    image_runtime=DockerImageRuntime(dry_run=True),
                    registry_url=f"http://{registry_host}:{registry_port}",
                )
                gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
                gateway_thread.start()
                try:
                    host, port = gateway.server_address
                    images = self._json_request(f"http://{host}:{port}/v1/images")
                finally:
                    gateway.shutdown()
                    gateway.server_close()
            finally:
                registry.shutdown()
                registry.server_close()

            self.assertEqual(images["images"], [])
            self.assertEqual(ImageStore(image_file).load(), {})

    def test_metrics_do_not_fan_out_to_node_build_endpoints(self) -> None:
        class BuildProbeHandler(BaseHTTPRequestHandler):
            called = Event()

            def do_GET(self) -> None:
                if self.path.split("?", 1)[0] == "/v1/images/builds":
                    self.called.set()
                    self._write_json({"builds": []})
                    return
                self.send_response(404)
                self.end_headers()

            def log_message(self, format: str, *args: object) -> None:
                del format, args

            def _write_json(self, payload: dict[str, object]) -> None:
                body = json.dumps(payload).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            node = ThreadingHTTPServer(("127.0.0.1", 0), BuildProbeHandler)
            node_thread = Thread(target=node.serve_forever, daemon=True)
            node_thread.start()
            try:
                node_host, node_port = node.server_address
                gateway = build_server(
                    "127.0.0.1",
                    0,
                    raw_path / "heartbeats.json",
                    routing_file=raw_path / "routes.sqlite",
                    metrics_file=raw_path / "metrics.jsonl",
                )
                gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
                gateway_thread.start()
                try:
                    host, port = gateway.server_address
                    result = post_heartbeat(
                        f"http://{host}:{port}/v1/nodes/heartbeat",
                        build_heartbeat(
                            job_id="job-builder",
                            node_id="builder-1",
                            node_url=f"http://{node_host}:{node_port}",
                            active_image_builds=1,
                            capabilities=("image-cache", "image-build", "snapshot"),
                        ),
                    )
                    self.assertEqual(result.status, 200)
                    metrics = self._json_request(f"http://{host}:{port}/v1/metrics")
                finally:
                    gateway.shutdown()
                    gateway.server_close()
            finally:
                node.shutdown()
                node.server_close()

        self.assertFalse(BuildProbeHandler.called.is_set())
        self.assertEqual(metrics["images"]["active_builds"], 1)

    def test_gateway_mode_proxies_node_agent_json_api(self) -> None:
        with TemporaryDirectory() as raw_dir:
            node_sandbox_file = Path(raw_dir) / "node-sandboxes.json"
            node_image_file = Path(raw_dir) / "node-images.json"
            node = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=node_sandbox_file,
                image_file=node_image_file,
                job_id="job-1",
                node_id="node-1",
                total_resources=ResourceQuantity(
                    vcpu=4, memory_mb=8192, disk_mb=100_000
                ),
                runtime=DockerGvisorRuntime(
                    dry_run=True,
                    allow_storage_opt_quota=True,
                ),
            )
            node_thread = Thread(target=node.serve_forever, daemon=True)
            node_thread.start()
            try:
                node_host, node_port = node.server_address
                gateway = build_server(
                    "127.0.0.1",
                    0,
                    Path(raw_dir) / "heartbeats.json",
                    upstream_node_url=f"http://{node_host}:{node_port}",
                )
                gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
                gateway_thread.start()
                try:
                    host, port = gateway.server_address
                    base = f"http://{host}:{port}"
                    created = self._json_request(
                        f"{base}/v1/sandboxes",
                        method="POST",
                        payload={
                            "id": "proxied-one",
                            "image": "busybox",
                            "disk_mb": 64,
                        },
                    )
                    listed = self._json_request(f"{base}/v1/sandboxes")
                    deleted = self._json_request(
                        f"{base}/v1/sandboxes/proxied-one",
                        method="DELETE",
                    )
                finally:
                    gateway.shutdown()
                    gateway.server_close()
            finally:
                node.shutdown()
                node.server_close()

            self.assertEqual(created["sandbox"]["spec"]["id"], "proxied-one")
            self.assertEqual(listed["sandboxes"][0]["spec"]["id"], "proxied-one")
            self.assertEqual(deleted["deleted"]["spec"]["id"], "proxied-one")

    def test_gateway_proxy_returns_json_bad_gateway_when_node_disconnects(self) -> None:
        class DisconnectingHandler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                self.connection.close()

            def log_message(self, format: str, *args: object) -> None:
                del format, args

        with TemporaryDirectory() as raw_dir:
            node = ThreadingHTTPServer(("127.0.0.1", 0), DisconnectingHandler)
            node_thread = Thread(target=node.serve_forever, daemon=True)
            node_thread.start()
            try:
                node_host, node_port = node.server_address
                gateway = build_server(
                    "127.0.0.1",
                    0,
                    Path(raw_dir) / "heartbeats.json",
                    upstream_node_url=f"http://{node_host}:{node_port}",
                )
                gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
                gateway_thread.start()
                try:
                    host, port = gateway.server_address
                    response = self._json_request(
                        f"http://{host}:{port}/v1/sandboxes",
                        method="POST",
                        payload={
                            "id": "disconnect-one",
                            "image": "busybox",
                            "disk_mb": 64,
                        },
                        allow_error=True,
                    )
                finally:
                    gateway.shutdown()
                    gateway.server_close()
            finally:
                node.shutdown()
                node.server_close()

        self.assertEqual(response["status"], 502)
        self.assertIn("node request failed", response["body"]["error"])

    def test_gateway_lists_sandboxes_from_cache_unless_refresh_requested(
        self,
    ) -> None:
        class ListingNode(BaseHTTPRequestHandler):
            listed = Event()

            def do_GET(self) -> None:
                if self.path.split("?", 1)[0] == "/v1/sandboxes":
                    self.listed.set()
                    self._write_json(
                        {
                            "sandboxes": [
                                {
                                    "spec": {
                                        "id": "cached-one",
                                        "image": "busybox",
                                        "labels": {"run": "r1"},
                                        "memory_mb": 512,
                                        "disk_mb": 1024,
                                    },
                                    "state": "running",
                                }
                            ]
                        }
                    )
                    return
                self.send_response(404)
                self.end_headers()

            def log_message(self, format: str, *args: object) -> None:
                del format, args

            def _write_json(self, payload: dict[str, object]) -> None:
                body = json.dumps(payload).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            route_file = raw_path / "routes.sqlite"
            node = ThreadingHTTPServer(("127.0.0.1", 0), ListingNode)
            node_thread = Thread(target=node.serve_forever, daemon=True)
            node_thread.start()
            try:
                node_host, node_port = node.server_address
                node_url = f"http://{node_host}:{node_port}"
                RoutingStore(route_file).upsert_sandbox(
                    SandboxRoute(
                        sandbox_id="cached-one",
                        node_id="node-1",
                        job_id="job-1",
                        node_url=node_url,
                        resources=ResourceQuantity(
                            vcpu=1.0,
                            memory_mb=512,
                            disk_mb=1024,
                        ),
                        spec={
                            "id": "cached-one",
                            "image": "busybox",
                            "labels": {"run": "r1"},
                            "memory_mb": 512,
                            "disk_mb": 1024,
                        },
                        state="running",
                    )
                )
                gateway = build_server(
                    "127.0.0.1",
                    0,
                    raw_path / "heartbeats.json",
                    routing_file=route_file,
                )
                gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
                gateway_thread.start()
                try:
                    host, port = gateway.server_address
                    base = f"http://{host}:{port}"
                    result = post_heartbeat(
                        f"{base}/v1/nodes/heartbeat",
                        build_heartbeat(
                            job_id="job-1",
                            node_id="node-1",
                            node_url=node_url,
                            active_sandboxes=1,
                            capabilities=("sandbox", "image-cache"),
                            total_resources=ResourceQuantity(
                                vcpu=4,
                                memory_mb=4096,
                                disk_mb=8192,
                            ),
                        ),
                    )
                    self.assertEqual(result.status, 200)
                    cached = self._json_request(f"{base}/v1/sandboxes")
                    self.assertFalse(ListingNode.listed.is_set())
                    refreshed = self._json_request(f"{base}/v1/sandboxes?refresh=true")
                finally:
                    gateway.shutdown()
                    gateway.server_close()
            finally:
                node.shutdown()
                node.server_close()

        self.assertTrue(cached["cached"])
        self.assertEqual(cached["sandboxes"][0]["id"], "cached-one")
        self.assertEqual(cached["sandboxes"][0]["state"], "running")
        self.assertEqual(cached["sandboxes"][0]["image"], "busybox")
        self.assertEqual(cached["sandboxes"][0]["labels"], {"run": "r1"})
        self.assertFalse(cached["sandboxes"][0]["route_only"])
        self.assertTrue(ListingNode.listed.is_set())
        self.assertFalse(refreshed["cached"])
        self.assertEqual(refreshed["sandboxes"][0]["spec"]["id"], "cached-one")

    def test_gateway_marks_cached_route_unknown_when_node_reports_empty(
        self,
    ) -> None:
        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            route_file = raw_path / "routes.sqlite"
            node_url = "http://127.0.0.1:9"
            RoutingStore(route_file).upsert_sandbox(
                SandboxRoute(
                    sandbox_id="stale-one",
                    node_id="node-1",
                    job_id="job-1",
                    node_url=node_url,
                    resources=ResourceQuantity(
                        vcpu=1.0,
                        memory_mb=512,
                        disk_mb=1024,
                    ),
                    spec={
                        "id": "stale-one",
                        "image": "busybox",
                        "memory_mb": 512,
                        "disk_mb": 1024,
                    },
                    state="running",
                )
            )
            gateway = build_server(
                "127.0.0.1",
                0,
                raw_path / "heartbeats.json",
                routing_file=route_file,
            )
            gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
            gateway_thread.start()
            try:
                host, port = gateway.server_address
                base = f"http://{host}:{port}"
                result = post_heartbeat(
                    f"{base}/v1/nodes/heartbeat",
                    build_heartbeat(
                        job_id="job-1",
                        node_id="node-1",
                        node_url=node_url,
                        active_sandboxes=0,
                        capabilities=("sandbox",),
                    ),
                )
                self.assertEqual(result.status, 200)
                cached = self._json_request(f"{base}/v1/sandboxes")
            finally:
                gateway.shutdown()
                gateway.server_close()

        record = cached["sandboxes"][0]
        self.assertTrue(cached["cached"])
        self.assertEqual(record["id"], "stale-one")
        self.assertEqual(record["cached_state"], "running")
        self.assertEqual(record["state"], "unknown")
        self.assertTrue(record["route_only"])
        self.assertEqual(record["node"]["active_sandboxes"], 0)

    def test_gateway_does_not_proxy_exec_to_proven_stale_route(self) -> None:
        class ListingNode(BaseHTTPRequestHandler):
            exec_called = Event()

            def do_GET(self) -> None:
                if self.path.split("?", 1)[0] == "/v1/sandboxes":
                    self._write_json({"sandboxes": []})
                    return
                self.send_response(404)
                self.end_headers()

            def do_POST(self) -> None:
                if self.path.split("?", 1)[0].endswith("/exec"):
                    self.exec_called.set()
                self.send_response(500)
                self.end_headers()

            def log_message(self, format: str, *args: object) -> None:
                del format, args

            def _write_json(self, payload: dict[str, object]) -> None:
                body = json.dumps(payload).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            route_file = raw_path / "routes.sqlite"
            node = ThreadingHTTPServer(("127.0.0.1", 0), ListingNode)
            node_thread = Thread(target=node.serve_forever, daemon=True)
            node_thread.start()
            try:
                node_host, node_port = node.server_address
                node_url = f"http://{node_host}:{node_port}"
                RoutingStore(route_file).upsert_sandbox(
                    SandboxRoute(
                        sandbox_id="stale-one",
                        node_id="node-1",
                        job_id="job-1",
                        node_url=node_url,
                        resources=ResourceQuantity(
                            vcpu=1.0,
                            memory_mb=512,
                            disk_mb=1024,
                        ),
                        spec={"id": "stale-one", "image": "busybox"},
                        state="running",
                    )
                )
                gateway = build_server(
                    "127.0.0.1",
                    0,
                    raw_path / "heartbeats.json",
                    routing_file=route_file,
                )
                gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
                gateway_thread.start()
                try:
                    host, port = gateway.server_address
                    base = f"http://{host}:{port}"
                    result = post_heartbeat(
                        f"{base}/v1/nodes/heartbeat",
                        build_heartbeat(
                            job_id="job-1",
                            node_id="node-1",
                            node_url=node_url,
                            active_sandboxes=0,
                            capabilities=("sandbox",),
                        ),
                    )
                    self.assertEqual(result.status, 200)
                    response = self._json_request(
                        f"{base}/v1/sandboxes/stale-one/exec",
                        method="POST",
                        payload={"cmd": "true"},
                        allow_error=True,
                    )
                finally:
                    gateway.shutdown()
                    gateway.server_close()
            finally:
                node.shutdown()
                node.server_close()

        self.assertEqual(response["status"], 404)
        self.assertEqual(response["body"]["error"], "sandbox route not found")
        self.assertFalse(ListingNode.exec_called.is_set())

    def test_gateway_returns_json_when_routing_store_unavailable_for_metrics(
        self,
    ) -> None:
        class BrokenRoutingStore:
            def load(self) -> object:
                raise sqlite3.DatabaseError("database disk image is malformed")

        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            gateway = build_server(
                "127.0.0.1",
                0,
                raw_path / "heartbeats.json",
                routing_file=raw_path / "routes.sqlite",
            )
            gateway.RequestHandlerClass.routing_store = BrokenRoutingStore()
            gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
            gateway_thread.start()
            try:
                host, port = gateway.server_address
                response = self._json_request(
                    f"http://{host}:{port}/v1/metrics",
                    allow_error=True,
                )
            finally:
                gateway.shutdown()
                gateway.server_close()

        self.assertEqual(response["status"], 503)
        self.assertEqual(response["body"]["error"], "routing state unavailable")
        self.assertTrue(response["body"]["retryable"])
        self.assertIn("malformed", response["body"]["details"])

    def test_gateway_bearer_token_protects_proxied_api(self) -> None:
        with TemporaryDirectory() as raw_dir:
            node = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=Path(raw_dir) / "node-sandboxes.json",
                image_file=Path(raw_dir) / "node-images.json",
                job_id="job-1",
                node_id="node-1",
                total_resources=ResourceQuantity(
                    vcpu=4, memory_mb=8192, disk_mb=100_000
                ),
                runtime=DockerGvisorRuntime(
                    dry_run=True,
                    allow_storage_opt_quota=True,
                ),
            )
            node_thread = Thread(target=node.serve_forever, daemon=True)
            node_thread.start()
            try:
                node_host, node_port = node.server_address
                gateway = build_server(
                    "127.0.0.1",
                    0,
                    Path(raw_dir) / "heartbeats.json",
                    upstream_node_url=f"http://{node_host}:{node_port}",
                    gateway_bearer_token="secret-token",
                )
                gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
                gateway_thread.start()
                try:
                    host, port = gateway.server_address
                    base = f"http://{host}:{port}"
                    healthz = self._json_request(f"{base}/healthz")
                    unauthorized = self._json_request(
                        f"{base}/v1/sandboxes",
                        allow_error=True,
                    )
                    authorized = self._json_request(
                        f"{base}/v1/sandboxes",
                        headers={"Authorization": "Bearer secret-token"},
                    )
                    header_authorized = self._json_request(
                        f"{base}/v1/sandboxes",
                        headers={"X-UCloud-Sandbox-Token": "secret-token"},
                    )
                finally:
                    gateway.shutdown()
                    gateway.server_close()
            finally:
                node.shutdown()
                node.server_close()

            self.assertEqual(
                healthz,
                {
                    "ok": True,
                    "service": "control-plane",
                    "version": package_version(),
                },
            )
            self.assertEqual(unauthorized["status"], 401)
            self.assertEqual(unauthorized["body"], {"error": "unauthorized"})
            self.assertEqual(authorized, {"sandboxes": []})
            self.assertEqual(header_authorized, {"sandboxes": []})

    def test_dashboard_assets_are_public_but_metrics_remain_protected(self) -> None:
        with TemporaryDirectory() as raw_dir:
            gateway = build_server(
                "127.0.0.1",
                0,
                Path(raw_dir) / "heartbeats.json",
                routing_file=Path(raw_dir) / "routes.sqlite",
                gateway_bearer_token="secret-token",
                metrics_file=Path(raw_dir) / "metrics.jsonl",
            )
            gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
            gateway_thread.start()
            try:
                host, port = gateway.server_address
                base = f"http://{host}:{port}"
                with request.urlopen(f"{base}/dashboard", timeout=5) as response:
                    html = response.read().decode("utf-8")
                    html_type = response.headers.get("Content-Type")
                with request.urlopen(
                    f"{base}/dashboard/dashboard.css",
                    timeout=5,
                ) as response:
                    css = response.read().decode("utf-8")
                    css_type = response.headers.get("Content-Type")
                with request.urlopen(
                    f"{base}/dashboard/dashboard.js",
                    timeout=5,
                ) as response:
                    js = response.read().decode("utf-8")
                    js_type = response.headers.get("Content-Type")
                unauthorized_metrics = self._json_request(
                    f"{base}/v1/metrics",
                    allow_error=True,
                )
                authorized_metrics = self._json_request(
                    f"{base}/v1/metrics",
                    headers={"Authorization": "Bearer secret-token"},
                )
            finally:
                gateway.shutdown()
                gateway.server_close()

        self.assertIn("text/html", html_type or "")
        self.assertIn("UCloud Sandboxes", html)
        self.assertIn("/dashboard/dashboard.js", html)
        self.assertIn('data-page-target="sandboxes"', html)
        self.assertIn('data-page-target="registry"', html)
        self.assertIn('id="terminateAllSandboxesButton"', html)
        self.assertIn('<span class="control-value">5s</span>', html)
        self.assertNotIn("refreshSelect", html)
        self.assertNotIn("secret-token", html)
        self.assertIn("text/css", css_type or "")
        self.assertIn(".metric-grid", css)
        self.assertIn(".sandbox-table", css)
        self.assertIn(".registry-full-grid", css)
        self.assertIn("application/javascript", js_type or "")
        self.assertIn("/v1/metrics", js)
        self.assertIn("/v1/sandboxes?refresh=true", js)
        self.assertIn("terminateAllSandboxes", js)
        self.assertIn("X-UCloud-Sandbox-Token", js)
        self.assertIn("renderRegistryPage", js)
        self.assertIn("const REFRESH_INTERVAL_MS = 5000;", js)
        self.assertNotIn("refreshSelect", js)
        self.assertNotIn("secret-token", js)
        self.assertEqual(unauthorized_metrics["status"], 401)
        self.assertEqual(authorized_metrics["nodes"]["total"], 0)

    def test_multi_node_gateway_places_and_routes_by_resource_fit(self) -> None:
        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            node1 = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=raw_path / "node1-sandboxes.json",
                image_file=raw_path / "node1-images.json",
                job_id="job-1",
                node_id="node-1",
                total_resources=ResourceQuantity(vcpu=2, memory_mb=1024, disk_mb=32),
                runtime=DockerGvisorRuntime(dry_run=True, allow_storage_opt_quota=True),
            )
            node2 = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=raw_path / "node2-sandboxes.json",
                image_file=raw_path / "node2-images.json",
                job_id="job-2",
                node_id="node-2",
                total_resources=ResourceQuantity(vcpu=2, memory_mb=1024, disk_mb=128),
                runtime=DockerGvisorRuntime(dry_run=True, allow_storage_opt_quota=True),
            )
            node1_thread = Thread(target=node1.serve_forever, daemon=True)
            node2_thread = Thread(target=node2.serve_forever, daemon=True)
            node1_thread.start()
            node2_thread.start()
            try:
                node1_host, node1_port = node1.server_address
                node2_host, node2_port = node2.server_address
                gateway = build_server(
                    "127.0.0.1",
                    0,
                    raw_path / "heartbeats.json",
                    routing_file=raw_path / "routes.json",
                    image_file=raw_path / "gateway-images.json",
                    local_image_builds_enabled=False,
                    metrics_file=raw_path / "metrics.jsonl",
                )
                gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
                gateway_thread.start()
                try:
                    host, port = gateway.server_address
                    base = f"http://{host}:{port}"
                    for heartbeat in (
                        build_heartbeat(
                            job_id="job-1",
                            node_id="node-1",
                            active_sandboxes=100,
                            node_url=f"http://{node1_host}:{node1_port}",
                            capabilities=("sandbox", "image-cache"),
                            total_resources=ResourceQuantity(
                                vcpu=2,
                                memory_mb=1024,
                                disk_mb=32,
                            ),
                        ),
                        build_heartbeat(
                            job_id="job-2",
                            node_id="node-2",
                            active_sandboxes=0,
                            node_url=f"http://{node2_host}:{node2_port}",
                            capabilities=("sandbox", "image-cache"),
                            total_resources=ResourceQuantity(
                                vcpu=2,
                                memory_mb=1024,
                                disk_mb=128,
                            ),
                        ),
                    ):
                        result = post_heartbeat(
                            f"{base}/v1/nodes/heartbeat",
                            heartbeat,
                        )
                        self.assertEqual(result.status, 200)

                    created = self._json_request(
                        f"{base}/v1/sandboxes",
                        method="POST",
                        payload={
                            "id": "multi-one",
                            "image": "busybox",
                            "memory_mb": 128,
                            "disk_mb": 64,
                        },
                    )
                    second = self._json_request(
                        f"{base}/v1/sandboxes",
                        method="POST",
                        payload={
                            "id": "multi-two",
                            "image": "busybox",
                            "memory_mb": 128,
                            "disk_mb": 64,
                        },
                    )
                    rejected = self._json_request(
                        f"{base}/v1/sandboxes",
                        method="POST",
                        payload={
                            "id": "multi-three",
                            "image": "busybox",
                            "memory_mb": 128,
                            "disk_mb": 64,
                        },
                        allow_error=True,
                    )
                    listed = self._json_request(f"{base}/v1/sandboxes?refresh=true")
                    exec_started = self._json_request(
                        f"{base}/v1/sandboxes/multi-one/exec",
                        method="POST",
                        payload={"command": ["true"]},
                    )
                    session_id = exec_started["session"]["id"]
                    exec_read = self._json_request(f"{base}/v1/exec/{session_id}")
                    deleted = self._json_request(
                        f"{base}/v1/sandboxes/multi-one",
                        method="DELETE",
                    )
                    second_deleted = self._json_request(
                        f"{base}/v1/sandboxes/multi-two",
                        method="DELETE",
                    )
                    metrics = self._json_request(f"{base}/v1/metrics")
                    with request.urlopen(
                        f"http://{node1_host}:{node1_port}/v1/sandboxes",
                        timeout=5,
                    ) as response:
                        node1_payload = json.loads(response.read().decode("utf-8"))
                    with request.urlopen(
                        f"http://{node2_host}:{node2_port}/v1/sandboxes",
                        timeout=5,
                    ) as response:
                        node2_payload = json.loads(response.read().decode("utf-8"))
                finally:
                    gateway.shutdown()
                    gateway.server_close()
            finally:
                node1.shutdown()
                node1.server_close()
                node2.shutdown()
                node2.server_close()

            route = RoutingStore(raw_path / "routes.json").get_sandbox("multi-one")
            self.assertIsNone(route)
            self.assertEqual(created["sandbox"]["spec"]["id"], "multi-one")
            self.assertEqual(second["sandbox"]["spec"]["id"], "multi-two")
            self.assertEqual(rejected["status"], 503)
            self.assertEqual(
                {record["spec"]["id"] for record in listed["sandboxes"]},
                {"multi-one", "multi-two"},
            )
            self.assertEqual(
                {record["node"]["job_id"] for record in listed["sandboxes"]},
                {"job-2"},
            )
            self.assertEqual(exec_read["session"]["id"], session_id)
            self.assertEqual(deleted["deleted"]["spec"]["id"], "multi-one")
            self.assertEqual(second_deleted["deleted"]["spec"]["id"], "multi-two")
            self.assertGreaterEqual(metrics["traces"]["span_count"], 3)
            self.assertTrue(
                any(
                    item["name"] == "gateway.sandbox_create"
                    for item in metrics["traces"]["recent"]
                )
            )
            self.assertEqual(node1_payload, {"sandboxes": []})
            self.assertEqual(node2_payload, {"sandboxes": []})

    def test_concurrent_create_reserves_in_flight_node_capacity(self) -> None:
        class SlowCreateRuntime(DockerGvisorRuntime):
            def create(self, spec):
                sleep(0.2)
                return super().create(spec)

        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            node1 = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=raw_path / "burst-node1-sandboxes.json",
                image_file=raw_path / "burst-node1-images.json",
                job_id="job-1",
                node_id="node-1",
                total_resources=ResourceQuantity(vcpu=1, memory_mb=1024, disk_mb=128),
                runtime=SlowCreateRuntime(
                    dry_run=True,
                    allow_storage_opt_quota=True,
                ),
            )
            node2 = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=raw_path / "burst-node2-sandboxes.json",
                image_file=raw_path / "burst-node2-images.json",
                job_id="job-2",
                node_id="node-2",
                total_resources=ResourceQuantity(vcpu=1, memory_mb=1024, disk_mb=128),
                runtime=SlowCreateRuntime(
                    dry_run=True,
                    allow_storage_opt_quota=True,
                ),
            )
            node1_thread = Thread(target=node1.serve_forever, daemon=True)
            node2_thread = Thread(target=node2.serve_forever, daemon=True)
            node1_thread.start()
            node2_thread.start()
            try:
                node1_host, node1_port = node1.server_address
                node2_host, node2_port = node2.server_address
                gateway = build_server(
                    "127.0.0.1",
                    0,
                    raw_path / "heartbeats.json",
                    routing_file=raw_path / "routes.sqlite",
                    image_file=raw_path / "gateway-images.json",
                    local_image_builds_enabled=False,
                    metrics_file=raw_path / "metrics.jsonl",
                )
                gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
                gateway_thread.start()
                try:
                    host, port = gateway.server_address
                    base = f"http://{host}:{port}"
                    for heartbeat in (
                        build_heartbeat(
                            job_id="job-1",
                            node_id="node-1",
                            active_sandboxes=0,
                            node_url=f"http://{node1_host}:{node1_port}",
                            capabilities=("sandbox", "image-cache"),
                            total_resources=ResourceQuantity(
                                vcpu=1,
                                memory_mb=1024,
                                disk_mb=128,
                            ),
                        ),
                        build_heartbeat(
                            job_id="job-2",
                            node_id="node-2",
                            active_sandboxes=0,
                            node_url=f"http://{node2_host}:{node2_port}",
                            capabilities=("sandbox", "image-cache"),
                            total_resources=ResourceQuantity(
                                vcpu=1,
                                memory_mb=1024,
                                disk_mb=128,
                            ),
                        ),
                    ):
                        result = post_heartbeat(
                            f"{base}/v1/nodes/heartbeat",
                            heartbeat,
                        )
                        self.assertEqual(result.status, 200)

                    def create(index: int) -> dict:
                        return self._json_request(
                            f"{base}/v1/sandboxes",
                            method="POST",
                            payload={
                                "id": f"burst-{index}",
                                "image": "busybox",
                                "cpus": 1,
                                "memory_mb": 128,
                                "disk_mb": 64,
                            },
                        )

                    with ThreadPoolExecutor(max_workers=2) as executor:
                        created = list(executor.map(create, (1, 2)))
                finally:
                    gateway.shutdown()
                    gateway.server_close()
            finally:
                node1.shutdown()
                node1.server_close()
                node2.shutdown()
                node2.server_close()

            state = RoutingStore(raw_path / "routes.sqlite").load()

        self.assertEqual(
            {item["sandbox"]["spec"]["id"] for item in created},
            {"burst-1", "burst-2"},
        )
        self.assertEqual(
            {state.sandboxes["burst-1"].job_id, state.sandboxes["burst-2"].job_id},
            {"job-1", "job-2"},
        )

    def test_retried_create_with_unresolved_reservation_returns_in_progress(
        self,
    ) -> None:
        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            route_file = raw_path / "routes.sqlite"
            node = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=raw_path / "node-sandboxes.json",
                image_file=raw_path / "node-images.json",
                job_id="job-1",
                node_id="node-1",
                total_resources=ResourceQuantity(vcpu=1, memory_mb=1024, disk_mb=2048),
                runtime=DockerGvisorRuntime(dry_run=True, allow_storage_opt_quota=True),
            )
            node_thread = Thread(target=node.serve_forever, daemon=True)
            node_thread.start()
            try:
                node_host, node_port = node.server_address
                node_base = f"http://{node_host}:{node_port}"
                gateway = build_server(
                    "127.0.0.1",
                    0,
                    raw_path / "heartbeats.json",
                    routing_file=route_file,
                    image_file=raw_path / "gateway-images.json",
                    local_image_builds_enabled=False,
                )
                gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
                gateway_thread.start()
                try:
                    host, port = gateway.server_address
                    base = f"http://{host}:{port}"
                    result = post_heartbeat(
                        f"{base}/v1/nodes/heartbeat",
                        build_heartbeat(
                            job_id="job-1",
                            node_id="node-1",
                            node_url=node_base,
                            capabilities=("sandbox", "image-cache"),
                            total_resources=ResourceQuantity(
                                vcpu=1,
                                memory_mb=1024,
                                disk_mb=2048,
                            ),
                        ),
                    )
                    self.assertEqual(result.status, 200)
                    route = SandboxRoute(
                        sandbox_id="retry-one",
                        node_id="node-1",
                        job_id="job-1",
                        node_url=node_base,
                        resources=ResourceQuantity(vcpu=1, memory_mb=512, disk_mb=512),
                        created_at=utc_now().isoformat(),
                        updated_at=utc_now().isoformat(),
                    )
                    with control_plane._GATEWAY_SCHEDULING_LOCK:
                        control_plane._GATEWAY_IN_FLIGHT_ROUTES["retry-one"] = (
                            control_plane._InFlightRoute(
                                route=route,
                                expires_at=utc_now() + timedelta(seconds=60),
                            )
                        )
                    retry = self._json_request(
                        f"{base}/v1/sandboxes",
                        method="POST",
                        payload={
                            "id": "retry-one",
                            "image": "busybox",
                            "cpus": 1,
                            "memory_mb": 512,
                            "disk_mb": 512,
                        },
                        allow_error=True,
                    )
                    node_sandboxes = self._json_request(f"{node_base}/v1/sandboxes")
                    demand = RoutingStore(route_file).pending_demand()
                finally:
                    control_plane._release_in_flight_route("retry-one")
                    gateway.shutdown()
                    gateway.server_close()
            finally:
                node.shutdown()
                node.server_close()

        self.assertEqual(retry["status"], 503)
        self.assertTrue(retry["body"]["retryable"])
        self.assertEqual(
            retry["body"]["error"],
            "sandbox creation is already in progress",
        )
        self.assertEqual(node_sandboxes["sandboxes"], [])
        self.assertEqual(demand.pending_resources, ResourceQuantity())

    def test_node_capacity_counts_routes_even_after_newer_heartbeat(self) -> None:
        now = utc_now()
        old = (now - timedelta(seconds=5)).isoformat()
        heartbeat = NodeHeartbeat(
            node_id="node-1",
            job_id="job-1",
            updated_at=now,
            active_sandboxes=0,
            node_url="http://node-1:8090",
            total_resources=ResourceQuantity(vcpu=2, memory_mb=2048, disk_mb=4096),
            used_resources=ResourceQuantity(),
        )
        routes = [
            SandboxRoute(
                sandbox_id="already-reserved",
                node_id="node-1",
                job_id="job-1",
                node_url="http://node-1:8090",
                resources=ResourceQuantity(vcpu=2, memory_mb=1024, disk_mb=1024),
                created_at=old,
                updated_at=old,
            )
        ]

        self.assertFalse(
            control_plane._node_can_fit(
                heartbeat,
                ResourceQuantity(vcpu=1, memory_mb=512, disk_mb=512),
                routes,
            )
        )

    def test_gateway_create_backpressure_fails_fast(self) -> None:
        class BlockingCreateRuntime(DockerGvisorRuntime):
            def __init__(self) -> None:
                super().__init__(dry_run=True, allow_storage_opt_quota=True)
                self.started = Event()
                self.release = Event()

            def create(self, spec):
                self.started.set()
                self.release.wait(timeout=5)
                return super().create(spec)

        runtime = BlockingCreateRuntime()
        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            node = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=raw_path / "limited-node-sandboxes.json",
                image_file=raw_path / "limited-node-images.json",
                job_id="job-1",
                node_id="node-1",
                total_resources=ResourceQuantity(vcpu=4, memory_mb=4096, disk_mb=128),
                runtime=runtime,
            )
            node_thread = Thread(target=node.serve_forever, daemon=True)
            node_thread.start()
            try:
                node_host, node_port = node.server_address
                gateway = build_server(
                    "127.0.0.1",
                    0,
                    raw_path / "heartbeats.json",
                    routing_file=raw_path / "routes.sqlite",
                    image_file=raw_path / "gateway-images.json",
                    local_image_builds_enabled=False,
                    metrics_file=raw_path / "metrics.jsonl",
                    max_concurrent_sandbox_creates=1,
                )
                gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
                gateway_thread.start()
                try:
                    host, port = gateway.server_address
                    base = f"http://{host}:{port}"
                    result = post_heartbeat(
                        f"{base}/v1/nodes/heartbeat",
                        build_heartbeat(
                            job_id="job-1",
                            node_id="node-1",
                            active_sandboxes=0,
                            node_url=f"http://{node_host}:{node_port}",
                            capabilities=("sandbox", "image-cache"),
                            total_resources=ResourceQuantity(
                                vcpu=4,
                                memory_mb=4096,
                                disk_mb=128,
                            ),
                        ),
                    )
                    self.assertEqual(result.status, 200)

                    def create_one() -> dict:
                        return self._json_request(
                            f"{base}/v1/sandboxes",
                            method="POST",
                            payload={
                                "id": "limited-one",
                                "image": "busybox",
                                "cpus": 1,
                                "memory_mb": 128,
                                "disk_mb": 64,
                            },
                        )

                    with ThreadPoolExecutor(max_workers=1) as executor:
                        future = executor.submit(create_one)
                        self.assertTrue(runtime.started.wait(timeout=5))
                        same_payload = json.dumps(
                            {
                                "id": "limited-one",
                                "image": "busybox",
                                "cpus": 1,
                                "memory_mb": 128,
                                "disk_mb": 64,
                            }
                        ).encode("utf-8")
                        same_req = request.Request(
                            f"{base}/v1/sandboxes",
                            data=same_payload,
                            method="POST",
                            headers={"Content-Type": "application/json"},
                        )
                        try:
                            with request.urlopen(same_req, timeout=5):
                                self.fail("expected duplicate create to fail")
                        except error.HTTPError as exc:
                            same_status = exc.code
                            same_retry_after = exc.headers.get("Retry-After")
                            same = json.loads(exc.read().decode("utf-8"))
                        busy_payload = json.dumps(
                            {
                                "id": "limited-two",
                                "image": "busybox",
                                "cpus": 1,
                                "memory_mb": 128,
                                "disk_mb": 64,
                            }
                        ).encode("utf-8")
                        req = request.Request(
                            f"{base}/v1/sandboxes",
                            data=busy_payload,
                            method="POST",
                            headers={"Content-Type": "application/json"},
                        )
                        try:
                            with request.urlopen(req, timeout=5):
                                self.fail("expected busy create to fail")
                        except error.HTTPError as exc:
                            busy_status = exc.code
                            retry_after = exc.headers.get("Retry-After")
                            busy = json.loads(exc.read().decode("utf-8"))
                        finally:
                            runtime.release.set()
                        created = future.result(timeout=5)
                    metrics = self._json_request(f"{base}/v1/metrics")
                finally:
                    runtime.release.set()
                    gateway.shutdown()
                    gateway.server_close()
            finally:
                runtime.release.set()
                node.shutdown()
                node.server_close()

        self.assertEqual(created["sandbox"]["spec"]["id"], "limited-one")
        self.assertEqual(same_status, 503)
        self.assertEqual(same_retry_after, "5")
        self.assertTrue(same["retryable"])
        self.assertEqual(same["sandbox_id"], "limited-one")
        self.assertEqual(same["error"], "sandbox creation is already in progress")
        self.assertEqual(busy_status, 503)
        self.assertEqual(retry_after, "2")
        self.assertTrue(busy["retryable"])
        self.assertEqual(busy["max_concurrent_sandbox_creates"], 1)
        with control_plane._GATEWAY_SANDBOX_CREATE_LOCKS_GUARD:
            self.assertNotIn("limited-one", control_plane._GATEWAY_SANDBOX_CREATE_LOCKS)
            self.assertNotIn("limited-two", control_plane._GATEWAY_SANDBOX_CREATE_LOCKS)
        self.assertTrue(
            any(
                item["status"] == "error"
                and item["spans"][0]["attributes"].get("outcome") == "gateway_busy"
                for item in metrics["traces"]["recent"]
            )
        )

    def test_gateway_persists_route_before_node_create_finishes(self) -> None:
        class SlowCreateNode(BaseHTTPRequestHandler):
            started = Event()
            release = Event()

            def do_POST(self) -> None:
                if self.path != "/v1/sandboxes":
                    self.send_response(404)
                    self.end_headers()
                    return
                length = int(self.headers.get("Content-Length", "0"))
                raw = json.loads(self.rfile.read(length).decode("utf-8"))
                self.started.set()
                self.release.wait(timeout=5)
                self._write_json(
                    {
                        "sandbox": {"spec": raw},
                        "command": ["docker", "run"],
                        "exitCode": 0,
                    },
                    status=201,
                )

            def log_message(self, format: str, *args: object) -> None:
                del format, args

            def _write_json(
                self, payload: dict[str, object], *, status: int = 200
            ) -> None:
                body = json.dumps(payload).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            route_file = raw_path / "routes.sqlite"
            node = ThreadingHTTPServer(("127.0.0.1", 0), SlowCreateNode)
            node_thread = Thread(target=node.serve_forever, daemon=True)
            node_thread.start()
            try:
                node_host, node_port = node.server_address
                gateway = build_server(
                    "127.0.0.1",
                    0,
                    raw_path / "heartbeats.json",
                    routing_file=route_file,
                    metrics_file=raw_path / "metrics.jsonl",
                )
                gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
                gateway_thread.start()
                try:
                    host, port = gateway.server_address
                    base = f"http://{host}:{port}"
                    result = post_heartbeat(
                        f"{base}/v1/nodes/heartbeat",
                        build_heartbeat(
                            job_id="job-1",
                            node_id="node-1",
                            node_url=f"http://{node_host}:{node_port}",
                            capabilities=("sandbox", "image-cache"),
                            cached_images=("busybox",),
                            total_resources=ResourceQuantity(
                                vcpu=4,
                                memory_mb=4096,
                                disk_mb=8192,
                            ),
                        ),
                    )
                    self.assertEqual(result.status, 200)

                    with ThreadPoolExecutor(max_workers=1) as executor:
                        future = executor.submit(
                            self._json_request,
                            f"{base}/v1/sandboxes",
                            method="POST",
                            payload={
                                "id": "slow-one",
                                "image": "busybox",
                                "cpus": 1,
                                "memory_mb": 512,
                                "disk_mb": 1024,
                            },
                        )
                        self.assertTrue(SlowCreateNode.started.wait(timeout=5))
                        route = RoutingStore(route_file).get_sandbox("slow-one")
                        SlowCreateNode.release.set()
                        created = future.result(timeout=5)
                finally:
                    SlowCreateNode.release.set()
                    gateway.shutdown()
                    gateway.server_close()
            finally:
                SlowCreateNode.release.set()
                node.shutdown()
                node.server_close()

        self.assertIsNotNone(route)
        self.assertEqual(created["sandbox"]["spec"]["id"], "slow-one")

    def test_gateway_keeps_recent_unresolved_route_without_retrying_create(
        self,
    ) -> None:
        class EmptyNode(BaseHTTPRequestHandler):
            post_count = 0

            def do_GET(self) -> None:
                if self.path == "/v1/sandboxes":
                    self._write_json({"sandboxes": []})
                    return
                self.send_response(404)
                self.end_headers()

            def do_POST(self) -> None:
                type(self).post_count += 1
                self._write_json({"error": "unexpected create"}, status=500)

            def log_message(self, format: str, *args: object) -> None:
                del format, args

            def _write_json(
                self, payload: dict[str, object], *, status: int = 200
            ) -> None:
                body = json.dumps(payload).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            route_file = raw_path / "routes.sqlite"
            node = ThreadingHTTPServer(("127.0.0.1", 0), EmptyNode)
            node_thread = Thread(target=node.serve_forever, daemon=True)
            node_thread.start()
            try:
                node_host, node_port = node.server_address
                node_url = f"http://{node_host}:{node_port}"
                RoutingStore(route_file).upsert_sandbox(
                    SandboxRoute(
                        sandbox_id="recovering-one",
                        node_id="node-1",
                        job_id="job-1",
                        node_url=node_url,
                        resources=ResourceQuantity(
                            vcpu=1,
                            memory_mb=512,
                            disk_mb=1024,
                        ),
                        created_at=utc_now().isoformat(),
                        updated_at=utc_now().isoformat(),
                    )
                )
                gateway = build_server(
                    "127.0.0.1",
                    0,
                    raw_path / "heartbeats.json",
                    routing_file=route_file,
                    metrics_file=raw_path / "metrics.jsonl",
                )
                gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
                gateway_thread.start()
                try:
                    host, port = gateway.server_address
                    base = f"http://{host}:{port}"
                    result = post_heartbeat(
                        f"{base}/v1/nodes/heartbeat",
                        build_heartbeat(
                            job_id="job-1",
                            node_id="node-1",
                            node_url=node_url,
                            capabilities=("sandbox", "image-cache"),
                            cached_images=("busybox",),
                        ),
                    )
                    self.assertEqual(result.status, 200)
                    retry = self._json_request(
                        f"{base}/v1/sandboxes",
                        method="POST",
                        payload={
                            "id": "recovering-one",
                            "image": "busybox",
                            "cpus": 1,
                            "memory_mb": 512,
                            "disk_mb": 1024,
                        },
                        allow_error=True,
                    )
                    route = RoutingStore(route_file).get_sandbox("recovering-one")
                finally:
                    gateway.shutdown()
                    gateway.server_close()
            finally:
                node.shutdown()
                node.server_close()

        self.assertEqual(retry["status"], 503)
        self.assertTrue(retry["body"]["retryable"])
        self.assertEqual(EmptyNode.post_count, 0)
        self.assertIsNotNone(route)

    def test_gateway_routes_sandbox_file_upload_and_download(self) -> None:
        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            node = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=raw_path / "node-sandboxes.json",
                image_file=raw_path / "node-images.json",
                job_id="job-1",
                node_id="node-1",
                total_resources=ResourceQuantity(vcpu=2, memory_mb=1024, disk_mb=128),
                runtime=FileRuntime(),
            )
            node_thread = Thread(target=node.serve_forever, daemon=True)
            node_thread.start()
            try:
                node_host, node_port = node.server_address
                gateway = build_server(
                    "127.0.0.1",
                    0,
                    raw_path / "heartbeats.json",
                    routing_file=raw_path / "routes.sqlite",
                )
                gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
                gateway_thread.start()
                try:
                    host, port = gateway.server_address
                    base = f"http://{host}:{port}"
                    result = post_heartbeat(
                        f"{base}/v1/nodes/heartbeat",
                        build_heartbeat(
                            job_id="job-1",
                            node_id="node-1",
                            active_sandboxes=0,
                            node_url=f"http://{node_host}:{node_port}",
                            capabilities=("sandbox", "image-cache"),
                            total_resources=ResourceQuantity(
                                vcpu=2,
                                memory_mb=1024,
                                disk_mb=128,
                            ),
                        ),
                    )
                    self.assertEqual(result.status, 200)
                    created = self._json_request(
                        f"{base}/v1/sandboxes",
                        method="POST",
                        payload={
                            "id": "file-one",
                            "image": "busybox",
                            "memory_mb": 128,
                        },
                    )
                    uploaded = self._bytes_request(
                        f"{base}/v1/sandboxes/file-one/files?path={quote('/tmp/out.txt')}",
                        method="PUT",
                        body=b"via gateway\n",
                    )
                    downloaded = self._bytes_request(
                        f"{base}/v1/sandboxes/file-one/files?path={quote('/tmp/out.txt')}",
                    )
                finally:
                    gateway.shutdown()
                    gateway.server_close()
            finally:
                node.shutdown()
                node.server_close()

        self.assertEqual(created["sandbox"]["spec"]["id"], "file-one")
        self.assertEqual(uploaded["json"]["size"], 12)
        self.assertEqual(downloaded["body"], b"via gateway\n")
        self.assertEqual(downloaded["headers"]["X-Sandbox-Path"], "/tmp/out.txt")

    def test_gateway_deduplicates_concurrent_cold_image_pull(self) -> None:
        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            image_runtime = CountingPullRuntime()
            node = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=raw_path / "node-sandboxes.json",
                image_file=raw_path / "node-images.json",
                job_id="job-1",
                node_id="node-1",
                total_resources=ResourceQuantity(
                    vcpu=16,
                    memory_mb=32768,
                    disk_mb=200_000,
                ),
                runtime=DockerGvisorRuntime(dry_run=True, allow_storage_opt_quota=True),
                image_runtime=image_runtime,
            )
            node_thread = Thread(target=node.serve_forever, daemon=True)
            node_thread.start()
            try:
                node_host, node_port = node.server_address
                gateway = build_server(
                    "127.0.0.1",
                    0,
                    raw_path / "heartbeats.json",
                    routing_file=raw_path / "routes.json",
                    image_file=raw_path / "gateway-images.json",
                    local_image_builds_enabled=False,
                )
                gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
                gateway_thread.start()
                try:
                    host, port = gateway.server_address
                    base = f"http://{host}:{port}"
                    result = post_heartbeat(
                        f"{base}/v1/nodes/heartbeat",
                        build_heartbeat(
                            job_id="job-1",
                            node_id="node-1",
                            node_url=f"http://{node_host}:{node_port}",
                            capabilities=("sandbox", "image-cache"),
                            total_resources=ResourceQuantity(
                                vcpu=16,
                                memory_mb=32768,
                                disk_mb=200_000,
                            ),
                        ),
                    )
                    self.assertEqual(result.status, 200)

                    results: dict[int, dict] = {}
                    errors: list[BaseException] = []

                    def create(index: int) -> None:
                        try:
                            results[index] = self._json_request(
                                f"{base}/v1/sandboxes",
                                method="POST",
                                payload={
                                    "id": f"cold-{index}",
                                    "image": "python:3.12-slim",
                                    "cpus": 1,
                                    "memory_mb": 512,
                                    "disk_mb": 1024,
                                },
                            )
                        except BaseException as exc:
                            errors.append(exc)

                    threads = [
                        Thread(target=create, args=(index,)) for index in range(8)
                    ]
                    for thread in threads:
                        thread.start()
                    for thread in threads:
                        thread.join()
                    sandboxes = self._json_request(f"{base}/v1/sandboxes")
                finally:
                    gateway.shutdown()
                    gateway.server_close()
            finally:
                node.shutdown()
                node.server_close()

            if errors:
                raise errors[0]
            self.assertEqual(len(results), 8)
            self.assertEqual(
                {result["sandbox"]["spec"]["id"] for result in results.values()},
                {f"cold-{index}" for index in range(8)},
            )
            self.assertEqual(image_runtime.pulls, ["python:3.12-slim"])
            self.assertEqual(len(sandboxes["sandboxes"]), 8)

    def test_gateway_recovers_idempotent_duplicate_create_on_node(self) -> None:
        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            node = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=raw_path / "node-sandboxes.json",
                image_file=raw_path / "node-images.json",
                job_id="job-1",
                node_id="node-1",
                total_resources=ResourceQuantity(
                    vcpu=4, memory_mb=8192, disk_mb=100_000
                ),
                runtime=DockerGvisorRuntime(dry_run=True, allow_storage_opt_quota=True),
            )
            node_thread = Thread(target=node.serve_forever, daemon=True)
            node_thread.start()
            try:
                node_host, node_port = node.server_address
                gateway = build_server(
                    "127.0.0.1",
                    0,
                    raw_path / "heartbeats.json",
                    routing_file=raw_path / "routes.json",
                    image_file=raw_path / "gateway-images.json",
                    local_image_builds_enabled=False,
                )
                gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
                gateway_thread.start()
                try:
                    node_base = f"http://{node_host}:{node_port}"
                    host, port = gateway.server_address
                    base = f"http://{host}:{port}"
                    result = post_heartbeat(
                        f"{base}/v1/nodes/heartbeat",
                        build_heartbeat(
                            job_id="job-1",
                            node_id="node-1",
                            node_url=node_base,
                            capabilities=("sandbox", "image-cache"),
                            total_resources=ResourceQuantity(
                                vcpu=4,
                                memory_mb=8192,
                                disk_mb=100_000,
                            ),
                        ),
                    )
                    self.assertEqual(result.status, 200)
                    payload = {
                        "id": "dup-one",
                        "image": "busybox",
                        "command": ["sh", "-lc", "sleep 2147483647"],
                        "cpus": 1,
                        "memory_mb": 512,
                        "disk_mb": 1024,
                    }
                    direct = self._json_request(
                        f"{node_base}/v1/sandboxes",
                        method="POST",
                        payload=payload,
                    )
                    recovered = self._json_request(
                        f"{base}/v1/sandboxes",
                        method="POST",
                        payload=payload,
                    )
                    sandboxes = self._json_request(f"{node_base}/v1/sandboxes")
                finally:
                    gateway.shutdown()
                    gateway.server_close()
            finally:
                node.shutdown()
                node.server_close()

            self.assertEqual(direct["sandbox"]["spec"]["id"], "dup-one")
            self.assertEqual(recovered["sandbox"]["spec"]["id"], "dup-one")
            self.assertTrue(recovered["timings"]["manager"]["idempotent"])
            self.assertEqual(len(sandboxes["sandboxes"]), 1)
            route = RoutingStore(raw_path / "routes.json").get_sandbox("dup-one")
            self.assertIsNotNone(route)

    def test_gateway_preserves_duplicate_create_conflict_for_different_spec(
        self,
    ) -> None:
        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            node = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=raw_path / "node-sandboxes.json",
                image_file=raw_path / "node-images.json",
                job_id="job-1",
                node_id="node-1",
                total_resources=ResourceQuantity(
                    vcpu=4, memory_mb=8192, disk_mb=100_000
                ),
                runtime=DockerGvisorRuntime(dry_run=True, allow_storage_opt_quota=True),
            )
            node_thread = Thread(target=node.serve_forever, daemon=True)
            node_thread.start()
            try:
                node_host, node_port = node.server_address
                gateway = build_server(
                    "127.0.0.1",
                    0,
                    raw_path / "heartbeats.json",
                    routing_file=raw_path / "routes.json",
                    image_file=raw_path / "gateway-images.json",
                    local_image_builds_enabled=False,
                )
                gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
                gateway_thread.start()
                try:
                    node_base = f"http://{node_host}:{node_port}"
                    host, port = gateway.server_address
                    base = f"http://{host}:{port}"
                    result = post_heartbeat(
                        f"{base}/v1/nodes/heartbeat",
                        build_heartbeat(
                            job_id="job-1",
                            node_id="node-1",
                            node_url=node_base,
                            capabilities=("sandbox", "image-cache"),
                            total_resources=ResourceQuantity(
                                vcpu=4,
                                memory_mb=8192,
                                disk_mb=100_000,
                            ),
                        ),
                    )
                    self.assertEqual(result.status, 200)
                    self._json_request(
                        f"{node_base}/v1/sandboxes",
                        method="POST",
                        payload={
                            "id": "dup-one",
                            "image": "busybox",
                            "cpus": 1,
                            "memory_mb": 512,
                            "disk_mb": 1024,
                        },
                    )
                    conflict = self._json_request(
                        f"{base}/v1/sandboxes",
                        method="POST",
                        payload={
                            "id": "dup-one",
                            "image": "python:3.12-slim",
                            "cpus": 1,
                            "memory_mb": 512,
                            "disk_mb": 1024,
                        },
                        allow_error=True,
                    )
                finally:
                    gateway.shutdown()
                    gateway.server_close()
            finally:
                node.shutdown()
                node.server_close()

            self.assertEqual(conflict["status"], 409)
            self.assertIn("already exists", conflict["body"]["error"])

    def test_gateway_lists_incompatible_nodes_but_does_not_place_new_sandboxes(
        self,
    ) -> None:
        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            node = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=raw_path / "node-sandboxes.json",
                image_file=raw_path / "node-images.json",
                job_id="job-old",
                node_id="node-old",
                total_resources=ResourceQuantity(
                    vcpu=4, memory_mb=8192, disk_mb=100_000
                ),
                runtime=DockerGvisorRuntime(dry_run=True, allow_storage_opt_quota=True),
            )
            node_thread = Thread(target=node.serve_forever, daemon=True)
            node_thread.start()
            try:
                node_host, node_port = node.server_address
                node_base = f"http://{node_host}:{node_port}"
                self._json_request(
                    f"{node_base}/v1/sandboxes",
                    method="POST",
                    payload={
                        "id": "old-one",
                        "image": "busybox",
                        "cpus": 1,
                        "memory_mb": 512,
                        "disk_mb": 1024,
                    },
                )
                gateway = build_server(
                    "127.0.0.1",
                    0,
                    raw_path / "heartbeats.json",
                    routing_file=raw_path / "routes.json",
                    image_file=raw_path / "gateway-images.json",
                    local_image_builds_enabled=False,
                )
                gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
                gateway_thread.start()
                try:
                    host, port = gateway.server_address
                    base = f"http://{host}:{port}"
                    post_heartbeat(
                        f"{base}/v1/nodes/heartbeat",
                        build_heartbeat(
                            job_id="job-old",
                            node_id="node-old",
                            node_url=node_base,
                            agent_version="0.0.0-old",
                            capabilities=("sandbox", "image-cache"),
                            total_resources=ResourceQuantity(
                                vcpu=4,
                                memory_mb=8192,
                                disk_mb=100_000,
                            ),
                        ),
                    )
                    listed = self._json_request(f"{base}/v1/sandboxes?refresh=true")
                    create = self._json_request(
                        f"{base}/v1/sandboxes",
                        method="POST",
                        payload={
                            "id": "new-one",
                            "image": "busybox",
                            "cpus": 1,
                            "memory_mb": 512,
                            "disk_mb": 1024,
                        },
                        allow_error=True,
                    )
                finally:
                    gateway.shutdown()
                    gateway.server_close()
            finally:
                node.shutdown()
                node.server_close()

            self.assertEqual(listed["sandboxes"][0]["id"], "old-one")
            self.assertEqual(listed["sandboxes"][0]["node"]["node_id"], "node-old")
            self.assertEqual(create["status"], 503)
            self.assertIn("no ready node", create["body"]["error"])

    def test_structures_non_json_proxy_errors(self) -> None:
        response = control_plane.ProxiedResponse(
            503,
            {"Content-Type": "text/html"},
            b"<html><title>Job is unavailable | UCloud</title></html>",
        )

        structured = control_plane._structured_proxy_error(response)

        self.assertIsNotNone(structured)
        assert structured is not None
        self.assertTrue(structured["retryable"])
        self.assertEqual(structured["status"], 503)
        self.assertIn("Job is unavailable", structured["upstream_body_preview"])

    def test_enriches_old_node_sandbox_records_with_top_level_identity(self) -> None:
        heartbeat = build_heartbeat(
            job_id="job-1",
            node_id="node-1",
            node_url="http://node-1:8090",
        )

        enriched = control_plane._enrich_sandbox_record(
            {
                "container_name": "ucloud-sandbox-old-one",
                "spec": {
                    "id": "old-one",
                    "image": "busybox",
                    "labels": {"sample": "old"},
                },
                "state": "running",
            },
            heartbeat,
        )

        self.assertEqual(enriched["id"], "old-one")
        self.assertEqual(enriched["sandbox_id"], "old-one")
        self.assertEqual(enriched["name"], "ucloud-sandbox-old-one")
        self.assertEqual(enriched["image"], "busybox")
        self.assertEqual(enriched["labels"], {"sample": "old"})
        self.assertEqual(enriched["node"]["node_id"], "node-1")

    def test_gateway_builds_images_locally_and_sandbox_nodes_pull_registry_tag(
        self,
    ) -> None:
        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            regular = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=raw_path / "regular-sandboxes.json",
                image_file=raw_path / "regular-images.json",
                job_id="job-1",
                node_id="node-1",
                total_resources=ResourceQuantity(
                    vcpu=4, memory_mb=8192, disk_mb=100_000
                ),
                runtime=DockerGvisorRuntime(dry_run=True, allow_storage_opt_quota=True),
            )
            regular_thread = Thread(target=regular.serve_forever, daemon=True)
            regular_thread.start()
            try:
                regular_host, regular_port = regular.server_address
                gateway = build_server(
                    "127.0.0.1",
                    0,
                    raw_path / "heartbeats.json",
                    routing_file=raw_path / "routes.json",
                    image_file=raw_path / "gateway-images.json",
                    image_runtime=DockerImageRuntime(dry_run=True),
                )
                gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
                gateway_thread.start()
                try:
                    host, port = gateway.server_address
                    base = f"http://{host}:{port}"
                    for heartbeat in (
                        build_heartbeat(
                            job_id="job-1",
                            node_id="node-1",
                            node_url=f"http://{regular_host}:{regular_port}",
                            capabilities=("sandbox", "image-cache"),
                            total_resources=ResourceQuantity(
                                vcpu=4,
                                memory_mb=8192,
                                disk_mb=100_000,
                            ),
                        ),
                        build_heartbeat(
                            job_id="job-2",
                            node_id="node-2",
                            node_url="http://builder.invalid:8090",
                            capabilities=("image-cache", "image-build", "snapshot"),
                            total_resources=ResourceQuantity(
                                vcpu=64,
                                memory_mb=262144,
                                disk_mb=1_000_000,
                            ),
                        ),
                    ):
                        result = post_heartbeat(
                            f"{base}/v1/nodes/heartbeat",
                            heartbeat,
                        )
                        self.assertEqual(result.status, 200)

                    built = self._json_request(
                        f"{base}/v1/images/build",
                        method="POST",
                        payload={
                            "id": "custom",
                            "tag": "registry.example.org/custom:latest",
                            "context_path": "/tmp/context",
                            "push": True,
                        },
                    )
                    images = self._json_request(f"{base}/v1/images")
                    created = self._json_request(
                        f"{base}/v1/sandboxes",
                        method="POST",
                        payload={
                            "id": "custom-one",
                            "image": "registry.example.org/custom:latest",
                            "memory_mb": 128,
                        },
                    )
                    with request.urlopen(
                        f"http://{regular_host}:{regular_port}/v1/sandboxes",
                        timeout=5,
                    ) as response:
                        regular_payload = json.loads(response.read().decode("utf-8"))
                    regular_images = self._json_request(
                        f"http://{regular_host}:{regular_port}/v1/images"
                    )
                finally:
                    gateway.shutdown()
                    gateway.server_close()
            finally:
                regular.shutdown()
                regular.server_close()

            self.assertEqual(built["image"]["id"], "custom")
            self.assertIn("pushCommand", built)
            self.assertEqual(
                [(image["id"], image.get("location")) for image in images["images"]],
                [("custom", "control-plane")],
            )
            self.assertEqual(created["sandbox"]["spec"]["id"], "custom-one")
            self.assertEqual(
                [record["spec"]["id"] for record in regular_payload["sandboxes"]],
                ["custom-one"],
            )
            self.assertEqual(
                [(image["id"], image["tag"]) for image in regular_images["images"]],
                [
                    (
                        "registry.example.org-custom-latest",
                        "registry.example.org/custom:latest",
                    )
                ],
            )

    def test_gateway_resolves_pushed_image_id_to_registry_tag_on_create(self) -> None:
        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            image_runtime = CountingPullRuntime()
            regular = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=raw_path / "regular-sandboxes.json",
                image_file=raw_path / "regular-images.json",
                job_id="job-1",
                node_id="node-1",
                total_resources=ResourceQuantity(
                    vcpu=4, memory_mb=8192, disk_mb=100_000
                ),
                runtime=DockerGvisorRuntime(dry_run=True, allow_storage_opt_quota=True),
                image_runtime=image_runtime,
            )
            regular_thread = Thread(target=regular.serve_forever, daemon=True)
            regular_thread.start()
            try:
                regular_host, regular_port = regular.server_address
                gateway = build_server(
                    "127.0.0.1",
                    0,
                    raw_path / "heartbeats.json",
                    routing_file=raw_path / "routes.json",
                    image_file=raw_path / "gateway-images.json",
                    image_runtime=DockerImageRuntime(dry_run=True),
                )
                gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
                gateway_thread.start()
                try:
                    host, port = gateway.server_address
                    base = f"http://{host}:{port}"
                    result = post_heartbeat(
                        f"{base}/v1/nodes/heartbeat",
                        build_heartbeat(
                            job_id="job-1",
                            node_id="node-1",
                            node_url=f"http://{regular_host}:{regular_port}",
                            capabilities=("sandbox", "image-cache"),
                            total_resources=ResourceQuantity(
                                vcpu=4,
                                memory_mb=8192,
                                disk_mb=100_000,
                            ),
                        ),
                    )
                    self.assertEqual(result.status, 200)

                    built = self._json_request(
                        f"{base}/v1/images/build",
                        method="POST",
                        payload={
                            "id": "custom",
                            "tag": "registry.example.org/custom:latest",
                            "context_path": "/tmp/context",
                            "push": True,
                        },
                    )
                    created = self._json_request(
                        f"{base}/v1/sandboxes",
                        method="POST",
                        payload={
                            "id": "custom-by-id",
                            "image": "custom",
                            "memory_mb": 128,
                        },
                    )
                finally:
                    gateway.shutdown()
                    gateway.server_close()
            finally:
                regular.shutdown()
                regular.server_close()

            self.assertTrue(built["image"]["pushed"])
            self.assertTrue(built["image"]["available_to_sandboxes"])
            self.assertEqual(
                created["sandbox"]["spec"]["image"],
                "registry.example.org/custom:latest",
            )
            self.assertEqual(
                image_runtime.pulls, ["registry.example.org/custom:latest"]
            )

    def test_gateway_rejects_unpushed_image_id_on_create(self) -> None:
        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            gateway = build_server(
                "127.0.0.1",
                0,
                raw_path / "heartbeats.json",
                routing_file=raw_path / "routes.json",
                image_file=raw_path / "gateway-images.json",
                image_runtime=DockerImageRuntime(dry_run=True),
            )
            gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
            gateway_thread.start()
            try:
                host, port = gateway.server_address
                base = f"http://{host}:{port}"
                built = self._json_request(
                    f"{base}/v1/images/build",
                    method="POST",
                    payload={
                        "id": "custom",
                        "tag": "registry.example.org/custom:latest",
                        "context_path": "/tmp/context",
                    },
                )
                created = self._json_request(
                    f"{base}/v1/sandboxes",
                    method="POST",
                    payload={
                        "id": "custom-by-id",
                        "image": "custom",
                        "memory_mb": 128,
                    },
                    allow_error=True,
                )
            finally:
                gateway.shutdown()
                gateway.server_close()

            self.assertFalse(built["image"]["pushed"])
            self.assertEqual(created["status"], 400)
            self.assertIn("not available to sandbox nodes", created["body"]["error"])
            self.assertEqual(created["body"]["image_id"], "custom")

    def test_gateway_records_pending_image_build_when_no_builder_is_ready(self) -> None:
        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            gateway = build_server(
                "127.0.0.1",
                0,
                raw_path / "heartbeats.json",
                routing_file=raw_path / "routes.json",
            )
            gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
            gateway_thread.start()
            try:
                host, port = gateway.server_address
                result = self._json_request(
                    f"http://{host}:{port}/v1/images/build",
                    method="POST",
                    payload={
                        "id": "custom",
                        "tag": "registry.example.org/custom:latest",
                        "context_path": "/tmp/context",
                        "push": True,
                    },
                    allow_error=True,
                )
            finally:
                gateway.shutdown()
                gateway.server_close()

            self.assertEqual(result["status"], 503)
            self.assertEqual(result["body"]["pending_image_builds"], 1)
            self.assertEqual(
                RoutingStore(raw_path / "routes.json").pending_image_build_count(), 1
            )

    def test_gateway_routes_image_build_to_builder_only_node(self) -> None:
        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            builder = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=raw_path / "builder-sandboxes.json",
                image_file=raw_path / "builder-images.json",
                job_id="job-builder",
                node_id="builder-1",
                runtime=DockerGvisorRuntime(dry_run=True, allow_storage_opt_quota=True),
                image_builds_enabled=True,
            )
            builder_thread = Thread(target=builder.serve_forever, daemon=True)
            builder_thread.start()
            try:
                builder_host, builder_port = builder.server_address
                gateway = build_server(
                    "127.0.0.1",
                    0,
                    raw_path / "heartbeats.json",
                    routing_file=raw_path / "routes.json",
                    image_file=raw_path / "gateway-images.json",
                    local_image_builds_enabled=False,
                )
                gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
                gateway_thread.start()
                try:
                    host, port = gateway.server_address
                    base = f"http://{host}:{port}"
                    result = post_heartbeat(
                        f"{base}/v1/nodes/heartbeat",
                        build_heartbeat(
                            job_id="job-builder",
                            node_id="builder-1",
                            node_url=f"http://{builder_host}:{builder_port}",
                            capabilities=("image-cache", "image-build", "snapshot"),
                            total_resources=ResourceQuantity(
                                vcpu=16, memory_mb=49152, disk_mb=200000
                            ),
                        ),
                    )
                    self.assertEqual(result.status, 200)

                    built = self._json_request(
                        f"{base}/v1/images/build",
                        method="POST",
                        payload={
                            "id": "custom",
                            "tag": "registry.example.org/custom:latest",
                            "context_path": "/tmp/context",
                            "push": True,
                        },
                    )
                    builder_heartbeat = self._json_request(
                        f"http://{builder_host}:{builder_port}/v1/heartbeat"
                    )
                    images = self._json_request(f"{base}/v1/images")
                finally:
                    gateway.shutdown()
                    gateway.server_close()
            finally:
                builder.shutdown()
                builder.server_close()

            self.assertEqual(built["image"]["id"], "custom")
            self.assertIn("pushCommand", built)
            self.assertNotIn("sandbox", builder_heartbeat["heartbeat"]["capabilities"])
            self.assertIn(
                ("custom", "control-plane", True),
                [
                    (
                        image["id"],
                        image.get("location"),
                        image.get("available_to_sandboxes"),
                    )
                    for image in images["images"]
                ],
            )
            self.assertEqual(
                RoutingStore(raw_path / "routes.json").pending_image_build_count(), 0
            )

    def test_gateway_keeps_pending_signal_for_async_builder_image_build(self) -> None:
        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            builder = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=raw_path / "builder-sandboxes.json",
                image_file=raw_path / "builder-images.json",
                job_id="job-builder",
                node_id="builder-1",
                runtime=DockerGvisorRuntime(dry_run=True, allow_storage_opt_quota=True),
                image_builds_enabled=True,
            )
            builder_thread = Thread(target=builder.serve_forever, daemon=True)
            builder_thread.start()
            try:
                builder_host, builder_port = builder.server_address
                gateway = build_server(
                    "127.0.0.1",
                    0,
                    raw_path / "heartbeats.json",
                    routing_file=raw_path / "routes.json",
                    image_file=raw_path / "gateway-images.json",
                    local_image_builds_enabled=False,
                )
                gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
                gateway_thread.start()
                try:
                    host, port = gateway.server_address
                    base = f"http://{host}:{port}"
                    result = post_heartbeat(
                        f"{base}/v1/nodes/heartbeat",
                        build_heartbeat(
                            job_id="job-builder",
                            node_id="builder-1",
                            node_url=f"http://{builder_host}:{builder_port}",
                            capabilities=("image-cache", "image-build", "snapshot"),
                            total_resources=ResourceQuantity(
                                vcpu=16, memory_mb=49152, disk_mb=200000
                            ),
                        ),
                    )
                    self.assertEqual(result.status, 200)

                    built = self._json_request(
                        f"{base}/v1/images/build",
                        method="POST",
                        payload={
                            "id": "custom",
                            "tag": "registry.example.org/custom:latest",
                            "context_path": "/tmp/context",
                            "push": True,
                            "wait": False,
                        },
                    )
                finally:
                    gateway.shutdown()
                    gateway.server_close()
            finally:
                builder.shutdown()
                builder.server_close()

            self.assertEqual(built["build"]["image_id"], "custom")
            self.assertEqual(built["build"]["status"], "running")
            self.assertEqual(
                RoutingStore(raw_path / "routes.json").pending_image_build_count(), 1
            )

    def test_gateway_uses_bounded_proxy_timeout_for_builder_image_builds(self) -> None:
        class FakeResponse:
            status = 201
            headers: dict[str, str] = {}

            def __enter__(self) -> "FakeResponse":
                return self

            def __exit__(self, *args: object) -> None:
                return None

            def read(self) -> bytes:
                return json.dumps(
                    {
                        "image": {
                            "id": "custom",
                            "tag": "registry.example.org/custom:latest",
                            "state": "available",
                            "pushed": True,
                        },
                        "command": ["docker", "build"],
                        "exitCode": 0,
                    }
                ).encode("utf-8")

        captured_timeouts: list[object] = []

        def fake_urlopen(req: object, timeout: object = None) -> FakeResponse:
            captured_timeouts.append(timeout)
            return FakeResponse()

        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            gateway = build_server(
                "127.0.0.1",
                0,
                raw_path / "heartbeats.json",
                routing_file=raw_path / "routes.sqlite",
                metrics_file=raw_path / "metrics.jsonl",
            )
            gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
            gateway_thread.start()
            try:
                host, port = gateway.server_address
                base = f"http://{host}:{port}"
                result = post_heartbeat(
                    f"{base}/v1/nodes/heartbeat",
                    build_heartbeat(
                        job_id="job-builder",
                        node_id="builder-1",
                        node_url="http://builder.invalid:8090",
                        capabilities=("image-cache", "image-build", "snapshot"),
                    ),
                )
                self.assertEqual(result.status, 200)

                with patch.object(control_plane.request, "urlopen", fake_urlopen):
                    body = json.dumps(
                        {
                            "id": "custom",
                            "tag": "registry.example.org/custom:latest",
                            "context_path": "/tmp/context",
                            "push": True,
                        }
                    )
                    conn = HTTPConnection(host, port, timeout=5)
                    try:
                        conn.request(
                            "POST",
                            "/v1/images/build",
                            body=body,
                            headers={"Content-Type": "application/json"},
                        )
                        response = conn.getresponse()
                        built = json.loads(response.read().decode("utf-8"))
                    finally:
                        conn.close()
            finally:
                gateway.shutdown()
                gateway.server_close()

        self.assertEqual(built["image"]["id"], "custom")
        self.assertEqual(IMAGE_BUILD_PROXY_TIMEOUT_SECONDS, 30 * 60)
        self.assertEqual(captured_timeouts, [IMAGE_BUILD_PROXY_TIMEOUT_SECONDS])

    def test_gateway_records_pending_demand_when_no_node_can_fit(self) -> None:
        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            gateway = build_server(
                "127.0.0.1",
                0,
                raw_path / "heartbeats.json",
                routing_file=raw_path / "routes.json",
            )
            gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
            gateway_thread.start()
            try:
                host, port = gateway.server_address
                base = f"http://{host}:{port}"
                result = self._json_request(
                    f"{base}/v1/sandboxes",
                    method="POST",
                    payload={
                        "id": "pending-one",
                        "image": "busybox",
                        "cpus": 1,
                        "memory_mb": 512,
                        "disk_mb": 1024,
                    },
                    allow_error=True,
                )
                demand = self._json_request(f"{base}/v1/demand")
                cleanup = self._json_request(
                    f"{base}/v1/sandboxes/pending-one",
                    method="DELETE",
                )
                demand_after_cleanup = self._json_request(f"{base}/v1/demand")
            finally:
                gateway.shutdown()
                gateway.server_close()

            self.assertEqual(result["status"], 503)
            self.assertEqual(demand["pending_resources"]["vcpu"], 1.0)
            self.assertEqual(demand["pending_resources"]["memory_mb"], 512)
            self.assertEqual(demand["pending_resources"]["disk_mb"], 1024)
            self.assertEqual(demand["pending"][0]["sandbox_id"], "pending-one")
            self.assertEqual(demand["pending"][0]["attempts"], 1)
            self.assertEqual(cleanup["ok"], True)
            self.assertEqual(demand_after_cleanup["pending_resources"]["vcpu"], 0.0)
            self.assertEqual(demand_after_cleanup["pending_resources"]["memory_mb"], 0)
            self.assertEqual(demand_after_cleanup["pending_resources"]["disk_mb"], 0)
            self.assertEqual(demand_after_cleanup["pending"], [])

    def test_gateway_prepares_capacity_as_expiring_demand_signal(self) -> None:
        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            gateway = build_server(
                "127.0.0.1",
                0,
                raw_path / "heartbeats.json",
                routing_file=raw_path / "routes.sqlite",
            )
            gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
            gateway_thread.start()
            try:
                host, port = gateway.server_address
                base = f"http://{host}:{port}"
                prepared = self._json_request(
                    f"{base}/v1/capacity/prepare",
                    method="POST",
                    payload={
                        "id": "eval-soon",
                        "count": 8,
                        "cpus": 1,
                        "memory_mb": 2048,
                        "disk_mb": 10_240,
                        "ttl_seconds": 600,
                    },
                )
                listed = self._json_request(f"{base}/v1/capacity/prepare")
                demand = self._json_request(f"{base}/v1/demand")
                deleted = self._json_request(
                    f"{base}/v1/capacity/prepare/eval-soon",
                    method="DELETE",
                )
                demand_after_delete = self._json_request(f"{base}/v1/demand")
            finally:
                gateway.shutdown()
                gateway.server_close()

        self.assertEqual(prepared["prepare"]["prepare_id"], "eval-soon")
        self.assertEqual(prepared["prepare"]["count"], 8)
        self.assertEqual(prepared["prepare"]["total_resources"]["vcpu"], 8.0)
        self.assertEqual(prepared["demand"]["pending_resources"]["vcpu"], 0.0)
        self.assertEqual(prepared["demand"]["prepared_resources"]["vcpu"], 8.0)
        self.assertEqual(prepared["demand"]["desired_resources"]["memory_mb"], 16_384)
        self.assertEqual(listed["prepared"][0]["prepare_id"], "eval-soon")
        self.assertEqual(demand["prepared_resources"]["disk_mb"], 81_920)
        self.assertEqual(demand["prepared"][0]["prepare_id"], "eval-soon")
        self.assertTrue(deleted["ok"])
        self.assertEqual(deleted["deleted"]["prepare_id"], "eval-soon")
        self.assertEqual(demand_after_delete["prepared_resources"]["vcpu"], 0.0)
        self.assertEqual(demand_after_delete["prepared"], [])

    def test_gateway_prepares_capacity_with_image_prewarm(self) -> None:
        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            nodes = [
                build_node_agent_server(
                    "127.0.0.1",
                    0,
                    sandbox_file=raw_path / f"node-{index}-sandboxes.json",
                    image_file=raw_path / f"node-{index}-images.json",
                    job_id=f"job-{index}",
                    node_id=f"node-{index}",
                    total_resources=ResourceQuantity(
                        vcpu=4, memory_mb=8192, disk_mb=100_000
                    ),
                    runtime=DockerGvisorRuntime(
                        dry_run=True, allow_storage_opt_quota=True
                    ),
                )
                for index in range(2)
            ]
            node_threads = [
                Thread(target=node.serve_forever, daemon=True) for node in nodes
            ]
            for thread in node_threads:
                thread.start()
            try:
                gateway = build_server(
                    "127.0.0.1",
                    0,
                    raw_path / "heartbeats.json",
                    routing_file=raw_path / "routes.sqlite",
                )
                gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
                gateway_thread.start()
                try:
                    host, port = gateway.server_address
                    base = f"http://{host}:{port}"
                    for index, node in enumerate(nodes):
                        node_host, node_port = node.server_address
                        result = post_heartbeat(
                            f"{base}/v1/nodes/heartbeat",
                            build_heartbeat(
                                job_id=f"job-{index}",
                                node_id=f"node-{index}",
                                node_url=f"http://{node_host}:{node_port}",
                                capabilities=("sandbox", "image-cache"),
                                total_resources=ResourceQuantity(
                                    vcpu=4,
                                    memory_mb=8192,
                                    disk_mb=100_000,
                                ),
                            ),
                        )
                        self.assertEqual(result.status, 200)
                    prepared = self._json_request(
                        f"{base}/v1/capacity/prepare",
                        method="POST",
                        payload={
                            "id": "eval-soon",
                            "count": 2,
                            "cpus": 1,
                            "memory_mb": 1024,
                            "disk_mb": 2048,
                            "image": "busybox:latest",
                        },
                    )
                    node_images = [
                        self._json_request(
                            f"http://{node.server_address[0]}:{node.server_address[1]}/v1/images"
                        )
                        for node in nodes
                    ]
                finally:
                    gateway.shutdown()
                    gateway.server_close()
            finally:
                for node in nodes:
                    node.shutdown()
                    node.server_close()

        self.assertEqual(prepared["prepare"]["image"], "busybox:latest")
        self.assertEqual(prepared["image_prewarm"]["requested"], 2)
        self.assertEqual(prepared["image_prewarm"]["ready"], 2)
        self.assertEqual(len(prepared["image_prewarm"]["pulled"]), 2)
        self.assertEqual(
            [payload["images"][0]["tag"] for payload in node_images],
            ["busybox:latest", "busybox:latest"],
        )

    def test_gateway_image_pull_warms_multiple_sandbox_nodes(self) -> None:
        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            nodes = [
                build_node_agent_server(
                    "127.0.0.1",
                    0,
                    sandbox_file=raw_path / f"pull-node-{index}-sandboxes.json",
                    image_file=raw_path / f"pull-node-{index}-images.json",
                    job_id=f"pull-job-{index}",
                    node_id=f"pull-node-{index}",
                    total_resources=ResourceQuantity(
                        vcpu=4, memory_mb=8192, disk_mb=100_000
                    ),
                    runtime=DockerGvisorRuntime(
                        dry_run=True, allow_storage_opt_quota=True
                    ),
                )
                for index in range(2)
            ]
            for node in nodes:
                Thread(target=node.serve_forever, daemon=True).start()
            try:
                gateway = build_server(
                    "127.0.0.1",
                    0,
                    raw_path / "heartbeats.json",
                    routing_file=raw_path / "routes.sqlite",
                )
                gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
                gateway_thread.start()
                try:
                    host, port = gateway.server_address
                    base = f"http://{host}:{port}"
                    for index, node in enumerate(nodes):
                        node_host, node_port = node.server_address
                        post_heartbeat(
                            f"{base}/v1/nodes/heartbeat",
                            build_heartbeat(
                                job_id=f"pull-job-{index}",
                                node_id=f"pull-node-{index}",
                                node_url=f"http://{node_host}:{node_port}",
                                capabilities=("sandbox", "image-cache"),
                                total_resources=ResourceQuantity(
                                    vcpu=4,
                                    memory_mb=8192,
                                    disk_mb=100_000,
                                ),
                            ),
                        )
                    pulled = self._json_request(
                        f"{base}/v1/images/pull",
                        method="POST",
                        payload={
                            "image": "busybox:latest",
                            "id": "busybox",
                            "count": 2,
                            "cpus": 1,
                            "memory_mb": 512,
                        },
                    )
                finally:
                    gateway.shutdown()
                    gateway.server_close()
            finally:
                for node in nodes:
                    node.shutdown()
                    node.server_close()

        self.assertEqual(pulled["image"]["id"], "busybox")
        self.assertEqual(pulled["requested"], 2)
        self.assertEqual(pulled["ready"], 2)
        self.assertEqual(
            sorted(item["node"]["node_id"] for item in pulled["pulled"]),
            ["pull-node-0", "pull-node-1"],
        )

    def test_gateway_uses_bounded_proxy_timeout_for_image_pulls(self) -> None:
        class FakeResponse:
            status = 201
            headers: dict[str, str] = {}

            def __enter__(self) -> "FakeResponse":
                return self

            def __exit__(self, *args: object) -> None:
                return None

            def read(self) -> bytes:
                return json.dumps(
                    {
                        "image": {
                            "id": "large-image",
                            "tag": "registry.example.org/large:latest",
                            "state": "available",
                            "pushed": True,
                        },
                        "command": ["docker", "pull"],
                        "exitCode": 0,
                    }
                ).encode("utf-8")

        captured_timeouts: list[object] = []

        def fake_urlopen(req: object, timeout: object = None) -> FakeResponse:
            del req
            captured_timeouts.append(timeout)
            return FakeResponse()

        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            gateway = build_server(
                "127.0.0.1",
                0,
                raw_path / "heartbeats.json",
                routing_file=raw_path / "routes.sqlite",
            )
            gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
            gateway_thread.start()
            try:
                host, port = gateway.server_address
                base = f"http://{host}:{port}"
                post_heartbeat(
                    f"{base}/v1/nodes/heartbeat",
                    build_heartbeat(
                        job_id="pull-job",
                        node_id="pull-node",
                        node_url="http://pull-node.invalid:8090",
                        capabilities=("sandbox", "image-cache"),
                        cached_images=(),
                        total_resources=ResourceQuantity(
                            vcpu=4,
                            memory_mb=8192,
                            disk_mb=100_000,
                        ),
                    ),
                )

                with patch.object(control_plane.request, "urlopen", fake_urlopen):
                    body = json.dumps(
                        {
                            "image": "registry.example.org/large:latest",
                            "id": "large-image",
                            "count": 1,
                        }
                    )
                    conn = HTTPConnection(host, port, timeout=5)
                    try:
                        conn.request(
                            "POST",
                            "/v1/images/pull",
                            body=body,
                            headers={"Content-Type": "application/json"},
                        )
                        response = conn.getresponse()
                        pulled = json.loads(response.read().decode("utf-8"))
                    finally:
                        conn.close()
            finally:
                gateway.shutdown()
                gateway.server_close()

        self.assertEqual(response.status, 200)
        self.assertEqual(pulled["ready"], 1)
        self.assertEqual(IMAGE_PULL_PROXY_TIMEOUT_SECONDS, 30 * 60)
        self.assertEqual(captured_timeouts, [IMAGE_PULL_PROXY_TIMEOUT_SECONDS])

    def test_gateway_reports_image_pull_failure_when_ready_nodes_fail(self) -> None:
        class FakeResponse:
            status = 502
            headers: dict[str, str] = {}

            def __enter__(self) -> "FakeResponse":
                return self

            def __exit__(self, *args: object) -> None:
                return None

            def read(self) -> bytes:
                return json.dumps({"error": "node request failed: timed out"}).encode(
                    "utf-8"
                )

        def fake_urlopen(req: object, timeout: object = None) -> FakeResponse:
            del req, timeout
            return FakeResponse()

        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            gateway = build_server(
                "127.0.0.1",
                0,
                raw_path / "heartbeats.json",
                routing_file=raw_path / "routes.sqlite",
            )
            gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
            gateway_thread.start()
            try:
                host, port = gateway.server_address
                base = f"http://{host}:{port}"
                post_heartbeat(
                    f"{base}/v1/nodes/heartbeat",
                    build_heartbeat(
                        job_id="pull-job",
                        node_id="pull-node",
                        node_url="http://pull-node.invalid:8090",
                        capabilities=("sandbox", "image-cache"),
                        cached_images=(),
                        total_resources=ResourceQuantity(
                            vcpu=4,
                            memory_mb=8192,
                            disk_mb=100_000,
                        ),
                    ),
                )

                with patch.object(control_plane.request, "urlopen", fake_urlopen):
                    body = json.dumps(
                        {
                            "image": "registry.example.org/large:latest",
                            "id": "large-image",
                            "count": 1,
                        }
                    )
                    conn = HTTPConnection(host, port, timeout=5)
                    try:
                        conn.request(
                            "POST",
                            "/v1/images/pull",
                            body=body,
                            headers={"Content-Type": "application/json"},
                        )
                        response = conn.getresponse()
                        failed = json.loads(response.read().decode("utf-8"))
                    finally:
                        conn.close()
            finally:
                gateway.shutdown()
                gateway.server_close()

        self.assertEqual(response.status, 503)
        self.assertEqual(failed["error"], "image pull failed on ready image-cache nodes")
        self.assertEqual(failed["result"]["ready"], 0)
        self.assertEqual(
            failed["result"]["failed"][0]["error"],
            "node request failed: timed out",
        )

    def test_gateway_rejects_invalid_prepared_capacity_resources(self) -> None:
        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            gateway = build_server(
                "127.0.0.1",
                0,
                raw_path / "heartbeats.json",
                routing_file=raw_path / "routes.sqlite",
            )
            gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
            gateway_thread.start()
            try:
                host, port = gateway.server_address
                base = f"http://{host}:{port}"
                zero = self._json_request(
                    f"{base}/v1/capacity/prepare",
                    method="POST",
                    payload={"count": 1, "cpus": 0, "memory_mb": 0, "disk_mb": 0},
                    allow_error=True,
                )
                negative = self._json_request(
                    f"{base}/v1/capacity/prepare",
                    method="POST",
                    payload={"count": 1, "cpus": -1, "memory_mb": 1024},
                    allow_error=True,
                )
                demand = self._json_request(f"{base}/v1/demand")
            finally:
                gateway.shutdown()
                gateway.server_close()

        self.assertEqual(zero["status"], 400)
        self.assertIn("resources are required", zero["body"]["error"])
        self.assertEqual(negative["status"], 400)
        self.assertIn("vcpu must be non-negative", negative["body"]["error"])
        self.assertEqual(demand["prepared_resources"]["vcpu"], 0.0)

    def test_gateway_prepares_builder_capacity_as_expiring_demand_signal(self) -> None:
        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            gateway = build_server(
                "127.0.0.1",
                0,
                raw_path / "heartbeats.json",
                routing_file=raw_path / "routes.sqlite",
            )
            gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
            gateway_thread.start()
            try:
                host, port = gateway.server_address
                base = f"http://{host}:{port}"
                prepared = self._json_request(
                    f"{base}/v1/builders/prepare",
                    method="POST",
                    payload={
                        "id": "builds-soon",
                        "count": 2,
                        "ttl_seconds": 600,
                    },
                )
                listed = self._json_request(f"{base}/v1/builders/prepare")
                demand = self._json_request(f"{base}/v1/demand")
                deleted = self._json_request(
                    f"{base}/v1/builders/prepare/builds-soon",
                    method="DELETE",
                )
                demand_after_delete = self._json_request(f"{base}/v1/demand")
            finally:
                gateway.shutdown()
                gateway.server_close()

        self.assertEqual(prepared["prepare"]["prepare_id"], "builds-soon")
        self.assertEqual(prepared["prepare"]["count"], 2)
        self.assertEqual(prepared["demand"]["prepared_builder_count"], 2)
        self.assertEqual(prepared["demand"]["desired_builders"], 2)
        self.assertEqual(listed["prepared_builders"][0]["prepare_id"], "builds-soon")
        self.assertEqual(demand["prepared_builders"][0]["count"], 2)
        self.assertTrue(deleted["ok"])
        self.assertEqual(deleted["deleted"]["prepare_id"], "builds-soon")
        self.assertEqual(demand_after_delete["prepared_builder_count"], 0)
        self.assertEqual(demand_after_delete["desired_builders"], 0)

    def test_gateway_rejects_invalid_prepared_builder_count(self) -> None:
        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            gateway = build_server(
                "127.0.0.1",
                0,
                raw_path / "heartbeats.json",
                routing_file=raw_path / "routes.sqlite",
            )
            gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
            gateway_thread.start()
            try:
                host, port = gateway.server_address
                rejected = self._json_request(
                    f"http://{host}:{port}/v1/builders/prepare",
                    method="POST",
                    payload={"id": "bad", "count": 0},
                    allow_error=True,
                )
                demand = self._json_request(f"http://{host}:{port}/v1/demand")
            finally:
                gateway.shutdown()
                gateway.server_close()

        self.assertEqual(rejected["status"], 400)
        self.assertIn("count must be positive", rejected["body"]["error"])
        self.assertEqual(demand["prepared_builder_count"], 0)

    def test_gateway_metrics_records_scaleup_wait_after_pending_sandbox_is_placed(
        self,
    ) -> None:
        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            node = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=raw_path / "node-sandboxes.json",
                image_file=raw_path / "node-images.json",
                job_id="job-1",
                node_id="node-1",
                total_resources=ResourceQuantity(
                    vcpu=4, memory_mb=8192, disk_mb=100_000
                ),
                runtime=DockerGvisorRuntime(dry_run=True, allow_storage_opt_quota=True),
            )
            node_thread = Thread(target=node.serve_forever, daemon=True)
            node_thread.start()
            try:
                node_host, node_port = node.server_address
                gateway = build_server(
                    "127.0.0.1",
                    0,
                    raw_path / "heartbeats.json",
                    routing_file=raw_path / "routes.json",
                    metrics_file=raw_path / "metrics.jsonl",
                )
                gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
                gateway_thread.start()
                try:
                    host, port = gateway.server_address
                    base = f"http://{host}:{port}"
                    rejected = self._json_request(
                        f"{base}/v1/sandboxes",
                        method="POST",
                        payload={
                            "id": "scale-one",
                            "image": "busybox",
                            "cpus": 1,
                            "memory_mb": 512,
                            "disk_mb": 1024,
                        },
                        allow_error=True,
                    )
                    result = post_heartbeat(
                        f"{base}/v1/nodes/heartbeat",
                        build_heartbeat(
                            job_id="job-1",
                            node_id="node-1",
                            node_url=f"http://{node_host}:{node_port}",
                            capabilities=("sandbox", "image-cache", "disk-quota"),
                            total_resources=ResourceQuantity(
                                vcpu=4,
                                memory_mb=8192,
                                disk_mb=100_000,
                            ),
                        ),
                    )
                    self.assertEqual(result.status, 200)
                    created = self._json_request(
                        f"{base}/v1/sandboxes",
                        method="POST",
                        payload={
                            "id": "scale-one",
                            "image": "busybox",
                            "cpus": 1,
                            "memory_mb": 512,
                            "disk_mb": 1024,
                        },
                    )
                    metrics = self._json_request(f"{base}/v1/metrics")
                finally:
                    gateway.shutdown()
                    gateway.server_close()
            finally:
                node.shutdown()
                node.server_close()

        self.assertEqual(rejected["status"], 503)
        self.assertEqual(created["sandbox"]["spec"]["id"], "scale-one")
        self.assertEqual(metrics["nodes"]["fresh"], 1)
        self.assertEqual(metrics["sandboxes"]["active_routes"], 1)
        self.assertEqual(metrics["sandboxes"]["pending"], 0)
        self.assertEqual(metrics["scale_up"]["samples"], 1)
        self.assertEqual(
            metrics["scale_up"]["recent"][0]["data"]["sandbox_id"],
            "scale-one",
        )
        self.assertTrue(metrics["scale_up"]["recent"][0]["data"]["had_pending_demand"])

    def _json_request(
        self,
        url: str,
        *,
        method: str = "GET",
        payload: dict | None = None,
        headers: dict[str, str] | None = None,
        allow_error: bool = False,
    ) -> dict:
        body = None
        request_headers = dict(headers or {})
        if payload is not None:
            body = json.dumps(payload).encode("utf-8")
            request_headers["Content-Type"] = "application/json"
        req = request.Request(url, data=body, method=method, headers=request_headers)
        try:
            with request.urlopen(req, timeout=5) as response:
                return json.loads(response.read().decode("utf-8"))
        except error.HTTPError as exc:
            if not allow_error:
                raise
            return {
                "status": exc.code,
                "body": json.loads(exc.read().decode("utf-8")),
            }

    def _bytes_request(
        self,
        url: str,
        *,
        method: str = "GET",
        body: bytes | None = None,
    ) -> dict:
        headers = {}
        if body is not None:
            headers["Content-Type"] = "application/octet-stream"
        req = request.Request(url, data=body, method=method, headers=headers)
        with request.urlopen(req, timeout=5) as response:
            raw = response.read()
            content_type = response.headers.get("Content-Type", "")
            if content_type.startswith("application/json"):
                return {
                    "json": json.loads(raw.decode("utf-8")),
                    "headers": response.headers,
                }
            return {"body": raw, "headers": response.headers}


if __name__ == "__main__":
    unittest.main()
