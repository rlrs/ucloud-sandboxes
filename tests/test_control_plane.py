from tempfile import TemporaryDirectory
from threading import Lock, Thread
from time import sleep
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
from urllib import error, request
from urllib.parse import quote
import unittest

from ucloud_sandboxes.agent import build_heartbeat, post_heartbeat
from ucloud_sandboxes.control_plane import build_server
from ucloud_sandboxes.images import DockerImageRuntime
from ucloud_sandboxes.models import ResourceQuantity
from ucloud_sandboxes.node_agent import build_node_agent_server
from ucloud_sandboxes.routing import RoutingStore
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

    def copy_to_container(
        self,
        sandbox_id: str,
        source_path: Path,
        container_path: str,
    ) -> CommandResult:
        result = super().copy_to_container(sandbox_id, source_path, container_path)
        self.files[(sandbox_id, container_path)] = source_path.read_bytes()
        return result

    def copy_from_container(
        self,
        sandbox_id: str,
        container_path: str,
        target_path: Path,
    ) -> CommandResult:
        result = super().copy_from_container(sandbox_id, container_path, target_path)
        target_path.write_bytes(self.files[(sandbox_id, container_path)])
        return result


class ControlPlaneTests(unittest.TestCase):
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
                with request.urlopen(f"http://{host}:{port}/v1/nodes", timeout=5) as response:
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
                total_resources=ResourceQuantity(vcpu=4, memory_mb=8192, disk_mb=100_000),
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

    def test_gateway_bearer_token_protects_proxied_api(self) -> None:
        with TemporaryDirectory() as raw_dir:
            node = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=Path(raw_dir) / "node-sandboxes.json",
                image_file=Path(raw_dir) / "node-images.json",
                job_id="job-1",
                node_id="node-1",
                total_resources=ResourceQuantity(vcpu=4, memory_mb=8192, disk_mb=100_000),
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
                finally:
                    gateway.shutdown()
                    gateway.server_close()
            finally:
                node.shutdown()
                node.server_close()

            self.assertEqual(healthz, {"ok": True})
            self.assertEqual(unauthorized["status"], 401)
            self.assertEqual(unauthorized["body"], {"error": "unauthorized"})
            self.assertEqual(authorized, {"sandboxes": []})

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
        self.assertNotIn("secret-token", html)
        self.assertIn("text/css", css_type or "")
        self.assertIn(".metric-grid", css)
        self.assertIn("application/javascript", js_type or "")
        self.assertIn("/v1/metrics", js)
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
                    listed = self._json_request(f"{base}/v1/sandboxes")
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
            self.assertEqual(node1_payload, {"sandboxes": []})
            self.assertEqual(node2_payload, {"sandboxes": []})

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
                        payload={"id": "file-one", "image": "busybox", "memory_mb": 128},
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

                    threads = [Thread(target=create, args=(index,)) for index in range(8)]
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
                total_resources=ResourceQuantity(vcpu=4, memory_mb=8192, disk_mb=100_000),
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
            self.assertTrue(recovered["recovered"])
            self.assertEqual(len(sandboxes["sandboxes"]), 1)
            route = RoutingStore(raw_path / "routes.json").get_sandbox("dup-one")
            self.assertIsNotNone(route)

    def test_gateway_preserves_duplicate_create_conflict_for_different_spec(self) -> None:
        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            node = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=raw_path / "node-sandboxes.json",
                image_file=raw_path / "node-images.json",
                job_id="job-1",
                node_id="node-1",
                total_resources=ResourceQuantity(vcpu=4, memory_mb=8192, disk_mb=100_000),
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

            self.assertEqual(conflict["status"], 400)
            self.assertIn("already exists", conflict["body"]["error"])

    def test_gateway_builds_images_locally_and_sandbox_nodes_pull_registry_tag(self) -> None:
        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            regular = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=raw_path / "regular-sandboxes.json",
                image_file=raw_path / "regular-images.json",
                job_id="job-1",
                node_id="node-1",
                total_resources=ResourceQuantity(vcpu=4, memory_mb=8192, disk_mb=100_000),
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
                [("registry.example.org-custom-latest", "registry.example.org/custom:latest")],
            )

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
            self.assertEqual(RoutingStore(raw_path / "routes.json").pending_image_build_count(), 1)

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
                            total_resources=ResourceQuantity(vcpu=16, memory_mb=49152, disk_mb=200000),
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
                finally:
                    gateway.shutdown()
                    gateway.server_close()
            finally:
                builder.shutdown()
                builder.server_close()

            self.assertEqual(built["image"]["id"], "custom")
            self.assertIn("pushCommand", built)
            self.assertNotIn("sandbox", builder_heartbeat["heartbeat"]["capabilities"])
            self.assertEqual(RoutingStore(raw_path / "routes.json").pending_image_build_count(), 0)

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
            self.assertEqual(cleanup["ok"], True)
            self.assertEqual(demand_after_cleanup["pending_resources"]["vcpu"], 0.0)
            self.assertEqual(demand_after_cleanup["pending_resources"]["memory_mb"], 0)
            self.assertEqual(demand_after_cleanup["pending_resources"]["disk_mb"], 0)

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
        self.assertTrue(deleted["ok"])
        self.assertEqual(deleted["deleted"]["prepare_id"], "eval-soon")
        self.assertEqual(demand_after_delete["prepared_resources"]["vcpu"], 0.0)

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

    def test_gateway_metrics_records_scaleup_wait_after_pending_sandbox_is_placed(self) -> None:
        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            node = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=raw_path / "node-sandboxes.json",
                image_file=raw_path / "node-images.json",
                job_id="job-1",
                node_id="node-1",
                total_resources=ResourceQuantity(vcpu=4, memory_mb=8192, disk_mb=100_000),
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
        self.assertTrue(
            metrics["scale_up"]["recent"][0]["data"]["had_pending_demand"]
        )

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
