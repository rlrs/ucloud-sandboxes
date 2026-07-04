from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Event, Thread
import asyncio
import json
from urllib import request
from urllib.parse import quote
import unittest

from ucloud_sandboxes.deployment import package_version
from ucloud_sandboxes.gateway import NodeGatewayClient
from ucloud_sandboxes.images import DockerImageRuntime
from ucloud_sandboxes.models import NodeRuntimeMetrics, ResourceQuantity, utc_now
from ucloud_sandboxes.node_agent import build_node_agent_server
from ucloud_sandboxes.sandbox import CommandResult, DockerGvisorRuntime
from ucloud_sandboxes.sandbox_exec import SandboxExecSpec


class NodeAgentTests(unittest.TestCase):
    def test_creates_lists_deletes_sandbox_over_http(self) -> None:
        with TemporaryDirectory() as raw_dir:
            sandbox_file = Path(raw_dir) / "sandboxes.json"
            image_file = Path(raw_dir) / "images.json"
            runtime = DockerGvisorRuntime(dry_run=True)
            server = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=sandbox_file,
                image_file=image_file,
                job_id="job-1",
                node_id="node-1",
                node_url="http://node-1:8090",
                total_resources=ResourceQuantity(vcpu=4, memory_mb=8192, disk_mb=100_000),
                cpu_overcommit=2.0,
                runtime=runtime,
                runtime_metrics_provider=lambda: NodeRuntimeMetrics(
                    collected_at=utc_now(),
                    cpu_percent=25.0,
                    cpu_vcpu=1.0,
                    cpu_count=4,
                    memory_total_mb=8192,
                    memory_used_mb=2048,
                    memory_available_mb=6144,
                    memory_percent=25.0,
                ),
            )
            thread = Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address
                base = f"http://{host}:{port}"
                create_payload = {
                    "id": "sbx-1",
                    "image": "busybox",
                    "command": ["true"],
                    "memory_mb": 128,
                }
                create = self._json_request(
                    f"{base}/v1/sandboxes",
                    method="POST",
                    payload=create_payload,
                )
                listed = self._json_request(f"{base}/v1/sandboxes")
                healthz = self._json_request(f"{base}/healthz")
                heartbeat = self._json_request(f"{base}/v1/heartbeat")
                deleted = self._json_request(
                    f"{base}/v1/sandboxes/sbx-1",
                    method="DELETE",
                )
            finally:
                server.shutdown()
                server.server_close()

            self.assertEqual(create["sandbox"]["spec"]["id"], "sbx-1")
            self.assertEqual(create["sandbox"]["state"], "planned")
            self.assertEqual(listed["sandboxes"][0]["spec"]["id"], "sbx-1")
            self.assertEqual(
                healthz,
                {
                    "ok": True,
                    "service": "node-agent",
                    "version": package_version(),
                },
            )
            self.assertEqual(heartbeat["heartbeat"]["node_url"], "http://node-1:8090")
            self.assertEqual(heartbeat["heartbeat"]["active_sandboxes"], 0)
            self.assertEqual(heartbeat["heartbeat"]["effective_resources"]["vcpu"], 8.0)
            self.assertEqual(heartbeat["heartbeat"]["runtime_metrics"]["cpu_percent"], 25.0)
            self.assertEqual(heartbeat["heartbeat"]["runtime_metrics"]["memory_used_mb"], 2048)
            self.assertEqual(deleted["deleted"]["spec"]["id"], "sbx-1")

    def test_builds_images_and_snapshots_over_http(self) -> None:
        with TemporaryDirectory() as raw_dir:
            sandbox_file = Path(raw_dir) / "sandboxes.json"
            image_file = Path(raw_dir) / "images.json"
            runtime = DockerGvisorRuntime(dry_run=True)
            server = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=sandbox_file,
                image_file=image_file,
                job_id="job-1",
                node_id="node-1",
                runtime=runtime,
                image_builds_enabled=True,
            )
            thread = Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address
                base = f"http://{host}:{port}"
                built = self._json_request(
                    f"{base}/v1/images/build",
                    method="POST",
                    payload={
                        "id": "python-base",
                        "tag": "local/python-base:latest",
                        "context_path": "/tmp/context",
                        "dockerfile": "Dockerfile",
                    },
                )
                heartbeat = self._json_request(f"{base}/v1/heartbeat")
                created = self._json_request(
                    f"{base}/v1/sandboxes",
                    method="POST",
                    payload={"id": "sbx-1", "image": "busybox", "memory_mb": 128},
                )
                snapshot = self._json_request(
                    f"{base}/v1/sandboxes/sbx-1/snapshot",
                    method="POST",
                    payload={
                        "id": "snap-1",
                        "image": "local/snap-1:latest",
                    },
                )
                images = self._json_request(f"{base}/v1/images")
            finally:
                server.shutdown()
                server.server_close()

            self.assertEqual(built["image"]["id"], "python-base")
            self.assertIn("build", built["command"])
            self.assertIn("timings", built)
            self.assertIn("wait_for_build_ms", built["timings"]["phases"])
            self.assertIn("docker_build_ms", built["build"]["timings"]["phases"])
            self.assertIn("timings", created)
            self.assertIn("manager_create_ms", created["timings"]["phases"])
            self.assertIn("docker_create_ms", created["timings"]["manager"]["phases"])
            self.assertEqual(
                heartbeat["heartbeat"]["capabilities"],
                ["image-cache", "image-build", "snapshot"],
            )
            self.assertEqual(snapshot["image"]["id"], "snap-1")
            self.assertIn("commit", snapshot["command"])
            self.assertEqual(len(images["images"]), 2)

    def test_image_builds_are_tracked_and_deduplicated(self) -> None:
        with TemporaryDirectory() as raw_dir:
            sandbox_file = Path(raw_dir) / "sandboxes.json"
            image_file = Path(raw_dir) / "images.json"
            executor = BlockingExecutor()
            server = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=sandbox_file,
                image_file=image_file,
                job_id="job-1",
                node_id="node-1",
                runtime=DockerGvisorRuntime(dry_run=True),
                image_runtime=DockerImageRuntime(executor=executor),
                image_builds_enabled=True,
            )
            thread = Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address
                base = f"http://{host}:{port}"
                payload = {
                    "id": "python-base",
                    "tag": "local/python-base:latest",
                    "context_path": "/tmp/context",
                    "wait": False,
                }
                first = self._json_request(
                    f"{base}/v1/images/build",
                    method="POST",
                    payload=payload,
                )
                self.assertTrue(executor.started.wait(2))
                duplicate = self._json_request(
                    f"{base}/v1/images/build",
                    method="POST",
                    payload=payload,
                )
                active = self._json_request(f"{base}/v1/images/builds/python-base")
                heartbeat = self._json_request(f"{base}/v1/heartbeat")
                executor.release.set()
                finished = self._wait_for_build(base, "python-base")
                images = self._json_request(f"{base}/v1/images")
            finally:
                server.shutdown()
                server.server_close()

            self.assertTrue(first["started"])
            self.assertFalse(duplicate["started"])
            self.assertEqual(first["build"]["build_id"], duplicate["build"]["build_id"])
            self.assertEqual(active["build"]["status"], "running")
            self.assertEqual(heartbeat["heartbeat"]["active_image_builds"], 1)
            self.assertEqual(finished["status"], "succeeded")
            self.assertIn("building layer", finished["log_tail"])
            self.assertEqual(executor.commands, [("docker", "build", "-f", "/tmp/context/Dockerfile", "-t", "local/python-base:latest", "--label", "ucloud-sandboxes.image=true", "--label", "ucloud-sandboxes.image-id=python-base", "/tmp/context")])
            self.assertEqual(images["images"][0]["id"], "python-base")

    def test_regular_node_rejects_image_builds(self) -> None:
        with TemporaryDirectory() as raw_dir:
            sandbox_file = Path(raw_dir) / "sandboxes.json"
            image_file = Path(raw_dir) / "images.json"
            server = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=sandbox_file,
                image_file=image_file,
                job_id="job-1",
                node_id="node-1",
                runtime=DockerGvisorRuntime(dry_run=True),
            )
            thread = Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address
                base = f"http://{host}:{port}"
                heartbeat = self._json_request(f"{base}/v1/heartbeat")
                result = self._json_request(
                    f"{base}/v1/images/build",
                    method="POST",
                    payload={
                        "id": "base",
                        "tag": "local/base:latest",
                        "context_path": "/tmp/context",
                    },
                    allow_error=True,
                )
            finally:
                server.shutdown()
                server.server_close()

            self.assertEqual(heartbeat["heartbeat"]["capabilities"], ["sandbox", "image-cache"])
            self.assertEqual(result["status"], 403)

    def test_node_heartbeat_includes_extra_security_capabilities(self) -> None:
        with TemporaryDirectory() as raw_dir:
            sandbox_file = Path(raw_dir) / "sandboxes.json"
            image_file = Path(raw_dir) / "images.json"
            server = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=sandbox_file,
                image_file=image_file,
                job_id="job-1",
                node_id="node-1",
                runtime=DockerGvisorRuntime(dry_run=True),
                extra_capabilities=("disk-quota",),
            )
            thread = Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address
                heartbeat = self._json_request(f"http://{host}:{port}/v1/heartbeat")
            finally:
                server.shutdown()
                server.server_close()

            self.assertEqual(
                heartbeat["heartbeat"]["capabilities"],
                ["sandbox", "image-cache", "disk-quota"],
            )

    def test_rejects_disk_request_without_validated_quota_runtime(self) -> None:
        with TemporaryDirectory() as raw_dir:
            sandbox_file = Path(raw_dir) / "sandboxes.json"
            image_file = Path(raw_dir) / "images.json"
            server = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=sandbox_file,
                image_file=image_file,
                job_id="job-1",
                node_id="node-1",
                runtime=DockerGvisorRuntime(dry_run=True),
            )
            thread = Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address
                result = self._json_request(
                    f"http://{host}:{port}/v1/sandboxes",
                    method="POST",
                    payload={
                        "id": "disk-denied",
                        "image": "busybox",
                        "disk_mb": 16,
                    },
                    allow_error=True,
                )
            finally:
                server.shutdown()
                server.server_close()

            self.assertEqual(result["status"], 400)
            self.assertIn("validated Docker storage quota", result["error"])

    def test_exec_session_over_http(self) -> None:
        with TemporaryDirectory() as raw_dir:
            sandbox_file = Path(raw_dir) / "sandboxes.json"
            image_file = Path(raw_dir) / "images.json"
            server = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=sandbox_file,
                image_file=image_file,
                job_id="job-1",
                node_id="node-1",
                runtime=DockerGvisorRuntime(dry_run=True),
            )
            thread = Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address
                base = f"http://{host}:{port}"
                self._json_request(
                    f"{base}/v1/sandboxes",
                    method="POST",
                    payload={"id": "sbx-1", "image": "busybox", "memory_mb": 128},
                )
                started = self._json_request(
                    f"{base}/v1/sandboxes/sbx-1/exec",
                    method="POST",
                    payload={"command": ["echo", "ok"]},
                )
                session_id = started["session"]["id"]
                events = self._json_request(f"{base}/v1/exec/{session_id}/events")
            finally:
                server.shutdown()
                server.server_close()

            self.assertEqual(started["session"]["status"], "exited")
            self.assertEqual(events["session"]["exit_code"], 0)
            self.assertEqual(
                [event["stream"] for event in events["events"]],
                ["status", "status", "exit"],
            )

    def test_sandbox_ssh_target_over_http(self) -> None:
        with TemporaryDirectory() as raw_dir:
            sandbox_file = Path(raw_dir) / "sandboxes.json"
            image_file = Path(raw_dir) / "images.json"
            server = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=sandbox_file,
                image_file=image_file,
                job_id="job-1",
                node_id="node-1",
                runtime=DockerGvisorRuntime(dry_run=True),
                ssh_port_range=(23000, 23001),
            )
            thread = Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address
                base = f"http://{host}:{port}"
                self._json_request(
                    f"{base}/v1/sandboxes",
                    method="POST",
                    payload={
                        "id": "ssh-one",
                        "image": "sandbox-ssh:latest",
                        "memory_mb": 128,
                        "network": "bridge",
                        "ssh": {"enabled": True, "user": "sandbox"},
                    },
                )
                target = self._json_request(f"{base}/v1/sandboxes/ssh-one/ssh")
            finally:
                server.shutdown()
                server.server_close()

            self.assertEqual(target["ssh"]["port"], 23000)
            self.assertEqual(target["ssh"]["command"], "ssh -p 23000 sandbox@127.0.0.1")

    def test_file_upload_and_download_over_http(self) -> None:
        with TemporaryDirectory() as raw_dir:
            sandbox_file = Path(raw_dir) / "sandboxes.json"
            image_file = Path(raw_dir) / "images.json"
            runtime = FileRuntime()
            server = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=sandbox_file,
                image_file=image_file,
                job_id="job-1",
                node_id="node-1",
                runtime=runtime,
            )
            thread = Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address
                base = f"http://{host}:{port}"
                self._json_request(
                    f"{base}/v1/sandboxes",
                    method="POST",
                    payload={"id": "sbx-1", "image": "busybox", "memory_mb": 128},
                )
                uploaded = self._bytes_request(
                    f"{base}/v1/sandboxes/sbx-1/files?path={quote('/workspace/a.txt')}",
                    method="PUT",
                    body=b"hello file\n",
                )
                downloaded = self._bytes_request(
                    f"{base}/v1/sandboxes/sbx-1/files?path={quote('/workspace/a.txt')}",
                )
                bad_path = self._json_request(
                    f"{base}/v1/sandboxes/sbx-1/files?path={quote('/workspace/')}",
                    method="PUT",
                    payload={},
                    allow_error=True,
                )
            finally:
                server.shutdown()
                server.server_close()

        self.assertEqual(uploaded["json"]["size"], 11)
        self.assertEqual(downloaded["body"], b"hello file\n")
        self.assertEqual(downloaded["headers"]["X-Sandbox-Path"], "/workspace/a.txt")
        self.assertEqual(bad_path["status"], 400)
        self.assertIn("must identify a file", bad_path["error"])

    def test_async_gateway_exec_handle_reads_events(self) -> None:
        async def scenario(base: str) -> list[str]:
            client = NodeGatewayClient(base)
            handle = await client.start_exec(
                "sbx-1",
                SandboxExecSpec(sandbox_id="sbx-1", command=("echo", "ok")),
            )
            events = []
            async for event in handle.events(wait_seconds=0.0):
                events.append(event["stream"])
            return events

        with TemporaryDirectory() as raw_dir:
            sandbox_file = Path(raw_dir) / "sandboxes.json"
            image_file = Path(raw_dir) / "images.json"
            server = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=sandbox_file,
                image_file=image_file,
                job_id="job-1",
                node_id="node-1",
                runtime=DockerGvisorRuntime(dry_run=True),
            )
            thread = Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address
                base = f"http://{host}:{port}"
                self._json_request(
                    f"{base}/v1/sandboxes",
                    method="POST",
                    payload={"id": "sbx-1", "image": "busybox", "memory_mb": 128},
                )
                streams = asyncio.run(scenario(base))
            finally:
                server.shutdown()
                server.server_close()

            self.assertEqual(streams, ["status", "status", "exit"])

    def _json_request(
        self,
        url: str,
        *,
        method: str = "GET",
        payload: dict | None = None,
        allow_error: bool = False,
    ) -> dict:
        body = None
        headers = {}
        if payload is not None:
            body = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"
        req = request.Request(url, data=body, method=method, headers=headers)
        try:
            with request.urlopen(req, timeout=5) as response:
                return json.loads(response.read().decode("utf-8"))
        except Exception as exc:
            if not allow_error or not hasattr(exc, "code"):
                raise
            body = json.loads(exc.read().decode("utf-8"))
            body["status"] = exc.code
            return body

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

    def _wait_for_build(self, base: str, image_id: str) -> dict:
        for _ in range(40):
            payload = self._json_request(f"{base}/v1/images/builds/{image_id}")
            build = payload["build"]
            if build["status"] in {"succeeded", "failed"}:
                return build
            asyncio.run(asyncio.sleep(0.05))
        raise AssertionError("image build did not finish")


class FileRuntime(DockerGvisorRuntime):
    def __init__(self) -> None:
        super().__init__(dry_run=True)
        self.files: dict[tuple[str, str], bytes] = {}

    def write_file_to_container(
        self,
        sandbox_id: str,
        container_path: str,
        content: bytes,
        *,
        owner: str | None = None,
    ):
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
    ):
        _, result = super().read_file_from_container(sandbox_id, container_path)
        return self.files[(sandbox_id, container_path)], result


class BlockingExecutor:
    def __init__(self) -> None:
        self.started = Event()
        self.release = Event()
        self.commands: list[tuple[str, ...]] = []

    def run(self, argv: tuple[str, ...], *, input: bytes | None = None) -> CommandResult:
        self.commands.append(argv)
        self.started.set()
        self.release.wait(5)
        return CommandResult(
            argv=argv,
            exit_code=0,
            stdout="building layer\n",
        )


if __name__ == "__main__":
    unittest.main()
