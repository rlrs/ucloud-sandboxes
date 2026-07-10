import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from aiohttp import ClientSession, web

from ucloud_sandboxes.async_gateway import AsyncNodeGatewayClient
from ucloud_sandboxes.async_node_agent import (
    IMAGE_MANAGER_KEY,
    create_async_node_agent_app,
)
from ucloud_sandboxes.deployment import package_version
from ucloud_sandboxes.images import ImageBuildSpec
from ucloud_sandboxes.models import ResourceQuantity
from ucloud_sandboxes.node_agent import (
    SANDBOX_GENERATION_HEADER,
    SANDBOX_OPERATION_ID_HEADER,
)
from ucloud_sandboxes.sandbox import (
    DockerGvisorRuntime,
    SandboxAdmissionClosedError,
    SandboxSpec,
    sandbox_spec_fingerprint,
)
from ucloud_sandboxes.sandbox_exec import SandboxExecSpec


class AsyncNodeAgentTests(unittest.TestCase):
    def test_node_control_auth_protects_http_and_client_calls(self) -> None:
        async def scenario() -> None:
            with TemporaryDirectory() as raw_dir:
                app = create_async_node_agent_app(
                    sandbox_file=Path(raw_dir) / "sandboxes.json",
                    image_file=Path(raw_dir) / "images.json",
                    runtime=DockerGvisorRuntime(dry_run=True),
                    node_control_bearer_token="node-secret",
                )
                runner = web.AppRunner(app)
                await runner.setup()
                site = web.TCPSite(runner, "127.0.0.1", 0)
                await site.start()
                sockets = site._server.sockets if site._server else []
                port = sockets[0].getsockname()[1]
                base = f"http://127.0.0.1:{port}"
                try:
                    async with ClientSession() as client:
                        async with client.get(f"{base}/healthz") as response:
                            self.assertEqual(response.status, 200)
                        async with client.get(f"{base}/v1/sandboxes") as response:
                            self.assertEqual(response.status, 401)
                        async with client.get(
                            f"{base}/v1/sandboxes",
                            headers={"X-UCloud-Sandbox-Token": "node-secret"},
                        ) as response:
                            self.assertEqual(response.status, 401)
                        async with client.get(
                            f"{base}/v1/sandboxes",
                            headers={"Authorization": "Bearer node-secret"},
                        ) as response:
                            self.assertEqual(response.status, 200)
                        async with client.post(
                            f"{base}/v1/sandboxes",
                            json={
                                "id": "authorized",
                                "image": "busybox",
                                "memory_mb": 64,
                            },
                            headers={"Authorization": "Bearer node-secret"},
                        ) as response:
                            self.assertEqual(response.status, 201)

                    async with AsyncNodeGatewayClient(
                        base,
                        node_control_bearer_token="node-secret",
                    ) as gateway:
                        created = await gateway.start_exec(
                            "authorized",
                            SandboxExecSpec(
                                sandbox_id="authorized",
                                command=("true",),
                            ),
                        )
                        self.assertIn("session", created)
                finally:
                    await runner.cleanup()

        asyncio.run(scenario())

    def test_node_control_auth_rejects_empty_configured_token(self) -> None:
        with TemporaryDirectory() as raw_dir:
            with self.assertRaisesRegex(ValueError, "cannot be empty"):
                create_async_node_agent_app(
                    sandbox_file=Path(raw_dir) / "sandboxes.json",
                    image_file=Path(raw_dir) / "images.json",
                    node_control_bearer_token="",
                )

    def test_create_capacity_uses_total_resources(self) -> None:
        async def scenario() -> None:
            with TemporaryDirectory() as raw_dir:
                app = create_async_node_agent_app(
                    sandbox_file=Path(raw_dir) / "sandboxes.json",
                    image_file=Path(raw_dir) / "images.json",
                    runtime=DockerGvisorRuntime(dry_run=True),
                    total_resources=ResourceQuantity(memory_mb=128),
                )
                runner = web.AppRunner(app)
                await runner.setup()
                site = web.TCPSite(runner, "127.0.0.1", 0)
                await site.start()
                sockets = site._server.sockets if site._server else []
                port = sockets[0].getsockname()[1]
                base = f"http://127.0.0.1:{port}"
                payload = {"id": "fills-node", "image": "busybox", "memory_mb": 128}
                try:
                    async with ClientSession() as client:
                        async with client.post(
                            f"{base}/v1/sandboxes", json=payload
                        ) as response:
                            self.assertEqual(response.status, 201)
                        async with client.post(
                            f"{base}/v1/sandboxes", json=payload
                        ) as response:
                            self.assertEqual(response.status, 200)
                        async with client.post(
                            f"{base}/v1/sandboxes",
                            json={
                                "id": "one-too-many",
                                "image": "busybox",
                                "memory_mb": 1,
                            },
                        ) as response:
                            self.assertEqual(response.status, 503)
                            self.assertIn("exhausted memory_mb", await response.text())
                finally:
                    await runner.cleanup()

        asyncio.run(scenario())

    def test_generation_and_drain_safety_matches_sync_agent(self) -> None:
        async def scenario() -> None:
            with TemporaryDirectory() as raw_dir:
                app = create_async_node_agent_app(
                    sandbox_file=Path(raw_dir) / "sandboxes.json",
                    image_file=Path(raw_dir) / "images.json",
                    runtime=DockerGvisorRuntime(dry_run=True),
                )
                runner = web.AppRunner(app)
                await runner.setup()
                site = web.TCPSite(runner, "127.0.0.1", 0)
                await site.start()
                sockets = site._server.sockets if site._server else []
                port = sockets[0].getsockname()[1]
                base = f"http://127.0.0.1:{port}"
                spec_payload = {
                    "id": "versioned",
                    "image": "busybox",
                    "memory_mb": 128,
                }
                spec_hash = sandbox_spec_fingerprint(
                    SandboxSpec.from_dict(spec_payload)
                )
                create_payload = {
                    **spec_payload,
                    "_ucloud_operation": {
                        "operation_id": "create-1",
                        "generation": 1,
                        "kind": "create",
                        "spec_hash": spec_hash,
                    },
                }
                try:
                    async with ClientSession() as client:
                        async with client.post(
                            f"{base}/v1/sandboxes", json=create_payload
                        ) as response:
                            self.assertEqual(response.status, 201)
                            created = await response.json()
                        async with client.post(
                            f"{base}/v1/sandboxes", json=create_payload
                        ) as response:
                            self.assertEqual(response.status, 200)
                        conflicting = {
                            **create_payload,
                            "_ucloud_operation": {
                                **create_payload["_ucloud_operation"],
                                "operation_id": "other-create",
                            },
                        }
                        async with client.post(
                            f"{base}/v1/sandboxes", json=conflicting
                        ) as response:
                            self.assertEqual(response.status, 409)
                        async with client.post(
                            f"{base}/v1/drain",
                            json={"token": "drain-async", "draining": True},
                        ) as response:
                            self.assertEqual(response.status, 200)
                            draining = (await response.json())["drain"]
                        async with client.post(
                            f"{base}/v1/sandboxes",
                            json={"id": "blocked", "image": "busybox", "memory_mb": 64},
                        ) as response:
                            self.assertEqual(response.status, 503)
                        async with client.delete(
                            f"{base}/v1/sandboxes/versioned"
                        ) as response:
                            self.assertEqual(response.status, 409)
                        async with client.delete(
                            f"{base}/v1/sandboxes/versioned",
                            headers={
                                SANDBOX_GENERATION_HEADER: "1",
                                SANDBOX_OPERATION_ID_HEADER: "delete-1",
                            },
                        ) as response:
                            self.assertEqual(response.status, 200)
                        async with client.post(
                            f"{base}/v1/drain",
                            json={"token": "drain-async", "draining": True},
                        ) as response:
                            ready = (await response.json())["drain"]
                        with self.assertRaises(SandboxAdmissionClosedError):
                            await asyncio.to_thread(
                                app[IMAGE_MANAGER_KEY].start_build,
                                ImageBuildSpec(
                                    id="blocked-image",
                                    tag="local/blocked:latest",
                                    context_path="/tmp/context",
                                ),
                            )
                        async with client.post(
                            f"{base}/v1/drain",
                            json={"token": "drain-async", "draining": False},
                        ) as response:
                            opened = (await response.json())["drain"]
                        async with client.post(
                            f"{base}/v1/sandboxes",
                            json={"id": "accepted", "image": "busybox", "memory_mb": 64},
                        ) as response:
                            self.assertEqual(response.status, 201)
                finally:
                    await runner.cleanup()

                self.assertEqual(created["sandbox"]["generation"], 1)
                self.assertEqual(created["sandbox"]["operation_id"], "create-1")
                self.assertFalse(draining["ready"])
                self.assertTrue(ready["ready"])
                self.assertFalse(opened["draining"])
                self.assertTrue(opened["admission_open"])

        asyncio.run(scenario())

    def test_healthz_reports_service_version(self) -> None:
        async def scenario() -> dict:
            with TemporaryDirectory() as raw_dir:
                app = create_async_node_agent_app(
                    sandbox_file=Path(raw_dir) / "sandboxes.json",
                    image_file=Path(raw_dir) / "images.json",
                    runtime=DockerGvisorRuntime(dry_run=True),
                )
                runner = web.AppRunner(app)
                await runner.setup()
                site = web.TCPSite(runner, "127.0.0.1", 0)
                await site.start()
                sockets = site._server.sockets if site._server else []
                port = sockets[0].getsockname()[1]
                try:
                    async with ClientSession() as client:
                        async with client.get(f"http://127.0.0.1:{port}/healthz") as response:
                            self.assertEqual(response.status, 200)
                            return await response.json()
                finally:
                    await runner.cleanup()

        self.assertEqual(
            asyncio.run(scenario()),
            {
                "ok": True,
                "service": "async-node-agent",
                "version": package_version(),
            },
        )

    def test_exec_websocket_streams_events(self) -> None:
        async def scenario() -> list[str]:
            with TemporaryDirectory() as raw_dir:
                app = create_async_node_agent_app(
                    sandbox_file=Path(raw_dir) / "sandboxes.json",
                    image_file=Path(raw_dir) / "images.json",
                    runtime=DockerGvisorRuntime(dry_run=True),
                )
                runner = web.AppRunner(app)
                await runner.setup()
                site = web.TCPSite(runner, "127.0.0.1", 0)
                await site.start()
                sockets = site._server.sockets if site._server else []
                port = sockets[0].getsockname()[1]
                base = f"http://127.0.0.1:{port}"
                try:
                    async with ClientSession() as raw_client:
                        async with raw_client.post(
                            f"{base}/v1/sandboxes",
                            json={"id": "sbx-1", "image": "busybox", "memory_mb": 128},
                        ) as response:
                            self.assertEqual(response.status, 201)
                    async with AsyncNodeGatewayClient(base) as client:
                        stream = await client.open_exec_stream(
                            "sbx-1",
                            SandboxExecSpec(
                                sandbox_id="sbx-1",
                                command=("echo", "ok"),
                            ),
                        )
                        events = []
                        async with stream:
                            async for event in stream.events():
                                events.append(event["type"])
                        return events
                finally:
                    await runner.cleanup()

        self.assertEqual(
            asyncio.run(scenario()),
            ["session", "status", "status", "exit"],
        )


if __name__ == "__main__":
    unittest.main()
