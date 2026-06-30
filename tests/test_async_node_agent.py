import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from aiohttp import ClientSession, web

from ucloud_sandboxes.async_gateway import AsyncNodeGatewayClient
from ucloud_sandboxes.async_node_agent import create_async_node_agent_app
from ucloud_sandboxes.sandbox import DockerGvisorRuntime
from ucloud_sandboxes.sandbox_exec import SandboxExecSpec


class AsyncNodeAgentTests(unittest.TestCase):
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
