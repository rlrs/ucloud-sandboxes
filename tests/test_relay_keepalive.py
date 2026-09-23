import asyncio
import unittest

from aiohttp import ClientSession, web

from ucloud_sandboxes.model_relay import create_model_relay_app
from tests.postgres_fixture import postgres_database


class RelayKeepaliveTests(unittest.IsolatedAsyncioTestCase):
    async def test_idle_connections_close_without_interrupting_active_requests(self):
        async with postgres_database() as database:
            app = create_model_relay_app(
                sandbox_bearer_token="sandbox-token",
                worker_bearer_token="worker-token",
                postgres_store=database,
            )
            started = asyncio.Event()
            release = asyncio.Event()

            async def held_request(_request):
                started.set()
                await release.wait()
                return web.Response(text="complete")

            app.router.add_get("/held", held_request)
            runner = web.AppRunner(app)
            await runner.setup()
            site = web.TCPSite(runner, "127.0.0.1", 0)
            await site.start()
            port = site._server.sockets[0].getsockname()[1]
            writer = None
            try:
                async with ClientSession() as client:
                    active = asyncio.create_task(
                        client.get(f"http://127.0.0.1:{port}/held")
                    )
                    await asyncio.wait_for(started.wait(), 2)
                    reader, writer = await asyncio.open_connection("127.0.0.1", port)
                    writer.write(b"GET /healthz HTTP/1.1\r\nHost: localhost\r\n\r\n")
                    await writer.drain()
                    headers = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 2)
                    self.assertIn(b"200 OK", headers)
                    length = next(
                        int(line.split(b":", 1)[1])
                        for line in headers.split(b"\r\n")
                        if line.lower().startswith(b"content-length:")
                    )
                    await reader.readexactly(length)
                    self.assertEqual(await asyncio.wait_for(reader.read(), 7), b"")
                    self.assertFalse(
                        active.done(), "active requests must outlive idle expiry"
                    )
                    release.set()
                    response = await asyncio.wait_for(active, 2)
                    async with response:
                        self.assertEqual(await response.text(), "complete")
            finally:
                release.set()
                if writer is not None:
                    writer.close()
                    await writer.wait_closed()
                await runner.cleanup()
