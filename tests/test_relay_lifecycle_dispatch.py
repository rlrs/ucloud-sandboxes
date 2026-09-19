import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextvars import ContextVar
from threading import Event
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from ucloud_sandboxes import cli


class RelayLifecycleDispatchTests(unittest.IsolatedAsyncioTestCase):
    async def test_park_wake_isolation_bounds_context_and_cancellation(self):
        loop = asyncio.get_running_loop()
        loop.set_default_executor(ThreadPoolExecutor(max_workers=1))
        trace = ContextVar("test_trace", default="missing")
        trace.set("request-trace")
        entered = {"park": 0, "wake": 0}
        ready = {action: asyncio.Event() for action in entered}
        release = {action: Event() for action in entered}
        limits = {"park": 16, "wake": 48}
        dispatcher = cli._RelayLifecycleDispatcher("http://gateway", "token")
        tasks = []

        def record(action):
            entered[action] += 1
            if entered[action] == limits[action]:
                ready[action].set()

        def post(_url, _token, _request, *, action):
            self.assertEqual(trace.get(), "request-trace")
            loop.call_soon_threadsafe(record, action)
            if not release[action].wait(10):
                raise TimeoutError("test lifecycle gate not released")
            return "epoch"

        with patch.object(cli, "_post_gateway_sandbox_lifecycle", side_effect=post):
            try:
                parks = [asyncio.create_task(dispatcher.notify(SimpleNamespace(), action="park"))
                         for _ in range(17)]
                tasks.extend(parks)
                await asyncio.wait_for(ready["park"].wait(), 3)
                parks[0].cancel()
                await asyncio.sleep(0.05)
                self.assertEqual(entered["park"], 16)
                self.assertFalse(parks[0].done())
                wakes = [asyncio.create_task(dispatcher.notify(SimpleNamespace(), action="wake"))
                         for _ in range(48)]
                tasks.extend(wakes)
                await asyncio.wait_for(ready["wake"].wait(), 3)
                release["wake"].set()
                self.assertEqual(await asyncio.wait_for(asyncio.gather(*wakes), 3), ["epoch"] * 48)
                self.assertEqual(entered["park"], 16)
                release["park"].set()
                outcomes = await asyncio.wait_for(asyncio.gather(*parks, return_exceptions=True), 3)
                self.assertIsInstance(outcomes[0], asyncio.CancelledError)
                self.assertEqual(outcomes[1:], ["epoch"] * 16)
            finally:
                for event in release.values():
                    event.set()
                await asyncio.gather(*tasks, return_exceptions=True)
                await dispatcher.close()
        for pool in dispatcher._pools.values():
            self.assertTrue(all(not thread.is_alive() for thread in pool._threads))
        with self.assertRaisesRegex(RuntimeError, "closed"):
            await dispatcher.notify(SimpleNamespace(), action="wake")

    async def test_lifecycle_failure_releases_slot_and_propagates(self):
        dispatcher = cli._RelayLifecycleDispatcher("http://gateway", "token")
        try:
            with patch.object(cli, "_post_gateway_sandbox_lifecycle", side_effect=[ValueError("failed"), "epoch"]):
                with self.assertRaisesRegex(ValueError, "failed"):
                    await dispatcher.notify(SimpleNamespace(), action="wake")
                self.assertEqual(await dispatcher.notify(SimpleNamespace(), action="wake"), "epoch")
        finally:
            await dispatcher.close()
