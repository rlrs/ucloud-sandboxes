import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextvars import ContextVar
from threading import Event
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from aiohttp import web
from aiohttp.test_utils import TestServer

from ucloud_sandboxes import cli


class RelayLifecycleDispatchTests(unittest.IsolatedAsyncioTestCase):
    async def test_completed_response_bypasses_queued_park_without_waiting_for_slot(self):
        dispatcher = cli._RelayLifecycleDispatcher("http://gateway", "token")
        dispatcher._slots["park"] = asyncio.Semaphore(1)
        await dispatcher._slots["park"].acquire()
        request = SimpleNamespace(completed_at=None, response_committed=asyncio.Event())
        with patch.object(cli, "_post_gateway_sandbox_lifecycle_once", return_value="epoch") as post:
            task = asyncio.create_task(dispatcher.notify(request, action="park"))
            try:
                await asyncio.sleep(0.01)
                request.completed_at = cli.time.time()
                request.response_committed.set()
                self.assertIsNone(await asyncio.wait_for(task, .5))
                post.assert_not_called()
                self.assertTrue(dispatcher._slots["park"].locked())
                dispatcher._slots["park"].release()
                other = SimpleNamespace(completed_at=None, response_committed=asyncio.Event())
                self.assertEqual(await dispatcher.notify(other, action="park"), "epoch")
                self.assertEqual(dispatcher._slots["park"]._value, 1)
            finally:
                await dispatcher.close()

    async def test_response_and_park_admission_race_returns_slot(self):
        dispatcher = cli._RelayLifecycleDispatcher("http://gateway", "token")
        request = SimpleNamespace(completed_at=None, response_committed=asyncio.Event())
        request.response_committed.set()
        try:
            self.assertFalse(await dispatcher._acquire_park_slot(request))
            self.assertEqual(dispatcher._slots["park"]._value, 16)
        finally:
            await dispatcher.close()

    async def test_completed_response_interrupts_park_retry_backoff(self):
        dispatcher = cli._RelayLifecycleDispatcher("http://gateway", "token")
        attempted = asyncio.Event()
        loop = asyncio.get_running_loop()
        request = SimpleNamespace(completed_at=None, response_committed=asyncio.Event())

        def retry(*_args, **_kwargs):
            loop.call_soon_threadsafe(attempted.set)
            raise cli._RelayLifecycleRetry(5)

        with patch.object(cli, "_post_gateway_sandbox_lifecycle_once", side_effect=retry) as post:
            try:
                task = asyncio.create_task(dispatcher.notify(request, action="park"))
                await asyncio.wait_for(attempted.wait(), 1)
                request.completed_at = cli.time.time()
                request.response_committed.set()
                self.assertIsNone(await asyncio.wait_for(task, .5))
                self.assertEqual(post.call_count, 1)
            finally:
                await dispatcher.close()

    async def test_committed_response_does_not_abandon_inflight_park(self):
        dispatcher = cli._RelayLifecycleDispatcher("http://gateway", "token")
        loop = asyncio.get_running_loop()
        entered, release = asyncio.Event(), Event()
        request = SimpleNamespace(completed_at=None, response_committed=asyncio.Event())

        def post(*_args, **_kwargs):
            loop.call_soon_threadsafe(entered.set)
            if not release.wait(2):
                raise TimeoutError("test park was not released")
            return "parked-epoch"

        with patch.object(cli, "_post_gateway_sandbox_lifecycle_once", side_effect=post):
            task = asyncio.create_task(dispatcher.notify(request, action="park"))
            try:
                await asyncio.wait_for(entered.wait(), 1)
                request.completed_at = cli.time.time()
                request.response_committed.set()
                await asyncio.sleep(.01)
                self.assertFalse(task.done())
                release.set()
                self.assertEqual(await task, "parked-epoch")
            finally:
                release.set()
                await dispatcher.close()

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

        def post(_url, _token, _request, *, action, attempt, deadline):
            self.assertEqual(trace.get(), "request-trace")
            loop.call_soon_threadsafe(record, action)
            if not release[action].wait(10):
                raise TimeoutError("test lifecycle gate not released")
            return "epoch"

        with patch.object(cli, "_post_gateway_sandbox_lifecycle_once", side_effect=post):
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
            with patch.object(cli, "_post_gateway_sandbox_lifecycle_once", side_effect=[ValueError("failed"), "epoch"]):
                with self.assertRaisesRegex(ValueError, "failed"):
                    await dispatcher.notify(SimpleNamespace(), action="wake")
                self.assertEqual(await dispatcher.notify(SimpleNamespace(), action="wake"), "epoch")
        finally:
            await dispatcher.close()

    async def test_capacity_backoff_releases_slots_and_preserves_retry_identity(self):
        dispatcher = cli._RelayLifecycleDispatcher("http://gateway", "token")
        sleeping = asyncio.Event()
        resume = asyncio.Event()
        waiting = 0
        attempts = {}

        async def backoff(delay):
            nonlocal waiting
            self.assertEqual(delay, 1.0)
            waiting += 1
            if waiting == 64:
                sleeping.set()
            await resume.wait()

        def post(_url, _token, request, *, action, attempt, deadline):
            calls = attempts.setdefault(request.request_id, [])
            calls.append((attempt, deadline))
            if request.request_id != "ready" and attempt == 0:
                raise cli._RelayLifecycleRetry(1.0)
            return "epoch"

        tasks = []
        with (
            patch.object(cli, "_post_gateway_sandbox_lifecycle_once", side_effect=post),
            patch.object(cli.asyncio, "sleep", side_effect=backoff),
        ):
            try:
                # More blocked requests than the entire wake dispatch pool.
                tasks = [asyncio.create_task(dispatcher.notify(
                    SimpleNamespace(request_id=str(index)), action="wake",
                )) for index in range(64)]
                await asyncio.wait_for(sleeping.wait(), 3)
                ready = await asyncio.wait_for(dispatcher.notify(
                    SimpleNamespace(request_id="ready"), action="wake",
                ), 3)
                self.assertEqual(ready, "epoch")
                tasks[0].cancel()
                self.assertFalse(tasks[0].done())
            finally:
                resume.set()
                results = await asyncio.gather(*tasks, return_exceptions=True)
                await dispatcher.close()
        self.assertIsInstance(results[0], asyncio.CancelledError)
        self.assertEqual(results[1:], ["epoch"] * 63)
        for index in range(64):
            calls = attempts[str(index)]
            self.assertEqual([call[0] for call in calls], [0, 1])
            self.assertEqual(calls[0][1], calls[1][1])

    async def test_expired_wake_never_sends_http_after_dispatch_queue(self):
        dispatcher = cli._RelayLifecycleDispatcher("http://gateway", "token")
        # Exhaust dispatch capacity; expiry must include time spent queued.
        for _ in range(48):
            await dispatcher._slots["wake"].acquire()
        try:
            with patch.object(cli, "_post_gateway_sandbox_lifecycle_once") as post:
                with self.assertRaises(TimeoutError):
                    await asyncio.wait_for(dispatcher.notify(SimpleNamespace(
                        expires_at=cli.time.time() + 0.05,
                    ), action="wake"), 3)
                post.assert_not_called()
        finally:
            for _ in range(48):
                dispatcher._slots["wake"].release()
            await dispatcher.close()

    async def test_shutdown_drains_async_backoff_without_another_attempt(self):
        dispatcher = cli._RelayLifecycleDispatcher("http://gateway", "token")
        sleeping, resume = asyncio.Event(), asyncio.Event()

        async def backoff(_delay):
            sleeping.set()
            await resume.wait()

        with (
            patch.object(cli, "_post_gateway_sandbox_lifecycle_once",
                         side_effect=cli._RelayLifecycleRetry(1)) as post,
            patch.object(cli.asyncio, "sleep", side_effect=backoff),
        ):
            task = asyncio.create_task(dispatcher.notify(SimpleNamespace(), action="wake"))
            try:
                await asyncio.wait_for(sleeping.wait(), 3)
            finally:
                close = asyncio.create_task(dispatcher.close())
                resume.set()
                result = await asyncio.gather(task, return_exceptions=True)
                await asyncio.wait_for(close, 3)
            self.assertIsInstance(result[0], RuntimeError)
            self.assertIn("closed", str(result[0]))
            self.assertEqual(post.call_count, 1)
            self.assertFalse(dispatcher._active)

    async def test_real_http_backpressure_preserves_operation_and_authentication(self):
        blocked = asyncio.Event()
        calls = {}

        async def wake(http_request):
            self.assertEqual(http_request.headers["Authorization"], "Bearer token")
            body = await http_request.json()
            request_id = body["request_id"]
            previous = calls.setdefault(request_id, [])
            previous.append(body)
            if request_id != "ready" and len(previous) == 1:
                if len(calls) == 2:
                    blocked.set()
                return web.json_response(
                    {"retryable": True, "error_code": "node_restore_busy"},
                    status=503, headers={"Retry-After": "1"},
                )
            return web.json_response({}, headers={
                "X-UCloud-Sandbox-Transport-Epoch": "epoch-restored",
            })

        def request(request_id):
            return SimpleNamespace(
                request_id=request_id, sandbox_id=request_id, sandbox_generation=7,
                rollout_id="rollout", created_at=cli.time.time(),
            )

        app = web.Application()
        app.router.add_post("/v1/sandboxes/{sandbox}/wake", wake)
        async with TestServer(app) as server:
            dispatcher = cli._RelayLifecycleDispatcher(str(server.make_url("/")), "token")
            dispatcher._slots["wake"] = asyncio.Semaphore(2)
            tasks = [asyncio.create_task(dispatcher.notify(request(str(index)), action="wake"))
                     for index in range(2)]
            try:
                await asyncio.wait_for(blocked.wait(), 3)
                ready = await asyncio.wait_for(
                    dispatcher.notify(request("ready"), action="wake"), 0.75,
                )
                self.assertEqual(ready, "epoch-restored")
                self.assertEqual(await asyncio.wait_for(asyncio.gather(*tasks), 3),
                                 ["epoch-restored"] * 2)
            finally:
                await asyncio.gather(*tasks, return_exceptions=True)
                await dispatcher.close()
        for request_id in ("0", "1"):
            self.assertEqual(len(calls[request_id]), 2)
            self.assertEqual(calls[request_id][0], calls[request_id][1])
            self.assertEqual(calls[request_id][0]["operation_id"], f"relay-wake:{request_id}")
            self.assertEqual(calls[request_id][0]["generation"], 7)
