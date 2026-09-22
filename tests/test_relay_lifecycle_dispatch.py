import asyncio
from contextvars import ContextVar
from threading import Event
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from aiohttp import web
from aiohttp import ClientSession
from aiohttp.test_utils import TestServer

from ucloud_sandboxes import cli


class RelayLifecycleDispatchTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_transport_preserves_failure_fences_and_rejects_redirects(self):
        cases = {
            'missing': (404, {}, cli.RelayCallerUnavailable),
            'lost': (410, {}, cli.RelayCallerUnavailable),
            'obsolete': (409, {'retryable': False}, cli.RelayCallerUnavailable),
            'busy': (409, {}, cli._RelayLifecycleRetry),
            'capacity': (503, {'retryable': True}, cli._RelayLifecycleRetry),
            'overload': (429, {'retryable': True}, cli._RelayLifecycleRetry),
            'failure': (503, {}, cli.HTTPError),
            'redirect': (307, {}, cli.HTTPError),
        }
        seen = []

        async def reply(request):
            name = request.match_info['sandbox']
            seen.append(name)
            status, body, _error = cases[name]
            return web.json_response(body, status=status, headers={'Location': '/unexpected'})

        app = web.Application()
        app.router.add_post('/v1/sandboxes/{sandbox}/wake', reply)
        async with TestServer(app) as server, ClientSession() as session:
            for name, (_status, _body, error) in cases.items():
                request = SimpleNamespace(sandbox_id=name, sandbox_generation=7,
                                          request_id=name, rollout_id='r', created_at=0)
                with self.subTest(name=name), self.assertRaises(error):
                    await cli._post_gateway_sandbox_lifecycle_once_async(
                        session, str(server.make_url('/')), 'token', request,
                        action='wake', attempt=0, deadline=cli.time.monotonic()+30,
                    )
        self.assertEqual(seen, list(cases))

    async def test_async_transport_bounds_streamed_bodies_and_actual_http_deadline(self):
        release = asyncio.Event()

        async def reply(request):
            if request.match_info['sandbox'] == 'slow':
                await release.wait()
                return web.json_response({})
            response = web.StreamResponse()
            await response.prepare(request)
            try:
                for _ in range(18):
                    await response.write(b'x' * 65536)
            except ConnectionResetError:
                pass
            return response

        app = web.Application()
        app.router.add_post('/v1/sandboxes/{sandbox}/wake', reply)
        async with TestServer(app) as server, ClientSession() as session:
            try:
                for name, error, seconds in [('large', ValueError, 5), ('slow', asyncio.TimeoutError, .03)]:
                    request = SimpleNamespace(sandbox_id=name, sandbox_generation=7,
                                              request_id=name, rollout_id='r', created_at=0)
                    with self.subTest(name=name), self.assertRaises(error):
                        await cli._post_gateway_sandbox_lifecycle_once_async(
                            session, str(server.make_url('/')), 'token', request,
                            action='wake', attempt=0, deadline=cli.time.monotonic()+seconds,
                        )
            finally:
                release.set()

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

    async def test_park_threads_do_not_limit_async_wakes_or_lose_context(self):
        trace = ContextVar('test_trace', default='missing')
        trace.set('request-trace')
        dispatcher = cli._RelayLifecycleDispatcher('http://gateway', 'token')
        entered = asyncio.Event()
        release = asyncio.Event()
        count = 0

        async def post(_session, _url, _token, _request, **_kwargs):
            nonlocal count
            self.assertEqual(trace.get(), 'request-trace')
            count += 1
            if count == 256:
                entered.set()
            await release.wait()
            return 'epoch'

        # Exhaust checkpoint dispatch. Every wake can still reach HTTP without
        # waiting for the old 48-thread fleet-wide limit or default executor.
        for _ in range(16):
            await dispatcher._slots['park'].acquire()
        with patch.object(cli, '_post_gateway_sandbox_lifecycle_once_async', side_effect=post):
            tasks = [asyncio.create_task(dispatcher.notify(SimpleNamespace(), action='wake'))
                     for _ in range(256)]
            try:
                await asyncio.wait_for(entered.wait(), 3)
                tasks[0].cancel()
                await asyncio.sleep(.01)
                self.assertFalse(tasks[0].done())
            finally:
                release.set()
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for _ in range(16):
                    dispatcher._slots['park'].release()
                await dispatcher.close()
        self.assertIsInstance(results[0], asyncio.CancelledError)
        self.assertEqual(results[1:], ['epoch'] * 255)
        self.assertTrue(dispatcher._wake_session.closed)
        self.assertEqual(set(dispatcher._pools), {'park'})
        with self.assertRaisesRegex(RuntimeError, 'closed'):
            await dispatcher.notify(SimpleNamespace(), action='wake')

    async def test_lifecycle_failure_releases_slot_and_propagates(self):
        dispatcher = cli._RelayLifecycleDispatcher("http://gateway", "token")
        try:
            with patch.object(cli, "_post_gateway_sandbox_lifecycle_once_async", side_effect=[ValueError("failed"), "epoch"]):
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

        def post(_session, _url, _token, request, *, action, attempt, deadline):
            calls = attempts.setdefault(request.request_id, [])
            calls.append((attempt, deadline))
            if request.request_id != "ready" and attempt == 0:
                raise cli._RelayLifecycleRetry(1.0)
            return "epoch"

        tasks = []
        with (
            patch.object(cli, "_post_gateway_sandbox_lifecycle_once_async", side_effect=post),
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

    async def test_expired_wake_never_sends_http(self):
        dispatcher = cli._RelayLifecycleDispatcher('http://gateway', 'token')
        try:
            with patch.object(cli, '_post_gateway_sandbox_lifecycle_once_async') as post:
                with self.assertRaises(TimeoutError):
                    await dispatcher.notify(SimpleNamespace(expires_at=cli.time.time() - 1), action='wake')
                post.assert_not_called()
        finally:
            await dispatcher.close()

    async def test_shutdown_drains_async_backoff_without_another_attempt(self):
        dispatcher = cli._RelayLifecycleDispatcher("http://gateway", "token")
        sleeping, resume = asyncio.Event(), asyncio.Event()

        async def backoff(_delay):
            sleeping.set()
            await resume.wait()

        with (
            patch.object(cli, "_post_gateway_sandbox_lifecycle_once_async",
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
