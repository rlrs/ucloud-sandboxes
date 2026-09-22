import asyncio
from contextvars import ContextVar
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

    async def test_async_park_honors_worker_deferral_without_treating_it_as_success(self):
        async def reply(request):
            return web.json_response({"error_code": "park_deferred", "retryable": True,
                                      "retry_after_seconds": 30}, status=409,
                                     headers={"X-UCloud-Sandbox-Transport-Epoch": "original"})
        app = web.Application()
        app.router.add_post('/v1/sandboxes/s/park', reply)
        async with TestServer(app) as server, ClientSession() as session:
            with self.assertRaises(cli._RelayLifecycleRetry) as caught:
                await cli._post_gateway_sandbox_lifecycle_once_async(
                    session, str(server.make_url('/')), 'token',
                    SimpleNamespace(sandbox_id='s', sandbox_generation=1, request_id='r',
                                    rollout_id='rollout', created_at=0),
                    action='park', attempt=0, deadline=cli.time.monotonic()+30,
                )
            self.assertEqual(caught.exception.delay_seconds, 30)
            self.assertEqual(caught.exception.transport_epoch, "original")

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

    async def test_completed_response_skips_park(self):
        dispatcher = cli._RelayLifecycleDispatcher("http://gateway", "token")
        with patch.object(cli, "_post_gateway_sandbox_lifecycle_once_async") as post:
            try:
                self.assertIsNone(await dispatcher.notify(SimpleNamespace(completed_at=1), action="park"))
                post.assert_not_called()
            finally:
                await dispatcher.close()

    async def test_durable_deferral_releases_dispatch_without_sleep(self):
        from ucloud_sandboxes.model_relay import RelayLifecycleDeferred
        dispatcher = cli._RelayLifecycleDispatcher("http://gateway", "token")
        with patch.object(cli, "_post_gateway_sandbox_lifecycle_once_async",
                          side_effect=cli._RelayLifecycleRetry(30, transport_epoch="original")):
            try:
                with self.assertRaises(RelayLifecycleDeferred) as caught:
                    await asyncio.wait_for(dispatcher.notify(
                        SimpleNamespace(durable_lifecycle=True), action="park"), .5)
                self.assertEqual(caught.exception.seconds, 30)
                self.assertEqual(caught.exception.transport_epoch, "original")
                self.assertFalse(dispatcher._active)
            finally:
                await dispatcher.close()

    async def test_completed_response_interrupts_park_retry_backoff(self):
        dispatcher = cli._RelayLifecycleDispatcher("http://gateway", "token")
        attempted = asyncio.Event()
        request = SimpleNamespace(completed_at=None, response_committed=asyncio.Event())

        async def retry(*_args, **_kwargs):
            attempted.set()
            raise cli._RelayLifecycleRetry(5)

        with patch.object(cli, "_post_gateway_sandbox_lifecycle_once_async", side_effect=retry) as post:
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
        entered, release = asyncio.Event(), asyncio.Event()
        request = SimpleNamespace(completed_at=None, response_committed=asyncio.Event())

        async def post(*_args, **_kwargs):
            entered.set()
            await asyncio.wait_for(release.wait(), 2)
            return "parked-epoch"

        with patch.object(cli, "_post_gateway_sandbox_lifecycle_once_async", side_effect=post):
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
            if count == 512:
                entered.set()
            await release.wait()
            return 'epoch'

        with patch.object(cli, '_post_gateway_sandbox_lifecycle_once_async', side_effect=post):
            tasks = [asyncio.create_task(dispatcher.notify(SimpleNamespace(), action=action))
                     for action in ('park', 'wake') for _ in range(256)]
            try:
                await asyncio.wait_for(entered.wait(), 3)
                tasks[0].cancel()
                await asyncio.sleep(.01)
                self.assertFalse(tasks[0].done())
            finally:
                release.set()
                results = await asyncio.gather(*tasks, return_exceptions=True)
                await dispatcher.close()
        self.assertIsInstance(results[0], asyncio.CancelledError)
        self.assertEqual(results[1:], ['epoch'] * 511)
        self.assertTrue(dispatcher._wake_session.closed)
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
