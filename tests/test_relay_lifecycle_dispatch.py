import asyncio
import io
from contextvars import ContextVar
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from aiohttp import web
from aiohttp import ClientSession
from aiohttp.test_utils import TestServer

from ucloud_sandboxes import relay_lifecycle as lifecycle
from ucloud_sandboxes.model_relay import RelayLifecycleDeferred


class RelayLifecycleDispatchTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_transport_preserves_failure_fences_and_rejects_redirects(self):
        cases = {
            "missing": (404, {}, lifecycle.RelayCallerUnavailable),
            "lost": (410, {}, lifecycle.RelayCallerUnavailable),
            "obsolete": (409, {"retryable": False}, lifecycle.RelayCallerUnavailable),
            "busy": (409, {}, lifecycle._RelayLifecycleRetry),
            "capacity": (503, {"retryable": True}, lifecycle._RelayLifecycleRetry),
            "overload": (429, {"retryable": True}, lifecycle._RelayLifecycleRetry),
            "failure": (503, {}, lifecycle.HTTPError),
            "redirect": (307, {}, lifecycle.HTTPError),
        }
        seen = []

        async def reply(request):
            name = request.match_info["sandbox"]
            seen.append(name)
            status, body, _error = cases[name]
            return web.json_response(
                body, status=status, headers={"Location": "/unexpected"}
            )

        app = web.Application()
        app.router.add_post("/v1/sandboxes/{sandbox}/wake", reply)
        async with TestServer(app) as server, ClientSession() as session:
            for name, (_status, _body, error) in cases.items():
                request = SimpleNamespace(
                    sandbox_id=name,
                    sandbox_generation=7,
                    request_id=name,
                    rollout_id="r",
                    created_at=0,
                )
                with self.subTest(name=name), self.assertRaises(error):
                    await lifecycle._post_lifecycle_attempt(
                        session,
                        str(server.make_url("/")),
                        "token",
                        request,
                        action="wake",
                        attempt=0,
                        deadline=lifecycle.time.monotonic() + 30,
                    )
        self.assertEqual(seen, list(cases))

    async def test_async_park_honors_worker_deferral_without_treating_it_as_success(
        self,
    ):
        async def reply(request):
            return web.json_response(
                {
                    "error_code": "park_deferred",
                    "retryable": True,
                    "retry_after_seconds": 30,
                },
                status=409,
                headers={"X-UCloud-Sandbox-Transport-Epoch": "original"},
            )

        app = web.Application()
        app.router.add_post("/v1/sandboxes/s/park", reply)
        async with TestServer(app) as server, ClientSession() as session:
            with self.assertRaises(lifecycle._RelayLifecycleRetry) as caught:
                await lifecycle._post_lifecycle_attempt(
                    session,
                    str(server.make_url("/")),
                    "token",
                    SimpleNamespace(
                        sandbox_id="s",
                        sandbox_generation=1,
                        request_id="r",
                        rollout_id="rollout",
                        created_at=0,
                    ),
                    action="park",
                    attempt=0,
                    deadline=lifecycle.time.monotonic() + 30,
                )
            self.assertEqual(caught.exception.delay_seconds, 30)
            self.assertEqual(caught.exception.transport_epoch, "original")

    async def test_async_transport_bounds_streamed_bodies_and_actual_http_deadline(
        self,
    ):
        release = asyncio.Event()

        async def reply(request):
            if request.match_info["sandbox"] == "slow":
                await release.wait()
                return web.json_response({})
            response = web.StreamResponse()
            await response.prepare(request)
            try:
                for _ in range(18):
                    await response.write(b"x" * 65536)
            except ConnectionResetError:
                pass
            return response

        app = web.Application()
        app.router.add_post("/v1/sandboxes/{sandbox}/wake", reply)
        async with TestServer(app) as server, ClientSession() as session:
            try:
                for name, error, seconds in [
                    ("large", ValueError, 5),
                    ("slow", asyncio.TimeoutError, 0.03),
                ]:
                    request = SimpleNamespace(
                        sandbox_id=name,
                        sandbox_generation=7,
                        request_id=name,
                        rollout_id="r",
                        created_at=0,
                    )
                    with self.subTest(name=name), self.assertRaises(error):
                        await lifecycle._post_lifecycle_attempt(
                            session,
                            str(server.make_url("/")),
                            "token",
                            request,
                            action="wake",
                            attempt=0,
                            deadline=lifecycle.time.monotonic() + seconds,
                        )
            finally:
                release.set()

    async def test_completed_response_skips_park(self):
        dispatcher = lifecycle.RelayLifecycleDispatcher("http://gateway", "token")
        with patch.object(lifecycle, "_post_lifecycle_attempt") as post:
            try:
                self.assertIsNone(
                    await dispatcher.notify(
                        SimpleNamespace(completed_at=1), action="park"
                    )
                )
                post.assert_not_called()
            finally:
                await dispatcher.close()

    async def test_durable_deferral_releases_dispatch_without_sleep(self):
        from ucloud_sandboxes.model_relay import RelayLifecycleDeferred

        dispatcher = lifecycle.RelayLifecycleDispatcher("http://gateway", "token")
        with patch.object(
            lifecycle,
            "_post_lifecycle_attempt",
            side_effect=lifecycle._RelayLifecycleRetry(30, transport_epoch="original"),
        ):
            try:
                with self.assertRaises(RelayLifecycleDeferred) as caught:
                    await asyncio.wait_for(
                        dispatcher.notify(
                            SimpleNamespace(durable_lifecycle=True), action="park"
                        ),
                        0.5,
                    )
                self.assertEqual(caught.exception.seconds, 30)
                self.assertEqual(caught.exception.transport_epoch, "original")
                self.assertFalse(dispatcher._active)
            finally:
                await dispatcher.close()

    async def test_completed_response_supersedes_next_durable_park_retry(self):
        from ucloud_sandboxes.model_relay import RelayLifecycleDeferred

        dispatcher = lifecycle.RelayLifecycleDispatcher("http://gateway", "token")
        request = SimpleNamespace(completed_at=None, response_committed=asyncio.Event())
        with patch.object(
            lifecycle,
            "_post_lifecycle_attempt",
            side_effect=lifecycle._RelayLifecycleRetry(5),
        ) as post:
            try:
                with self.assertRaises(RelayLifecycleDeferred):
                    await dispatcher.notify(request, action="park")
                request.completed_at = lifecycle.time.time()
                request.response_committed.set()
                self.assertIsNone(await dispatcher.notify(request, action="park"))
                self.assertEqual(post.call_count, 1)
            finally:
                await dispatcher.close()

    async def test_committed_response_does_not_abandon_inflight_park(self):
        dispatcher = lifecycle.RelayLifecycleDispatcher("http://gateway", "token")
        entered, release = asyncio.Event(), asyncio.Event()
        request = SimpleNamespace(completed_at=None, response_committed=asyncio.Event())

        async def post(*_args, **_kwargs):
            entered.set()
            await asyncio.wait_for(release.wait(), 2)
            return "parked-epoch"

        with patch.object(lifecycle, "_post_lifecycle_attempt", side_effect=post):
            task = asyncio.create_task(dispatcher.notify(request, action="park"))
            try:
                await asyncio.wait_for(entered.wait(), 1)
                request.completed_at = lifecycle.time.time()
                request.response_committed.set()
                await asyncio.sleep(0.01)
                self.assertFalse(task.done())
                release.set()
                self.assertEqual(await task, "parked-epoch")
            finally:
                release.set()
                await dispatcher.close()

    async def test_park_threads_do_not_limit_async_wakes_or_lose_context(self):
        trace = ContextVar("test_trace", default="missing")
        trace.set("request-trace")
        dispatcher = lifecycle.RelayLifecycleDispatcher("http://gateway", "token")
        entered = asyncio.Event()
        release = asyncio.Event()
        count = 0

        async def post(_session, _url, _token, _request, **_kwargs):
            nonlocal count
            self.assertEqual(trace.get(), "request-trace")
            count += 1
            if count == 512:
                entered.set()
            await release.wait()
            return "epoch"

        with patch.object(lifecycle, "_post_lifecycle_attempt", side_effect=post):
            tasks = [
                asyncio.create_task(dispatcher.notify(SimpleNamespace(), action=action))
                for action in ("park", "wake")
                for _ in range(256)
            ]
            try:
                await asyncio.wait_for(entered.wait(), 3)
                tasks[0].cancel()
                await asyncio.sleep(0.01)
                self.assertFalse(tasks[0].done())
            finally:
                release.set()
                results = await asyncio.gather(*tasks, return_exceptions=True)
                await dispatcher.close()
        self.assertIsInstance(results[0], asyncio.CancelledError)
        self.assertEqual(results[1:], ["epoch"] * 511)
        self.assertTrue(dispatcher._wake_session.closed)
        with self.assertRaisesRegex(RuntimeError, "closed"):
            await dispatcher.notify(SimpleNamespace(), action="wake")

    async def test_lifecycle_failure_releases_slot_and_propagates(self):
        dispatcher = lifecycle.RelayLifecycleDispatcher("http://gateway", "token")
        try:
            with patch.object(
                lifecycle,
                "_post_lifecycle_attempt",
                side_effect=[ValueError("failed"), "epoch"],
            ):
                with self.assertRaisesRegex(ValueError, "failed"):
                    await dispatcher.notify(SimpleNamespace(), action="wake")
                self.assertEqual(
                    await dispatcher.notify(SimpleNamespace(), action="wake"), "epoch"
                )
        finally:
            await dispatcher.close()

    async def test_capacity_deferral_releases_slots_and_preserves_retry_identity(self):
        from ucloud_sandboxes.model_relay import RelayLifecycleDeferred

        dispatcher = lifecycle.RelayLifecycleDispatcher("http://gateway", "token")
        attempts = {}

        def post(_session, _url, _token, request, *, action, attempt, deadline):
            attempts[request.request_id] = attempts.get(request.request_id, 0) + 1
            if request.request_id != "ready" and attempts[request.request_id] == 1:
                raise lifecycle._RelayLifecycleRetry(1.0)
            return "epoch"

        requests = [SimpleNamespace(request_id=str(index)) for index in range(64)]
        with patch.object(lifecycle, "_post_lifecycle_attempt", side_effect=post):
            try:
                results = await asyncio.wait_for(
                    asyncio.gather(
                        *(
                            dispatcher.notify(request, action="wake")
                            for request in requests
                        ),
                        return_exceptions=True,
                    ),
                    3,
                )
                self.assertTrue(
                    all(
                        isinstance(result, RelayLifecycleDeferred) for result in results
                    )
                )
                self.assertFalse(dispatcher._active)
                self.assertEqual(
                    await dispatcher.notify(
                        SimpleNamespace(request_id="ready"), action="wake"
                    ),
                    "epoch",
                )
                # The durable dispatcher may later retry the same identities;
                # this transport holds no hidden task or timer in the meantime.
                self.assertEqual(
                    await asyncio.gather(
                        *(
                            dispatcher.notify(request, action="wake")
                            for request in requests
                        )
                    ),
                    ["epoch"] * 64,
                )
                self.assertTrue(
                    all(attempts[request.request_id] == 2 for request in requests)
                )
            finally:
                await dispatcher.close()

    async def test_expired_wake_never_sends_http(self):
        dispatcher = lifecycle.RelayLifecycleDispatcher("http://gateway", "token")
        try:
            with patch.object(lifecycle, "_post_lifecycle_attempt") as post:
                with self.assertRaises(TimeoutError):
                    await dispatcher.notify(
                        SimpleNamespace(expires_at=lifecycle.time.time() - 1),
                        action="wake",
                    )
                post.assert_not_called()
        finally:
            await dispatcher.close()

    async def test_shutdown_after_deferral_has_no_background_retry(self):
        dispatcher = lifecycle.RelayLifecycleDispatcher("http://gateway", "token")
        with patch.object(
            lifecycle,
            "_post_lifecycle_attempt",
            side_effect=lifecycle._RelayLifecycleRetry(1),
        ) as post:
            with self.assertRaises(RelayLifecycleDeferred):
                await dispatcher.notify(SimpleNamespace(), action="wake")
            await dispatcher.close()
            with self.assertRaisesRegex(RuntimeError, "closed"):
                await dispatcher.notify(SimpleNamespace(), action="wake")
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
                    status=503,
                    headers={"Retry-After": "1"},
                )
            return web.json_response(
                {},
                headers={
                    "X-UCloud-Sandbox-Transport-Epoch": "epoch-restored",
                },
            )

        def request(request_id):
            return SimpleNamespace(
                request_id=request_id,
                sandbox_id=request_id,
                sandbox_generation=7,
                rollout_id="rollout",
                created_at=1.0,
            )

        app = web.Application()
        app.router.add_post("/v1/sandboxes/{sandbox}/wake", wake)
        async with TestServer(app) as server:
            dispatcher = lifecycle.RelayLifecycleDispatcher(
                str(server.make_url("/")), "token"
            )
            tasks = [
                asyncio.create_task(
                    dispatcher.notify(request(str(index)), action="wake")
                )
                for index in range(2)
            ]
            try:
                await asyncio.wait_for(blocked.wait(), 3)
                ready = await asyncio.wait_for(
                    dispatcher.notify(request("ready"), action="wake"),
                    0.75,
                )
                self.assertEqual(ready, "epoch-restored")
                self.assertEqual(
                    [
                        type(result)
                        for result in await asyncio.gather(
                            *tasks, return_exceptions=True
                        )
                    ],
                    [RelayLifecycleDeferred] * 2,
                )
                self.assertEqual(
                    await asyncio.gather(
                        *(
                            dispatcher.notify(request(str(index)), action="wake")
                            for index in range(2)
                        )
                    ),
                    ["epoch-restored"] * 2,
                )
            finally:
                await asyncio.gather(*tasks, return_exceptions=True)
                await dispatcher.close()
        for request_id in ("0", "1"):
            self.assertEqual(len(calls[request_id]), 2)
            self.assertEqual(calls[request_id][0], calls[request_id][1])
            self.assertEqual(
                calls[request_id][0]["operation_id"], f"relay-wake:{request_id}"
            )
            self.assertEqual(calls[request_id][0]["generation"], 7)


class CanonicalLifecycleProtocolTests(unittest.IsolatedAsyncioTestCase):
    async def test_error_classifier_closes_bodies_and_preserves_failure_types(self):
        for status, body, terminal in (
            (404, b"not found", True),
            (410, b"gone", True),
            (409, b'{"retryable":false}', True),
            (503, b'{"retryable":false}', False),
            (503, b"upstream unavailable", False),
            (504, b"timeout", False),
            (403, b"forbidden", False),
        ):
            stream = io.BytesIO(body)
            error = lifecycle.HTTPError("http://gateway", status, "failure", {}, stream)
            with (
                self.subTest(status=status),
                self.assertRaises(
                    lifecycle.RelayCallerUnavailable
                    if terminal
                    else lifecycle.HTTPError
                ),
            ):
                lifecycle._raise_relay_lifecycle_http_error(
                    error,
                    action="wake",
                    attempt=0,
                    deadline=lifecycle.time.monotonic() + 60,
                )
            self.assertTrue(stream.closed)

    async def test_many_capacity_retries_keep_one_fenced_operation(self):
        calls = []

        async def wake(request):
            calls.append(await request.json())
            if len(calls) <= 80:
                return web.json_response(
                    {"retryable": True, "error_code": "node_startup_busy"},
                    status=503,
                    headers={"Retry-After": "1"},
                )
            return web.json_response(
                {}, headers={"X-UCloud-Sandbox-Transport-Epoch": "restored"}
            )

        app = web.Application()
        app.router.add_post("/v1/sandboxes/sandbox/wake", wake)
        async with TestServer(app) as server:
            dispatcher = lifecycle.RelayLifecycleDispatcher(
                str(server.make_url("/")), "token"
            )
            try:
                request = SimpleNamespace(
                    sandbox_id="sandbox",
                    sandbox_generation=3,
                    request_id="request",
                    rollout_id="rollout",
                    created_at=1.0,
                )
                with patch.object(lifecycle.asyncio, "sleep") as sleep:
                    for _ in range(80):
                        with self.assertRaises(RelayLifecycleDeferred):
                            await dispatcher.notify(request, action="wake")
                    result = await dispatcher.notify(request, action="wake")
                self.assertEqual(result, "restored")
                sleep.assert_not_called()
            finally:
                await dispatcher.close()
        self.assertEqual(len(calls), 81)
        self.assertTrue(all(body == calls[0] for body in calls))
        self.assertEqual(calls[0]["operation_id"], "relay-wake:request")

    async def test_capacity_retry_respects_remaining_deadline_and_action(self):
        for action, retry_after in (("wake", "1"), ("wake", "invalid"), ("park", "1")):
            calls = []

            async def reply(request):
                calls.append(await request.json())
                return web.json_response(
                    {"retryable": True, "error_code": "node_startup_busy"},
                    status=503,
                    headers={"Retry-After": retry_after},
                )

            app = web.Application()
            app.router.add_post("/v1/sandboxes/sandbox/" + action, reply)
            async with TestServer(app) as server:
                dispatcher = lifecycle.RelayLifecycleDispatcher(
                    str(server.make_url("/")), "token"
                )
                try:
                    with self.assertRaisesRegex(
                        lifecycle.HTTPError, "node_startup_busy"
                    ):
                        await dispatcher.notify(
                            SimpleNamespace(
                                sandbox_id="sandbox",
                                sandbox_generation=3,
                                request_id="request",
                                rollout_id="rollout",
                                created_at=1.0,
                                expires_at=lifecycle.time.time() + 0.5,
                            ),
                            action=action,
                        )
                    self.assertEqual(len(calls), 1)
                finally:
                    await dispatcher.close()

    async def test_park_conflicts_retry_with_same_body_and_return_epoch(self):
        calls = []

        async def park(request):
            calls.append(await request.json())
            if len(calls) < 3:
                return web.json_response(
                    {
                        "error": (
                            "lifecycle transition is in progress"
                            if len(calls) == 1
                            else "sandbox has active exec/file activity that cannot survive park; use SDK start_agent()"
                        )
                    },
                    status=409,
                )
            return web.json_response(
                {}, headers={"X-UCloud-Sandbox-Transport-Epoch": "parked"}
            )

        app = web.Application()
        app.router.add_post("/v1/sandboxes/sandbox/park", park)
        async with TestServer(app) as server:
            dispatcher = lifecycle.RelayLifecycleDispatcher(
                str(server.make_url("/")), "token"
            )
            try:
                request = SimpleNamespace(
                    sandbox_id="sandbox",
                    sandbox_generation=2,
                    request_id="request",
                    rollout_id="rollout",
                    created_at=1.0,
                )
                for _ in range(2):
                    with self.assertRaises(RelayLifecycleDeferred):
                        await dispatcher.notify(request, action="park")
                result = await dispatcher.notify(request, action="park")
                self.assertEqual(result, "parked")
            finally:
                await dispatcher.close()
        self.assertEqual(len(calls), 3)
        self.assertEqual(calls, [calls[0]] * 3)
