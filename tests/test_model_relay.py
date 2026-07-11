from __future__ import annotations

import asyncio
import base64
from contextlib import asynccontextmanager
import time
from typing import Any, AsyncIterator
import unittest

from aiohttp import ClientSession, web

from ucloud_sandboxes.deployment import package_version
from ucloud_sandboxes.model_relay import (
    ModelRelayState,
    RelayWorkerResponse,
    create_model_relay_app,
)


class RelayHarness:
    def __init__(
        self,
        base_url: str,
        client: ClientSession,
    ) -> None:
        self.base_url = base_url
        self.client = client

    async def request(
        self,
        method: str,
        path: str,
        *,
        expected: int | None = None,
        **kwargs: Any,
    ) -> tuple[int, Any]:
        async with self.client.request(
            method,
            self.base_url + path,
            **kwargs,
        ) as response:
            try:
                payload = await response.json(content_type=None)
            except ValueError:
                payload = await response.text()
            if expected is not None and response.status != expected:
                raise AssertionError(
                    f"{method} {path} returned {response.status}, expected "
                    f"{expected}: {payload!r}"
                )
            return response.status, payload

    async def request_bytes(
        self,
        method: str,
        path: str,
        *,
        expected: int | None = None,
        **kwargs: Any,
    ) -> tuple[int, bytes, dict[str, str]]:
        async with self.client.request(
            method,
            self.base_url + path,
            **kwargs,
        ) as response:
            payload = await response.read()
            if expected is not None and response.status != expected:
                raise AssertionError(
                    f"{method} {path} returned {response.status}, expected "
                    f"{expected}: {payload!r}"
                )
            return response.status, payload, dict(response.headers)

    async def register(
        self,
        rollout_id: str,
        *,
        headers: dict[str, str] | None = None,
    ) -> str:
        _status, payload = await self.request(
            "POST",
            "/register_rollout",
            expected=201,
            headers=headers,
            json={"rollout_id": rollout_id},
        )
        return str(payload["rollout"]["registration_token"])

    async def poll(
        self,
        rollout_id: str,
        registration_token: str,
        **params: Any,
    ) -> dict[str, Any]:
        _status, payload = await self.request(
            "GET",
            "/worker/poll",
            expected=200,
            params={
                "rollout_id": rollout_id,
                "registration_token": registration_token,
                **params,
            },
        )
        return payload

    async def respond(
        self,
        request: dict[str, Any],
        registration_token: str,
        body: object,
        *,
        expected: int = 200,
    ) -> dict[str, Any] | str:
        _status, payload = await self.request(
            "POST",
            "/worker/respond",
            expected=expected,
            json={
                "request_id": request["request_id"],
                "registration_token": registration_token,
                "lease_id": request["lease_id"],
                "response": body,
            },
        )
        return payload

    async def respond_bytes(
        self,
        request: dict[str, Any],
        registration_token: str,
        body: bytes,
        *,
        status: int = 200,
        headers: dict[str, str] | None = None,
        auth_headers: dict[str, str] | None = None,
        expected: int = 200,
    ) -> dict[str, Any] | str:
        _status, payload = await self.request(
            "POST",
            "/worker/respond",
            expected=expected,
            headers=auth_headers,
            json={
                "request_id": request["request_id"],
                "registration_token": registration_token,
                "lease_id": request["lease_id"],
                "body_base64": base64.b64encode(body).decode("ascii"),
                "status": status,
                "headers": headers or {},
            },
        )
        return payload

    async def stats(self) -> dict[str, Any]:
        _status, payload = await self.request(
            "GET",
            "/v1/relay/stats",
            expected=200,
        )
        return payload

    async def model_call(
        self,
        rollout_id: str,
        *,
        path: str | None = None,
        headers: dict[str, str] | None = None,
        body: dict[str, Any] | None = None,
    ) -> tuple[int, Any]:
        return await self.request(
            "POST",
            path or f"/rollouts/{rollout_id}/v1/chat/completions",
            headers=headers,
            json=body or {"model": "m", "messages": []},
        )


@asynccontextmanager
async def relay_app(**kwargs: Any) -> AsyncIterator[RelayHarness]:
    runner = web.AppRunner(create_model_relay_app(**kwargs))
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    sockets = site._server.sockets if site._server else []
    base_url = f"http://127.0.0.1:{sockets[0].getsockname()[1]}"
    try:
        async with ClientSession() as client:
            yield RelayHarness(base_url, client)
    finally:
        await runner.cleanup()


async def enqueue_and_poll(
    state: ModelRelayState,
    rollout_id: str,
    registration_token: str,
    *,
    lease_seconds: float = 30,
    worker_id: str | None = None,
):
    request = await state.enqueue(
        rollout_id=rollout_id,
        endpoint="/v1/responses",
        body={"model": "m"},
        headers={},
    )
    delivery = (
        await state.poll(
            rollout_id=rollout_id,
            registration_token=registration_token,
            timeout_seconds=0,
            lease_seconds=lease_seconds,
            worker_id=worker_id,
        )
    )[0]
    return request, delivery


class ModelRelayTests(unittest.IsolatedAsyncioTestCase):
    async def test_reregister_fences_every_operation_from_prior_incarnation(
        self,
    ) -> None:
        state = ModelRelayState()
        first_token = str(
            (await state.register_rollout("rollout-aba"))["registration_token"]
        )
        first_request, first_delivery = await enqueue_and_poll(
            state,
            "rollout-aba",
            first_token,
            worker_id="old-worker",
        )
        second_registration = await state.register_rollout("rollout-aba")
        second_token = str(second_registration["registration_token"])
        self.assertNotEqual(first_token, second_token)
        self.assertEqual(
            (await state.wait_for_response(first_request, timeout_seconds=1)).status,
            410,
        )

        stale_operations = {
            "unregister": lambda: state.unregister_rollout(
                "rollout-aba", registration_token=first_token
            ),
            "poll": lambda: state.poll(
                rollout_id="rollout-aba",
                registration_token=first_token,
                timeout_seconds=0,
            ),
            "heartbeat": lambda: state.record_worker_heartbeat(
                rollout_id="rollout-aba",
                registration_token=first_token,
                worker_id="old-worker",
            ),
            "renew": lambda: state.renew_lease(
                request_id=first_delivery.request_id,
                registration_token=first_token,
                lease_id=str(first_delivery.lease_id),
                lease_seconds=30,
            ),
            "respond": lambda: state.respond(
                request_id=first_delivery.request_id,
                registration_token=first_token,
                lease_id=first_delivery.lease_id,
                response=RelayWorkerResponse(200, {"old": True}),
            ),
        }
        for name, operation in stale_operations.items():
            with self.subTest(operation=name), self.assertRaises(web.HTTPConflict):
                await operation()

        request, delivery = await enqueue_and_poll(
            state,
            "rollout-aba",
            second_token,
        )
        await state.respond(
            request_id=delivery.request_id,
            registration_token=second_token,
            lease_id=delivery.lease_id,
            response=RelayWorkerResponse(200, {"new": True}),
        )
        observed = await state.wait_for_response(request, timeout_seconds=1)
        self.assertEqual((observed.status, observed.body), (200, {"new": True}))
        self.assertEqual(
            (await state.list_rollouts())[0]["registration_token"],
            second_token,
        )

    async def test_worker_routes_require_registration_token(self) -> None:
        async with relay_app(worker_poll_timeout_seconds=0) as relay:
            await relay.register("token-required")
            statuses = []
            for method, path, kwargs in (
                ("GET", "/worker/poll", {"params": {"rollout_id": "token-required"}}),
                (
                    "POST",
                    "/unregister_rollout",
                    {"json": {"rollout_id": "token-required"}},
                ),
            ):
                status, _payload = await relay.request(method, path, **kwargs)
                statuses.append(status)
        self.assertEqual(statuses, [400, 400])

    async def test_diagnostics_and_completed_payloads_expire_without_reregistration(
        self,
    ) -> None:
        state = ModelRelayState(
            completed_request_retention_seconds=0.005,
            worker_retention_seconds=0.005,
        )
        token = str((await state.register_rollout("retention"))["registration_token"])
        await state.record_worker_heartbeat(
            rollout_id="retention",
            registration_token=token,
            worker_id="stale-worker",
            metadata={"large": "diagnostic"},
        )
        request, delivery = await enqueue_and_poll(state, "retention", token)
        await state.respond(
            request_id=delivery.request_id,
            registration_token=token,
            lease_id=delivery.lease_id,
            response=RelayWorkerResponse(200, {"large": "response-payload"}),
        )
        await state.wait_for_response(request, timeout_seconds=1)
        await asyncio.sleep(0.02)
        stats = await state.stats()
        self.assertEqual((stats["completed_retained"], stats["workers"]), (0, []))

    async def test_respond_rechecks_lease_expiry_before_accepting_result(self) -> None:
        state = ModelRelayState()
        token = str(
            (await state.register_rollout("expiry-check"))["registration_token"]
        )
        request, leased = await enqueue_and_poll(state, "expiry-check", token)
        leased.lease_expires_at = time.time() - 1
        with self.assertRaises(web.HTTPConflict):
            await state.respond(
                request_id=request.request_id,
                registration_token=token,
                lease_id=leased.lease_id,
                response=RelayWorkerResponse(200, {"stale": True}),
            )
        stats = await state.stats()
        retried = (
            await state.poll(
                rollout_id="expiry-check",
                registration_token=token,
                timeout_seconds=0,
            )
        )[0]
        await state.cancel_request(
            request_id=request.request_id,
            response=RelayWorkerResponse(499, {}),
        )
        self.assertEqual(stats["pending"]["expiry-check"], 1)
        self.assertEqual(stats["counters"]["lease_expired"], 1)
        self.assertEqual(retried.delivery_count, 2)

    async def test_admission_limits_release_capacity_after_cancellation(self) -> None:
        state = ModelRelayState(
            max_inflight_requests=1,
            max_inflight_requests_per_rollout=1,
            max_inflight_bytes=1024,
        )
        await state.register_rollout("bounded")
        first = await state.enqueue(
            rollout_id="bounded", endpoint="/v1/responses", body={}, headers={}
        )
        with self.assertRaises(web.HTTPTooManyRequests):
            await state.enqueue(
                rollout_id="bounded", endpoint="/v1/responses", body={}, headers={}
            )
        rejected = await state.stats()
        await state.cancel_request(
            request_id=first.request_id,
            response=RelayWorkerResponse(499, {}),
        )
        second = await state.enqueue(
            rollout_id="bounded", endpoint="/v1/responses", body={}, headers={}
        )
        await state.cancel_request(
            request_id=second.request_id,
            response=RelayWorkerResponse(499, {}),
        )
        released = await state.stats()
        self.assertEqual(rejected["inflight"], 1)
        self.assertEqual(rejected["counters"]["admission_rejected"], 1)
        self.assertEqual((released["inflight"], released["inflight_bytes"]), (0, 0))

    async def test_absolute_request_expiry_releases_admission(self) -> None:
        state = ModelRelayState(request_timeout_seconds=0.005, max_inflight_requests=1)
        await state.register_rollout("expiry-admission")
        expired = await state.enqueue(
            rollout_id="expiry-admission", endpoint="/v1/responses", body={}, headers={}
        )
        await asyncio.sleep(0.02)
        replacement = await state.enqueue(
            rollout_id="expiry-admission", endpoint="/v1/responses", body={}, headers={}
        )
        response = await state.wait_for_response(expired, timeout_seconds=1)
        stats = await state.stats()
        await state.cancel_request(
            request_id=replacement.request_id,
            response=RelayWorkerResponse(499, {}),
        )
        self.assertEqual(response.status, 504)
        self.assertEqual((stats["inflight"], stats["counters"]["timed_out"]), (1, 1))

    async def test_completed_tombstones_and_worker_diagnostics_have_hard_caps(
        self,
    ) -> None:
        state = ModelRelayState(max_completed_requests=2, max_workers=2)
        token = str((await state.register_rollout("hard-caps"))["registration_token"])
        for index in range(3):
            await state.record_worker_heartbeat(
                rollout_id="hard-caps",
                registration_token=token,
                worker_id=f"worker-{index}",
            )
            request = await state.enqueue(
                rollout_id="hard-caps",
                endpoint="/v1/responses",
                body={"index": index},
                headers={},
            )
            await state.cancel_request(
                request_id=request.request_id,
                response=RelayWorkerResponse(499, {}),
            )
        stats = await state.stats()
        self.assertEqual(stats["completed_retained"], 2)
        self.assertEqual(
            {worker["worker_id"] for worker in stats["workers"]},
            {"worker-1", "worker-2"},
        )

    async def test_healthz_reports_service_version(self) -> None:
        async with relay_app() as relay:
            _status, payload = await relay.request("GET", "/healthz", expected=200)
        self.assertEqual(
            payload,
            {"ok": True, "service": "model-relay", "version": package_version()},
        )

    async def test_openai_chat_request_round_trips_through_worker_poll(self) -> None:
        async with relay_app(
            sandbox_bearer_token="sandbox-token",
            worker_bearer_token="worker-token",
            request_timeout_seconds=5,
            worker_poll_timeout_seconds=1,
        ) as relay:
            worker_headers = {"Authorization": "Bearer worker-token"}
            token = await relay.register("rollout-1", headers=worker_headers)
            sandbox_task = asyncio.create_task(
                relay.model_call(
                    "rollout-1",
                    headers={
                        "Authorization": "Bearer sandbox-token",
                        "Proxy-Authorization": "Bearer proxy-secret",
                        "X-UCloud-Sandbox-Token": "public-secret",
                        "X-Request-Metadata": "safe",
                    },
                    body={"model": "local-model", "messages": [{"content": "ping"}]},
                )
            )
            _status, polled = await relay.request(
                "GET",
                "/worker/poll",
                expected=200,
                headers=worker_headers,
                params={"rollout_id": "rollout-1", "registration_token": token},
            )
            request = polled["request"]
            await relay.request(
                "POST",
                "/worker/respond",
                expected=200,
                headers=worker_headers,
                json={
                    "request_id": request["request_id"],
                    "registration_token": token,
                    "lease_id": request["lease_id"],
                    "response": {"choices": [{"message": {"content": "pong"}}]},
                },
            )
            status, body = await sandbox_task

        forwarded = {key.lower(): value for key, value in request["headers"].items()}
        self.assertEqual(status, 200)
        self.assertEqual(request["rollout_id"], "rollout-1")
        self.assertEqual(polled["requests"][0]["request_id"], request["request_id"])
        self.assertIsInstance(request["lease_id"], str)
        self.assertEqual(request["endpoint"], "/v1/chat/completions")
        self.assertEqual(request["body"]["model"], "local-model")
        self.assertNotIn("authorization", forwarded)
        self.assertNotIn("proxy-authorization", forwarded)
        self.assertNotIn("x-ucloud-sandbox-token", forwarded)
        self.assertEqual(forwarded["x-request-metadata"], "safe")
        self.assertEqual(body["choices"][0]["message"]["content"], "pong")

    async def test_general_tunnel_preserves_http_bytes_path_query_and_headers(
        self,
    ) -> None:
        async with relay_app(
            sandbox_bearer_token="sandbox-token",
            worker_bearer_token="worker-token",
            request_timeout_seconds=5,
            worker_poll_timeout_seconds=1,
        ) as relay:
            worker_headers = {"Authorization": "Bearer worker-token"}
            _status, registered = await relay.request(
                "POST",
                "/v1/tunnels/register",
                expected=201,
                headers=worker_headers,
                json={"tunnel_id": "tunnel-1", "metadata": {"kind": "http"}},
            )
            token = registered["rollout"]["registration_token"]
            request_body = b"\x00\xffbinary-request"
            client_task = asyncio.create_task(
                relay.request_bytes(
                    "PUT",
                    "/tunnels/tunnel-1/api/a%2Fb%20c?x=1&x=2&literal=one+two",
                    headers={
                        "X-UCloud-Relay-Token": "sandbox-token",
                        "Authorization": "Bearer upstream-secret",
                        "Content-Type": "application/octet-stream",
                        "X-Custom": "safe",
                    },
                    data=request_body,
                )
            )
            _status, polled = await relay.request(
                "GET",
                "/worker/poll",
                expected=200,
                headers=worker_headers,
                params={
                    "tunnel_id": "tunnel-1",
                    "registration_token": token,
                },
            )
            request = polled["request"]
            await relay.respond_bytes(
                request,
                token,
                b"\xffbinary-response",
                status=207,
                auth_headers=worker_headers,
                headers={
                    "Content-Type": "application/vnd.ucloud.test",
                    "X-Upstream": "worker",
                    "Connection": "close",
                },
            )
            response_status, response_body, response_headers = await client_task

        forwarded = {key.lower(): value for key, value in request["headers"].items()}
        response_headers = {
            key.lower(): value for key, value in response_headers.items()
        }
        self.assertEqual(request["rollout_id"], "tunnel-1")
        self.assertEqual(request["tunnel_id"], "tunnel-1")
        self.assertEqual(request["method"], "PUT")
        self.assertEqual(
            request["endpoint"],
            "/api/a%2Fb%20c?x=1&x=2&literal=one+two",
        )
        self.assertEqual(base64.b64decode(request["body_base64"]), request_body)
        self.assertEqual(request["body_size"], len(request_body))
        self.assertIsNone(request["body"])
        self.assertEqual(forwarded["authorization"], "Bearer upstream-secret")
        self.assertEqual(forwarded["content-type"], "application/octet-stream")
        self.assertEqual(forwarded["x-custom"], "safe")
        self.assertNotIn("x-ucloud-relay-token", forwarded)
        self.assertEqual(
            (response_status, response_body), (207, b"\xffbinary-response")
        )
        self.assertEqual(
            response_headers["content-type"],
            "application/vnd.ucloud.test",
        )
        self.assertEqual(response_headers["x-upstream"], "worker")
        self.assertNotEqual(response_headers.get("connection"), "close")

    async def test_general_tunnel_exposes_json_and_rejects_invalid_base64_response(
        self,
    ) -> None:
        async with relay_app(request_timeout_seconds=5) as relay:
            token = await relay.register("json-tunnel")
            client_task = asyncio.create_task(
                relay.request_bytes(
                    "POST",
                    "/tunnels/json-tunnel/echo",
                    json={"hello": "world"},
                )
            )
            request = (await relay.poll("json-tunnel", token))["request"]
            invalid_status, _payload = await relay.request(
                "POST",
                "/worker/respond",
                json={
                    "request_id": request["request_id"],
                    "registration_token": token,
                    "lease_id": request["lease_id"],
                    "body_base64": "not base64!",
                },
            )
            await relay.respond_bytes(
                request,
                token,
                b'{"echo":true}',
                headers={"Content-Type": "application/json"},
            )
            status, body, _headers = await client_task

        self.assertEqual(request["body"], {"hello": "world"}, repr(request))
        self.assertEqual(invalid_status, 400)
        self.assertEqual((status, body), (200, b'{"echo":true}'))

    async def test_plain_v1_endpoint_accepts_rollout_header(self) -> None:
        async with relay_app(request_timeout_seconds=5) as relay:
            token = await relay.register("rollout-2")
            sandbox_task = asyncio.create_task(
                relay.model_call(
                    "rollout-2",
                    path="/v1/chat/completions",
                    headers={"X-Relay-Rollout-Id": "rollout-2"},
                )
            )
            request = (await relay.poll("rollout-2", token))["request"]
            await relay.respond(request, token, {"ok": True})
            result = await sandbox_task
        self.assertEqual(result, (200, {"ok": True}))

    async def test_auth_is_enforced_when_configured(self) -> None:
        async with relay_app(sandbox_bearer_token="sandbox-token") as relay:
            await relay.register("rollout-1")
            status, _payload = await relay.model_call(
                "rollout-1",
                path="/v1/chat/completions",
                headers={"X-Relay-Rollout-Id": "rollout-1"},
            )
        self.assertEqual(status, 401)

    async def test_empty_worker_poll_returns_null_request(self) -> None:
        async with relay_app(worker_poll_timeout_seconds=0) as relay:
            token = await relay.register("rollout-empty")
            _status, body = await relay.request(
                "GET",
                "/worker/poll",
                expected=200,
                params={
                    "rollout_id": "rollout-empty",
                    "registration_token": token,
                    "timeout_seconds": "0",
                },
            )
        self.assertEqual(body, {"request": None, "requests": []})

    async def test_worker_can_poll_batches_and_respond_idempotently(self) -> None:
        async with relay_app(request_timeout_seconds=5) as relay:
            token = await relay.register("rollout-batch")
            tasks = [
                asyncio.create_task(
                    relay.model_call(
                        "rollout-batch",
                        body={"model": "m", "messages": [{"content": str(index)}]},
                    )
                )
                for index in range(3)
            ]
            for _ in range(100):
                if (await relay.stats())["pending"].get("rollout-batch") == 3:
                    break
                await asyncio.sleep(0.01)

            first = (
                await relay.poll(
                    "rollout-batch", token, limit="2", worker_id="worker-a"
                )
            )["requests"]
            self.assertEqual(len(first), 2)
            for request in first:
                await relay.respond(
                    request,
                    token,
                    {"index": request["body"]["messages"][0]["content"]},
                )
            duplicate = await relay.respond(first[0], token, {"ignored": True})
            last = (await relay.poll("rollout-batch", token, limit="2"))["request"]
            await relay.respond(
                last,
                token,
                {"index": last["body"]["messages"][0]["content"]},
            )
            results = await asyncio.gather(*tasks)
            stats = await relay.stats()

        self.assertEqual(len({request["request_id"] for request in first}), 2)
        self.assertTrue(duplicate["duplicate"])
        self.assertEqual([status for status, _body in results], [200, 200, 200])
        self.assertEqual(stats["counters"]["completed"], 3)
        self.assertEqual(stats["counters"]["duplicate_responses"], 1)
        self.assertEqual(stats["workers"][0]["worker_id"], "worker-a")

    async def test_expired_lease_is_retried_and_stale_response_rejected(self) -> None:
        async with relay_app(
            request_timeout_seconds=5, worker_lease_seconds=0.01
        ) as relay:
            token = await relay.register("rollout-retry")
            task = asyncio.create_task(relay.model_call("rollout-retry"))
            first = (
                await relay.poll(
                    "rollout-retry",
                    token,
                    worker_id="slow-worker",
                    lease_seconds="0.01",
                )
            )["request"]
            await asyncio.sleep(0.03)
            second = (
                await relay.poll(
                    "rollout-retry",
                    token,
                    worker_id="fast-worker",
                    lease_seconds="1",
                )
            )["request"]
            await relay.respond(first, token, {"stale": True}, expected=409)
            await relay.respond(second, token, {"ok": True})
            result, stats = await task, await relay.stats()

        self.assertEqual(first["request_id"], second["request_id"])
        self.assertNotEqual(first["lease_id"], second["lease_id"])
        self.assertEqual(second["delivery_count"], 2)
        self.assertEqual(result, (200, {"ok": True}))
        self.assertEqual(stats["counters"]["lease_expired"], 1)

    async def test_worker_can_renew_lease_for_long_inference(self) -> None:
        async with relay_app(
            request_timeout_seconds=5, worker_lease_seconds=0.01
        ) as relay:
            token = await relay.register("rollout-renew")
            task = asyncio.create_task(relay.model_call("rollout-renew"))
            leased = (
                await relay.poll(
                    "rollout-renew",
                    token,
                    worker_id="worker-renew",
                    lease_seconds="0.05",
                )
            )["request"]
            await asyncio.sleep(0.02)
            _status, renewed_payload = await relay.request(
                "POST",
                "/worker/renew",
                expected=200,
                json={
                    "request_id": leased["request_id"],
                    "registration_token": token,
                    "lease_id": leased["lease_id"],
                    "worker_id": "worker-renew",
                    "lease_seconds": 1,
                },
            )
            await asyncio.sleep(0.04)
            await relay.respond(leased, token, {"renewed": True})
            result, stats = await task, await relay.stats()

        renewed = renewed_payload["request"]
        self.assertGreater(renewed["lease_expires_at"], renewed["delivered_at"])
        self.assertEqual(result, (200, {"renewed": True}))
        self.assertEqual(stats["counters"]["lease_renewed"], 1)
        self.assertEqual(stats["counters"]["lease_expired"], 0)

    async def test_expired_lease_cannot_be_renewed(self) -> None:
        async with relay_app(
            request_timeout_seconds=5, worker_lease_seconds=0.01
        ) as relay:
            token = await relay.register("rollout-expired-renew")
            task = asyncio.create_task(relay.model_call("rollout-expired-renew"))
            leased = (
                await relay.poll("rollout-expired-renew", token, lease_seconds="0.01")
            )["request"]
            await asyncio.sleep(0.03)
            status, _payload = await relay.request(
                "POST",
                "/worker/renew",
                json={
                    "request_id": leased["request_id"],
                    "registration_token": token,
                    "lease_id": leased["lease_id"],
                    "lease_seconds": 1,
                },
            )
            retried = (
                await relay.poll("rollout-expired-renew", token, lease_seconds="1")
            )["request"]
            await relay.respond(retried, token, {"ok": True})
            result = await task

        self.assertEqual(status, 409)
        self.assertEqual(result, (200, {"ok": True}))

    async def test_worker_heartbeat_updates_stats(self) -> None:
        async with relay_app() as relay:
            token = await relay.register("rollout-heartbeat")
            await relay.request(
                "POST",
                "/worker/heartbeat",
                expected=200,
                json={
                    "rollout_id": "rollout-heartbeat",
                    "registration_token": token,
                    "worker_id": "worker-heartbeat",
                    "metadata": {"host": "lumi"},
                },
            )
            stats = await relay.stats()

        self.assertEqual(stats["workers"][0]["worker_id"], "worker-heartbeat")
        self.assertEqual(stats["workers"][0]["metadata"], {"host": "lumi"})


if __name__ == "__main__":
    unittest.main()
