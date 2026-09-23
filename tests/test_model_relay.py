from __future__ import annotations

import asyncio
import base64
from contextlib import asynccontextmanager
import json
from typing import Any, AsyncIterator
import unittest
from unittest.mock import patch

from aiohttp import ClientSession, web


from ucloud_sandboxes.model_relay import create_model_relay_app
from tests.postgres_fixture import postgres_database


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
            "/v1/relay/rollouts",
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
                "body": {"encoding": "json", "value": body},
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
                "body": {
                    "encoding": "base64",
                    "value": base64.b64encode(body).decode("ascii"),
                },
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
    async with postgres_database() as database:
        runner = web.AppRunner(
            create_model_relay_app(postgres_store=database, **kwargs)
        )
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


class ModelRelayTests(unittest.IsolatedAsyncioTestCase):
    async def test_maintenance_only_checks_outstanding_caller_incarnations(self):
        from types import SimpleNamespace
        from unittest.mock import AsyncMock
        from ucloud_sandboxes.model_relay import _model_relay_maintenance_loop

        for candidates in (set(), {("current", 2)}):
            terminal = {("current", 2): "node_lost"}
            state = SimpleNamespace(
                maintain=AsyncMock(),
                pending_caller_incarnations=AsyncMock(return_value=candidates),
                reconcile_unavailable_callers=AsyncMock(),
            )
            lookup = AsyncMock(return_value=terminal)
            with patch(
                "ucloud_sandboxes.model_relay.asyncio.sleep",
                side_effect=asyncio.CancelledError,
            ):
                with self.assertRaises(asyncio.CancelledError):
                    await _model_relay_maintenance_loop(state, 1, lookup)
            if candidates:
                lookup.assert_awaited_once_with(candidates)
                state.reconcile_unavailable_callers.assert_awaited_once_with(terminal)
            else:
                lookup.assert_not_awaited()
                state.reconcile_unavailable_callers.assert_not_awaited()

    async def test_worker_routes_require_registration_token(self) -> None:
        async with relay_app(worker_poll_timeout_seconds=0) as relay:
            await relay.register("token-required")
            statuses = []
            for method, path, kwargs in (
                ("GET", "/worker/poll", {"params": {"rollout_id": "token-required"}}),
                (
                    "DELETE",
                    "/v1/relay/rollouts/token-required",
                    {"json": {}},
                ),
            ):
                status, _payload = await relay.request(method, path, **kwargs)
                statuses.append(status)
        self.assertEqual(statuses, [400, 400])

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
                "/v1/relay/rollouts",
                expected=201,
                headers=worker_headers,
                json={"rollout_id": "tunnel-1", "metadata": {"kind": "http"}},
            )
            token = registered["rollout"]["registration_token"]
            request_body = b"\x00\xffbinary-request"
            client_task = asyncio.create_task(
                relay.request_bytes(
                    "PUT",
                    f"/tunnels/tunnel-1/_relay/{token}/"
                    "api/a%2Fb%20c?x=1&x=2&literal=one+two",
                    headers={
                        "Authorization": "Bearer upstream-secret",
                        "Content-Type": "application/octet-stream",
                        "X-Custom": "safe",
                        "Forwarded": "for=100.64.0.10",
                        "X-Forwarded-For": "100.64.0.10",
                        "X-Real-IP": "100.64.0.10",
                        "job-id": "provider-job",
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
                    "rollout_id": "tunnel-1",
                    "registration_token": token,
                },
            )
            request = polled["requests"][0]
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
        self.assertEqual(request["method"], "PUT")
        self.assertEqual(
            request["endpoint"],
            "/api/a%2Fb%20c?x=1&x=2&literal=one+two",
        )
        self.assertEqual(request["body"]["encoding"], "base64")
        self.assertEqual(base64.b64decode(request["body"]["value"]), request_body)
        self.assertEqual(forwarded["authorization"], "Bearer upstream-secret")
        self.assertEqual(forwarded["content-type"], "application/octet-stream")
        self.assertEqual(forwarded["x-custom"], "safe")
        self.assertNotIn("x-ucloud-relay-token", forwarded)
        self.assertNotIn("forwarded", forwarded)
        self.assertNotIn("x-forwarded-for", forwarded)
        self.assertNotIn("x-real-ip", forwarded)
        self.assertNotIn("job-id", forwarded)
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
            request = (await relay.poll("json-tunnel", token))["requests"][0]
            invalid_status, _payload = await relay.request(
                "POST",
                "/worker/respond",
                json={
                    "request_id": request["request_id"],
                    "registration_token": token,
                    "lease_id": request["lease_id"],
                    "body": {"encoding": "base64", "value": "not base64!"},
                },
            )
            await relay.respond_bytes(
                request,
                token,
                b'{"echo":true}',
                headers={"Content-Type": "application/json"},
            )
            status, body, _headers = await client_task

        self.assertEqual(request["body"]["encoding"], "base64")
        self.assertEqual(
            json.loads(base64.b64decode(request["body"]["value"])),
            {"hello": "world"},
            repr(request),
        )
        self.assertEqual(invalid_status, 400)
        self.assertEqual((status, body), (200, b'{"echo":true}'))

    async def test_auth_is_enforced_when_configured(self) -> None:
        async with relay_app(sandbox_bearer_token="sandbox-token") as relay:
            await relay.register("rollout-1")
            status, _payload = await relay.model_call(
                "rollout-1",
            )
        self.assertEqual(status, 401)

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
                    {"index": request["body"]["value"]["messages"][0]["content"]},
                )
            duplicate = await relay.respond(
                first[0],
                token,
                {"index": first[0]["body"]["value"]["messages"][0]["content"]},
            )
            await relay.respond(first[0], token, {"changed": True}, expected=409)
            last = (await relay.poll("rollout-batch", token, limit="2"))["requests"][0]
            await relay.respond(
                last,
                token,
                {"index": last["body"]["value"]["messages"][0]["content"]},
            )
            results = await asyncio.gather(*tasks)
            stats = await relay.stats()

        self.assertEqual(len({request["request_id"] for request in first}), 2)
        self.assertTrue(duplicate["duplicate"])
        self.assertTrue(duplicate["committed"])
        self.assertEqual([status for status, _body in results], [200, 200, 200])
        self.assertIn("worker-a", {item["worker_id"] for item in stats["workers"]})
        self.assertEqual(stats["completed_retained"], 3)

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
            )["requests"][0]
            await asyncio.sleep(0.03)
            renew_status, _payload = await relay.request(
                "POST",
                "/worker/renew",
                json={
                    "request_id": first["request_id"],
                    "registration_token": token,
                    "lease_id": first["lease_id"],
                    "lease_seconds": 1,
                },
            )
            second = (
                await relay.poll(
                    "rollout-retry",
                    token,
                    worker_id="fast-worker",
                    lease_seconds="1",
                )
            )["requests"][0]
            await relay.respond(first, token, {"stale": True}, expected=409)
            await relay.respond(second, token, {"ok": True})
            result, stats = await task, await relay.stats()

        self.assertEqual(first["request_id"], second["request_id"])
        self.assertNotEqual(first["lease_id"], second["lease_id"])
        self.assertEqual(renew_status, 409)
        self.assertEqual(second["delivery_count"], 2)
        self.assertEqual(stats["inflight"], 0)
        self.assertEqual(result, (200, {"ok": True}))

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
            )["requests"][0]
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
        self.assertEqual(stats["inflight"], 0)

    async def test_sdk_worker_contract_preserves_request_identity(self) -> None:
        try:
            from ucloud_sandboxes_sdk import AsyncRelayWorkerClient
        except ImportError:
            self.skipTest("requires the SDK")

        rollout_id = "sdk-contract-rollout"
        worker_id = "sdk-contract-worker"
        worker_token = "sdk-contract-token"
        async with relay_app(
            worker_bearer_token=worker_token,
            request_timeout_seconds=5,
        ) as relay:
            async with AsyncRelayWorkerClient(
                relay.base_url,
                worker_token=worker_token,
            ) as client:
                sandbox = type(
                    "Sandbox",
                    (),
                    {
                        "id": "sdk-contract-sandbox",
                        "record": {
                            "generation": 7,
                            "spec": {
                                "parkable": True,
                                "managed_process": True,
                            },
                        },
                    },
                )()
                registration = await client.register_agent_rollout(
                    rollout_id,
                    sandbox,
                )
                registration_token = registration["rollout"]["registration_token"]
                model_call = asyncio.create_task(
                    relay.model_call(
                        rollout_id,
                        body={"model": "contract-model", "messages": []},
                    )
                )
                try:
                    polled = await client.poll(
                        rollout_id,
                        worker_id=worker_id,
                        timeout_seconds=1,
                        lease_seconds=1,
                    )
                    self.assertEqual(len(polled.requests), 1)
                    request = polled.requests[0]
                    self.assertEqual(request.rollout_id, rollout_id)
                    self.assertEqual(request.registration_token, registration_token)
                    self.assertTrue(request.request_id)
                    self.assertTrue(request.lease_id)
                    self.assertEqual(request.leased_by, worker_id)
                    self.assertEqual(request.sandbox_id, "sdk-contract-sandbox")
                    self.assertEqual(request.sandbox_generation, 7)

                    renewed = await client.renew_request(
                        request,
                        worker_id=worker_id,
                        lease_seconds=2,
                    )
                    self.assertEqual(renewed.request_id, request.request_id)
                    self.assertEqual(renewed.rollout_id, request.rollout_id)
                    self.assertEqual(
                        renewed.registration_token,
                        request.registration_token,
                    )
                    self.assertEqual(renewed.lease_id, request.lease_id)
                    self.assertEqual(renewed.sandbox_id, request.sandbox_id)
                    self.assertEqual(
                        renewed.sandbox_generation,
                        request.sandbox_generation,
                    )
                    self.assertGreater(
                        renewed.lease_expires_at or 0,
                        request.lease_expires_at or 0,
                    )

                    responded = await client.respond_to(
                        renewed,
                        {"contract": "ok"},
                        status=201,
                    )
                    self.assertEqual(responded["request_id"], request.request_id)
                    self.assertFalse(responded["duplicate"])
                    self.assertEqual(await model_call, (201, {"contract": "ok"}))
                finally:
                    if not model_call.done():
                        model_call.cancel()
                        try:
                            await model_call
                        except asyncio.CancelledError:
                            pass


if __name__ == "__main__":
    unittest.main()
