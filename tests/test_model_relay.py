from __future__ import annotations

import asyncio
import unittest

from aiohttp import ClientSession, web

from ucloud_sandboxes.model_relay import create_model_relay_app


class ModelRelayTests(unittest.TestCase):
    def test_openai_chat_request_round_trips_through_worker_poll(self) -> None:
        async def scenario() -> tuple[dict, int, dict]:
            app = create_model_relay_app(
                sandbox_bearer_token="sandbox-token",
                worker_bearer_token="worker-token",
                request_timeout_seconds=5,
                worker_poll_timeout_seconds=1,
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
                    async with client.post(
                        f"{base}/register_rollout",
                        headers={"Authorization": "Bearer worker-token"},
                        json={"rollout_id": "rollout-1"},
                    ) as response:
                        self.assertEqual(response.status, 201)

                    async def sandbox_request() -> tuple[int, dict]:
                        async with client.post(
                            f"{base}/rollouts/rollout-1/v1/chat/completions",
                            headers={"Authorization": "Bearer sandbox-token"},
                            json={
                                "model": "local-model",
                                "messages": [{"role": "user", "content": "ping"}],
                            },
                        ) as response:
                            return response.status, await response.json()

                    sandbox_task = asyncio.create_task(sandbox_request())
                    async with client.get(
                        f"{base}/worker/poll",
                        headers={"Authorization": "Bearer worker-token"},
                        params={"rollout_id": "rollout-1", "timeout_seconds": "1"},
                    ) as response:
                        self.assertEqual(response.status, 200)
                        polled = await response.json()

                    request_id = polled["request"]["request_id"]
                    lease_id = polled["request"]["lease_id"]
                    async with client.post(
                        f"{base}/worker/respond",
                        headers={"Authorization": "Bearer worker-token"},
                        json={
                            "request_id": request_id,
                            "lease_id": lease_id,
                            "response": {
                                "id": "chatcmpl-test",
                                "object": "chat.completion",
                                "choices": [
                                    {
                                        "index": 0,
                                        "message": {
                                            "role": "assistant",
                                            "content": "pong",
                                        },
                                        "finish_reason": "stop",
                                    }
                                ],
                            },
                        },
                    ) as response:
                        self.assertEqual(response.status, 200)

                    status, body = await sandbox_task
                    return polled, status, body
            finally:
                await runner.cleanup()

        polled, status, body = asyncio.run(scenario())

        self.assertEqual(status, 200)
        self.assertEqual(polled["request"]["rollout_id"], "rollout-1")
        self.assertEqual(polled["requests"][0]["request_id"], polled["request"]["request_id"])
        self.assertIsInstance(polled["request"]["lease_id"], str)
        self.assertEqual(polled["request"]["endpoint"], "/v1/chat/completions")
        self.assertEqual(polled["request"]["body"]["model"], "local-model")
        self.assertEqual(body["choices"][0]["message"]["content"], "pong")

    def test_plain_v1_endpoint_accepts_rollout_header(self) -> None:
        async def scenario() -> tuple[int, dict]:
            app = create_model_relay_app(request_timeout_seconds=5)
            runner = web.AppRunner(app)
            await runner.setup()
            site = web.TCPSite(runner, "127.0.0.1", 0)
            await site.start()
            sockets = site._server.sockets if site._server else []
            port = sockets[0].getsockname()[1]
            base = f"http://127.0.0.1:{port}"
            try:
                async with ClientSession() as client:
                    async with client.post(
                        f"{base}/register_rollout",
                        json={"rollout_id": "rollout-2"},
                    ) as response:
                        self.assertEqual(response.status, 201)

                    async def sandbox_request() -> tuple[int, dict]:
                        async with client.post(
                            f"{base}/v1/chat/completions",
                            headers={"X-Relay-Rollout-Id": "rollout-2"},
                            json={"model": "m", "messages": []},
                        ) as response:
                            return response.status, await response.json()

                    sandbox_task = asyncio.create_task(sandbox_request())
                    async with client.get(
                        f"{base}/worker/poll",
                        params={"rollout_id": "rollout-2", "timeout_seconds": "1"},
                    ) as response:
                        self.assertEqual(response.status, 200)
                        polled = await response.json()
                    async with client.post(
                        f"{base}/worker/respond",
                        json={
                            "request_id": polled["request"]["request_id"],
                            "lease_id": polled["request"]["lease_id"],
                            "response": {"ok": True},
                        },
                    ) as response:
                        self.assertEqual(response.status, 200)
                    return await sandbox_task
            finally:
                await runner.cleanup()

        status, body = asyncio.run(scenario())

        self.assertEqual(status, 200)
        self.assertEqual(body, {"ok": True})

    def test_auth_is_enforced_when_configured(self) -> None:
        async def scenario() -> int:
            app = create_model_relay_app(sandbox_bearer_token="sandbox-token")
            runner = web.AppRunner(app)
            await runner.setup()
            site = web.TCPSite(runner, "127.0.0.1", 0)
            await site.start()
            sockets = site._server.sockets if site._server else []
            port = sockets[0].getsockname()[1]
            base = f"http://127.0.0.1:{port}"
            try:
                async with ClientSession() as client:
                    async with client.post(
                        f"{base}/v1/chat/completions",
                        headers={"X-Relay-Rollout-Id": "rollout-1"},
                        json={"model": "m", "messages": []},
                    ) as response:
                        return response.status
            finally:
                await runner.cleanup()

        self.assertEqual(asyncio.run(scenario()), 401)

    def test_empty_worker_poll_returns_null_request(self) -> None:
        async def scenario() -> tuple[int, dict]:
            app = create_model_relay_app(worker_poll_timeout_seconds=0)
            runner = web.AppRunner(app)
            await runner.setup()
            site = web.TCPSite(runner, "127.0.0.1", 0)
            await site.start()
            sockets = site._server.sockets if site._server else []
            port = sockets[0].getsockname()[1]
            base = f"http://127.0.0.1:{port}"
            try:
                async with ClientSession() as client:
                    async with client.post(
                        f"{base}/register_rollout",
                        json={"rollout_id": "rollout-empty"},
                    ) as response:
                        self.assertEqual(response.status, 201)
                    async with client.get(
                        f"{base}/worker/poll",
                        params={"rollout_id": "rollout-empty", "timeout_seconds": "0"},
                    ) as response:
                        return response.status, await response.json()
            finally:
                await runner.cleanup()

        status, body = asyncio.run(scenario())

        self.assertEqual(status, 200)
        self.assertEqual(body, {"request": None, "requests": []})

    def test_worker_can_poll_batches_and_respond_idempotently(self) -> None:
        async def scenario() -> tuple[list[dict], dict, dict]:
            app = create_model_relay_app(request_timeout_seconds=5)
            runner = web.AppRunner(app)
            await runner.setup()
            site = web.TCPSite(runner, "127.0.0.1", 0)
            await site.start()
            sockets = site._server.sockets if site._server else []
            port = sockets[0].getsockname()[1]
            base = f"http://127.0.0.1:{port}"
            try:
                async with ClientSession() as client:
                    async with client.post(
                        f"{base}/register_rollout",
                        json={"rollout_id": "rollout-batch"},
                    ) as response:
                        self.assertEqual(response.status, 201)

                    async def sandbox_request(index: int) -> tuple[int, dict]:
                        async with client.post(
                            f"{base}/rollouts/rollout-batch/v1/chat/completions",
                            json={"model": "m", "messages": [{"content": str(index)}]},
                        ) as response:
                            return response.status, await response.json()

                    sandbox_tasks = [
                        asyncio.create_task(sandbox_request(index))
                        for index in range(3)
                    ]
                    for _ in range(100):
                        async with client.get(f"{base}/v1/relay/stats") as response:
                            self.assertEqual(response.status, 200)
                            stats = await response.json()
                        if stats["pending"].get("rollout-batch") == 3:
                            break
                        await asyncio.sleep(0.01)
                    async with client.get(
                        f"{base}/worker/poll",
                        params={
                            "rollout_id": "rollout-batch",
                            "limit": "2",
                            "worker_id": "worker-a",
                        },
                    ) as response:
                        self.assertEqual(response.status, 200)
                        first_poll = await response.json()

                    first_requests = first_poll["requests"]
                    self.assertEqual(len(first_requests), 2)
                    for request in first_requests:
                        async with client.post(
                            f"{base}/worker/respond",
                            json={
                                "request_id": request["request_id"],
                                "lease_id": request["lease_id"],
                                "response": {"index": request["body"]["messages"][0]["content"]},
                            },
                        ) as response:
                            self.assertEqual(response.status, 200)

                    duplicate_request = first_requests[0]
                    async with client.post(
                        f"{base}/worker/respond",
                        json={
                            "request_id": duplicate_request["request_id"],
                            "lease_id": duplicate_request["lease_id"],
                            "response": {"ignored": True},
                        },
                    ) as response:
                        self.assertEqual(response.status, 200)
                        duplicate = await response.json()

                    async with client.get(
                        f"{base}/worker/poll",
                        params={"rollout_id": "rollout-batch", "limit": "2"},
                    ) as response:
                        self.assertEqual(response.status, 200)
                        second_poll = await response.json()
                    self.assertEqual(len(second_poll["requests"]), 1)
                    request = second_poll["request"]
                    async with client.post(
                        f"{base}/worker/respond",
                        json={
                            "request_id": request["request_id"],
                            "lease_id": request["lease_id"],
                            "response": {"index": request["body"]["messages"][0]["content"]},
                        },
                    ) as response:
                        self.assertEqual(response.status, 200)

                    results = [await task for task in sandbox_tasks]
                    async with client.get(f"{base}/v1/relay/stats") as response:
                        self.assertEqual(response.status, 200)
                        stats = await response.json()
                    return first_requests, duplicate, {"results": results, "stats": stats}
            finally:
                await runner.cleanup()

        requests, duplicate, result = asyncio.run(scenario())

        self.assertEqual(len({request["request_id"] for request in requests}), 2)
        self.assertTrue(duplicate["duplicate"])
        self.assertEqual([status for status, _body in result["results"]], [200, 200, 200])
        self.assertEqual(result["stats"]["counters"]["completed"], 3)
        self.assertEqual(result["stats"]["counters"]["duplicate_responses"], 1)
        self.assertEqual(result["stats"]["workers"][0]["worker_id"], "worker-a")

    def test_expired_lease_is_retried_and_stale_response_rejected(self) -> None:
        async def scenario() -> tuple[dict, dict, int, tuple[int, dict], dict]:
            app = create_model_relay_app(request_timeout_seconds=5, worker_lease_seconds=0.01)
            runner = web.AppRunner(app)
            await runner.setup()
            site = web.TCPSite(runner, "127.0.0.1", 0)
            await site.start()
            sockets = site._server.sockets if site._server else []
            port = sockets[0].getsockname()[1]
            base = f"http://127.0.0.1:{port}"
            try:
                async with ClientSession() as client:
                    async with client.post(
                        f"{base}/register_rollout",
                        json={"rollout_id": "rollout-retry"},
                    ) as response:
                        self.assertEqual(response.status, 201)

                    async def sandbox_request() -> tuple[int, dict]:
                        async with client.post(
                            f"{base}/rollouts/rollout-retry/v1/chat/completions",
                            json={"model": "m", "messages": []},
                        ) as response:
                            return response.status, await response.json()

                    sandbox_task = asyncio.create_task(sandbox_request())
                    async with client.get(
                        f"{base}/worker/poll",
                        params={
                            "rollout_id": "rollout-retry",
                            "worker_id": "slow-worker",
                            "lease_seconds": "0.01",
                        },
                    ) as response:
                        self.assertEqual(response.status, 200)
                        first = (await response.json())["request"]

                    await asyncio.sleep(0.03)

                    async with client.get(
                        f"{base}/worker/poll",
                        params={
                            "rollout_id": "rollout-retry",
                            "worker_id": "fast-worker",
                            "lease_seconds": "1",
                        },
                    ) as response:
                        self.assertEqual(response.status, 200)
                        second = (await response.json())["request"]

                    async with client.post(
                        f"{base}/worker/respond",
                        json={
                            "request_id": first["request_id"],
                            "lease_id": first["lease_id"],
                            "response": {"stale": True},
                        },
                    ) as response:
                        stale_status = response.status

                    async with client.post(
                        f"{base}/worker/respond",
                        json={
                            "request_id": second["request_id"],
                            "lease_id": second["lease_id"],
                            "response": {"ok": True},
                        },
                    ) as response:
                        self.assertEqual(response.status, 200)

                    sandbox_result = await sandbox_task
                    async with client.get(f"{base}/v1/relay/stats") as response:
                        stats = await response.json()
                    return first, second, stale_status, sandbox_result, stats
            finally:
                await runner.cleanup()

        first, second, stale_status, sandbox_result, stats = asyncio.run(scenario())

        self.assertEqual(first["request_id"], second["request_id"])
        self.assertNotEqual(first["lease_id"], second["lease_id"])
        self.assertEqual(second["delivery_count"], 2)
        self.assertEqual(stale_status, 409)
        self.assertEqual(sandbox_result, (200, {"ok": True}))
        self.assertEqual(stats["counters"]["lease_expired"], 1)

    def test_worker_heartbeat_updates_stats(self) -> None:
        async def scenario() -> dict:
            app = create_model_relay_app()
            runner = web.AppRunner(app)
            await runner.setup()
            site = web.TCPSite(runner, "127.0.0.1", 0)
            await site.start()
            sockets = site._server.sockets if site._server else []
            port = sockets[0].getsockname()[1]
            base = f"http://127.0.0.1:{port}"
            try:
                async with ClientSession() as client:
                    async with client.post(
                        f"{base}/register_rollout",
                        json={"rollout_id": "rollout-heartbeat"},
                    ) as response:
                        self.assertEqual(response.status, 201)
                    async with client.post(
                        f"{base}/worker/heartbeat",
                        json={
                            "rollout_id": "rollout-heartbeat",
                            "worker_id": "worker-heartbeat",
                            "metadata": {"host": "lumi"},
                        },
                    ) as response:
                        self.assertEqual(response.status, 200)
                    async with client.get(f"{base}/v1/relay/stats") as response:
                        return await response.json()
            finally:
                await runner.cleanup()

        stats = asyncio.run(scenario())

        self.assertEqual(stats["workers"][0]["worker_id"], "worker-heartbeat")
        self.assertEqual(stats["workers"][0]["metadata"], {"host": "lumi"})


if __name__ == "__main__":
    unittest.main()
