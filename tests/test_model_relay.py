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
                    async with client.post(
                        f"{base}/worker/respond",
                        headers={"Authorization": "Bearer worker-token"},
                        json={
                            "request_id": request_id,
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
        self.assertEqual(body, {"request": None})


if __name__ == "__main__":
    unittest.main()
