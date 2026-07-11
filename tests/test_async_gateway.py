import asyncio
import unittest

from aiohttp import web

from ucloud_sandboxes.async_gateway import (
    AsyncGatewayError,
    AsyncNodeGatewayClient,
)


class AsyncNodeGatewayClientForkTests(unittest.TestCase):
    def test_fork_methods_quote_source_authenticate_and_validate_shapes(self) -> None:
        async def scenario() -> None:
            requests: list[dict[str, object]] = []
            followed_redirect = False

            async def fork(request: web.Request) -> web.Response:
                body = await request.json()
                requests.append(
                    {
                        "path": request.raw_path,
                        "authorization": request.headers.get("Authorization"),
                        "body": body,
                    }
                )
                source_id = request.match_info["source_id"]
                if source_id == "redirect":
                    raise web.HTTPTemporaryRedirect(location="/followed")
                if source_id == "invalid":
                    return web.json_response({"sandboxes": {}, "forks": []})
                if source_id == "invalid-confirmation":
                    return web.json_response(
                        {
                            "intent_persisted": True,
                            "timings": {},
                            "sandbox": {
                                "id": "child",
                                "state": "restoring",
                                "creation_kind": "restore",
                                "source_sandbox_id": source_id,
                                "checkpoint_id": "fork-invalid",
                                "fork_nonce": "c" * 64,
                            },
                            "fork": {
                                "checkpoint_id": "fork-other",
                                "restored": False,
                                "commands": [],
                            },
                        }
                    )
                if "sandboxes" in body:
                    checkpoint_id = "fork-shared"
                    nonce = "a" * 64
                    return web.json_response(
                        {
                            "intent_persisted": True,
                            "timings": {},
                            "sandboxes": [
                                {
                                    "id": item["id"],
                                    "state": "running",
                                    "creation_kind": "restore",
                                    "source_sandbox_id": "parent /?#",
                                    "checkpoint_id": checkpoint_id,
                                    "fork_nonce": nonce,
                                }
                                for item in body["sandboxes"]
                            ],
                            "forks": [
                                {
                                    "sandbox_id": item["id"],
                                    "checkpoint_id": checkpoint_id,
                                    "restored": True,
                                    "commands": [],
                                }
                                for item in body["sandboxes"]
                            ],
                        }
                    )
                return web.json_response(
                    {
                        "intent_persisted": True,
                        "timings": {},
                        "sandbox": {
                            "id": body["sandbox"]["id"],
                            "state": "running",
                            "creation_kind": "restore",
                            "source_sandbox_id": "parent /?#",
                            "checkpoint_id": "fork-one",
                            "fork_nonce": "b" * 64,
                        },
                        "fork": {
                            "checkpoint_id": "fork-one",
                            "restored": True,
                            "commands": [],
                        },
                    }
                )

            async def followed(_request: web.Request) -> web.Response:
                nonlocal followed_redirect
                followed_redirect = True
                return web.json_response({"unexpected": True})

            app = web.Application()
            app.router.add_post("/v1/sandboxes/{source_id:.+}/forks", fork)
            app.router.add_post("/followed", followed)
            runner = web.AppRunner(app)
            await runner.setup()
            site = web.TCPSite(runner, "127.0.0.1", 0)
            await site.start()
            sockets = site._server.sockets if site._server else []
            port = sockets[0].getsockname()[1]
            try:
                async with AsyncNodeGatewayClient(
                    f"http://127.0.0.1:{port}",
                    node_control_bearer_token="node-secret",
                ) as client:
                    singular = await client.fork_sandbox(
                        "parent /?#",
                        {"id": "child-1"},
                    )
                    batch = await client.fork_sandboxes(
                        "parent /?#",
                        ({"id": "child-2"}, {"id": "child-3"}),
                    )
                    with self.assertRaises(AsyncGatewayError):
                        await client.fork_sandbox("redirect", {"id": "child"})
                    with self.assertRaisesRegex(
                        AsyncGatewayError, "fork success"
                    ):
                        await client.fork_sandboxes(
                            "invalid",
                            ({"id": "child"},),
                        )
                    with self.assertRaisesRegex(
                        AsyncGatewayError, "inconsistent fork"
                    ):
                        await client.fork_sandbox(
                            "invalid-confirmation", {"id": "child"}
                        )
            finally:
                await runner.cleanup()

            self.assertEqual(singular["sandbox"]["id"], "child-1")
            self.assertEqual(
                [record["id"] for record in batch["sandboxes"]],
                ["child-2", "child-3"],
            )
            expected_path = "/v1/sandboxes/parent%20%2F%3F%23/forks"
            self.assertEqual(
                [item["path"] for item in requests[:2]],
                [expected_path, expected_path],
            )
            self.assertEqual(
                [item["authorization"] for item in requests],
                ["Bearer node-secret"] * 5,
            )
            self.assertEqual(
                requests[0]["body"],
                {"sandbox": {"id": "child-1"}},
            )
            self.assertEqual(
                requests[1]["body"],
                {
                    "sandboxes": [{"id": "child-2"}, {"id": "child-3"}],
                },
            )
            self.assertFalse(followed_redirect)

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
