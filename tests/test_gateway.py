import asyncio
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from threading import Thread
import unittest

from ucloud_sandboxes.gateway import GatewayError, NodeGatewayClient


class _ForkGatewayHandler(BaseHTTPRequestHandler):
    requests: list[dict[str, object]] = []
    followed_redirect = False

    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        body = json.loads(self.rfile.read(length))
        type(self).requests.append(
            {
                "path": self.path,
                "authorization": self.headers.get("Authorization"),
                "body": body,
            }
        )
        if self.path == "/followed":
            type(self).followed_redirect = True
            self._write_json({"unexpected": True})
            return
        if self.path == "/v1/sandboxes/redirect/forks":
            self.send_response(307)
            self.send_header("Location", "/followed")
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if self.path == "/v1/sandboxes/invalid/forks":
            self._write_json({"sandbox": [], "fork": {}})
            return
        if self.path == "/v1/sandboxes/invalid-confirmation/forks":
            self._write_json(
                {
                    "intent_persisted": True,
                    "timings": {},
                    "sandbox": {
                        "id": "child",
                        "state": "restoring",
                        "creation_kind": "restore",
                        "source_sandbox_id": "invalid-confirmation",
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
            return
        if "sandboxes" in body:
            checkpoint_id = "fork-shared"
            nonce = "a" * 64
            self._write_json(
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
            return
        self._write_json(
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

    def _write_json(self, payload: object) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, _format: str, *args: object) -> None:
        del args


class NodeGatewayClientForkTests(unittest.TestCase):
    def setUp(self) -> None:
        _ForkGatewayHandler.requests = []
        _ForkGatewayHandler.followed_redirect = False
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), _ForkGatewayHandler)
        self.thread = Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        host, port = self.server.server_address
        self.client = NodeGatewayClient(
            f"http://{host}:{port}",
            node_control_bearer_token="node-secret",
        )

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def test_fork_methods_quote_source_and_send_expected_payloads(self) -> None:
        async def scenario() -> tuple[dict, dict]:
            singular = await self.client.fork_sandbox(
                "parent /?#",
                {"id": "child-1"},
            )
            batch = await self.client.fork_sandboxes(
                "parent /?#",
                ({"id": "child-2"}, {"id": "child-3"}),
            )
            return singular, batch

        singular, batch = asyncio.run(scenario())

        self.assertEqual(singular["sandbox"]["id"], "child-1")
        self.assertEqual(
            [record["id"] for record in batch["sandboxes"]],
            ["child-2", "child-3"],
        )
        expected_path = "/v1/sandboxes/parent%20%2F%3F%23/forks"
        self.assertEqual(
            [item["path"] for item in _ForkGatewayHandler.requests],
            [expected_path, expected_path],
        )
        self.assertEqual(
            [item["authorization"] for item in _ForkGatewayHandler.requests],
            ["Bearer node-secret", "Bearer node-secret"],
        )
        self.assertEqual(
            _ForkGatewayHandler.requests[0]["body"],
            {"sandbox": {"id": "child-1"}},
        )
        self.assertEqual(
            _ForkGatewayHandler.requests[1]["body"],
            {
                "sandboxes": [{"id": "child-2"}, {"id": "child-3"}],
            },
        )

    def test_fork_methods_reject_redirects_and_invalid_shapes(self) -> None:
        async def scenario() -> None:
            with self.assertRaises(GatewayError):
                await self.client.fork_sandbox("redirect", {"id": "child"})
            with self.assertRaisesRegex(GatewayError, "fork success"):
                await self.client.fork_sandbox("invalid", {"id": "child"})
            with self.assertRaisesRegex(GatewayError, "inconsistent fork"):
                await self.client.fork_sandbox(
                    "invalid-confirmation", {"id": "child"}
                )

        asyncio.run(scenario())

        self.assertFalse(_ForkGatewayHandler.followed_redirect)


if __name__ == "__main__":
    unittest.main()
