import asyncio
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import tempfile
from threading import Thread, Lock
import time
import unittest
from unittest.mock import AsyncMock, patch

from scripts import live_inventory_load as load


class IsolatedInventoryTests(unittest.IsolatedAsyncioTestCase):
    async def test_status_view_preserves_transport_and_does_not_fallback(self):
        records = [{"spec": {"id": "fixture"}, "state": "running", "node": {"job_id": "worker"}}]
        client = AsyncMock()
        client.list_sandbox_statuses.return_value = records
        self.assertEqual(await load.list_inventory(client, "status"), records)
        client.list_sandbox_statuses.assert_awaited_once_with()
        client._request_json.assert_not_awaited()
        client.list_sandboxes.assert_not_awaited()
        client.list_sandbox_statuses.side_effect = RuntimeError("projection unavailable")
        with self.assertRaises(RuntimeError):
            await load.list_inventory(client, "status")
        client.list_sandboxes.assert_not_awaited()

    async def test_default_view_uses_existing_full_sdk_method(self):
        client = AsyncMock()
        client.list_sandboxes.return_value = [{"spec": {"id": "fixture"}}]
        self.assertEqual(await load.list_inventory(client), client.list_sandboxes.return_value)
        client.list_sandboxes.assert_awaited_once_with()
        client.list_sandbox_statuses.assert_not_awaited()
        client._request_json.assert_not_awaited()

    async def test_keeps_polling_after_failure_and_reaps_on_parent_cleanup(self):
        requests = []
        guard = Lock()

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args):
                pass

            def do_GET(self):
                with guard:
                    requests.append(time.monotonic())
                    first = len(requests) == 1
                payload = (
                    {"error": "never-emit-this-response-secret"}
                    if first
                    else {"sandboxes": [{"spec": {"id": "fixture"}}]}
                )
                body = json.dumps(payload).encode()
                self.send_response(500 if first else 200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with tempfile.TemporaryDirectory() as directory:
                token = Path(directory) / "token"
                token.write_text("never-emit-this-token")
                samples = []
                diagnostic = {}
                task = asyncio.create_task(
                    load.isolated_inventory_load(
                        f"http://127.0.0.1:{server.server_port}",
                        token,
                        2,
                        10,
                        samples,
                        diagnostic,
                    )
                )
                try:
                    deadline = time.monotonic() + 8
                    while len(samples) < 4 and time.monotonic() < deadline:
                        await asyncio.sleep(0.02)
                    self.assertGreaterEqual(len(samples), 4)
                    self.assertTrue(any(not row["ok"] for row in samples))
                    self.assertTrue(any(row["ok"] for row in samples))
                    self.assertEqual({row["poller"] for row in samples}, {0, 1})
                    self.assertGreaterEqual(requests[2] - requests[0], 0.95)
                finally:
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
                self.assertEqual(diagnostic["returncode"], 0)
                self.assertTrue(diagnostic["stopped_by_parent"])
                self.assertEqual(diagnostic["completion"]["failed_polls"], 1)
                self.assertNotIn("never-emit", json.dumps([samples, diagnostic]))
                self.assertEqual(
                    diagnostic["completion"]["completed_polls"], len(samples)
                )
                with self.assertRaises(ProcessLookupError):
                    os.kill(diagnostic["pid"], 0)
        finally:
            server.shutdown()
            server.server_close()
            thread.join()

    async def test_unexpected_child_exit_fails_qualification_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            child = Path(directory) / "child.py"
            child.write_text(
                'import json,os,sys\nprint(json.dumps({"event":"ready","pollers":2,"pid":os.getpid()}),flush=True)\nsys.exit(7)\n'
            )
            samples = []
            diagnostic = {}
            with patch.object(load, "__file__", str(child)):
                await load.isolated_inventory_load(
                    "http://unused",
                    Path(directory) / "unused",
                    2,
                    5,
                    samples,
                    diagnostic,
                )
            self.assertEqual(diagnostic["returncode"], 7)
            self.assertTrue(diagnostic["failed"])
            self.assertEqual(samples[0]["ok"], False)


if __name__ == "__main__":
    unittest.main()
