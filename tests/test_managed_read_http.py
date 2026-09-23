"""Read admission exceptions reach their retryable HTTP contract, before parent errors."""

import asyncio
import importlib.util
from unittest import TestCase, skipUnless

from tests import test_managed_start_retry as fixtures
from ucloud_sandboxes.managed_process import (
    ManagedProcessError,
    ManagedProcessReadUnavailable,
)


@skipUnless(
    importlib.util.find_spec("ucloud_sandboxes_sdk"), "released SDK unavailable"
)
class ManagedReadHttpTests(TestCase):
    def setUp(self):
        self.fixture = fixtures.ManagedStartRetryTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.responses = []
        self.url = self.fixture.serve(self.responses.append)
        self.attempts = {}

    def response(self, registration, payload, **kwargs):
        action = payload["action"]
        self.attempts[action] = self.attempts.get(action, 0) + 1
        if self.attempts[action] == 1:
            raise ManagedProcessReadUnavailable(
                "managed process read timed out; retry the read"
            )
        if action == "logs":
            return {
                "ok": True,
                "stream": "stdout",
                "offset": 0,
                "next_offset": 2,
                "data": "b2s=",
                "eof": False,
            }
        return self.fixture.fixture.response(registration, payload, **kwargs)

    def assert_retried_read_contract(self):
        self.assertEqual(self.attempts, {"status": 2, "logs": 2})
        errors = [response for response in self.responses if "error" in response]
        self.assertEqual(len(errors), 2)
        for error in errors:
            self.assertTrue(error["retryable"])
            self.assertEqual(error["error_code"], "managed_process_read_unavailable")

    def test_sync_sdk_retries_status_and_logs_from_actual_handlers(self):
        from ucloud_sandboxes_sdk import SandboxClient

        self.fixture.fixture.control.side_effect = self.response
        client = SandboxClient(self.url, timeout_seconds=5)
        self.assertEqual(client.get_job("one", "primary").job_id, "primary")
        self.assertEqual(client.read_job_logs("one", "primary").data, b"ok")
        self.assert_retried_read_contract()

    def test_async_sdk_retries_status_and_logs_from_actual_handlers(self):
        from ucloud_sandboxes_sdk import AsyncSandboxClient

        self.fixture.fixture.control.side_effect = self.response

        async def run():
            async with AsyncSandboxClient(self.url, timeout_seconds=5) as client:
                self.assertEqual(
                    (await client.get_job("one", "primary")).job_id, "primary"
                )
                self.assertEqual(
                    (await client.read_job_logs("one", "primary")).data, b"ok"
                )

        asyncio.run(run())
        self.assert_retried_read_contract()

    def test_semantic_status_and_log_conflicts_remain409_without_retry(self):
        from ucloud_sandboxes_sdk import SandboxClient, SandboxApiError

        self.fixture.fixture.control.side_effect = ManagedProcessError(
            "unknown managed job"
        )
        client = SandboxClient(self.url, timeout_seconds=5)
        for operation in (client.get_job, client.read_job_logs):
            with self.assertRaises(SandboxApiError) as caught:
                operation("one", "primary")
            self.assertEqual(caught.exception.status_code, 409)
            self.assertNotIn("retryable", caught.exception.body)
        self.assertEqual(self.fixture.fixture.control.call_count, 2)
