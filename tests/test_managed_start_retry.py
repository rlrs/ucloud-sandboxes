"""Primary growth admission is retryable only before supervisor dispatch."""

import asyncio
import importlib.util
from pathlib import Path
from threading import Thread
from unittest import TestCase, skipUnless

from tests import test_direct_provisioner as node_fixtures
from tests import test_managed_growth as growth_fixtures
from ucloud_sandboxes.models import NodeRuntimeMetrics, ResourceQuantity, utc_now
from ucloud_sandboxes.sandbox import (
    SandboxCapacityUnavailableError,
    SandboxStartupBusyError,
)


class ManagedStartRetryTests(TestCase):
    def setUp(self):
        self.fixture = growth_fixtures.ManagedGrowthTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.service = self.fixture.service
        self.service.admission_wait_seconds = 0.05
        # These HTTP fixtures have durable registry ownership but no native
        # sentry. Background memory observation must explicitly see no PID.
        for record in self.service.warden.records.values():
            record.sentry_pid = 0

    def test_pressure_timeout_retains_queued_identity_and_has_no_dispatch(self):
        self.service.start_managed_process("one", self.fixture.spec)
        with self.assertRaises(SandboxStartupBusyError):
            self.service.start_managed_process("two", self.fixture.spec)
        queued = self.fixture.registry.growth_intents()[1]
        self.assertEqual(queued.phase, "queued")
        self.assertEqual(self.fixture.control.call_count, 1)
        self.assertFalse(self.service._transitions.foreground_waiting)
        self.service.observe_managed_wait("one", 7, "first-wait")
        self.assertEqual(
            self.service.start_managed_process("two", self.fixture.spec).job_id,
            "primary",
        )
        active = self.fixture.registry.growth_intents()[1]
        self.assertEqual(
            (active.job_id, active.launch_sha256), (queued.job_id, queued.launch_sha256)
        )
        self.assertEqual(active.phase, "active")
        self.assertEqual(self.fixture.control.call_count, 2)

    def test_capacity_shaped_supervisor_failure_is_not_reclassified(self):
        error = SandboxCapacityUnavailableError("ambiguous supervisor failure")
        self.fixture.control.side_effect = error
        with self.assertRaises(SandboxCapacityUnavailableError) as result:
            self.service.start_managed_process("one", self.fixture.spec)
        self.assertIs(result.exception, error)
        self.assertNotIsInstance(result.exception, SandboxStartupBusyError)
        self.assertEqual(self.fixture.registry.growth_intents()[0].phase, "active")
        self.assertEqual(self.fixture.control.call_count, 1)

    def serve(self, on_response):
        server = node_fixtures.build_direct_node_agent_server(
            "127.0.0.1",
            0,
            service=self.service,
            image_file=Path(self.fixture.tmp.name) / "images.json",
            job_id="worker",
            node_id="worker",
            total_resources=ResourceQuantity(vcpu=4, memory_mb=8192),
            runtime_metrics_provider=lambda: NodeRuntimeMetrics(
                collected_at=utc_now(),
                cpu_percent=0,
                cpu_count=4,
                memory_total_mb=8192,
                memory_available_mb=self.fixture.available,
            ),
        )
        original = server.RequestHandlerClass._write_json

        def write(handler, payload, **kwargs):
            on_response(payload)
            return original(handler, payload, **kwargs)

        server.RequestHandlerClass._write_json = write
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()

        def close():
            server.shutdown()
            server.server_close()
            thread.join(2)

        self.addCleanup(close)
        return f"http://127.0.0.1:{server.server_address[1]}"

    @skipUnless(
        importlib.util.find_spec("ucloud_sandboxes_sdk"), "released SDK unavailable"
    )
    def test_released_sync_sdk_retries_typed_queue_response(self):
        self.assert_sdk_pressure_retry(asynchronous=False)

    @skipUnless(
        importlib.util.find_spec("ucloud_sandboxes_sdk"), "released SDK unavailable"
    )
    def test_released_async_sdk_retries_typed_queue_response(self):
        self.assert_sdk_pressure_retry(asynchronous=True)

    def assert_sdk_pressure_retry(self, *, asynchronous):
        from ucloud_sandboxes_sdk import SandboxClient, AsyncSandboxClient

        self.service.start_managed_process("one", self.fixture.spec)
        responses = []

        def observe(payload):
            responses.append(payload)
            if payload.get("error_code") == "node_startup_busy":
                self.assertTrue(payload["retryable"])
                self.assertEqual(
                    self.fixture.registry.growth_intents()[1].phase, "queued"
                )
                self.service.observe_managed_wait("one", 7, "first-wait")

        url = self.serve(observe)

        async def start():
            async with AsyncSandboxClient(url, timeout_seconds=5) as client:
                return await client.start_agent(
                    "two", ["/bin/agent"], job_id="primary", working_dir="/workspace"
                )

        if asynchronous:
            job = asyncio.run(start())
        else:
            job = SandboxClient(url, timeout_seconds=5).start_agent(
                "two", ["/bin/agent"], job_id="primary", working_dir="/workspace"
            )
        self.assertEqual(job.job_id, "primary")
        self.assertEqual(
            sum(p.get("error_code") == "node_startup_busy" for p in responses), 1
        )
        self.assertEqual(self.fixture.control.call_count, 2)

    @skipUnless(
        importlib.util.find_spec("ucloud_sandboxes_sdk"), "released SDK unavailable"
    )
    def test_released_sdk_does_not_retry_ambiguous_primary_dispatch(self):
        from ucloud_sandboxes_sdk import SandboxClient, SandboxApiError

        self.fixture.control.side_effect = SandboxCapacityUnavailableError(
            "ambiguous supervisor failure"
        )
        responses = []
        url = self.serve(responses.append)
        client = SandboxClient(url, timeout_seconds=5)
        with self.assertRaises(SandboxApiError):
            client.start_agent(
                "one", ["/bin/agent"], job_id="primary", working_dir="/workspace"
            )
        self.assertEqual(self.fixture.control.call_count, 1)
        self.assertEqual(len(responses), 1)
        self.assertNotIn("retryable", responses[0])
