"""Primary growth admission is retryable only before supervisor dispatch."""

import asyncio
from dataclasses import replace
import json
from pathlib import Path
from threading import Thread
from unittest import TestCase
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from tests import test_direct_provisioner as node_fixtures
from tests import test_managed_growth as growth_fixtures
from tests.support import requires_sdk
from ucloud_sandboxes.direct_registry import ManagedPrimaryOwnedError
from ucloud_sandboxes.models import NodeRuntimeMetrics, ResourceQuantity, utc_now
from ucloud_sandboxes.sandbox import (
    SandboxCapacityUnavailableError,
    SandboxStartupBusyError,
)

TEST_TIER = "contract"


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

    def dispatched(self):
        return [call.args[1]["job_id"] for call in self.fixture.control.call_args_list]

    def test_timed_out_launch_is_replaced_by_the_next_start(self):
        # The SDK chooses a fresh job id for every start_agent call.
        self.service.start_managed_process("one", self.fixture.spec)
        with self.assertRaises(SandboxStartupBusyError):
            self.service.start_managed_process("two", self.fixture.spec)
        self.service.observe_managed_wait("one", 7, "first-wait")
        fresh = replace(self.fixture.spec, job_id="fresh", argv=("/bin/other",))
        self.assertEqual(self.service.start_managed_process("two", fresh).job_id, "fresh")
        intent = self.fixture.registry.growth_intents()[1]
        self.assertEqual((intent.job_id, intent.phase), ("fresh", "active"))
        self.assertEqual(self.dispatched(), ["primary", "fresh"])
        # A dispatched launch is permanent for its generation.
        with self.assertRaises(ManagedPrimaryOwnedError) as caught:
            self.service.start_managed_process("two", self.fixture.spec)
        self.assertEqual(caught.exception.job_id, "fresh")
        self.assertEqual(self.dispatched(), ["primary", "fresh"])

    def test_replacement_before_activation_fences_the_old_dispatch(self):
        registry = self.fixture.registry
        original = registry.growth_intent
        replaced = False

        def replace_first(sandbox_id, generation, **kwargs):
            nonlocal replaced
            if kwargs["action"] == "activate" and kwargs.get("job_id") == "primary" and not replaced:
                replaced = True
                # A newer start commits its launch while this one is admitted.
                original(sandbox_id, generation, action="launch", job_id="fresh",
                         launch_sha256="f" * 64)
            return original(sandbox_id, generation, **kwargs)

        with patch.object(registry, "growth_intent", side_effect=replace_first):
            with self.assertRaises(ManagedPrimaryOwnedError) as caught:
                self.service.start_managed_process("one", self.fixture.spec)
        self.assertTrue(replaced)
        self.assertEqual(caught.exception.job_id, "fresh")
        self.assertEqual(self.dispatched(), [])
        # The late activation did not charge the replacement's growth.
        self.assertEqual(registry.growth_intents()[0].phase, "queued")
        self.assertEqual(self.service.warm_park_demand().physical_bytes, 0)

    def test_replacement_dispatched_first_fences_the_old_dispatch(self):
        fresh = replace(self.fixture.spec, job_id="fresh")
        original = self.service._admit_managed_growth
        raced = []

        def newer_start_wins(registration, **kwargs):
            if kwargs.get("launch", ("",))[0] == "primary" and not raced:
                # A newer start replaces, admits and dispatches in between.
                raced.append(self.service.start_managed_process("one", fresh))
            return original(registration, **kwargs)

        with patch.object(self.service, "_admit_managed_growth", side_effect=newer_start_wins):
            with self.assertRaises(ManagedPrimaryOwnedError) as caught:
                self.service.start_managed_process("one", self.fixture.spec)
        self.assertEqual(raced[0].job_id, "fresh")
        self.assertEqual(caught.exception.job_id, "fresh")
        self.assertEqual(self.dispatched(), ["fresh"])
        self.assertEqual(self.fixture.registry.growth_intents()[0].phase, "active")

    def test_owned_primary_conflict_is_a_typed_409(self):
        self.service.start_managed_process("one", self.fixture.spec)
        url = self.serve(lambda payload: None)
        body = json.dumps({"job_id": "other", "argv": ["/bin/agent"], "cwd": "/workspace"})
        request = Request(f"{url}/v1/sandboxes/one/jobs", data=body.encode(), method="POST",
                          headers={"Content-Type": "application/json"})
        with self.assertRaises(HTTPError) as caught:
            urlopen(request, timeout=5)
        with caught.exception:
            self.assertEqual(caught.exception.code, 409)
            payload = json.loads(caught.exception.read())
        self.assertEqual(payload["error_code"], "primary_already_owned")
        self.assertEqual(payload["job_id"], "primary")
        self.assertNotIn("retryable", payload)
        self.assertEqual(self.dispatched(), ["primary"])

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
        thread = Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
        thread.start()

        def close():
            server.shutdown()
            server.server_close()
            thread.join(2)

        self.addCleanup(close)
        return f"http://127.0.0.1:{server.server_address[1]}"

    @requires_sdk()
    def test_released_sync_sdk_retries_typed_queue_response(self):
        self.assert_sdk_pressure_retry(asynchronous=False)

    @requires_sdk()
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

    @requires_sdk()
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
