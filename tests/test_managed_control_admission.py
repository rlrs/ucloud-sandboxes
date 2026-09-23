from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from ucloud_sandboxes.direct_service import DirectExecTimeoutError, DirectSandboxService
from ucloud_sandboxes.direct_registry import DirectSandboxRegistry
from ucloud_sandboxes.hibernation import HibernationState
from ucloud_sandboxes.managed_process import (
    ManagedProcessError,
    ManagedProcessReadUnavailable,
)
from ucloud_sandboxes.node_agent import NodeAgentHandler
from ucloud_sandboxes.sandbox import SandboxConflictError


class ManagedControlAdmissionTests(unittest.TestCase):
    def setUp(self):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.registration = SimpleNamespace(
            sandbox_id="s",
            sandbox_generation=1,
            operation_id="create",
            to_direct_sandbox=lambda: "sandbox",
        )
        self.current = self.registration

        @contextmanager
        def lease(sandbox, command, **kwargs):
            yield command

        warden = SimpleNamespace(
            config=SimpleNamespace(),
            inspect=lambda sandbox: SimpleNamespace(state=HibernationState.RUNNING),
            exec_lease=lease,
        )
        self.service = DirectSandboxService(
            SimpleNamespace(warden=warden, registry=DirectSandboxRegistry(Path(directory.name).resolve() / "registry.sqlite")),
            process_runner=Mock(),
            max_concurrent_startups=1,
        )
        self.service._require_managed_registration = lambda sandbox: self.current
        self.service._ensure_running = lambda sandbox: None
        self.service.process_runner.run.return_value = self.response({"ok": True})

    @staticmethod
    def response(payload, code=0, stderr=b""):
        return SimpleNamespace(
            exit_code=code,
            stdout=json.dumps(payload).encode() if payload is not None else b"",
            stderr=stderr,
        )

    def call(self, action="status", **kwargs):
        return self.service._managed_control(
            self.registration,
            {"version": 1, "action": action, "job_id": "job"},
            **kwargs,
        )

    def assert_resources_released(self):
        self.assertTrue(self.service._management_read_slots.acquire(blocking=False))
        self.service._management_read_slots.release()
        with self.service._try_lock("s", 1) as available:
            self.assertTrue(available)

    def test_read_exec_timeout_is_typed_retryable_without_replaying_mutation(self):
        for action in ("status", "logs", "start", "signal"):
            with self.subTest(action=action):
                self.service.process_runner.reset_mock()
                self.service.process_runner.run.side_effect = DirectExecTimeoutError(
                    "timeout"
                )
                expected = (
                    ManagedProcessReadUnavailable
                    if action in ("status", "logs")
                    else DirectExecTimeoutError
                )
                with self.assertRaises(expected):
                    self.call(action, retry_not_ready=action == "start")
                self.assertEqual(self.service.process_runner.run.call_count, 1)
                self.assert_resources_released()

    def test_queue_does_not_hold_owner_and_expires_before_dispatch(self):
        self.service._management_read_slots.acquire()
        with patch(
            "ucloud_sandboxes.direct_service._MANAGED_CONTROL_DEADLINE_SECONDS", 0.05
        ):
            with ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(self.call)
                deadline = time.monotonic() + 1
                while (
                    not self.service._management_read_slots.waiting
                    and time.monotonic() < deadline
                ):
                    time.sleep(0.001)
                with self.service._try_lock("s", 1) as available:
                    self.assertTrue(available)
                with self.assertRaisesRegex(ManagedProcessReadUnavailable, "admission"):
                    future.result(timeout=1)
        self.service._management_read_slots.release()
        self.service.process_runner.run.assert_not_called()
        self.assert_resources_released()

    def test_queue_revalidates_generation_before_dispatch(self):
        self.service._management_read_slots.acquire()
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(self.call)
            deadline = time.monotonic() + 1
            while (
                not self.service._management_read_slots.waiting
                and time.monotonic() < deadline
            ):
                time.sleep(0.001)
            self.current = SimpleNamespace(
                **{**vars(self.current), "sandbox_generation": 2}
            )
            self.service._management_read_slots.release()
            with self.assertRaises(SandboxConflictError):
                future.result(timeout=1)
        self.service.process_runner.run.assert_not_called()
        self.assert_resources_released()

    def test_start_retries_only_explicit_before_dispatch_failure_without_locks(self):
        self.service.process_runner.run.side_effect = [
            self.response(
                {
                    "ok": False,
                    "error": "socket unavailable",
                    "error_code": "control_not_connected",
                },
                1,
            ),
            self.response({"ok": True}),
        ]
        with patch(
            "ucloud_sandboxes.direct_service.time.sleep",
            side_effect=lambda delay: self.assert_resources_released(),
        ):
            self.assertEqual(self.call("start", retry_not_ready=True), {"ok": True})
        self.assertEqual(self.service.process_runner.run.call_count, 2)

    def test_authoritative_persistence_error_is_never_hidden_as_overload(self):
        self.service.process_runner.run.return_value = self.response(
            {"ok": False, "error": "persist failed"}, 1
        )
        for action in ("status", "start"):
            with self.subTest(action=action):
                with self.assertRaises(ManagedProcessError) as caught:
                    self.call(action, retry_not_ready=True)
                self.assertNotIsInstance(
                    caught.exception, ManagedProcessReadUnavailable
                )
        self.assertEqual(self.service.process_runner.run.call_count, 2)

    def test_unstructured_start_error_remains_ambiguous_and_is_not_retried(self):
        self.service.process_runner.run.return_value = self.response(
            None, 1, b"connection reset after write"
        )
        with self.assertRaisesRegex(ManagedProcessError, "connection reset"):
            self.call("start", retry_not_ready=True)
        self.assertEqual(self.service.process_runner.run.call_count, 1)

    def test_node_returns_explicit_retryable_read_response(self):
        handler = SimpleNamespace(_write_json=Mock())
        NodeAgentHandler._write_exception(
            handler, ManagedProcessReadUnavailable("timeout")
        )
        self.assertEqual(handler._write_json.call_args.kwargs["status"], 503)
        self.assertEqual(
            handler._write_json.call_args.args[0]["error_code"],
            "managed_process_read_unavailable",
        )
        self.assertTrue(handler._write_json.call_args.args[0]["retryable"])
