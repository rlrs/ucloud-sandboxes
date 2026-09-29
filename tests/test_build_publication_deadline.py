"""A slow publication consumes the same budget as Docker execution."""
import fcntl
import io
from pathlib import Path
from tempfile import TemporaryDirectory
import time
import unittest
from unittest.mock import Mock, patch

from ucloud_sandboxes.build_deadline import (
    ImageBuildTimeoutError, build_execution_deadline, remaining_build_execution_seconds,
)
from ucloud_sandboxes.environment_builder import FreshEnvironmentBuilder
from ucloud_sandboxes.image_rootfs import DockerOverlay2RootfsStore
from ucloud_sandboxes.managed_registry import RegistryClient, _read_response_bytes, _upload_blocks


class BuildPublicationDeadlineTests(unittest.TestCase):
    def test_waiting_for_same_layer_expires_and_releases_its_descriptor(self):
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            builder = FreshEnvironmentBuilder(None, None, None, root)
            with builder._group_lock("group"):
                with build_execution_deadline(0.05), self.assertRaises(ImageBuildTimeoutError):
                    with builder._group_lock("group"):
                        self.fail("contended lock was acquired")
            # Timeout must not leave another lease or poison future operations.
            with builder._group_lock("group"):
                pass

    def test_materialization_queue_timeout_does_not_leak_capacity(self):
        with TemporaryDirectory() as temporary:
            store = DockerOverlay2RootfsStore(Path(temporary), max_concurrent_operations=1)
            with store._operation_slot():
                with build_execution_deadline(0.02), self.assertRaises(ImageBuildTimeoutError):
                    with store._operation_slot():
                        self.fail("capacity was exceeded")
                self.assertEqual(store.operation_snapshot()["waiting_operations"], 0)
            with store._operation_slot():
                self.assertEqual(store.operation_snapshot()["active_operations"], 1)
            self.assertEqual(store.operation_snapshot()["active_operations"], 0)

    def test_digest_lock_wait_consumes_budget(self):
        with TemporaryDirectory() as temporary:
            store = DockerOverlay2RootfsStore(Path(temporary))
            with store._locked("a" * 64):
                descriptor = store._open_digest_lock("a" * 64)
                try:
                    with build_execution_deadline(0.02), self.assertRaises(ImageBuildTimeoutError):
                        store._acquire_digest_lock(descriptor, fcntl.LOCK_EX)
                finally:
                    import os
                    os.close(descriptor)
            self.assertEqual(store.operation_snapshot()["waiting_operations"], 0)

    def test_registry_connection_and_streaming_share_execution_budget(self):
        client = RegistryClient("http://registry.invalid", timeout_seconds=30)
        with build_execution_deadline(0.05):
            with patch("ucloud_sandboxes.managed_registry.request.urlopen") as opened:
                client._request("/v2/", timeout_seconds=600)
                self.assertGreater(opened.call_args.kwargs["timeout"], 0)
                self.assertLessEqual(opened.call_args.kwargs["timeout"], 0.05)

            class SlowResponse:
                def read1(self, size):
                    time.sleep(0.06)
                    return b"x"
                read = read1

            with self.assertRaises(ImageBuildTimeoutError):
                _read_response_bytes(SlowResponse(), 100, deadline=None)
            with self.assertRaises(ImageBuildTimeoutError):
                next(_upload_blocks(io.BytesIO(b"payload")))

    def test_mkfs_receives_remaining_budget(self):
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            builder = FreshEnvironmentBuilder(None, None, None, root)
            with build_execution_deadline(0.1), patch(
                "ucloud_sandboxes.environment_builder.subprocess.run"
            ) as run:
                builder._mkfs(root / "out", root, exclude_runtime_mounts=True)
                self.assertLessEqual(run.call_args.kwargs["timeout"], 0.1)
                self.assertGreater(run.call_args.kwargs["timeout"], 0)

    def test_docker_fallback_command_receives_remaining_budget(self):
        with TemporaryDirectory() as temporary:
            runner = Mock()
            runner.run.return_value = Mock(returncode=0, stdout="ok")
            store = DockerOverlay2RootfsStore(Path(temporary), runner=runner)
            with build_execution_deadline(0.1):
                self.assertEqual(store._checked("docker", "pull", "test", timeout=600), "ok")
                self.assertLessEqual(runner.run.call_args.kwargs["timeout"], 0.1)
                self.assertIsNotNone(remaining_build_execution_seconds())

    def test_cleanup_gets_a_separate_bound_and_defers_contended_collection(self):
        store = Mock()
        builder = FreshEnvironmentBuilder(store, None, None, Path("/unused"))
        budgets = []
        def collect(*args, **kwargs):
            budgets.append(remaining_build_execution_seconds())
            raise ImageBuildTimeoutError("fixture holds image lease")
        store.collect_image.side_effect = collect
        with build_execution_deadline(0.01):
            time.sleep(0.02)
            with self.assertLogs("ucloud_sandboxes.environment_builder", level="WARNING"):
                builder._collect_image("sha256:" + "a" * 64)
            self.assertGreater(budgets[0], 9)
            self.assertLessEqual(budgets[0], 10)
            with self.assertRaises(ImageBuildTimeoutError):
                remaining_build_execution_seconds()


if __name__ == "__main__":
    unittest.main()
