"""A slow publication consumes the same budget as Docker execution."""
import fcntl
import io
from pathlib import Path
from tempfile import TemporaryDirectory
import subprocess
import time
import unittest
from unittest.mock import Mock, patch

from ucloud_sandboxes.build_deadline import (
    ImageBuildTimeoutError, build_execution_deadline, remaining_build_execution_seconds,
)
from ucloud_sandboxes.environment_builder import FreshEnvironmentBuilder
from ucloud_sandboxes.image_rootfs import DockerOverlay2RootfsStore
from ucloud_sandboxes.managed_registry import RegistryClient, _read_response_bytes, _upload_blocks
from ucloud_sandboxes.direct_warden import DirectWardenError
from tests.test_image_rootfs import IMAGE_DIGEST, Overlay2Runner, image_store


class TimedOverlayRunner(Overlay2Runner):
    def __init__(self, root, clock):
        super().__init__(root, single_layer=True)
        self.clock = clock
        self.budgets = []
        self.remount_timeout = False
        self.unmount_timeout = False

    def run(self, argv, *, timeout):
        self.budgets.append((tuple(argv), timeout))
        if self.remount_timeout and "remount,bind,ro" in argv:
            self.clock[0] += 1
            raise subprocess.TimeoutExpired(argv, timeout)
        if self.unmount_timeout and argv[0] == "umount":
            raise subprocess.TimeoutExpired(argv, timeout)
        return super().run(argv, timeout=timeout)


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

    def test_direct_inspect_mount_and_discard_use_the_active_budget(self):
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            clock = [100.0]
            runner = TimedOverlayRunner(root / "docker", clock)
            store = image_store(root, runner)
            with patch("ucloud_sandboxes.build_deadline.time.monotonic", side_effect=lambda: clock[0]):
                with build_execution_deadline(0.25):
                    store.image_content_id("example/image:latest")
                    store.image_tag_times(("sha256:" + IMAGE_DIGEST,))
                    with store.operation_lease("example/image:latest") as image:
                        pass
                    self.assertTrue(store.collect_image(image.image_id, is_referenced=lambda _: False))
            self.assertTrue(any(command[0] == "mountpoint" for command, _ in runner.budgets))
            self.assertTrue(any(command[0] == "umount" for command, _ in runner.budgets))
            self.assertTrue(all(0 < timeout <= 0.25 for _, timeout in runner.budgets))
            runner.budgets.clear()
            store.image_content_id("example/image:latest")
            self.assertEqual(runner.budgets[0][1], 60)

    def test_expired_readonly_remount_cleans_partial_bind_with_separate_budget(self):
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            clock = [100.0]
            runner = TimedOverlayRunner(root / "docker", clock)
            runner.remount_timeout = True
            store = image_store(root, runner)
            with patch("ucloud_sandboxes.build_deadline.time.monotonic", side_effect=lambda: clock[0]):
                with build_execution_deadline(0.25):
                    with self.assertRaises(subprocess.TimeoutExpired):
                        with store.operation_lease("example/image:latest"):
                            self.fail("timed-out remount was accepted")
                    with self.assertRaises(ImageBuildTimeoutError):
                        remaining_build_execution_seconds()
            self.assertEqual(runner.mounted, set())
            self.assertEqual(runner.pins, {})
            self.assertFalse((store.images / IMAGE_DIGEST).exists())
            self.assertEqual(store.operation_snapshot()["active_operations"], 0)
            cleanup_budgets = [timeout for command, timeout in runner.budgets
                               if command[0] in {"mountpoint", "umount"} or command[:3] == ("docker", "image", "rm")]
            self.assertTrue(cleanup_budgets)
            self.assertTrue(all(0 < timeout <= 10 for timeout in cleanup_budgets))
            self.assertTrue(any(timeout > 0.25 for timeout in cleanup_budgets))

    def test_failed_cleanup_preserves_partial_mount_and_pin_for_gc_retry(self):
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            runner = TimedOverlayRunner(root / "docker", [100.0])
            runner.remount_timeout = runner.unmount_timeout = True
            store = image_store(root, runner)
            with build_execution_deadline(0.25), self.assertRaisesRegex(DirectWardenError, "could not be released"):
                with store.operation_lease("example/image:latest"):
                    self.fail("failed cleanup was ignored")
            target = store.images / IMAGE_DIGEST
            self.assertTrue(target.is_dir())
            self.assertIn(str(target / "rootfs"), runner.mounted)
            self.assertIn(IMAGE_DIGEST, runner.pins)
            runner.unmount_timeout = False
            self.assertTrue(store.collect_image("sha256:" + IMAGE_DIGEST, is_referenced=lambda _: False))
            self.assertFalse(target.exists())
            self.assertEqual(runner.pins, {})

    def test_collection_subprocess_timeout_defers_without_losing_gc_state(self):
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            clock = [100.0]
            runner = TimedOverlayRunner(root / "docker", clock)
            store = image_store(root, runner)
            with store.operation_lease("example/image:latest") as image:
                pass
            runner.unmount_timeout = True
            builder = FreshEnvironmentBuilder(store, None, None, root / "scratch")
            runner.budgets.clear()
            with patch("ucloud_sandboxes.build_deadline.time.monotonic", side_effect=lambda: clock[0]):
                with build_execution_deadline(0.25):
                    clock[0] += 1
                    with self.assertLogs("ucloud_sandboxes.environment_builder", level="WARNING"):
                        builder._collect_image(image.image_id)
                    with self.assertRaises(ImageBuildTimeoutError):
                        remaining_build_execution_seconds()
            self.assertTrue(all(0 < timeout <= 10 for _, timeout in runner.budgets))
            self.assertTrue(image.rootfs.parent.is_dir())
            self.assertIn(IMAGE_DIGEST, runner.pins)
            runner.unmount_timeout = False
            builder._collect_image(image.image_id)
            self.assertFalse(image.rootfs.parent.exists())
            self.assertEqual(runner.pins, {})

    def test_existing_cache_remount_timeout_releases_bind_and_preserves_metadata(self):
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            runner = TimedOverlayRunner(root / "docker", [100.0])
            store = image_store(root, runner)
            with store.operation_lease("example/image:latest") as image:
                pass
            runner.mounted.clear()  # Simulate a cache restored after reboot.
            runner.remount_timeout = True
            with build_execution_deadline(0.25), self.assertRaises(subprocess.TimeoutExpired):
                with store.operation_lease("example/image:latest"):
                    self.fail("timed-out remount was accepted")
            self.assertEqual(runner.mounted, set())
            self.assertTrue((image.rootfs.parent / store.COMPLETE).is_file())
            self.assertIn(IMAGE_DIGEST, runner.pins)
            runner.remount_timeout = False
            with store.operation_lease("example/image:latest") as recovered:
                self.assertEqual(recovered.image_id, image.image_id)

    def test_existing_partial_bind_is_not_trusted_when_cleanup_also_times_out(self):
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            runner = TimedOverlayRunner(root / "docker", [100.0])
            store = image_store(root, runner)
            with store.operation_lease("example/image:latest") as image:
                pass
            runner.mounted.clear()
            runner.remount_timeout = runner.unmount_timeout = True
            with build_execution_deadline(0.25), self.assertRaises(subprocess.TimeoutExpired):
                with store.operation_lease("example/image:latest"):
                    self.fail("partial bind was accepted")
            self.assertIn(str(image.rootfs), runner.mounted)
            self.assertFalse((image.rootfs.parent / store.COMPLETE).exists())
            self.assertIn(IMAGE_DIGEST, runner.pins)
            with build_execution_deadline(0.25), self.assertRaises(subprocess.TimeoutExpired):
                with store.operation_lease("example/image:latest"):
                    self.fail("later lease trusted the still-mounted incomplete bind")
            runner.remount_timeout = runner.unmount_timeout = False
            with store.operation_lease("example/image:latest") as recovered:
                self.assertEqual(recovered.image_id, image.image_id)
            self.assertTrue((image.rootfs.parent / store.COMPLETE).is_file())


if __name__ == "__main__":
    unittest.main()
