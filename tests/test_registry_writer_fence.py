"""The writer lifetime, not elapsed time, decides when collection may start."""
from concurrent.futures import ThreadPoolExecutor
import subprocess
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Event
import time
import unittest
from unittest.mock import patch

from tests.test_registry_disk import filesystem_config
from tests.test_registry_sweep import FakeDistribution
from ucloud_sandboxes.systemd import (
    run_registry_process, run_registry_sweep, registry_restart_marker,
    main,
)
from ucloud_sandboxes.registry_sweep import RegistrySweepResult


def completed(command, stdout=""):
    return subprocess.CompletedProcess(command, 0, stdout, "")


class RegistryWriterFenceTests(unittest.TestCase):
    def test_failure_recovery_only_starts_a_registry_stopped_by_collection(self):
        with TemporaryDirectory() as raw:
            writer = Path(raw) / "writer"
            with patch("ucloud_sandboxes.systemd.REGISTRY_WRITER_LOCK", writer), \
                    patch("ucloud_sandboxes.systemd.subprocess.run") as runner:
                self.assertEqual(main(["registry-recover"]), 0)
                runner.assert_not_called()
                registry_restart_marker(writer).touch()
                self.assertEqual(main(["registry-recover"]), 0)
                runner.assert_called_once_with(["systemctl", "start", "--no-block",
                    "ucloud-sandbox-registry.service"], check=True, text=True)

    def test_a_delayed_manifest_commit_finishes_before_collection(self):
        with TemporaryDirectory() as raw, ThreadPoolExecutor(2) as pool:
            root = Path(raw)
            config = filesystem_config(root)
            registry = FakeDistribution(config.registry_data_dir(), now=time.time())
            layer = registry.blob(b"reused-old-layer")
            registry.layer_link("repo", layer)
            verified, finish, stop_called, inspected = Event(), Event(), Event(), Event()

            def writer(command, **kwargs):
                if command[:2] == ["docker", "run"]:
                    verified.set()  # PUT has verified its old blob.
                    if not finish.wait(10):
                        raise TimeoutError("test writer was not released")
                    registry.manifest("repo", layers=[layer], age=0)
                return completed(command)

            def coordinator(command, **kwargs):
                if command[:2] == ["systemctl", "stop"]:
                    stop_called.set()
                if command[:2] == ["docker", "ps"]:
                    inspected.set()
                return completed(command)

            old = pool.submit(run_registry_process, config, writer_lock=root / "writer", runner=writer, environ={})
            try:
                self.assertTrue(verified.wait(5))
                gc = pool.submit(run_registry_sweep, config=config, lock_file=root / "maintenance",
                                 writer_lock=root / "writer", runner=coordinator)
                self.assertTrue(stop_called.wait(5))
                self.assertFalse(inspected.wait(0.1))
                self.assertTrue(registry.exists(layer))
            finally:
                finish.set()
            old.result(timeout=5)
            gc.result(timeout=5)
            self.assertTrue(registry.exists(layer))
            self.assertTrue(inspected.is_set())

    def test_startup_and_stale_container_cleanup_wait_for_collection(self):
        with TemporaryDirectory() as raw, ThreadPoolExecutor(2) as pool:
            root = Path(raw)
            config = filesystem_config(root)
            collecting, finish, started = Event(), Event(), Event()

            def sweep(*args, **kwargs):
                collecting.set()
                if not finish.wait(10):
                    raise TimeoutError("test collection was not released")
                return RegistrySweepResult()

            def writer(command, **kwargs):
                started.set()  # Even docker rm must stay behind the fence.
                return completed(command)

            gc = pool.submit(run_registry_sweep, config=config, lock_file=root / "maintenance",
                             writer_lock=root / "writer", sweep=sweep,
                             runner=lambda command, **kw: completed(command))
            try:
                self.assertTrue(collecting.wait(5))
                startup = pool.submit(run_registry_process, config, writer_lock=root / "writer", runner=writer, environ={})
                self.assertFalse(started.wait(0.1))
            finally:
                finish.set()
            gc.result(timeout=5)
            startup.result(timeout=5)
            self.assertTrue(started.is_set())

    def test_remaining_container_or_failed_collection_restarts_without_false_success(self):
        for live_container in (True, False):
            with self.subTest(live_container=live_container), TemporaryDirectory() as raw:
                root = Path(raw)
                calls, collected = [], []

                def runner(command, **kwargs):
                    calls.append(command)
                    return completed(command, "container-id\n" if live_container and command[:2] == ["docker", "ps"] else "")

                def sweep(*args, **kwargs):
                    collected.append(True)
                    raise RuntimeError("unreadable tree")

                with self.assertRaises(RuntimeError):
                    run_registry_sweep(config=filesystem_config(root), lock_file=root / "maintenance",
                                       writer_lock=root / "writer", runner=runner, sweep=sweep)
                self.assertEqual(bool(collected), not live_container)
                self.assertEqual(calls[-1][:2], ["systemctl", "start"])
                self.assertFalse(registry_restart_marker(root / "writer").exists())
