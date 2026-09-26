"""Public gateway replicas: host-wide coordination, shared port, divided budgets."""

import multiprocessing
import os
from pathlib import Path
import socket
from tempfile import TemporaryDirectory
import time
import unittest
from unittest.mock import patch

from ucloud_sandboxes.config import DeploymentConfig
from ucloud_sandboxes.host_locks import HostKeyedLocks
from ucloud_sandboxes.http_server import HighBacklogThreadingHTTPServer, JsonHttpHandler
from ucloud_sandboxes.shared_control.database import (
    GATEWAY_PROCESS_COUNT_ENV,
    process_pool_share,
)


def _hold_in_child(directory, ready, release):
    locks = HostKeyedLocks(Path(directory))
    with locks.hold("migration", "move-1"):
        ready.set()
        release.wait(10)


class HostKeyedLockTests(unittest.TestCase):
    def test_serializes_processes_sharing_the_directory(self):
        with TemporaryDirectory() as raw:
            context = multiprocessing.get_context("spawn")
            ready, release = context.Event(), context.Event()
            child = context.Process(target=_hold_in_child, args=(raw, ready, release))
            child.start()
            try:
                self.assertTrue(ready.wait(10))
                locks = HostKeyedLocks(Path(raw))
                acquired = []

                def other_key_is_independent():
                    # A different namespace never waits on the migration stripe.
                    with locks.hold("image-dispatch", "move-1"):
                        acquired.append("image")

                other_key_is_independent()
                self.assertEqual(acquired, ["image"])
                started = time.monotonic()
                release_at = started + 0.3

                def release_later():
                    time.sleep(max(0, release_at - time.monotonic()))
                    release.set()

                import threading
                threading.Thread(target=release_later, daemon=True).start()
                with locks.hold("migration", "move-1"):
                    waited = time.monotonic() - started
                self.assertGreaterEqual(waited, 0.25)
            finally:
                release.set()
                child.join(10)

    def test_reentrant_and_same_stripe_nesting_does_not_self_deadlock(self):
        with TemporaryDirectory() as raw:
            locks = HostKeyedLocks(Path(raw))
            with patch("ucloud_sandboxes.host_locks._STRIPES", 1):
                with locks.hold("registry-leases", ""):
                    with locks.hold("registry-leases", ""):
                        with locks.hold("registry-leases", "other-key"):
                            pass
            self.assertLessEqual(len(list(Path(raw).iterdir())), 2)

    def test_unconfigured_locks_serialize_threads_only(self):
        locks = HostKeyedLocks()
        with locks.hold("migration", "a"):
            pass


class SharedPortTests(unittest.TestCase):
    def test_replicas_bind_the_same_port(self):
        if not hasattr(socket, "SO_REUSEPORT"):
            self.skipTest("SO_REUSEPORT unavailable")

        class Shared(HighBacklogThreadingHTTPServer):
            reuse_port = True

        first = Shared(("127.0.0.1", 0), JsonHttpHandler)
        try:
            second = Shared(("127.0.0.1", first.server_address[1]), JsonHttpHandler)
            second.server_close()
        finally:
            first.server_close()
        exclusive = HighBacklogThreadingHTTPServer(("127.0.0.1", 0), JsonHttpHandler)
        try:
            with self.assertRaises(OSError):
                HighBacklogThreadingHTTPServer(
                    ("127.0.0.1", exclusive.server_address[1]), JsonHttpHandler
                )
        finally:
            exclusive.server_close()


class BudgetTests(unittest.TestCase):
    def test_pool_share_divides_only_for_gateway_replicas(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop(GATEWAY_PROCESS_COUNT_ENV, None)
            self.assertEqual(process_pool_share(16), 16)
            os.environ[GATEWAY_PROCESS_COUNT_ENV] = "3"
            self.assertEqual(process_pool_share(16), 5)
            os.environ[GATEWAY_PROCESS_COUNT_ENV] = "16"
            self.assertEqual(process_pool_share(16), 4)
            os.environ[GATEWAY_PROCESS_COUNT_ENV] = "junk"
            self.assertEqual(process_pool_share(16), 16)

    def test_gateway_processes_defaults_to_one_and_round_trips(self):
        raw = DeploymentConfig.default(scope_id="project").to_dict()
        raw["deployment_id"] = "replicas"
        raw.pop("gateway_processes")
        config = DeploymentConfig.from_dict(raw)
        self.assertEqual(config.gateway_processes, 1)
        raw["gateway_processes"] = 3
        self.assertEqual(DeploymentConfig.from_dict(raw).to_dict()["gateway_processes"], 3)
        raw["gateway_processes"] = 0
        with self.assertRaises(ValueError):
            DeploymentConfig.from_dict(raw)


if __name__ == "__main__":
    unittest.main()
