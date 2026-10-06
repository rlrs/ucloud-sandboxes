"""Phase 2a of the Rust node daemon, the agent's half (docs/rust-node-daemon-plan.md).

runtime/noded runs execs on running sandboxes without asking the agent. The
agent's lifecycle transitions fence them through two flock files per sandbox
and read their activity from one file's mtime (ucloud_sandboxes/exec_fence.py).
A subprocess plays noded here: it takes the locks exactly as noded does.
"""

import hashlib
from http.client import HTTPConnection
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
from tempfile import TemporaryDirectory
from threading import Event, Thread
import time
import unittest
from unittest.mock import patch

from tests import test_direct_provisioner as fixtures
from tests import test_vm_init as vm_init_fixtures
from ucloud_sandboxes import exec_fence
from ucloud_sandboxes.cli import build_parser, vm_init_options_to_dict
from ucloud_sandboxes.config import DeploymentConfig
from ucloud_sandboxes.direct_service import DirectSandboxService
from ucloud_sandboxes.node_runtime import DirectNodeRuntime
from ucloud_sandboxes.sandbox import SandboxBusyError
from ucloud_sandboxes.vm_init import render_vm_init_script

TEST_TIER = "contract"
TOKEN = "node-secret"
# noded's side: T shared and non-blocking, A shared, T released, A kept.
_NODED = """
import fcntl, os, sys
flags = os.O_RDWR | os.O_CREAT | os.O_CLOEXEC | os.O_NOFOLLOW
held = []
for path in sys.argv[1:]:
    fd = os.open(path, flags, 0o600)
    fcntl.flock(fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
    held.append(fd)
print("held", flush=True)
sys.stdin.read()
"""


class _Noded:
    """A process holding the given fence files shared until closed."""

    def __init__(self, *paths: Path) -> None:
        self.process = subprocess.Popen([sys.executable, "-c", _NODED, *map(str, paths)],
                                        stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
        if self.process.stdout.readline().strip() != "held":
            raise AssertionError("the fence holder did not start")

    def close(self) -> None:
        self.process.stdin.close()
        self.process.wait(5)
        self.process.stdout.close()


class ExecFenceRuntimeTests(unittest.TestCase):
    fixture = fixtures.DirectProvisionerTests()

    def setUp(self) -> None:
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        root = Path(directory.name).resolve()
        provisioner, _, _, _, warden = self.fixture.make(root)
        warden.config.runtime_root = root / "runsc"
        (root / "runsc" / "warden-locks").mkdir(parents=True)
        self.service = DirectSandboxService(provisioner)
        self.runtime = DirectNodeRuntime(self.service, rust_execs=True)
        self.fence = self.runtime.exec_fence
        self.spec = self.fixture.spec()
        self.record = self.fixture.create(self.service, self.spec)
        self.transition = self.fence.transition_path(self.spec.id)
        self.activity = self.fence.activity_path(self.spec.id)

    def noded(self, *paths: Path) -> _Noded:
        holder = _Noded(*paths)
        self.addCleanup(lambda: holder.process.poll() is not None or holder.close())
        return holder

    def park(self, operation_id: str = "park:1"):
        return self.runtime.park(self.spec.id, operation_id=operation_id)

    def test_files_live_in_the_warden_lock_directory(self) -> None:
        locks = self.service.warden.config.runtime_root / "warden-locks"
        self.assertEqual((self.transition, self.activity),
                         (locks / ".sandbox.transition", locks / ".sandbox.activity"))
        with self.assertRaisesRegex(ValueError, "sandbox id"):
            self.fence.activity_path("../escape")

    def test_an_exec_refuses_park_and_pause_but_not_delete(self) -> None:
        exec_ = self.noded(self.activity)
        with self.assertRaisesRegex(SandboxBusyError, "cannot survive park: sandbox"):
            self.park()
        with self.assertRaisesRegex(SandboxBusyError, r"^sandbox has active exec/file activity: sandbox$"):
            self.runtime.pause_model_wait((self.spec.id, self.record.generation))
        self.assertFalse(self.runtime.lifecycle.is_idle(self.spec.id))
        # A refusal leaves neither the in-process fence nor T behind.
        self.assertTrue(self.runtime.lifecycle._coordinator.is_idle(self.spec.id))
        exec_.close()
        self.assertTrue(self.runtime.lifecycle.is_idle(self.spec.id))
        self.assertEqual(self.park().state, "parked")
        self.runtime.wake(self.spec.id, generation=self.record.generation, operation_id="wake:1")
        # Delete severs running execs, as allow_shared does in process.
        exec_ = self.noded(self.activity)
        self.runtime.delete(self.spec.id, generation=self.record.generation, operation_id="delete:1")
        self.assertIsNone(self.service.get(self.spec.id))
        self.assertFalse(self.transition.exists() or self.activity.exists())
        exec_.close()

    def test_a_held_transition_lock_delays_every_transition(self) -> None:
        for allow_shared in (True, False):
            with self.subTest(allow_shared=allow_shared):
                starting = self.noded(self.transition)  # noded between its T and A locks
                entered = Event()

                def transition():
                    with self.runtime.lifecycle.exclusive(self.spec.id, allow_shared=allow_shared):
                        entered.set()

                thread = Thread(target=transition)
                thread.start()
                self.assertFalse(entered.wait(0.2))
                starting.close()
                self.assertTrue(entered.wait(5))
                thread.join(5)

    def test_a_transition_shuts_noded_out(self) -> None:
        with self.runtime.lifecycle.exclusive(self.spec.id, allow_shared=True):
            with self.assertRaises(subprocess.CalledProcessError):
                subprocess.run([sys.executable, "-c", _NODED, str(self.transition)], check=True,
                               input="", capture_output=True, text=True)
        with self.runtime.lifecycle.exclusive(self.spec.id):
            with self.assertRaises(subprocess.CalledProcessError):
                subprocess.run([sys.executable, "-c", _NODED, str(self.activity)], check=True,
                               input="", capture_output=True, text=True)

    def test_an_orphaned_file_after_delete_fences_nothing(self) -> None:
        orphan = self.noded(self.activity)  # an exec of the deleted incarnation
        old = self.activity.stat().st_ino
        self.runtime.delete(self.spec.id, generation=self.record.generation, operation_id="delete:1")
        self.record = self.fixture.create(self.service, self.spec, generation=8)
        self.assertEqual(self.park().state, "parked")
        self.assertNotEqual(self.activity.stat().st_ino, old)
        orphan.close()

    def test_a_lock_on_a_replaced_inode_is_taken_again(self) -> None:
        self.transition.touch()
        flock, attempts = exec_fence.fcntl.flock, []

        def racing_delete(descriptor, operation):
            attempts.append(os.fstat(descriptor).st_ino)
            if len(attempts) == 1:  # a delete unlinks it and a new opener remakes it
                self.transition.unlink()
                self.transition.touch()
            return flock(descriptor, operation)

        with patch.object(exec_fence.fcntl, "flock", racing_delete):
            descriptor = exec_fence._locked(self.transition, exec_fence.fcntl.LOCK_EX)
        try:
            self.assertEqual(len(attempts), 2)
            self.assertNotEqual(attempts[0], attempts[1])
            self.assertEqual(os.fstat(descriptor).st_ino, self.transition.stat().st_ino)
        finally:
            os.close(descriptor)

    def test_the_idle_clock_follows_the_activity_mtime(self) -> None:
        key = (self.spec.id, self.record.generation)
        self.activity.unlink(missing_ok=True)
        now = time.monotonic()
        self.service._last_activity[key] = now - 100
        self.assertAlmostEqual(self.service.idle_for_seconds(*key, now=now), 100, delta=1)
        self.noded(self.activity).close()  # noded's exec start creates and touches it
        os.utime(self.activity, (time.time() - 5, time.time() - 5))
        self.assertAlmostEqual(self.service.idle_for_seconds(*key, now=now), 5, delta=1)
        os.utime(self.activity, (time.time() - 500, time.time() - 500))
        self.assertAlmostEqual(self.service.idle_for_seconds(*key, now=now), 100, delta=1)
        # The agent's own marks advance the same clock.
        self.service.mark_activity(*key)
        self.assertLess(time.time() - self.activity.stat().st_mtime, 5)
        # Reclaim's currency check sees a touch by noded.
        mark = self.service._activity_mark(key)
        stamp = self.activity.stat().st_mtime_ns + 1_000_000
        os.utime(self.activity, ns=(stamp, stamp))
        self.assertNotEqual(self.service._activity_mark(key), mark)

    def test_without_the_flag_nothing_changes(self) -> None:
        runtime = DirectNodeRuntime(self.service)
        self.assertIsNone(runtime.exec_fence)
        self.service.exec_fence = None
        self.transition.unlink(missing_ok=True)
        self.activity.unlink(missing_ok=True)  # the create's activity mark made it
        with runtime.lifecycle.exclusive(self.spec.id):
            pass
        self.service.mark_activity(self.spec.id, self.record.generation)
        self.assertFalse(self.transition.exists() or self.activity.exists())


class _UnixConnection(HTTPConnection):
    def __init__(self, path: str) -> None:
        super().__init__("node", timeout=10)
        self._path = path

    def connect(self) -> None:
        self.sock = socket.socket(socket.AF_UNIX)
        self.sock.connect(self._path)


class ExecConfigTests(unittest.TestCase):
    fixture = fixtures.DirectProvisionerTests()

    def serve(self, **kwargs):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        root = Path(directory.name).resolve()
        provisioner, _, storage, images, warden = self.fixture.make(root)
        config = warden.config
        config.runsc, config.runtime_root = Path("/opt/runsc"), root / "runsc"
        config.journal_root = root / "journals"
        storage.socket_path, images.root = Path("/run/storage.sock"), root / "cache"
        service = DirectSandboxService(provisioner)
        path = str(root / "agent.sock")
        server = fixtures.build_direct_node_agent_server(
            "127.0.0.1", 0, service=service, image_file=root / "images.json", job_id="job",
            node_id="node", node_epoch="epoch", node_control_bearer_token=TOKEN,
            unix_socket=Path(path), **kwargs)
        thread = Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
        thread.start()
        self.addCleanup(thread.join, 2)
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        connection = _UnixConnection(path)
        try:
            connection.request("GET", "/internal/v1/creates/config", headers={"Authorization": f"Bearer {TOKEN}"})
            response = connection.getresponse()
            self.assertEqual(response.status, 200)
            return root, json.loads(response.read())
        finally:
            connection.close()

    def test_the_configuration_carries_the_exec_settings_under_its_digest(self) -> None:
        root, effective = self.serve(rust_execs=True, total_resources=fixtures.ResourceQuantity(vcpu=4, memory_mb=8192))
        self.assertEqual(effective["exec"], {
            "runsc": "/opt/runsc", "runtime_root": str(root / "runsc"),
            "warden_locks_dir": str(root / "runsc" / "warden-locks"),
            "warden_paused_dir": str(root / "runsc" / "warden-paused"),
            "active_capacity_configured": True, "memory_floor_mib": 2048,
            "sessions": {"max_sessions": 1024, "max_events_per_session": 512,
                         "completed_retention_seconds": 30.0, "delivered_grace_seconds": 2.0,
                         "output_idle_timeout_seconds": 300.0},
            "admission_wait_seconds": 30.0, "rust_execs_enabled": True,
        })
        self.assertFalse(effective["rust_creates_enabled"])
        digest = effective.pop("config_sha256")
        self.assertEqual(digest, hashlib.sha256(
            json.dumps(effective, sort_keys=True, separators=(",", ":")).encode()).hexdigest())
        _, plain = self.serve()
        self.assertEqual((plain["exec"]["rust_execs_enabled"], plain["exec"]["active_capacity_configured"]),
                         (False, False))
        self.assertNotEqual(plain["config_sha256"], digest)

    def test_rust_execs_need_the_socket(self) -> None:
        with self.assertRaisesRegex(ValueError, "Rust execs need the agent on its Unix socket"):
            fixtures.build_direct_node_agent_server(
                "127.0.0.1", 0, service=object(), image_file=Path("/nonexistent"), job_id="job",
                node_id="node", rust_execs=True)


class RustExecFlagTests(unittest.TestCase):
    def test_the_flag_requires_the_front_door_and_renders_both_sides(self) -> None:
        raw = DeploymentConfig.default(scope_id="project-1").to_dict()
        self.assertFalse(DeploymentConfig.from_dict(raw).sandbox.direct_node_rust_exec)
        with self.assertRaisesRegex(ValueError, "direct_node_rust_exec requires"):
            DeploymentConfig.from_dict({**raw, "sandbox": {**raw["sandbox"], "direct_node_rust_exec": True}})
        exec_only = {**raw["sandbox"], "direct_node_front_door": True, "direct_node_rust_exec": True}
        with self.assertRaisesRegex(ValueError, "direct_node_rust_exec requires"):
            DeploymentConfig.from_dict({**raw, "sandbox": exec_only})
        enabled = {**exec_only, "direct_node_rust_create": True}
        self.assertTrue(DeploymentConfig.from_dict({**raw, "sandbox": enabled}).sandbox.direct_node_rust_exec)
        options = vm_init_fixtures.VmInitTests._options(
            direct_node_front_door=True, direct_node_rust_create=True, direct_node_rust_exec=True)
        self.assertTrue(vm_init_options_to_dict(options)["directNodeRustExec"])
        token = options.node_control_bearer_token_file
        with self.assertRaisesRegex(ValueError, "require the node front door and Rust node creates"):
            render_vm_init_script(vm_init_fixtures.VmInitTests._options(
                direct_node_front_door=True, direct_node_rust_exec=True))
        both = render_vm_init_script(options)
        self.assertIn("--registry-foreign --rust-creates --rust-execs", both)
        self.assertIn(f"--rust-create --rust-exec --node-control-token-file {token}\n", both)
        front_door_only = render_vm_init_script(vm_init_fixtures.VmInitTests._options(direct_node_front_door=True))
        self.assertNotIn("--rust-exec", front_door_only)
        with self.assertRaisesRegex(ValueError, "execs require the node front door"):
            render_vm_init_script(vm_init_fixtures.VmInitTests._options(direct_node_rust_exec=True))
        required = ["--deployment-id", "d", "--state-root", "/s", "--image-file", "/i", "--volume-mount-root", "/v",
                    "--storage-native-socket", "/n", "--runsc", "/r", "--runsc-commit", "c",
                    "--node-control-bearer-token-file", "/t"]
        parser = build_parser()
        self.assertTrue(parser.parse_args(["serve-direct-node-agent", *required, "--rust-execs"]).rust_execs)
        self.assertFalse(parser.parse_args(["serve-direct-node-agent", *required]).rust_execs)


if __name__ == "__main__":
    unittest.main()
