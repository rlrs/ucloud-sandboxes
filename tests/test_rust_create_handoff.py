"""Phase 1 of the Rust node daemon, the agent's half (docs/rust-node-daemon-plan.md).

runtime/noded owns the registry and runs creates; the agent is a foreign
registry process with a revalidated read cache, and holds each create's
admission between two Unix-socket requests (ucloud_sandboxes/create_handoff.py).
"""

import copy
from dataclasses import replace
from http.client import HTTPConnection
import json
from pathlib import Path
import socket
import subprocess
import sys
from tempfile import TemporaryDirectory
from threading import Thread
import time
import unittest
from unittest.mock import DEFAULT, patch

from tests import test_direct_provisioner as fixtures
from tests import test_vm_init as vm_init_fixtures
from ucloud_sandboxes.cli import vm_init_options_to_dict
from ucloud_sandboxes.config import DeploymentConfig
from ucloud_sandboxes.direct_registry import DirectSandboxRegistry
from ucloud_sandboxes.direct_service import DirectSandboxService
from ucloud_sandboxes.sandbox import sandbox_spec_fingerprint
from ucloud_sandboxes.vm_init import render_vm_init_script

TEST_TIER = "contract"
REPO = Path(__file__).resolve().parents[1]
TOKEN = "node-secret"


class _UnixConnection(HTTPConnection):
    def __init__(self, path: str) -> None:
        super().__init__("node", timeout=10)
        self._path = path

    def connect(self) -> None:
        self.sock = socket.socket(socket.AF_UNIX)
        self.sock.connect(self._path)


def wait_for(condition, message: str) -> None:
    deadline = time.monotonic() + 5
    while not condition():
        if time.monotonic() > deadline:
            raise AssertionError(message)
        time.sleep(0.01)


class ForeignRegistryIndexTests(unittest.TestCase):
    fixtures = fixtures.DirectProvisionerTests()

    def setUp(self) -> None:
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name).resolve() / "registry.sqlite"

    def plan(self, registry, name: str):
        return registry.plan(spec=replace(self.fixtures.spec(), id=name), sandbox_generation=1,
                             operation_id=f"create:{name}", runtime_compatibility_sha256="b" * 64)

    def test_reads_are_cached_and_revalidated_after_another_owner_writes(self) -> None:
        owner = DirectSandboxRegistry(self.path, owner=True)  # noded's role
        self.addCleanup(owner.close)
        foreign = DirectSandboxRegistry(self.path, cached_reads=True)
        self.addCleanup(foreign.close)
        self.assertFalse(foreign.is_owner)
        one = self.plan(owner, "one")
        first = foreign.get("one")
        self.assertEqual(first, one)
        idle = patch.multiple(foreign, _borrow=DEFAULT, _refresh_foreign_index=DEFAULT, _check_file=DEFAULT)
        with idle as never:
            for _ in range(3):
                self.assertIs(foreign.get("one"), first)
                self.assertEqual(foreign.snapshot().records, (first,))
                self.assertEqual(foreign.list(), (first,))
                self.assertEqual(foreign.activity_revision(), owner.activity_revision())
        for mock in never.values():
            mock.assert_not_called()
        self.plan(owner, "two")
        self.assertIsNone(foreign.get("two"))  # Within the recheck interval.
        decode = patch.object(DirectSandboxRegistry, "_decode", wraps=DirectSandboxRegistry._decode)
        with decode as calls:
            self.assertEqual(foreign.get("two", fresh=True).phase, "planned")
        self.assertEqual(calls.call_count, 1)  # Only the changed row.
        self.assertIs(foreign.get("one"), first)
        # Its own write is visible to its next read, without waiting.
        deleting = foreign.begin_delete("one", expected_revision=first.revision)
        self.assertEqual(foreign.get("one"), deleting)
        self.assertEqual(foreign.activity_revision(), DirectSandboxRegistry(self.path).activity_revision())
        # Ownership is untouched: the owner lock stays noded's.
        owner.close()
        DirectSandboxRegistry(self.path, owner=True).close()

    def test_another_process_commit_reaches_the_cache_after_the_interval(self) -> None:
        foreign = DirectSandboxRegistry(self.path, cached_reads=True)
        self.addCleanup(foreign.close)
        self.assertIsNone(foreign.get("child"))
        subprocess.run([sys.executable, "-c", (
            "import sys\n"
            "from dataclasses import replace\n"
            "from pathlib import Path\n"
            "from ucloud_sandboxes.direct_registry import DirectSandboxRegistry\n"
            "from tests.test_direct_provisioner import DirectProvisionerTests\n"
            "owner = DirectSandboxRegistry(Path(sys.argv[1]), owner=True)\n"
            "owner.plan(spec=replace(DirectProvisionerTests.spec(), id='child'), sandbox_generation=1,\n"
            "           operation_id='create:child', runtime_compatibility_sha256='b' * 64)\n"
        ), str(self.path)], cwd=REPO, check=True)
        self.assertIsNone(foreign.get("child"))
        index, sequence, _ = foreign._foreign
        foreign._foreign = (index, sequence, float("-inf"))  # The interval passed.
        self.assertEqual(foreign.get("child").phase, "planned")
        self.assertEqual(foreign.snapshot().activity_revision, 1)


class CreateHandoffTests(unittest.TestCase):
    fixture = fixtures.DirectProvisionerTests()

    def setUp(self) -> None:
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name).resolve()
        provisioner, _, self.storage, self.images, self.warden = self.fixture.make(self.root)
        # What create_config reads from the real stores.
        config = self.warden.config
        config.runsc, config.runtime_root = Path("/opt/runsc"), self.root / "runsc"
        config.journal_root = self.root / "journals"
        self.storage.socket_path, self.images.root = Path("/run/storage.sock"), self.root / "cache"
        path = provisioner.registry.path
        provisioner.registry = DirectSandboxRegistry(path, cached_reads=True)
        self.provisioner = provisioner
        self.service = DirectSandboxService(provisioner, max_concurrent_startups=2)
        self.adopted = []
        self.warden.adopt_created = self.adopted.append
        # noded: the registry owner, creating through the same fakes.
        self.daemon = copy.copy(provisioner)
        self.daemon.registry = DirectSandboxRegistry(path, owner=True)
        self.addCleanup(self.daemon.registry.close)
        self.socket = str(self.root / "agent.sock")
        self.server = fixtures.build_direct_node_agent_server(
            "127.0.0.1", 0, service=self.service, image_file=self.root / "images.json", job_id="job",
            node_id="node", node_epoch="epoch", node_control_bearer_token=TOKEN,
            unix_socket=Path(self.socket), rust_creates=True)
        self.handoff = self.server.RequestHandlerClass.create_handoff
        thread = Thread(target=self.server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
        thread.start()
        self.addCleanup(thread.join, 2)
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)

    def call(self, method, path, body=None, session="session-a"):
        connection = _UnixConnection(self.socket)
        try:
            headers = {"Authorization": f"Bearer {TOKEN}", "X-UCloud-Noded-Session": session}
            payload = None if body is None else json.dumps(body).encode()
            if payload is not None:
                headers["Content-Type"] = "application/json"
            connection.request(method, path, body=payload, headers=headers)
            response = connection.getresponse()
            return response.status, dict(response.getheaders()), json.loads(response.read())
        finally:
            connection.close()

    def admission(self, spec, **overrides):
        return {"sandbox_id": spec.id, "generation": 7, "operation_id": f"create:{spec.id}:7",
                "spec": spec.to_dict(), "spec_hash": sandbox_spec_fingerprint(spec),
                "admission_wait_seconds": None, **overrides}

    def creates_in_flight(self) -> int:
        return self.service.activity_snapshot().active_sandbox_creates

    def daemon_create(self, spec):
        return self.daemon.create(spec=spec, sandbox_generation=7, operation_id=f"create:{spec.id}:7")

    def test_admit_holds_admission_until_finish_returns_the_create_response(self) -> None:
        spec = self.fixture.spec()
        status, _, admitted = self.call("POST", "/internal/v1/creates/admit", self.admission(spec))
        self.assertEqual(status, 200, admitted)
        self.assertEqual((admitted["existing"], admitted["split"], admitted["initial_claim"]), (None, False, None))
        self.assertEqual(admitted["spec"], spec.to_dict())
        self.assertEqual(admitted["requested_resources"], spec.requested_resources().to_dict())
        # Heartbeats and drain see the held admission as an in-flight create.
        heartbeat = self.server.RequestHandlerClass.manager.heartbeat_snapshot(active_build_count=lambda: 0)
        self.assertEqual(heartbeat.activity.active_sandbox_creates, 1)
        self.assertEqual(heartbeat.activity.reserved_resources.memory_mb, 1024)
        self.assertEqual(self.handoff.held_count, 1)
        # The same incarnation is busy while held, as today's create is.
        status, headers, busy = self.call("POST", "/internal/v1/creates/admit", self.admission(spec))
        self.assertEqual((status, busy["error_code"], headers["Retry-After"]),
                         (503, "node_active_admission_deferred", "1"))
        self.daemon_create(spec)
        status, _, finished = self.call("POST", f"/internal/v1/creates/{admitted['token']}/finish",
                                        {"outcome": "created"})
        self.assertEqual(status, 200, finished)
        self.assertEqual(finished["status"], 201)
        self.assertEqual(finished["sandbox"]["state"], "running")
        self.assertEqual(finished["sandbox"]["node_epoch"], "epoch")
        self.assertGreater(finished["sandbox"]["activity_epoch"], self.provisioner.registry.activity_revision())
        self.assertLessEqual({"validate_spec_ms", "startup_admission_ms", "active_capacity_ms", "request_lock_ms"},
                             set(finished["phases"]))
        self.assertEqual((self.creates_in_flight(), self.handoff.held_count), (0, 0))
        self.assertEqual([sandbox.sandbox_id for sandbox in self.adopted], ["sandbox"])
        status, _, error = self.call("POST", f"/internal/v1/creates/{admitted['token']}/finish",
                                     {"outcome": "created"})
        self.assertEqual((status, error["error_code"]), (404, "create_token_unknown"))
        # A replay of the same operation is idempotent: 200, not 201.
        status, _, again = self.call("POST", "/internal/v1/creates/admit", self.admission(spec))
        self.assertEqual(again["existing"]["generation"], 7)
        status, _, replay = self.call("POST", f"/internal/v1/creates/{again['token']}/finish",
                                      {"outcome": "created", "runtime_started": False})
        self.assertEqual((replay["status"], replay["sandbox"]["id"]), (200, "sandbox"))
        # A replay the daemon did not start a runtime for keeps its claim.
        self.assertEqual(len(self.adopted), 1)
        # Admissions carry the configuration digest the daemon loaded.
        _, _, config = self.call("GET", "/internal/v1/creates/config")
        self.assertEqual((admitted["config_sha256"], again["config_sha256"]), (config["config_sha256"],) * 2)

    def test_refusals_and_rollbacks_match_the_create_endpoint(self) -> None:
        spec = self.fixture.spec()
        status, _, error = self.call("POST", "/internal/v1/creates/admit", self.admission(spec, spec_hash="0" * 64))
        self.assertEqual(status, 400, error)
        wrong_network = replace(spec, network="sandbox")
        status, _, error = self.call("POST", "/internal/v1/creates/admit", self.admission(wrong_network))
        self.assertEqual((status, self.creates_in_flight()), (400, 0), error)
        # A capacity rejection rolls back what the daemon committed, under the held lock.
        cap = replace(spec, id="cap")
        _, _, admitted = self.call("POST", "/internal/v1/creates/admit", self.admission(cap))
        self.daemon.registry.plan(spec=cap, sandbox_generation=7, operation_id="create:cap:7",
                                  runtime_compatibility_sha256=self.provisioner.runtime_compatibility_sha256)
        status, headers, error = self.call("POST", f"/internal/v1/creates/{admitted['token']}/finish",
                                           {"outcome": "capacity_rejected", "message": "disk is full"})
        self.assertEqual((status, error["error_code"], headers["Retry-After"], headers["X-UCloud-Sandbox-Retryable"]),
                         (503, "node_active_admission_deferred", "1", "true"))
        self.assertIsNone(self.daemon.registry.get("cap", fresh=True))
        # A failed create only releases.
        _, _, admitted = self.call("POST", "/internal/v1/creates/admit", self.admission(spec))
        status, _, released = self.call("POST", f"/internal/v1/creates/{admitted['token']}/finish",
                                        {"outcome": "failed", "status": 503, "body": {"error": "runsc"}})
        self.assertEqual((status, released, self.creates_in_flight()), (200, {"released": True}, 0))
        # Drain closes admission: no Retry-After, exactly like create.
        self.service.close_admission()
        status, headers, error = self.call("POST", "/internal/v1/creates/admit", self.admission(spec))
        self.assertEqual((status, error["error_code"]), (503, "node_admission_closed"))
        self.assertNotIn("Retry-After", headers)

    def test_tokens_expire_and_die_with_their_daemon_session(self) -> None:
        spec = self.fixture.spec()
        self.handoff.expiry_seconds = 0.2
        _, _, admitted = self.call("POST", "/internal/v1/creates/admit", self.admission(spec))
        wait_for(lambda: self.creates_in_flight() == 0, "an expired admission was not released")
        status, _, _ = self.call("POST", f"/internal/v1/creates/{admitted['token']}/finish", {"outcome": "failed"})
        self.assertEqual(status, 404)
        self.handoff.expiry_seconds = 600
        _, _, admitted = self.call("POST", "/internal/v1/creates/admit", self.admission(spec))
        self.call("GET", "/v1/sandboxes", session="session-a")  # The same daemon: kept.
        self.assertEqual(self.creates_in_flight(), 1)
        self.call("GET", "/v1/sandboxes", session="session-b")  # A restarted daemon.
        wait_for(lambda: self.creates_in_flight() == 0, "a previous session's admission was kept")
        status, _, _ = self.call("POST", f"/internal/v1/creates/{admitted['token']}/finish",
                                 {"outcome": "created"}, session="session-b")
        self.assertEqual(status, 404)

    def test_image_materialization_and_the_create_configuration(self) -> None:
        resolution = {"root": "sha256:" + "c" * 64, "source": "image", "environment": {"schema": 1}}
        requested = []
        self.images.materialize_resolution = lambda image, root: requested.append((image, root)) or resolution
        status, _, image = self.call("POST", "/internal/v1/images/materialize",
                                     {"image": "image", "environment_root": None})
        self.assertEqual((status, image, requested), (200, {"resolution": resolution}, [("image", None)]))
        status, _, effective = self.call("GET", "/internal/v1/creates/config")
        self.assertEqual(status, 200, effective)
        self.assertLessEqual({
            "state_root": str(self.root), "image_cache_root": str(self.root / "cache"),
            "volume_mount_root": str(self.root / "quota"), "storage_native_socket": "/run/storage.sock",
            "runsc": "/opt/runsc", "bundle_root": str(self.root / "bundles"), "network": "none",
            "network_mtu": 1420, "direct_network_allow_tcp": [], "dns_named_egress": False,
            "relays_configured": False, "split_memory_backing": False, "application_memory_root": None,
            "workspace_initial_grant_mb": 0, "demonstrated_memory": False, "environment": None,
            "runtime_compatibility_sha256": self.provisioner.runtime_compatibility_sha256,
            "node_epoch": "epoch", "rust_creates_enabled": True,
        }.items(), effective.items())

    def test_rust_creates_need_the_socket_and_a_foreign_registry(self) -> None:
        with self.assertRaisesRegex(ValueError, "Unix socket and a foreign registry"):
            fixtures.build_direct_node_agent_server(
                "127.0.0.1", 0, service=self.service, image_file=self.root / "images.json", job_id="job",
                node_id="node", rust_creates=True)


class RustCreateFlagTests(unittest.TestCase):
    def test_the_flag_requires_the_front_door_and_renders_both_sides(self) -> None:
        raw = DeploymentConfig.default(scope_id="project-1").to_dict()
        self.assertFalse(DeploymentConfig.from_dict(raw).sandbox.direct_node_rust_create)
        with self.assertRaisesRegex(ValueError, "direct_node_rust_create requires"):
            DeploymentConfig.from_dict({**raw, "sandbox": {**raw["sandbox"], "direct_node_rust_create": True}})
        enabled = {**raw["sandbox"], "direct_node_front_door": True, "direct_node_rust_create": True}
        self.assertTrue(DeploymentConfig.from_dict({**raw, "sandbox": enabled}).sandbox.direct_node_rust_create)
        options = vm_init_fixtures.VmInitTests._options(direct_node_front_door=True, direct_node_rust_create=True)
        self.assertTrue(vm_init_options_to_dict(options)["directNodeRustCreate"])
        script = render_vm_init_script(options)
        self.assertIn("--unix-socket /run/ucloud-sandboxes/node-agent/agent.sock --registry-foreign --rust-creates",
                      script)
        self.assertIn("--upstream-unix /run/ucloud-sandboxes/node-agent/agent.sock --rust-create"
                      f" --node-control-token-file {options.node_control_bearer_token_file}\n", script)
        front_door_only = render_vm_init_script(vm_init_fixtures.VmInitTests._options(direct_node_front_door=True))
        self.assertNotIn("--rust-create", front_door_only)
        with self.assertRaisesRegex(ValueError, "require the node front door"):
            render_vm_init_script(vm_init_fixtures.VmInitTests._options(direct_node_rust_create=True))


if __name__ == "__main__":
    unittest.main()
