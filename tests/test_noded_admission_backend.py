"""The agent's NodedAdmission against a real noded (docs/admission-port-3c.md, 3c.3).

What the both-backend suites cannot show: claims a restarted noded takes back
through the agent's hello (§3.5), the heartbeat's composition with noded's
snapshot (§3.6), lost answers as safe retries (I10), and growth events of the
agent's start that wait for noded to come up.
"""

from dataclasses import replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import socket
import time
from tempfile import TemporaryDirectory, mkdtemp
from threading import Event, Thread
import shutil
import unittest
from unittest.mock import patch

from tests import test_direct_provisioner as fixtures
from tests.noded_admission import TOKEN, NodedHarness, install
from ucloud_sandboxes.admission_backend import NodedAdmission, NodedSnapshotUnavailable
from ucloud_sandboxes.direct_service import DirectSandboxService
from ucloud_sandboxes.http_server import UnixSocketServerMixin
from ucloud_sandboxes.models import ResourceQuantity
from ucloud_sandboxes.node_runtime import DirectNodeRuntime
from ucloud_sandboxes.noded_client import NodedClient, NodedLink
from ucloud_sandboxes.sandbox import SandboxRestoreBusyError, SandboxStartupBusyError, compose_activity_revision
from ucloud_sandboxes.transition_admission import TransitionCost, TransitionKind

TEST_TIER = "contract"


class NodedAdmissionTests(unittest.TestCase):
    def setUp(self) -> None:
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.fixture = fixtures.DirectProvisionerTests()
        self.provisioner, self.registry, *_ = self.fixture.make(Path(directory.name).resolve())
        self.provisioner.oci = replace(self.provisioner.oci, managed_init_binary=Path("/bin/sh").resolve())
        self.service = DirectSandboxService(self.provisioner, max_concurrent_restores=1)
        self.harness = install(self, self.service)

    def reservations(self):
        return self.service.admission.snapshot()["resource_reservations"]

    def test_a_restarted_noded_takes_back_the_claims_the_agent_holds(self) -> None:
        self.fixture.create(self.service, self.fixture.spec())
        restore = TransitionCost(TransitionKind.RESTORE, 1 << 30, provenance="authenticated-checkpoint-allocation")
        with self.service.admission.startup(("s1", 1), ResourceQuantity(vcpu=2, memory_mb=512), managed=False), \
                self.service.admission.restore(("sandbox", 7), ResourceQuantity(vcpu=1, memory_mb=1024), restore,
                                               deadline=time.monotonic() + 5, validate_owner=lambda: None):
            before = self.service.admission.snapshot()
            self.harness.restart()  # noded loses everything; the agent's hello reasserts.
            after = self.service.admission.snapshot()
            self.assertNotEqual(after["noded_session"], before["noded_session"])
            self.assertEqual(sorted(after["resource_reservations"], key=str),
                             sorted(before["resource_reservations"], key=str))
            self.assertEqual(after["transitions"]["restore"]["memory_bytes"], 1 << 30)
            self.assertEqual(after["slots"]["restore"]["available"], 0, "the reasserted restore holds its slot")
        # Released under the reasserted tokens.
        self.assertEqual(self.reservations(), [])
        self.assertEqual(self.service.admission.slots()["restore"]["available"], 1)

    def test_the_heartbeat_reads_noded_first_and_adds_both_revisions(self) -> None:
        runtime = DirectNodeRuntime(self.service)
        self.addCleanup(runtime.stop)
        with self.service.admission.startup(("s1", 1), ResourceQuantity(vcpu=2, memory_mb=512), managed=False):
            token = self.service.acquire_exec_capacity(*self.created("alpha"))
            try:
                snapshot = runtime.heartbeat_snapshot(active_build_count=lambda: 0)
                noded = self.service.admission.snapshot()
            finally:
                self.service.release_exec_capacity(token)
        activity = snapshot.activity
        self.assertEqual((activity.active_sandbox_creates, activity.active_operations), (1, 2))
        self.assertEqual(activity.reserved_resources, ResourceQuantity(vcpu=2, memory_mb=512))
        # One more for the exec's release; the noded addend is unchanged.
        transient = self.service._activity_epoch - 1 + noded["transient_revision"]
        self.assertEqual(activity.activity_revision, compose_activity_revision(
            durable_revision=self.registry.activity_revision(), transient_revision=transient))
        # A lifecycle answer composes the same two addends, so it never runs backwards.
        self.assertGreater(self.service.advance_lifecycle_activity_revision(), activity.activity_revision)

    def created(self, sandbox_id):
        self.fixture.create(self.service, replace(self.fixture.spec(), id=sandbox_id))
        return sandbox_id, 7

    def test_a_heartbeat_skips_its_sample_when_noded_restarts_or_is_down(self) -> None:
        runtime = DirectNodeRuntime(self.service)
        self.addCleanup(runtime.stop)
        inventory = self.service.inventory_snapshot

        def restarting():
            self.harness.restart()  # Between noded's snapshot and the inventory.
            return inventory()

        with patch.object(self.service, "inventory_snapshot", side_effect=restarting):
            with self.assertRaisesRegex(NodedSnapshotUnavailable, "restarted"):
                runtime.heartbeat_snapshot(active_build_count=lambda: 0)
        self.assertEqual(runtime.heartbeat_snapshot(active_build_count=lambda: 0).activity.active_operations, 0)
        self.harness._stop_process()
        with patch.object(self.harness, "sync"):  # Keep it down.
            with self.assertRaises(NodedSnapshotUnavailable):
                runtime.heartbeat_snapshot(active_build_count=lambda: 0)

    def test_parked_records_found_at_start_reach_a_noded_that_comes_up_later(self) -> None:
        spec = replace(self.fixture.spec(), id="m1", memory_mb=2048, parkable=True, managed_process=True)
        self.fixture.create(self.service, spec)
        self.service.admit_managed_continuation("m1", 7, "request-1")  # Active, as a launch leaves it.
        self.service.warden.park(self.registry.get("m1").to_direct_sandbox(), operation_id="park:crash")
        # The agent restarts before noded does: its start re-observes the park.
        harness = NodedHarness(self.registry.path)
        self.addCleanup(harness.close)
        restarted = DirectSandboxService(self.provisioner)
        backend = NodedAdmission(restarted, NodedLink(NodedClient(harness.socket, TOKEN)))
        restarted.use_noded_admission(backend)
        self.harness._stop_process()
        restarted.start()
        self.addCleanup(restarted.stop)
        self.assertEqual(self.registry.growth_intents()[0].phase, "active")
        harness.sync(restarted)  # noded comes up.
        backend.link.hello_now()
        self.assertEqual(self.registry.growth_intents()[0].phase, "parked")


class RecoveringNodedTests(unittest.TestCase):
    """A noded that has not loaded the node answers every call, hello
    included, with a retryable 503 admission_recovering."""

    def setUp(self) -> None:
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        fixture = fixtures.DirectProvisionerTests()
        provisioner, *_ = fixture.make(Path(directory.name).resolve())
        self.service = DirectSandboxService(provisioner)
        root = mkdtemp(prefix="recovering-")
        self.addCleanup(shutil.rmtree, root, True)
        self.calls = []
        calls = self.calls

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args) -> None:
                pass

            def handle_one(self) -> None:
                self.rfile.read(int(self.headers.get("Content-Length") or 0))
                calls.append(self.path)
                body = json.dumps({"error": "noded has not loaded the node's admission yet",
                                   "error_code": "admission_recovering", "retryable": True}).encode()
                self.send_response(503)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.send_header("X-UCloud-Noded-Session", "loading")
                self.end_headers()
                self.wfile.write(body)

            do_GET = do_POST = handle_one

        server = type("Recovering", (UnixSocketServerMixin, ThreadingHTTPServer), {"daemon_threads": True})(
            str(Path(root) / "noded.sock"), Handler)
        Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True).start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        self.service.use_noded_admission(NodedAdmission(
            self.service, NodedLink(NodedClient(Path(root) / "noded.sock", TOKEN))))

    def test_the_heartbeat_skips_its_sample_and_grants_are_safe_retries(self) -> None:
        runtime = DirectNodeRuntime(self.service)
        self.addCleanup(runtime.stop)
        with self.assertRaises(NodedSnapshotUnavailable):
            runtime.heartbeat_snapshot(active_build_count=lambda: 0)
        # It tried a hello (refused too) before giving up; nothing was published as zero.
        self.assertEqual(self.calls[-2:], ["/internal/v1/admission/snapshot", "/internal/v1/admission/hello"])
        with self.assertRaises(SandboxStartupBusyError):
            with self.service.admission.startup(("new", 1), ResourceQuantity(memory_mb=512), managed=False):
                self.fail("granted while recovering")


class LostAnswerTests(unittest.TestCase):
    """A request noded read but never answered was not dispatched (I10)."""

    def setUp(self) -> None:
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.fixture = fixtures.DirectProvisionerTests()
        provisioner, *_ = self.fixture.make(Path(directory.name).resolve())
        provisioner.oci = replace(provisioner.oci, managed_init_binary=Path("/bin/sh").resolve())
        self.service = DirectSandboxService(provisioner)
        self.fixture.create(self.service, replace(self.fixture.spec(), parkable=True, managed_process=True))
        root = mkdtemp(prefix="lost-")
        self.addCleanup(shutil.rmtree, root, True)
        path = str(Path(root) / "noded.sock")
        listener = socket.socket(socket.AF_UNIX)
        listener.bind(path)
        listener.listen()
        stopped = Event()

        def drop():
            while not stopped.is_set():
                try:
                    connection, _ = listener.accept()
                except OSError:
                    return
                connection.recv(65536)
                connection.close()  # Read, never answered.

        Thread(target=drop, daemon=True).start()
        self.addCleanup(listener.close)
        self.addCleanup(stopped.set)
        self.service.use_noded_admission(NodedAdmission(self.service, NodedLink(NodedClient(path, TOKEN))))

    def test_every_admission_that_got_no_answer_is_a_safe_retry(self) -> None:
        registration = self.service.provisioner.registry.get("sandbox")
        admission = self.service.admission
        with self.assertRaises(SandboxStartupBusyError):
            with admission.startup(("new", 1), ResourceQuantity(memory_mb=512), managed=False):
                self.fail("granted without an answer")
        cost = TransitionCost(TransitionKind.RESTORE, 1 << 30)
        with self.assertRaises(SandboxRestoreBusyError):
            with admission.restore(("sandbox", 7), ResourceQuantity(memory_mb=1024), cost,
                                   deadline=time.monotonic() + 5, validate_owner=lambda: None):
                self.fail("granted without an answer")
        with self.assertRaises(SandboxStartupBusyError):
            self.service.admit_managed_continuation("sandbox", 7, "request-1")
        with self.assertRaises(SandboxStartupBusyError):
            admission.launch(registration, "primary", "a" * 64)
        # A publication is skipped, a capture proceeds unobserved.
        self.assertIsNone(admission.publication(("sandbox", 7)))
        with self.assertLogs("ucloud_sandboxes.admission_backend", "WARNING"):
            with admission.observe(("sandbox", 7), TransitionCost(TransitionKind.CAPTURE, None)):
                pass


if __name__ == "__main__":
    unittest.main()
