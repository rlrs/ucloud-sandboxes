"""runtime/noded's managed-process growth calls into the agent.

noded starts, signals and reads managed processes itself; the growth ledger
stays in the agent (docs/managed-process-reads.md, "Start and signal in noded
too"). These are its three internal routes on the agent's Unix socket.
"""

from dataclasses import replace
import hashlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Thread
import unittest
from unittest.mock import patch

from tests import test_direct_provisioner as fixtures
from tests.test_rust_exec_fence import TOKEN, _UnixConnection
from ucloud_sandboxes.direct_service import DirectSandboxService
from ucloud_sandboxes.managed_process import ManagedProcessStart, control_request_bytes
from ucloud_sandboxes.sandbox import SandboxCapacityUnavailableError

TEST_TIER = "contract"
DIGEST = "a" * 64
OTHER = "b" * 64


class ManagedGrowthRouteTests(unittest.TestCase):
    def setUp(self) -> None:
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name).resolve()
        provisioner, self.registry, *_ = fixtures.DirectProvisionerTests().make(self.root)
        provisioner.oci = replace(provisioner.oci, managed_init_binary=Path("/bin/sh").resolve())
        self.service = DirectSandboxService(provisioner, process_runner=fixtures.FakeProcessRunner())
        self.service.admission_wait_seconds = 1
        self.spec = replace(fixtures.DirectProvisionerTests.spec(), parkable=True, managed_process=True)
        self.record = fixtures.DirectProvisionerTests.create(self.service, self.spec)
        self.plain = fixtures.DirectProvisionerTests.create(
            self.service, replace(fixtures.DirectProvisionerTests.spec(), id="plain", parkable=True))
        self.socket = str(self.root / "agent.sock")
        server = fixtures.build_direct_node_agent_server(
            "127.0.0.1", 0, service=self.service, image_file=self.root / "images.json", job_id="job",
            node_id="node", node_epoch="epoch", node_control_bearer_token=TOKEN,
            unix_socket=Path(self.socket))
        thread = Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
        thread.start()
        self.addCleanup(thread.join, 2)
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)

    def call(self, route, body):
        connection = _UnixConnection(self.socket)
        try:
            connection.request("POST", "/internal/v1/managed/" + route, body=json.dumps(body).encode(),
                               headers={"Authorization": f"Bearer {TOKEN}"})
            response = connection.getresponse()
            return response.status, json.loads(response.read())
        finally:
            connection.close()

    def body(self, route="launch", **overrides):
        body = {"sandbox_id": self.spec.id, "generation": self.record.generation, "job_id": "primary"}
        if route != "terminal":
            body["launch_sha256"] = DIGEST
        return {**body, **overrides}

    def intent(self):
        return {item.sandbox_id: item for item in self.registry.growth_intents()}.get(self.spec.id)

    def test_launch_commits_and_admits_the_primary_once(self) -> None:
        self.assertEqual(self.call("launch", self.body()), (200, {"job_id": "primary"}))
        intent = self.intent()
        self.assertEqual((intent.job_id, intent.launch_sha256, intent.phase), ("primary", DIGEST, "active"))
        # Idempotent for the same identity, as a replayed start is.
        self.assertEqual(self.call("launch", self.body()), (200, {"job_id": "primary"}))
        for job_id, digest in (("primary", OTHER), ("second", DIGEST)):
            status, body = self.call("launch", self.body(job_id=job_id, launch_sha256=digest))
            self.assertEqual((status, body["error_code"], body["job_id"]), (409, "primary_owned", "primary"))

    def test_launch_queued_for_memory_is_busy_and_retryable(self) -> None:
        with patch.object(self.service, "_admit_managed_growth",
                          side_effect=SandboxCapacityUnavailableError("no memory headroom")):
            status, body = self.call("launch", self.body())
        self.assertEqual((status, body), (503, {
            "error": "no memory headroom", "error_code": "startup_busy", "retryable": True}))
        # Nothing was dispatched: the launch keeps its queued identity.
        self.assertEqual(self.intent().phase, "queued")
        self.assertEqual(self.call("launch", self.body()), (200, {"job_id": "primary"}))
        self.assertEqual(self.intent().phase, "active")

    def test_bind_and_terminal_follow_the_launch(self) -> None:
        self.assertEqual(self.call("launch", self.body()), (200, {"job_id": "primary"}))
        self.assertEqual(self.call("bind", self.body("bind")), (200, {"job_id": "primary"}))
        status, body = self.call("bind", self.body("bind", job_id="second"))
        self.assertEqual((status, body["error_code"], body["job_id"]), (409, "primary_owned", "primary"))
        # Another job's terminal record changes nothing.
        self.assertEqual(self.call("terminal", self.body("terminal", job_id="second")),
                         (200, {"job_id": "primary"}))
        self.assertEqual(self.intent().phase, "active")
        self.assertEqual(self.call("terminal", self.body("terminal")), (200, {"job_id": "primary"}))
        self.assertEqual(self.intent().phase, "terminal")

    def test_terminal_without_a_launch_is_a_no_op(self) -> None:
        self.assertEqual(self.call("terminal", self.body("terminal")), (200, {"job_id": ""}))
        self.assertIsNone(self.intent())

    def test_every_route_fences_the_generation_and_the_mode(self) -> None:
        for route in ("launch", "bind", "terminal"):
            with self.subTest(route=route):
                status, body = self.call(route, self.body(route, generation=self.record.generation + 1))
                self.assertEqual((status, body["error_code"], body["retryable"]),
                                 (409, "generation_mismatch", False))
                status, body = self.call(route, self.body(route, sandbox_id="plain",
                                                          generation=self.plain.generation))
                self.assertEqual((status, body["error_code"]), (409, "not_managed_process"))
        self.assertIsNone(self.intent())

    def test_bodies_are_parsed_strictly(self) -> None:
        for route in ("launch", "bind", "terminal"):
            invalid = [{"extra": 1}, {"generation": 0}, {"generation": True}, {"generation": "7"},
                       {"sandbox_id": "../x"}, {"job_id": ""}, {"job_id": "-leading"}, {"job_id": 7}]
            if route != "terminal":
                invalid += [{"launch_sha256": "A" * 64}, {"launch_sha256": "a" * 63}]
            else:
                invalid += [{"launch_sha256": DIGEST}]
            for overrides in invalid:
                with self.subTest(route=route, overrides=overrides):
                    status, body = self.call(route, self.body(route, **overrides))
                    self.assertEqual((status, body["error_code"]), (400, "invalid_request"))
            missing = self.body(route)
            del missing["job_id"]
            self.assertEqual(self.call(route, missing)[0], 400)
        self.assertIsNone(self.intent())

    def test_the_python_start_shares_the_ledger_with_noded_launches(self) -> None:
        start = ManagedProcessStart("primary", ("/bin/agent",), cwd="/workspace")
        replies = []

        def control(registration, payload, **_):
            replies.append(payload)
            return {"version": 1, "ok": True, "job": {
                "job_id": payload["job_id"], "spec_sha256": "c" * 64, "state": "running",
                "pid": 10, "sequence": 2}}

        with patch.object(self.service, "_managed_control", side_effect=control):
            self.assertEqual(self.service.start_managed_process(self.spec.id, start).state, "running")
        digest = hashlib.sha256(control_request_bytes(replies[0])).hexdigest()
        intent = self.intent()
        self.assertEqual((intent.job_id, intent.launch_sha256, intent.phase), ("primary", digest, "active"))
        # noded computing the same digest is admitted as the same launch.
        self.assertEqual(self.call("launch", self.body(launch_sha256=digest)), (200, {"job_id": "primary"}))
        status, body = self.call("launch", self.body())
        self.assertEqual((status, body["error_code"]), (409, "primary_owned"))


if __name__ == "__main__":
    unittest.main()
