"""Step 3c.0 of docs/admission-port-3c.md, the agent's half: the client for
noded's internal socket (ucloud_sandboxes/noded_client.py), the hello at
agent start, the ``admission`` block of the create configuration, and the
``--rust-admission`` flag. runtime/noded/tests/internal_admission.rs runs the
same client against the daemon's real listener.
"""

from dataclasses import replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import hashlib
import json
from pathlib import Path
import re
from tempfile import TemporaryDirectory
from threading import Lock, Thread
import time
import unittest
from unittest.mock import patch
import uuid

from tests import test_direct_provisioner as fixtures
from tests import test_vm_init as vm_init_fixtures
from tests.test_pause_tier import _PausingWarden
from tests.test_rust_create_handoff import _UnixConnection
from ucloud_sandboxes import noded_client
from ucloud_sandboxes.admission_backend import LocalAdmission, NodedAdmission
from ucloud_sandboxes.cli import build_parser, vm_init_options_to_dict
from ucloud_sandboxes.config import DeploymentConfig
from ucloud_sandboxes.direct_registry import DirectSandboxRegistry
from ucloud_sandboxes.direct_service import DirectSandboxService
from ucloud_sandboxes.http_server import UnixSocketServerMixin
from ucloud_sandboxes.models import ResourceQuantity
from ucloud_sandboxes.resident_memory import PublishedResidentMemory, ResidentMemorySampler
from ucloud_sandboxes.noded_client import (
    NodedClient, NodedLink, NodedLost, NodedProtocolError, NodedRefused, NodedUnavailable,
)
from ucloud_sandboxes.vm_init import render_vm_init_script

TEST_TIER = "contract"
TOKEN = "node-secret"
DIGEST = "c" * 64


class FakeNoded:
    """noded's internal listener as the contract describes it, with knobs."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.session = "session-1"
        self.seen: list[tuple[str, str, dict[str, str], bytes]] = []
        self.connections = 0
        self.close_after_answer = False
        self.hello_answer: dict | None = None
        self.delay = 0.0
        self._lock = Lock()
        fake = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def setup(self) -> None:
                super().setup()
                with fake._lock:
                    fake.connections += 1

            def log_message(self, *args) -> None:
                pass

            def answer(self, status: int, body: bytes) -> None:
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.send_header("X-UCloud-Noded-Session", fake.session)
                self.end_headers()
                self.wfile.write(body)
                if fake.close_after_answer:
                    self.close_connection = True  # Closed while the client pools it.

            def handle_one(self) -> None:
                length = int(self.headers.get("Content-Length") or 0)
                body = self.rfile.read(length)
                with fake._lock:
                    fake.seen.append((self.command, self.path, dict(self.headers), body))
                time.sleep(fake.delay)
                if self.path == "/internal/v1/admission/hello":
                    answer = fake.hello_answer or {"noded_session": fake.session, "admission_open": True,
                                                   "config_sha256": DIGEST}
                    self.answer(200, json.dumps(answer).encode())
                elif self.path == "/internal/v1/admission/ping":
                    self.answer(200, json.dumps({"noded_session": fake.session, "hello": True}).encode())
                elif self.path == "/drop":
                    self.close_connection = True  # Read, then gone without an answer.
                elif self.path == "/text":
                    self.answer(200, b"not json")
                else:
                    self.answer(404, json.dumps({"error": "no route", "error_code": "admission_route_unknown",
                                                 "retryable": False}).encode())

            do_GET = do_POST = handle_one

        server_class = type("FakeNodedServer", (UnixSocketServerMixin, ThreadingHTTPServer), {"daemon_threads": True})
        self.server = server_class(str(path), Handler)
        self.thread = Thread(target=self.server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
        self.thread.start()

    def hellos(self) -> list[dict[str, str]]:
        with self._lock:
            return [headers for _, path, headers, _ in self.seen if path == "/internal/v1/admission/hello"]

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(2)


class NodedClientTests(unittest.TestCase):
    def setUp(self) -> None:
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.socket = Path(directory.name) / "noded" / "noded.sock"

    def serve(self) -> FakeNoded:
        noded = FakeNoded(self.socket)
        self.addCleanup(noded.close)
        return noded

    def client(self, **kwargs) -> NodedClient:
        client = NodedClient(self.socket, TOKEN, **kwargs)
        self.addCleanup(client.close)
        return client

    def test_calls_carry_both_sessions_and_reuse_one_connection(self) -> None:
        noded = self.serve()
        client = self.client()
        self.assertEqual(str(uuid.UUID(client.agent_session)), client.agent_session)  # One uuid per agent start.
        self.assertIsNone(client.noded_session)
        self.assertEqual(client.hello(), {"noded_session": "session-1", "admission_open": True, "config_sha256": DIGEST})
        self.assertEqual(client.ping(), {"noded_session": "session-1", "hello": True})
        self.assertEqual(client.ping(), {"noded_session": "session-1", "hello": True})
        self.assertEqual((client.noded_session, client.connections_opened, noded.connections), ("session-1", 1, 1))
        method, path, headers, body = noded.seen[0]
        self.assertEqual((method, path), ("POST", "/internal/v1/admission/hello"))
        self.assertEqual(json.loads(body), {"agent_session": client.agent_session})
        self.assertEqual((headers["Authorization"], headers["Content-Type"]), (f"Bearer {TOKEN}", "application/json"))
        for _, _, headers, _ in noded.seen:
            self.assertEqual(headers["X-UCloud-Agent-Session"], client.agent_session)
        self.assertNotIn("Content-Type", noded.seen[1][2])
        self.assertEqual(self.client(agent_session="fixed").agent_session, "fixed")
        with self.assertRaisesRegex(ValueError, "token"):
            NodedClient(self.socket, " ")

    def test_a_connection_noded_closed_or_left_idle_is_replaced_not_reused(self) -> None:
        noded = self.serve()
        client = self.client()
        noded.close_after_answer = True
        client.ping()
        time.sleep(0.05)  # The server's close reaches the pooled socket.
        noded.close_after_answer = False
        self.assertEqual(client.ping()["hello"], True)  # Not NodedLost: the probe saw the close.
        self.assertEqual(client.connections_opened, 2)
        with patch.object(noded_client, "POOL_IDLE_SECONDS", 0.0):
            time.sleep(0.01)
            client.ping()
        self.assertEqual((client.connections_opened, noded.connections), (3, 3))
        client.ping()
        self.assertEqual(client.connections_opened, 3)

    def test_errors_say_whether_noded_may_have_acted(self) -> None:
        client = self.client(timeout=0.5)
        with self.assertRaises(NodedUnavailable):
            client.hello()  # Nothing listens yet.
        noded = self.serve()
        with self.assertRaises(NodedRefused) as refused:
            client.call("POST", "/internal/v1/admission/startup", {})
        self.assertEqual((refused.exception.status, refused.exception.error_code, refused.exception.retryable),
                         (404, "admission_route_unknown", False))
        self.assertEqual(refused.exception.payload["error"], "no route")
        with self.assertRaises(NodedProtocolError):
            client.call("GET", "/text")
        with self.assertRaises(NodedLost):
            client.call("POST", "/drop", {})
        noded.delay = 1.0
        with self.assertRaises(NodedLost):
            client.ping(timeout=0.1)
        noded.delay = 0.0
        for answer in ({"noded_session": "s", "admission_open": True},
                       {"noded_session": "", "admission_open": True, "config_sha256": None},
                       {"noded_session": "s", "admission_open": "yes", "config_sha256": None},
                       {"noded_session": "s", "admission_open": None, "config_sha256": "short"}):
            noded.hello_answer = answer
            with self.subTest(answer=answer), self.assertRaises(NodedProtocolError):
                client.hello()
        noded.hello_answer = {"noded_session": "s", "admission_open": None, "config_sha256": None}
        self.assertEqual(client.hello()["admission_open"], None)  # noded has not loaded the node yet.

    def test_a_new_noded_session_is_reported_once(self) -> None:
        noded = self.serve()
        sessions = []
        client = self.client(on_session=sessions.append)
        client.ping()
        client.ping()
        noded.session = "session-2"
        client.ping()
        client.ping()
        self.assertEqual((sessions, client.noded_session), (["session-2"], "session-2"))


class NodedLinkTests(unittest.TestCase):
    def setUp(self) -> None:
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.socket = Path(directory.name) / "noded.sock"
        self.link = NodedLink(NodedClient(self.socket, TOKEN), retry_seconds=(0.01, 0.05))
        self.addCleanup(self.link.stop)

    def test_hello_waits_for_noded_and_follows_its_restarts(self) -> None:
        self.link.start()
        self.assertIsNone(self.link.wait_hello(0.1))  # noded is not up: the agent starts anyway.
        noded = FakeNoded(self.socket)
        self.addCleanup(noded.close)
        hello = self.link.wait_hello(5)
        self.assertEqual(hello["noded_session"], "session-1")
        self.assertEqual(len(noded.hellos()), 1)
        self.link.observe_session("session-1")  # The session it already met.
        self.link.observe_session(None)
        time.sleep(0.05)
        self.assertEqual(len(noded.hellos()), 1)
        noded.session = "session-2"  # noded restarted; its next request to the agent says so.
        self.link.observe_session("session-2")
        self.assertEqual(self.link.wait_hello(5, session="session-2")["noded_session"], "session-2")
        time.sleep(0.05)
        self.assertEqual(len(noded.hellos()), 2)
        # An answer under a new session says hello again too.
        noded.session = "session-3"
        self.link.client.ping()
        self.assertEqual(self.link.wait_hello(5, session="session-3")["noded_session"], "session-3")
        time.sleep(0.05)
        self.assertEqual(len(noded.hellos()), 3)
        agent_sessions = {headers["X-UCloud-Agent-Session"] for headers in noded.hellos()}
        self.assertEqual(agent_sessions, {self.link.client.agent_session})

    def test_stop_ends_the_retries(self) -> None:
        self.link.start()
        self.link.stop()
        self.assertFalse(self.link._thread.is_alive())
        self.assertIsNone(self.link.wait_hello(0.01))


def _service(root: Path, **kwargs) -> DirectSandboxService:
    provisioner, _, storage, images, warden = fixtures.DirectProvisionerTests().make(root)
    config = warden.config  # What create_config reads from the real stores.
    config.runsc, config.runtime_root = Path("/opt/runsc"), root / "runsc"
    config.journal_root = root / "journals"
    storage.socket_path, images.root = Path("/run/storage.sock"), root / "cache"
    return DirectSandboxService(provisioner, **kwargs)


class AdmissionConfigTests(unittest.TestCase):
    def test_the_block_reads_the_live_service_under_the_digest(self) -> None:
        from ucloud_sandboxes.create_handoff import admission_config, create_config

        with TemporaryDirectory() as directory:
            service = _service(Path(directory).resolve(), max_concurrent_startups=12, max_concurrent_restores=3)
            self.assertEqual(admission_config(service, rust_admission_enabled=False), {
                "rust_admission_enabled": False, "max_concurrent_startups": 12, "max_concurrent_restores": 3,
                "admission_wait_seconds": 30.0, "active_capacity": None, "physical_memory_floor_mb": 2048,
                "growth_observation_max_age_seconds": 30.0, "prefetch_max_bytes": 1 << 30,
                "publication_lanes": 16, "retry_seconds": 0.21,
            })
            service.admission_wait_seconds = 12.5
            service.configure_active_capacity(ResourceQuantity(vcpu=6, memory_mb=24576),
                                              runtime_metrics_provider=lambda: None)
            block = admission_config(service, rust_admission_enabled=True)
            self.assertEqual((block["active_capacity"], block["admission_wait_seconds"], block["rust_admission_enabled"]),
                             ({"vcpu": 6, "memory_mb": 24576}, 12.5, True))
            off = create_config(service, node_epoch="e", rust_creates_enabled=True)
            on = create_config(service, node_epoch="e", rust_creates_enabled=True, rust_admission_enabled=True)
            self.assertEqual((off["admission"], on["admission"]), (dict(block, rust_admission_enabled=False), block))

            def digest(config):
                return hashlib.sha256(json.dumps(config, sort_keys=True, separators=(",", ":")).encode()).hexdigest()

            self.assertNotEqual(digest(off), digest(on))


class RustAdmissionAgentTests(unittest.TestCase):
    """The agent with --rust-admission: hello at start, and again for each new noded session."""

    def setUp(self) -> None:
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name).resolve()
        with patch.object(fixtures, "FakeWarden", _PausingWarden):
            self.service = _service(self.root, process_runner=fixtures.FakeProcessRunner())
        provisioner = self.service.provisioner
        (self.root / "runsc" / "warden-locks").mkdir(parents=True)
        # noded owns the registry; the agent is a foreign process.
        self.owner = DirectSandboxRegistry(provisioner.registry.path, owner=True)
        self.addCleanup(self.owner.close)
        provisioner.registry = DirectSandboxRegistry(provisioner.registry.path, cached_reads=True)
        self.socket = str(self.root / "agent.sock")

    def build(self, **flags):
        return fixtures.build_direct_node_agent_server(
            "127.0.0.1", 0, service=self.service, image_file=self.root / "images.json", job_id="job",
            node_id="node", node_epoch="epoch", node_control_bearer_token=TOKEN,
            unix_socket=Path(self.socket), **flags)

    def test_the_flag_needs_the_other_rust_fronts(self) -> None:
        for flags in ({}, {"rust_creates": True, "rust_execs": True},
                      {"rust_execs": True, "rust_pause_tier": True}):
            with self.subTest(flags=flags), self.assertRaisesRegex(ValueError, "Rust admission needs"):
                self.build(rust_admission=True, **flags)

    def test_the_agent_says_hello_and_reports_the_flag_in_its_configuration(self) -> None:
        noded = FakeNoded(self.root / "noded" / "noded.sock")
        self.addCleanup(noded.close)
        server = self.build(rust_creates=True, rust_execs=True, rust_pause_tier=True, rust_admission=True)
        thread = Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
        thread.start()
        self.addCleanup(thread.join, 2)
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        link = server.RequestHandlerClass.noded_link
        self.assertEqual(link.client.socket_path, str(self.root / "noded" / "noded.sock"))
        # Step 3c.3: one ledger, noded's. The agent publishes no reclaim demand.
        self.assertIsInstance(self.service.admission, NodedAdmission)
        self.assertIs(self.service.admission.link, link)
        self.assertIsNone(server.RequestHandlerClass.manager._demand_publisher)
        self.assertEqual(link.wait_hello(5)["noded_session"], "session-1")
        # Phase 3b: the agent reads noded's resident samples instead of sampling.
        self.assertIsInstance(self.service._resident_memory, PublishedResidentMemory)
        self.assertEqual(self.service._resident_memory.path, self.root / "noded" / "resident.json")

        def call(session):
            connection = _UnixConnection(self.socket)
            try:
                connection.request("GET", "/internal/v1/creates/config",
                                   headers={"Authorization": f"Bearer {TOKEN}", "X-UCloud-Noded-Session": session})
                response = connection.getresponse()
                return response.status, json.loads(response.read())
            finally:
                connection.close()

        status, config = call("session-1")
        self.assertEqual((status, config["admission"]["rust_admission_enabled"]), (200, True))
        self.assertEqual(len(noded.hellos()), 1)
        noded.session = "session-2"  # A restarted noded calls the agent under its new session.
        call("session-2")
        self.assertEqual(link.wait_hello(5, session="session-2")["noded_session"], "session-2")
        server.shutdown()
        server.server_close()
        self.assertFalse(link._thread.is_alive())

    def test_without_the_flag_nothing_says_hello(self) -> None:
        server = self.build(rust_creates=True, rust_execs=True, rust_pause_tier=True)
        self.addCleanup(server.server_close)
        self.assertIsNone(server.RequestHandlerClass.noded_link)
        self.assertIsInstance(self.service.admission, LocalAdmission)
        self.assertIsNotNone(server.RequestHandlerClass.manager._demand_publisher)
        self.assertIsInstance(self.service._resident_memory, ResidentMemorySampler)
        self.assertFalse((self.root / "noded" / "noded.sock").exists())


class RustAdmissionFlagTests(unittest.TestCase):
    def test_the_flag_requires_every_rust_front_and_renders_both_sides(self) -> None:
        raw = DeploymentConfig.default(scope_id="project-1").to_dict()
        self.assertFalse(DeploymentConfig.from_dict(raw).sandbox.direct_node_rust_admission)
        pause = {**raw["sandbox"], "direct_node_front_door": True, "direct_node_rust_create": True,
                 "direct_node_rust_exec": True, "direct_pause_tier": True, "swap_gb": 8,
                 "direct_node_rust_pause": True}
        for sandbox in ({**raw["sandbox"], "direct_node_rust_admission": True},
                        {**pause, "direct_node_rust_pause": False, "direct_node_rust_admission": True}):
            with self.assertRaisesRegex(ValueError, "direct_node_rust_admission requires"):
                DeploymentConfig.from_dict({**raw, "sandbox": sandbox})
        with self.assertRaisesRegex(ValueError, "must be a boolean"):
            DeploymentConfig.from_dict({**raw, "sandbox": {**pause, "direct_node_rust_admission": 1}})
        enabled = {**pause, "direct_node_rust_admission": True}
        self.assertTrue(DeploymentConfig.from_dict({**raw, "sandbox": enabled}).sandbox.direct_node_rust_admission)

        options = vm_init_fixtures.VmInitTests._options(
            direct_node_front_door=True, direct_node_rust_create=True, direct_node_rust_exec=True,
            direct_pause_tier=True, swap_gb=8, direct_node_rust_pause=True, direct_node_rust_admission=True)
        self.assertTrue(vm_init_options_to_dict(options)["directNodeRustAdmission"])
        script = render_vm_init_script(options)
        self.assertIn("--rust-execs --rust-pause-tier --rust-admission", script)
        state = re.search(r"^UCLOUD_STATE_DIR=(\S+)$", script, re.MULTILINE).group(1)  # The agent's --state-root parent.
        self.assertIn("--state-root ${UCLOUD_STATE_DIR}/direct-runtime", script)
        self.assertIn(f"--rust-pause --node-control-token-file {options.node_control_bearer_token_file}"
                      f" --rust-admission --internal-unix {state}/direct-runtime/noded/noded.sock\n", script)
        off = render_vm_init_script(replace(options, direct_node_rust_admission=False))
        self.assertNotIn("--rust-admission", off)
        self.assertNotIn("--internal-unix", off)
        with self.assertRaisesRegex(ValueError, "Rust node admission requires"):
            render_vm_init_script(replace(options, direct_node_rust_pause=False))
        required = ["--deployment-id", "d", "--state-root", "/s", "--image-file", "/i", "--volume-mount-root", "/v",
                    "--storage-native-socket", "/n", "--runsc", "/r", "--runsc-commit", "c",
                    "--node-control-bearer-token-file", "/t"]
        parser = build_parser()
        self.assertTrue(parser.parse_args(["serve-direct-node-agent", *required, "--rust-admission"]).rust_admission)
        self.assertFalse(parser.parse_args(["serve-direct-node-agent", *required]).rust_admission)


if __name__ == "__main__":
    unittest.main()
