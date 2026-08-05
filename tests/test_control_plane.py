from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import timedelta
from io import BytesIO
from tempfile import TemporaryDirectory
from http.client import HTTPConnection
from threading import Event, Lock, Thread
from time import monotonic, sleep
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import base64
import hashlib
from pathlib import Path
import sqlite3
import tarfile
from types import SimpleNamespace
from urllib import error, request
from urllib.parse import quote
import unittest
from unittest.mock import patch

from ucloud_sandboxes.agent import (
    build_heartbeat,
    post_heartbeat,
    post_heartbeat_with_headers,
)
from ucloud_sandboxes import control_plane
from ucloud_sandboxes.control_plane import (
    DEFAULT_MAX_GATEWAY_HTTP_REQUEST_THREADS,
    IMAGE_BUILD_PROXY_TIMEOUT_SECONDS,
    IMAGE_PULL_PROXY_TIMEOUT_SECONDS,
    build_server,
)
from ucloud_sandboxes.deployment import package_version
from ucloud_sandboxes.http_server import DEFAULT_HTTP_REQUEST_QUEUE_SIZE
from ucloud_sandboxes.images import DockerImageRuntime, ImageRecord, ImageStore
from ucloud_sandboxes.managed_registry import (
    RegistryLayerDescriptor,
    RegistryManifestLayers,
    RegistryUsageStore,
)
from ucloud_sandboxes.managed_process import ManagedProcessRecord
from ucloud_sandboxes.models import (
    NodeHeartbeat,
    NodeRuntimeMetrics,
    ResourceQuantity,
    SandboxInventoryEntry,
    utc_now,
)
from ucloud_sandboxes.node_agent import build_node_agent_server
from ucloud_sandboxes.registry import HeartbeatStore
from ucloud_sandboxes.routing import (
    RoutingStore,
    SandboxRoute,
    SandboxRouteConflictError,
)
from ucloud_sandboxes.sandbox import (
    CommandResult,
    DockerGvisorRuntime,
    FORK_REQUEST_TIMEOUT_SECONDS,
    SandboxSpec,
    SandboxForkProtocolSpec,
    sandbox_fork_target,
    sandbox_spec_fingerprint,
)
from ucloud_sandboxes.sandbox_exec import new_exec_session_id

FORK_PROTOCOL = SandboxForkProtocolSpec(
    version="agent-v1",
    prepare_command=("/ucloud/fork-agent", "prepare"),
    ready_command=("/ucloud/fork-agent", "ready"),
)


def _wait_for(predicate, *, timeout_seconds: float = 2.0) -> bool:
    deadline = monotonic() + timeout_seconds
    while monotonic() < deadline:
        if predicate():
            return True
        sleep(0.01)
    return bool(predicate())


class CountingPullRuntime(DockerImageRuntime):
    def __init__(self) -> None:
        super().__init__(dry_run=True)
        self._lock = Lock()
        self.pulls: list[str] = []

    def pull(self, image: str) -> CommandResult:
        sleep(0.05)
        with self._lock:
            self.pulls.append(image)
        return CommandResult(argv=("docker", "pull", image), exit_code=0)


class ContextRecordingRuntime(DockerImageRuntime):
    def __init__(self) -> None:
        super().__init__(dry_run=True)
        self.context_paths: list[Path] = []
        self.dockerfiles: list[bytes] = []

    def build(self, spec, *, push=False, on_output=None):
        context_path = Path(spec.context_path)
        self.context_paths.append(context_path)
        self.dockerfiles.append((context_path / spec.dockerfile).read_bytes())
        return super().build(spec, push=push, on_output=on_output)


def _tar_gz_context(files: dict[str, bytes]) -> bytes:
    output = BytesIO()
    with tarfile.open(fileobj=output, mode="w:gz") as archive:
        for name, payload in sorted(files.items()):
            info = tarfile.TarInfo(name)
            info.size = len(payload)
            info.mode = 0o644
            info.mtime = 0
            archive.addfile(info, BytesIO(payload))
    return output.getvalue()


class FileRuntime(DockerGvisorRuntime):
    def __init__(self) -> None:
        super().__init__(dry_run=True, allow_storage_opt_quota=True)
        self.files: dict[tuple[str, str], bytes] = {}

    def write_file_to_container(
        self,
        sandbox_id: str,
        container_path: str,
        content: bytes,
        *,
        owner: str | None = None,
    ) -> CommandResult:
        result = super().write_file_to_container(
            sandbox_id,
            container_path,
            content,
            owner=owner,
        )
        self.files[(sandbox_id, container_path)] = content
        return result

    def read_file_from_container(
        self,
        sandbox_id: str,
        container_path: str,
        *,
        max_bytes: int | None = None,
    ) -> tuple[bytes, CommandResult]:
        _, result = super().read_file_from_container(
            sandbox_id,
            container_path,
            max_bytes=max_bytes,
        )
        content = self.files[(sandbox_id, container_path)]
        return content, result


class ControlPlaneTests(unittest.TestCase):
    def test_parked_managed_job_status_is_gateway_state_and_does_not_wake(self) -> None:
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            route_file = root / "routes.sqlite"
            heartbeat_file = root / "heartbeats.json"
            routing = RoutingStore(route_file)
            route = routing.upsert_sandbox(
                SandboxRoute(
                    sandbox_id="managed-one",
                    node_id="node-1",
                    job_id="vm-1",
                    node_url="http://node.invalid",
                    state="parked",
                    generation=1,
                    create_operation_id="create-managed-one",
                    spec_hash="b" * 64,
                    spec={
                        "id": "managed-one",
                        "image": "busybox",
                        "parkable": True,
                        "managed_process": True,
                    },
                )
            )
            routing.upsert_managed_process(
                route,
                ManagedProcessRecord(
                    sandbox_id=route.sandbox_id,
                    sandbox_generation=route.generation,
                    job_id="rollout-1",
                    spec_sha256="a" * 64,
                    state="running",
                    pid=42,
                    sequence=2,
                    updated_at="2026-08-03T00:00:00+00:00",
                ),
            )
            HeartbeatStore(heartbeat_file).upsert(
                build_heartbeat(
                    job_id=route.job_id,
                    node_id=route.node_id,
                    node_url=route.node_url,
                    active_sandboxes=0,
                    inventory=(
                        SandboxInventoryEntry(
                            sandbox_id=route.sandbox_id,
                            state="parked",
                        ),
                    ),
                    inventory_complete=True,
                )
            )
            gateway = build_server(
                "127.0.0.1",
                0,
                heartbeat_file,
                routing_file=route_file,
            )
            proxied = Event()

            def fail_proxy(*_args, **_kwargs):
                proxied.set()
                raise AssertionError("parked status must not reach a node")

            gateway.RequestHandlerClass._proxy_request = fail_proxy
            thread = Thread(target=gateway.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = gateway.server_address
                payload = self._json_request(
                    f"http://{host}:{port}/v1/sandboxes/managed-one/jobs/rollout-1"
                )
            finally:
                gateway.shutdown()
                gateway.server_close()
                thread.join(timeout=1)

        self.assertFalse(proxied.is_set())
        self.assertEqual(payload["job"]["state"], "running")
        self.assertEqual(payload["job"]["sandbox_generation"], 1)

    def test_managed_job_status_uses_gateway_state_during_node_transition(self) -> None:
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            route_file = root / "routes.sqlite"
            heartbeat_file = root / "heartbeats.json"
            routing = RoutingStore(route_file)
            route = routing.upsert_sandbox(
                SandboxRoute(
                    sandbox_id="managed-transition",
                    node_id="node-1",
                    job_id="vm-1",
                    node_url="http://node.invalid",
                    state="running",
                    generation=1,
                    create_operation_id="create-managed-transition",
                    spec_hash="b" * 64,
                    spec={
                        "id": "managed-transition",
                        "image": "busybox",
                        "parkable": True,
                        "managed_process": True,
                    },
                )
            )
            routing.upsert_managed_process(
                route,
                ManagedProcessRecord(
                    sandbox_id=route.sandbox_id,
                    sandbox_generation=route.generation,
                    job_id="rollout-1",
                    spec_sha256="a" * 64,
                    state="running",
                    pid=42,
                    sequence=2,
                    updated_at="2026-08-03T00:00:00+00:00",
                ),
            )
            gateway = build_server(
                "127.0.0.1",
                0,
                heartbeat_file,
                routing_file=route_file,
            )

            def transition_response(_handler, *_args, **_kwargs):
                return control_plane.ProxiedResponse(
                    400,
                    {"Content-Type": "application/json"},
                    b'{"error":"sandbox lifecycle transition is in progress: '
                    b'managed-transition"}',
                )

            gateway.RequestHandlerClass._proxy_request = transition_response
            thread = Thread(target=gateway.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = gateway.server_address
                payload = self._json_request(
                    f"http://{host}:{port}/v1/sandboxes/managed-transition/"
                    "jobs/rollout-1"
                )
            finally:
                gateway.shutdown()
                gateway.server_close()
                thread.join(timeout=1)

        self.assertEqual(payload["job"]["state"], "running")
        self.assertEqual(payload["job"]["sequence"], 2)

    def test_relay_lifecycle_persists_program_request_transitions(self) -> None:
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            route_file = root / "routes.sqlite"
            heartbeat_file = root / "heartbeats.json"
            metrics_file = root / "metrics.sqlite"
            routing = RoutingStore(route_file)
            route = routing.upsert_sandbox(
                SandboxRoute(
                    sandbox_id="sandbox-1",
                    node_id="node-1",
                    job_id="job-1",
                    node_url="http://node.invalid",
                    resources=ResourceQuantity(
                        vcpu=1,
                        memory_mb=1024,
                        disk_mb=4096,
                    ),
                    state="running",
                )
            )
            HeartbeatStore(heartbeat_file).upsert(
                build_heartbeat(
                    job_id=route.job_id,
                    node_id=route.node_id,
                    node_url=route.node_url,
                    capabilities=("sandbox", "disk-quota"),
                    total_resources=ResourceQuantity(
                        vcpu=8,
                        memory_mb=16_384,
                        disk_mb=100_000,
                    ),
                    inventory_complete=True,
                )
            )
            gateway = build_server(
                "127.0.0.1",
                0,
                heartbeat_file,
                routing_file=route_file,
                metrics_file=metrics_file,
            )

            def fake_proxy_request(_handler, *_args, **_kwargs):
                return control_plane.ProxiedResponse(
                    200,
                    {"Content-Type": "application/json"},
                    b'{"ok":true}',
                )

            gateway.RequestHandlerClass._proxy_request = fake_proxy_request
            gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
            gateway_thread.start()
            try:
                host, port = gateway.server_address
                base = f"http://{host}:{port}/v1/sandboxes/{route.sandbox_id}"
                identity = {
                    "generation": route.generation,
                    "request_id": "request-1",
                    "rollout_id": "rollout-1",
                    "request_created_at": 1_785_489_600.0,
                }
                self._json_request(
                    f"{base}/park",
                    method="POST",
                    payload=identity,
                )
                self._json_request(
                    f"{base}/wake",
                    method="POST",
                    payload=identity,
                )
                self._json_request(
                    f"{base}/wake",
                    method="POST",
                    payload=identity,
                )
                records = RoutingStore(route_file).program_requests_readonly()
                wake_events = control_plane.MetricsStore(metrics_file).load_events(
                    max_events=10,
                    kinds=(
                        "program_wake_shadow_plan",
                        "program_wake_actual",
                    ),
                )
            finally:
                gateway.shutdown()
                gateway.server_close()
                gateway_thread.join(timeout=1)

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].state, "acting")
        self.assertEqual(records[0].rollout_id, "rollout-1")
        self.assertTrue(records[0].parked_at)
        self.assertTrue(records[0].response_ready_at)
        self.assertTrue(records[0].wake_started_at)
        self.assertTrue(records[0].wake_completed_at)
        self.assertEqual(
            {event.kind for event in wake_events},
            {"program_wake_shadow_plan", "program_wake_actual"},
        )
        self.assertEqual(
            sum(event.kind == "program_wake_shadow_plan" for event in wake_events),
            1,
        )
        self.assertEqual(
            sum(event.kind == "program_wake_actual" for event in wake_events),
            1,
        )

    def test_failed_wake_rolls_route_back_and_deduplicates_program_error(self) -> None:
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            route_file = root / "routes.sqlite"
            heartbeat_file = root / "heartbeats.json"
            metrics_file = root / "metrics.sqlite"
            routing = RoutingStore(route_file)
            route = routing.upsert_sandbox(
                SandboxRoute(
                    sandbox_id="sandbox-1",
                    node_id="node-1",
                    job_id="job-1",
                    node_url="http://node.invalid",
                    resources=ResourceQuantity(vcpu=1, memory_mb=1024, disk_mb=4096),
                    state="parked",
                )
            )
            HeartbeatStore(heartbeat_file).upsert(
                build_heartbeat(
                    job_id=route.job_id,
                    node_id=route.node_id,
                    node_url=route.node_url,
                    capabilities=("sandbox", "disk-quota"),
                    total_resources=ResourceQuantity(
                        vcpu=8,
                        memory_mb=16_384,
                        disk_mb=100_000,
                    ),
                    inventory_complete=True,
                )
            )
            gateway = build_server(
                "127.0.0.1",
                0,
                heartbeat_file,
                routing_file=route_file,
                metrics_file=metrics_file,
            )

            def failed_wake(_handler, *_args, **_kwargs):
                return control_plane.ProxiedResponse(
                    503,
                    {"Content-Type": "application/json"},
                    b'{"error":"restore validation failed",'
                    b'"lifecycle_state":"parked"}',
                )

            gateway.RequestHandlerClass._proxy_request = failed_wake
            gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
            gateway_thread.start()
            try:
                host, port = gateway.server_address
                base = f"http://{host}:{port}/v1/sandboxes/{route.sandbox_id}"
                identity = {
                    "generation": route.generation,
                    "request_id": "request-1",
                    "rollout_id": "rollout-1",
                }
                first = self._json_request(
                    f"{base}/wake",
                    method="POST",
                    payload=identity,
                    allow_error=True,
                )
                after_first = RoutingStore(route_file).program_requests_readonly()[0]
                second = self._json_request(
                    f"{base}/wake",
                    method="POST",
                    payload=identity,
                    allow_error=True,
                )
                after_second = RoutingStore(route_file).program_requests_readonly()[0]
                final_route = RoutingStore(route_file).get_sandbox_readonly(
                    route.sandbox_id
                )
                events = control_plane.MetricsStore(metrics_file).load_events(
                    max_events=20,
                    kinds=("program_state_transition", "program_wake_shadow_plan"),
                )
            finally:
                gateway.shutdown()
                gateway.server_close()
                gateway_thread.join(timeout=1)

        self.assertEqual(first["status"], 503)
        self.assertEqual(second["status"], 503)
        self.assertIsNotNone(final_route)
        self.assertEqual(final_route.state, "parked")
        self.assertEqual(after_first.state, "waking")
        self.assertEqual(after_first.last_error, "HTTP 503: restore validation failed")
        self.assertEqual(after_second.updated_at, after_first.updated_at)
        self.assertEqual(
            sum(event.kind == "program_wake_shadow_plan" for event in events),
            1,
        )
        self.assertEqual(
            sum(event.kind == "program_state_transition" for event in events),
            3,
        )

    def test_transport_epoch_changes_only_after_committed_route_handoff(
        self,
    ) -> None:
        route = SandboxRoute(
            sandbox_id="sandbox-1",
            node_id="source",
            job_id="source-job",
            node_url="http://source:8090",
            generation=7,
            create_operation_id="create:7",
        )
        migration = {
            "migration_id": "move:7",
            "sandbox_id": route.sandbox_id,
            "generation": route.generation,
            "create_operation_id": route.create_operation_id,
        }
        baseline = control_plane._sandbox_transport_epoch(route, [])
        staged = control_plane._sandbox_transport_epoch(
            route,
            [SimpleNamespace(**migration, phase="staged")],
        )
        routed = control_plane._sandbox_transport_epoch(
            route,
            [SimpleNamespace(**migration, phase="routed")],
        )
        returned = control_plane._sandbox_transport_epoch(
            route,
            [
                SimpleNamespace(**migration, phase="complete"),
                SimpleNamespace(
                    **{
                        **migration,
                        "migration_id": "move:return",
                    },
                    phase="routed",
                ),
            ],
        )

        self.assertEqual(staged, baseline)
        self.assertNotEqual(routed, baseline)
        self.assertNotEqual(returned, routed)

    def test_fork_proxy_timeout_matches_bounded_runtime_budget(self) -> None:
        self.assertEqual(
            control_plane.FORK_PROXY_TIMEOUT_SECONDS,
            FORK_REQUEST_TIMEOUT_SECONDS,
        )
        self.assertEqual(FORK_REQUEST_TIMEOUT_SECONDS, 55 * 60)

    def test_fork_request_preflight_accounts_for_expanded_batch_specs(self) -> None:
        source = SandboxSpec(
            id="fork-parent",
            image="busybox",
            env={"LARGE_INHERITED_VALUE": "x" * 4096},
            memory_mb=64,
            disk_mb=64,
            forkable=True,
            fork_protocol=FORK_PROTOCOL,
        )
        source_route = SandboxRoute(
            sandbox_id=source.id,
            node_id="node-1",
            job_id="job-1",
            node_url="http://node.invalid",
            resources=source.requested_resources(),
            spec=source.to_dict(),
            state="running",
            generation=1,
            create_operation_id="create-parent",
            spec_hash=sandbox_spec_fingerprint(source),
        )
        targets = tuple(
            sandbox_fork_target(source, {"id": f"child-{index}"}) for index in range(4)
        )
        public_body = json.dumps(
            {"sandboxes": [{"id": target.id} for target in targets]}
        ).encode("utf-8")

        expanded_size = control_plane._sandbox_fork_request_body_upper_bound(
            source_route,
            targets,
            batch=True,
        )

        self.assertGreater(expanded_size, len(public_body) * 20)
        self.assertGreater(expanded_size, 16_000)

    def test_fork_route_release_follows_node_intent_signal(self) -> None:
        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            route_file = raw_path / "routes.sqlite"
            source_spec = SandboxSpec(
                id="fork-parent",
                image="busybox",
                command=("sleep", "infinity"),
                memory_mb=64,
                disk_mb=64,
                forkable=True,
                fork_protocol=FORK_PROTOCOL,
            )
            source_hash = sandbox_spec_fingerprint(source_spec)
            routes = RoutingStore(route_file)
            routes.upsert_sandbox(
                SandboxRoute(
                    sandbox_id=source_spec.id,
                    node_id="node-1",
                    job_id="job-1",
                    node_url="http://node.invalid",
                    resources=source_spec.requested_resources(),
                    spec=source_spec.to_dict(),
                    state="running",
                    generation=1,
                    create_operation_id="create-parent",
                    spec_hash=source_hash,
                )
            )
            gateway = build_server(
                "127.0.0.1",
                0,
                raw_path / "heartbeats.json",
                routing_file=route_file,
            )
            responses = [
                control_plane.ProxiedResponse(
                    503,
                    {"Content-Type": "application/json"},
                    b'{"error":"capacity changed","intent_persisted":false}',
                ),
                control_plane.ProxiedResponse(
                    409,
                    {"Content-Type": "application/json"},
                    b'{"error":"restore interrupted","intent_persisted":true}',
                ),
                control_plane.ProxiedResponse(
                    503,
                    {"Content-Type": "application/json"},
                    b'{"error":"batch rejected","intent_persisted":false}',
                ),
                control_plane.ProxiedResponse(
                    502,
                    {"Content-Type": "application/json"},
                    b'{"error":"ambiguous"}',
                ),
                control_plane.ProxiedResponse(
                    503,
                    {"Content-Type": "application/json"},
                    b'{"error":"definitive","intent_persisted":false}',
                ),
                control_plane.ProxiedResponse(
                    409,
                    {"Content-Type": "application/json"},
                    b'{"error":"existing intent","intent_persisted":true}',
                ),
                control_plane.ProxiedResponse(
                    409,
                    {"Content-Type": "application/json"},
                    (
                        b'{"error":"overlapping fanout","intents":['
                        b'{"sandbox_id":"overlap-a","intent_persisted":true},'
                        b'{"sandbox_id":"overlap-c","intent_persisted":false}]}'
                    ),
                ),
            ]

            def fake_proxy_request(_handler, *_args, **_kwargs):
                return responses.pop(0)

            gateway.RequestHandlerClass._proxy_request = fake_proxy_request
            Thread(target=gateway.serve_forever, daemon=True).start()
            try:
                host, port = gateway.server_address
                base = f"http://{host}:{port}"
                self.assertEqual(
                    post_heartbeat(
                        f"{base}/v1/nodes/heartbeat",
                        build_heartbeat(
                            job_id="job-1",
                            node_id="node-1",
                            node_url="http://node.invalid",
                            agent_version=package_version(),
                            capabilities=(
                                "sandbox",
                                "fork-local-v1",
                                "disk-quota",
                            ),
                            total_resources=ResourceQuantity(
                                memory_mb=1024,
                                disk_mb=1024,
                            ),
                        ),
                    ).status,
                    200,
                )
                mixed_shape = self._json_request(
                    f"{base}/v1/sandboxes/fork-parent/forks",
                    method="POST",
                    payload={
                        "sandbox": {"id": "mixed-one"},
                        "sandboxes": [{"id": "mixed-two"}],
                    },
                    allow_error=True,
                )
                injected_fence = self._json_request(
                    f"{base}/v1/sandboxes/fork-parent/forks",
                    method="POST",
                    payload={
                        "sandbox": {"id": "injected"},
                        "_ucloud_source": {
                            "generation": 1,
                            "spec_hash": source_hash,
                        },
                    },
                    allow_error=True,
                )
                before_intent = self._json_request(
                    f"{base}/v1/sandboxes/fork-parent/forks",
                    method="POST",
                    payload={"sandbox": {"id": "before-intent"}},
                    allow_error=True,
                )
                released = RoutingStore(route_file).get_sandbox("before-intent")
                after_intent = self._json_request(
                    f"{base}/v1/sandboxes/fork-parent/forks",
                    method="POST",
                    payload={"sandbox": {"id": "after-intent"}},
                    allow_error=True,
                )
                retained = RoutingStore(route_file).get_sandbox("after-intent")
                rejected_batch = self._json_request(
                    f"{base}/v1/sandboxes/fork-parent/forks",
                    method="POST",
                    payload={
                        "sandboxes": [
                            {"id": "batch-before-intent-a"},
                            {"id": "batch-before-intent-b"},
                        ]
                    },
                    allow_error=True,
                )
                rejected_routes = (
                    RoutingStore(route_file).get_sandbox("batch-before-intent-a"),
                    RoutingStore(route_file).get_sandbox("batch-before-intent-b"),
                )
                self._json_request(
                    f"{base}/v1/sandboxes/fork-parent/forks",
                    method="POST",
                    payload={"sandbox": {"id": "ambiguous-then-false"}},
                    allow_error=True,
                )
                ambiguous_route = RoutingStore(route_file).get_sandbox(
                    "ambiguous-then-false"
                )
                self._json_request(
                    f"{base}/v1/sandboxes/fork-parent/forks",
                    method="POST",
                    payload={"sandbox": {"id": "ambiguous-then-false"}},
                    allow_error=True,
                )
                definitive_route = RoutingStore(route_file).get_sandbox(
                    "ambiguous-then-false"
                )
                self._json_request(
                    f"{base}/v1/sandboxes/fork-parent/forks",
                    method="POST",
                    payload={"sandbox": {"id": "overlap-a"}},
                    allow_error=True,
                )
                overlap_a_before = RoutingStore(route_file).get_sandbox("overlap-a")
                self._json_request(
                    f"{base}/v1/sandboxes/fork-parent/forks",
                    method="POST",
                    payload={
                        "sandboxes": [
                            {"id": "overlap-a"},
                            {"id": "overlap-c"},
                        ]
                    },
                    allow_error=True,
                )
                overlap_a_after = RoutingStore(route_file).get_sandbox("overlap-a")
                overlap_c_after = RoutingStore(route_file).get_sandbox("overlap-c")
            finally:
                gateway.shutdown()
                gateway.server_close()

        self.assertEqual(before_intent["status"], 503, before_intent)
        self.assertEqual(mixed_shape["status"], 400)
        self.assertEqual(injected_fence["status"], 400)
        self.assertIsNone(released)
        self.assertEqual(after_intent["status"], 409)
        self.assertIsNotNone(retained)
        self.assertEqual(rejected_batch["status"], 503)
        self.assertEqual(rejected_routes, (None, None))
        self.assertIsNotNone(ambiguous_route)
        self.assertIsNone(definitive_route)
        self.assertIsNotNone(overlap_a_before)
        self.assertEqual(overlap_a_after, overlap_a_before)
        self.assertIsNone(overlap_c_after)

    def test_fork_route_survives_ambiguous_then_busy_replay(self) -> None:
        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            route_file = raw_path / "routes.sqlite"
            source_spec = SandboxSpec(
                id="fork-parent",
                image="busybox",
                command=("sleep", "infinity"),
                memory_mb=64,
                disk_mb=64,
                forkable=True,
                fork_protocol=FORK_PROTOCOL,
            )
            source_hash = sandbox_spec_fingerprint(source_spec)
            routes = RoutingStore(route_file)
            routes.upsert_sandbox(
                SandboxRoute(
                    sandbox_id=source_spec.id,
                    node_id="node-1",
                    job_id="job-1",
                    node_url="http://node.invalid",
                    resources=source_spec.requested_resources(),
                    spec=source_spec.to_dict(),
                    state="running",
                    generation=1,
                    create_operation_id="create-parent",
                    spec_hash=source_hash,
                )
            )
            gateway = build_server(
                "127.0.0.1",
                0,
                raw_path / "heartbeats.json",
                routing_file=route_file,
            )
            responses = [
                control_plane.ProxiedResponse(
                    502,
                    {"Content-Type": "application/json"},
                    b'{"error":"node connection closed"}',
                ),
                control_plane.ProxiedResponse(
                    409,
                    {"Content-Type": "application/json"},
                    b'{"error":"sandbox has active exec/file activity"}',
                ),
            ]

            def fake_proxy_request(_handler, *_args, **_kwargs):
                return responses.pop(0)

            gateway.RequestHandlerClass._proxy_request = fake_proxy_request
            Thread(target=gateway.serve_forever, daemon=True).start()
            try:
                host, port = gateway.server_address
                base = f"http://{host}:{port}"
                self.assertEqual(
                    post_heartbeat(
                        f"{base}/v1/nodes/heartbeat",
                        build_heartbeat(
                            job_id="job-1",
                            node_id="node-1",
                            node_url="http://node.invalid",
                            agent_version=package_version(),
                            capabilities=(
                                "sandbox",
                                "fork-local-v1",
                                "disk-quota",
                            ),
                            total_resources=ResourceQuantity(
                                memory_mb=1024,
                                disk_mb=1024,
                            ),
                        ),
                    ).status,
                    200,
                )
                first = self._json_request(
                    f"{base}/v1/sandboxes/fork-parent/forks",
                    method="POST",
                    payload={"sandbox": {"id": "fork-child"}},
                    allow_error=True,
                )
                after_first = RoutingStore(route_file).get_sandbox("fork-child")
                second = self._json_request(
                    f"{base}/v1/sandboxes/fork-parent/forks",
                    method="POST",
                    payload={"sandbox": {"id": "fork-child"}},
                    allow_error=True,
                )
                after_second = RoutingStore(route_file).get_sandbox("fork-child")
            finally:
                gateway.shutdown()
                gateway.server_close()

        self.assertEqual(first["status"], 502)
        self.assertEqual(second["status"], 409)
        self.assertIsNotNone(after_first)
        self.assertEqual(after_second, after_first)

    def test_live_fork_reserves_and_replays_child_on_source_node(self) -> None:
        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            runtime = DockerGvisorRuntime(
                dry_run=True,
                allow_storage_opt_quota=True,
                fork_enabled=True,
                checkpoint_root=raw_path / "checkpoints",
            )
            node = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=raw_path / "node-sandboxes.json",
                image_file=raw_path / "node-images.json",
                job_id="job-1",
                node_id="node-1",
                total_resources=ResourceQuantity(
                    vcpu=4,
                    memory_mb=4096,
                    disk_mb=4096,
                ),
                runtime=runtime,
                extra_capabilities=("fork-local-v1", "disk-quota"),
            )
            Thread(target=node.serve_forever, daemon=True).start()
            try:
                node_host, node_port = node.server_address
                node_url = f"http://{node_host}:{node_port}"
                route_file = raw_path / "routes.sqlite"
                gateway = build_server(
                    "127.0.0.1",
                    0,
                    raw_path / "heartbeats.json",
                    routing_file=route_file,
                )
                Thread(target=gateway.serve_forever, daemon=True).start()
                try:
                    host, port = gateway.server_address
                    base = f"http://{host}:{port}"
                    heartbeat = build_heartbeat(
                        job_id="job-1",
                        node_id="node-1",
                        node_url=node_url,
                        agent_version=package_version(),
                        capabilities=(
                            "sandbox",
                            "image-cache",
                            "fork-local-v1",
                            "disk-quota",
                        ),
                        total_resources=ResourceQuantity(
                            vcpu=4,
                            memory_mb=4096,
                            disk_mb=4096,
                        ),
                    )
                    self.assertEqual(
                        post_heartbeat(f"{base}/v1/nodes/heartbeat", heartbeat).status,
                        200,
                    )
                    parent = self._json_request(
                        f"{base}/v1/sandboxes",
                        method="POST",
                        payload={
                            "id": "fork-parent",
                            "image": "busybox",
                            "command": ["sleep", "infinity"],
                            "memory_mb": 64,
                            "disk_mb": 64,
                            "forkable": True,
                            "fork_protocol": FORK_PROTOCOL.to_dict(),
                            "network": "bridge",
                        },
                    )["sandbox"]
                    self.assertEqual(
                        parent["spec_hash"],
                        sandbox_spec_fingerprint(SandboxSpec.from_dict(parent["spec"])),
                    )

                    node_manager = node.RequestHandlerClass.manager
                    node_parent = node_manager.get("fork-parent")
                    self.assertIsNotNone(node_parent)
                    node_manager.store.upsert(replace(node_parent, state="running"))
                    routes = RoutingStore(route_file)
                    parent_route = routes.get_sandbox("fork-parent")
                    self.assertIsNotNone(parent_route)
                    routes.upsert_sandbox(replace(parent_route, state="running"))

                    forked = self._json_request(
                        f"{base}/v1/sandboxes/fork-parent/forks",
                        method="POST",
                        payload={
                            "id": "fork-child",
                            "env": {"AGENT_BRANCH": "child"},
                        },
                    )
                    replayed = self._json_request(
                        f"{base}/v1/sandboxes/fork-parent/forks",
                        method="POST",
                        payload={
                            "id": "fork-child",
                            "env": {"AGENT_BRANCH": "child"},
                        },
                    )
                    fanout = self._json_request(
                        f"{base}/v1/sandboxes/fork-parent/forks",
                        method="POST",
                        payload={
                            "sandboxes": [
                                {"id": "fork-child-a"},
                                {"id": "fork-child-b"},
                            ]
                        },
                    )
                    fanout_replayed = self._json_request(
                        f"{base}/v1/sandboxes/fork-parent/forks",
                        method="POST",
                        payload={
                            "sandboxes": [
                                {"id": "fork-child-a"},
                                {"id": "fork-child-b"},
                            ]
                        },
                    )
                finally:
                    gateway.shutdown()
                    gateway.server_close()
            finally:
                node.shutdown()
                node.server_close()

            parent_route = RoutingStore(route_file).get_sandbox("fork-parent")
            child_route = RoutingStore(route_file).get_sandbox("fork-child")
            child_a_route = RoutingStore(route_file).get_sandbox("fork-child-a")
            child_b_route = RoutingStore(route_file).get_sandbox("fork-child-b")
            self.assertIsNotNone(parent_route)
            self.assertIsNotNone(child_route)
            self.assertIsNotNone(child_a_route)
            self.assertIsNotNone(child_b_route)
            self.assertEqual(child_route.node_id, parent_route.node_id)
            self.assertTrue(child_route.create_operation_id.startswith("fork-"))
            self.assertEqual(forked["sandbox"]["source_sandbox_id"], "fork-parent")
            self.assertEqual(
                forked["sandbox"]["source_generation"], parent["generation"]
            )
            self.assertEqual(forked["fork"]["commands"], [])
            self.assertTrue(replayed["timings"]["manager"]["idempotent"])
            self.assertEqual(
                [record["id"] for record in fanout["sandboxes"]],
                ["fork-child-a", "fork-child-b"],
            )
            self.assertEqual(
                len({item["checkpoint_id"] for item in fanout["forks"]}),
                1,
            )
            self.assertEqual(child_a_route.node_id, parent_route.node_id)
            self.assertEqual(child_b_route.node_id, parent_route.node_id)
            self.assertTrue(fanout_replayed["timings"]["manager"]["idempotent"])

    def test_gateway_replaces_public_auth_with_node_control_credential(self) -> None:
        observed: dict[str, str | None] = {}

        class NodeProbeHandler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                observed["authorization"] = self.headers.get("Authorization")
                observed["public_token"] = self.headers.get("X-UCloud-Sandbox-Token")
                observed["proxy_authorization"] = self.headers.get(
                    "Proxy-Authorization"
                )
                body = b'{"sandboxes": []}'
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *_args: object) -> None:
                return

        with TemporaryDirectory() as raw_dir:
            node = ThreadingHTTPServer(("127.0.0.1", 0), NodeProbeHandler)
            Thread(target=node.serve_forever, daemon=True).start()
            try:
                node_host, node_port = node.server_address
                gateway = build_server(
                    "127.0.0.1",
                    0,
                    Path(raw_dir) / "heartbeats.json",
                    upstream_node_url=f"http://{node_host}:{node_port}",
                    gateway_bearer_token="gateway-secret",
                    node_control_bearer_token="node-secret",
                )
                Thread(target=gateway.serve_forever, daemon=True).start()
                try:
                    host, port = gateway.server_address
                    payload = self._json_request(
                        f"http://{host}:{port}/v1/sandboxes",
                        headers={
                            "Authorization": "Bearer gateway-secret",
                            "X-UCloud-Sandbox-Token": "gateway-secret",
                            "Proxy-Authorization": "Bearer leaked",
                        },
                    )
                finally:
                    gateway.shutdown()
                    gateway.server_close()
            finally:
                node.shutdown()
                node.server_close()

        self.assertEqual(payload, {"sandboxes": []})
        self.assertEqual(observed["authorization"], "Bearer node-secret")
        self.assertIsNone(observed["public_token"])
        self.assertIsNone(observed["proxy_authorization"])

    def test_gateway_server_uses_high_listen_backlog(self) -> None:
        with TemporaryDirectory() as raw_dir:
            server = build_server(
                "127.0.0.1",
                0,
                Path(raw_dir) / "heartbeats.json",
            )
            try:
                self.assertGreaterEqual(
                    server.request_queue_size,
                    DEFAULT_HTTP_REQUEST_QUEUE_SIZE,
                )
                self.assertEqual(
                    server.max_request_threads,
                    DEFAULT_MAX_GATEWAY_HTTP_REQUEST_THREADS,
                )
            finally:
                server.server_close()

    def test_accepts_heartbeat_and_lists_nodes(self) -> None:
        with TemporaryDirectory() as raw_dir:
            heartbeat_file = Path(raw_dir) / "heartbeats.json"
            server = build_server(
                "127.0.0.1",
                0,
                heartbeat_file,
                metrics_file=Path(raw_dir) / "metrics.jsonl",
            )
            thread = Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address
                heartbeat = build_heartbeat(
                    job_id="job-1",
                    node_id="node-1",
                    active_sandboxes=1,
                )

                result = post_heartbeat(
                    f"http://{host}:{port}/v1/nodes/heartbeat",
                    heartbeat,
                )

                self.assertEqual(result.status, 200)
                with request.urlopen(
                    f"http://{host}:{port}/v1/nodes", timeout=5
                ) as response:
                    payload = json.loads(response.read().decode("utf-8"))
                metrics = self._json_request(f"http://{host}:{port}/v1/metrics")
            finally:
                server.shutdown()
                server.server_close()

            self.assertEqual(len(payload["nodes"]), 1)
            self.assertEqual(payload["nodes"][0]["job_id"], "job-1")
            self.assertEqual(metrics["nodes"]["samples"], 1)
            self.assertEqual(
                metrics["nodes"]["recent_samples"][0]["data"]["job_id"],
                "job-1",
            )
            self.assertTrue(heartbeat_file.exists())

    def test_gateway_stamps_heartbeat_receipt_time_and_enforces_deployment(
        self,
    ) -> None:
        with TemporaryDirectory() as raw_dir:
            heartbeat_file = Path(raw_dir) / "heartbeats.json"
            server = build_server(
                "127.0.0.1",
                0,
                heartbeat_file,
                deployment_id="prod-a",
            )
            thread = Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address
                base = f"http://{host}:{port}/v1/nodes/heartbeat"
                rejected = post_heartbeat(
                    base,
                    build_heartbeat(
                        job_id="wrong-job",
                        node_id="wrong-node",
                        deployment_id="prod-b",
                    ),
                )
                future = utc_now() + timedelta(days=30)
                before = utc_now()
                accepted = post_heartbeat(
                    base,
                    build_heartbeat(
                        job_id="job-1",
                        node_id="node-1",
                        deployment_id="prod-a",
                        now=future,
                    ),
                )
                after = utc_now()
                stored = HeartbeatStore(heartbeat_file).load()["job-1"]
            finally:
                server.shutdown()
                server.server_close()

        self.assertEqual(rejected.status, 403)
        self.assertEqual(accepted.status, 200)
        self.assertEqual(stored.reported_at, future)
        self.assertIsNotNone(stored.received_at)
        self.assertGreaterEqual(stored.received_at, before)
        self.assertLessEqual(stored.received_at, after)
        self.assertEqual(stored.updated_at, stored.received_at)
        self.assertTrue(stored.is_fresh(after, 5))

    def test_disk_request_requires_disk_quota_capability(self) -> None:
        heartbeat = NodeHeartbeat(
            node_id="node-1",
            job_id="job-1",
            updated_at=utc_now(),
            active_sandboxes=0,
            node_url="http://node-1:8090",
            capabilities=("sandbox",),
            total_resources=ResourceQuantity(
                vcpu=4,
                memory_mb=8192,
                disk_mb=100_000,
            ),
        )
        requested = ResourceQuantity(vcpu=1, memory_mb=512, disk_mb=1024)

        self.assertFalse(control_plane._node_can_fit(heartbeat, requested, []))
        self.assertTrue(
            control_plane._node_can_fit(
                replace(heartbeat, capabilities=("sandbox", "disk-quota")),
                requested,
                [],
            )
        )

    def test_memory_pressure_blocks_new_work_only_on_overcommitted_nodes(self) -> None:
        requested = ResourceQuantity(vcpu=1, memory_mb=4096, disk_mb=0)
        heartbeat = NodeHeartbeat(
            node_id="node-1",
            job_id="job-1",
            updated_at=utc_now(),
            active_sandboxes=0,
            total_resources=ResourceQuantity(vcpu=32, memory_mb=98304, disk_mb=450560),
            memory_overcommit=2.0,
            runtime_metrics=NodeRuntimeMetrics(
                collected_at=utc_now(),
                memory_total_mb=98304,
                memory_available_mb=1024,
                swap_total_mb=98304,
                swap_free_mb=1024,
            ),
        )

        self.assertFalse(control_plane._node_can_fit(heartbeat, requested, []))
        self.assertFalse(
            control_plane._node_can_fit(
                replace(
                    heartbeat,
                    runtime_metrics=replace(
                        heartbeat.runtime_metrics,
                        memory_available_mb=8192,
                        swap_free_mb=8192,
                        memory_psi_full_avg10=10.0,
                    ),
                ),
                requested,
                [],
            )
        )
        self.assertTrue(
            control_plane._node_can_fit(
                replace(heartbeat, memory_overcommit=1.0), requested, []
            )
        )

    def test_dynamic_direct_placement_uses_live_pressure_not_resident_limits(
        self,
    ) -> None:
        requested = ResourceQuantity(vcpu=4, memory_mb=8192, disk_mb=4096)
        heartbeat = NodeHeartbeat(
            node_id="node-1",
            job_id="job-1",
            updated_at=utc_now(),
            active_sandboxes=64,
            total_resources=ResourceQuantity(
                vcpu=32,
                memory_mb=98304,
                disk_mb=1_449_984,
            ),
            used_resources=ResourceQuantity(
                vcpu=64 * requested.vcpu,
                memory_mb=64 * requested.memory_mb,
                disk_mb=64 * requested.disk_mb,
            ),
            capabilities=("disk-quota", "dynamic-active-admission-v1"),
            runtime_metrics=NodeRuntimeMetrics(
                collected_at=utc_now(),
                cpu_percent=5.0,
                cpu_count=32,
                memory_total_mb=98304,
                memory_available_mb=80000,
                swap_total_mb=98304,
                swap_free_mb=90000,
                memory_psi_full_avg10=0.0,
                load_average_1m=2.0,
            ),
        )

        self.assertTrue(control_plane._node_can_fit(heartbeat, requested, []))
        self.assertFalse(
            control_plane._node_can_fit(
                replace(heartbeat, runtime_metrics=None),
                requested,
                [],
            )
        )
        self.assertFalse(
            control_plane._node_can_fit(
                replace(
                    heartbeat,
                    runtime_metrics=replace(
                        heartbeat.runtime_metrics,
                        cpu_percent=95.0,
                    ),
                ),
                requested,
                [],
            )
        )

    def test_dynamic_direct_placement_ignores_provisional_compute_but_not_disk(
        self,
    ) -> None:
        requested = ResourceQuantity(vcpu=4, memory_mb=4096, disk_mb=17_472)
        heartbeat = NodeHeartbeat(
            node_id="node-1",
            job_id="job-1",
            updated_at=utc_now(),
            active_sandboxes=0,
            node_url="http://node-1:8090",
            total_resources=ResourceQuantity(
                vcpu=32,
                memory_mb=98304,
                disk_mb=1_449_984,
            ),
            capabilities=("disk-quota", "dynamic-active-admission-v1"),
            runtime_metrics=NodeRuntimeMetrics(
                collected_at=utc_now(),
                cpu_percent=2.0,
                cpu_count=32,
                memory_total_mb=98304,
                memory_available_mb=80000,
                swap_total_mb=98304,
                swap_free_mb=90000,
                memory_psi_full_avg10=0.0,
                load_average_1m=1.0,
            ),
        )
        routes = [
            SandboxRoute(
                sandbox_id=f"creating-{index}",
                node_id="node-1",
                job_id="job-1",
                node_url="http://node-1:8090",
                resources=requested,
                state="creating",
            )
            for index in range(8)
        ]

        self.assertTrue(control_plane._node_can_fit(heartbeat, requested, routes))

        disk_filling_route = SandboxRoute(
            sandbox_id="disk-filling",
            node_id="node-1",
            job_id="job-1",
            node_url="http://node-1:8090",
            resources=ResourceQuantity(disk_mb=1_449_984 - 8 * requested.disk_mb),
            state="creating",
        )
        self.assertFalse(
            control_plane._node_can_fit(
                heartbeat,
                requested,
                [*routes, disk_filling_route],
            )
        )

    def test_waking_route_reserves_compute_delta_without_double_charging_disk(
        self,
    ) -> None:
        resources = ResourceQuantity(vcpu=2, memory_mb=4096, disk_mb=8192)
        heartbeat = NodeHeartbeat(
            node_id="node-1",
            job_id="job-1",
            updated_at=utc_now(),
            active_sandboxes=0,
            node_url="http://node-1:8090",
            used_resources=ResourceQuantity(disk_mb=resources.disk_mb),
            inventory=(
                SandboxInventoryEntry(
                    sandbox_id="sandbox-1",
                    state="parked",
                    resources=resources,
                ),
            ),
            inventory_complete=True,
        )
        route = SandboxRoute(
            sandbox_id="sandbox-1",
            node_id="node-1",
            job_id="job-1",
            node_url=heartbeat.node_url or "",
            resources=resources,
            state="waking",
        )

        reserved = control_plane._node_reserved_route_resources(
            heartbeat,
            [route],
        )

        self.assertEqual(
            reserved,
            ResourceQuantity(vcpu=2, memory_mb=4096),
        )

    def test_failed_parked_wake_records_full_relocation_demand(self) -> None:
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            routing = RoutingStore(root / "routes.sqlite")
            resources = ResourceQuantity(vcpu=2, memory_mb=4096, disk_mb=8192)
            route = routing.upsert_sandbox(
                SandboxRoute(
                    sandbox_id="sandbox-1",
                    node_id="node-1",
                    job_id="job-1",
                    node_url="http://node-1:8090",
                    resources=resources,
                    state="parked",
                )
            )
            heartbeats = HeartbeatStore(root / "heartbeats.json")
            heartbeats.upsert(
                NodeHeartbeat(
                    node_id="node-1",
                    job_id="job-1",
                    updated_at=utc_now(),
                    active_sandboxes=1,
                    node_url=route.node_url,
                    capabilities=(
                        "sandbox",
                        "disk-quota",
                        "sandbox-migrate-v2",
                    ),
                    total_resources=resources,
                    used_resources=resources,
                    inventory=(
                        SandboxInventoryEntry(
                            sandbox_id=route.sandbox_id,
                            state="parked",
                            resources=resources,
                        ),
                    ),
                    inventory_complete=True,
                )
            )
            handler = object.__new__(control_plane.ControlPlaneHandler)
            handler.routing_store = routing
            handler.store = heartbeats
            handler.heartbeat_ttl_seconds = 120
            written: list[tuple[dict, object, dict | None]] = []
            handler._write_json = lambda payload, *, status, headers=None: written.append(
                (payload, status, headers)
            )

            selected = handler._ensure_parked_sandbox_wake_placement(route)
            pending = {
                item.sandbox_id: item
                for item in routing.pending_sandboxes()
            }

        self.assertIsNone(selected)
        demand_id = control_plane._wake_pending_demand_id(route.sandbox_id)
        self.assertEqual(pending[demand_id].resources, resources)
        self.assertEqual(written[0][1], 503)
        self.assertTrue(written[0][0]["retryable"])

    def test_wake_image_pull_does_not_hold_global_placement_lock(self) -> None:
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            routing = RoutingStore(root / "routes.sqlite")
            resources = ResourceQuantity(vcpu=4, memory_mb=8192, disk_mb=8192)
            parked = routing.upsert_sandbox(
                SandboxRoute(
                    sandbox_id="parked",
                    node_id="source-node",
                    job_id="source-job",
                    node_url="http://source:8090",
                    resources=resources,
                    spec={"id": "parked", "image": "cold-image"},
                    state="parked",
                )
            )
            routing.upsert_sandbox(
                SandboxRoute(
                    sandbox_id="source-busy",
                    node_id="source-node",
                    job_id="source-job",
                    node_url="http://source:8090",
                    resources=resources,
                    state="running",
                )
            )
            heartbeats = HeartbeatStore(root / "heartbeats.json")
            for heartbeat in (
                NodeHeartbeat(
                    node_id="source-node",
                    job_id="source-job",
                    updated_at=utc_now(),
                    active_sandboxes=1,
                    node_url="http://source:8090",
                    agent_version=package_version(),
                    capabilities=("sandbox", "disk-quota", "sandbox-migrate-v2"),
                    total_resources=ResourceQuantity(
                        vcpu=4,
                        memory_mb=8192,
                        disk_mb=100_000,
                    ),
                    inventory_complete=True,
                ),
                NodeHeartbeat(
                    node_id="destination-node",
                    job_id="destination-job",
                    updated_at=utc_now(),
                    active_sandboxes=0,
                    node_url="http://destination:8090",
                    agent_version=package_version(),
                    capabilities=("sandbox", "disk-quota", "sandbox-migrate-v2"),
                    total_resources=ResourceQuantity(
                        vcpu=8,
                        memory_mb=16_384,
                        disk_mb=100_000,
                    ),
                    inventory_complete=True,
                ),
            ):
                heartbeats.upsert(heartbeat)

            handler = object.__new__(control_plane.ControlPlaneHandler)
            handler.routing_store = routing
            handler.store = heartbeats
            handler.heartbeat_ttl_seconds = 120
            handler.registry_url = ""
            handler.registry_worker_url = ""
            handler.registry_layer_cache = None
            handler._write_json = lambda *_args, **_kwargs: None
            pull_started = Event()
            release_pull = Event()

            def slow_prepare(_source, _destination):
                pull_started.set()
                release_pull.wait(timeout=2)
                return False

            handler._prepare_migration_destination_image = slow_prepare
            wake_thread = Thread(
                target=handler._ensure_parked_sandbox_wake_placement,
                args=(parked,),
                daemon=True,
            )
            wake_thread.start()
            self.assertTrue(pull_started.wait(timeout=1))
            started = monotonic()
            placement = handler._select_and_reserve_node(
                "concurrent-create",
                ResourceQuantity(vcpu=1, memory_mb=1024, disk_mb=1024),
                image="",
                spec={"id": "concurrent-create", "image": "busybox"},
                spec_hash="concurrent-spec",
            )
            elapsed = monotonic() - started
            release_pull.set()
            wake_thread.join(timeout=2)
            migrations = routing.sandbox_migrations(active_only=True)

        self.assertIsNotNone(placement)
        self.assertLess(elapsed, control_plane.SANDBOX_PLACEMENT_LOCK_WAIT_SECONDS)
        self.assertEqual(len(migrations), 1)
        self.assertEqual(migrations[0].destination_node_id, "destination-node")

    def test_inflight_import_reserves_destination_for_normal_placement(self) -> None:
        with TemporaryDirectory() as raw_dir:
            routing = RoutingStore(Path(raw_dir) / "routes.sqlite")
            source = routing.upsert_sandbox(
                SandboxRoute(
                    sandbox_id="sandbox-1",
                    node_id="source-node",
                    job_id="source-job",
                    node_url="http://source:8090",
                    resources=ResourceQuantity(
                        vcpu=2,
                        memory_mb=4096,
                        disk_mb=8192,
                    ),
                    state="parked",
                )
            )
            routing.begin_sandbox_migration(
                source,
                migration_id="migration-1",
                destination_node_id="destination-node",
                destination_job_id="destination-job",
                destination_node_url="http://destination:8090",
            )
            handler = object.__new__(control_plane.ControlPlaneHandler)
            handler.routing_store = routing

            routes = handler._placement_routes()

        reservation = next(
            route
            for route in routes
            if route.sandbox_id == "__migration__:migration-1"
        )
        self.assertEqual(reservation.node_id, "destination-node")
        self.assertEqual(reservation.resources, source.resources)
        self.assertEqual(reservation.state, "creating")

    def test_forkable_placement_requires_fork_and_disk_capabilities(self) -> None:
        with TemporaryDirectory() as raw_dir:
            handler = object.__new__(control_plane.ControlPlaneHandler)
            handler.routing_store = RoutingStore(Path(raw_dir) / "routes.sqlite")
            base = NodeHeartbeat(
                node_id="node-base",
                job_id="job-base",
                updated_at=utc_now(),
                active_sandboxes=0,
                node_url="http://node-base:8090",
                agent_version=package_version(),
                total_resources=ResourceQuantity(
                    vcpu=4,
                    memory_mb=8192,
                    disk_mb=100_000,
                ),
            )
            candidates = [
                replace(
                    base,
                    node_id="fork-only",
                    capabilities=("sandbox", "fork-local-v1"),
                ),
                replace(
                    base,
                    node_id="disk-only",
                    capabilities=("sandbox", "disk-quota"),
                ),
                replace(
                    base,
                    node_id="both",
                    capabilities=(
                        "sandbox",
                        "fork-local-v1",
                        "disk-quota",
                    ),
                ),
            ]
            handler._ready_sandbox_heartbeats = lambda: candidates
            handler._nodes_with_image = lambda *_args, **_kwargs: set()

            selected = handler._select_node(
                ResourceQuantity(memory_mb=512, disk_mb=1024),
                required_capabilities=("fork-local-v1", "disk-quota"),
            )

        self.assertIsNotNone(selected)
        self.assertEqual(selected.node_id, "both")

    def test_cold_image_placement_spreads_distinct_pulls_and_reuses_inflight(
        self,
    ) -> None:
        with TemporaryDirectory() as raw_dir:
            handler = object.__new__(control_plane.ControlPlaneHandler)
            handler.routing_store = RoutingStore(Path(raw_dir) / "routes.sqlite")
            base = NodeHeartbeat(
                node_id="node-1",
                job_id="job-1",
                updated_at=utc_now(),
                active_sandboxes=0,
                node_url="http://node-1:8090",
                agent_version=package_version(),
                capabilities=("sandbox", "image-cache", "disk-quota"),
                total_resources=ResourceQuantity(
                    vcpu=4,
                    memory_mb=8192,
                    disk_mb=100_000,
                ),
                cached_images_known=True,
            )
            candidates = [
                base,
                replace(
                    base,
                    node_id="node-2",
                    job_id="job-2",
                    node_url="http://node-2:8090",
                ),
            ]
            handler._ready_sandbox_heartbeats = lambda: candidates
            handler._nodes_with_image = lambda *_args, **_kwargs: set()
            handler.routing_store.upsert_sandbox(
                SandboxRoute(
                    sandbox_id="first",
                    node_id="node-1",
                    job_id="job-1",
                    node_url="http://node-1:8090",
                    resources=ResourceQuantity(
                        vcpu=1,
                        memory_mb=512,
                        disk_mb=1024,
                    ),
                    spec={"image": "registry.test/team/a@sha256:" + "a" * 64},
                    state="creating",
                )
            )

            distinct = handler._select_node(
                ResourceQuantity(vcpu=1, memory_mb=512, disk_mb=1024),
                image="registry.test/team/b@sha256:" + "b" * 64,
            )
            same = handler._select_node(
                ResourceQuantity(vcpu=1, memory_mb=512, disk_mb=1024),
                image="registry.test/team/a@sha256:" + "a" * 64,
            )

        self.assertIsNotNone(distinct)
        self.assertEqual(distinct.node_id, "node-2")
        self.assertIsNotNone(same)
        self.assertEqual(same.node_id, "node-1")

    def test_cached_image_spills_when_create_pipeline_is_saturated(self) -> None:
        image = "registry.test/team/a@sha256:" + "a" * 64
        with TemporaryDirectory() as raw_dir:
            handler = object.__new__(control_plane.ControlPlaneHandler)
            handler.routing_store = RoutingStore(Path(raw_dir) / "routes.sqlite")
            handler.registry_layer_cache = None
            base = NodeHeartbeat(
                node_id="cached-node",
                job_id="cached-job",
                updated_at=utc_now(),
                active_sandboxes=0,
                node_url="http://cached-node:8090",
                agent_version=package_version(),
                capabilities=("sandbox", "image-cache", "disk-quota"),
                total_resources=ResourceQuantity(
                    vcpu=32,
                    memory_mb=98304,
                    disk_mb=100_000,
                ),
                cached_images=(image,),
                cached_images_known=True,
            )
            candidates = [
                base,
                replace(
                    base,
                    node_id="spill-node",
                    job_id="spill-job",
                    node_url="http://spill-node:8090",
                    cached_images=(),
                ),
            ]
            handler._ready_sandbox_heartbeats = lambda: candidates
            handler._nodes_with_image = lambda *_args, **_kwargs: {"cached-node"}
            for index in range(control_plane.CREATE_PIPELINE_TARGET_PER_NODE):
                handler.routing_store.upsert_sandbox(
                    SandboxRoute(
                        sandbox_id=f"creating-{index}",
                        node_id="cached-node",
                        job_id="cached-job",
                        node_url="http://cached-node:8090",
                        resources=ResourceQuantity(
                            vcpu=1,
                            memory_mb=512,
                            disk_mb=1024,
                        ),
                        spec={"image": image},
                        state="creating",
                    )
                )

            selected = handler._select_node(
                ResourceQuantity(vcpu=1, memory_mb=512, disk_mb=1024),
                image=image,
            )

        self.assertIsNotNone(selected)
        self.assertEqual(selected.node_id, "spill-node")

    def test_cold_image_placement_prefers_shared_cached_layers(self) -> None:
        target = "registry.test/team/target@sha256:" + "b" * 64
        cached = "registry.test/team/cached@sha256:" + "a" * 64
        shared_layer = "sha256:" + "1" * 64
        target_layer = "sha256:" + "2" * 64
        cached_layer = "sha256:" + "3" * 64
        manifests = {
            target: RegistryManifestLayers(
                repository="team/target",
                manifest_digest="sha256:" + "b" * 64,
                layers=(
                    RegistryLayerDescriptor(shared_layer, 1024 * 1024 * 1024),
                    RegistryLayerDescriptor(target_layer, 10 * 1024 * 1024),
                ),
            ),
            cached: RegistryManifestLayers(
                repository="team/cached",
                manifest_digest="sha256:" + "a" * 64,
                layers=(
                    RegistryLayerDescriptor(shared_layer, 1024 * 1024 * 1024),
                    RegistryLayerDescriptor(cached_layer, 5 * 1024 * 1024),
                ),
            ),
        }

        class FakeLayerCache:
            def get(self, image: str, *, load: bool = False):
                del load
                return manifests.get(image)

        with TemporaryDirectory() as raw_dir:
            handler = object.__new__(control_plane.ControlPlaneHandler)
            handler.routing_store = RoutingStore(Path(raw_dir) / "routes.sqlite")
            handler.registry_layer_cache = FakeLayerCache()
            base = NodeHeartbeat(
                node_id="layer-node",
                job_id="job-layer",
                updated_at=utc_now(),
                active_sandboxes=0,
                node_url="http://layer-node:8090",
                agent_version=package_version(),
                capabilities=("sandbox", "image-cache", "disk-quota"),
                total_resources=ResourceQuantity(
                    vcpu=4,
                    memory_mb=8192,
                    disk_mb=100_000,
                ),
                cached_images=(cached,),
                cached_images_known=True,
            )
            candidates = [
                base,
                replace(
                    base,
                    node_id="packed-node",
                    job_id="job-packed",
                    node_url="http://packed-node:8090",
                    cached_images=(),
                ),
            ]
            handler._ready_sandbox_heartbeats = lambda: candidates
            handler._nodes_with_image = lambda *_args, **_kwargs: set()
            handler.routing_store.upsert_sandbox(
                SandboxRoute(
                    sandbox_id="already-running",
                    node_id="packed-node",
                    job_id="job-packed",
                    node_url="http://packed-node:8090",
                    resources=ResourceQuantity(
                        vcpu=1,
                        memory_mb=512,
                        disk_mb=1024,
                    ),
                    spec={"image": "busybox:latest"},
                    state="running",
                )
            )

            selected = handler._select_node(
                ResourceQuantity(vcpu=1, memory_mb=512, disk_mb=1024),
                image=target,
            )

        self.assertIsNotNone(selected)
        self.assertEqual(selected.node_id, "layer-node")

    def test_registry_layer_metadata_cache_loads_immutable_manifest_once(self) -> None:
        digest = "sha256:" + "a" * 64
        image = f"registry.test:5000/team/image:v1@{digest}"
        manifest = RegistryManifestLayers(
            repository="team/image",
            manifest_digest=digest,
            layers=(
                RegistryLayerDescriptor("sha256:" + "1" * 64, 123),
            ),
        )
        cache = control_plane.RegistryLayerMetadataCache(
            "http://registry.test:5000"
        )
        started = Event()
        release = Event()
        results: list[RegistryManifestLayers | None] = []

        def load_manifest(_repository: str, _digest: str) -> RegistryManifestLayers:
            started.set()
            release.wait(1)
            return manifest

        with patch.object(
            control_plane.RegistryClient,
            "manifest_layers",
            side_effect=load_manifest,
        ) as load:
            first_thread = Thread(
                target=lambda: results.append(cache.get(image, load=True))
            )
            second_thread = Thread(
                target=lambda: results.append(cache.get(image, load=True))
            )
            first_thread.start()
            self.assertTrue(started.wait(1))
            second_thread.start()
            sleep(0.01)
            release.set()
            first_thread.join()
            second_thread.join()
            cached = cache.get(image, load=True)

        self.assertEqual(results, [manifest, manifest])
        self.assertEqual(cached, manifest)
        load.assert_called_once_with("team/image", digest)

    def test_create_hydrates_layer_metadata_off_the_critical_path(self) -> None:
        class RecordingLayerCache:
            def __init__(self) -> None:
                self.loads: list[bool] = []
                self.hydrated: list[tuple[str, ...]] = []

            def get(self, _image: str, *, load: bool = False) -> None:
                self.loads.append(load)
                return None

            def hydrate_async(self, images: tuple[str, ...]) -> None:
                self.hydrated.append(images)

        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            gateway = build_server(
                "127.0.0.1",
                0,
                raw_path / "heartbeats.json",
                routing_file=raw_path / "routes.sqlite",
            )
            cache = RecordingLayerCache()
            gateway.RequestHandlerClass.registry_layer_cache = (  # type: ignore[attr-defined]
                cache
            )
            Thread(target=gateway.serve_forever, daemon=True).start()
            try:
                host, port = gateway.server_address
                created = self._json_request(
                    f"http://{host}:{port}/v1/sandboxes",
                    method="POST",
                    payload={
                        "id": "nonblocking-layers",
                        "image": "registry.example/repo:latest",
                        "cpus": 1,
                        "memory_mb": 512,
                        "disk_mb": 1024,
                    },
                    allow_error=True,
                )
            finally:
                gateway.shutdown()
                gateway.server_close()

        self.assertEqual(created["status"], 503)
        self.assertEqual(cache.loads, [False])
        self.assertEqual(
            cache.hydrated,
            [("registry.example/repo:latest",)],
        )

    def test_metrics_include_registry_summary_when_configured(self) -> None:
        class RegistryHandler(BaseHTTPRequestHandler):
            calls: list[str] = []

            def do_GET(self) -> None:
                path = self.path.split("?", 1)[0]
                type(self).calls.append(path)
                if path == "/v2/_catalog":
                    self._write_json({"repositories": ["prime/base", "prime/mini-swe"]})
                    return
                if path == "/v2/prime/base/tags/list":
                    self._write_json({"name": "prime/base", "tags": ["py311"]})
                    return
                if path == "/v2/prime/mini-swe/tags/list":
                    self._write_json(
                        {"name": "prime/mini-swe", "tags": ["mswe-2.2.8", "latest"]}
                    )
                    return
                self.send_response(404)
                self.end_headers()

            def log_message(self, format: str, *args: object) -> None:
                del format, args

            def _write_json(self, payload: dict[str, object]) -> None:
                body = json.dumps(payload).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        with TemporaryDirectory() as raw_dir:
            registry = ThreadingHTTPServer(("127.0.0.1", 0), RegistryHandler)
            registry_thread = Thread(target=registry.serve_forever, daemon=True)
            registry_thread.start()
            try:
                registry_host, registry_port = registry.server_address
                gateway = build_server(
                    "127.0.0.1",
                    0,
                    Path(raw_dir) / "heartbeats.json",
                    metrics_file=Path(raw_dir) / "metrics.jsonl",
                    registry_url=f"http://{registry_host}:{registry_port}",
                )
                gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
                gateway_thread.start()
                try:
                    host, port = gateway.server_address
                    metrics = self._json_request(f"http://{host}:{port}/v1/metrics")
                    cached_metrics = self._json_request(
                        f"http://{host}:{port}/v1/metrics"
                    )
                    calls_after_cached_metrics = list(RegistryHandler.calls)
                    refreshed_metrics = self._json_request(
                        f"http://{host}:{port}/v1/metrics?refresh_registry=true"
                    )
                    direct = self._json_request(f"http://{host}:{port}/v1/registry")
                finally:
                    gateway.shutdown()
                    gateway.server_close()
            finally:
                registry.shutdown()
                registry.server_close()

        self.assertTrue(metrics["registry"]["ok"])
        self.assertEqual(metrics["registry"]["repository_count"], 2)
        self.assertEqual(metrics["registry"]["scanned_tag_count"], 3)
        self.assertEqual(metrics["registry"]["visible_tag_count"], 3)
        self.assertEqual(
            metrics["registry"]["repositories"][1]["visible_tag_count"],
            2,
        )
        self.assertEqual(
            direct["registry"]["repositories"][1]["repository"],
            "prime/mini-swe",
        )
        self.assertFalse(metrics["registry"]["cached"])
        self.assertTrue(cached_metrics["registry"]["cached"])
        self.assertFalse(refreshed_metrics["registry"]["cached"])
        self.assertEqual(cached_metrics["registry"]["repository_count"], 2)
        self.assertEqual(calls_after_cached_metrics.count("/v2/_catalog"), 1)
        self.assertEqual(RegistryHandler.calls.count("/v2/_catalog"), 3)

    def test_gateway_hides_stale_private_registry_image_records(self) -> None:
        class MissingManifestRegistryHandler(BaseHTTPRequestHandler):
            def do_HEAD(self) -> None:
                if self.path.startswith("/v2/prime-rl/missing/manifests/latest"):
                    self.send_response(404)
                    self.end_headers()
                    return
                self.send_response(200)
                self.send_header("Docker-Content-Digest", "sha256:ok")
                self.end_headers()

            def log_message(self, format: str, *args: object) -> None:
                del format, args

        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            image_file = raw_path / "images.json"
            now = utc_now()
            ImageStore(image_file).upsert(
                ImageRecord(
                    id="missing",
                    tag="ucloud-sandbox-registry:5000/prime-rl/missing:latest",
                    source="build:/tmp/missing",
                    state="available",
                    created_at=now,
                    updated_at=now,
                    pushed=True,
                )
            )
            registry = ThreadingHTTPServer(
                ("127.0.0.1", 0),
                MissingManifestRegistryHandler,
            )
            registry_thread = Thread(target=registry.serve_forever, daemon=True)
            registry_thread.start()
            try:
                registry_host, registry_port = registry.server_address
                gateway = build_server(
                    "127.0.0.1",
                    0,
                    raw_path / "heartbeats.json",
                    routing_file=raw_path / "routes.sqlite",
                    image_file=image_file,
                    image_runtime=DockerImageRuntime(dry_run=True),
                    registry_url=f"http://{registry_host}:{registry_port}",
                )
                gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
                gateway_thread.start()
                try:
                    host, port = gateway.server_address
                    images = self._json_request(f"http://{host}:{port}/v1/images")
                finally:
                    gateway.shutdown()
                    gateway.server_close()
            finally:
                registry.shutdown()
                registry.server_close()

            self.assertEqual(images["images"], [])
            self.assertEqual(ImageStore(image_file).load(), {})

    def test_metrics_do_not_fan_out_to_node_build_endpoints(self) -> None:
        class BuildProbeHandler(BaseHTTPRequestHandler):
            called = Event()

            def do_GET(self) -> None:
                if self.path.split("?", 1)[0] == "/v1/images/builds":
                    self.called.set()
                    self._write_json({"builds": []})
                    return
                self.send_response(404)
                self.end_headers()

            def log_message(self, format: str, *args: object) -> None:
                del format, args

            def _write_json(self, payload: dict[str, object]) -> None:
                body = json.dumps(payload).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            node = ThreadingHTTPServer(("127.0.0.1", 0), BuildProbeHandler)
            node_thread = Thread(target=node.serve_forever, daemon=True)
            node_thread.start()
            try:
                node_host, node_port = node.server_address
                gateway = build_server(
                    "127.0.0.1",
                    0,
                    raw_path / "heartbeats.json",
                    routing_file=raw_path / "routes.sqlite",
                    metrics_file=raw_path / "metrics.jsonl",
                )
                gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
                gateway_thread.start()
                try:
                    host, port = gateway.server_address
                    result = post_heartbeat(
                        f"http://{host}:{port}/v1/nodes/heartbeat",
                        build_heartbeat(
                            job_id="job-builder",
                            node_id="builder-1",
                            node_url=f"http://{node_host}:{node_port}",
                            active_image_builds=1,
                            capabilities=("image-cache", "image-build", "snapshot"),
                        ),
                    )
                    self.assertEqual(result.status, 200)
                    metrics = self._json_request(f"http://{host}:{port}/v1/metrics")
                finally:
                    gateway.shutdown()
                    gateway.server_close()
            finally:
                node.shutdown()
                node.server_close()

        self.assertFalse(BuildProbeHandler.called.is_set())
        self.assertEqual(metrics["images"]["active_builds"], 1)

    def test_gateway_mode_proxies_node_agent_json_api(self) -> None:
        with TemporaryDirectory() as raw_dir:
            node_sandbox_file = Path(raw_dir) / "node-sandboxes.json"
            node_image_file = Path(raw_dir) / "node-images.json"
            node = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=node_sandbox_file,
                image_file=node_image_file,
                job_id="job-1",
                node_id="node-1",
                total_resources=ResourceQuantity(
                    vcpu=4, memory_mb=8192, disk_mb=100_000
                ),
                runtime=DockerGvisorRuntime(
                    dry_run=True,
                    allow_storage_opt_quota=True,
                ),
            )
            node_thread = Thread(target=node.serve_forever, daemon=True)
            node_thread.start()
            try:
                node_host, node_port = node.server_address
                gateway = build_server(
                    "127.0.0.1",
                    0,
                    Path(raw_dir) / "heartbeats.json",
                    upstream_node_url=f"http://{node_host}:{node_port}",
                )
                gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
                gateway_thread.start()
                try:
                    host, port = gateway.server_address
                    base = f"http://{host}:{port}"
                    created = self._json_request(
                        f"{base}/v1/sandboxes",
                        method="POST",
                        payload={
                            "id": "proxied-one",
                            "image": "busybox",
                            "disk_mb": 64,
                        },
                    )
                    listed = self._json_request(f"{base}/v1/sandboxes")
                    deleted = self._json_request(
                        f"{base}/v1/sandboxes/proxied-one",
                        method="DELETE",
                    )
                finally:
                    gateway.shutdown()
                    gateway.server_close()
            finally:
                node.shutdown()
                node.server_close()

            self.assertEqual(created["sandbox"]["spec"]["id"], "proxied-one")
            self.assertEqual(listed["sandboxes"][0]["spec"]["id"], "proxied-one")
            self.assertEqual(deleted["deleted"]["spec"]["id"], "proxied-one")

    def test_gateway_proxy_returns_json_bad_gateway_when_node_disconnects(self) -> None:
        class DisconnectingHandler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                self.connection.close()

            def log_message(self, format: str, *args: object) -> None:
                del format, args

        with TemporaryDirectory() as raw_dir:
            node = ThreadingHTTPServer(("127.0.0.1", 0), DisconnectingHandler)
            node_thread = Thread(target=node.serve_forever, daemon=True)
            node_thread.start()
            try:
                node_host, node_port = node.server_address
                gateway = build_server(
                    "127.0.0.1",
                    0,
                    Path(raw_dir) / "heartbeats.json",
                    upstream_node_url=f"http://{node_host}:{node_port}",
                )
                gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
                gateway_thread.start()
                try:
                    host, port = gateway.server_address
                    response = self._json_request(
                        f"http://{host}:{port}/v1/sandboxes",
                        method="POST",
                        payload={
                            "id": "disconnect-one",
                            "image": "busybox",
                            "disk_mb": 64,
                        },
                        allow_error=True,
                    )
                finally:
                    gateway.shutdown()
                    gateway.server_close()
            finally:
                node.shutdown()
                node.server_close()

        self.assertEqual(response["status"], 502)
        self.assertIn("node request failed", response["body"]["error"])

    def test_gateway_lists_sandboxes_from_cache_unless_refresh_requested(
        self,
    ) -> None:
        class ListingNode(BaseHTTPRequestHandler):
            listed = Event()

            def do_GET(self) -> None:
                if self.path.split("?", 1)[0] == "/v1/sandboxes":
                    self.listed.set()
                    self._write_json(
                        {
                            "sandboxes": [
                                {
                                    "spec": {
                                        "id": "cached-one",
                                        "image": "busybox",
                                        "labels": {"run": "r1"},
                                        "memory_mb": 512,
                                        "disk_mb": 1024,
                                    },
                                    "state": "running",
                                }
                            ]
                        }
                    )
                    return
                self.send_response(404)
                self.end_headers()

            def log_message(self, format: str, *args: object) -> None:
                del format, args

            def _write_json(self, payload: dict[str, object]) -> None:
                body = json.dumps(payload).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            route_file = raw_path / "routes.sqlite"
            node = ThreadingHTTPServer(("127.0.0.1", 0), ListingNode)
            node_thread = Thread(target=node.serve_forever, daemon=True)
            node_thread.start()
            try:
                node_host, node_port = node.server_address
                node_url = f"http://{node_host}:{node_port}"
                RoutingStore(route_file).upsert_sandbox(
                    SandboxRoute(
                        sandbox_id="cached-one",
                        node_id="node-1",
                        job_id="job-1",
                        node_url=node_url,
                        resources=ResourceQuantity(
                            vcpu=1.0,
                            memory_mb=512,
                            disk_mb=1024,
                        ),
                        spec={
                            "id": "cached-one",
                            "image": "busybox",
                            "labels": {"run": "r1"},
                            "memory_mb": 512,
                            "disk_mb": 1024,
                        },
                        state="running",
                    )
                )
                gateway = build_server(
                    "127.0.0.1",
                    0,
                    raw_path / "heartbeats.json",
                    routing_file=route_file,
                )
                gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
                gateway_thread.start()
                try:
                    host, port = gateway.server_address
                    base = f"http://{host}:{port}"
                    result = post_heartbeat(
                        f"{base}/v1/nodes/heartbeat",
                        build_heartbeat(
                            job_id="job-1",
                            node_id="node-1",
                            node_url=node_url,
                            active_sandboxes=1,
                            capabilities=("sandbox", "image-cache", "disk-quota"),
                            total_resources=ResourceQuantity(
                                vcpu=4,
                                memory_mb=4096,
                                disk_mb=8192,
                            ),
                        ),
                    )
                    self.assertEqual(result.status, 200)
                    cached = self._json_request(f"{base}/v1/sandboxes")
                    self.assertFalse(ListingNode.listed.is_set())
                    refreshed = self._json_request(f"{base}/v1/sandboxes?refresh=true")
                finally:
                    gateway.shutdown()
                    gateway.server_close()
            finally:
                node.shutdown()
                node.server_close()

        self.assertTrue(cached["cached"])
        self.assertEqual(cached["sandboxes"][0]["id"], "cached-one")
        self.assertEqual(cached["sandboxes"][0]["state"], "running")
        self.assertEqual(cached["sandboxes"][0]["image"], "busybox")
        self.assertEqual(cached["sandboxes"][0]["labels"], {"run": "r1"})
        self.assertFalse(cached["sandboxes"][0]["route_only"])
        self.assertTrue(ListingNode.listed.is_set())
        self.assertFalse(refreshed["cached"])
        self.assertEqual(refreshed["sandboxes"][0]["spec"]["id"], "cached-one")

    def test_gateway_marks_cached_route_unknown_when_node_reports_empty(
        self,
    ) -> None:
        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            route_file = raw_path / "routes.sqlite"
            node_url = "http://127.0.0.1:9"
            RoutingStore(route_file).upsert_sandbox(
                SandboxRoute(
                    sandbox_id="stale-one",
                    node_id="node-1",
                    job_id="job-1",
                    node_url=node_url,
                    resources=ResourceQuantity(
                        vcpu=1.0,
                        memory_mb=512,
                        disk_mb=1024,
                    ),
                    spec={
                        "id": "stale-one",
                        "image": "busybox",
                        "memory_mb": 512,
                        "disk_mb": 1024,
                    },
                    state="running",
                )
            )
            gateway = build_server(
                "127.0.0.1",
                0,
                raw_path / "heartbeats.json",
                routing_file=route_file,
            )
            gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
            gateway_thread.start()
            try:
                host, port = gateway.server_address
                base = f"http://{host}:{port}"
                result = post_heartbeat(
                    f"{base}/v1/nodes/heartbeat",
                    build_heartbeat(
                        job_id="job-1",
                        node_id="node-1",
                        node_url=node_url,
                        active_sandboxes=0,
                        capabilities=("sandbox", "disk-quota"),
                    ),
                )
                self.assertEqual(result.status, 200)
                cached = self._json_request(f"{base}/v1/sandboxes")
            finally:
                gateway.shutdown()
                gateway.server_close()

        record = cached["sandboxes"][0]
        self.assertTrue(cached["cached"])
        self.assertEqual(record["id"], "stale-one")
        self.assertEqual(record["cached_state"], "running")
        self.assertEqual(record["state"], "unknown")
        self.assertTrue(record["route_only"])
        self.assertEqual(record["node"]["active_sandboxes"], 0)

    def test_gateway_keeps_parked_route_visible_from_complete_inventory(
        self,
    ) -> None:
        route = SandboxRoute(
            sandbox_id="parked-one",
            node_id="node-1",
            job_id="job-1",
            node_url="http://node-1:8090",
            resources=ResourceQuantity(vcpu=1, memory_mb=512, disk_mb=1024),
            spec={"id": "parked-one", "image": "busybox"},
            state="parked",
        )
        heartbeat = build_heartbeat(
            job_id="job-1",
            node_id="node-1",
            node_url=route.node_url,
            active_sandboxes=0,
            inventory=(
                SandboxInventoryEntry(
                    sandbox_id=route.sandbox_id,
                    state="parked",
                ),
            ),
            inventory_complete=True,
        )

        record = control_plane._route_only_sandbox_record(
            route,
            heartbeat,
        )

        self.assertEqual(record["state"], "parked")
        self.assertEqual(record["cached_state"], "parked")

    def test_gateway_does_not_proxy_exec_to_proven_stale_route(self) -> None:
        class ListingNode(BaseHTTPRequestHandler):
            exec_called = Event()

            def do_GET(self) -> None:
                if self.path.split("?", 1)[0] == "/v1/sandboxes":
                    self._write_json({"sandboxes": []})
                    return
                self.send_response(404)
                self.end_headers()

            def do_POST(self) -> None:
                if self.path.split("?", 1)[0].endswith("/exec"):
                    self.exec_called.set()
                self.send_response(500)
                self.end_headers()

            def log_message(self, format: str, *args: object) -> None:
                del format, args

            def _write_json(self, payload: dict[str, object]) -> None:
                body = json.dumps(payload).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            route_file = raw_path / "routes.sqlite"
            node = ThreadingHTTPServer(("127.0.0.1", 0), ListingNode)
            node_thread = Thread(target=node.serve_forever, daemon=True)
            node_thread.start()
            try:
                node_host, node_port = node.server_address
                node_url = f"http://{node_host}:{node_port}"
                RoutingStore(route_file).upsert_sandbox(
                    SandboxRoute(
                        sandbox_id="stale-one",
                        node_id="node-1",
                        job_id="job-1",
                        node_url=node_url,
                        resources=ResourceQuantity(
                            vcpu=1.0,
                            memory_mb=512,
                            disk_mb=1024,
                        ),
                        spec={"id": "stale-one", "image": "busybox"},
                        state="running",
                    )
                )
                gateway = build_server(
                    "127.0.0.1",
                    0,
                    raw_path / "heartbeats.json",
                    routing_file=route_file,
                )
                gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
                gateway_thread.start()
                try:
                    host, port = gateway.server_address
                    base = f"http://{host}:{port}"
                    result = post_heartbeat(
                        f"{base}/v1/nodes/heartbeat",
                        build_heartbeat(
                            job_id="job-1",
                            node_id="node-1",
                            node_url=node_url,
                            active_sandboxes=0,
                            capabilities=("sandbox", "disk-quota"),
                            node_epoch="epoch-1",
                            activity_epoch=1,
                            inventory=(),
                            inventory_complete=True,
                        ),
                    )
                    self.assertEqual(result.status, 200)
                    response = self._json_request(
                        f"{base}/v1/sandboxes/stale-one/exec",
                        method="POST",
                        payload={"cmd": "true"},
                        allow_error=True,
                    )
                finally:
                    gateway.shutdown()
                    gateway.server_close()
            finally:
                node.shutdown()
                node.server_close()

        self.assertEqual(response["status"], 404)
        self.assertEqual(response["body"]["error"], "sandbox route not found")
        self.assertFalse(ListingNode.exec_called.is_set())

    def test_gateway_returns_json_when_routing_store_unavailable_for_metrics(
        self,
    ) -> None:
        class BrokenRoutingStore:
            def load(self) -> object:
                raise sqlite3.DatabaseError("database disk image is malformed")

        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            gateway = build_server(
                "127.0.0.1",
                0,
                raw_path / "heartbeats.json",
                routing_file=raw_path / "routes.sqlite",
            )
            gateway.RequestHandlerClass.routing_store = BrokenRoutingStore()
            gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
            gateway_thread.start()
            try:
                host, port = gateway.server_address
                response = self._json_request(
                    f"http://{host}:{port}/v1/metrics",
                    allow_error=True,
                )
            finally:
                gateway.shutdown()
                gateway.server_close()

        self.assertEqual(response["status"], 503)
        self.assertEqual(response["body"]["error"], "routing state unavailable")
        self.assertTrue(response["body"]["retryable"])
        self.assertIn("malformed", response["body"]["details"])

    def test_gateway_bearer_token_protects_proxied_api(self) -> None:
        with TemporaryDirectory() as raw_dir:
            node = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=Path(raw_dir) / "node-sandboxes.json",
                image_file=Path(raw_dir) / "node-images.json",
                job_id="job-1",
                node_id="node-1",
                total_resources=ResourceQuantity(
                    vcpu=4, memory_mb=8192, disk_mb=100_000
                ),
                runtime=DockerGvisorRuntime(
                    dry_run=True,
                    allow_storage_opt_quota=True,
                ),
            )
            node_thread = Thread(target=node.serve_forever, daemon=True)
            node_thread.start()
            try:
                node_host, node_port = node.server_address
                gateway = build_server(
                    "127.0.0.1",
                    0,
                    Path(raw_dir) / "heartbeats.json",
                    upstream_node_url=f"http://{node_host}:{node_port}",
                    gateway_bearer_token="secret-token",
                )
                gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
                gateway_thread.start()
                try:
                    host, port = gateway.server_address
                    base = f"http://{host}:{port}"
                    healthz = self._json_request(f"{base}/healthz")
                    unauthorized = self._json_request(
                        f"{base}/v1/sandboxes",
                        allow_error=True,
                    )
                    authorized = self._json_request(
                        f"{base}/v1/sandboxes",
                        headers={"Authorization": "Bearer secret-token"},
                    )
                    header_authorized = self._json_request(
                        f"{base}/v1/sandboxes",
                        headers={"X-UCloud-Sandbox-Token": "secret-token"},
                    )
                finally:
                    gateway.shutdown()
                    gateway.server_close()
            finally:
                node.shutdown()
                node.server_close()

            self.assertEqual(
                healthz,
                {
                    "ok": True,
                    "service": "control-plane",
                    "version": package_version(),
                },
            )
            self.assertEqual(unauthorized["status"], 401)
            self.assertEqual(unauthorized["body"], {"error": "unauthorized"})
            self.assertEqual(authorized, {"sandboxes": []})
            self.assertEqual(header_authorized, {"sandboxes": []})

    def test_health_reports_unavailable_registry_usage_state(self) -> None:
        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            usage_path = raw_path / "registry-usage.json"
            usage_path.mkdir()
            gateway = build_server(
                "127.0.0.1",
                0,
                raw_path / "heartbeats.json",
                registry_usage_file=usage_path,
            )
            Thread(target=gateway.serve_forever, daemon=True).start()
            try:
                host, port = gateway.server_address
                health = self._json_request(
                    f"http://{host}:{port}/healthz",
                    allow_error=True,
                )
            finally:
                gateway.shutdown()
                gateway.server_close()

        self.assertEqual(health["status"], 503)
        self.assertFalse(health["body"]["ok"])
        self.assertEqual(
            health["body"]["registry_usage"],
            {"ok": False, "error": "state file is unavailable"},
        )

    def test_distinct_gateway_and_heartbeat_tokens_are_channel_scoped(self) -> None:
        with TemporaryDirectory() as raw_dir:
            gateway = build_server(
                "127.0.0.1",
                0,
                Path(raw_dir) / "heartbeats.json",
                gateway_bearer_token="gateway-secret",
                heartbeat_bearer_token="heartbeat-secret",
            )
            gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
            gateway_thread.start()
            try:
                host, port = gateway.server_address
                base = f"http://{host}:{port}"
                heartbeat = build_heartbeat(job_id="job-1", node_id="node-1")
                no_token = post_heartbeat(
                    f"{base}/v1/nodes/heartbeat",
                    heartbeat,
                )
                gateway_token_on_heartbeat = post_heartbeat_with_headers(
                    f"{base}/v1/nodes/heartbeat",
                    heartbeat,
                    {"Authorization": "Bearer gateway-secret"},
                )
                public_header_on_heartbeat = post_heartbeat_with_headers(
                    f"{base}/v1/nodes/heartbeat",
                    heartbeat,
                    {"X-UCloud-Sandbox-Token": "heartbeat-secret"},
                )
                accepted_heartbeat = post_heartbeat_with_headers(
                    f"{base}/v1/nodes/heartbeat",
                    heartbeat,
                    {"Authorization": "Bearer heartbeat-secret"},
                )
                heartbeat_token_on_gateway = self._json_request(
                    f"{base}/v1/nodes",
                    headers={"Authorization": "Bearer heartbeat-secret"},
                    allow_error=True,
                )
                gateway_token_on_gateway = self._json_request(
                    f"{base}/v1/nodes",
                    headers={"Authorization": "Bearer gateway-secret"},
                )
            finally:
                gateway.shutdown()
                gateway.server_close()

        self.assertEqual(no_token.status, 401)
        self.assertEqual(gateway_token_on_heartbeat.status, 401)
        self.assertEqual(public_header_on_heartbeat.status, 401)
        self.assertEqual(accepted_heartbeat.status, 200)
        self.assertEqual(heartbeat_token_on_gateway["status"], 401)
        self.assertEqual(len(gateway_token_on_gateway["nodes"]), 1)

    def test_heartbeat_auth_falls_back_to_gateway_token_only_when_omitted(
        self,
    ) -> None:
        with TemporaryDirectory() as raw_dir:
            gateway = build_server(
                "127.0.0.1",
                0,
                Path(raw_dir) / "heartbeats.json",
                gateway_bearer_token="legacy-secret",
            )
            gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
            gateway_thread.start()
            try:
                host, port = gateway.server_address
                heartbeat_url = f"http://{host}:{port}/v1/nodes/heartbeat"
                heartbeat = build_heartbeat(job_id="job-1", node_id="node-1")
                no_token = post_heartbeat(heartbeat_url, heartbeat)
                bearer = post_heartbeat_with_headers(
                    heartbeat_url,
                    heartbeat,
                    {"Authorization": "Bearer legacy-secret"},
                )
                public_link_header = post_heartbeat_with_headers(
                    heartbeat_url,
                    heartbeat,
                    {"X-UCloud-Sandbox-Token": "legacy-secret"},
                )
            finally:
                gateway.shutdown()
                gateway.server_close()

        self.assertEqual(no_token.status, 401)
        self.assertEqual(bearer.status, 200)
        self.assertEqual(public_link_header.status, 200)

    def test_authenticated_malformed_heartbeat_returns_bad_request(self) -> None:
        with TemporaryDirectory() as raw_dir:
            gateway = build_server(
                "127.0.0.1",
                0,
                Path(raw_dir) / "heartbeats.json",
                gateway_bearer_token="gateway-secret",
                heartbeat_bearer_token="heartbeat-secret",
            )
            gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
            gateway_thread.start()
            try:
                host, port = gateway.server_address
                response = self._json_request(
                    f"http://{host}:{port}/v1/nodes/heartbeat",
                    method="POST",
                    headers={"Authorization": "Bearer heartbeat-secret"},
                    payload={
                        "node_id": "node-1",
                        "job_id": "job-1",
                        "updated_at": utc_now().isoformat(),
                        "active_sandboxes": "not-an-integer",
                        "runtime_metrics": {
                            "collected_at": utc_now().isoformat(),
                            "cpu_count": "not-an-integer",
                        },
                    },
                    allow_error=True,
                )
            finally:
                gateway.shutdown()
                gateway.server_close()

        self.assertEqual(response["status"], 400)
        self.assertEqual(response["body"], {"error": "invalid heartbeat payload"})

    def test_dashboard_assets_are_public_but_metrics_remain_protected(self) -> None:
        with TemporaryDirectory() as raw_dir:
            gateway = build_server(
                "127.0.0.1",
                0,
                Path(raw_dir) / "heartbeats.json",
                routing_file=Path(raw_dir) / "routes.sqlite",
                gateway_bearer_token="secret-token",
                metrics_file=Path(raw_dir) / "metrics.jsonl",
            )
            gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
            gateway_thread.start()
            try:
                host, port = gateway.server_address
                base = f"http://{host}:{port}"
                with request.urlopen(f"{base}/dashboard", timeout=5) as response:
                    html = response.read().decode("utf-8")
                    html_type = response.headers.get("Content-Type")
                with request.urlopen(
                    f"{base}/dashboard/dashboard.css",
                    timeout=5,
                ) as response:
                    css = response.read().decode("utf-8")
                    css_type = response.headers.get("Content-Type")
                with request.urlopen(
                    f"{base}/dashboard/dashboard.js",
                    timeout=5,
                ) as response:
                    js = response.read().decode("utf-8")
                    js_type = response.headers.get("Content-Type")
                unauthorized_metrics = self._json_request(
                    f"{base}/v1/metrics",
                    allow_error=True,
                )
                authorized_metrics = self._json_request(
                    f"{base}/v1/metrics",
                    headers={"Authorization": "Bearer secret-token"},
                )
            finally:
                gateway.shutdown()
                gateway.server_close()

        self.assertIn("text/html", html_type or "")
        self.assertIn("UCloud Sandboxes", html)
        self.assertIn("/dashboard/dashboard.js", html)
        self.assertIn('data-page-target="scheduler"', html)
        self.assertIn('data-page-target="nodes"', html)
        self.assertIn('data-page-target="sandboxes"', html)
        self.assertIn('data-page-target="registry"', html)
        self.assertIn('id="sandboxStateFilter"', html)
        self.assertNotIn('id="terminateAllSandboxesButton"', html)
        self.assertNotIn('id="refreshSelect"', html)
        self.assertNotIn('id="pauseButton"', html)
        self.assertIn('id="refreshNowButton"', html)
        self.assertNotIn("secret-token", html)
        self.assertIn("text/css", css_type or "")
        self.assertIn(".metric-grid", css)
        self.assertIn(".sandbox-table", css)
        self.assertIn(".registry-full-grid", css)
        self.assertIn("application/javascript", js_type or "")
        self.assertIn("/v1/metrics", js)
        self.assertIn("/v1/sandboxes?refresh=true", js)
        self.assertIn("terminateAllSandboxes", js)
        self.assertIn("X-UCloud-Sandbox-Token", js)
        self.assertIn("renderRegistryPage", js)
        self.assertIn("const DEFAULT_REFRESH_INTERVAL_MS = 2000;", js)
        self.assertIn("state.metricsRequest.abort()", js)
        self.assertIn("scheduleNextRefresh", js)
        self.assertNotIn("secret-token", js)
        self.assertEqual(unauthorized_metrics["status"], 401)
        self.assertEqual(authorized_metrics["nodes"]["total"], 0)

    def test_exec_route_survives_transient_worker_heartbeat_gap(self) -> None:
        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            route_file = raw_path / "routes.sqlite"
            node = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=raw_path / "node-sandboxes.json",
                image_file=raw_path / "node-images.json",
                job_id="job-1",
                node_id="node-1",
                total_resources=ResourceQuantity(
                    vcpu=2,
                    memory_mb=1024,
                    disk_mb=1024,
                ),
                runtime=DockerGvisorRuntime(
                    dry_run=True,
                    allow_storage_opt_quota=True,
                ),
            )
            Thread(target=node.serve_forever, daemon=True).start()
            try:
                node_host, node_port = node.server_address
                gateway = build_server(
                    "127.0.0.1",
                    0,
                    raw_path / "heartbeats.json",
                    routing_file=route_file,
                )
                Thread(target=gateway.serve_forever, daemon=True).start()
                try:
                    host, port = gateway.server_address
                    base = f"http://{host}:{port}"
                    self.assertEqual(
                        post_heartbeat(
                            f"{base}/v1/nodes/heartbeat",
                            build_heartbeat(
                                job_id="job-1",
                                node_id="node-1",
                                node_url=f"http://{node_host}:{node_port}",
                                active_sandboxes=0,
                                capabilities=(
                                    "sandbox",
                                    "image-cache",
                                    "disk-quota",
                                ),
                                total_resources=ResourceQuantity(
                                    vcpu=2,
                                    memory_mb=1024,
                                    disk_mb=1024,
                                ),
                            ),
                        ).status,
                        200,
                    )
                    self._json_request(
                        f"{base}/v1/sandboxes",
                        method="POST",
                        payload={
                            "id": "heartbeat-gap",
                            "image": "busybox",
                            "memory_mb": 128,
                            "disk_mb": 64,
                        },
                    )
                    started = self._json_request(
                        f"{base}/v1/sandboxes/heartbeat-gap/exec",
                        method="POST",
                        payload={"command": ["true"]},
                    )
                    session_id = started["session"]["id"]
                    persisted = RoutingStore(route_file).get_exec(session_id)
                    gateway.RequestHandlerClass.store.remove(["job-1"])

                    read = self._json_request(f"{base}/v1/exec/{session_id}")
                finally:
                    gateway.shutdown()
                    gateway.server_close()
            finally:
                node.shutdown()
                node.server_close()

        self.assertIsNotNone(persisted)
        self.assertEqual(read["session"]["id"], session_id)

    def test_missing_routable_exec_route_is_retryable(self) -> None:
        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            gateway = build_server(
                "127.0.0.1",
                0,
                raw_path / "heartbeats.json",
                routing_file=raw_path / "routes.sqlite",
            )
            Thread(target=gateway.serve_forever, daemon=True).start()
            try:
                host, port = gateway.server_address
                session_id = new_exec_session_id(
                    "missing-sandbox",
                    node_id="missing-node",
                    job_id="missing-job",
                )
                response = self._json_request(
                    f"http://{host}:{port}/v1/exec/{session_id}",
                    allow_error=True,
                )
            finally:
                gateway.shutdown()
                gateway.server_close()

        self.assertEqual(response["status"], 503)
        self.assertEqual(response["body"]["error"], "exec route not found")
        self.assertTrue(response["body"]["retryable"])

    def test_stale_legacy_exec_cleanup_preserves_sandbox_route(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = RoutingStore(Path(raw_dir) / "routes.sqlite")
            sandbox = SandboxRoute(
                sandbox_id="parked-one",
                node_id="node-1",
                job_id="job-1",
                node_url="http://node-1:8090",
                resources=ResourceQuantity(vcpu=1, memory_mb=512, disk_mb=1024),
                state="parked",
            )
            store.upsert_sandbox(sandbox)
            route = control_plane.ExecRoute(
                session_id="exec-legacy",
                sandbox_id=sandbox.sandbox_id,
                node_id=sandbox.node_id,
                job_id=sandbox.job_id,
                node_url=sandbox.node_url,
            )
            store.upsert_exec(route)
            handler = object.__new__(control_plane.ControlPlaneHandler)
            handler.routing_store = store
            handler._exec_route_is_proven_stale = lambda _route: True
            responses: list[tuple[dict[str, object], int]] = []
            handler._write_json = lambda payload, status=200, **_kwargs: responses.append(
                (payload, int(status))
            )

            handler._route_exec_request(route.session_id, "/v1/exec/exec-legacy")

            self.assertIsNone(store.get_exec(route.session_id))
            self.assertIsNotNone(store.get_sandbox(sandbox.sandbox_id))
            self.assertEqual(responses[-1][1], 404)

    def test_exec_staleness_respects_complete_parked_inventory(self) -> None:
        now = utc_now()
        heartbeat = replace(
            build_heartbeat(
                job_id="job-1",
                node_id="node-1",
                active_sandboxes=0,
                now=now,
            ),
            inventory=(
                SandboxInventoryEntry(
                    sandbox_id="parked-one",
                    state="parked",
                ),
            ),
            inventory_complete=True,
        )
        route = control_plane.ExecRoute(
            session_id="exec-legacy",
            sandbox_id="parked-one",
            node_id="node-1",
            job_id="job-1",
            node_url="http://node-1:8090",
            created_at=(now - timedelta(seconds=30)).isoformat(),
            updated_at=(now - timedelta(seconds=30)).isoformat(),
        )
        handler = object.__new__(control_plane.ControlPlaneHandler)
        handler.heartbeat_ttl_seconds = 120
        handler._heartbeat_for_route = lambda **_kwargs: heartbeat

        self.assertFalse(handler._exec_route_is_proven_stale(route))

    def test_multi_node_gateway_places_and_routes_by_resource_fit(self) -> None:
        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            node1 = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=raw_path / "node1-sandboxes.json",
                image_file=raw_path / "node1-images.json",
                job_id="job-1",
                node_id="node-1",
                total_resources=ResourceQuantity(vcpu=2, memory_mb=1024, disk_mb=32),
                runtime=DockerGvisorRuntime(dry_run=True, allow_storage_opt_quota=True),
            )
            node2 = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=raw_path / "node2-sandboxes.json",
                image_file=raw_path / "node2-images.json",
                job_id="job-2",
                node_id="node-2",
                total_resources=ResourceQuantity(vcpu=2, memory_mb=1024, disk_mb=128),
                runtime=DockerGvisorRuntime(dry_run=True, allow_storage_opt_quota=True),
            )
            node1_thread = Thread(target=node1.serve_forever, daemon=True)
            node2_thread = Thread(target=node2.serve_forever, daemon=True)
            node1_thread.start()
            node2_thread.start()
            try:
                node1_host, node1_port = node1.server_address
                node2_host, node2_port = node2.server_address
                gateway = build_server(
                    "127.0.0.1",
                    0,
                    raw_path / "heartbeats.json",
                    routing_file=raw_path / "routes.json",
                    image_file=raw_path / "gateway-images.json",
                    local_image_builds_enabled=False,
                    metrics_file=raw_path / "metrics.jsonl",
                )
                gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
                gateway_thread.start()
                try:
                    host, port = gateway.server_address
                    base = f"http://{host}:{port}"
                    for heartbeat in (
                        build_heartbeat(
                            job_id="job-1",
                            node_id="node-1",
                            active_sandboxes=100,
                            node_url=f"http://{node1_host}:{node1_port}",
                            capabilities=("sandbox", "image-cache", "disk-quota"),
                            total_resources=ResourceQuantity(
                                vcpu=2,
                                memory_mb=1024,
                                disk_mb=32,
                            ),
                        ),
                        build_heartbeat(
                            job_id="job-2",
                            node_id="node-2",
                            active_sandboxes=0,
                            node_url=f"http://{node2_host}:{node2_port}",
                            capabilities=("sandbox", "image-cache", "disk-quota"),
                            total_resources=ResourceQuantity(
                                vcpu=2,
                                memory_mb=1024,
                                disk_mb=128,
                            ),
                        ),
                    ):
                        result = post_heartbeat(
                            f"{base}/v1/nodes/heartbeat",
                            heartbeat,
                        )
                        self.assertEqual(result.status, 200)

                    created = self._json_request(
                        f"{base}/v1/sandboxes",
                        method="POST",
                        payload={
                            "id": "multi-one",
                            "image": "busybox",
                            "memory_mb": 128,
                            "disk_mb": 64,
                        },
                    )
                    second = self._json_request(
                        f"{base}/v1/sandboxes",
                        method="POST",
                        payload={
                            "id": "multi-two",
                            "image": "busybox",
                            "memory_mb": 128,
                            "disk_mb": 64,
                        },
                    )
                    rejected = self._json_request(
                        f"{base}/v1/sandboxes",
                        method="POST",
                        payload={
                            "id": "multi-three",
                            "image": "busybox",
                            "memory_mb": 128,
                            "disk_mb": 64,
                        },
                        allow_error=True,
                    )
                    listed = self._json_request(f"{base}/v1/sandboxes?refresh=true")
                    exec_started = self._json_request(
                        f"{base}/v1/sandboxes/multi-one/exec",
                        method="POST",
                        payload={"command": ["true"]},
                    )
                    session_id = exec_started["session"]["id"]
                    route_store = RoutingStore(raw_path / "routes.json")
                    original_exec_sandbox_route = route_store.get_sandbox("multi-one")
                    assert original_exec_sandbox_route is not None
                    route_store.delete_sandbox("multi-one")
                    route_store.upsert_sandbox(
                        replace(
                            original_exec_sandbox_route,
                            node_id="node-1",
                            job_id="job-1",
                            node_url=f"http://{node1_host}:{node1_port}",
                        )
                    )
                    exec_read = self._json_request(f"{base}/v1/exec/{session_id}")
                    exec_routes_after_start = route_store.load().exec_sessions
                    route_store.delete_sandbox("multi-one")
                    route_store.upsert_sandbox(original_exec_sandbox_route)
                    deleted = self._json_request(
                        f"{base}/v1/sandboxes/multi-one",
                        method="DELETE",
                    )
                    second_deleted = self._json_request(
                        f"{base}/v1/sandboxes/multi-two",
                        method="DELETE",
                    )
                    metrics = self._json_request(f"{base}/v1/metrics")
                    with request.urlopen(
                        f"http://{node1_host}:{node1_port}/v1/sandboxes",
                        timeout=5,
                    ) as response:
                        node1_payload = json.loads(response.read().decode("utf-8"))
                    with request.urlopen(
                        f"http://{node2_host}:{node2_port}/v1/sandboxes",
                        timeout=5,
                    ) as response:
                        node2_payload = json.loads(response.read().decode("utf-8"))
                finally:
                    gateway.shutdown()
                    gateway.server_close()
            finally:
                node1.shutdown()
                node1.server_close()
                node2.shutdown()
                node2.server_close()

            route = RoutingStore(raw_path / "routes.json").get_sandbox("multi-one")
            self.assertIsNone(route)
            self.assertEqual(created["sandbox"]["spec"]["id"], "multi-one")
            self.assertEqual(second["sandbox"]["spec"]["id"], "multi-two")
            self.assertEqual(rejected["status"], 503)
            self.assertEqual(
                {record["spec"]["id"] for record in listed["sandboxes"]},
                {"multi-one", "multi-two"},
            )
            self.assertEqual(
                {record["node"]["job_id"] for record in listed["sandboxes"]},
                {"job-2"},
            )
            self.assertEqual(exec_read["session"]["id"], session_id)
            self.assertEqual(set(exec_routes_after_start), {session_id})
            persisted_exec_route = exec_routes_after_start[session_id]
            self.assertEqual(persisted_exec_route.sandbox_id, "multi-one")
            self.assertEqual(persisted_exec_route.node_id, "node-2")
            self.assertEqual(persisted_exec_route.job_id, "job-2")
            self.assertEqual(deleted["deleted"]["spec"]["id"], "multi-one")
            self.assertEqual(second_deleted["deleted"]["spec"]["id"], "multi-two")
            self.assertGreaterEqual(metrics["traces"]["span_count"], 3)
            self.assertTrue(
                any(
                    item["name"] == "gateway.sandbox_create"
                    for item in metrics["traces"]["recent"]
                )
            )
            self.assertEqual(node1_payload, {"sandboxes": []})
            self.assertEqual(node2_payload, {"sandboxes": []})

    def test_concurrent_create_reserves_in_flight_node_capacity(self) -> None:
        class SlowCreateRuntime(DockerGvisorRuntime):
            def create(self, spec):
                sleep(0.2)
                return super().create(spec)

        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            node1 = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=raw_path / "burst-node1-sandboxes.json",
                image_file=raw_path / "burst-node1-images.json",
                job_id="job-1",
                node_id="node-1",
                total_resources=ResourceQuantity(vcpu=1, memory_mb=1024, disk_mb=128),
                runtime=SlowCreateRuntime(
                    dry_run=True,
                    allow_storage_opt_quota=True,
                ),
            )
            node2 = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=raw_path / "burst-node2-sandboxes.json",
                image_file=raw_path / "burst-node2-images.json",
                job_id="job-2",
                node_id="node-2",
                total_resources=ResourceQuantity(vcpu=1, memory_mb=1024, disk_mb=128),
                runtime=SlowCreateRuntime(
                    dry_run=True,
                    allow_storage_opt_quota=True,
                ),
            )
            node1_thread = Thread(target=node1.serve_forever, daemon=True)
            node2_thread = Thread(target=node2.serve_forever, daemon=True)
            node1_thread.start()
            node2_thread.start()
            try:
                node1_host, node1_port = node1.server_address
                node2_host, node2_port = node2.server_address
                gateway = build_server(
                    "127.0.0.1",
                    0,
                    raw_path / "heartbeats.json",
                    routing_file=raw_path / "routes.sqlite",
                    image_file=raw_path / "gateway-images.json",
                    local_image_builds_enabled=False,
                    metrics_file=raw_path / "metrics.jsonl",
                )
                gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
                gateway_thread.start()
                try:
                    host, port = gateway.server_address
                    base = f"http://{host}:{port}"
                    for heartbeat in (
                        build_heartbeat(
                            job_id="job-1",
                            node_id="node-1",
                            active_sandboxes=0,
                            node_url=f"http://{node1_host}:{node1_port}",
                            capabilities=("sandbox", "image-cache", "disk-quota"),
                            total_resources=ResourceQuantity(
                                vcpu=1,
                                memory_mb=1024,
                                disk_mb=128,
                            ),
                        ),
                        build_heartbeat(
                            job_id="job-2",
                            node_id="node-2",
                            active_sandboxes=0,
                            node_url=f"http://{node2_host}:{node2_port}",
                            capabilities=("sandbox", "image-cache", "disk-quota"),
                            total_resources=ResourceQuantity(
                                vcpu=1,
                                memory_mb=1024,
                                disk_mb=128,
                            ),
                        ),
                    ):
                        result = post_heartbeat(
                            f"{base}/v1/nodes/heartbeat",
                            heartbeat,
                        )
                        self.assertEqual(result.status, 200)

                    def create(index: int) -> dict:
                        return self._json_request(
                            f"{base}/v1/sandboxes",
                            method="POST",
                            payload={
                                "id": f"burst-{index}",
                                "image": "busybox",
                                "cpus": 1,
                                "memory_mb": 128,
                                "disk_mb": 64,
                            },
                        )

                    with ThreadPoolExecutor(max_workers=2) as executor:
                        created = list(executor.map(create, (1, 2)))
                finally:
                    gateway.shutdown()
                    gateway.server_close()
            finally:
                node1.shutdown()
                node1.server_close()
                node2.shutdown()
                node2.server_close()

            state = RoutingStore(raw_path / "routes.sqlite").load()

        self.assertEqual(
            {item["sandbox"]["spec"]["id"] for item in created},
            {"burst-1", "burst-2"},
        )
        self.assertEqual(
            {state.sandboxes["burst-1"].job_id, state.sandboxes["burst-2"].job_id},
            {"job-1", "job-2"},
        )

    def test_node_capacity_counts_routes_even_after_newer_heartbeat(self) -> None:
        now = utc_now()
        old = (now - timedelta(seconds=5)).isoformat()
        heartbeat = NodeHeartbeat(
            node_id="node-1",
            job_id="job-1",
            updated_at=now,
            active_sandboxes=0,
            node_url="http://node-1:8090",
            total_resources=ResourceQuantity(vcpu=2, memory_mb=2048, disk_mb=4096),
            used_resources=ResourceQuantity(),
        )
        routes = [
            SandboxRoute(
                sandbox_id="already-reserved",
                node_id="node-1",
                job_id="job-1",
                node_url="http://node-1:8090",
                resources=ResourceQuantity(vcpu=2, memory_mb=1024, disk_mb=1024),
                created_at=old,
                updated_at=old,
            )
        ]

        self.assertFalse(
            control_plane._node_can_fit(
                heartbeat,
                ResourceQuantity(vcpu=1, memory_mb=512, disk_mb=512),
                routes,
            )
        )

    def test_gateway_create_backpressure_fails_fast(self) -> None:
        class BlockingCreateRuntime(DockerGvisorRuntime):
            def __init__(self) -> None:
                super().__init__(dry_run=True, allow_storage_opt_quota=True)
                self.started = Event()
                self.release = Event()

            def create(self, spec, *, operation=None):
                self.started.set()
                self.release.wait(timeout=5)
                return super().create(spec, operation=operation)

        runtime = BlockingCreateRuntime()
        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            node = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=raw_path / "limited-node-sandboxes.json",
                image_file=raw_path / "limited-node-images.json",
                job_id="job-1",
                node_id="node-1",
                total_resources=ResourceQuantity(vcpu=4, memory_mb=4096, disk_mb=128),
                runtime=runtime,
            )
            node_thread = Thread(target=node.serve_forever, daemon=True)
            node_thread.start()
            try:
                node_host, node_port = node.server_address
                gateway = build_server(
                    "127.0.0.1",
                    0,
                    raw_path / "heartbeats.json",
                    routing_file=raw_path / "routes.sqlite",
                    image_file=raw_path / "gateway-images.json",
                    local_image_builds_enabled=False,
                    metrics_file=raw_path / "metrics.jsonl",
                    max_concurrent_sandbox_creates=1,
                )
                gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
                gateway_thread.start()
                try:
                    host, port = gateway.server_address
                    base = f"http://{host}:{port}"
                    result = post_heartbeat(
                        f"{base}/v1/nodes/heartbeat",
                        build_heartbeat(
                            job_id="job-1",
                            node_id="node-1",
                            active_sandboxes=0,
                            node_url=f"http://{node_host}:{node_port}",
                            capabilities=("sandbox", "image-cache", "disk-quota"),
                            total_resources=ResourceQuantity(
                                vcpu=4,
                                memory_mb=4096,
                                disk_mb=128,
                            ),
                        ),
                    )
                    self.assertEqual(result.status, 200)

                    def create_one() -> dict:
                        return self._json_request(
                            f"{base}/v1/sandboxes",
                            method="POST",
                            payload={
                                "id": "limited-one",
                                "image": "busybox",
                                "cpus": 1,
                                "memory_mb": 128,
                                "disk_mb": 64,
                            },
                        )

                    with ThreadPoolExecutor(max_workers=1) as executor:
                        future = executor.submit(create_one)
                        self.assertTrue(runtime.started.wait(timeout=5))
                        same_payload = json.dumps(
                            {
                                "id": "limited-one",
                                "image": "busybox",
                                "cpus": 1,
                                "memory_mb": 128,
                                "disk_mb": 64,
                            }
                        ).encode("utf-8")
                        same_req = request.Request(
                            f"{base}/v1/sandboxes",
                            data=same_payload,
                            method="POST",
                            headers={"Content-Type": "application/json"},
                        )
                        try:
                            with request.urlopen(same_req, timeout=5):
                                self.fail("expected duplicate create to fail")
                        except error.HTTPError as exc:
                            same_status = exc.code
                            same_retry_after = exc.headers.get("Retry-After")
                            same = json.loads(exc.read().decode("utf-8"))
                        busy_payload = json.dumps(
                            {
                                "id": "limited-two",
                                "image": "busybox",
                                "cpus": 1,
                                "memory_mb": 128,
                                "disk_mb": 64,
                            }
                        ).encode("utf-8")
                        req = request.Request(
                            f"{base}/v1/sandboxes",
                            data=busy_payload,
                            method="POST",
                            headers={"Content-Type": "application/json"},
                        )
                        try:
                            with request.urlopen(req, timeout=5):
                                self.fail("expected busy create to fail")
                        except error.HTTPError as exc:
                            busy_status = exc.code
                            retry_after = exc.headers.get("Retry-After")
                            busy = json.loads(exc.read().decode("utf-8"))
                        finally:
                            runtime.release.set()
                        created = future.result(timeout=5)
                    metrics = self._json_request(f"{base}/v1/metrics")
                finally:
                    runtime.release.set()
                    gateway.shutdown()
                    gateway.server_close()
            finally:
                runtime.release.set()
                node.shutdown()
                node.server_close()

        self.assertEqual(created["sandbox"]["spec"]["id"], "limited-one")
        self.assertEqual(same_status, 503)
        self.assertEqual(same_retry_after, "2")
        self.assertTrue(same["retryable"])
        self.assertEqual(
            same["error"],
            "gateway is busy creating sandboxes; retry shortly",
        )
        self.assertEqual(busy_status, 503)
        self.assertEqual(retry_after, "2")
        self.assertTrue(busy["retryable"])
        self.assertEqual(busy["max_concurrent_sandbox_creates"], 1)
        self.assertTrue(
            any(
                item["status"] == "error"
                and item["spans"][0]["attributes"].get("outcome") == "gateway_busy"
                for item in metrics["traces"]["recent"]
            )
        )

    def test_gateway_placement_contention_fails_fast_with_retryable_json(self) -> None:
        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            gateway = build_server(
                "127.0.0.1",
                0,
                raw_path / "heartbeats.json",
                routing_file=raw_path / "routes.sqlite",
                metrics_file=raw_path / "metrics.jsonl",
            )
            Thread(target=gateway.serve_forever, daemon=True).start()
            try:
                host, port = gateway.server_address
                self.assertTrue(
                    control_plane._GATEWAY_SCHEDULING_LOCK.acquire(blocking=False)
                )
                started = monotonic()
                try:
                    result = self._json_request(
                        f"http://{host}:{port}/v1/sandboxes",
                        method="POST",
                        payload={
                            "id": "placement-busy",
                            "image": "busybox",
                            "cpus": 1,
                            "memory_mb": 128,
                            "disk_mb": 64,
                        },
                        allow_error=True,
                    )
                finally:
                    control_plane._GATEWAY_SCHEDULING_LOCK.release()
                elapsed = monotonic() - started
                metrics = self._json_request(f"http://{host}:{port}/v1/metrics")
            finally:
                gateway.shutdown()
                gateway.server_close()

        self.assertEqual(result["status"], 503)
        self.assertTrue(result["body"]["retryable"])
        self.assertIn("reserving sandbox placement", result["body"]["error"])
        self.assertLess(elapsed, 1)
        self.assertTrue(
            any(
                item["status"] == "error"
                and item["spans"][0]["attributes"].get("outcome")
                == "placement_busy"
                for item in metrics["traces"]["recent"]
            )
        )

    def test_gateway_create_burst_returns_only_retryable_json(self) -> None:
        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            gateway = build_server(
                "127.0.0.1",
                0,
                raw_path / "heartbeats.json",
                routing_file=raw_path / "routes.sqlite",
                max_concurrent_sandbox_creates=8,
            )
            Thread(target=gateway.serve_forever, daemon=True).start()
            try:
                host, port = gateway.server_address

                def create(index: int) -> dict:
                    return self._json_request(
                        f"http://{host}:{port}/v1/sandboxes",
                        method="POST",
                        payload={
                            "id": f"overload-{index}",
                            "image": "busybox",
                            "cpus": 1,
                            "memory_mb": 128,
                            "disk_mb": 64,
                        },
                        allow_error=True,
                    )

                self.assertTrue(
                    control_plane._GATEWAY_SCHEDULING_LOCK.acquire(blocking=False)
                )
                started = monotonic()
                try:
                    with ThreadPoolExecutor(max_workers=96) as executor:
                        results = list(executor.map(create, range(192)))
                finally:
                    control_plane._GATEWAY_SCHEDULING_LOCK.release()
                elapsed = monotonic() - started
            finally:
                gateway.shutdown()
                gateway.server_close()

        self.assertEqual({result["status"] for result in results}, {503})
        self.assertTrue(all(result["body"]["retryable"] for result in results))
        self.assertTrue(
            all(
                isinstance(result["body"].get("error"), str)
                for result in results
            )
        )
        self.assertLess(elapsed, 5)

    def test_gateway_create_admission_precedes_body_read_and_caps_json(self) -> None:
        with TemporaryDirectory() as raw_dir:
            gateway = build_server(
                "127.0.0.1",
                0,
                Path(raw_dir) / "heartbeats.json",
                routing_file=Path(raw_dir) / "routes.sqlite",
                max_concurrent_sandbox_creates=1,
            )
            Thread(target=gateway.serve_forever, daemon=True).start()
            try:
                host, port = gateway.server_address
                limiter = gateway.RequestHandlerClass.sandbox_create_limiter
                assert limiter is not None
                self.assertTrue(limiter.acquire(blocking=False))
                try:
                    connection = HTTPConnection(host, port, timeout=2)
                    connection.putrequest("POST", "/v1/sandboxes")
                    connection.putheader("Content-Type", "application/json")
                    connection.putheader("Content-Length", "1024")
                    connection.endheaders()
                    response = connection.getresponse()
                    busy_body = json.loads(response.read().decode("utf-8"))
                    connection.close()
                finally:
                    limiter.release()

                connection = HTTPConnection(host, port, timeout=2)
                connection.putrequest("POST", "/v1/sandboxes")
                connection.putheader("Content-Type", "application/json")
                connection.putheader(
                    "Content-Length",
                    str(control_plane.DEFAULT_MAX_JSON_BODY_BYTES + 1),
                )
                connection.endheaders()
                response = connection.getresponse()
                oversized_body = json.loads(response.read().decode("utf-8"))
                oversized_status = response.status
                connection.close()

                # Bad input must release admission for the next request.
                self.assertTrue(limiter.acquire(blocking=False))
                limiter.release()
            finally:
                gateway.shutdown()
                gateway.server_close()

        self.assertEqual(busy_body["retryable"], True)
        self.assertEqual(oversized_status, 400)
        self.assertIn("16777216 byte limit", oversized_body["error"])

    def test_brand_new_placement_trusts_complete_heartbeat_inventory(self) -> None:
        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            node = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=raw_path / "node-sandboxes.json",
                image_file=raw_path / "node-images.json",
                job_id="job-1",
                node_id="node-1",
                total_resources=ResourceQuantity(
                    vcpu=4,
                    memory_mb=4096,
                    disk_mb=4096,
                ),
                runtime=DockerGvisorRuntime(
                    dry_run=True,
                    allow_storage_opt_quota=True,
                ),
            )
            Thread(target=node.serve_forever, daemon=True).start()
            try:
                node_host, node_port = node.server_address
                gateway = build_server(
                    "127.0.0.1",
                    0,
                    raw_path / "heartbeats.json",
                    routing_file=raw_path / "routes.sqlite",
                )
                calls: list[tuple[str, str]] = []
                original_proxy = gateway.RequestHandlerClass._proxy_request

                def recording_proxy(handler, node_url, path, *, method, **kwargs):
                    calls.append((method, path))
                    return original_proxy(
                        handler,
                        node_url,
                        path,
                        method=method,
                        **kwargs,
                    )

                gateway.RequestHandlerClass._proxy_request = recording_proxy
                Thread(target=gateway.serve_forever, daemon=True).start()
                try:
                    host, port = gateway.server_address
                    base = f"http://{host}:{port}"
                    posted = post_heartbeat(
                        f"{base}/v1/nodes/heartbeat",
                        build_heartbeat(
                            job_id="job-1",
                            node_id="node-1",
                            node_url=f"http://{node_host}:{node_port}",
                            capabilities=("sandbox", "image-cache", "disk-quota"),
                            total_resources=ResourceQuantity(
                                vcpu=4,
                                memory_mb=4096,
                                disk_mb=4096,
                            ),
                            inventory_complete=True,
                            cached_images=("busybox",),
                        ),
                    )
                    created = self._json_request(
                        f"{base}/v1/sandboxes",
                        method="POST",
                        payload={
                            "id": "inventory-fast-path",
                            "image": "busybox",
                            "memory_mb": 64,
                            "disk_mb": 64,
                        },
                    )
                finally:
                    gateway.shutdown()
                    gateway.server_close()
            finally:
                node.shutdown()
                node.server_close()

        self.assertEqual(posted.status, 200)
        self.assertEqual(created["sandbox"]["spec"]["id"], "inventory-fast-path")
        self.assertNotIn(("GET", "/v1/sandboxes"), calls)

    def test_gateway_persists_route_before_node_create_finishes(self) -> None:
        class SlowCreateNode(BaseHTTPRequestHandler):
            started = Event()
            release = Event()

            def do_POST(self) -> None:
                if self.path != "/v1/sandboxes":
                    self.send_response(404)
                    self.end_headers()
                    return
                length = int(self.headers.get("Content-Length", "0"))
                raw = json.loads(self.rfile.read(length).decode("utf-8"))
                operation = raw.pop("_ucloud_operation")
                self.started.set()
                self.release.wait(timeout=5)
                self._write_json(
                    {
                        "sandbox": {
                            "spec": raw,
                            "generation": operation["generation"],
                            "operation_id": operation["operation_id"],
                            "spec_hash": operation["spec_hash"],
                        },
                        "command": ["docker", "run"],
                        "exitCode": 0,
                    },
                    status=201,
                )

            def log_message(self, format: str, *args: object) -> None:
                del format, args

            def _write_json(
                self, payload: dict[str, object], *, status: int = 200
            ) -> None:
                body = json.dumps(payload).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            route_file = raw_path / "routes.sqlite"
            node = ThreadingHTTPServer(("127.0.0.1", 0), SlowCreateNode)
            node_thread = Thread(target=node.serve_forever, daemon=True)
            node_thread.start()
            try:
                node_host, node_port = node.server_address
                gateway = build_server(
                    "127.0.0.1",
                    0,
                    raw_path / "heartbeats.json",
                    routing_file=route_file,
                    metrics_file=raw_path / "metrics.jsonl",
                )
                gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
                gateway_thread.start()
                try:
                    host, port = gateway.server_address
                    base = f"http://{host}:{port}"
                    result = post_heartbeat(
                        f"{base}/v1/nodes/heartbeat",
                        build_heartbeat(
                            job_id="job-1",
                            node_id="node-1",
                            node_url=f"http://{node_host}:{node_port}",
                            capabilities=("sandbox", "image-cache", "disk-quota"),
                            cached_images=("busybox",),
                            total_resources=ResourceQuantity(
                                vcpu=4,
                                memory_mb=4096,
                                disk_mb=8192,
                            ),
                        ),
                    )
                    self.assertEqual(result.status, 200)

                    with ThreadPoolExecutor(max_workers=1) as executor:
                        future = executor.submit(
                            self._json_request,
                            f"{base}/v1/sandboxes",
                            method="POST",
                            payload={
                                "id": "slow-one",
                                "image": "busybox",
                                "cpus": 1,
                                "memory_mb": 512,
                                "disk_mb": 1024,
                            },
                        )
                        self.assertTrue(SlowCreateNode.started.wait(timeout=5))
                        route = RoutingStore(route_file).get_sandbox("slow-one")
                        SlowCreateNode.release.set()
                        created = future.result(timeout=5)
                finally:
                    SlowCreateNode.release.set()
                    gateway.shutdown()
                    gateway.server_close()
            finally:
                SlowCreateNode.release.set()
                node.shutdown()
                node.server_close()

        self.assertIsNotNone(route)
        self.assertEqual(created["sandbox"]["spec"]["id"], "slow-one")

    def test_gateway_keeps_old_ambiguous_create_route_without_rescheduling(
        self,
    ) -> None:
        class EmptyNode(BaseHTTPRequestHandler):
            post_count = 0

            def do_GET(self) -> None:
                if self.path == "/v1/sandboxes":
                    self._write_json({"sandboxes": []})
                    return
                self.send_response(404)
                self.end_headers()

            def do_POST(self) -> None:
                type(self).post_count += 1
                self._write_json({"error": "unexpected create"}, status=500)

            def log_message(self, format: str, *args: object) -> None:
                del format, args

            def _write_json(
                self, payload: dict[str, object], *, status: int = 200
            ) -> None:
                body = json.dumps(payload).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            route_file = raw_path / "routes.sqlite"
            node = ThreadingHTTPServer(("127.0.0.1", 0), EmptyNode)
            node_thread = Thread(target=node.serve_forever, daemon=True)
            node_thread.start()
            try:
                node_host, node_port = node.server_address
                node_url = f"http://{node_host}:{node_port}"
                RoutingStore(route_file).upsert_sandbox(
                    SandboxRoute(
                        sandbox_id="recovering-one",
                        node_id="node-1",
                        job_id="job-1",
                        node_url=node_url,
                        resources=ResourceQuantity(
                            vcpu=1,
                            memory_mb=512,
                            disk_mb=1024,
                        ),
                        state="creating",
                        created_at=(utc_now() - timedelta(days=1)).isoformat(),
                        updated_at=(utc_now() - timedelta(days=1)).isoformat(),
                    )
                )
                gateway = build_server(
                    "127.0.0.1",
                    0,
                    raw_path / "heartbeats.json",
                    routing_file=route_file,
                    metrics_file=raw_path / "metrics.jsonl",
                )
                gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
                gateway_thread.start()
                try:
                    host, port = gateway.server_address
                    base = f"http://{host}:{port}"
                    result = post_heartbeat(
                        f"{base}/v1/nodes/heartbeat",
                        build_heartbeat(
                            job_id="job-1",
                            node_id="node-1",
                            node_url=node_url,
                            capabilities=("sandbox", "image-cache", "disk-quota"),
                            cached_images=("busybox",),
                        ),
                    )
                    self.assertEqual(result.status, 200)
                    refreshed = self._json_request(f"{base}/v1/sandboxes?refresh=true")
                    retry = self._json_request(
                        f"{base}/v1/sandboxes",
                        method="POST",
                        payload={
                            "id": "recovering-one",
                            "image": "busybox",
                            "cpus": 1,
                            "memory_mb": 512,
                            "disk_mb": 1024,
                        },
                        allow_error=True,
                    )
                    route = RoutingStore(route_file).get_sandbox("recovering-one")
                finally:
                    gateway.shutdown()
                    gateway.server_close()
            finally:
                node.shutdown()
                node.server_close()

        self.assertEqual(retry["status"], 503)
        self.assertTrue(retry["body"]["retryable"])
        self.assertEqual(refreshed["sandboxes"], [])
        self.assertEqual(EmptyNode.post_count, 0)
        self.assertIsNotNone(route)

    def test_gateway_fences_tool_traffic_until_direct_create_is_owned(self) -> None:
        class PlannedNode(BaseHTTPRequestHandler):
            post_count = 0
            record: dict[str, object] = {}

            def do_GET(self) -> None:
                if self.path == "/v1/sandboxes":
                    self._write_json({"sandboxes": [type(self).record]})
                    return
                self.send_response(404)
                self.end_headers()

            def do_POST(self) -> None:
                type(self).post_count += 1
                self._write_json({"error": "tool traffic leaked"}, status=500)

            def log_message(self, format: str, *args: object) -> None:
                del format, args

            def _write_json(
                self, payload: dict[str, object], *, status: int = 200
            ) -> None:
                body = json.dumps(payload).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            route_file = raw_path / "routes.sqlite"
            node = ThreadingHTTPServer(("127.0.0.1", 0), PlannedNode)
            Thread(target=node.serve_forever, daemon=True).start()
            try:
                node_host, node_port = node.server_address
                spec = SandboxSpec(
                    id="planned-one",
                    image="busybox",
                    cpus=1,
                    memory_mb=512,
                )
                spec_hash = control_plane.sandbox_spec_fingerprint(spec)
                operation_id = "create-planned-one"
                PlannedNode.record = {
                    "state": "planned",
                    "spec": spec.to_dict(),
                    "generation": 1,
                    "operation_id": operation_id,
                    "spec_hash": spec_hash,
                }
                RoutingStore(route_file).upsert_sandbox(
                    SandboxRoute(
                        sandbox_id=spec.id,
                        node_id="node-1",
                        job_id="job-1",
                        node_url=f"http://{node_host}:{node_port}",
                        resources=spec.requested_resources(),
                        spec=spec.to_dict(),
                        state="creating",
                        generation=1,
                        create_operation_id=operation_id,
                        spec_hash=spec_hash,
                    )
                )
                gateway = build_server(
                    "127.0.0.1",
                    0,
                    raw_path / "heartbeats.json",
                    routing_file=route_file,
                )
                Thread(target=gateway.serve_forever, daemon=True).start()
                try:
                    host, port = gateway.server_address
                    result = self._json_request(
                        f"http://{host}:{port}/v1/sandboxes/{spec.id}/exec",
                        method="POST",
                        payload={"command": ["/bin/true"]},
                        allow_error=True,
                    )
                finally:
                    gateway.shutdown()
                    gateway.server_close()
            finally:
                node.shutdown()
                node.server_close()

        self.assertEqual(result["status"], 503)
        self.assertTrue(result["body"]["retryable"])
        self.assertIn("creation is already in progress", result["body"]["error"])
        self.assertEqual(PlannedNode.post_count, 0)

    def test_registry_reference_survives_ambiguous_create_restart_and_reconciliation(
        self,
    ) -> None:
        class AmbiguousCreateNode(BaseHTTPRequestHandler):
            create_count = 0
            created_spec: dict[str, object] | None = None
            operation: dict[str, object] | None = None

            def do_GET(self) -> None:
                if self.path == "/v1/sandboxes":
                    sandboxes: list[dict[str, object]] = []
                    if type(self).created_spec is not None and type(self).operation:
                        operation = type(self).operation or {}
                        sandboxes.append(
                            {
                                "state": "running",
                                "spec": type(self).created_spec,
                                "generation": operation["generation"],
                                "operation_id": operation["operation_id"],
                                "spec_hash": operation["spec_hash"],
                            }
                        )
                    self._write_json({"sandboxes": sandboxes})
                    return
                self.send_response(404)
                self.end_headers()

            def do_POST(self) -> None:
                length = int(self.headers.get("Content-Length", "0"))
                raw = json.loads(self.rfile.read(length).decode("utf-8"))
                type(self).operation = raw.pop("_ucloud_operation")
                type(self).created_spec = raw
                type(self).create_count += 1
                self._write_json({"error": "create timed out"}, status=503)

            def log_message(self, format: str, *args: object) -> None:
                del format, args

            def _write_json(
                self, payload: dict[str, object], *, status: int = 200
            ) -> None:
                body = json.dumps(payload).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            route_file = raw_path / "routes.sqlite"
            usage_file = raw_path / "registry-usage.json"
            node = ThreadingHTTPServer(("127.0.0.1", 0), AmbiguousCreateNode)
            Thread(target=node.serve_forever, daemon=True).start()
            try:
                node_host, node_port = node.server_address
                node_url = f"http://{node_host}:{node_port}"
                heartbeat = build_heartbeat(
                    job_id="job-1",
                    node_id="node-1",
                    node_url=node_url,
                    active_sandboxes=1,
                    capabilities=("sandbox", "image-cache", "disk-quota"),
                    cached_images=("registry.example.org/repo:v1",),
                    total_resources=ResourceQuantity(
                        vcpu=4,
                        memory_mb=4096,
                        disk_mb=8192,
                    ),
                    inventory=(),
                    inventory_complete=True,
                )
                first_gateway = build_server(
                    "127.0.0.1",
                    0,
                    raw_path / "heartbeats.json",
                    routing_file=route_file,
                    registry_usage_file=usage_file,
                )
                Thread(target=first_gateway.serve_forever, daemon=True).start()
                try:
                    host, port = first_gateway.server_address
                    base = f"http://{host}:{port}"
                    self.assertEqual(
                        post_heartbeat(
                            f"{base}/v1/nodes/heartbeat",
                            heartbeat,
                        ).status,
                        200,
                    )
                    created = self._json_request(
                        f"{base}/v1/sandboxes",
                        method="POST",
                        payload={
                            "id": "ambiguous-one",
                            "image": "registry.example.org/repo:v1",
                            "cpus": 1,
                            "memory_mb": 512,
                        },
                        allow_error=True,
                    )
                    before_restart = RegistryUsageStore(usage_file).snapshot()
                finally:
                    first_gateway.shutdown()
                    first_gateway.server_close()

                second_gateway = build_server(
                    "127.0.0.1",
                    0,
                    raw_path / "heartbeats.json",
                    routing_file=route_file,
                    registry_usage_file=usage_file,
                )
                Thread(target=second_gateway.serve_forever, daemon=True).start()
                try:
                    host, port = second_gateway.server_address
                    base = f"http://{host}:{port}"
                    self.assertEqual(
                        post_heartbeat(
                            f"{base}/v1/nodes/heartbeat",
                            heartbeat,
                        ).status,
                        200,
                    )
                    refreshed = self._json_request(f"{base}/v1/sandboxes?refresh=true")
                    after_restart = RegistryUsageStore(usage_file).snapshot()
                    route = RoutingStore(route_file).get_sandbox("ambiguous-one")
                finally:
                    second_gateway.shutdown()
                    second_gateway.server_close()
            finally:
                node.shutdown()
                node.server_close()

        self.assertEqual(created["status"], 503)
        self.assertEqual(AmbiguousCreateNode.create_count, 1)
        self.assertEqual(len(before_restart.leases), 1)
        self.assertEqual(set(after_restart.leases), set(before_restart.leases))
        before_lease = next(iter(before_restart.leases.values()))
        after_lease = next(iter(after_restart.leases.values()))
        self.assertEqual(before_lease.expires_at, "")
        self.assertEqual(after_lease.expires_at, before_lease.expires_at)
        self.assertEqual(refreshed["sandboxes"][0]["state"], "running")
        self.assertIsNotNone(route)
        assert route is not None
        self.assertEqual(route.state, "running")

    def test_closed_node_admission_requeues_create_without_pinning_route(
        self,
    ) -> None:
        class ClosedAdmissionNode(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                self._write_json({"sandboxes": []})

            def do_POST(self) -> None:
                self._write_json(
                    {
                        "error": "direct node admission is closed",
                        "error_code": "node_admission_closed",
                        "retryable": True,
                    },
                    status=503,
                )

            def log_message(self, format: str, *args: object) -> None:
                del format, args

            def _write_json(
                self, payload: dict[str, object], *, status: int = 200
            ) -> None:
                body = json.dumps(payload).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            route_file = raw_path / "routes.sqlite"
            node = ThreadingHTTPServer(("127.0.0.1", 0), ClosedAdmissionNode)
            Thread(target=node.serve_forever, daemon=True).start()
            try:
                node_host, node_port = node.server_address
                image = "busybox"
                gateway = build_server(
                    "127.0.0.1",
                    0,
                    raw_path / "heartbeats.json",
                    routing_file=route_file,
                )
                Thread(target=gateway.serve_forever, daemon=True).start()
                try:
                    host, port = gateway.server_address
                    base = f"http://{host}:{port}"
                    self.assertEqual(
                        post_heartbeat(
                            f"{base}/v1/nodes/heartbeat",
                            build_heartbeat(
                                job_id="job-1",
                                node_id="node-1",
                                node_url=f"http://{node_host}:{node_port}",
                                capabilities=(
                                    "sandbox",
                                    "image-cache",
                                    "disk-quota",
                                ),
                                cached_images=(image,),
                                total_resources=ResourceQuantity(
                                    vcpu=4,
                                    memory_mb=4096,
                                    disk_mb=8192,
                                ),
                            ),
                        ).status,
                        200,
                    )
                    created = self._json_request(
                        f"{base}/v1/sandboxes",
                        method="POST",
                        payload={
                            "id": "drain-race",
                            "image": image,
                            "cpus": 1,
                            "memory_mb": 512,
                        },
                        allow_error=True,
                    )
                    state = RoutingStore(route_file).load()
                finally:
                    gateway.shutdown()
                    gateway.server_close()
            finally:
                node.shutdown()
                node.server_close()

        self.assertEqual(created["status"], 503)
        self.assertTrue(created["body"]["retryable"])
        self.assertEqual(created["body"]["error_code"], "node_admission_closed")
        self.assertNotIn("drain-race", state.sandboxes)
        self.assertEqual(
            state.pending["drain-race"].failure_reason,
            "node_admission_closed",
        )

    def test_registry_lease_failure_blocks_sandbox_create_and_image_pull_dispatch(
        self,
    ) -> None:
        class CountingNode(BaseHTTPRequestHandler):
            post_count = 0

            def do_POST(self) -> None:
                type(self).post_count += 1
                self._write_json({"ok": True}, status=201)

            def log_message(self, format: str, *args: object) -> None:
                del format, args

            def _write_json(
                self, payload: dict[str, object], *, status: int = 200
            ) -> None:
                body = json.dumps(payload).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        class BrokenRegistryUsageStore:
            def touch_image(self, image_ref: str) -> None:
                del image_ref
                raise OSError("usage store unavailable")

        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            route_file = raw_path / "routes.sqlite"
            node = ThreadingHTTPServer(("127.0.0.1", 0), CountingNode)
            Thread(target=node.serve_forever, daemon=True).start()
            try:
                node_host, node_port = node.server_address
                image = "registry.example.org/repo:v1"
                gateway = build_server(
                    "127.0.0.1",
                    0,
                    raw_path / "heartbeats.json",
                    routing_file=route_file,
                    registry_usage_file=raw_path / "registry-usage.json",
                )
                Thread(target=gateway.serve_forever, daemon=True).start()
                try:
                    host, port = gateway.server_address
                    base = f"http://{host}:{port}"
                    self.assertEqual(
                        post_heartbeat(
                            f"{base}/v1/nodes/heartbeat",
                            build_heartbeat(
                                job_id="job-1",
                                node_id="node-1",
                                node_url=f"http://{node_host}:{node_port}",
                                capabilities=(
                                    "sandbox",
                                    "image-cache",
                                    "disk-quota",
                                ),
                                cached_images=(image,),
                                total_resources=ResourceQuantity(
                                    vcpu=4,
                                    memory_mb=4096,
                                    disk_mb=8192,
                                ),
                            ),
                        ).status,
                        200,
                    )
                    gateway.RequestHandlerClass.registry_usage_store = (
                        BrokenRegistryUsageStore()
                    )
                    create = self._json_request(
                        f"{base}/v1/sandboxes",
                        method="POST",
                        payload={
                            "id": "blocked-one",
                            "image": image,
                            "cpus": 1,
                            "memory_mb": 512,
                        },
                        allow_error=True,
                    )
                    retry_spec = SandboxSpec.from_dict(
                        {
                            "id": "blocked-retry",
                            "image": image,
                            "cpus": 1,
                            "memory_mb": 512,
                        }
                    )
                    RoutingStore(route_file).allocate_sandbox_create(
                        SandboxRoute(
                            sandbox_id=retry_spec.id,
                            node_id="node-1",
                            job_id="job-1",
                            node_url=f"http://{node_host}:{node_port}",
                            resources=retry_spec.requested_resources(),
                            spec=retry_spec.to_dict(),
                            state="creating",
                        ),
                        spec_hash=sandbox_spec_fingerprint(retry_spec),
                    )
                    retry = self._json_request(
                        f"{base}/v1/sandboxes",
                        method="POST",
                        payload={
                            "id": "blocked-retry",
                            "image": image,
                            "cpus": 1,
                            "memory_mb": 512,
                        },
                        allow_error=True,
                    )
                    pull = self._json_request(
                        f"{base}/v1/images/pull",
                        method="POST",
                        payload={"image": image, "count": 1},
                        allow_error=True,
                    )
                finally:
                    gateway.shutdown()
                    gateway.server_close()
            finally:
                node.shutdown()
                node.server_close()

        self.assertEqual(create["status"], 503)
        self.assertEqual(retry["status"], 503)
        self.assertEqual(pull["status"], 503)
        self.assertTrue(create["body"]["retryable"])
        self.assertTrue(pull["body"]["retryable"])
        self.assertEqual(CountingNode.post_count, 0)
        self.assertIsNone(RoutingStore(route_file).get_sandbox("blocked-one"))

    def test_pinned_registry_reference_persists_digest_and_protection_usage(
        self,
    ) -> None:
        digest = "sha256:" + "6" * 64
        with TemporaryDirectory() as raw_dir:
            store = RegistryUsageStore(Path(raw_dir) / "usage.json")
            self.assertTrue(
                control_plane._persist_registry_image_protection(
                    store,
                    ("ucloud-sandbox-registry:5000/repo/a:v1" f"@{digest}"),
                    "sandbox:one",
                    touch=True,
                    persistent=True,
                )
            )
            snapshot = store.snapshot()

        lease = snapshot.leases[("repo/a", "v1", "sandbox:one")]
        self.assertEqual(lease.digest, digest)
        self.assertIn(
            ("repo/a", control_plane.digest_protection_tag(digest)),
            snapshot.records,
        )

    def test_explicit_managed_digest_fails_closed_without_protection_tag(
        self,
    ) -> None:
        digest = "sha256:" + "7" * 64
        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            gateway = build_server(
                "127.0.0.1",
                0,
                raw_path / "heartbeats.json",
                routing_file=raw_path / "routes.sqlite",
                registry_url="http://registry.invalid:5000",
            )
            Thread(target=gateway.serve_forever, daemon=True).start()
            try:
                host, port = gateway.server_address
                with patch.object(
                    control_plane.RegistryClient,
                    "ensure_digest_protection_tag",
                    side_effect=OSError("registry unavailable"),
                ):
                    prepared = self._json_request(
                        f"http://{host}:{port}/v1/capacity/prepare",
                        method="POST",
                        payload={
                            "id": "unprotected-digest",
                            "count": 1,
                            "ttl_seconds": 60,
                            "image": ("registry.invalid:5000/repo/a:v1" f"@{digest}"),
                            "cpus": 1,
                            "memory_mb": 512,
                        },
                        allow_error=True,
                    )
                stored_prepared = RoutingStore(
                    raw_path / "routes.sqlite"
                ).prepared_capacity()
            finally:
                gateway.shutdown()
                gateway.server_close()

        self.assertEqual(prepared["status"], 400)
        self.assertTrue(prepared["body"]["retryable"])
        self.assertEqual(stored_prepared, [])

    def test_managed_image_id_does_not_fall_back_when_digest_protection_fails(
        self,
    ) -> None:
        digest = "sha256:" + "a" * 64
        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            image_file = raw_path / "images.json"
            now = utc_now()
            ImageStore(image_file).upsert(
                ImageRecord(
                    id="protected-image",
                    tag="registry.invalid:5000/repo/a:v1",
                    source="build:/tmp/context",
                    state="available",
                    created_at=now,
                    updated_at=now,
                    pushed=True,
                    manifest_digest=digest,
                )
            )
            gateway = build_server(
                "127.0.0.1",
                0,
                raw_path / "heartbeats.json",
                routing_file=raw_path / "routes.sqlite",
                image_file=image_file,
                registry_url="http://registry.invalid:5000",
            )
            Thread(target=gateway.serve_forever, daemon=True).start()
            try:
                host, port = gateway.server_address
                with patch.object(
                    control_plane.RegistryClient,
                    "manifest_digest",
                    return_value=digest,
                ), patch.object(
                    control_plane.RegistryClient,
                    "ensure_digest_protection_tag",
                    side_effect=OSError("registry unavailable"),
                ):
                    images = self._json_request(f"http://{host}:{port}/v1/images")
                    prepared = self._json_request(
                        f"http://{host}:{port}/v1/capacity/prepare",
                        method="POST",
                        payload={
                            "id": "unprotected-image-id",
                            "count": 1,
                            "ttl_seconds": 60,
                            "image": "protected-image",
                            "cpus": 1,
                            "memory_mb": 512,
                        },
                        allow_error=True,
                    )
            finally:
                gateway.shutdown()
                gateway.server_close()

        self.assertEqual(images["images"][0]["manifest_digest"], "")
        self.assertEqual(prepared["status"], 400)
        self.assertTrue(prepared["body"]["retryable"])

    def test_unrelated_image_records_are_not_enriched_during_create(self) -> None:
        digest = "sha256:" + "b" * 64
        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            image_file = raw_path / "images.json"
            now = utc_now()
            image_store = ImageStore(image_file)
            for index in range(20):
                image_store.upsert(
                    ImageRecord(
                        id=f"unrelated-{index}",
                        tag=f"registry.invalid:5000/repo/image-{index}:v1",
                        source="build:/tmp/context",
                        state="available",
                        created_at=now,
                        updated_at=now,
                        pushed=True,
                        manifest_digest=digest,
                    )
                )
            gateway = build_server(
                "127.0.0.1",
                0,
                raw_path / "heartbeats.json",
                routing_file=raw_path / "routes.sqlite",
                image_file=image_file,
                registry_url="http://registry.invalid:5000",
            )
            Thread(target=gateway.serve_forever, daemon=True).start()
            try:
                host, port = gateway.server_address
                with patch.object(
                    control_plane.RegistryClient,
                    "manifest_digest",
                    return_value=digest,
                ) as manifest_digest:
                    created = self._json_request(
                        f"http://{host}:{port}/v1/sandboxes",
                        method="POST",
                        payload={
                            "id": "regular-image-create",
                            "image": "busybox",
                            "cpus": 1,
                            "memory_mb": 128,
                            "disk_mb": 1024,
                        },
                        allow_error=True,
                    )
            finally:
                gateway.shutdown()
                gateway.server_close()

        self.assertEqual(created["status"], 503)
        manifest_digest.assert_not_called()

    def test_failed_sandbox_pull_persists_incarnation_demand_until_cancel(self) -> None:
        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            route_file = raw_path / "routes.sqlite"
            gateway = build_server(
                "127.0.0.1",
                0,
                raw_path / "heartbeats.json",
                routing_file=route_file,
            )
            gateway.RequestHandlerClass._ensure_image_on_node = (  # type: ignore[attr-defined]
                lambda _self, _heartbeat, _image: control_plane.ProxiedResponse(
                    502,
                    {"Content-Type": "application/json"},
                    b'{"error":"registry unavailable"}',
                )
            )
            gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
            gateway_thread.start()
            try:
                host, port = gateway.server_address
                base = f"http://{host}:{port}"
                self.assertEqual(
                    post_heartbeat(
                        f"{base}/v1/nodes/heartbeat",
                        build_heartbeat(
                            job_id="job-1",
                            node_id="node-1",
                            node_url="http://node-1.invalid:8090",
                            capabilities=("sandbox", "image-cache", "disk-quota"),
                            cached_images=(),
                            total_resources=ResourceQuantity(
                                vcpu=4,
                                memory_mb=4096,
                                disk_mb=8192,
                            ),
                            inventory=(),
                            inventory_complete=True,
                        ),
                    ).status,
                    200,
                )
                failed = self._json_request(
                    f"{base}/v1/sandboxes",
                    method="POST",
                    payload={
                        "id": "pull-failed",
                        "image": "busybox",
                        "cpus": 1,
                        "memory_mb": 512,
                        "disk_mb": 1024,
                    },
                    allow_error=True,
                )
                pending = RoutingStore(route_file).get_pending("pull-failed")
                route = RoutingStore(route_file).get_sandbox("pull-failed")
                demand = self._json_request(f"{base}/v1/demand")
                canceled = self._json_request(
                    f"{base}/v1/sandboxes/pull-failed",
                    method="DELETE",
                )
                pending_after_cancel = RoutingStore(route_file).get_pending(
                    "pull-failed"
                )
            finally:
                gateway.shutdown()
                gateway.server_close()

        self.assertEqual(failed["status"], 502)
        self.assertIsNone(route)
        self.assertIsNotNone(pending)
        assert pending is not None
        self.assertEqual(pending.generation, 1)
        self.assertTrue(pending.operation_id.startswith("create-"))
        self.assertTrue(pending.spec_hash)
        self.assertEqual(pending.failure_reason, "image_pull_http_502")
        self.assertEqual(demand["pending_resources"]["disk_mb"], 0)
        self.assertEqual(demand["pending_count"], 0)
        self.assertEqual(
            demand["suppressed_pending_resources"]["disk_mb"],
            1024,
        )
        self.assertEqual(demand["suppressed_pending_count"], 1)
        self.assertFalse(demand["pending"][0]["capacity_demand"])
        self.assertTrue(canceled["ok"])
        self.assertIsNone(pending_after_cancel)

    def test_gateway_preserves_route_when_node_delete_returns_client_error(
        self,
    ) -> None:
        class FailingDeleteNode(BaseHTTPRequestHandler):
            delete_headers: list[tuple[str | None, str | None]] = []

            def do_DELETE(self) -> None:
                type(self).delete_headers.append(
                    (
                        self.headers.get("X-UCloud-Sandbox-Generation"),
                        self.headers.get("X-UCloud-Sandbox-Operation-Id"),
                    )
                )
                self._write_json({"error": "docker delete failed"}, status=400)

            def log_message(self, format: str, *args: object) -> None:
                del format, args

            def _write_json(
                self, payload: dict[str, object], *, status: int = 200
            ) -> None:
                body = json.dumps(payload).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            route_file = raw_path / "routes.sqlite"
            usage_file = raw_path / "registry-usage.json"
            node = ThreadingHTTPServer(("127.0.0.1", 0), FailingDeleteNode)
            Thread(target=node.serve_forever, daemon=True).start()
            try:
                node_host, node_port = node.server_address
                node_url = f"http://{node_host}:{node_port}"
                routing_store = RoutingStore(route_file)
                routing_store.upsert_sandbox(
                    SandboxRoute(
                        sandbox_id="delete-one",
                        node_id="node-1",
                        job_id="job-1",
                        node_url=node_url,
                        spec={
                            "id": "delete-one",
                            "image": "registry.example.org/repo:v1",
                        },
                        state="running",
                        generation=3,
                        create_operation_id="create-3",
                        spec_hash="spec-hash-3",
                        node_epoch="epoch-1",
                        activity_epoch=7,
                    )
                )
                stored_route = routing_store.get_sandbox("delete-one")
                assert stored_route is not None
                RegistryUsageStore(usage_file).acquire_reference(
                    "repo",
                    "v1",
                    control_plane._registry_route_reference_owner(
                        stored_route,
                        deployment_id="",
                    ),
                )
                gateway = build_server(
                    "127.0.0.1",
                    0,
                    raw_path / "heartbeats.json",
                    routing_file=route_file,
                    registry_usage_file=usage_file,
                )
                Thread(target=gateway.serve_forever, daemon=True).start()
                try:
                    host, port = gateway.server_address
                    base = f"http://{host}:{port}"
                    posted = post_heartbeat(
                        f"{base}/v1/nodes/heartbeat",
                        build_heartbeat(
                            job_id="job-1",
                            node_id="node-1",
                            node_url=node_url,
                            active_sandboxes=1,
                            capabilities=("sandbox", "disk-quota"),
                        ),
                    )
                    self.assertEqual(posted.status, 200)
                    leases_before = RegistryUsageStore(usage_file).snapshot().leases
                    response = self._json_request(
                        f"{base}/v1/sandboxes/delete-one",
                        method="DELETE",
                        allow_error=True,
                    )
                    retried = self._json_request(
                        f"{base}/v1/sandboxes/delete-one",
                        method="DELETE",
                        allow_error=True,
                    )
                    route = RoutingStore(route_file).get_sandbox("delete-one")
                    leases_after = RegistryUsageStore(usage_file).snapshot().leases
                finally:
                    gateway.shutdown()
                    gateway.server_close()
            finally:
                node.shutdown()
                node.server_close()

        self.assertEqual(response["status"], 400)
        self.assertEqual(retried["status"], 400)
        self.assertIsNotNone(route)
        assert route is not None
        self.assertTrue(route.delete_operation_id.startswith("delete-"))
        self.assertEqual(
            FailingDeleteNode.delete_headers,
            [("3", route.delete_operation_id), ("3", route.delete_operation_id)],
        )
        self.assertEqual(len(leases_before), 1)
        self.assertEqual(leases_after, leases_before)

    def test_gateway_releases_registry_reference_only_after_successful_delete(
        self,
    ) -> None:
        class SuccessfulDeleteNode(BaseHTTPRequestHandler):
            def do_DELETE(self) -> None:
                self._write_json(
                    {
                        "ok": True,
                        "deleted": {
                            "generation": int(
                                self.headers["X-UCloud-Sandbox-Generation"]
                            )
                        },
                    }
                )

            def log_message(self, format: str, *args: object) -> None:
                del format, args

            def _write_json(self, payload: dict[str, object]) -> None:
                body = json.dumps(payload).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            route_file = raw_path / "routes.sqlite"
            usage_file = raw_path / "registry-usage.json"
            node = ThreadingHTTPServer(("127.0.0.1", 0), SuccessfulDeleteNode)
            Thread(target=node.serve_forever, daemon=True).start()
            try:
                node_host, node_port = node.server_address
                node_url = f"http://{node_host}:{node_port}"
                routing_store = RoutingStore(route_file)
                routing_store.upsert_sandbox(
                    SandboxRoute(
                        sandbox_id="delete-success",
                        node_id="node-1",
                        job_id="job-1",
                        node_url=node_url,
                        spec={
                            "id": "delete-success",
                            "image": "registry.example.org/repo:v1",
                        },
                        state="running",
                        generation=4,
                        create_operation_id="create-4",
                        spec_hash="spec-hash-4",
                    )
                )
                stored_route = routing_store.get_sandbox("delete-success")
                assert stored_route is not None
                RegistryUsageStore(usage_file).acquire_reference(
                    "repo",
                    "v1",
                    control_plane._registry_route_reference_owner(
                        stored_route,
                        deployment_id="",
                    ),
                )
                gateway = build_server(
                    "127.0.0.1",
                    0,
                    raw_path / "heartbeats.json",
                    routing_file=route_file,
                    registry_usage_file=usage_file,
                )
                Thread(target=gateway.serve_forever, daemon=True).start()
                try:
                    host, port = gateway.server_address
                    base = f"http://{host}:{port}"
                    self.assertEqual(
                        post_heartbeat(
                            f"{base}/v1/nodes/heartbeat",
                            build_heartbeat(
                                job_id="job-1",
                                node_id="node-1",
                                node_url=node_url,
                                active_sandboxes=1,
                                capabilities=("sandbox", "disk-quota"),
                            ),
                        ).status,
                        200,
                    )
                    self.assertEqual(
                        len(RegistryUsageStore(usage_file).snapshot().leases),
                        1,
                    )
                    deleted = self._json_request(
                        f"{base}/v1/sandboxes/delete-success",
                        method="DELETE",
                    )
                    leases = RegistryUsageStore(usage_file).snapshot().leases
                    route = RoutingStore(route_file).get_sandbox("delete-success")
                finally:
                    gateway.shutdown()
                    gateway.server_close()
            finally:
                node.shutdown()
                node.server_close()

        self.assertTrue(deleted["deleted"])
        self.assertEqual(leases, {})
        self.assertIsNone(route)

    def test_registry_route_reference_owner_is_incarnation_sensitive(self) -> None:
        route = SandboxRoute(
            sandbox_id="sandbox-one",
            node_id="node-1",
            job_id="job-1",
            node_url="http://node-1:8090",
            spec={"image": "registry.example.org/repo:v1"},
            created_at="2026-07-09T10:00:00+00:00",
        )

        owner = control_plane._registry_route_reference_owner(
            route,
            deployment_id="prod",
        )
        same_owner = control_plane._registry_route_reference_owner(
            route,
            deployment_id="prod",
        )
        next_generation_owner = control_plane._registry_route_reference_owner(
            route,
            deployment_id="prod",
            route_generation=2,
        )
        next_incarnation_owner = control_plane._registry_route_reference_owner(
            replace(route, created_at="2026-07-09T11:00:00+00:00"),
            deployment_id="prod",
        )

        self.assertEqual(owner, same_owner)
        self.assertNotEqual(owner, next_generation_owner)
        self.assertNotEqual(owner, next_incarnation_owner)

    def test_gateway_routes_sandbox_file_upload_and_download(self) -> None:
        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            node = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=raw_path / "node-sandboxes.json",
                image_file=raw_path / "node-images.json",
                job_id="job-1",
                node_id="node-1",
                total_resources=ResourceQuantity(vcpu=2, memory_mb=1024, disk_mb=128),
                runtime=FileRuntime(),
            )
            node_thread = Thread(target=node.serve_forever, daemon=True)
            node_thread.start()
            try:
                node_host, node_port = node.server_address
                gateway = build_server(
                    "127.0.0.1",
                    0,
                    raw_path / "heartbeats.json",
                    routing_file=raw_path / "routes.sqlite",
                )
                gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
                gateway_thread.start()
                try:
                    host, port = gateway.server_address
                    base = f"http://{host}:{port}"
                    result = post_heartbeat(
                        f"{base}/v1/nodes/heartbeat",
                        build_heartbeat(
                            job_id="job-1",
                            node_id="node-1",
                            active_sandboxes=0,
                            node_url=f"http://{node_host}:{node_port}",
                            capabilities=("sandbox", "image-cache", "disk-quota"),
                            total_resources=ResourceQuantity(
                                vcpu=2,
                                memory_mb=1024,
                                disk_mb=128,
                            ),
                        ),
                    )
                    self.assertEqual(result.status, 200)
                    created = self._json_request(
                        f"{base}/v1/sandboxes",
                        method="POST",
                        payload={
                            "id": "file-one",
                            "image": "busybox",
                            "memory_mb": 128,
                        },
                    )
                    uploaded = self._bytes_request(
                        f"{base}/v1/sandboxes/file-one/files?path={quote('/tmp/out.txt')}",
                        method="PUT",
                        body=b"via gateway\n",
                    )
                    downloaded = self._bytes_request(
                        f"{base}/v1/sandboxes/file-one/files?path={quote('/tmp/out.txt')}",
                    )
                finally:
                    gateway.shutdown()
                    gateway.server_close()
            finally:
                node.shutdown()
                node.server_close()

        self.assertEqual(created["sandbox"]["spec"]["id"], "file-one")
        self.assertEqual(uploaded["json"]["size"], 12)
        self.assertEqual(downloaded["body"], b"via gateway\n")
        self.assertEqual(downloaded["headers"]["X-Sandbox-Path"], "/tmp/out.txt")

    def test_gateway_bounds_buffered_node_responses(self) -> None:
        read_sizes: list[int | None] = []

        class OversizedResponse:
            status = 200
            headers: dict[str, str] = {"Content-Type": "application/json"}

            def __enter__(self) -> "OversizedResponse":
                return self

            def __exit__(self, *args: object) -> None:
                return None

            def read(self, size: int | None = None) -> bytes:
                read_sizes.append(size)
                return b"x" * 9

        with TemporaryDirectory() as raw_dir:
            gateway = build_server(
                "127.0.0.1",
                0,
                Path(raw_dir) / "heartbeats.json",
                upstream_node_url="http://node.invalid:8090",
            )
            gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
            gateway_thread.start()
            try:
                host, port = gateway.server_address
                with (
                    patch.object(control_plane, "DEFAULT_MAX_PROXY_RESPONSE_BYTES", 8),
                    patch.object(
                        control_plane.request,
                        "urlopen",
                        return_value=OversizedResponse(),
                    ),
                ):
                    conn = HTTPConnection(host, port, timeout=5)
                    try:
                        conn.request("GET", "/v1/sandboxes")
                        response = conn.getresponse()
                        payload = json.loads(response.read().decode("utf-8"))
                    finally:
                        conn.close()
            finally:
                gateway.shutdown()
                gateway.server_close()

        self.assertEqual(response.status, 502)
        self.assertEqual(payload["max_bytes"], 8)
        self.assertEqual(read_sizes, [9])

    def test_gateway_streams_file_downloads_in_bounded_chunks(self) -> None:
        body = b"a" * (control_plane.PROXY_STREAM_CHUNK_BYTES * 2 + 7)
        remaining = bytearray(body)
        read_sizes: list[int | None] = []

        class StreamingResponse:
            status = 200
            headers = {
                "Content-Type": "application/octet-stream",
                "Content-Length": str(len(body)),
                "X-Sandbox-Path": "/tmp/large.bin",
            }

            def __enter__(self) -> "StreamingResponse":
                return self

            def __exit__(self, *args: object) -> None:
                return None

            def read(self, size: int | None = None) -> bytes:
                read_sizes.append(size)
                assert size is not None
                chunk = bytes(remaining[:size])
                del remaining[:size]
                return chunk

        with TemporaryDirectory() as raw_dir:
            gateway = build_server(
                "127.0.0.1",
                0,
                Path(raw_dir) / "heartbeats.json",
                upstream_node_url="http://node.invalid:8090",
            )
            gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
            gateway_thread.start()
            try:
                host, port = gateway.server_address
                with patch.object(
                    control_plane.request,
                    "urlopen",
                    return_value=StreamingResponse(),
                ):
                    conn = HTTPConnection(host, port, timeout=5)
                    try:
                        conn.request(
                            "GET",
                            "/v1/sandboxes/file-one/files?path=/tmp/large.bin",
                        )
                        response = conn.getresponse()
                        downloaded = response.read()
                        response_path = response.headers["X-Sandbox-Path"]
                    finally:
                        conn.close()
            finally:
                gateway.shutdown()
                gateway.server_close()

        self.assertEqual(response.status, 200)
        self.assertEqual(downloaded, body)
        self.assertEqual(response_path, "/tmp/large.bin")
        self.assertGreater(len(read_sizes), 2)
        self.assertTrue(
            all(
                size is not None
                and size <= control_plane.PROXY_STREAM_CHUNK_BYTES
                for size in read_sizes
            )
        )

    def test_gateway_deduplicates_concurrent_cold_image_pull(self) -> None:
        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            image_runtime = CountingPullRuntime()
            node = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=raw_path / "node-sandboxes.json",
                image_file=raw_path / "node-images.json",
                job_id="job-1",
                node_id="node-1",
                total_resources=ResourceQuantity(
                    vcpu=16,
                    memory_mb=32768,
                    disk_mb=200_000,
                ),
                runtime=DockerGvisorRuntime(dry_run=True, allow_storage_opt_quota=True),
                image_runtime=image_runtime,
            )
            node_thread = Thread(target=node.serve_forever, daemon=True)
            node_thread.start()
            try:
                node_host, node_port = node.server_address
                gateway = build_server(
                    "127.0.0.1",
                    0,
                    raw_path / "heartbeats.json",
                    routing_file=raw_path / "routes.json",
                    image_file=raw_path / "gateway-images.json",
                    local_image_builds_enabled=False,
                )
                gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
                gateway_thread.start()
                try:
                    host, port = gateway.server_address
                    base = f"http://{host}:{port}"
                    result = post_heartbeat(
                        f"{base}/v1/nodes/heartbeat",
                        build_heartbeat(
                            job_id="job-1",
                            node_id="node-1",
                            node_url=f"http://{node_host}:{node_port}",
                            capabilities=("sandbox", "image-cache", "disk-quota"),
                            total_resources=ResourceQuantity(
                                vcpu=16,
                                memory_mb=32768,
                                disk_mb=200_000,
                            ),
                        ),
                    )
                    self.assertEqual(result.status, 200)

                    results: dict[int, dict] = {}
                    errors: list[BaseException] = []

                    def create(index: int) -> None:
                        try:
                            results[index] = self._json_request(
                                f"{base}/v1/sandboxes",
                                method="POST",
                                payload={
                                    "id": f"cold-{index}",
                                    "image": "python:3.12-slim",
                                    "cpus": 1,
                                    "memory_mb": 512,
                                    "disk_mb": 1024,
                                },
                            )
                        except BaseException as exc:
                            errors.append(exc)

                    threads = [
                        Thread(target=create, args=(index,)) for index in range(8)
                    ]
                    for thread in threads:
                        thread.start()
                    for thread in threads:
                        thread.join()
                    sandboxes = self._json_request(f"{base}/v1/sandboxes")
                finally:
                    gateway.shutdown()
                    gateway.server_close()
            finally:
                node.shutdown()
                node.server_close()

            if errors:
                raise errors[0]
            self.assertEqual(len(results), 8)
            self.assertEqual(
                {result["sandbox"]["spec"]["id"] for result in results.values()},
                {f"cold-{index}" for index in range(8)},
            )
            self.assertEqual(image_runtime.pulls, ["python:3.12-slim"])
            self.assertEqual(len(sandboxes["sandboxes"]), 8)

    def test_gateway_recovers_idempotent_duplicate_create_on_node(self) -> None:
        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            node = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=raw_path / "node-sandboxes.json",
                image_file=raw_path / "node-images.json",
                job_id="job-1",
                node_id="node-1",
                total_resources=ResourceQuantity(
                    vcpu=4, memory_mb=8192, disk_mb=100_000
                ),
                runtime=DockerGvisorRuntime(dry_run=True, allow_storage_opt_quota=True),
            )
            node_thread = Thread(target=node.serve_forever, daemon=True)
            node_thread.start()
            try:
                node_host, node_port = node.server_address
                gateway = build_server(
                    "127.0.0.1",
                    0,
                    raw_path / "heartbeats.json",
                    routing_file=raw_path / "routes.json",
                    image_file=raw_path / "gateway-images.json",
                    local_image_builds_enabled=False,
                )
                gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
                gateway_thread.start()
                try:
                    node_base = f"http://{node_host}:{node_port}"
                    host, port = gateway.server_address
                    base = f"http://{host}:{port}"
                    result = post_heartbeat(
                        f"{base}/v1/nodes/heartbeat",
                        build_heartbeat(
                            job_id="job-1",
                            node_id="node-1",
                            node_url=node_base,
                            capabilities=("sandbox", "image-cache", "disk-quota"),
                            total_resources=ResourceQuantity(
                                vcpu=4,
                                memory_mb=8192,
                                disk_mb=100_000,
                            ),
                        ),
                    )
                    self.assertEqual(result.status, 200)
                    payload = {
                        "id": "dup-one",
                        "image": "busybox",
                        "command": ["sh", "-lc", "sleep 2147483647"],
                        "cpus": 1,
                        "memory_mb": 512,
                        "disk_mb": 1024,
                    }
                    direct = self._json_request(
                        f"{node_base}/v1/sandboxes",
                        method="POST",
                        payload=payload,
                    )
                    recovered = self._json_request(
                        f"{base}/v1/sandboxes",
                        method="POST",
                        payload=payload,
                    )
                    sandboxes = self._json_request(f"{node_base}/v1/sandboxes")
                finally:
                    gateway.shutdown()
                    gateway.server_close()
            finally:
                node.shutdown()
                node.server_close()

            self.assertEqual(direct["sandbox"]["spec"]["id"], "dup-one")
            self.assertEqual(recovered["sandbox"]["spec"]["id"], "dup-one")
            self.assertTrue(recovered["recovered"])
            self.assertEqual(len(sandboxes["sandboxes"]), 1)
            route = RoutingStore(raw_path / "routes.json").get_sandbox("dup-one")
            self.assertIsNotNone(route)

    def test_gateway_preserves_duplicate_create_conflict_for_different_spec(
        self,
    ) -> None:
        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            node = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=raw_path / "node-sandboxes.json",
                image_file=raw_path / "node-images.json",
                job_id="job-1",
                node_id="node-1",
                total_resources=ResourceQuantity(
                    vcpu=4, memory_mb=8192, disk_mb=100_000
                ),
                runtime=DockerGvisorRuntime(dry_run=True, allow_storage_opt_quota=True),
            )
            node_thread = Thread(target=node.serve_forever, daemon=True)
            node_thread.start()
            try:
                node_host, node_port = node.server_address
                gateway = build_server(
                    "127.0.0.1",
                    0,
                    raw_path / "heartbeats.json",
                    routing_file=raw_path / "routes.json",
                    image_file=raw_path / "gateway-images.json",
                    local_image_builds_enabled=False,
                )
                gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
                gateway_thread.start()
                try:
                    node_base = f"http://{node_host}:{node_port}"
                    host, port = gateway.server_address
                    base = f"http://{host}:{port}"
                    result = post_heartbeat(
                        f"{base}/v1/nodes/heartbeat",
                        build_heartbeat(
                            job_id="job-1",
                            node_id="node-1",
                            node_url=node_base,
                            capabilities=("sandbox", "image-cache", "disk-quota"),
                            total_resources=ResourceQuantity(
                                vcpu=4,
                                memory_mb=8192,
                                disk_mb=100_000,
                            ),
                        ),
                    )
                    self.assertEqual(result.status, 200)
                    self._json_request(
                        f"{node_base}/v1/sandboxes",
                        method="POST",
                        payload={
                            "id": "dup-one",
                            "image": "busybox",
                            "cpus": 1,
                            "memory_mb": 512,
                            "disk_mb": 1024,
                        },
                    )
                    conflict = self._json_request(
                        f"{base}/v1/sandboxes",
                        method="POST",
                        payload={
                            "id": "dup-one",
                            "image": "python:3.12-slim",
                            "cpus": 1,
                            "memory_mb": 512,
                            "disk_mb": 1024,
                        },
                        allow_error=True,
                    )
                finally:
                    gateway.shutdown()
                    gateway.server_close()
            finally:
                node.shutdown()
                node.server_close()

            self.assertEqual(conflict["status"], 409)
            self.assertIn("already exists", conflict["body"]["error"])

    def test_gateway_lists_incompatible_nodes_but_does_not_place_new_sandboxes(
        self,
    ) -> None:
        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            node = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=raw_path / "node-sandboxes.json",
                image_file=raw_path / "node-images.json",
                job_id="job-old",
                node_id="node-old",
                total_resources=ResourceQuantity(
                    vcpu=4, memory_mb=8192, disk_mb=100_000
                ),
                runtime=DockerGvisorRuntime(dry_run=True, allow_storage_opt_quota=True),
            )
            node_thread = Thread(target=node.serve_forever, daemon=True)
            node_thread.start()
            try:
                node_host, node_port = node.server_address
                node_base = f"http://{node_host}:{node_port}"
                self._json_request(
                    f"{node_base}/v1/sandboxes",
                    method="POST",
                    payload={
                        "id": "old-one",
                        "image": "busybox",
                        "cpus": 1,
                        "memory_mb": 512,
                        "disk_mb": 1024,
                    },
                )
                gateway = build_server(
                    "127.0.0.1",
                    0,
                    raw_path / "heartbeats.json",
                    routing_file=raw_path / "routes.json",
                    image_file=raw_path / "gateway-images.json",
                    local_image_builds_enabled=False,
                )
                gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
                gateway_thread.start()
                try:
                    host, port = gateway.server_address
                    base = f"http://{host}:{port}"
                    post_heartbeat(
                        f"{base}/v1/nodes/heartbeat",
                        build_heartbeat(
                            job_id="job-old",
                            node_id="node-old",
                            node_url=node_base,
                            agent_version="0.0.0-old",
                            capabilities=("sandbox", "image-cache", "disk-quota"),
                            total_resources=ResourceQuantity(
                                vcpu=4,
                                memory_mb=8192,
                                disk_mb=100_000,
                            ),
                        ),
                    )
                    listed = self._json_request(f"{base}/v1/sandboxes?refresh=true")
                    create = self._json_request(
                        f"{base}/v1/sandboxes",
                        method="POST",
                        payload={
                            "id": "new-one",
                            "image": "busybox",
                            "cpus": 1,
                            "memory_mb": 512,
                            "disk_mb": 1024,
                        },
                        allow_error=True,
                    )
                finally:
                    gateway.shutdown()
                    gateway.server_close()
            finally:
                node.shutdown()
                node.server_close()

            self.assertEqual(listed["sandboxes"][0]["id"], "old-one")
            self.assertEqual(listed["sandboxes"][0]["node"]["node_id"], "node-old")
            self.assertEqual(create["status"], 503)
            self.assertIn("no ready node", create["body"]["error"])

    def test_structures_non_json_proxy_errors(self) -> None:
        response = control_plane.ProxiedResponse(
            503,
            {"Content-Type": "text/html"},
            b"<html><title>Job is unavailable | UCloud</title></html>",
        )

        structured = control_plane._structured_proxy_error(response)

        self.assertIsNotNone(structured)
        assert structured is not None
        self.assertTrue(structured["retryable"])
        self.assertEqual(structured["status"], 503)
        self.assertIn("Job is unavailable", structured["upstream_body_preview"])

    def test_node_dns_failure_is_a_structured_retryable_503(self) -> None:
        response = control_plane._node_transport_error_response(
            OSError("Temporary failure in name resolution")
        )

        self.assertEqual(response.status, 503)
        self.assertEqual(response.transport_error_kind, "dns")
        self.assertEqual(response.json()["code"], "node_dns_unavailable")
        self.assertTrue(response.json()["retryable"])
        self.assertFalse(control_plane._node_create_may_still_be_running(response))

    def test_node_timeout_is_a_structured_retryable_504(self) -> None:
        response = control_plane._node_transport_error_response(
            TimeoutError("timed out")
        )

        self.assertEqual(response.status, 504)
        self.assertEqual(response.json()["code"], "node_request_timeout")
        self.assertTrue(response.json()["retryable"])
        self.assertTrue(control_plane._node_create_may_still_be_running(response))

    def test_active_admission_deferral_is_a_definitive_create_rejection(self) -> None:
        response = control_plane.ProxiedResponse(
            503,
            {"Content-Type": "application/json"},
            json.dumps(
                {
                    "error": "direct node has insufficient active headroom",
                    "error_code": "node_active_admission_deferred",
                    "retryable": True,
                }
            ).encode("utf-8"),
        )

        self.assertTrue(control_plane._node_create_definitively_rejected(response))
        self.assertEqual(
            control_plane._node_create_rejection_reason(response),
            "node_active_admission_deferred",
        )

        old_node_response = control_plane.ProxiedResponse(
            503,
            {"Content-Type": "application/json"},
            json.dumps(
                {
                    "error": (
                        "direct node has insufficient active CPU or memory "
                        "admission headroom; retry on another node"
                    ),
                    "retryable": True,
                }
            ).encode("utf-8"),
        )
        self.assertEqual(
            control_plane._node_create_rejection_reason(old_node_response),
            "node_active_admission_deferred",
        )

    def test_planned_node_record_is_not_ready_for_client_traffic(self) -> None:
        self.assertFalse(control_plane._sandbox_record_is_ready({"state": "planned"}))
        self.assertFalse(
            control_plane._sandbox_record_is_ready({"state": "rootfs_ready"})
        )
        self.assertTrue(control_plane._sandbox_record_is_ready({"state": "running"}))
        self.assertTrue(control_plane._sandbox_record_is_ready({"state": "parked"}))

    def test_enriches_old_node_sandbox_records_with_top_level_identity(self) -> None:
        heartbeat = build_heartbeat(
            job_id="job-1",
            node_id="node-1",
            node_url="http://node-1:8090",
        )

        enriched = control_plane._enrich_sandbox_record(
            {
                "container_name": "ucloud-sandbox-old-one",
                "spec": {
                    "id": "old-one",
                    "image": "busybox",
                    "labels": {"sample": "old"},
                },
                "state": "running",
            },
            heartbeat,
        )

        self.assertEqual(enriched["id"], "old-one")
        self.assertEqual(enriched["sandbox_id"], "old-one")
        self.assertEqual(enriched["name"], "ucloud-sandbox-old-one")
        self.assertEqual(enriched["image"], "busybox")
        self.assertEqual(enriched["labels"], {"sample": "old"})
        self.assertEqual(enriched["node"]["node_id"], "node-1")

    def test_gateway_builds_images_locally_and_sandbox_nodes_pull_registry_tag(
        self,
    ) -> None:
        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            regular = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=raw_path / "regular-sandboxes.json",
                image_file=raw_path / "regular-images.json",
                job_id="job-1",
                node_id="node-1",
                total_resources=ResourceQuantity(
                    vcpu=4, memory_mb=8192, disk_mb=100_000
                ),
                runtime=DockerGvisorRuntime(dry_run=True, allow_storage_opt_quota=True),
            )
            regular_thread = Thread(target=regular.serve_forever, daemon=True)
            regular_thread.start()
            try:
                regular_host, regular_port = regular.server_address
                gateway = build_server(
                    "127.0.0.1",
                    0,
                    raw_path / "heartbeats.json",
                    routing_file=raw_path / "routes.json",
                    image_file=raw_path / "gateway-images.json",
                    image_runtime=DockerImageRuntime(dry_run=True),
                    registry_worker_url="http://sandbox-gateway-prod:5000",
                )
                gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
                gateway_thread.start()
                try:
                    host, port = gateway.server_address
                    base = f"http://{host}:{port}"
                    for heartbeat in (
                        build_heartbeat(
                            job_id="job-1",
                            node_id="node-1",
                            node_url=f"http://{regular_host}:{regular_port}",
                            capabilities=("sandbox", "image-cache", "disk-quota"),
                            total_resources=ResourceQuantity(
                                vcpu=4,
                                memory_mb=8192,
                                disk_mb=100_000,
                            ),
                        ),
                        build_heartbeat(
                            job_id="job-2",
                            node_id="node-2",
                            node_url="http://builder.invalid:8090",
                            capabilities=("image-cache", "image-build", "snapshot"),
                            total_resources=ResourceQuantity(
                                vcpu=64,
                                memory_mb=262144,
                                disk_mb=1_000_000,
                            ),
                        ),
                    ):
                        result = post_heartbeat(
                            f"{base}/v1/nodes/heartbeat",
                            heartbeat,
                        )
                        self.assertEqual(result.status, 200)

                    built = self._json_request(
                        f"{base}/v1/images/build",
                        method="POST",
                        payload={
                            "id": "custom",
                            "context_path": "/tmp/context",
                        },
                    )
                    images = self._json_request(f"{base}/v1/images")
                    created = self._json_request(
                        f"{base}/v1/sandboxes",
                        method="POST",
                        payload={
                            "id": "custom-one",
                            "image": "registry.example.org/custom:latest",
                            "memory_mb": 128,
                        },
                    )
                    with request.urlopen(
                        f"http://{regular_host}:{regular_port}/v1/sandboxes",
                        timeout=5,
                    ) as response:
                        regular_payload = json.loads(response.read().decode("utf-8"))
                    regular_images = self._json_request(
                        f"http://{regular_host}:{regular_port}/v1/images"
                    )
                finally:
                    gateway.shutdown()
                    gateway.server_close()
            finally:
                regular.shutdown()
                regular.server_close()

            self.assertEqual(built["image"]["id"], "custom")
            self.assertTrue(built["image"]["tag"].startswith(
                "sandbox-gateway-prod:5000/ucloud-managed/custom-"
            ))
            self.assertTrue(built["image"]["tag"].endswith(":latest"))
            self.assertIn("pushCommand", built)
            self.assertEqual(
                [(image["id"], image.get("location")) for image in images["images"]],
                [("custom", "control-plane")],
            )
            self.assertEqual(created["sandbox"]["spec"]["id"], "custom-one")
            self.assertEqual(
                [record["spec"]["id"] for record in regular_payload["sandboxes"]],
                ["custom-one"],
            )
            self.assertEqual(
                [(image["id"], image["tag"]) for image in regular_images["images"]],
                [
                    (
                        "registry.example.org-custom-latest",
                        "registry.example.org/custom:latest",
                    )
                ],
            )

    def test_gateway_persists_manifest_digest_after_managed_registry_push(
        self,
    ) -> None:
        digest = "sha256:" + "9" * 64

        class RegistryHandler(BaseHTTPRequestHandler):
            def do_HEAD(self) -> None:
                self.send_response(200)
                self.send_header("Docker-Content-Digest", digest)
                self.end_headers()

            def log_message(self, format: str, *args: object) -> None:
                del format, args

        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            image_file = raw_path / "images.json"
            registry = ThreadingHTTPServer(("127.0.0.1", 0), RegistryHandler)
            Thread(target=registry.serve_forever, daemon=True).start()
            try:
                registry_host, registry_port = registry.server_address
                gateway = build_server(
                    "127.0.0.1",
                    0,
                    raw_path / "heartbeats.json",
                    routing_file=raw_path / "routes.sqlite",
                    image_file=image_file,
                    image_runtime=DockerImageRuntime(dry_run=True),
                    registry_url=f"http://{registry_host}:{registry_port}",
                    registry_worker_url="http://sandbox-gateway-prod:5000",
                )
                Thread(target=gateway.serve_forever, daemon=True).start()
                try:
                    host, port = gateway.server_address
                    built = self._json_request(
                        f"http://{host}:{port}/v1/images/build",
                        method="POST",
                        payload={
                            "id": "digest-build",
                            "context_path": "/tmp/context",
                        },
                    )
                    prepared = self._json_request(
                        f"http://{host}:{port}/v1/capacity/prepare",
                        method="POST",
                        payload={
                            "id": "digest-build-warmup",
                            "count": 1,
                            "ttl_seconds": 60,
                            "image": "digest-build",
                            "cpus": 1,
                            "memory_mb": 512,
                        },
                    )
                    stored_digest = (
                        ImageStore(image_file).load()["digest-build"].manifest_digest
                    )
                finally:
                    gateway.shutdown()
                    gateway.server_close()
            finally:
                registry.shutdown()
                registry.server_close()

        self.assertEqual(built["image"]["manifest_digest"], digest)
        generated_tag = built["image"]["tag"]
        self.assertTrue(generated_tag.startswith(
            "sandbox-gateway-prod:5000/ucloud-managed/digest-build-"
        ))
        self.assertEqual(stored_digest, digest)
        self.assertEqual(
            prepared["prepare"]["image"],
            f"{generated_tag}@{digest}",
        )

    def test_gateway_resolves_pushed_image_id_to_registry_tag_on_create(self) -> None:
        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            image_runtime = CountingPullRuntime()
            regular = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=raw_path / "regular-sandboxes.json",
                image_file=raw_path / "regular-images.json",
                job_id="job-1",
                node_id="node-1",
                total_resources=ResourceQuantity(
                    vcpu=4, memory_mb=8192, disk_mb=100_000
                ),
                runtime=DockerGvisorRuntime(dry_run=True, allow_storage_opt_quota=True),
                image_runtime=image_runtime,
            )
            regular_thread = Thread(target=regular.serve_forever, daemon=True)
            regular_thread.start()
            try:
                regular_host, regular_port = regular.server_address
                gateway = build_server(
                    "127.0.0.1",
                    0,
                    raw_path / "heartbeats.json",
                    routing_file=raw_path / "routes.json",
                    image_file=raw_path / "gateway-images.json",
                    image_runtime=DockerImageRuntime(dry_run=True),
                )
                gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
                gateway_thread.start()
                try:
                    host, port = gateway.server_address
                    base = f"http://{host}:{port}"
                    result = post_heartbeat(
                        f"{base}/v1/nodes/heartbeat",
                        build_heartbeat(
                            job_id="job-1",
                            node_id="node-1",
                            node_url=f"http://{regular_host}:{regular_port}",
                            capabilities=("sandbox", "image-cache", "disk-quota"),
                            total_resources=ResourceQuantity(
                                vcpu=4,
                                memory_mb=8192,
                                disk_mb=100_000,
                            ),
                        ),
                    )
                    self.assertEqual(result.status, 200)

                    built = self._json_request(
                        f"{base}/v1/images/build",
                        method="POST",
                        payload={
                            "id": "custom",
                            "tag": "registry.example.org/custom:latest",
                            "context_path": "/tmp/context",
                            "push": True,
                        },
                    )
                    created = self._json_request(
                        f"{base}/v1/sandboxes",
                        method="POST",
                        payload={
                            "id": "custom-by-id",
                            "image": "custom",
                            "memory_mb": 128,
                        },
                    )
                finally:
                    gateway.shutdown()
                    gateway.server_close()
            finally:
                regular.shutdown()
                regular.server_close()

            self.assertTrue(built["image"]["pushed"])
            self.assertTrue(built["image"]["available_to_sandboxes"])
            self.assertEqual(
                created["sandbox"]["spec"]["image"],
                "registry.example.org/custom:latest",
            )
            self.assertEqual(
                image_runtime.pulls, ["registry.example.org/custom:latest"]
            )

    def test_gateway_rejects_unpushed_image_id_on_create(self) -> None:
        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            gateway = build_server(
                "127.0.0.1",
                0,
                raw_path / "heartbeats.json",
                routing_file=raw_path / "routes.json",
                image_file=raw_path / "gateway-images.json",
                image_runtime=DockerImageRuntime(dry_run=True),
            )
            gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
            gateway_thread.start()
            try:
                host, port = gateway.server_address
                base = f"http://{host}:{port}"
                built = self._json_request(
                    f"{base}/v1/images/build",
                    method="POST",
                    payload={
                        "id": "custom",
                        "tag": "registry.example.org/custom:latest",
                        "context_path": "/tmp/context",
                    },
                )
                created = self._json_request(
                    f"{base}/v1/sandboxes",
                    method="POST",
                    payload={
                        "id": "custom-by-id",
                        "image": "custom",
                        "memory_mb": 128,
                    },
                    allow_error=True,
                )
            finally:
                gateway.shutdown()
                gateway.server_close()

            self.assertFalse(built["image"]["pushed"])
            self.assertEqual(created["status"], 400)
            self.assertIn("not available to sandbox nodes", created["body"]["error"])
            self.assertEqual(created["body"]["image_id"], "custom")

    def test_gateway_records_pending_image_build_when_no_builder_is_ready(self) -> None:
        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            gateway = build_server(
                "127.0.0.1",
                0,
                raw_path / "heartbeats.json",
                routing_file=raw_path / "routes.json",
            )
            gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
            gateway_thread.start()
            try:
                host, port = gateway.server_address
                result = self._json_request(
                    f"http://{host}:{port}/v1/images/build",
                    method="POST",
                    payload={
                        "id": "custom",
                        "tag": "registry.example.org/custom:latest",
                        "context_path": "/tmp/context",
                        "push": True,
                    },
                    allow_error=True,
                )
            finally:
                gateway.shutdown()
                gateway.server_close()

            self.assertEqual(result["status"], 503)
            self.assertEqual(result["body"]["pending_image_builds"], 1)
            self.assertEqual(
                RoutingStore(raw_path / "routes.json").pending_image_build_count(), 1
            )

    def test_content_addressed_context_survives_503_and_streams_to_builder(
        self,
    ) -> None:
        archive = _tar_gz_context({"Dockerfile": b"FROM scratch\n"})
        digest = f"sha256:{hashlib.sha256(archive).hexdigest()}"
        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            gateway_contexts = raw_path / "gateway-contexts"
            builder_contexts = raw_path / "builder-contexts"
            runtime = ContextRecordingRuntime()
            builder = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=raw_path / "builder-sandboxes.json",
                image_file=raw_path / "builder-images.json",
                job_id="job-builder",
                node_id="builder-1",
                image_runtime=runtime,
                image_builds_enabled=True,
                node_control_bearer_token="node-secret",
                build_context_store_dir=builder_contexts,
            )
            Thread(target=builder.serve_forever, daemon=True).start()
            gateway = build_server(
                "127.0.0.1",
                0,
                raw_path / "heartbeats.json",
                routing_file=raw_path / "routes.json",
                gateway_bearer_token="gateway-secret",
                node_control_bearer_token="node-secret",
                local_image_builds_enabled=False,
                build_context_store_dir=gateway_contexts,
            )
            Thread(target=gateway.serve_forever, daemon=True).start()
            try:
                host, port = gateway.server_address
                base = f"http://{host}:{port}"

                def upload(
                    target_digest: str,
                    *,
                    authorized: bool = True,
                    allow_error: bool = False,
                ) -> tuple[int, dict]:
                    headers = {"Content-Type": "application/gzip"}
                    if authorized:
                        headers["Authorization"] = "Bearer gateway-secret"
                    req = request.Request(
                        f"{base}/v1/image-contexts/{target_digest}",
                        data=archive,
                        method="PUT",
                        headers=headers,
                    )
                    try:
                        with request.urlopen(req, timeout=5) as response:
                            return response.status, json.loads(response.read())
                    except error.HTTPError as exc:
                        if not allow_error:
                            raise
                        return exc.code, json.loads(exc.read())

                unauthorized = upload(digest, authorized=False, allow_error=True)
                mismatch = upload(
                    "sha256:" + "0" * 64,
                    allow_error=True,
                )
                stored = upload(digest)
                duplicate = upload(digest)
                exists = self._json_request(
                    f"{base}/v1/image-contexts/{digest}",
                    headers={"Authorization": "Bearer gateway-secret"},
                )

                build_payload = {
                    "id": "content-addressed",
                    "tag": "registry.example.org/content-addressed:latest",
                    "context_path": ".",
                    "context_archive_digest": digest,
                    "context_archive_format": "tar.gz",
                    "context_archive_size": len(archive),
                }
                queued = self._json_request(
                    f"{base}/v1/images/build",
                    method="POST",
                    payload=build_payload,
                    headers={"Authorization": "Bearer gateway-secret"},
                    allow_error=True,
                )
                self.assertTrue(
                    (
                        gateway_contexts / "sha256" / digest.removeprefix("sha256:")
                    ).is_file()
                )

                builder_host, builder_port = builder.server_address
                heartbeat = post_heartbeat_with_headers(
                    f"{base}/v1/nodes/heartbeat",
                    build_heartbeat(
                        job_id="job-builder",
                        node_id="builder-1",
                        node_url=f"http://{builder_host}:{builder_port}",
                        capabilities=("image-cache", "image-build", "snapshot"),
                        total_resources=ResourceQuantity(
                            vcpu=16, memory_mb=49152, disk_mb=200000
                        ),
                    ),
                    {"Authorization": "Bearer gateway-secret"},
                )
                built = self._json_request(
                    f"{base}/v1/images/build",
                    method="POST",
                    payload=build_payload,
                    headers={"Authorization": "Bearer gateway-secret"},
                    allow_error=True,
                )
                builder_store = builder.RequestHandlerClass.build_context_store
                original_put = builder_store.put_with_status
                builder_uploads = 0

                def counting_put(*args, **kwargs):
                    nonlocal builder_uploads
                    builder_uploads += 1
                    return original_put(*args, **kwargs)

                builder_store.put_with_status = counting_put
                built_from_cached_context = self._json_request(
                    f"{base}/v1/images/build",
                    method="POST",
                    payload={
                        **build_payload,
                        "id": "content-addressed-cached",
                        "tag": "registry.example.org/content-addressed:cached",
                    },
                    headers={"Authorization": "Bearer gateway-secret"},
                )
                missing = self._json_request(
                    f"{base}/v1/images/build",
                    method="POST",
                    payload={
                        **build_payload,
                        "id": "missing-context",
                        "context_archive_digest": "sha256:" + "f" * 64,
                    },
                    headers={"Authorization": "Bearer gateway-secret"},
                    allow_error=True,
                )

                legacy = self._json_request(
                    f"http://{builder_host}:{builder_port}/v1/images/build",
                    method="POST",
                    payload={
                        "id": "legacy-context",
                        "tag": "registry.example.org/legacy:latest",
                        "context_path": ".",
                        "context_archive_base64": base64.b64encode(archive).decode(),
                        "context_archive_format": "tar.gz",
                    },
                    headers={"Authorization": "Bearer node-secret"},
                    allow_error=True,
                )
            finally:
                gateway.shutdown()
                gateway.server_close()
                builder.shutdown()
                builder.server_close()

            self.assertEqual(unauthorized[0], 401)
            self.assertEqual(mismatch[0], 400)
            self.assertIn("digest mismatch", mismatch[1]["error"])
            self.assertEqual(
                stored,
                (
                    201,
                    {"deduplicated": False, "digest": digest, "size": len(archive)},
                ),
            )
            self.assertEqual(
                duplicate,
                (
                    200,
                    {"deduplicated": True, "digest": digest, "size": len(archive)},
                ),
            )
            self.assertEqual(
                exists,
                {"deduplicated": True, "digest": digest, "size": len(archive)},
            )
            self.assertEqual(queued["status"], 503)
            self.assertEqual(heartbeat.status, 200)
            self.assertNotIn("status", built, built)
            self.assertEqual(built["image"]["id"], "content-addressed")
            self.assertEqual(
                built_from_cached_context["image"]["id"],
                "content-addressed-cached",
            )
            self.assertEqual(builder_uploads, 0)
            self.assertEqual(missing["status"], 400)
            self.assertIn("has not been uploaded", missing["body"]["error"])
            self.assertNotIn("status", legacy, legacy)
            self.assertEqual(legacy["image"]["id"], "legacy-context")
            self.assertEqual(
                runtime.dockerfiles,
                [b"FROM scratch\n", b"FROM scratch\n", b"FROM scratch\n"],
            )
            self.assertTrue(runtime.context_paths)
            self.assertTrue(all(not path.exists() for path in runtime.context_paths))
            self.assertTrue(
                (builder_contexts / "sha256" / digest.removeprefix("sha256:")).is_file()
            )

    def test_gateway_routes_image_build_to_builder_only_node(self) -> None:
        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            builder = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=raw_path / "builder-sandboxes.json",
                image_file=raw_path / "builder-images.json",
                job_id="job-builder",
                node_id="builder-1",
                runtime=DockerGvisorRuntime(dry_run=True, allow_storage_opt_quota=True),
                image_builds_enabled=True,
            )
            builder_thread = Thread(target=builder.serve_forever, daemon=True)
            builder_thread.start()
            try:
                builder_host, builder_port = builder.server_address
                gateway = build_server(
                    "127.0.0.1",
                    0,
                    raw_path / "heartbeats.json",
                    routing_file=raw_path / "routes.json",
                    image_file=raw_path / "gateway-images.json",
                    local_image_builds_enabled=False,
                    registry_worker_url="http://sandbox-gateway-prod:5000",
                )
                gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
                gateway_thread.start()
                try:
                    host, port = gateway.server_address
                    base = f"http://{host}:{port}"
                    result = post_heartbeat(
                        f"{base}/v1/nodes/heartbeat",
                        build_heartbeat(
                            job_id="job-builder",
                            node_id="builder-1",
                            node_url=f"http://{builder_host}:{builder_port}",
                            capabilities=("image-cache", "image-build", "snapshot"),
                            total_resources=ResourceQuantity(
                                vcpu=16, memory_mb=49152, disk_mb=200000
                            ),
                        ),
                    )
                    self.assertEqual(result.status, 200)

                    built = self._json_request(
                        f"{base}/v1/images/build",
                        method="POST",
                        payload={
                            "id": "custom",
                            "context_path": "/tmp/context",
                        },
                    )
                    builder_heartbeat = self._json_request(
                        f"http://{builder_host}:{builder_port}/v1/heartbeat"
                    )
                    images = self._json_request(f"{base}/v1/images")
                finally:
                    gateway.shutdown()
                    gateway.server_close()
            finally:
                builder.shutdown()
                builder.server_close()

            self.assertEqual(built["image"]["id"], "custom")
            self.assertTrue(built["image"]["tag"].startswith(
                "sandbox-gateway-prod:5000/ucloud-managed/custom-"
            ))
            self.assertIn("pushCommand", built)
            self.assertNotIn("sandbox", builder_heartbeat["heartbeat"]["capabilities"])
            self.assertIn(
                ("custom", "control-plane", True),
                [
                    (
                        image["id"],
                        image.get("location"),
                        image.get("available_to_sandboxes"),
                    )
                    for image in images["images"]
                ],
            )
            self.assertEqual(
                RoutingStore(raw_path / "routes.json").pending_image_build_count(), 0
            )

    def test_managed_registry_rewrites_legacy_alias_for_worker_transport(self) -> None:
        digest = "sha256:" + "a" * 64
        legacy = (
            "ucloud-sandbox-registry:5000/prime-rl/tmax:task-1"
            f"@{digest}"
        )

        rewritten = control_plane._managed_registry_worker_reference(
            legacy,
            "http://127.0.0.1:5000",
            "http://sandbox-gateway-prod:5000",
        )

        self.assertEqual(
            rewritten,
            "sandbox-gateway-prod:5000/prime-rl/tmax:task-1"
            f"@{digest}",
        )
        self.assertEqual(
            control_plane._managed_registry_worker_reference(
                "registry.example.org/team/image:latest",
                "http://127.0.0.1:5000",
                "http://sandbox-gateway-prod:5000",
            ),
            "registry.example.org/team/image:latest",
        )

    def test_gateway_clears_pending_signal_after_async_build_is_accepted(self) -> None:
        digest = "sha256:" + "8" * 64
        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            gateway_image_file = raw_path / "gateway-images.json"
            builder = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=raw_path / "builder-sandboxes.json",
                image_file=raw_path / "builder-images.json",
                job_id="job-builder",
                node_id="builder-1",
                runtime=DockerGvisorRuntime(dry_run=True, allow_storage_opt_quota=True),
                image_builds_enabled=True,
            )
            builder_thread = Thread(target=builder.serve_forever, daemon=True)
            builder_thread.start()
            try:
                builder_host, builder_port = builder.server_address
                gateway = build_server(
                    "127.0.0.1",
                    0,
                    raw_path / "heartbeats.json",
                    routing_file=raw_path / "routes.json",
                    image_file=gateway_image_file,
                    local_image_builds_enabled=False,
                    registry_url="http://registry.invalid:5000",
                )
                gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
                gateway_thread.start()
                try:
                    host, port = gateway.server_address
                    base = f"http://{host}:{port}"
                    result = post_heartbeat(
                        f"{base}/v1/nodes/heartbeat",
                        build_heartbeat(
                            job_id="job-builder",
                            node_id="builder-1",
                            node_url=f"http://{builder_host}:{builder_port}",
                            capabilities=("image-cache", "image-build", "snapshot"),
                            total_resources=ResourceQuantity(
                                vcpu=16, memory_mb=49152, disk_mb=200000
                            ),
                        ),
                    )
                    self.assertEqual(result.status, 200)

                    with patch.object(
                        control_plane.RegistryClient,
                        "manifest_digest",
                        return_value=digest,
                    ):
                        built = self._json_request(
                            f"{base}/v1/images/build",
                            method="POST",
                            payload={
                                "id": "custom",
                                "tag": "registry.invalid:5000/custom:latest",
                                "context_path": "/tmp/context",
                                "push": True,
                                "wait": False,
                            },
                        )
                        route_store = RoutingStore(raw_path / "routes.json")
                        self.assertEqual(route_store.pending_image_build_count(), 0)
                        # A stale signal from an older gateway/retry is also
                        # cleared when terminal status is observed.
                        route_store.upsert_pending_image_build(
                            "custom",
                            "registry.invalid:5000/custom:latest",
                        )
                        deadline = monotonic() + 2
                        while True:
                            finished = self._json_request(
                                f"{base}/v1/images/builds/custom"
                            )
                            if finished["build"]["status"] == "succeeded":
                                break
                            if monotonic() >= deadline:
                                self.fail("async image build did not finish")
                            sleep(0.01)
                finally:
                    gateway.shutdown()
                    gateway.server_close()
            finally:
                builder.shutdown()
                builder.server_close()

            self.assertEqual(built["build"]["image_id"], "custom")
            self.assertEqual(built["build"]["status"], "running")
            self.assertEqual(finished["build"]["status"], "succeeded")
            self.assertEqual(
                finished["build"]["image"]["manifest_digest"],
                digest,
            )
            self.assertEqual(
                ImageStore(gateway_image_file).load()["custom"].manifest_digest,
                digest,
            )
            self.assertEqual(
                RoutingStore(raw_path / "routes.json").pending_image_build_count(), 0
            )

    def test_gateway_uses_bounded_proxy_timeout_for_builder_image_builds(self) -> None:
        class FakeResponse:
            status = 201
            headers: dict[str, str] = {}

            def __enter__(self) -> "FakeResponse":
                return self

            def __exit__(self, *args: object) -> None:
                return None

            def read(self) -> bytes:
                return json.dumps(
                    {
                        "image": {
                            "id": "custom",
                            "tag": "registry.example.org/custom:latest",
                            "state": "available",
                            "pushed": True,
                        },
                        "command": ["docker", "build"],
                        "exitCode": 0,
                    }
                ).encode("utf-8")

        captured_timeouts: list[object] = []

        def fake_urlopen(req: object, timeout: object = None) -> FakeResponse:
            captured_timeouts.append(timeout)
            return FakeResponse()

        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            gateway = build_server(
                "127.0.0.1",
                0,
                raw_path / "heartbeats.json",
                routing_file=raw_path / "routes.sqlite",
                metrics_file=raw_path / "metrics.jsonl",
            )
            gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
            gateway_thread.start()
            try:
                host, port = gateway.server_address
                base = f"http://{host}:{port}"
                result = post_heartbeat(
                    f"{base}/v1/nodes/heartbeat",
                    build_heartbeat(
                        job_id="job-builder",
                        node_id="builder-1",
                        node_url="http://builder.invalid:8090",
                        capabilities=("image-cache", "image-build", "snapshot"),
                    ),
                )
                self.assertEqual(result.status, 200)

                with patch.object(control_plane.request, "urlopen", fake_urlopen):
                    body = json.dumps(
                        {
                            "id": "custom",
                            "tag": "registry.example.org/custom:latest",
                            "context_path": "/tmp/context",
                            "push": True,
                        }
                    )
                    conn = HTTPConnection(host, port, timeout=5)
                    try:
                        conn.request(
                            "POST",
                            "/v1/images/build",
                            body=body,
                            headers={"Content-Type": "application/json"},
                        )
                        response = conn.getresponse()
                        built = json.loads(response.read().decode("utf-8"))
                    finally:
                        conn.close()
            finally:
                gateway.shutdown()
                gateway.server_close()

        self.assertEqual(built["image"]["id"], "custom")
        self.assertEqual(IMAGE_BUILD_PROXY_TIMEOUT_SECONDS, 30 * 60)
        self.assertEqual(captured_timeouts, [IMAGE_BUILD_PROXY_TIMEOUT_SECONDS])

    def test_gateway_records_pending_demand_when_no_node_can_fit(self) -> None:
        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            gateway = build_server(
                "127.0.0.1",
                0,
                raw_path / "heartbeats.json",
                routing_file=raw_path / "routes.json",
            )
            gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
            gateway_thread.start()
            try:
                host, port = gateway.server_address
                base = f"http://{host}:{port}"
                result = self._json_request(
                    f"{base}/v1/sandboxes",
                    method="POST",
                    payload={
                        "id": "pending-one",
                        "image": "busybox",
                        "cpus": 1,
                        "memory_mb": 512,
                        "disk_mb": 1024,
                    },
                    allow_error=True,
                )
                demand = self._json_request(f"{base}/v1/demand")
                cleanup = self._json_request(
                    f"{base}/v1/sandboxes/pending-one",
                    method="DELETE",
                )
                demand_after_cleanup = self._json_request(f"{base}/v1/demand")
            finally:
                gateway.shutdown()
                gateway.server_close()

            self.assertEqual(result["status"], 503)
            self.assertTrue(result["body"]["retryable"])
            self.assertEqual(result["headers"]["Retry-After"], "2")
            self.assertEqual(
                result["headers"]["X-UCloud-Sandbox-Retryable"],
                "true",
            )
            self.assertEqual(demand["pending_resources"]["vcpu"], 1.0)
            self.assertEqual(demand["pending_resources"]["memory_mb"], 512)
            self.assertEqual(demand["pending_resources"]["disk_mb"], 1024)
            self.assertEqual(demand["pending"][0]["sandbox_id"], "pending-one")
            self.assertEqual(demand["pending"][0]["attempts"], 1)
            self.assertEqual(cleanup["ok"], True)
            self.assertEqual(demand_after_cleanup["pending_resources"]["vcpu"], 0.0)
            self.assertEqual(demand_after_cleanup["pending_resources"]["memory_mb"], 0)
            self.assertEqual(demand_after_cleanup["pending_resources"]["disk_mb"], 0)
            self.assertEqual(demand_after_cleanup["pending"], [])

    def test_gateway_rejects_shape_larger_than_autoscaled_node(self) -> None:
        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            gateway = build_server(
                "127.0.0.1",
                0,
                raw_path / "heartbeats.json",
                routing_file=raw_path / "routes.sqlite",
                max_sandbox_resources=ResourceQuantity(
                    vcpu=4,
                    memory_mb=8192,
                    disk_mb=16_384,
                ),
            )
            gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
            gateway_thread.start()
            try:
                host, port = gateway.server_address
                base = f"http://{host}:{port}"
                result = self._json_request(
                    f"{base}/v1/sandboxes",
                    method="POST",
                    payload={
                        "id": "too-large",
                        "image": "busybox",
                        "cpus": 5,
                        "memory_mb": 512,
                        "disk_mb": 1024,
                    },
                    allow_error=True,
                )
                demand = self._json_request(f"{base}/v1/demand")
            finally:
                gateway.shutdown()
                gateway.server_close()

        self.assertEqual(result["status"], 422)
        self.assertEqual(result["body"]["error_code"], "sandbox_shape_unschedulable")
        self.assertFalse(result["body"]["retryable"])
        self.assertEqual(demand["pending"], [])

    def test_gateway_prepares_capacity_as_expiring_demand_signal(self) -> None:
        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            gateway = build_server(
                "127.0.0.1",
                0,
                raw_path / "heartbeats.json",
                routing_file=raw_path / "routes.sqlite",
            )
            gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
            gateway_thread.start()
            try:
                host, port = gateway.server_address
                base = f"http://{host}:{port}"
                prepared = self._json_request(
                    f"{base}/v1/capacity/prepare",
                    method="POST",
                    payload={
                        "id": "eval-soon",
                        "count": 8,
                        "cpus": 1,
                        "memory_mb": 2048,
                        "disk_mb": 10_240,
                        "ttl_seconds": 600,
                    },
                )
                listed = self._json_request(f"{base}/v1/capacity/prepare")
                demand = self._json_request(f"{base}/v1/demand")
                deleted = self._json_request(
                    f"{base}/v1/capacity/prepare/eval-soon",
                    method="DELETE",
                )
                demand_after_delete = self._json_request(f"{base}/v1/demand")
            finally:
                gateway.shutdown()
                gateway.server_close()

        self.assertEqual(prepared["prepare"]["prepare_id"], "eval-soon")
        self.assertEqual(prepared["prepare"]["count"], 8)
        self.assertEqual(prepared["prepare"]["total_resources"]["vcpu"], 8.0)
        self.assertEqual(prepared["demand"]["pending_resources"]["vcpu"], 0.0)
        self.assertEqual(prepared["demand"]["prepared_resources"]["vcpu"], 8.0)
        self.assertEqual(prepared["demand"]["desired_resources"]["memory_mb"], 16_384)
        self.assertEqual(listed["prepared"][0]["prepare_id"], "eval-soon")
        self.assertEqual(demand["prepared_resources"]["disk_mb"], 81_920)
        self.assertEqual(demand["prepared"][0]["prepare_id"], "eval-soon")
        self.assertTrue(deleted["ok"])
        self.assertEqual(deleted["deleted"]["prepare_id"], "eval-soon")
        self.assertEqual(demand_after_delete["prepared_resources"]["vcpu"], 0.0)
        self.assertEqual(demand_after_delete["prepared"], [])

    def test_gateway_normalizes_parkable_prepared_capacity_for_exact_claim(
        self,
    ) -> None:
        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            route_file = raw_path / "routes.sqlite"
            gateway = build_server(
                "127.0.0.1",
                0,
                raw_path / "heartbeats.json",
                routing_file=route_file,
            )
            gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
            gateway_thread.start()
            try:
                host, port = gateway.server_address
                base = f"http://{host}:{port}"
                prepared = self._json_request(
                    f"{base}/v1/capacity/prepare",
                    method="POST",
                    payload={
                        "id": "parkable-eval",
                        "count": 1,
                        "cpus": 0.25,
                        "memory_mb": 128,
                        "disk_mb": 64,
                        "parkable": True,
                        "ttl_seconds": 600,
                    },
                )
                spec = SandboxSpec.from_dict(
                    {
                        "id": "parkable-one",
                        "image": "busybox",
                        "cpus": 0.25,
                        "memory_mb": 128,
                        "disk_mb": 64,
                        "parkable": True,
                    }
                )
                resources = spec.requested_resources()
                store = RoutingStore(route_file)
                store.allocate_sandbox_create(
                    SandboxRoute(
                        sandbox_id=spec.id,
                        node_id="node-1",
                        job_id="job-1",
                        node_url="http://node-1:8090",
                        resources=resources,
                        spec=spec.to_dict(),
                    ),
                    spec_hash="parkable-spec",
                )
                remaining = store.prepared_capacity()
            finally:
                gateway.shutdown()
                gateway.server_close()

        self.assertEqual(
            prepared["prepare"]["resources"],
            {"vcpu": 0.25, "memory_mb": 128, "disk_mb": 1280},
        )
        self.assertEqual(resources.disk_mb, 1280)
        self.assertEqual(remaining, [])

    def test_gateway_pins_managed_registry_image_before_capacity_warmup(self) -> None:
        digest = "sha256:" + "d" * 64

        class RegistryHandler(BaseHTTPRequestHandler):
            def do_HEAD(self) -> None:
                self.send_response(200)
                self.send_header("Docker-Content-Digest", digest)
                self.end_headers()

            def log_message(self, format: str, *args: object) -> None:
                del format, args

        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            registry = ThreadingHTTPServer(("127.0.0.1", 0), RegistryHandler)
            Thread(target=registry.serve_forever, daemon=True).start()
            try:
                registry_host, registry_port = registry.server_address
                gateway = build_server(
                    "127.0.0.1",
                    0,
                    raw_path / "heartbeats.json",
                    routing_file=raw_path / "routes.sqlite",
                    registry_url=f"http://{registry_host}:{registry_port}",
                )
                Thread(target=gateway.serve_forever, daemon=True).start()
                try:
                    host, port = gateway.server_address
                    prepared = self._json_request(
                        f"http://{host}:{port}/v1/capacity/prepare",
                        method="POST",
                        payload={
                            "id": "digest-warmup",
                            "count": 1,
                            "ttl_seconds": 60,
                            "image": ("ucloud-sandbox-registry:5000/team/image:v1"),
                            "cpus": 1,
                            "memory_mb": 512,
                        },
                    )
                finally:
                    gateway.shutdown()
                    gateway.server_close()
            finally:
                registry.shutdown()
                registry.server_close()

        expected = "ucloud-sandbox-registry:5000/team/image:v1" f"@{digest}"
        self.assertEqual(prepared["prepare"]["image"], expected)
        self.assertEqual(prepared["image_warmup"]["image"], expected)

    def test_managed_mutable_tag_is_not_a_digest_cache_hit(self) -> None:
        mutable = "ucloud-sandbox-registry:5000/team/image:v1"
        digest = "sha256:" + "e" * 64
        heartbeat = build_heartbeat(
            job_id="job-1",
            node_id="node-1",
            cached_images=(mutable,),
        )

        self.assertEqual(
            control_plane._requested_image_cache_keys(
                mutable,
                "image-id",
                require_digest=True,
            ),
            set(),
        )
        self.assertFalse(
            control_plane._heartbeat_has_image(
                heartbeat,
                mutable,
                "image-id",
                require_digest=True,
            )
        )
        self.assertEqual(
            control_plane._requested_image_cache_keys(
                f"{mutable}@{digest}",
                "image-id",
                require_digest=True,
            ),
            {
                f"{mutable}@{digest}",
                f"ucloud-sandbox-registry:5000/team/image@{digest}",
            },
        )

    def test_registry_resolution_failure_does_not_trust_mutable_tag_heartbeat(
        self,
    ) -> None:
        mutable = "ucloud-sandbox-registry:5000/team/image:v1"

        class ImageNode(BaseHTTPRequestHandler):
            pull_count = 0

            def do_GET(self) -> None:
                if self.path == "/v1/images":
                    self._write_json(
                        {
                            "images": [
                                {
                                    "id": "image-id",
                                    "tag": mutable,
                                    "source": "registry",
                                    "state": "available",
                                }
                            ]
                        }
                    )
                    return
                self.send_response(404)
                self.end_headers()

            def do_POST(self) -> None:
                if self.path == "/v1/images/pull":
                    type(self).pull_count += 1
                    self._write_json({"error": "registry unavailable"}, status=503)
                    return
                self.send_response(404)
                self.end_headers()

            def _write_json(
                self,
                payload: dict[str, object],
                *,
                status: int = 200,
            ) -> None:
                body = json.dumps(payload).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, format: str, *args: object) -> None:
                del format, args

        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            node = ThreadingHTTPServer(("127.0.0.1", 0), ImageNode)
            Thread(target=node.serve_forever, daemon=True).start()
            try:
                node_host, node_port = node.server_address
                gateway = build_server(
                    "127.0.0.1",
                    0,
                    raw_path / "heartbeats.json",
                    routing_file=raw_path / "routes.sqlite",
                    registry_url="http://127.0.0.1:9",
                )
                Thread(target=gateway.serve_forever, daemon=True).start()
                try:
                    host, port = gateway.server_address
                    base = f"http://{host}:{port}"
                    self.assertEqual(
                        post_heartbeat(
                            f"{base}/v1/nodes/heartbeat",
                            build_heartbeat(
                                job_id="job-1",
                                node_id="node-1",
                                node_url=f"http://{node_host}:{node_port}",
                                capabilities=("sandbox", "image-cache"),
                                cached_images=(mutable,),
                                total_resources=ResourceQuantity(
                                    vcpu=2,
                                    memory_mb=2048,
                                ),
                            ),
                        ).status,
                        200,
                    )
                    with patch.object(
                        control_plane.RegistryClient,
                        "manifest_digest",
                        side_effect=OSError("registry unavailable"),
                    ):
                        created = self._json_request(
                            f"{base}/v1/sandboxes",
                            method="POST",
                            payload={
                                "id": "mutable-cache",
                                "image": mutable,
                                "cpus": 1,
                                "memory_mb": 512,
                            },
                            allow_error=True,
                        )
                finally:
                    gateway.shutdown()
                    gateway.server_close()
            finally:
                node.shutdown()
                node.server_close()

        self.assertEqual(created["status"], 502)
        self.assertEqual(ImageNode.pull_count, 1)

    def test_gateway_prepares_capacity_with_image_prewarm(self) -> None:
        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            nodes = [
                build_node_agent_server(
                    "127.0.0.1",
                    0,
                    sandbox_file=raw_path / f"node-{index}-sandboxes.json",
                    image_file=raw_path / f"node-{index}-images.json",
                    job_id=f"job-{index}",
                    node_id=f"node-{index}",
                    total_resources=ResourceQuantity(
                        vcpu=4, memory_mb=8192, disk_mb=100_000
                    ),
                    runtime=DockerGvisorRuntime(
                        dry_run=True, allow_storage_opt_quota=True
                    ),
                )
                for index in range(2)
            ]
            node_threads = [
                Thread(target=node.serve_forever, daemon=True) for node in nodes
            ]
            for thread in node_threads:
                thread.start()
            try:
                gateway = build_server(
                    "127.0.0.1",
                    0,
                    raw_path / "heartbeats.json",
                    routing_file=raw_path / "routes.sqlite",
                )
                gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
                gateway_thread.start()
                try:
                    host, port = gateway.server_address
                    base = f"http://{host}:{port}"
                    for index, node in enumerate(nodes):
                        node_host, node_port = node.server_address
                        result = post_heartbeat(
                            f"{base}/v1/nodes/heartbeat",
                            build_heartbeat(
                                job_id=f"job-{index}",
                                node_id=f"node-{index}",
                                node_url=f"http://{node_host}:{node_port}",
                                capabilities=("sandbox", "image-cache", "disk-quota"),
                                total_resources=ResourceQuantity(
                                    vcpu=4,
                                    memory_mb=8192,
                                    disk_mb=100_000,
                                ),
                            ),
                        )
                        self.assertEqual(result.status, 200)
                    prepared = self._json_request(
                        f"{base}/v1/capacity/prepare",
                        method="POST",
                        payload={
                            "id": "eval-soon",
                            "count": 2,
                            "cpus": 1,
                            "memory_mb": 1024,
                            "disk_mb": 2048,
                            "image": "busybox:latest",
                        },
                    )
                    deadline = utc_now() + timedelta(seconds=5)
                    warmed_tags: list[str] = []
                    while utc_now() < deadline:
                        node_images = [
                            self._json_request(
                                f"http://{node.server_address[0]}:{node.server_address[1]}/v1/images"
                            )
                            for node in nodes
                        ]
                        warmed_tags = [
                            image["tag"]
                            for payload in node_images
                            for image in payload["images"]
                        ]
                        if "busybox:latest" in warmed_tags:
                            break
                        sleep(0.05)
                finally:
                    gateway.shutdown()
                    gateway.server_close()
            finally:
                for node in nodes:
                    node.shutdown()
                    node.server_close()

        self.assertEqual(prepared["prepare"]["image"], "busybox:latest")
        self.assertEqual(prepared["image_warmup"]["image"], "busybox:latest")
        self.assertEqual(prepared["image_prewarm"]["scheduled"], 1)
        self.assertIn("busybox:latest", warmed_tags)
        self.assertEqual(warmed_tags.count("busybox:latest"), 1)

    def test_gateway_prepare_image_warmup_runs_after_future_node_heartbeat(
        self,
    ) -> None:
        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            node = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=raw_path / "future-node-sandboxes.json",
                image_file=raw_path / "future-node-images.json",
                job_id="future-job",
                node_id="future-node",
                total_resources=ResourceQuantity(
                    vcpu=4, memory_mb=8192, disk_mb=100_000
                ),
                runtime=DockerGvisorRuntime(dry_run=True, allow_storage_opt_quota=True),
                node_control_bearer_token="node-secret",
            )
            node_thread = Thread(target=node.serve_forever, daemon=True)
            node_thread.start()
            try:
                gateway = build_server(
                    "127.0.0.1",
                    0,
                    raw_path / "heartbeats.json",
                    routing_file=raw_path / "routes.sqlite",
                    node_control_bearer_token="node-secret",
                )
                gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
                gateway_thread.start()
                try:
                    host, port = gateway.server_address
                    base = f"http://{host}:{port}"
                    prepared = self._json_request(
                        f"{base}/v1/capacity/prepare",
                        method="POST",
                        payload={
                            "id": "future-eval",
                            "count": 2,
                            "cpus": 1,
                            "memory_mb": 1024,
                            "disk_mb": 2048,
                            "image": "busybox:latest",
                        },
                    )
                    node_host, node_port = node.server_address
                    heartbeat_result = post_heartbeat(
                        f"{base}/v1/nodes/heartbeat",
                        build_heartbeat(
                            job_id="future-job",
                            node_id="future-node",
                            node_url=f"http://{node_host}:{node_port}",
                            capabilities=("sandbox", "image-cache", "disk-quota"),
                            total_resources=ResourceQuantity(
                                vcpu=4,
                                memory_mb=8192,
                                disk_mb=100_000,
                            ),
                        ),
                    )
                    deadline = utc_now() + timedelta(seconds=5)
                    warmed_tags: list[str] = []
                    while utc_now() < deadline:
                        payload = self._json_request(
                            f"http://{node_host}:{node_port}/v1/images",
                            headers={"Authorization": "Bearer node-secret"},
                        )
                        warmed_tags = [image["tag"] for image in payload["images"]]
                        if "busybox:latest" in warmed_tags:
                            break
                        sleep(0.05)
                    demand = self._json_request(f"{base}/v1/demand")
                finally:
                    gateway.shutdown()
                    gateway.server_close()
            finally:
                node.shutdown()
                node.server_close()

        self.assertEqual(prepared["image_prewarm"]["scheduled"], 0)
        self.assertEqual(heartbeat_result.status, 200)
        self.assertIn("busybox:latest", warmed_tags)
        self.assertEqual(demand["image_warmups"], [])

    def test_gateway_image_pull_warms_multiple_sandbox_nodes(self) -> None:
        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            nodes = [
                build_node_agent_server(
                    "127.0.0.1",
                    0,
                    sandbox_file=raw_path / f"pull-node-{index}-sandboxes.json",
                    image_file=raw_path / f"pull-node-{index}-images.json",
                    job_id=f"pull-job-{index}",
                    node_id=f"pull-node-{index}",
                    total_resources=ResourceQuantity(
                        vcpu=4, memory_mb=8192, disk_mb=100_000
                    ),
                    runtime=DockerGvisorRuntime(
                        dry_run=True, allow_storage_opt_quota=True
                    ),
                )
                for index in range(2)
            ]
            for node in nodes:
                Thread(target=node.serve_forever, daemon=True).start()
            try:
                gateway = build_server(
                    "127.0.0.1",
                    0,
                    raw_path / "heartbeats.json",
                    routing_file=raw_path / "routes.sqlite",
                )
                gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
                gateway_thread.start()
                try:
                    host, port = gateway.server_address
                    base = f"http://{host}:{port}"
                    for index, node in enumerate(nodes):
                        node_host, node_port = node.server_address
                        post_heartbeat(
                            f"{base}/v1/nodes/heartbeat",
                            build_heartbeat(
                                job_id=f"pull-job-{index}",
                                node_id=f"pull-node-{index}",
                                node_url=f"http://{node_host}:{node_port}",
                                capabilities=("sandbox", "image-cache", "disk-quota"),
                                total_resources=ResourceQuantity(
                                    vcpu=4,
                                    memory_mb=8192,
                                    disk_mb=100_000,
                                ),
                            ),
                        )
                    pulled = self._json_request(
                        f"{base}/v1/images/pull",
                        method="POST",
                        payload={
                            "image": "busybox:latest",
                            "id": "busybox",
                            "count": 2,
                            "cpus": 1,
                            "memory_mb": 512,
                        },
                    )
                finally:
                    gateway.shutdown()
                    gateway.server_close()
            finally:
                for node in nodes:
                    node.shutdown()
                    node.server_close()

        self.assertEqual(pulled["image"]["id"], "busybox")
        self.assertEqual(pulled["requested"], 2)
        self.assertEqual(pulled["ready"], 2)
        self.assertEqual(
            sorted(item["node"]["node_id"] for item in pulled["pulled"]),
            ["pull-node-0", "pull-node-1"],
        )

    def test_gateway_uses_bounded_proxy_timeout_for_image_pulls(self) -> None:
        class FakeResponse:
            status = 201
            headers: dict[str, str] = {}

            def __enter__(self) -> "FakeResponse":
                return self

            def __exit__(self, *args: object) -> None:
                return None

            def read(self) -> bytes:
                return json.dumps(
                    {
                        "image": {
                            "id": "large-image",
                            "tag": "registry.example.org/large:latest",
                            "state": "available",
                            "pushed": True,
                        },
                        "command": ["docker", "pull"],
                        "exitCode": 0,
                    }
                ).encode("utf-8")

        captured_timeouts: list[object] = []

        def fake_urlopen(req: object, timeout: object = None) -> FakeResponse:
            del req
            captured_timeouts.append(timeout)
            return FakeResponse()

        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            gateway = build_server(
                "127.0.0.1",
                0,
                raw_path / "heartbeats.json",
                routing_file=raw_path / "routes.sqlite",
            )
            gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
            gateway_thread.start()
            try:
                host, port = gateway.server_address
                base = f"http://{host}:{port}"
                post_heartbeat(
                    f"{base}/v1/nodes/heartbeat",
                    build_heartbeat(
                        job_id="pull-job",
                        node_id="pull-node",
                        node_url="http://pull-node.invalid:8090",
                        capabilities=("sandbox", "image-cache", "disk-quota"),
                        cached_images=(),
                        total_resources=ResourceQuantity(
                            vcpu=4,
                            memory_mb=8192,
                            disk_mb=100_000,
                        ),
                    ),
                )

                with patch.object(control_plane.request, "urlopen", fake_urlopen):
                    body = json.dumps(
                        {
                            "image": "registry.example.org/large:latest",
                            "id": "large-image",
                            "count": 1,
                        }
                    )
                    conn = HTTPConnection(host, port, timeout=5)
                    try:
                        conn.request(
                            "POST",
                            "/v1/images/pull",
                            body=body,
                            headers={"Content-Type": "application/json"},
                        )
                        response = conn.getresponse()
                        pulled = json.loads(response.read().decode("utf-8"))
                    finally:
                        conn.close()
            finally:
                gateway.shutdown()
                gateway.server_close()

        self.assertEqual(response.status, 200)
        self.assertEqual(pulled["ready"], 1)
        self.assertEqual(IMAGE_PULL_PROXY_TIMEOUT_SECONDS, 30 * 60)
        self.assertEqual(captured_timeouts, [IMAGE_PULL_PROXY_TIMEOUT_SECONDS])

    def test_gateway_reports_image_pull_failure_when_ready_nodes_fail(self) -> None:
        class FakeResponse:
            status = 502
            headers: dict[str, str] = {}

            def __enter__(self) -> "FakeResponse":
                return self

            def __exit__(self, *args: object) -> None:
                return None

            def read(self) -> bytes:
                return json.dumps({"error": "node request failed: timed out"}).encode(
                    "utf-8"
                )

        def fake_urlopen(req: object, timeout: object = None) -> FakeResponse:
            del req, timeout
            return FakeResponse()

        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            gateway = build_server(
                "127.0.0.1",
                0,
                raw_path / "heartbeats.json",
                routing_file=raw_path / "routes.sqlite",
            )
            gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
            gateway_thread.start()
            try:
                host, port = gateway.server_address
                base = f"http://{host}:{port}"
                post_heartbeat(
                    f"{base}/v1/nodes/heartbeat",
                    build_heartbeat(
                        job_id="pull-job",
                        node_id="pull-node",
                        node_url="http://pull-node.invalid:8090",
                        capabilities=("sandbox", "image-cache", "disk-quota"),
                        cached_images=(),
                        total_resources=ResourceQuantity(
                            vcpu=4,
                            memory_mb=8192,
                            disk_mb=100_000,
                        ),
                    ),
                )

                with patch.object(control_plane.request, "urlopen", fake_urlopen):
                    body = json.dumps(
                        {
                            "image": "registry.example.org/large:latest",
                            "id": "large-image",
                            "count": 1,
                        }
                    )
                    conn = HTTPConnection(host, port, timeout=5)
                    try:
                        conn.request(
                            "POST",
                            "/v1/images/pull",
                            body=body,
                            headers={"Content-Type": "application/json"},
                        )
                        response = conn.getresponse()
                        failed = json.loads(response.read().decode("utf-8"))
                    finally:
                        conn.close()
            finally:
                gateway.shutdown()
                gateway.server_close()

        self.assertEqual(response.status, 503)
        self.assertEqual(
            failed["error"], "image pull failed on ready image-cache nodes"
        )
        self.assertEqual(failed["result"]["ready"], 0)
        self.assertEqual(
            failed["result"]["failed"][0]["error"],
            "node request failed: timed out",
        )

    def test_gateway_rejects_invalid_prepared_capacity_resources(self) -> None:
        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            gateway = build_server(
                "127.0.0.1",
                0,
                raw_path / "heartbeats.json",
                routing_file=raw_path / "routes.sqlite",
            )
            gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
            gateway_thread.start()
            try:
                host, port = gateway.server_address
                base = f"http://{host}:{port}"
                zero = self._json_request(
                    f"{base}/v1/capacity/prepare",
                    method="POST",
                    payload={"count": 1, "cpus": 0, "memory_mb": 0, "disk_mb": 0},
                    allow_error=True,
                )
                negative = self._json_request(
                    f"{base}/v1/capacity/prepare",
                    method="POST",
                    payload={"count": 1, "cpus": -1, "memory_mb": 1024},
                    allow_error=True,
                )
                non_finite = self._json_request(
                    f"{base}/v1/capacity/prepare",
                    method="POST",
                    payload={"count": 1, "cpus": "NaN", "memory_mb": 1024},
                    allow_error=True,
                )
                parkable_without_memory = self._json_request(
                    f"{base}/v1/capacity/prepare",
                    method="POST",
                    payload={"count": 1, "disk_mb": 64, "parkable": True},
                    allow_error=True,
                )
                demand = self._json_request(f"{base}/v1/demand")
            finally:
                gateway.shutdown()
                gateway.server_close()

        self.assertEqual(zero["status"], 400)
        self.assertIn("resources are required", zero["body"]["error"])
        self.assertEqual(negative["status"], 400)
        self.assertIn("vcpu must be non-negative", negative["body"]["error"])
        self.assertEqual(non_finite["status"], 400)
        self.assertIn("vcpu must be non-negative", non_finite["body"]["error"])
        self.assertEqual(parkable_without_memory["status"], 400)
        self.assertIn(
            "parkable prepared capacity requires memory_mb",
            parkable_without_memory["body"]["error"],
        )
        self.assertEqual(demand["prepared_resources"]["vcpu"], 0.0)

    def test_gateway_prepares_builder_capacity_as_expiring_demand_signal(self) -> None:
        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            gateway = build_server(
                "127.0.0.1",
                0,
                raw_path / "heartbeats.json",
                routing_file=raw_path / "routes.sqlite",
            )
            gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
            gateway_thread.start()
            try:
                host, port = gateway.server_address
                base = f"http://{host}:{port}"
                prepared = self._json_request(
                    f"{base}/v1/builders/prepare",
                    method="POST",
                    payload={
                        "id": "builds-soon",
                        "count": 2,
                        "ttl_seconds": 600,
                    },
                )
                listed = self._json_request(f"{base}/v1/builders/prepare")
                demand = self._json_request(f"{base}/v1/demand")
                deleted = self._json_request(
                    f"{base}/v1/builders/prepare/builds-soon",
                    method="DELETE",
                )
                demand_after_delete = self._json_request(f"{base}/v1/demand")
            finally:
                gateway.shutdown()
                gateway.server_close()

        self.assertEqual(prepared["prepare"]["prepare_id"], "builds-soon")
        self.assertEqual(prepared["prepare"]["count"], 2)
        self.assertEqual(prepared["demand"]["prepared_builder_count"], 2)
        self.assertEqual(prepared["demand"]["desired_builders"], 2)
        self.assertEqual(listed["prepared_builders"][0]["prepare_id"], "builds-soon")
        self.assertEqual(demand["prepared_builders"][0]["count"], 2)
        self.assertTrue(deleted["ok"])
        self.assertEqual(deleted["deleted"]["prepare_id"], "builds-soon")
        self.assertEqual(demand_after_delete["prepared_builder_count"], 0)
        self.assertEqual(demand_after_delete["desired_builders"], 0)

    def test_gateway_rejects_invalid_prepared_builder_count(self) -> None:
        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            gateway = build_server(
                "127.0.0.1",
                0,
                raw_path / "heartbeats.json",
                routing_file=raw_path / "routes.sqlite",
            )
            gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
            gateway_thread.start()
            try:
                host, port = gateway.server_address
                rejected = self._json_request(
                    f"http://{host}:{port}/v1/builders/prepare",
                    method="POST",
                    payload={"id": "bad", "count": 0},
                    allow_error=True,
                )
                demand = self._json_request(f"http://{host}:{port}/v1/demand")
            finally:
                gateway.shutdown()
                gateway.server_close()

        self.assertEqual(rejected["status"], 400)
        self.assertIn("count must be positive", rejected["body"]["error"])
        self.assertEqual(demand["prepared_builder_count"], 0)

    def test_gateway_metrics_records_scaleup_wait_after_pending_sandbox_is_placed(
        self,
    ) -> None:
        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            node = build_node_agent_server(
                "127.0.0.1",
                0,
                sandbox_file=raw_path / "node-sandboxes.json",
                image_file=raw_path / "node-images.json",
                job_id="job-1",
                node_id="node-1",
                total_resources=ResourceQuantity(
                    vcpu=4, memory_mb=8192, disk_mb=100_000
                ),
                runtime=DockerGvisorRuntime(dry_run=True, allow_storage_opt_quota=True),
            )
            node_thread = Thread(target=node.serve_forever, daemon=True)
            node_thread.start()
            try:
                node_host, node_port = node.server_address
                gateway = build_server(
                    "127.0.0.1",
                    0,
                    raw_path / "heartbeats.json",
                    routing_file=raw_path / "routes.json",
                    metrics_file=raw_path / "metrics.jsonl",
                )
                gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
                gateway_thread.start()
                try:
                    host, port = gateway.server_address
                    base = f"http://{host}:{port}"
                    rejected = self._json_request(
                        f"{base}/v1/sandboxes",
                        method="POST",
                        payload={
                            "id": "scale-one",
                            "image": "busybox",
                            "cpus": 1,
                            "memory_mb": 512,
                            "disk_mb": 1024,
                        },
                        allow_error=True,
                    )
                    result = post_heartbeat(
                        f"{base}/v1/nodes/heartbeat",
                        build_heartbeat(
                            job_id="job-1",
                            node_id="node-1",
                            node_url=f"http://{node_host}:{node_port}",
                            capabilities=("sandbox", "image-cache", "disk-quota"),
                            total_resources=ResourceQuantity(
                                vcpu=4,
                                memory_mb=8192,
                                disk_mb=100_000,
                            ),
                        ),
                    )
                    self.assertEqual(result.status, 200)
                    created = self._json_request(
                        f"{base}/v1/sandboxes",
                        method="POST",
                        payload={
                            "id": "scale-one",
                            "image": "busybox",
                            "cpus": 1,
                            "memory_mb": 512,
                            "disk_mb": 1024,
                        },
                    )
                    metrics = self._json_request(f"{base}/v1/metrics")
                finally:
                    gateway.shutdown()
                    gateway.server_close()
            finally:
                node.shutdown()
                node.server_close()

        self.assertEqual(rejected["status"], 503)
        self.assertEqual(created["sandbox"]["spec"]["id"], "scale-one")
        self.assertEqual(metrics["nodes"]["fresh"], 1)
        self.assertEqual(metrics["sandboxes"]["active_routes"], 1)
        self.assertEqual(metrics["sandboxes"]["pending"], 0)
        self.assertEqual(metrics["scale_up"]["samples"], 1)
        self.assertEqual(
            metrics["scale_up"]["recent"][0]["data"]["sandbox_id"],
            "scale-one",
        )
        self.assertTrue(metrics["scale_up"]["recent"][0]["data"]["had_pending_demand"])

    def test_atomic_allocation_spec_conflict_never_dispatches_to_node(self) -> None:
        class CountingNode(BaseHTTPRequestHandler):
            creates = 0

            def do_GET(self) -> None:
                body = json.dumps({"sandboxes": []}).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_POST(self) -> None:
                type(self).creates += 1
                self.send_response(500)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def log_message(self, format: str, *args: object) -> None:
                del format, args

        with TemporaryDirectory() as raw_dir:
            raw_path = Path(raw_dir)
            node = ThreadingHTTPServer(("127.0.0.1", 0), CountingNode)
            Thread(target=node.serve_forever, daemon=True).start()
            gateway = build_server(
                "127.0.0.1",
                0,
                raw_path / "heartbeats.json",
                routing_file=raw_path / "routes.sqlite",
            )
            Thread(target=gateway.serve_forever, daemon=True).start()
            try:
                node_host, node_port = node.server_address
                host, port = gateway.server_address
                base = f"http://{host}:{port}"
                self.assertEqual(
                    post_heartbeat(
                        f"{base}/v1/nodes/heartbeat",
                        build_heartbeat(
                            job_id="job-1",
                            node_id="node-1",
                            node_url=f"http://{node_host}:{node_port}",
                            capabilities=("sandbox", "image-cache", "disk-quota"),
                            cached_images=("busybox",),
                            total_resources=ResourceQuantity(
                                vcpu=4, memory_mb=4096, disk_mb=8192
                            ),
                        ),
                    ).status,
                    200,
                )
                with patch.object(
                    RoutingStore,
                    "allocate_sandbox_create_with_pending",
                    side_effect=SandboxRouteConflictError("different spec"),
                ):
                    response = self._json_request(
                        f"{base}/v1/sandboxes",
                        method="POST",
                        payload={
                            "id": "raced",
                            "image": "busybox",
                            "cpus": 1,
                            "memory_mb": 512,
                            "disk_mb": 1024,
                        },
                        allow_error=True,
                    )
            finally:
                gateway.shutdown()
                gateway.server_close()
                node.shutdown()
                node.server_close()

        self.assertEqual(response["status"], 409)
        self.assertEqual(CountingNode.creates, 0)

    def test_node_capacity_unions_heartbeat_reservations_without_route_duplicates(
        self,
    ) -> None:
        heartbeat = build_heartbeat(
            job_id="job-1",
            node_id="node-1",
            node_url="http://node-1:8090",
            total_resources=ResourceQuantity(vcpu=10, memory_mb=10_000, disk_mb=10_000),
            used_resources=ResourceQuantity(vcpu=2, memory_mb=2_000, disk_mb=2_000),
            reserved_resources=ResourceQuantity(vcpu=1, memory_mb=1_000, disk_mb=1_000),
            build_reserved_resources=ResourceQuantity(
                vcpu=1, memory_mb=1_000, disk_mb=1_000
            ),
            inventory=(
                SandboxInventoryEntry(
                    sandbox_id="represented",
                    generation=2,
                    operation_id="create-2",
                    spec_hash="hash-2",
                ),
            ),
            inventory_complete=True,
        )
        routes = [
            SandboxRoute(
                sandbox_id="represented",
                node_id="node-1",
                job_id="job-1",
                node_url="http://node-1:8090",
                resources=ResourceQuantity(vcpu=2, memory_mb=2_000, disk_mb=2_000),
                generation=2,
                create_operation_id="create-2",
                spec_hash="hash-2",
            ),
            SandboxRoute(
                sandbox_id="control-only",
                node_id="node-1",
                job_id="job-1",
                node_url="http://node-1:8090",
                resources=ResourceQuantity(vcpu=3, memory_mb=3_000, disk_mb=3_000),
                generation=3,
                create_operation_id="create-3",
                spec_hash="hash-3",
            ),
        ]
        routes.append(routes[-1])  # Persisted route plus process-local in-flight copy.

        available = control_plane._node_available_resources(heartbeat, routes)

        self.assertEqual(
            available,
            ResourceQuantity(vcpu=3, memory_mb=3_000, disk_mb=3_000),
        )

    def test_node_capacity_clamps_route_accounting_to_storage_authority(self) -> None:
        heartbeat = build_heartbeat(
            job_id="job-storage",
            node_id="node-storage",
            node_url="http://node-storage:8090",
            capabilities=("disk-quota", "storage-native-v1"),
            total_resources=ResourceQuantity(
                vcpu=32,
                memory_mb=98_304,
                disk_mb=1_449_984,
            ),
            used_resources=ResourceQuantity(disk_mb=17_472),
            runtime_metrics=NodeRuntimeMetrics(
                collected_at=utc_now(),
                storage_hard_capacity_mb=1_449_984,
                storage_hard_reserved_mb=1_441_600,
            ),
        )

        available = control_plane._node_available_resources(heartbeat, [])

        self.assertEqual(available.disk_mb, 8_384)
        self.assertEqual(available.vcpu, 32)
        self.assertEqual(available.memory_mb, 98_304)

    def test_control_plane_rejects_unsupported_or_oversized_request_framing(
        self,
    ) -> None:
        with TemporaryDirectory() as raw_dir:
            server = build_server(
                "127.0.0.1",
                0,
                Path(raw_dir) / "heartbeats.json",
            )
            Thread(target=server.serve_forever, daemon=True).start()
            try:
                host, port = server.server_address

                def rejected(headers: dict[str, str]) -> int:
                    connection = HTTPConnection(host, port, timeout=5)
                    connection.putrequest("POST", "/v1/nodes/heartbeat")
                    for key, value in headers.items():
                        connection.putheader(key, value)
                    connection.endheaders()
                    response = connection.getresponse()
                    response.read()
                    connection.close()
                    return response.status

                chunked = rejected(
                    {"Transfer-Encoding": "chunked", "Content-Length": "0"}
                )
                missing_length = rejected({})
                oversized = rejected(
                    {
                        "Content-Length": str(
                            control_plane.DEFAULT_MAX_JSON_BODY_BYTES + 1
                        )
                    }
                )
            finally:
                server.shutdown()
                server.server_close()

        self.assertEqual(chunked, 400)
        self.assertEqual(missing_length, 400)
        self.assertEqual(oversized, 400)

    def test_gateway_commits_journaled_parked_migration_between_nodes(self) -> None:
        calls: list[tuple[str, str]] = []

        def node_handler(role: str):
            class MigrationNode(BaseHTTPRequestHandler):
                def do_POST(self) -> None:
                    length = int(self.headers.get("Content-Length") or 0)
                    raw = json.loads(self.rfile.read(length) or b"{}")
                    calls.append((role, self.path))
                    if self.path.endswith("/migration/prepare"):
                        payload = {
                            "migration": {
                                "archive_sha256": "a" * 64,
                                "archive_token": "archive-token",
                                "migration_id": raw["migration_id"],
                                "sandbox_id": "moving-one",
                            }
                        }
                    elif self.path == "/v1/images/pull":
                        payload = {"image": {"tag": raw["image"]}}
                    elif self.path == "/v1/migrations/import":
                        self.assert_source_url(raw["source_url"])
                        payload = {"sandbox": {"state": "import_ready"}}
                    elif self.path.endswith("/migration/activate"):
                        payload = {"sandbox": {"state": "parked"}}
                    elif self.path.endswith("/migration/finalize"):
                        payload = {"ok": True}
                    else:
                        self.send_error(404)
                        return
                    body = json.dumps(payload).encode("utf-8")
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)

                @staticmethod
                def assert_source_url(source_url: str) -> None:
                    if (
                        "/v1/sandboxes/moving-one/migration/archive"
                        not in source_url
                    ):
                        raise AssertionError("gateway supplied the wrong source URL")

                def log_message(self, format: str, *args: object) -> None:
                    del format, args

            return MigrationNode

        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            source_node = ThreadingHTTPServer(
                ("127.0.0.1", 0),
                node_handler("source"),
            )
            destination_node = ThreadingHTTPServer(
                ("127.0.0.1", 0),
                node_handler("destination"),
            )
            source_thread = Thread(target=source_node.serve_forever, daemon=True)
            destination_thread = Thread(
                target=destination_node.serve_forever,
                daemon=True,
            )
            source_thread.start()
            destination_thread.start()
            source_host, source_port = source_node.server_address
            destination_host, destination_port = destination_node.server_address
            source_url = f"http://{source_host}:{source_port}"
            destination_url = f"http://{destination_host}:{destination_port}"
            heartbeat_store = HeartbeatStore(root / "heartbeats.json")
            total = ResourceQuantity(vcpu=32, memory_mb=96_000, disk_mb=50_000)
            heartbeat_store.upsert(
                build_heartbeat(
                    job_id="source-job",
                    node_id="source-node",
                    node_url=source_url,
                    capabilities=(
                        "sandbox",
                        "disk-quota",
                        "sandbox-migrate-v2",
                    ),
                    total_resources=total,
                    used_resources=ResourceQuantity(disk_mb=4096),
                    inventory_complete=True,
                )
            )
            heartbeat_store.upsert(
                build_heartbeat(
                    job_id="destination-job",
                    node_id="destination-node",
                    node_url=destination_url,
                    capabilities=(
                        "sandbox",
                        "disk-quota",
                        "sandbox-migrate-v2",
                    ),
                    total_resources=total,
                    cached_images=(),
                    inventory_complete=True,
                )
            )
            route_file = root / "routes.sqlite"
            routing = RoutingStore(route_file)
            route = routing.allocate_sandbox_create(
                SandboxRoute(
                    sandbox_id="moving-one",
                    node_id="source-node",
                    job_id="source-job",
                    node_url=source_url,
                    resources=ResourceQuantity(
                        vcpu=1,
                        memory_mb=1024,
                        disk_mb=4096,
                    ),
                    spec={"id": "moving-one", "image": "busybox"},
                ),
                spec_hash="spec-hash",
                create_operation_id="create-operation",
            )
            routing.finalize_sandbox_create(replace(route, state="parked"))
            gateway = build_server(
                "127.0.0.1",
                0,
                heartbeat_store.path,
                routing_file=route_file,
            )
            gateway_thread = Thread(target=gateway.serve_forever, daemon=True)
            gateway_thread.start()
            try:
                gateway_host, gateway_port = gateway.server_address
                result = self._json_request(
                    (
                        f"http://{gateway_host}:{gateway_port}"
                        "/v1/sandboxes/moving-one/migration"
                    ),
                    method="POST",
                    payload={
                        "destination_node_id": "destination-node",
                        "migration_id": "gateway-migration",
                    },
                )
                stored_route = RoutingStore(route_file).get_sandbox_readonly(
                    "moving-one"
                )
                stored_migration = RoutingStore(route_file).get_sandbox_migration(
                    "gateway-migration"
                )
            finally:
                gateway.shutdown()
                source_node.shutdown()
                destination_node.shutdown()
                gateway.server_close()
                source_node.server_close()
                destination_node.server_close()
                gateway_thread.join(timeout=1)
                source_thread.join(timeout=1)
                destination_thread.join(timeout=1)

        self.assertEqual(result["migration"]["phase"], "complete")
        self.assertIsNotNone(stored_route)
        self.assertIsNotNone(stored_migration)
        assert stored_route is not None and stored_migration is not None
        self.assertEqual(stored_route.node_id, "destination-node")
        self.assertEqual(stored_migration.phase, "complete")
        self.assertEqual(
            set(result["timings_ms"]),
            {
                "activate_destination",
                "finalize_source",
                "prepare_export",
                "protocol_total",
                "route_commit",
                "transfer_and_stage",
            },
        )
        self.assertGreaterEqual(result["timings_ms"]["route_commit"], 0)
        self.assertGreaterEqual(
            result["timings_ms"]["protocol_total"] + 0.01,
            sum(
                result["timings_ms"][key]
                for key in (
                    "activate_destination",
                    "finalize_source",
                    "prepare_export",
                    "route_commit",
                    "transfer_and_stage",
                )
            ),
        )
        self.assertEqual(
            calls,
            [
                ("destination", "/v1/images/pull"),
                ("source", "/v1/sandboxes/moving-one/migration/prepare"),
                ("destination", "/v1/migrations/import"),
                ("destination", "/v1/sandboxes/moving-one/migration/activate"),
                ("source", "/v1/sandboxes/moving-one/migration/finalize"),
            ],
        )

    def _json_request(
        self,
        url: str,
        *,
        method: str = "GET",
        payload: dict | None = None,
        headers: dict[str, str] | None = None,
        allow_error: bool = False,
    ) -> dict:
        body = None
        request_headers = dict(headers or {})
        if payload is not None:
            body = json.dumps(payload).encode("utf-8")
            request_headers["Content-Type"] = "application/json"
        req = request.Request(url, data=body, method=method, headers=request_headers)
        try:
            with request.urlopen(req, timeout=5) as response:
                return json.loads(response.read().decode("utf-8"))
        except error.HTTPError as exc:
            if not allow_error:
                raise
            return {
                "status": exc.code,
                "body": json.loads(exc.read().decode("utf-8")),
                "headers": dict(exc.headers),
            }

    def _bytes_request(
        self,
        url: str,
        *,
        method: str = "GET",
        body: bytes | None = None,
    ) -> dict:
        headers = {}
        if body is not None:
            headers["Content-Type"] = "application/octet-stream"
        req = request.Request(url, data=body, method=method, headers=headers)
        with request.urlopen(req, timeout=5) as response:
            raw = response.read()
            content_type = response.headers.get("Content-Type", "")
            if content_type.startswith("application/json"):
                return {
                    "json": json.loads(raw.decode("utf-8")),
                    "headers": response.headers,
                }
            return {"body": raw, "headers": response.headers}


if __name__ == "__main__":
    unittest.main()
