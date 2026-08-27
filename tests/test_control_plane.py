from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import replace
from datetime import timedelta
from io import BytesIO
from tempfile import TemporaryDirectory
from http.client import HTTPConnection
from threading import Event, Lock, Thread
from time import monotonic, sleep
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import hashlib
from pathlib import Path
import tarfile
from urllib import error, request
import unittest
from unittest.mock import patch

from ucloud_sandboxes.agent import (
    build_heartbeat as _agent_build_heartbeat,
    post_heartbeat,
    post_heartbeat_with_headers,
)
from ucloud_sandboxes import control_plane
from ucloud_sandboxes.control_state import ControlStateStore
from ucloud_sandboxes.control_plane import (
    build_server as _build_server,
)
from ucloud_sandboxes.deployment import package_version
from ucloud_sandboxes.images import (
    DockerImageRuntime,
    ImageRecord,
    ImageStore,
)
from ucloud_sandboxes.managed_registry import (
    RegistryUsageStore,
)
from ucloud_sandboxes.managed_process import ManagedProcessRecord
from ucloud_sandboxes.hibernation import (
    HibernationArtifactFile,
    HibernationFileRole,
    HibernationRuntimeFingerprint,
)
from ucloud_sandboxes.models import (
    NodeHeartbeat,
    NodeRuntimeMetrics,
    ResourceQuantity,
    SandboxInventoryEntry,
    utc_now,
)
from ucloud_sandboxes.node_agent import (
    build_builder_node_agent_server as _build_builder_node_agent_server,
)
from ucloud_sandboxes.routing import (
    RoutingStore,
    SandboxRoute,
    SandboxRouteAllocation,
    SandboxRouteConflictError,
)
from ucloud_sandboxes.sandbox import (
    CommandResult,
    SandboxSpec,
    sandbox_spec_fingerprint,
)
from ucloud_sandboxes.storage_native_migration import (
    StorageNativeMigration,
    StorageNativeSandboxManifest,
)
from ucloud_sandboxes.storage_native_registry import (
    PublishedStorageLayer,
    StorageSnapshotPublication,
)


def build_heartbeat(**kwargs):
    kwargs.setdefault("deployment_id", "test-deployment")
    kwargs.setdefault(
        "runtime_metrics",
        NodeRuntimeMetrics(
            collected_at=utc_now(),
            cpu_percent=0.0,
            cpu_count=128,
            load_average_1m=0.0,
            memory_total_mb=1_000_000,
            memory_available_mb=1_000_000,
        ),
    )
    return _agent_build_heartbeat(**kwargs)


def build_server(*args, **kwargs):
    """Build an auth-bypassed server for tests unrelated to channel security."""

    explicit_public_auth = (
        "gateway_bearer_token" in kwargs
        or "sandbox_api_token" in kwargs
        or "heartbeat_bearer_token" in kwargs
    )
    kwargs.setdefault("gateway_bearer_token", "test-gateway-secret")
    kwargs.setdefault("sandbox_api_token", "test-sandbox-api-secret")
    kwargs.setdefault("heartbeat_bearer_token", "test-heartbeat-secret")
    kwargs.setdefault("node_control_bearer_token", "test-node-secret")
    kwargs.setdefault("deployment_id", "test-deployment")
    heartbeat_file = Path(args[2])
    kwargs.setdefault(
        "routing_file",
        heartbeat_file.with_name(f"{heartbeat_file.stem}-routes.sqlite"),
    )
    kwargs.setdefault(
        "image_file",
        heartbeat_file.with_name(f"{heartbeat_file.stem}-images.json"),
    )
    kwargs.setdefault(
        "metrics_file",
        heartbeat_file.with_name(f"{heartbeat_file.stem}-metrics.sqlite"),
    )
    server = _build_server(*args, **kwargs)
    if not explicit_public_auth:
        server.RequestHandlerClass._check_authorized = lambda _self: True
        server.RequestHandlerClass._check_heartbeat_authorized = lambda _self: True
    server.RequestHandlerClass._heartbeat_identity_error = lambda _self, _heartbeat: (
        None
    )
    return server


def build_builder_node_agent_server(*args, **kwargs):
    kwargs.setdefault("node_control_bearer_token", "test-node-secret")
    kwargs.setdefault("deployment_id", "test-deployment")
    kwargs.setdefault("image_runtime", DockerImageRuntime(dry_run=True))
    server = _build_builder_node_agent_server(*args, **kwargs)
    server.RequestHandlerClass._check_node_control_authorized = lambda _self: True
    return server


def _sandbox_route(**kwargs) -> SandboxRoute:
    """Build a route with the canonical fenced identity required by the store."""

    raw_spec = dict(kwargs.get("spec") or {})
    raw_spec.setdefault("id", kwargs.get("sandbox_id"))
    kwargs["spec"] = raw_spec
    kwargs.setdefault("resources", ResourceQuantity())
    kwargs.setdefault("state", "unknown")
    kwargs.setdefault("generation", 1)
    kwargs.setdefault("create_operation_id", "00000000-0000-4000-8000-000000000001")
    if "spec_hash" not in kwargs:
        try:
            kwargs["spec_hash"] = sandbox_spec_fingerprint(
                SandboxSpec.from_dict(raw_spec)
            )
        except ValueError:
            kwargs["spec_hash"] = "a" * 64
    return SandboxRoute(**kwargs)


def _portable_snapshot(
    sandbox_id: str,
    *,
    generation: int = 1,
    create_operation_id: str = "00000000-0000-4000-8000-000000000001",
) -> StorageNativeMigration:
    spec = SandboxSpec.from_dict(
        {
            "id": sandbox_id,
            "image": "registry.test/image@sha256:" + "1" * 64,
            "parkable": True,
            "memory_mb": 1024,
            "disk_mb": 2048,
        }
    )
    runtime = HibernationRuntimeFingerprint(
        runsc_sha256="2" * 64,
        runsc_commit="3" * 40,
        platform="systrap",
        architecture="x86_64",
        page_size=4096,
        cpu_features_sha256="4" * 64,
        boot_config_sha256="5" * 64,
        rootfs_sha256="6" * 64,
    )
    files = tuple(
        HibernationArtifactFile(name, role, 1, 1)
        for name, role in (
            ("application_memory.img", HibernationFileRole.MAIN_MEMORY),
            ("checkpoint.img", HibernationFileRole.KERNEL_STATE),
            ("pages_meta.img", HibernationFileRole.ALLOCATOR_METADATA),
        )
    )
    return StorageNativeMigration(
        manifest=StorageNativeSandboxManifest(
            spec=spec,
            sandbox_generation=generation,
            create_operation_id=create_operation_id,
            hibernation_generation=1,
            park_operation_id="park:test",
            captured_ns=1,
            runtime=runtime,
            source_manifest_sha256="7" * 64,
            source_guest_ip=None,
            connection_policy="none",
            files=files,
        ),
        publication=StorageSnapshotPublication(
            manifest_digest="sha256:" + "8" * 64,
            tag=f"{sandbox_id}-{generation}",
            repository="snapshots",
            repo_blob_url="https://registry.test/v2/snapshots/blobs",
            virtual_size=4096,
            layers=(
                PublishedStorageLayer(
                    digest="sha256:" + "9" * 64,
                    size=4096,
                ),
            ),
        ),
    )


def _seed_gateway_node(
    root: Path,
    *,
    node_url: str,
    sandbox_id: str,
) -> tuple[Path, Path]:
    """Install one canonical heartbeat and matching route for proxy tests."""

    heartbeat_file = root / "control-state.sqlite"
    route_file = root / "routes.sqlite"
    route = RoutingStore(route_file).upsert_sandbox(
        _sandbox_route(
            sandbox_id=sandbox_id,
            node_id="node-1",
            job_id="job-1",
            node_url=node_url,
            node_epoch="epoch-1",
            state="running",
        )
    )
    ControlStateStore(heartbeat_file).upsert_heartbeat(
        build_heartbeat(
            node_id=route.node_id,
            job_id=route.job_id,
            node_url=route.node_url,
            node_epoch=route.node_epoch,
            active_sandboxes=1,
            capabilities=("sandbox", "disk-quota"),
            inventory=(
                SandboxInventoryEntry(
                    sandbox_id=route.sandbox_id,
                    state=route.state,
                    resources=route.resources,
                    generation=route.generation,
                    operation_id=route.create_operation_id,
                    spec_hash=route.spec_hash,
                ),
            ),
            inventory_complete=True,
        )
    )
    return heartbeat_file, route_file


def _wait_for(predicate, *, timeout_seconds: float = 2.0) -> bool:
    deadline = monotonic() + timeout_seconds
    while monotonic() < deadline:
        if predicate():
            return True
        sleep(0.01)
    return bool(predicate())


@contextmanager
def _temporary_root():
    with TemporaryDirectory() as raw_dir:
        yield Path(raw_dir)


def _gateway_server(root: Path, *, heartbeat_file: Path | None = None, **kwargs):
    return build_server(
        "127.0.0.1",
        0,
        heartbeat_file or root / "control-state.sqlite",
        **kwargs,
    )


@contextmanager
def _running_server(server: ThreadingHTTPServer):
    thread = Thread(
        target=server.serve_forever,
        kwargs={"poll_interval": 0.01},
        daemon=True,
    )
    thread.start()
    try:
        host, port = server.server_address
        yield f"http://{host}:{port}"
    finally:
        server.shutdown()
        thread.join(timeout=1)
        server.server_close()


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


def _store_build_context(server, archive: bytes) -> dict[str, object]:
    digest = f"sha256:{hashlib.sha256(archive).hexdigest()}"
    server.RequestHandlerClass.build_context_store.put_with_status(
        digest,
        BytesIO(archive),
        content_length=len(archive),
    )
    return {
        "context_path": ".",
        "context_archive_digest": digest,
        "context_archive_format": "tar.gz",
        "context_archive_size": len(archive),
    }


class ControlPlaneTests(unittest.TestCase):
    def test_parked_managed_job_status_is_gateway_state_and_does_not_wake(self) -> None:
        with _temporary_root() as root:
            route_file = root / "routes.sqlite"
            heartbeat_file = root / "control-state.sqlite"
            routing = RoutingStore(route_file)
            route = routing.upsert_sandbox(
                _sandbox_route(
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
            ControlStateStore(heartbeat_file).upsert_heartbeat(
                build_heartbeat(
                    job_id=route.job_id,
                    node_id=route.node_id,
                    node_url=route.node_url,
                    active_sandboxes=0,
                    inventory=(
                        SandboxInventoryEntry(
                            sandbox_id=route.sandbox_id,
                            state="parked",
                            generation=1,
                            operation_id="00000000-0000-4000-8000-000000000001",
                            spec_hash="a" * 64,
                        ),
                    ),
                    inventory_complete=True,
                )
            )
            gateway = _gateway_server(
                root,
                heartbeat_file=heartbeat_file,
                routing_file=route_file,
            )
            proxied = Event()

            def fail_proxy(*_args, **_kwargs):
                proxied.set()
                raise AssertionError("parked status must not reach a node")

            gateway.RequestHandlerClass._proxy_request = fail_proxy
            with _running_server(gateway) as gateway_url:
                payload = self._json_request(
                    f"{gateway_url}/v1/sandboxes/managed-one/jobs/rollout-1"
                )

        self.assertFalse(proxied.is_set())
        self.assertEqual(payload["job"]["state"], "running")
        self.assertEqual(payload["job"]["sandbox_generation"], 1)

    def test_managed_job_status_uses_gateway_state_during_node_transition(self) -> None:
        with _temporary_root() as root:
            route_file = root / "routes.sqlite"
            heartbeat_file = root / "control-state.sqlite"
            routing = RoutingStore(route_file)
            route = routing.upsert_sandbox(
                _sandbox_route(
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
            gateway = _gateway_server(
                root,
                heartbeat_file=heartbeat_file,
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
            with _running_server(gateway) as gateway_url:
                payload = self._json_request(
                    f"{gateway_url}/v1/sandboxes/managed-transition/jobs/rollout-1"
                )

        self.assertEqual(payload["job"]["state"], "running")
        self.assertEqual(payload["job"]["sequence"], 2)

    def test_relay_lifecycle_persists_program_request_transitions(self) -> None:
        with _temporary_root() as root:
            route_file = root / "routes.sqlite"
            heartbeat_file = root / "control-state.sqlite"
            metrics_file = root / "metrics.sqlite"
            routing = RoutingStore(route_file)
            route = routing.upsert_sandbox(
                _sandbox_route(
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
            ControlStateStore(heartbeat_file).upsert_heartbeat(
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
            gateway = _gateway_server(
                root,
                heartbeat_file=heartbeat_file,
                routing_file=route_file,
                metrics_file=metrics_file,
            )

            proxied_bodies = []

            def fake_proxy_request(_handler, *_args, **kwargs):
                proxied_bodies.append(json.loads(kwargs["body"]))
                return control_plane.ProxiedResponse(
                    200,
                    {"Content-Type": "application/json"},
                    b'{"ok":true}',
                )

            gateway.RequestHandlerClass._proxy_request = fake_proxy_request
            with _running_server(gateway) as gateway_url:
                base = f"{gateway_url}/v1/sandboxes/{route.sandbox_id}"
                identity = {
                    "generation": route.generation,
                    "operation_id": "relay-lifecycle:request-1",
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

        self.assertEqual(len(records), 1)
        self.assertEqual(
            proxied_bodies,
            [
                {"operation_id": "relay-lifecycle:request-1"},
                {
                    "generation": route.generation,
                    "operation_id": "relay-lifecycle:request-1",
                },
                {
                    "generation": route.generation,
                    "operation_id": "relay-lifecycle:request-1",
                },
            ],
        )
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
        with _temporary_root() as root:
            route_file = root / "routes.sqlite"
            heartbeat_file = root / "control-state.sqlite"
            metrics_file = root / "metrics.sqlite"
            routing = RoutingStore(route_file)
            route = routing.upsert_sandbox(
                _sandbox_route(
                    sandbox_id="sandbox-1",
                    node_id="node-1",
                    job_id="job-1",
                    node_url="http://node.invalid",
                    resources=ResourceQuantity(vcpu=1, memory_mb=1024, disk_mb=4096),
                    state="parked",
                )
            )
            ControlStateStore(heartbeat_file).upsert_heartbeat(
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
            gateway = _gateway_server(
                root,
                heartbeat_file=heartbeat_file,
                routing_file=route_file,
                metrics_file=metrics_file,
            )

            def failed_wake(_handler, *_args, **_kwargs):
                return control_plane.ProxiedResponse(
                    503,
                    {"Content-Type": "application/json"},
                    b'{"error":"restore validation failed","lifecycle_state":"parked"}',
                )

            gateway.RequestHandlerClass._proxy_request = failed_wake
            with _running_server(gateway) as gateway_url:
                base = f"{gateway_url}/v1/sandboxes/{route.sandbox_id}"
                identity = {
                    "generation": route.generation,
                    "operation_id": "relay-wake:request-1",
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

    def test_successful_exec_implicitly_commits_parked_route_wake(self) -> None:
        with _temporary_root() as root:
            route_file = root / "routes.sqlite"
            heartbeat_file = root / "control-state.sqlite"
            snapshot = _portable_snapshot("sandbox-implicit-wake")
            route = RoutingStore(route_file).upsert_sandbox(
                _sandbox_route(
                    sandbox_id=snapshot.manifest.sandbox_id,
                    node_id="node-1",
                    job_id="job-1",
                    node_url="http://node.invalid",
                    resources=snapshot.manifest.spec.requested_resources(),
                    spec=snapshot.manifest.spec.to_dict(),
                    state="parked",
                    storage_schema="storage-native-v1",
                    snapshot_manifest_digest=snapshot.publication.manifest_digest,
                    snapshot_repository=snapshot.publication.repository,
                    snapshot_tag=snapshot.publication.tag,
                    storage_snapshot=snapshot.to_dict(),
                )
            )
            ControlStateStore(heartbeat_file).upsert_heartbeat(
                build_heartbeat(
                    job_id=route.job_id,
                    node_id=route.node_id,
                    node_url=route.node_url,
                    active_sandboxes=0,
                    capabilities=("sandbox", "disk-quota"),
                    total_resources=ResourceQuantity(
                        vcpu=8,
                        memory_mb=16_384,
                        disk_mb=100_000,
                    ),
                    inventory_complete=True,
                )
            )
            gateway = _gateway_server(
                root,
                heartbeat_file=heartbeat_file,
                routing_file=route_file,
            )

            def successful_exec(_handler, *_args, **_kwargs):
                return control_plane.ProxiedResponse(
                    201,
                    {"Content-Type": "application/json"},
                    b'{"session":{"id":"exec-implicit-wake"}}',
                )

            gateway.RequestHandlerClass._proxy_request = successful_exec
            with _running_server(gateway) as base:
                response = self._json_request(
                    f"{base}/v1/sandboxes/{route.sandbox_id}/exec",
                    method="POST",
                    payload={
                        "command": ["true"],
                        "env": {},
                        "working_dir": None,
                        "stdin": False,
                        "tty": False,
                    },
                )
                stored = RoutingStore(route_file).get_sandbox_readonly(route.sandbox_id)

        self.assertEqual(response["session"]["id"], "exec-implicit-wake")
        self.assertIsNotNone(stored)
        assert stored is not None
        self.assertEqual(stored.state, "running")
        self.assertEqual(stored.storage_schema, "storage-native-v1")
        self.assertEqual(stored.snapshot_manifest_digest, "")
        self.assertEqual(stored.snapshot_repository, "")
        self.assertEqual(stored.snapshot_tag, "")
        self.assertEqual(stored.storage_snapshot, {})

    def test_worker_detach_retries_ambiguous_eviction_and_commits_once(self) -> None:
        with _temporary_root() as root:
            routing = RoutingStore(root / "routes.sqlite")
            snapshot = _portable_snapshot("sandbox-1")
            route = routing.upsert_sandbox(
                _sandbox_route(
                    sandbox_id="sandbox-1",
                    node_id="node-1",
                    job_id="job-1",
                    node_url="http://node-1:8090",
                    resources=snapshot.manifest.spec.requested_resources(),
                    spec=snapshot.manifest.spec.to_dict(),
                    state="parked",
                    storage_schema="storage-native-v1",
                    snapshot_manifest_digest=(snapshot.publication.manifest_digest),
                    snapshot_repository=snapshot.publication.repository,
                    snapshot_tag=snapshot.publication.tag,
                    storage_snapshot=snapshot.to_dict(),
                )
            )
            handler = object.__new__(control_plane.ControlPlaneHandler)
            handler.routing_store = routing
            handler.registry_usage_store = None
            handler.heartbeat_ttl_seconds = 120
            handler.store = ControlStateStore(root / "control.sqlite")
            handler._read_json_body = lambda: {}
            writes: list[tuple[dict, object]] = []
            handler._write_json = lambda payload, *, status=200, **_kwargs: (
                writes.append((payload, status))
            )
            responses = iter(
                (
                    control_plane.ProxiedResponse(
                        503,
                        {"Content-Type": "application/json"},
                        b'{"error":"ambiguous local deletion"}',
                    ),
                    control_plane.ProxiedResponse(
                        200,
                        {"Content-Type": "application/json"},
                        b'{"ok":true}',
                    ),
                )
            )
            calls: list[tuple[str, dict]] = []

            def proxy(_node_url, path, **kwargs):
                calls.append((path, json.loads(kwargs["body"])))
                return next(responses)

            handler._proxy_request = proxy

            handler._detach_sandbox_from_worker(route.sandbox_id)
            after_failure = routing.get_sandbox_readonly(route.sandbox_id)
            handler._detach_sandbox_from_worker(route.sandbox_id)
            after_success = routing.get_sandbox_readonly(route.sandbox_id)

        assert after_failure is not None
        assert after_success is not None
        self.assertEqual(after_failure.worker_state, "detaching")
        self.assertEqual(after_success.worker_state, "detached")
        self.assertEqual([status for _payload, status in writes], [503, 200])
        self.assertEqual(len(calls), 2)
        self.assertEqual(
            calls[0][1],
            {
                "generation": route.generation,
                "snapshot_manifest_digest": route.snapshot_manifest_digest,
            },
        )

    def test_worker_detach_publishes_park_before_eviction(self) -> None:
        with _temporary_root() as root:
            routing = RoutingStore(root / "routes.sqlite")
            snapshot = _portable_snapshot("sandbox-1")
            route = routing.upsert_sandbox(
                _sandbox_route(
                    sandbox_id="sandbox-1",
                    node_id="node-1",
                    job_id="job-1",
                    node_url="http://node-1:8090",
                    resources=snapshot.manifest.spec.requested_resources(),
                    spec=snapshot.manifest.spec.to_dict(),
                    state="parked",
                )
            )
            handler = object.__new__(control_plane.ControlPlaneHandler)
            handler.routing_store = routing
            handler.registry_usage_store = None
            handler.heartbeat_ttl_seconds = 120
            handler.store = ControlStateStore(root / "control.sqlite")
            handler._read_json_body = lambda: {}
            writes: list[tuple[dict, object]] = []
            handler._write_json = lambda payload, *, status=200, **_kwargs: (
                writes.append((payload, status))
            )
            responses = iter(
                (
                    control_plane.ProxiedResponse(
                        200,
                        {"Content-Type": "application/json"},
                        json.dumps(
                            {
                                "storage_schema": "storage-native-v1",
                                "snapshot_sha256": snapshot.sha256,
                                "storage_snapshot": snapshot.to_dict(),
                                "snapshot_manifest_digest": (
                                    snapshot.publication.manifest_digest
                                ),
                                "snapshot_repository": (
                                    snapshot.publication.repository
                                ),
                                "snapshot_tag": snapshot.publication.tag,
                            }
                        ).encode("utf-8"),
                    ),
                    control_plane.ProxiedResponse(
                        200,
                        {"Content-Type": "application/json"},
                        b'{"ok":true}',
                    ),
                )
            )
            calls: list[tuple[str, dict]] = []

            def proxy(_node_url, path, **kwargs):
                calls.append((path, json.loads(kwargs["body"])))
                return next(responses)

            handler._proxy_request = proxy

            handler._detach_sandbox_from_worker(route.sandbox_id)
            stored = routing.get_sandbox_readonly(route.sandbox_id)

        assert stored is not None
        self.assertEqual(stored.worker_state, "detached")
        self.assertEqual(stored.storage_snapshot, snapshot.to_dict())
        self.assertEqual(
            [path for path, _payload in calls],
            [
                "/v1/sandboxes/sandbox-1/publish-parked",
                "/v1/sandboxes/sandbox-1/evict-published",
            ],
        )
        self.assertEqual(
            calls[0][1],
            {
                "generation": route.generation,
                "create_operation_id": route.create_operation_id,
                "spec_hash": route.spec_hash,
            },
        )
        self.assertEqual(writes[0][1], 200)
        self.assertEqual(
            calls[1][1],
            {
                "generation": route.generation,
                "snapshot_manifest_digest": snapshot.publication.manifest_digest,
            },
        )

    def test_detached_park_wakes_without_contacting_former_worker(self) -> None:
        with _temporary_root() as root:
            routing = RoutingStore(root / "routes.sqlite")
            snapshot = _portable_snapshot("sandbox-1")
            route = routing.upsert_sandbox(
                _sandbox_route(
                    sandbox_id="sandbox-1",
                    node_id="lost-node",
                    job_id="lost-job",
                    node_url="http://lost-node:8090",
                    resources=snapshot.manifest.spec.requested_resources(),
                    spec=snapshot.manifest.spec.to_dict(),
                    state="parked",
                    worker_state="detached",
                    storage_schema="storage-native-v1",
                    snapshot_manifest_digest=(snapshot.publication.manifest_digest),
                    snapshot_repository=snapshot.publication.repository,
                    snapshot_tag=snapshot.publication.tag,
                    storage_snapshot=snapshot.to_dict(),
                )
            )
            heartbeats = ControlStateStore(root / "control.sqlite")
            heartbeats.upsert_heartbeat(
                NodeHeartbeat(
                    node_id="destination-node",
                    job_id="destination-job",
                    deployment_id="test-deployment",
                    updated_at=utc_now(),
                    active_sandboxes=0,
                    node_url="http://destination:8090",
                    agent_version=package_version(),
                    capabilities=(
                        "sandbox",
                        "disk-quota",
                        "storage-native-v1",
                        "sandbox-migrate-storage-native-v1",
                    ),
                    total_resources=ResourceQuantity(
                        vcpu=8,
                        memory_mb=16_384,
                        disk_mb=100_000,
                    ),
                    resources_known=True,
                    runtime_metrics=NodeRuntimeMetrics(
                        collected_at=utc_now(),
                        cpu_percent=0.0,
                        cpu_count=8,
                        memory_total_mb=16_384,
                        memory_available_mb=16_384,
                        storage_hard_capacity_mb=100_000,
                    ),
                    inventory_complete=True,
                )
            )
            handler = object.__new__(control_plane.ControlPlaneHandler)
            handler.routing_store = routing
            handler.store = heartbeats
            handler.heartbeat_ttl_seconds = 120
            handler.registry_usage_store = None
            handler.registry_url = ""
            handler.registry_worker_url = ""
            handler.registry_layer_cache = None
            handler._write_json = lambda *_args, **_kwargs: None
            handler._prepare_migration_destination_image = lambda *_args: True
            paths: list[str] = []

            def proxy(_node_url, path, **_kwargs):
                paths.append(path)
                if path == "/v1/migrations/import":
                    return control_plane.ProxiedResponse(
                        201,
                        {"Content-Type": "application/json"},
                        json.dumps(
                            {
                                "storage_schema": "storage-native-v1",
                                "storage_snapshot": snapshot.to_dict(),
                            }
                        ).encode("utf-8"),
                    )
                if path.endswith("/migration/activate"):
                    return control_plane.ProxiedResponse(
                        200,
                        {"Content-Type": "application/json"},
                        b'{"ok":true}',
                    )
                raise AssertionError(f"unexpected source request: {path}")

            handler._proxy_request = proxy

            selected = handler._ensure_parked_sandbox_wake_placement(route)
            stored = routing.get_sandbox_readonly(route.sandbox_id)

        assert selected is not None
        assert stored is not None
        self.assertEqual(stored.node_id, "destination-node")
        self.assertEqual(stored.worker_state, "attached")
        self.assertEqual(stored.state, "waking")
        self.assertEqual(
            paths,
            [
                "/v1/migrations/import",
                "/v1/sandboxes/sandbox-1/migration/activate",
            ],
        )

    def test_attached_park_never_uses_registry_failover_without_source_proof(
        self,
    ) -> None:
        with _temporary_root() as root:
            routing = RoutingStore(root / "routes.sqlite")
            snapshot = _portable_snapshot("sandbox-1")
            route = routing.upsert_sandbox(
                _sandbox_route(
                    sandbox_id="sandbox-1",
                    node_id="temporarily-missing-node",
                    job_id="temporarily-missing-job",
                    node_url="http://temporarily-missing:8090",
                    resources=snapshot.manifest.spec.requested_resources(),
                    spec=snapshot.manifest.spec.to_dict(),
                    state="parked",
                    worker_state="attached",
                    storage_schema="storage-native-v1",
                    snapshot_manifest_digest=snapshot.publication.manifest_digest,
                    snapshot_repository=snapshot.publication.repository,
                    snapshot_tag=snapshot.publication.tag,
                    storage_snapshot=snapshot.to_dict(),
                )
            )
            heartbeats = ControlStateStore(root / "control.sqlite")
            heartbeats.upsert_heartbeat(
                NodeHeartbeat(
                    node_id="destination-node",
                    job_id="destination-job",
                    deployment_id="test-deployment",
                    updated_at=utc_now(),
                    active_sandboxes=0,
                    node_url="http://destination:8090",
                    agent_version=package_version(),
                    capabilities=(
                        "sandbox",
                        "disk-quota",
                        "storage-native-v1",
                        "sandbox-migrate-storage-native-v1",
                    ),
                    total_resources=ResourceQuantity(
                        vcpu=8,
                        memory_mb=16_384,
                        disk_mb=100_000,
                    ),
                    resources_known=True,
                    inventory_complete=True,
                )
            )
            handler = object.__new__(control_plane.ControlPlaneHandler)
            handler.routing_store = routing
            handler.store = heartbeats
            handler.heartbeat_ttl_seconds = 120

            selected = handler._select_migration_destination(
                route,
                requested_node_id="",
                require_active_resources=True,
            )

        self.assertIsNone(selected)

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

        with _temporary_root() as root:
            node = ThreadingHTTPServer(("127.0.0.1", 0), NodeProbeHandler)
            with _running_server(node) as node_url:
                heartbeat_file, route_file = _seed_gateway_node(
                    root,
                    node_url=node_url,
                    sandbox_id="auth-one",
                )
                gateway = _gateway_server(
                    root,
                    heartbeat_file=heartbeat_file,
                    routing_file=route_file,
                    gateway_bearer_token="gateway-secret",
                    node_control_bearer_token="node-secret",
                )
                with _running_server(gateway) as gateway_url:
                    payload = self._json_request(
                        f"{gateway_url}/v1/sandboxes/auth-one",
                        headers={
                            "Authorization": "Bearer gateway-secret",
                            "X-UCloud-Sandbox-Token": "gateway-secret",
                            "Proxy-Authorization": "Bearer leaked",
                        },
                    )

        self.assertEqual(payload, {"sandboxes": []})
        self.assertEqual(observed["authorization"], "Bearer node-secret")
        self.assertIsNone(observed["public_token"])
        self.assertIsNone(observed["proxy_authorization"])

    def test_gateway_stamps_heartbeat_receipt_time_and_enforces_deployment(
        self,
    ) -> None:
        with _temporary_root() as root:
            heartbeat_file = root / "control-state.sqlite"
            server = _gateway_server(
                root,
                heartbeat_file=heartbeat_file,
                deployment_id="prod-a",
            )
            with _running_server(server) as server_url:
                base = f"{server_url}/v1/nodes/heartbeat"
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
                stored = ControlStateStore(heartbeat_file).load_heartbeats()["job-1"]

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
            deployment_id="test-deployment",
            updated_at=utc_now(),
            active_sandboxes=0,
            node_url="http://node-1:8090",
            capabilities=("sandbox",),
            total_resources=ResourceQuantity(
                vcpu=4,
                memory_mb=8192,
                disk_mb=100_000,
            ),
            runtime_metrics=NodeRuntimeMetrics(
                collected_at=utc_now(),
                cpu_count=4,
                memory_total_mb=8192,
                memory_available_mb=8192,
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

    def test_ublk_device_limit_accounts_for_inflight_route_reservations(self) -> None:
        heartbeat = build_heartbeat(
            node_id="node-1",
            job_id="job-1",
            node_url="http://node-1:8090",
            capabilities=("sandbox", "disk-quota", "storage-native-v1"),
            total_resources=ResourceQuantity(
                vcpu=32,
                memory_mb=96 * 1024,
                disk_mb=1_000_000,
            ),
            runtime_metrics=NodeRuntimeMetrics(
                collected_at=utc_now(),
                cpu_percent=0.0,
                cpu_count=32,
                memory_total_mb=96 * 1024,
                memory_available_mb=96 * 1024,
                storage_hard_capacity_mb=1_000_000,
                storage_ublk_active_devices=63,
                storage_ublk_live_devices=64,
                storage_ublk_max_devices=64,
            ),
            inventory=(),
            inventory_complete=True,
        )
        requested = ResourceQuantity(vcpu=1, memory_mb=512, disk_mb=1024)
        inflight = _sandbox_route(
            sandbox_id="inflight",
            node_id=heartbeat.node_id,
            job_id=heartbeat.job_id,
            node_url=heartbeat.node_url or "",
            resources=requested,
            state="creating",
        )

        self.assertTrue(control_plane._node_can_fit(heartbeat, requested, []))
        self.assertFalse(
            control_plane._node_can_fit(heartbeat, requested, [inflight])
        )
        self.assertFalse(
            control_plane._node_can_fit(
                replace(
                    heartbeat,
                    runtime_metrics=replace(
                        heartbeat.runtime_metrics,
                        storage_ublk_active_devices=64,
                    ),
                ),
                requested,
                [],
            )
        )

    def test_wake_image_pull_does_not_hold_global_placement_lock(self) -> None:
        with _temporary_root() as root:
            routing = RoutingStore(root / "routes.sqlite")
            resources = ResourceQuantity(vcpu=4, memory_mb=8192, disk_mb=8192)
            parked = routing.upsert_sandbox(
                _sandbox_route(
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
                _sandbox_route(
                    sandbox_id="source-busy",
                    node_id="source-node",
                    job_id="source-job",
                    node_url="http://source:8090",
                    resources=resources,
                    state="running",
                )
            )
            heartbeats = ControlStateStore(root / "control-state.sqlite")
            for heartbeat in (
                NodeHeartbeat(
                    node_id="source-node",
                    job_id="source-job",
                    deployment_id="test-deployment",
                    updated_at=utc_now(),
                    active_sandboxes=1,
                    node_url="http://source:8090",
                    agent_version=package_version(),
                    capabilities=(
                        "sandbox",
                        "disk-quota",
                        "storage-native-v1",
                        "sandbox-migrate-storage-native-v1",
                    ),
                    total_resources=ResourceQuantity(
                        vcpu=4,
                        memory_mb=8192,
                        disk_mb=100_000,
                    ),
                    resources_known=True,
                    runtime_metrics=NodeRuntimeMetrics(
                        collected_at=utc_now(),
                        cpu_percent=95.0,
                        cpu_count=4,
                        storage_hard_capacity_mb=100_000,
                    ),
                    inventory_complete=True,
                ),
                NodeHeartbeat(
                    node_id="destination-node",
                    job_id="destination-job",
                    deployment_id="test-deployment",
                    updated_at=utc_now(),
                    active_sandboxes=0,
                    node_url="http://destination:8090",
                    agent_version=package_version(),
                    capabilities=(
                        "sandbox",
                        "disk-quota",
                        "storage-native-v1",
                        "sandbox-migrate-storage-native-v1",
                    ),
                    total_resources=ResourceQuantity(
                        vcpu=8,
                        memory_mb=16_384,
                        disk_mb=100_000,
                    ),
                    resources_known=True,
                    runtime_metrics=NodeRuntimeMetrics(
                        collected_at=utc_now(),
                        cpu_percent=0.0,
                        cpu_count=8,
                        storage_hard_capacity_mb=100_000,
                    ),
                    inventory_complete=True,
                ),
            ):
                heartbeats.upsert_heartbeat(heartbeat)

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
                spec_hash=sandbox_spec_fingerprint(
                    SandboxSpec.from_dict(
                        {"id": "concurrent-create", "image": "busybox"}
                    )
                ),
            )
            elapsed = monotonic() - started
            release_pull.set()
            wake_thread.join(timeout=2)
            migrations = routing.sandbox_migrations(active_only=True)

        self.assertIsNotNone(placement)
        self.assertLess(elapsed, control_plane.SANDBOX_PLACEMENT_LOCK_WAIT_SECONDS)
        self.assertEqual(len(migrations), 1)
        self.assertEqual(migrations[0].destination_node_id, "destination-node")

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

        with _temporary_root() as raw_path:
            image_file = raw_path / "images.json"
            registry = ThreadingHTTPServer(
                ("127.0.0.1", 0),
                MissingManifestRegistryHandler,
            )
            with _running_server(registry):
                registry_host, registry_port = registry.server_address
                now = utc_now()
                ImageStore(image_file).upsert(
                    ImageRecord(
                        id="missing",
                        tag=(
                            f"{registry_host}:{registry_port}/prime-rl/missing:latest"
                        ),
                        source="build:/tmp/missing",
                        state="available",
                        created_at=now,
                        updated_at=now,
                        pushed=True,
                    )
                )
                gateway = _gateway_server(
                    raw_path,
                    routing_file=raw_path / "routes.sqlite",
                    image_file=image_file,
                    registry_url=f"http://{registry_host}:{registry_port}",
                )
                with _running_server(gateway) as gateway_url:
                    images = self._json_request(f"{gateway_url}/v1/images")

            self.assertEqual(images["images"], [])
            self.assertEqual(ImageStore(image_file).load(), {})

    def test_gateway_proxy_returns_json_bad_gateway_when_node_disconnects(self) -> None:
        class DisconnectingHandler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                self.connection.close()

            def log_message(self, format: str, *args: object) -> None:
                del format, args

        with _temporary_root() as root:
            node = ThreadingHTTPServer(("127.0.0.1", 0), DisconnectingHandler)
            with _running_server(node) as node_url:
                heartbeat_file, route_file = _seed_gateway_node(
                    root,
                    node_url=node_url,
                    sandbox_id="disconnect-one",
                )
                gateway = _gateway_server(
                    root,
                    heartbeat_file=heartbeat_file,
                    routing_file=route_file,
                )
                with _running_server(gateway) as gateway_url:
                    response = self._json_request(
                        f"{gateway_url}/v1/sandboxes/disconnect-one",
                        allow_error=True,
                    )

        self.assertEqual(response["status"], 502)
        self.assertIn("node request failed", response["body"]["error"])

    def test_gateway_lists_sandboxes_from_cache_unless_refresh_requested(
        self,
    ) -> None:
        canonical_spec = {
            "id": "cached-one",
            "image": "busybox",
            "labels": {"run": "r1"},
            "memory_mb": 512,
            "disk_mb": 1024,
        }
        spec_hash = sandbox_spec_fingerprint(SandboxSpec.from_dict(canonical_spec))

        class ListingNode(BaseHTTPRequestHandler):
            listed = Event()
            invalid_storage = False

            def do_GET(self) -> None:
                if self.path.split("?", 1)[0] == "/v1/sandboxes":
                    self.listed.set()
                    record = {
                        "spec": canonical_spec,
                        "state": "running",
                        "generation": 1,
                        "operation_id": "00000000-0000-4000-8000-000000000001",
                        "spec_hash": spec_hash,
                    }
                    if type(self).invalid_storage:
                        record["storage_schema"] = "invalid-storage"
                    self._write_json({"sandboxes": [record]})
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

        with _temporary_root() as raw_path:
            route_file = raw_path / "routes.sqlite"
            node = ThreadingHTTPServer(("127.0.0.1", 0), ListingNode)
            with _running_server(node) as node_url:
                RoutingStore(route_file).upsert_sandbox(
                    _sandbox_route(
                        sandbox_id="cached-one",
                        node_id="node-1",
                        job_id="job-1",
                        node_url=node_url,
                        resources=ResourceQuantity(
                            vcpu=1.0,
                            memory_mb=512,
                            disk_mb=1024,
                        ),
                        spec=canonical_spec,
                        state="running",
                        generation=1,
                        create_operation_id=("00000000-0000-4000-8000-000000000001"),
                        spec_hash=spec_hash,
                    )
                )
                gateway = _gateway_server(
                    raw_path,
                    routing_file=route_file,
                )
                with _running_server(gateway) as base:
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
                    ListingNode.invalid_storage = True
                    malformed = self._json_request(f"{base}/v1/sandboxes?refresh=true")
                    stored = RoutingStore(route_file).get_sandbox("cached-one")

        self.assertTrue(cached["cached"])
        self.assertEqual(cached["sandboxes"][0]["id"], "cached-one")
        self.assertEqual(cached["sandboxes"][0]["state"], "running")
        self.assertEqual(cached["sandboxes"][0]["image"], "busybox")
        self.assertEqual(cached["sandboxes"][0]["labels"], {"run": "r1"})
        self.assertFalse(cached["sandboxes"][0]["route_only"])
        self.assertTrue(ListingNode.listed.is_set())
        self.assertFalse(refreshed["cached"])
        self.assertEqual(refreshed["sandboxes"][0]["spec"]["id"], "cached-one")
        self.assertEqual(malformed["sandboxes"], [])
        self.assertEqual(stored.state if stored is not None else None, "running")

    def test_gateway_rejects_unavailable_registry_usage_state(self) -> None:
        with _temporary_root() as raw_path:
            usage_path = raw_path / "registry-usage.json"
            usage_path.mkdir()
            with self.assertRaisesRegex(
                ValueError,
                "registry usage database is invalid or unavailable",
            ):
                _gateway_server(
                    raw_path,
                    registry_usage_file=usage_path,
                )

    def test_gateway_sdk_and_heartbeat_tokens_are_channel_scoped(self) -> None:
        with _temporary_root() as root:
            gateway = _build_server(
                "127.0.0.1",
                0,
                root / "control-state.sqlite",
                routing_file=root / "routes.sqlite",
                image_file=root / "images.json",
                metrics_file=root / "metrics.sqlite",
                gateway_bearer_token="gateway-secret",
                sandbox_api_token="sandbox-api-secret",
                heartbeat_bearer_token="heartbeat-secret",
                node_control_bearer_token="node-secret",
                deployment_id="test-deployment",
            )
            with _running_server(gateway) as base:
                heartbeat = build_heartbeat(
                    job_id="job-1",
                    node_id="node-1",
                    node_url="http://node-1:8090",
                    node_epoch="boot-1",
                    activity_epoch=100,
                    deployment_id="test-deployment",
                )
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
                sdk_token_on_sandbox_api = self._json_request(
                    f"{base}/v1/sandboxes",
                    headers={"X-UCloud-Sandbox-Token": "sandbox-api-secret"},
                )
                sdk_bearer_on_sandbox_api = self._json_request(
                    f"{base}/v1/sandboxes",
                    headers={"Authorization": "Bearer sandbox-api-secret"},
                )
                sdk_token_on_operator_api = self._json_request(
                    f"{base}/v1/nodes",
                    headers={"X-UCloud-Sandbox-Token": "sandbox-api-secret"},
                    allow_error=True,
                )
                gateway_token_in_public_link_header = self._json_request(
                    f"{base}/v1/sandboxes",
                    headers={"X-UCloud-Sandbox-Token": "gateway-secret"},
                )

        self.assertEqual(no_token.status, 401)
        self.assertEqual(gateway_token_on_heartbeat.status, 401)
        self.assertEqual(public_header_on_heartbeat.status, 401)
        self.assertEqual(accepted_heartbeat.status, 200)
        self.assertEqual(heartbeat_token_on_gateway["status"], 401)
        self.assertEqual(len(gateway_token_on_gateway["nodes"]), 1)
        self.assertEqual(sdk_token_on_sandbox_api["sandboxes"], [])
        self.assertEqual(sdk_bearer_on_sandbox_api["sandboxes"], [])
        self.assertEqual(sdk_token_on_operator_api["status"], 403)
        self.assertEqual(gateway_token_in_public_link_header["sandboxes"], [])

    def test_build_server_requires_distinct_nonempty_credentials(self) -> None:
        with _temporary_root() as root:
            for credentials in (
                {
                    "gateway_bearer_token": "",
                    "sandbox_api_token": "sandbox-api",
                    "heartbeat_bearer_token": "heartbeat",
                    "node_control_bearer_token": "node",
                },
                {
                    "gateway_bearer_token": "shared",
                    "sandbox_api_token": "sandbox-api",
                    "heartbeat_bearer_token": "shared",
                    "node_control_bearer_token": "node",
                },
                {
                    "gateway_bearer_token": "shared",
                    "sandbox_api_token": "shared",
                    "heartbeat_bearer_token": "heartbeat",
                    "node_control_bearer_token": "node",
                },
            ):
                with (
                    self.subTest(credentials=credentials),
                    self.assertRaises(ValueError),
                ):
                    _build_server(
                        "127.0.0.1",
                        0,
                        root / "control-state.sqlite",
                        routing_file=root / "routes.sqlite",
                        image_file=root / "images.json",
                        metrics_file=root / "metrics.sqlite",
                        deployment_id="test-deployment",
                        **credentials,
                    )

    def test_authenticated_malformed_heartbeat_returns_bad_request(self) -> None:
        with _temporary_root() as root:
            gateway = _build_server(
                "127.0.0.1",
                0,
                root / "control-state.sqlite",
                routing_file=root / "routes.sqlite",
                image_file=root / "images.json",
                metrics_file=root / "metrics.sqlite",
                gateway_bearer_token="gateway-secret",
                sandbox_api_token="sandbox-api-secret",
                heartbeat_bearer_token="heartbeat-secret",
                node_control_bearer_token="node-secret",
                deployment_id="test-deployment",
            )
            with _running_server(gateway) as gateway_url:
                response = self._json_request(
                    f"{gateway_url}/v1/nodes/heartbeat",
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

        self.assertEqual(response["status"], 400)
        self.assertEqual(response["body"], {"error": "invalid heartbeat payload"})

    def test_heartbeat_identity_is_bound_to_authoritative_route(self) -> None:
        with _temporary_root() as root:
            route_file = root / "routes.sqlite"
            RoutingStore(route_file).upsert_sandbox(
                _sandbox_route(
                    sandbox_id="sandbox-1",
                    node_id="node-1",
                    job_id="job-1",
                    node_url="http://node-1:8090",
                    generation=1,
                    create_operation_id="create-1",
                    spec_hash="a" * 64,
                )
            )
            gateway = _build_server(
                "127.0.0.1",
                0,
                root / "control-state.sqlite",
                routing_file=route_file,
                image_file=root / "images.json",
                metrics_file=root / "metrics.sqlite",
                gateway_bearer_token="gateway-secret",
                sandbox_api_token="sandbox-api-secret",
                heartbeat_bearer_token="heartbeat-secret",
                node_control_bearer_token="node-secret",
                deployment_id="prod",
            )
            with _running_server(gateway) as gateway_url:
                response = post_heartbeat_with_headers(
                    f"{gateway_url}/v1/nodes/heartbeat",
                    build_heartbeat(
                        job_id="job-1",
                        node_id="node-1",
                        node_url="http://attacker.invalid:8090",
                        node_epoch="boot-1",
                        activity_epoch=100,
                        deployment_id="prod",
                    ),
                    {"Authorization": "Bearer heartbeat-secret"},
                )
            stored = ControlStateStore(root / "control-state.sqlite").load_heartbeats()

        self.assertEqual(response.status, 403)
        self.assertEqual(stored, {})

    def test_heartbeat_rejects_node_url_owned_by_another_route(self) -> None:
        with _temporary_root() as root:
            route_file = root / "routes.sqlite"
            RoutingStore(route_file).upsert_sandbox(
                _sandbox_route(
                    sandbox_id="sandbox-1",
                    node_id="node-2",
                    job_id="job-2",
                    node_url="http://shared-node:8090",
                    generation=1,
                    create_operation_id="create-1",
                    spec_hash="a" * 64,
                )
            )
            gateway = _build_server(
                "127.0.0.1",
                0,
                root / "control-state.sqlite",
                routing_file=route_file,
                image_file=root / "images.json",
                metrics_file=root / "metrics.sqlite",
                gateway_bearer_token="gateway-secret",
                sandbox_api_token="sandbox-api-secret",
                heartbeat_bearer_token="heartbeat-secret",
                node_control_bearer_token="node-secret",
                deployment_id="prod",
            )
            with _running_server(gateway) as gateway_url:
                response = post_heartbeat_with_headers(
                    f"{gateway_url}/v1/nodes/heartbeat",
                    build_heartbeat(
                        job_id="job-1",
                        node_id="node-1",
                        node_url="http://shared-node:8090/",
                        node_epoch="boot-1",
                        activity_epoch=100,
                        deployment_id="prod",
                    ),
                    {"Authorization": "Bearer heartbeat-secret"},
                )
            stored = ControlStateStore(root / "control-state.sqlite").load_heartbeats()

        self.assertEqual(response.status, 403)
        self.assertEqual(stored, {})

    def test_dashboard_assets_are_public_but_metrics_remain_protected(self) -> None:
        with _temporary_root() as root:
            gateway = _gateway_server(
                root,
                routing_file=root / "routes.sqlite",
                gateway_bearer_token="secret-token",
                metrics_file=root / "metrics.sqlite",
            )
            with _running_server(gateway) as base:
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

    def test_gateway_placement_contention_fails_fast_with_retryable_json(self) -> None:
        with _temporary_root() as raw_path:
            gateway = _gateway_server(
                raw_path,
                routing_file=raw_path / "routes.sqlite",
                metrics_file=raw_path / "metrics.sqlite",
            )
            with _running_server(gateway) as base:
                self.assertTrue(
                    control_plane._GATEWAY_SCHEDULING_LOCK.acquire(blocking=False)
                )
                started = monotonic()
                try:
                    result = self._json_request(
                        f"{base}/v1/sandboxes",
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
                metrics = self._json_request(f"{base}/v1/metrics")

        self.assertEqual(result["status"], 503)
        self.assertTrue(result["body"]["retryable"])
        self.assertIn("reserving sandbox placement", result["body"]["error"])
        self.assertLess(elapsed, 1)
        self.assertNotIn("traces", metrics)
        self.assertFalse(metrics["telemetry"]["enabled"])

    def test_gateway_create_burst_returns_only_retryable_json(self) -> None:
        with _temporary_root() as raw_path:
            gateway = _gateway_server(
                raw_path,
                routing_file=raw_path / "routes.sqlite",
                max_concurrent_sandbox_creates=8,
            )
            with _running_server(gateway) as base:

                def create(index: int) -> dict:
                    return self._json_request(
                        f"{base}/v1/sandboxes",
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

        self.assertEqual({result["status"] for result in results}, {503})
        self.assertTrue(all(result["body"]["retryable"] for result in results))
        self.assertTrue(
            all(isinstance(result["body"].get("error"), str) for result in results)
        )
        self.assertLess(elapsed, 5)

    def test_gateway_create_admission_precedes_body_read_and_caps_json(self) -> None:
        with _temporary_root() as root:
            gateway = _gateway_server(
                root,
                routing_file=root / "routes.sqlite",
                max_concurrent_sandbox_creates=1,
            )
            with _running_server(gateway):
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

        self.assertEqual(busy_body["retryable"], True)
        self.assertEqual(oversized_status, 400)
        self.assertIn("16777216 byte limit", oversized_body["error"])

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
                            "state": "running",
                            "generation": operation["generation"],
                            "operation_id": operation["operation_id"],
                            "spec_hash": operation["spec_hash"],
                        },
                        "command": ["docker", "run"],
                        "exit_code": 0,
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

        with _temporary_root() as raw_path:
            route_file = raw_path / "routes.sqlite"
            node = ThreadingHTTPServer(("127.0.0.1", 0), SlowCreateNode)
            with _running_server(node) as node_url:
                gateway = _gateway_server(
                    raw_path,
                    routing_file=route_file,
                    metrics_file=raw_path / "metrics.sqlite",
                )
                with _running_server(gateway) as base:
                    try:
                        result = post_heartbeat(
                            f"{base}/v1/nodes/heartbeat",
                            build_heartbeat(
                                job_id="job-1",
                                node_id="node-1",
                                node_url=node_url,
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

        self.assertIsNotNone(route)
        self.assertEqual(created["sandbox"]["spec"]["id"], "slow-one")

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

        with _temporary_root() as raw_path:
            route_file = raw_path / "routes.sqlite"
            node = ThreadingHTTPServer(("127.0.0.1", 0), PlannedNode)
            with _running_server(node) as node_url:
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
                    _sandbox_route(
                        sandbox_id=spec.id,
                        node_id="node-1",
                        job_id="job-1",
                        node_url=node_url,
                        resources=spec.requested_resources(),
                        spec=spec.to_dict(),
                        state="planned",
                        generation=1,
                        create_operation_id=operation_id,
                        spec_hash=spec_hash,
                    )
                )
                gateway = _gateway_server(
                    raw_path,
                    routing_file=route_file,
                )
                with _running_server(gateway) as gateway_url:
                    result = self._json_request(
                        f"{gateway_url}/v1/sandboxes/{spec.id}/exec",
                        method="POST",
                        payload={"command": ["/bin/true"]},
                        allow_error=True,
                    )

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

        with _temporary_root() as raw_path:
            route_file = raw_path / "routes.sqlite"
            usage_file = raw_path / "registry-usage.json"
            node = ThreadingHTTPServer(("127.0.0.1", 0), AmbiguousCreateNode)
            with _running_server(node) as node_url:
                image_ref = "registry.example.org/repo:v1@sha256:" + "a" * 64
                heartbeat = build_heartbeat(
                    job_id="job-1",
                    node_id="node-1",
                    node_url=node_url,
                    active_sandboxes=1,
                    capabilities=("sandbox", "image-cache", "disk-quota"),
                    cached_images=(image_ref,),
                    total_resources=ResourceQuantity(
                        vcpu=4,
                        memory_mb=4096,
                        disk_mb=8192,
                    ),
                    inventory=(),
                    inventory_complete=True,
                )
                first_gateway = _gateway_server(
                    raw_path,
                    routing_file=route_file,
                    registry_usage_file=usage_file,
                )
                with _running_server(first_gateway) as base:
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
                            "image": image_ref,
                            "cpus": 1,
                            "memory_mb": 512,
                        },
                        allow_error=True,
                    )
                    before_restart = RegistryUsageStore(usage_file).snapshot()

                second_gateway = _gateway_server(
                    raw_path,
                    routing_file=route_file,
                    registry_usage_file=usage_file,
                )
                with _running_server(second_gateway) as base:
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

        self.assertEqual(created["status"], 503)
        self.assertEqual(AmbiguousCreateNode.create_count, 1, created)
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

        with _temporary_root() as raw_path:
            route_file = raw_path / "routes.sqlite"
            node = ThreadingHTTPServer(("127.0.0.1", 0), ClosedAdmissionNode)
            with _running_server(node) as node_url:
                image = "busybox"
                gateway = _gateway_server(
                    raw_path,
                    routing_file=route_file,
                )
                with _running_server(gateway) as base:
                    self.assertEqual(
                        post_heartbeat(
                            f"{base}/v1/nodes/heartbeat",
                            build_heartbeat(
                                job_id="job-1",
                                node_id="node-1",
                                node_url=node_url,
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
            def touch_images(self, image_refs, *, when=None) -> None:
                del image_refs, when
                raise OSError("usage store unavailable")

        with _temporary_root() as raw_path:
            route_file = raw_path / "routes.sqlite"
            node = ThreadingHTTPServer(("127.0.0.1", 0), CountingNode)
            with _running_server(node) as node_url:
                image = "registry.example.org/repo:v1"
                gateway = _gateway_server(
                    raw_path,
                    routing_file=route_file,
                    registry_usage_file=raw_path / "registry-usage.json",
                )
                with _running_server(gateway) as base:
                    self.assertEqual(
                        post_heartbeat(
                            f"{base}/v1/nodes/heartbeat",
                            build_heartbeat(
                                job_id="job-1",
                                node_id="node-1",
                                node_url=node_url,
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
                    RoutingStore(route_file).allocate_sandbox_create_with_pending(
                        SandboxRouteAllocation(
                            sandbox_id=retry_spec.id,
                            node_id="node-1",
                            job_id="job-1",
                            node_url=node_url,
                            resources=retry_spec.requested_resources(),
                            spec=retry_spec.to_dict(),
                        ),
                        spec_hash=sandbox_spec_fingerprint(retry_spec),
                    )[0]
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

        self.assertEqual(create["status"], 503)
        self.assertEqual(retry["status"], 503)
        self.assertEqual(pull["status"], 503)
        expected_error = {
            "error": "registry image-use state is unavailable",
            "retryable": True,
        }
        self.assertEqual(create["body"], expected_error)
        self.assertEqual(retry["body"], expected_error)
        self.assertEqual(pull["body"], expected_error)
        self.assertEqual(CountingNode.post_count, 0)
        self.assertIsNone(RoutingStore(route_file).get_sandbox("blocked-one"))

    def test_explicit_managed_digest_fails_closed_without_protection_tag(
        self,
    ) -> None:
        digest = "sha256:" + "7" * 64
        with _temporary_root() as raw_path:
            gateway = _gateway_server(
                raw_path,
                routing_file=raw_path / "routes.sqlite",
                registry_url="http://registry.invalid:5000",
            )
            with _running_server(gateway) as base:
                with patch.object(
                    control_plane.RegistryClient,
                    "ensure_digest_protection_tag",
                    side_effect=OSError("registry unavailable"),
                ):
                    prepared = self._json_request(
                        f"{base}/v1/capacity/prepare",
                        method="POST",
                        payload={
                            "id": "unprotected-digest",
                            "count": 1,
                            "ttl_seconds": 60,
                            "image": (f"registry.invalid:5000/repo/a:v1@{digest}"),
                            "cpus": 1,
                            "memory_mb": 512,
                        },
                        allow_error=True,
                    )
                stored_prepared = RoutingStore(
                    raw_path / "routes.sqlite"
                ).prepared_capacity()

        self.assertEqual(prepared["status"], 400)
        self.assertTrue(prepared["body"]["retryable"])
        self.assertEqual(stored_prepared, [])

    def test_failed_sandbox_pull_persists_incarnation_demand_until_cancel(self) -> None:
        with _temporary_root() as raw_path:
            route_file = raw_path / "routes.sqlite"
            gateway = _gateway_server(
                raw_path,
                routing_file=route_file,
            )
            gateway.RequestHandlerClass._ensure_image_on_node = (  # type: ignore[attr-defined]
                lambda _self, _heartbeat, _image: control_plane.ProxiedResponse(
                    502,
                    {"Content-Type": "application/json"},
                    b'{"error":"registry unavailable"}',
                )
            )
            with _running_server(gateway) as base:
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

        with _temporary_root() as raw_path:
            route_file = raw_path / "routes.sqlite"
            usage_file = raw_path / "registry-usage.json"
            node = ThreadingHTTPServer(("127.0.0.1", 0), FailingDeleteNode)
            with _running_server(node) as node_url:
                routing_store = RoutingStore(route_file)
                routing_store.upsert_sandbox(
                    _sandbox_route(
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
                        spec_hash="3" * 64,
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
                        deployment_id="test-deployment",
                    ),
                    digest="sha256:" + "a" * 64,
                )
                gateway = _gateway_server(
                    raw_path,
                    routing_file=route_file,
                    registry_usage_file=usage_file,
                )
                with _running_server(gateway) as base:
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

        with _temporary_root() as raw_path:
            route_file = raw_path / "routes.sqlite"
            usage_file = raw_path / "registry-usage.json"
            node = ThreadingHTTPServer(("127.0.0.1", 0), SuccessfulDeleteNode)
            with _running_server(node) as node_url:
                routing_store = RoutingStore(route_file)
                routing_store.upsert_sandbox(
                    _sandbox_route(
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
                        spec_hash="4" * 64,
                    )
                )
                stored_route = routing_store.get_sandbox("delete-success")
                assert stored_route is not None
                RegistryUsageStore(usage_file).acquire_reference(
                    "repo",
                    "v1",
                    control_plane._registry_route_reference_owner(
                        stored_route,
                        deployment_id="test-deployment",
                    ),
                    digest="sha256:" + "a" * 64,
                )
                gateway = _gateway_server(
                    raw_path,
                    routing_file=route_file,
                    registry_usage_file=usage_file,
                )
                with _running_server(gateway) as base:
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

        self.assertTrue(deleted["deleted"])
        self.assertEqual(leases, {})
        self.assertIsNone(route)

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

        with _temporary_root() as root:
            heartbeat_file, route_file = _seed_gateway_node(
                root,
                node_url="http://node.invalid:8090",
                sandbox_id="bounded-one",
            )
            gateway = _gateway_server(
                root,
                heartbeat_file=heartbeat_file,
                routing_file=route_file,
            )
            with _running_server(gateway):
                host, port = gateway.server_address
                with (
                    patch.object(control_plane, "DEFAULT_MAX_PROXY_RESPONSE_BYTES", 8),
                    patch.object(
                        control_plane,
                        "_open_node_request",
                        return_value=OversizedResponse(),
                    ),
                ):
                    conn = HTTPConnection(host, port, timeout=5)
                    try:
                        conn.request("GET", "/v1/sandboxes/bounded-one")
                        response = conn.getresponse()
                        payload = json.loads(response.read().decode("utf-8"))
                    finally:
                        conn.close()

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

        with _temporary_root() as root:
            heartbeat_file, route_file = _seed_gateway_node(
                root,
                node_url="http://node.invalid:8090",
                sandbox_id="file-one",
            )
            gateway = _gateway_server(
                root,
                heartbeat_file=heartbeat_file,
                routing_file=route_file,
            )
            with _running_server(gateway):
                host, port = gateway.server_address
                with patch.object(
                    control_plane,
                    "_open_node_request",
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

        self.assertEqual(response.status, 200)
        self.assertEqual(downloaded, body)
        self.assertEqual(response_path, "/tmp/large.bin")
        self.assertGreater(len(read_sizes), 2)
        self.assertTrue(
            all(
                size is not None and size <= control_plane.PROXY_STREAM_CHUNK_BYTES
                for size in read_sizes
            )
        )

    def test_content_addressed_context_survives_503_and_streams_to_builder(
        self,
    ) -> None:
        archive = _tar_gz_context({"Dockerfile": b"FROM scratch\n"})
        digest = f"sha256:{hashlib.sha256(archive).hexdigest()}"
        with _temporary_root() as raw_path:
            gateway_contexts = raw_path / "gateway-contexts"
            builder_contexts = raw_path / "builder-contexts"
            runtime = ContextRecordingRuntime()
            builder = build_builder_node_agent_server(
                "127.0.0.1",
                0,
                state_file=raw_path / "builder-sandboxes.json",
                image_file=raw_path / "builder-images.json",
                job_id="job-builder",
                node_id="builder-1",
                image_runtime=runtime,
                node_control_bearer_token="node-secret",
                build_context_store_dir=builder_contexts,
            )
            gateway = _gateway_server(
                raw_path,
                routing_file=raw_path / "routes.json",
                gateway_bearer_token="gateway-secret",
                node_control_bearer_token="node-secret",
                build_context_store_dir=gateway_contexts,
            )
            with (
                _running_server(builder) as builder_url,
                _running_server(gateway) as base,
            ):

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

                heartbeat = post_heartbeat_with_headers(
                    f"{base}/v1/nodes/heartbeat",
                    build_heartbeat(
                        job_id="job-builder",
                        node_id="builder-1",
                        node_url=builder_url,
                        capabilities=("image-cache", "image-build", "snapshot"),
                        total_resources=ResourceQuantity(
                            vcpu=16, memory_mb=49152, disk_mb=200000
                        ),
                    ),
                    {"Authorization": "Bearer test-heartbeat-secret"},
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
            self.assertEqual(
                runtime.dockerfiles,
                [b"FROM scratch\n", b"FROM scratch\n"],
            )
            self.assertTrue(runtime.context_paths)
            self.assertTrue(all(not path.exists() for path in runtime.context_paths))
            self.assertTrue(
                (builder_contexts / "sha256" / digest.removeprefix("sha256:")).is_file()
            )

    def test_gateway_records_pending_demand_when_no_node_can_fit(self) -> None:
        with _temporary_root() as raw_path:
            gateway = _gateway_server(
                raw_path,
                routing_file=raw_path / "routes.json",
            )
            with _running_server(gateway) as base:
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

    def test_registry_resolution_failure_does_not_trust_mutable_tag_heartbeat(
        self,
    ) -> None:
        mutable = "127.0.0.1:9/team/image:v1"

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

        with _temporary_root() as raw_path:
            node = ThreadingHTTPServer(("127.0.0.1", 0), ImageNode)
            with _running_server(node) as node_url:
                gateway = _gateway_server(
                    raw_path,
                    routing_file=raw_path / "routes.sqlite",
                    registry_url="http://127.0.0.1:9",
                )
                with _running_server(gateway) as base:
                    self.assertEqual(
                        post_heartbeat(
                            f"{base}/v1/nodes/heartbeat",
                            build_heartbeat(
                                job_id="job-1",
                                node_id="node-1",
                                node_url=node_url,
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

        self.assertEqual(created["status"], 502)
        self.assertEqual(ImageNode.pull_count, 1)

    def test_retryable_node_image_pull_is_retried_within_route_fence(self) -> None:
        handler = object.__new__(control_plane.ControlPlaneHandler)
        responses = [
            control_plane.ProxiedResponse(
                503,
                {"Content-Type": "application/json"},
                b'{"error_code":"image_pull_failed","retryable":true}',
            ),
            control_plane.ProxiedResponse(
                503,
                {"Content-Type": "application/json"},
                b'{"error_code":"image_pull_failed","retryable":true}',
            ),
            control_plane.ProxiedResponse(
                200,
                {"Content-Type": "application/json"},
                b'{"image":{"id":"sha256:ready"}}',
            ),
        ]
        calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

        def proxy(*args, **kwargs):
            calls.append((args, kwargs))
            return responses.pop(0)

        handler._proxy_request = proxy
        heartbeat = build_heartbeat(
            node_id="node-1",
            job_id="job-1",
            node_url="http://node-1:8090",
        )

        with patch.object(control_plane.time, "sleep") as sleeper:
            response = handler._pull_image_on_node(heartbeat, "image")

        self.assertEqual(response.status, 200)
        self.assertEqual(len(calls), 3)
        self.assertEqual(
            [call.args[0] for call in sleeper.call_args_list],
            [0.25, 0.5],
        )

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

        with _temporary_root() as raw_path:
            node = ThreadingHTTPServer(("127.0.0.1", 0), CountingNode)
            gateway = _gateway_server(
                raw_path,
                routing_file=raw_path / "routes.sqlite",
            )
            with (
                _running_server(node) as node_url,
                _running_server(gateway) as base,
            ):
                self.assertEqual(
                    post_heartbeat(
                        f"{base}/v1/nodes/heartbeat",
                        build_heartbeat(
                            job_id="job-1",
                            node_id="node-1",
                            node_url=node_url,
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

        self.assertEqual(response["status"], 409)
        self.assertEqual(CountingNode.creates, 0)

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
