"""Cross-component regressions for lifecycle, persistence and storage boundaries."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import replace
import json
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from tests import test_control_plane as gateway_fixtures
from tests import test_direct_provisioner as direct_fixtures
from tests import test_storage_native_daemon as storage_fixtures
from tests import test_storage_native_s3 as s3_fixtures
from tests import test_storage_native_s3_gc as gc_fixtures
from ucloud_sandboxes.control_plane import ControlPlaneHandler
from ucloud_sandboxes.direct_registry import (
    DirectRegistryConflictError,
    DirectRegistryError,
)
from ucloud_sandboxes.direct_service import DirectProcessRunner, DirectSandboxService
from ucloud_sandboxes.direct_warden import DirectWardenError
from ucloud_sandboxes.images import (
    DockerImageRuntime,
    ImageBuildSpec,
    ImageManager,
    ImageStore,
    MaterializedBuildContext,
)
from ucloud_sandboxes.models import NodeRuntimeMetrics, ResourceQuantity, utc_now
from ucloud_sandboxes.node_runtime import DirectNodeRuntime
from ucloud_sandboxes.node_agent import NodeAgentHandler
from ucloud_sandboxes.models import SandboxInventoryEntry
from ucloud_sandboxes.routing import RoutingStore
from ucloud_sandboxes.sandbox import (
    CommandResult,
    SandboxAdmissionClosedError,
    SandboxCapacityUnavailableError,
    SandboxFileTooLargeError,
)
from ucloud_sandboxes.storage_native_daemon import (
    StorageNativeNodeClient,
    StorageVolumeState,
    _StorageNativeUnixServer,
)
from ucloud_sandboxes.storage_native_migration import StorageNativeMigration
from ucloud_sandboxes.storage_native_s3_gc import (
    plan_s3_snapshot_gc,
    execute_s3_snapshot_gc,
)
from ucloud_sandboxes.telemetry import Telemetry


class LifecycleBoundaryTests(unittest.TestCase):
    def test_delayed_delete_cannot_destroy_replacement_generation(self):
        with TemporaryDirectory() as directory:
            fixture = direct_fixtures.DirectProvisionerTests()
            provisioner, registry, *_ = fixture.make(Path(directory).resolve())
            service = DirectSandboxService(provisioner)
            fixture.create(service, fixture.spec())
            entered, resume = threading.Event(), threading.Event()
            original = service._lock

            @contextmanager
            def delayed_lock(sandbox_id, generation):
                if threading.current_thread().name.startswith("stale-delete"):
                    entered.set()
                    self.assertTrue(resume.wait(5))
                with original(sandbox_id, generation):
                    yield

            with patch.object(service, "_lock", side_effect=delayed_lock):
                with ThreadPoolExecutor(
                    max_workers=1, thread_name_prefix="stale-delete"
                ) as pool:
                    stale = pool.submit(service.delete, "sandbox", generation=7)
                    try:
                        self.assertTrue(entered.wait(5))
                        service.delete("sandbox", generation=7)
                        fixture.create(service, fixture.spec(), generation=8)
                    finally:
                        resume.set()
                    with self.assertRaises((DirectWardenError, DirectRegistryError)):
                        stale.result(timeout=5)
            self.assertEqual(registry.get("sandbox").sandbox_generation, 8)

    def test_delete_transaction_fences_generation_even_when_revision_matches(self):
        with TemporaryDirectory() as directory:
            fixture = direct_fixtures.DirectProvisionerTests()
            provisioner, registry, *_ = fixture.make(Path(directory).resolve())
            service = DirectSandboxService(provisioner)
            fixture.create(service, fixture.spec(), generation=8)
            current = registry.get("sandbox")
            with self.assertRaises(DirectRegistryConflictError):
                registry.begin_delete(
                    "sandbox", expected_revision=current.revision, expected_generation=7
                )
            self.assertEqual(registry.get("sandbox"), current)

    def test_explicit_wake_checks_drain_and_live_pressure(self):
        for closed, cpu, memory, error in (
            (True, 0, 8192, SandboxAdmissionClosedError),
            (False, 99, 8192, SandboxCapacityUnavailableError),
            (False, 0, 10, SandboxCapacityUnavailableError),
        ):
            with (
                self.subTest(closed=closed, cpu=cpu, memory=memory),
                TemporaryDirectory() as directory,
            ):
                fixture = direct_fixtures.DirectProvisionerTests()
                provisioner, *_ = fixture.make(Path(directory).resolve())
                service = DirectSandboxService(provisioner)
                fixture.create(service, fixture.spec())
                service.park("sandbox", operation_id="park:test")
                service.configure_active_capacity(
                    ResourceQuantity(vcpu=4, memory_mb=8192),
                    runtime_metrics_provider=lambda: NodeRuntimeMetrics(
                        collected_at=utc_now(),
                        cpu_percent=cpu,
                        cpu_count=4,
                        memory_total_mb=8192,
                        memory_available_mb=memory,
                    ),
                )
                runtime = DirectNodeRuntime(service)
                if closed:
                    service.close_admission()
                with self.assertRaises(error):
                    runtime.wake("sandbox", generation=7, operation_id="wake:test")
                sandbox = provisioner.registry.get("sandbox").to_direct_sandbox()
                self.assertEqual(service.warden.inspect(sandbox).state.value, "parked")

    def test_explicit_wake_waits_for_restore_slot(self):
        with TemporaryDirectory() as directory:
            fixture = direct_fixtures.DirectProvisionerTests()
            provisioner, *_ = fixture.make(Path(directory).resolve())
            service = DirectSandboxService(provisioner, max_concurrent_restores=1)
            fixture.create(service, fixture.spec())
            service.park("sandbox", operation_id="park:test")
            admitted = threading.Event()
            original = service._reserve_active_capacity

            @contextmanager
            def reserve(*args):
                with original(*args):
                    admitted.set()
                    yield

            with patch.object(service, "_reserve_active_capacity", side_effect=reserve):
                with ThreadPoolExecutor(max_workers=1) as pool:
                    service._restore_slots.acquire()
                    future = pool.submit(
                        service.wake, "sandbox", generation=7, operation_id="wake:test"
                    )
                    try:
                        self.assertTrue(admitted.wait(5))
                        self.assertFalse(future.done())
                    finally:
                        service._restore_slots.release()
                    self.assertEqual(future.result(timeout=5).state, "running")


class ProcessDeadlineTests(unittest.TestCase):
    def run_python(self, script, *, timeout=0.2, input_bytes=None, limit=1024):
        return DirectProcessRunner().run(
            (sys.executable, "-c", script),
            input_bytes=input_bytes,
            timeout_seconds=timeout,
            max_stdout_bytes=limit,
            max_stderr_bytes=limit,
        )

    def test_deadline_includes_descendant_output_pipes(self):
        started = time.monotonic()
        with self.assertRaisesRegex(DirectWardenError, "timed out"):
            self.run_python(
                'import subprocess,sys; subprocess.Popen([sys.executable,"-c","import time; time.sleep(3)"])'
            )
        self.assertLess(time.monotonic() - started, 2)

    def test_overflow_kills_a_process_that_ignores_sigterm(self):
        started = time.monotonic()
        with self.assertRaises(SandboxFileTooLargeError):
            self.run_python(
                'import signal,os,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); os.write(1,b"x"*65536); time.sleep(3)',
                timeout=1,
            )
        self.assertLess(time.monotonic() - started, 2)

    def test_blocked_input_obeys_deadline(self):
        started = time.monotonic()
        with self.assertRaisesRegex(DirectWardenError, "timed out"):
            self.run_python(
                "import time; time.sleep(3)", input_bytes=b"x" * (1024 * 1024)
            )
        self.assertLess(time.monotonic() - started, 2)

    def test_full_duplex_binary_io(self):
        payload = bytes(range(256)) * 1024
        result = self.run_python(
            'import sys; data=sys.stdin.buffer.read(); sys.stdout.buffer.write(data); sys.stderr.buffer.write(b"err")',
            input_bytes=payload,
            timeout=5,
            limit=len(payload),
        )
        self.assertEqual(
            (result.exit_code, result.stdout, result.stderr), (0, payload, b"err")
        )


class StorageBoundaryTests(unittest.TestCase):
    def test_reconciliation_waits_for_inflight_create(self):
        with TemporaryDirectory() as directory:
            service, backend, _ = (
                storage_fixtures.StorageNativeNodeServiceTests()._service(
                    Path(directory).resolve()
                )
            )
            pending, resume, reconciling = (
                threading.Event(),
                threading.Event(),
                threading.Event(),
            )
            original = service.journal.finish

            def finish(record):
                pending.set()
                self.assertTrue(resume.wait(5))
                return original(record)

            def reconcile():
                reconciling.set()
                return service.reconcile()

            with patch.object(service.journal, "finish", side_effect=finish):
                with ThreadPoolExecutor(max_workers=2) as pool:
                    creation = pool.submit(
                        service.create_volume,
                        sandbox_id="s",
                        sandbox_generation=1,
                        volume_id="v",
                        operation_id="create:v",
                        virtual_size=4096,
                    )
                    try:
                        self.assertTrue(pending.wait(5))
                        recovery = pool.submit(reconcile)
                        self.assertTrue(reconciling.wait(5))
                        # The maintenance request must not interpret CREATING as
                        # an interrupted operation while its worker is live.
                        time.sleep(0.05)
                        self.assertFalse(recovery.done())
                    finally:
                        resume.set()
                    record = creation.result(timeout=5)
                    self.assertEqual(recovery.result(timeout=5)["terminal_records"], [])
            self.assertEqual(
                service.journal.load("v").state, StorageVolumeState.MOUNTED
            )
            self.assertIn(record.device_id, backend.live)

    def test_creation_waits_for_reconciliation_snapshot(self):
        with TemporaryDirectory() as directory:
            service, backend, _ = (
                storage_fixtures.StorageNativeNodeServiceTests()._service(
                    Path(directory).resolve()
                )
            )
            captured, resume, creating = (
                threading.Event(),
                threading.Event(),
                threading.Event(),
            )
            original = service.journal.list

            def snapshot():
                records = original()
                captured.set()
                self.assertTrue(resume.wait(5))
                return records

            def create():
                creating.set()
                return service.create_volume(
                    sandbox_id="s",
                    sandbox_generation=1,
                    volume_id="v",
                    operation_id="create:v",
                    virtual_size=4096,
                )

            with patch.object(service.journal, "list", side_effect=snapshot):
                with ThreadPoolExecutor(max_workers=2) as pool:
                    recovery = pool.submit(service.reconcile)
                    try:
                        self.assertTrue(captured.wait(5))
                        creation = pool.submit(create)
                        self.assertTrue(creating.wait(5))
                        time.sleep(0.05)
                        self.assertFalse(creation.done())
                    finally:
                        resume.set()
                    self.assertEqual(
                        recovery.result(timeout=5)["deleted_orphan_device_ids"], []
                    )
                    record = creation.result(timeout=5)
            self.assertIn(record.device_id, backend.live)

    def test_live_inventory_pages_and_retains_deleted_replay_records(self):
        with TemporaryDirectory(prefix="ucr-", dir="/tmp") as directory:
            root = Path(directory).resolve()
            service, _, _ = storage_fixtures.StorageNativeNodeServiceTests()._service(
                root
            )
            record = service.create_volume(
                sandbox_id="s",
                sandbox_generation=1,
                volume_id="v",
                operation_id="create:v",
                virtual_size=4096,
            )
            deleted = service.delete_volume(
                sandbox_id="s",
                sandbox_generation=1,
                volume_id="v",
                operation_id="delete:v",
                expected_revision=record.revision,
            )
            with service.journal._connect() as conn:
                for i in range(2000):
                    service.journal._upsert_record(
                        conn,
                        replace(
                            deleted,
                            volume_id=f"dead-{i:04}",
                            sandbox_id=f"s-{i}",
                            accounting_id=i + 2,
                        ),
                    )
                for i in range(260):
                    service.journal._upsert_record(
                        conn,
                        replace(
                            record,
                            volume_id=f"live-{i:04}",
                            sandbox_id=f"live-{i}",
                            accounting_id=i + 3000,
                        ),
                    )
            server = _StorageNativeUnixServer(
                root / "s.sock",
                service,
                require_root_peer=False,
                telemetry=Telemetry.disabled("test"),
            )
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                client = StorageNativeNodeClient(root / "s.sock")
                with patch.object(client, "_call", wraps=client._call) as calls:
                    inventory = client.list_volumes()
                self.assertGreater(calls.call_count, 2)
                self.assertEqual(
                    [r.volume_id for r in inventory],
                    [f"live-{i:04}" for i in range(260)],
                )
                self.assertEqual(
                    client.get_volume("v").state, StorageVolumeState.DELETED
                )
                self.assertEqual(len(service.journal.list()), 2261)
            finally:
                server.shutdown()
                server.server_close()
                thread.join(5)

    def test_wake_retires_restore_authority_but_keeps_remote_layers_live(self):
        with TemporaryDirectory(prefix="ucg-", dir="/tmp") as directory:
            root = Path(directory).resolve()
            s3 = s3_fixtures.FakeS3()
            publication = gc_fixtures._publish_live_snapshot(root, s3)
            service, _, _ = storage_fixtures.StorageNativeNodeServiceTests()._service(
                root, publisher=True
            )
            imported = service.acquire_snapshot(
                sandbox_id="s",
                sandbox_generation=1,
                volume_id="v",
                operation_id="import:v",
                publication_raw=publication.to_dict(),
            )
            mounted = service.mount_snapshot_cow(
                sandbox_id="s",
                sandbox_generation=1,
                volume_id="v",
                operation_id="mount:v",
                expected_revision=imported.revision,
            )
            self.assertTrue(mounted.published_layers)
            self.assertEqual(
                json.loads(Path(mounted.source_image_config).read_text())[
                    "repoBlobUrl"
                ],
                publication.repo_blob_url,
            )
            migration = replace(
                gateway_fixtures._portable_snapshot("s"), publication=publication
            )
            store = RoutingStore(root / "routes.sqlite")
            route = store.upsert_sandbox(
                gateway_fixtures._sandbox_route(
                    sandbox_id="s",
                    node_id="node",
                    job_id="job",
                    node_url="http://node",
                    state="waking",
                    storage_schema="ucloud-storage-native-v1",
                    node_epoch="boot",
                    snapshot_manifest_digest=publication.manifest_digest,
                    snapshot_repository=publication.repository,
                    snapshot_tag=publication.tag,
                    storage_snapshot=migration.to_dict(),
                )
            )
            handler = SimpleNamespace(
                routing_store=store,
                _lifecycle_response_fence=lambda *args: ("boot", 11),
                _release_registry_snapshot_reference=lambda *args, **kwargs: None,
                _write_json=lambda *args, **kwargs: self.fail("wake commit failed"),
            )
            running = ControlPlaneHandler._commit_successful_wake(handler, route, None)
            self.assertEqual(running.state, "running")
            self.assertEqual(running.storage_snapshot, {})
            store = RoutingStore(store.path)
            dependencies = [
                StorageNativeMigration.from_dict(raw).publication
                for raw in store.storage_snapshot_dependencies_readonly()
            ]
            live_key = f"production/managed-layers/{publication.layers[0].digest}"
            plan = plan_s3_snapshot_gc(
                s3,
                prefix="production",
                publications=dependencies,
                now=1_000_000,
                grace_seconds=0,
            )
            self.assertIn(live_key, plan.protected)
            self.assertNotIn(live_key, plan.markers_to_create)
            store.delete_sandbox_if_current("s", generation=1)
            self.assertEqual(store.storage_snapshot_dependencies_readonly(), [])
            unreferenced = plan_s3_snapshot_gc(
                s3, prefix="production", publications=[], now=1_000_000, grace_seconds=0
            )
            execute_s3_snapshot_gc(
                s3,
                unreferenced,
                max_delete_objects=100,
                revalidate=lambda: unreferenced,
            )
            expired = plan_s3_snapshot_gc(
                s3, prefix="production", publications=[], now=1_000_001, grace_seconds=0
            )
            self.assertIn(live_key, expired.candidates)

    def test_running_heartbeat_recovers_previously_unobserved_remote_dependencies(self):
        with TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            fixture = direct_fixtures.DirectProvisionerTests()
            provisioner, *_ = fixture.make(root)
            service = DirectSandboxService(provisioner)
            created = fixture.create(service, fixture.spec())
            publication = gateway_fixtures._portable_snapshot(
                "sandbox", generation=7
            ).publication
            storage = provisioner.warden.storage
            original = storage.records[("sandbox", 7)]
            publication = replace(publication, virtual_size=original.virtual_size)
            storage.records[("sandbox", 7)] = replace(
                original,
                published_manifest_digest=publication.manifest_digest,
                published_tag=publication.tag,
                published_repository=publication.repository,
                published_repo_blob_url=publication.repo_blob_url,
                published_backend=publication.backend,
                published_layers=tuple(layer.to_dict() for layer in publication.layers),
            )
            runtime = DirectNodeRuntime(service)
            snapshot = runtime.heartbeat_snapshot(active_build_count=lambda: 0)
            handler = SimpleNamespace(manager=runtime)
            entry = NodeAgentHandler._sandbox_inventory_entry(
                handler,
                snapshot.activity.records[0],
                storage_dependency=snapshot.storage_dependencies["sandbox"],
            )
            entry = SandboxInventoryEntry.from_dict(entry.to_dict())
            self.assertIsNotNone(entry)
            self.assertEqual(entry.state, "running")
            self.assertEqual(entry.storage_snapshot, {})
            self.assertEqual(
                entry.storage_dependency["manifest_digest"], publication.manifest_digest
            )
            store = RoutingStore(root / "routes.sqlite")
            store.upsert_sandbox(
                gateway_fixtures._sandbox_route(
                    sandbox_id="sandbox",
                    node_id="node",
                    job_id="job",
                    node_url="http://node",
                    state="running",
                    node_epoch="boot",
                    generation=7,
                    create_operation_id=created.operation_id,
                    spec_hash=created.spec_hash,
                )
            )
            with self.assertRaisesRegex(ValueError, "cannot GC"):
                store.storage_snapshot_dependencies_readonly(require_complete=True)
            for observation in (entry, replace(entry, storage_dependency={})):
                store.reconcile_sandboxes_for_node(
                    "http://node",
                    (observation,),
                    node_id="node",
                    job_id="job",
                    reported_sandbox_ids=("sandbox",),
                    observed_at=utc_now().isoformat(),
                    node_epoch="boot",
                    activity_epoch=1,
                )
                # A delayed empty publication observation must not erase a
                # remote dependency that has already been learned.
                dependencies = store.storage_snapshot_dependencies_readonly(
                    require_complete=True
                )
                self.assertEqual(dependencies, [{"publication": publication.to_dict()}])
            self.assertEqual(store.get_sandbox_readonly("sandbox").storage_snapshot, {})

    def test_local_dependency_report_allows_gc_without_a_publication(self):
        with TemporaryDirectory() as directory:
            store = RoutingStore(Path(directory) / "routes.sqlite")
            route = store.upsert_sandbox(
                gateway_fixtures._sandbox_route(
                    sandbox_id="local",
                    node_id="node",
                    job_id="job",
                    node_url="http://node",
                    state="running",
                    node_epoch="boot",
                )
            )
            entry = SandboxInventoryEntry(
                sandbox_id="local",
                generation=route.generation,
                operation_id=route.create_operation_id,
                spec_hash=route.spec_hash,
                state="running",
                storage_dependency={},
            )
            store.reconcile_sandboxes_for_node(
                "http://node",
                (entry,),
                node_id="node",
                job_id="job",
                reported_sandbox_ids=("local",),
                observed_at=utc_now().isoformat(),
                node_epoch="boot",
                activity_epoch=1,
            )
            self.assertEqual(
                store.storage_snapshot_dependencies_readonly(require_complete=True), []
            )


class BuildPersistenceTests(unittest.TestCase):
    def test_terminal_result_is_retried_after_worker_exits(self):
        entered, release = threading.Event(), threading.Event()

        class Runtime(DockerImageRuntime):
            def build(self, spec, **kwargs):
                entered.set()
                if not release.wait(5):
                    raise RuntimeError("test worker timed out")
                return CommandResult(argv=("docker",), exit_code=0)

        with TemporaryDirectory() as directory:
            manager = ImageManager(
                ImageStore(Path(directory) / "images.sqlite"),
                Runtime(dry_run=True),
                max_active_builds=1,
            )
            identity = "archive:sha256:" + "a" * 64

            def materialize():
                temporary = TemporaryDirectory()
                return MaterializedBuildContext(
                    Path(temporary.name), temporary, identity
                )

            record, _ = manager.start_build(
                ImageBuildSpec(id="image", tag="tag", context_path="."),
                context_identity=identity,
                materialize_context=materialize,
            )
            self.assertTrue(entered.wait(5))
            worker = manager._active_threads[record.build_id]
            with (
                patch.object(
                    manager.build_store, "upsert", side_effect=OSError("disk full")
                ),
                patch("threading.excepthook"),
            ):
                release.set()
                worker.join(5)
                self.assertFalse(worker.is_alive())
            self.assertEqual(manager.build_store.get(record.build_id).status, "running")
            self.assertEqual(manager.active_build_count(), 0)
            self.assertEqual(manager.get_build(record.build_id).status, "failed")
            following, started = manager.start_build(
                ImageBuildSpec(id="next", tag="next", context_path="."),
                context_identity=identity,
                materialize_context=materialize,
            )
            self.assertTrue(started)
            self.assertEqual(
                manager.wait_for_build(following.build_id, timeout_seconds=5).status,
                "succeeded",
            )
