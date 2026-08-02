from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from tempfile import TemporaryDirectory
from threading import Event, Lock, Thread
from urllib import request
import hashlib
import json
import shutil
import unittest

from ucloud_sandboxes.direct_oci import DirectOciConfigBuilder
from ucloud_sandboxes.direct_node_adapter import (
    DirectNodeManagerAdapter,
    DirectNodeStateStore,
)
from ucloud_sandboxes.direct_migration import DirectMigrationArchiveStore
from ucloud_sandboxes.direct_provisioner import DirectSandboxProvisioner
from ucloud_sandboxes.direct_service import DirectExecResult, DirectSandboxService
from ucloud_sandboxes.direct_registry import (
    DirectRegistryError,
    DirectSandboxRegistration,
    DirectSandboxRegistry,
)
from ucloud_sandboxes.direct_warden import DirectSandbox
from ucloud_sandboxes.hibernation import (
    HibernationArtifactFile,
    HibernationArtifactStore,
    HibernationDiskLedger,
    HibernationFileRole,
    HibernationManifest,
    HibernationRuntimeFingerprint,
    HibernationState,
)
from ucloud_sandboxes.image_rootfs import (
    DockerImageConfig,
    MaterializedRootfs,
    OverlayRootfsLease,
)
from ucloud_sandboxes.runtime_identity import NodeRuntimeIdentityStore
from ucloud_sandboxes.node_agent import build_direct_node_agent_server
from ucloud_sandboxes.models import NodeRuntimeMetrics, ResourceQuantity, utc_now
from ucloud_sandboxes.sandbox import (
    SandboxAdmissionClosedError,
    SandboxBusyError,
    SandboxCapacityUnavailableError,
    SandboxSecuritySpec,
    SandboxSpec,
)


class FakeImageStore:
    def __init__(self, root: Path) -> None:
        rootfs = root / "image-rootfs"
        rootfs.mkdir()
        self.image = MaterializedRootfs(
            image_ref="image",
            image_id="sha256:" + "a" * 64,
            rootfs_identity_sha256="b" * 64,
            rootfs=rootfs,
            image_config=DockerImageConfig(command=("sleep", "3600")),
        )
        self.reconciled = False
        self.materialized_refs: list[str] = []

    def materialize(self, image_ref: str) -> MaterializedRootfs:
        if image_ref != "image":
            raise AssertionError("wrong image")
        self.materialized_refs.append(image_ref)
        return self.image

    def reconcile_export_containers(self) -> tuple[str, ...]:
        self.reconciled = True
        return ()

    def operation_snapshot(self) -> dict[str, int]:
        return {
            "active_operations": 0,
            "waiting_operations": 0,
            "max_concurrent_operations": 4,
        }


class FakeQuota:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.items: dict[tuple[str, int], dict] = {}

    def prepare(self, reservation):
        path = self.root / (
            f"{reservation.sandbox_id}.sandbox-{reservation.sandbox_generation}"
        )
        path.mkdir(mode=0o700, parents=True, exist_ok=True)
        payload = {
            "hard_limit_mb": reservation.total_mb,
            "path": str(path),
            "project_id": reservation.project_id,
            "sandbox_generation": reservation.sandbox_generation,
            "sandbox_id": reservation.sandbox_id,
            "state": "ready",
        }
        self.items[(reservation.sandbox_id, reservation.sandbox_generation)] = payload
        return payload

    def drop(self, reservation):
        key = (reservation.sandbox_id, reservation.sandbox_generation)
        payload = self.items.pop(key, None)
        if payload is not None:
            shutil.rmtree(payload["path"])
        return {
            "project_id": reservation.project_id,
            "removed": payload is not None,
            "sandbox_generation": reservation.sandbox_generation,
            "sandbox_id": reservation.sandbox_id,
            "state": "absent",
        }

    def inventory(self):
        return tuple(self.items.values())


class FakeOverlays:
    require_precreated_writable = True

    def __init__(self, image_store: FakeImageStore, root: Path) -> None:
        self.image_store = image_store
        self.writable_root = root / "quota"
        self.bundle_root = root / "bundles"
        self.writable_root.mkdir()
        self.bundle_root.mkdir()

    def discard_unregistered(self, *, sandbox_id, sandbox_generation):
        bundle = self.bundle_root / f"{sandbox_id}.sandbox-{sandbox_generation}"
        if bundle.exists():
            shutil.rmtree(bundle)

    def prepare(
        self,
        *,
        sandbox_id,
        sandbox_generation,
        image_ref,
        config_template,
        spec_sha256,
        imported_parked=False,
    ):
        del config_template
        image = self.image_store.materialize(image_ref)
        incarnation = f"{sandbox_id}.sandbox-{sandbox_generation}"
        writable = self.writable_root / incarnation
        if not writable.is_dir():
            raise AssertionError("quota was not prepared first")
        upper = writable / "upper"
        work = writable / "work"
        if not imported_parked:
            upper.mkdir()
        work.mkdir()
        bundle = self.bundle_root / incarnation
        merged = bundle / "rootfs"
        merged.mkdir(parents=True)
        sandbox = DirectSandbox(
            sandbox_id=sandbox_id,
            sandbox_generation=sandbox_generation,
            container_id=hashlib.sha256(
                f"{sandbox_id}:{sandbox_generation}".encode("utf-8")
            ).hexdigest(),
            spec_sha256=spec_sha256,
            rootfs_sha256=image.rootfs_identity_sha256,
            bundle=bundle,
            memory_directory=incarnation,
        )
        return OverlayRootfsLease(
            sandbox=sandbox,
            image=image,
            writable=writable,
            upper=upper,
            work=work,
            merged=merged,
            writable_owned_by_manager=False,
        )

    def release_sandbox(self, sandbox):
        if sandbox.bundle.exists():
            shutil.rmtree(sandbox.bundle)


class FakeWarden:
    def __init__(self, root: Path) -> None:
        fingerprint = HibernationRuntimeFingerprint(
            runsc_sha256="d" * 64,
            runsc_commit="e" * 40,
            platform="systrap",
            architecture="x86_64",
            page_size=4096,
            cpu_features_sha256="f" * 64,
            boot_config_sha256="1" * 64,
            rootfs_sha256="2" * 64,
        )
        quota = root / "quota"
        bundles = root / "bundles"
        self.config = SimpleNamespace(
            artifact_root=quota,
            bundle_root=bundles,
            memory_root=quota,
            network="none",
            remove_memory_directory_on_delete=False,
            runtime_fingerprint=fingerprint,
        )
        self.artifacts = HibernationArtifactStore(quota)
        self.records = {}
        self.discarded = []
        self.alive = True

    @staticmethod
    def key(sandbox):
        return sandbox.sandbox_id, sandbox.sandbox_generation

    def inspect(self, sandbox):
        return self.records.get(self.key(sandbox))

    def inspect_snapshot(self, sandbox):
        return self.records.get(self.key(sandbox))

    def discard_unjournaled(self, sandbox):
        if self.inspect(sandbox) is not None:
            raise AssertionError("journal already exists")
        self.discarded.append(self.key(sandbox))

    def create(self, sandbox, *, operation_id):
        del operation_id
        record = SimpleNamespace(state=HibernationState.RUNNING)
        self.records[self.key(sandbox)] = record
        return record

    def reconcile(self, sandbox):
        if (
            not self.alive
            and self.records[self.key(sandbox)].state == HibernationState.RUNNING
        ):
            self.records[self.key(sandbox)] = SimpleNamespace(
                state=HibernationState.RECOVERY_REQUIRED
            )
        return self.records[self.key(sandbox)]

    def running_process_alive(self, sandbox):
        return (
            self.alive
            and self.records[self.key(sandbox)].state == HibernationState.RUNNING
        )

    def park(self, sandbox, *, operation_id):
        del operation_id
        record = SimpleNamespace(state=HibernationState.PARKED)
        self.records[self.key(sandbox)] = record
        return record

    def resume(self, sandbox, *, operation_id, timings=None):
        del operation_id
        if timings is not None:
            timings["runsc_restore"] = 1.0
        record = SimpleNamespace(state=HibernationState.RUNNING)
        self.records[self.key(sandbox)] = record
        return record

    def adopt_parked(self, sandbox, manifest):
        del manifest
        record = SimpleNamespace(state=HibernationState.PARKED)
        self.records[self.key(sandbox)] = record
        return record

    @contextmanager
    def exec_lease(self, sandbox, argv, *, env=None, working_dir=None, user=None):
        del env, working_dir, user
        yield ("runsc", "exec", sandbox.container_id, *argv)

    def delete(self, sandbox):
        self.records.pop(self.key(sandbox), None)


class FakeProcessRunner:
    def __init__(self) -> None:
        self.calls = []

    def run(
        self,
        argv,
        *,
        input_bytes,
        timeout_seconds,
        max_stdout_bytes,
        max_stderr_bytes,
    ):
        del timeout_seconds, max_stdout_bytes, max_stderr_bytes
        self.calls.append((tuple(argv), input_bytes))
        return DirectExecResult(tuple(argv), 0, b"ok\n", b"")


class DirectProvisionerTests(unittest.TestCase):
    def make(self, root: Path):
        images = FakeImageStore(root)
        overlays = FakeOverlays(images, root)
        quota = FakeQuota(overlays.writable_root)
        warden = FakeWarden(root)
        registry = DirectSandboxRegistry((root / "registry.json").resolve())
        ledger = HibernationDiskLedger(
            (root / "ledger.json").resolve(),
            capacity_mb=100_000,
            safety_headroom_mb=1_000,
        )
        provisioner = DirectSandboxProvisioner(
            identity_store=NodeRuntimeIdentityStore(
                (root / "runtime-identity.json").resolve()
            ),
            registry=registry,
            disk_ledger=ledger,
            quota_backend=quota,
            image_store=images,
            overlays=overlays,
            oci=DirectOciConfigBuilder(),
            warden=warden,
        )
        return provisioner, registry, ledger, quota, images, warden

    @staticmethod
    def spec() -> SandboxSpec:
        return SandboxSpec(
            id="sandbox",
            image="image",
            memory_mb=1024,
            disk_mb=2048,
            security=SandboxSecuritySpec(init=False),
        )

    def test_create_and_delete_order_all_owners(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            provisioner, registry, ledger, quota, _, warden = self.make(root)

            created = provisioner.create(
                spec=self.spec(),
                sandbox_generation=7,
                operation_id="create:7",
            )

            self.assertEqual(created.registration.phase, "owned")
            self.assertEqual(created.lifecycle_state, HibernationState.RUNNING)
            self.assertEqual(len(ledger.inventory().reservations), 1)
            self.assertEqual(len(quota.inventory()), 1)
            self.assertIn(("sandbox", 7), warden.discarded)

            provisioner.delete("sandbox")

            self.assertIsNone(registry.get("sandbox"))
            self.assertEqual(ledger.inventory().reservations, ())
            self.assertEqual(quota.inventory(), ())

    def test_import_stays_unavailable_until_explicit_activation(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            provisioner, registry, _, _, images, warden = self.make(root)
            source_root = root / "source"
            source = source_root / "sandbox.sandbox-7"
            generation = source / "hibernate-2"
            upper = source / "upper"
            generation.mkdir(parents=True)
            upper.mkdir()
            (upper / "payload").write_bytes(b"writable-state")
            roles = {
                "application_memory.img": HibernationFileRole.MAIN_MEMORY,
                "checkpoint.img": HibernationFileRole.KERNEL_STATE,
                "pages_meta.img": HibernationFileRole.ALLOCATOR_METADATA,
            }
            for name in roles:
                (generation / name).write_bytes(name.encode("ascii"))
            runtime = replace(
                warden.config.runtime_fingerprint,
                rootfs_sha256=images.image.rootfs_identity_sha256,
            )
            container_id = hashlib.sha256(b"sandbox:7").hexdigest()
            source_registration = DirectSandboxRegistration(
                spec=self.spec(),
                sandbox_generation=7,
                operation_id="create:7",
                runtime_identity_sha256=provisioner.identity.digest,
                phase="owned",
                revision=4,
                created_ns=1,
                updated_ns=1,
                quota_project_id=200_007,
                quota_total_mb=4096,
                quota_path=str(source),
                image_id=images.image.image_id,
                rootfs_sha256=images.image.rootfs_identity_sha256,
                container_id=container_id,
                bundle=str(source_root / "bundle"),
                memory_directory=source.name,
            )
            source_manifest = HibernationManifest(
                sandbox_id="sandbox",
                sandbox_generation=7,
                hibernation_generation=2,
                operation_id="park:source",
                spec_sha256=source_registration.spec_sha256,
                container_id=container_id,
                created_ns=1,
                runtime=runtime,
                files=tuple(
                    HibernationArtifactFile.from_path(
                        generation / name,
                        role=role,
                    )
                    for name, role in roles.items()
                ),
            )
            HibernationArtifactStore(source_root).publish_complete(source_manifest)
            archive_path = root / "migration.tar"
            archive = DirectMigrationArchiveStore().export(
                registration=source_registration,
                local_manifest=source_manifest,
                runtime_identity=provisioner.identity,
                writable_incarnation=source,
                archive_path=archive_path,
            )

            staged = provisioner.stage_import(
                archive_path,
                expected_sha256=archive.sha256,
                migration_id="move:7",
            )

            self.assertEqual(staged.registration.phase, "import_ready")
            self.assertEqual(staged.lifecycle_state, HibernationState.PARKED)
            self.assertEqual(registry.get("sandbox").phase, "import_ready")
            activated = provisioner.activate_import(
                "sandbox",
                migration_id="move:7",
                migration_sha256=archive.sha256,
            )
            self.assertEqual(activated.registration.phase, "owned")
            replay = provisioner.stage_import(
                archive_path,
                expected_sha256=archive.sha256,
                migration_id="move:7",
            )
            self.assertEqual(replay.registration.phase, "owned")

    def test_restart_advances_quota_ready_registration(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            provisioner, registry, ledger, quota, images, _ = self.make(root)
            planned = registry.plan(
                spec=self.spec(),
                sandbox_generation=9,
                operation_id="create:9",
                runtime_identity_sha256=provisioner.identity.digest,
            )
            reservation = ledger.reserve(
                sandbox_id="sandbox",
                sandbox_generation=9,
                memory_mb=1024,
                writable_disk_mb=2048,
            )
            payload = quota.prepare(reservation)
            registry.commit_quota(
                "sandbox",
                expected_revision=planned.revision,
                project_id=reservation.project_id,
                total_mb=reservation.total_mb,
                quota_path=Path(payload["path"]),
            )

            results = provisioner.start()

            self.assertTrue(images.reconciled)
            self.assertEqual(len(results), 1)
            self.assertEqual(results[0].registration.phase, "owned")

    def test_restart_never_turns_interrupted_import_into_new_sandbox(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            provisioner, registry, ledger, quota, _, warden = self.make(root)
            planned = registry.plan(
                spec=self.spec(),
                sandbox_generation=9,
                operation_id="create:9",
                runtime_identity_sha256=provisioner.identity.digest,
            )
            reservation = ledger.reserve(
                sandbox_id="sandbox",
                sandbox_generation=9,
                memory_mb=1024,
                writable_disk_mb=2048,
            )
            payload = quota.prepare(reservation)
            quota_ready = registry.commit_quota(
                "sandbox",
                expected_revision=planned.revision,
                project_id=reservation.project_id,
                total_mb=reservation.total_mb,
                quota_path=Path(payload["path"]),
            )
            registry.begin_import(
                "sandbox",
                expected_revision=quota_ready.revision,
                migration_id="move:interrupted",
                migration_sha256="a" * 64,
            )

            results = provisioner.start()

            self.assertEqual(results, ())
            self.assertEqual(registry.get("sandbox").phase, "importing")
            self.assertNotIn(("sandbox", 9), warden.records)

    def test_start_fails_closed_on_orphan_capacity_owner(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            provisioner, _, ledger, _, _, _ = self.make(root)
            ledger.reserve(
                sandbox_id="orphan",
                sandbox_generation=1,
                memory_mb=1024,
                writable_disk_mb=1024,
            )

            with self.assertRaisesRegex(DirectRegistryError, "absent"):
                provisioner.start()

    def test_service_wakes_for_exec_and_supports_binary_file_input(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            provisioner, _, _, _, _, _ = self.make(root)
            runner = FakeProcessRunner()
            service = DirectSandboxService(provisioner, process_runner=runner)
            created = service.create(self.spec())
            service.park(created.spec.id)

            result = service.exec(created.spec.id, ("/bin/echo", "ok"))
            service.write_file(created.spec.id, "/workspace/payload", b"\0binary")

            self.assertEqual(result.stdout, b"ok\n")
            self.assertEqual(
                service.get(created.spec.id).state,
                HibernationState.RUNNING.value,
            )
            self.assertEqual(runner.calls[-1][1], b"\0binary")

    def test_storage_native_background_park_does_not_wait_for_publication(
        self,
    ) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            provisioner, _, _, _, _, warden = self.make(root)
            publication_started = Event()
            allow_publication = Event()
            warden.storage = object()

            def publish_storage_snapshot(
                _sandbox,
                *,
                operation_id,
            ):
                self.assertEqual(operation_id, "park:test:publish")
                publication_started.set()
                if not allow_publication.wait(timeout=5):
                    raise TimeoutError("test did not release publication")
                return {"state": "published"}

            warden.publish_storage_snapshot = publish_storage_snapshot
            service = DirectSandboxService(
                provisioner,
                process_runner=FakeProcessRunner(),
            )
            created = service.create(self.spec())

            parked = service.park(
                created.spec.id,
                operation_id="park:test",
                background=True,
            )

            self.assertEqual(parked.state, HibernationState.PARKED.value)
            self.assertTrue(publication_started.wait(timeout=1))
            self.assertTrue(
                service.storage_native_publication_pending(created.spec.id)
            )
            allow_publication.set()
            for thread in tuple(service._publication_threads.values()):
                thread.join(timeout=5)
            self.assertFalse(
                service.storage_native_publication_pending(created.spec.id)
            )

    def test_service_quarantines_dead_running_sentry_on_read(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            provisioner, _, _, _, _, warden = self.make(root)
            service = DirectSandboxService(
                provisioner,
                process_runner=FakeProcessRunner(),
            )
            created = service.create(self.spec())
            warden.alive = False

            observed = service.get(created.spec.id)

            self.assertIsNotNone(observed)
            self.assertEqual(observed.state, HibernationState.RECOVERY_REQUIRED.value)

    def test_service_rejects_restore_beyond_hard_active_capacity(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            provisioner, _, _, _, _, _ = self.make(root)
            service = DirectSandboxService(
                provisioner,
                process_runner=FakeProcessRunner(),
            )
            first = service.create(replace(self.spec(), id="sandbox-a"))
            second = service.create(replace(self.spec(), id="sandbox-b"))
            service.park(first.spec.id)
            service.park(second.spec.id)
            service.configure_active_capacity(ResourceQuantity(vcpu=1, memory_mb=1024))

            service.exec(first.spec.id, ("/bin/true",))
            with self.assertRaisesRegex(
                SandboxCapacityUnavailableError,
                "insufficient active CPU or memory",
            ):
                service.exec(second.spec.id, ("/bin/true",))

            self.assertEqual(service.get(first.spec.id).state, "running")
            self.assertEqual(service.get(second.spec.id).state, "parked")

    def test_dynamic_active_admission_reuses_idle_cpu_and_memory_limits(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            provisioner, _, _, _, _, _ = self.make(root)
            service = DirectSandboxService(
                provisioner,
                process_runner=FakeProcessRunner(),
            )
            service.configure_active_capacity(
                ResourceQuantity(vcpu=1, memory_mb=1024),
                dynamic=True,
                runtime_metrics_provider=lambda: NodeRuntimeMetrics(
                    collected_at=utc_now(),
                    cpu_percent=1.0,
                    cpu_count=1,
                    memory_total_mb=1024,
                    memory_available_mb=4096,
                    swap_total_mb=4096,
                    swap_free_mb=4096,
                ),
            )

            for index in range(3):
                service.create(replace(self.spec(), id=f"sandbox-{index}"))

            self.assertEqual(
                {record.state for record in service.list()},
                {"running"},
            )

    def test_dynamic_active_admission_stops_on_live_pressure(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            provisioner, _, _, _, _, _ = self.make(root)
            service = DirectSandboxService(
                provisioner,
                process_runner=FakeProcessRunner(),
            )
            service.configure_active_capacity(
                ResourceQuantity(vcpu=4, memory_mb=8192),
                dynamic=True,
                runtime_metrics_provider=lambda: NodeRuntimeMetrics(
                    collected_at=utc_now(),
                    cpu_percent=95.0,
                    cpu_count=4,
                    memory_total_mb=8192,
                    memory_available_mb=8192,
                ),
            )

            with self.assertRaisesRegex(
                SandboxCapacityUnavailableError,
                "CPU pressure",
            ):
                service.create(self.spec())

    def test_dynamic_active_admission_fails_closed_without_metrics(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            provisioner, _, _, _, _, _ = self.make(root)
            service = DirectSandboxService(
                provisioner,
                process_runner=FakeProcessRunner(),
            )
            service.configure_active_capacity(
                ResourceQuantity(vcpu=4, memory_mb=8192),
                dynamic=True,
                runtime_metrics_provider=lambda: None,
            )

            with self.assertRaisesRegex(
                SandboxCapacityUnavailableError,
                "no fresh runtime metrics",
            ):
                service.create(self.spec())

    def test_dynamic_active_admission_stops_on_cpu_load(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            provisioner, _, _, _, _, _ = self.make(root)
            service = DirectSandboxService(
                provisioner,
                process_runner=FakeProcessRunner(),
            )
            service.configure_active_capacity(
                ResourceQuantity(vcpu=4, memory_mb=8192),
                dynamic=True,
                runtime_metrics_provider=lambda: NodeRuntimeMetrics(
                    collected_at=utc_now(),
                    cpu_percent=20.0,
                    cpu_count=4,
                    load_average_1m=5.0,
                    memory_total_mb=8192,
                    memory_available_mb=8192,
                ),
            )

            with self.assertRaisesRegex(
                SandboxCapacityUnavailableError,
                "CPU load",
            ):
                service.create(self.spec())


    def test_node_adapter_releases_exec_start_fence_and_accounts_parked_memory(
        self,
    ) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            provisioner, _, _, _, _, _ = self.make(root)
            service = DirectSandboxService(
                provisioner,
                process_runner=FakeProcessRunner(),
            )
            created = service.create(self.spec())
            manager = DirectNodeManagerAdapter(service)
            service.park(created.spec.id)

            manager.lifecycle.acquire_shared(created.spec.id)
            command = manager.runtime.exec_command(
                created.spec.id,
                ("/bin/true",),
                interactive=False,
            )
            manager.runtime.exec_started(created.spec.id)
            manager.lifecycle.release_shared(created.spec.id)
            snapshot = manager.heartbeat_snapshot(active_build_count=lambda: 0)

            self.assertEqual(command[:2], ("runsc", "exec"))
            self.assertEqual(snapshot.activity.active_sandboxes, 1)
            self.assertEqual(snapshot.activity.used_resources.memory_mb, 0)
            service.park(created.spec.id)
            parked = manager.heartbeat_snapshot(active_build_count=lambda: 0)
            self.assertEqual(parked.activity.active_sandboxes, 0)
            self.assertEqual(parked.activity.used_resources.memory_mb, 0)

    def test_node_adapter_delete_preempts_attached_exec_but_park_does_not(
        self,
    ) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            provisioner, _, _, _, _, _ = self.make(root)
            service = DirectSandboxService(
                provisioner,
                process_runner=FakeProcessRunner(),
            )
            created = service.create(self.spec())
            manager = DirectNodeManagerAdapter(service)

            manager.lifecycle.acquire_shared(created.spec.id)
            manager.runtime.exec_command(
                created.spec.id,
                ("/bin/sleep", "3600"),
                interactive=False,
            )
            manager.runtime.exec_started(created.spec.id)
            with self.assertRaisesRegex(SandboxBusyError, "active exec"):
                manager.park(created.spec.id)

            deleted, _ = manager.delete(
                created.spec.id,
                generation=created.generation,
                operation_id="delete:test",
            )
            manager.lifecycle.release_shared(created.spec.id)

            self.assertEqual(deleted, created)
            self.assertIsNone(service.get(created.spec.id))

    def test_node_adapter_heartbeat_accounts_incomplete_create_registrations(
        self,
    ) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            provisioner, registry, _, _, _, _ = self.make(root)
            service = DirectSandboxService(
                provisioner,
                process_runner=FakeProcessRunner(),
            )
            planned_spec = replace(self.spec(), id="planned")
            quota_spec = replace(self.spec(), id="quota-ready")
            registry.plan(
                spec=planned_spec,
                sandbox_generation=1,
                operation_id="create:planned",
                runtime_identity_sha256=provisioner.identity.digest,
            )
            quota_planned = registry.plan(
                spec=quota_spec,
                sandbox_generation=2,
                operation_id="create:quota-ready",
                runtime_identity_sha256=provisioner.identity.digest,
            )
            registry.commit_quota(
                quota_spec.id,
                expected_revision=quota_planned.revision,
                project_id=200_002,
                total_mb=4096,
                quota_path=(root / "quota" / "quota-ready.sandbox-2").resolve(),
            )
            manager = DirectNodeManagerAdapter(service)

            snapshot = manager.heartbeat_snapshot(active_build_count=lambda: 0)

            self.assertEqual(snapshot.activity.active_sandboxes, 0)
            self.assertEqual(snapshot.activity.used_resources, ResourceQuantity())
            self.assertEqual(snapshot.activity.reserved_resources.memory_mb, 2048)
            self.assertEqual(snapshot.activity.reserved_resources.disk_mb, 6144)
            self.assertEqual(
                {record.state for record in snapshot.activity.records},
                {"planned", "quota_ready"},
            )

    def test_drain_fences_create_before_rootfs_registration(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            provisioner, _, _, _, images, _ = self.make(root)
            service = DirectSandboxService(
                provisioner,
                process_runner=FakeProcessRunner(),
            )
            manager = DirectNodeManagerAdapter(service)
            materialize_started = Event()
            materialize_release = Event()
            original_materialize = images.materialize

            def blocked_materialize(image_ref):
                materialize_started.set()
                if not materialize_release.wait(timeout=5):
                    raise AssertionError("test did not release image materialization")
                return original_materialize(image_ref)

            images.materialize = blocked_materialize
            created: list[object] = []
            create_thread = Thread(target=lambda: created.append(service.create(self.spec())))
            create_thread.start()
            self.assertTrue(materialize_started.wait(timeout=2))

            active = manager.heartbeat_snapshot(active_build_count=lambda: 0)
            draining = manager.configure_drain(
                "drain-create",
                True,
                active_build_count=lambda: 0,
            )

            self.assertEqual(active.activity.active_operations, 1)
            self.assertEqual(active.activity.records, ())
            self.assertEqual(active.activity.reserved_resources.memory_mb, 1024)
            self.assertFalse(draining.ready)
            self.assertEqual(draining.activity.active_operations, 1)
            with self.assertRaises(SandboxAdmissionClosedError):
                service.create(replace(self.spec(), id="rejected"))

            materialize_release.set()
            create_thread.join(timeout=5)
            self.assertFalse(create_thread.is_alive())
            self.assertEqual(len(created), 1)
            owned = manager.heartbeat_snapshot(active_build_count=lambda: 0)
            self.assertFalse(owned.ready)
            self.assertEqual(owned.activity.active_operations, 0)
            self.assertEqual(len(owned.activity.records), 1)

    def test_drain_waits_for_an_inflight_heartbeat_empty_proof(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            provisioner, _, _, _, _, _ = self.make(root)
            service = DirectSandboxService(
                provisioner,
                process_runner=FakeProcessRunner(),
            )
            manager = DirectNodeManagerAdapter(service)
            snapshot_started = Event()
            snapshot_release = Event()
            configure_done = Event()
            calls_guard = Lock()
            calls = 0
            original_snapshot = service.active_reservations_snapshot

            def block_first_snapshot():
                nonlocal calls
                with calls_guard:
                    calls += 1
                    first = calls == 1
                if first:
                    snapshot_started.set()
                    if not snapshot_release.wait(timeout=5):
                        raise AssertionError("test did not release heartbeat snapshot")
                return original_snapshot()

            service.active_reservations_snapshot = block_first_snapshot
            heartbeat_results: list[object] = []
            drain_results: list[object] = []
            heartbeat_thread = Thread(
                target=lambda: heartbeat_results.append(
                    manager.heartbeat_snapshot(active_build_count=lambda: 0)
                )
            )

            def configure():
                drain_results.append(
                    manager.configure_drain(
                        "drain-heartbeat",
                        True,
                        active_build_count=lambda: 0,
                    )
                )
                configure_done.set()

            heartbeat_thread.start()
            self.assertTrue(snapshot_started.wait(timeout=2))
            configure_thread = Thread(target=configure)
            configure_thread.start()
            self.assertFalse(configure_done.wait(timeout=0.1))

            snapshot_release.set()
            heartbeat_thread.join(timeout=5)
            configure_thread.join(timeout=5)

            self.assertFalse(heartbeat_thread.is_alive())
            self.assertFalse(configure_thread.is_alive())
            self.assertFalse(heartbeat_results[0].drain.draining)
            self.assertTrue(drain_results[0].ready)

    def test_drain_fences_image_pull_and_rootfs_materialization(self) -> None:
        class ImageOperations:
            def __init__(self) -> None:
                self.active = 0

            @contextmanager
            def image_operation(self):
                self.active += 1
                try:
                    yield
                finally:
                    self.active -= 1

        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            provisioner, _, _, _, _, _ = self.make(root)
            service = DirectSandboxService(
                provisioner,
                process_runner=FakeProcessRunner(),
            )
            manager = DirectNodeManagerAdapter(service)
            images = ImageOperations()

            with manager.image_operation(images):
                draining = manager.configure_drain(
                    "drain-image",
                    True,
                    active_build_count=lambda: images.active,
                )
                self.assertFalse(draining.ready)
                self.assertEqual(draining.active_image_builds, 1)

            ready = manager.heartbeat_snapshot(
                active_build_count=lambda: images.active
            )
            self.assertTrue(ready.ready)
            with self.assertRaises(SandboxAdmissionClosedError):
                with manager.image_operation(images):
                    pass

    def test_node_adapter_heartbeat_does_not_join_exec_lifecycle_fence(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            provisioner, _, _, _, _, warden = self.make(root)
            service = DirectSandboxService(
                provisioner,
                process_runner=FakeProcessRunner(),
            )
            created = service.create(self.spec())
            manager = DirectNodeManagerAdapter(service)

            def fenced_inspect(_sandbox):
                raise AssertionError("heartbeat joined the streaming exec fence")

            warden.inspect = fenced_inspect
            snapshot = manager.heartbeat_snapshot(active_build_count=lambda: 0)
            listed = manager.list()

            self.assertEqual(snapshot.activity.active_sandboxes, 1)
            self.assertEqual(snapshot.activity.used_resources.memory_mb, 0)
            self.assertEqual([record.spec.id for record in listed], [created.spec.id])

    def test_direct_node_drain_survives_adapter_restart(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            provisioner, _, _, _, _, _ = self.make(root)
            service = DirectSandboxService(
                provisioner,
                process_runner=FakeProcessRunner(),
            )
            state_store = DirectNodeStateStore(root / "direct-node-state.json")
            manager = DirectNodeManagerAdapter(service, state_store=state_store)

            drained = manager.configure_drain(
                "drain-test",
                True,
                active_build_count=lambda: 0,
            )
            restarted = DirectNodeManagerAdapter(
                service,
                state_store=DirectNodeStateStore(root / "direct-node-state.json"),
            )
            restarted_snapshot = restarted.heartbeat_snapshot(
                active_build_count=lambda: 0
            )
            restarted_admission_open = service.admission_open
            opened = restarted.configure_drain(
                "drain-test",
                False,
                active_build_count=lambda: 0,
            )

            self.assertTrue(drained.drain.draining)
            self.assertTrue(restarted_snapshot.drain.draining)
            self.assertFalse(restarted_admission_open)
            self.assertFalse(opened.drain.draining)
            self.assertTrue(service.admission_open)

    def test_direct_node_server_binds_only_direct_manager(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            provisioner, _, _, _, _, _ = self.make(root)
            service = DirectSandboxService(
                provisioner,
                process_runner=FakeProcessRunner(),
            )

            server = build_direct_node_agent_server(
                "127.0.0.1",
                0,
                service=service,
                image_file=root / "images.json",
                job_id="job",
                node_id="node",
            )
            try:
                self.assertIs(server.RequestHandlerClass.manager.service, service)
                self.assertIn(
                    "direct-runsc-v1",
                    server.RequestHandlerClass.capabilities,
                )
                self.assertNotIn(
                    "dynamic-active-admission-v1",
                    server.RequestHandlerClass.capabilities,
                )
                self.assertIsNone(service._parking_thread)
            finally:
                server.server_close()

    def test_direct_node_advertises_dynamic_admission_with_live_metrics(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            provisioner, _, _, _, _, _ = self.make(root)
            service = DirectSandboxService(
                provisioner,
                process_runner=FakeProcessRunner(),
            )
            server = build_direct_node_agent_server(
                "127.0.0.1",
                0,
                service=service,
                image_file=root / "images.json",
                job_id="job",
                node_id="node",
                total_resources=ResourceQuantity(vcpu=32, memory_mb=98304),
                runtime_metrics_provider=lambda: NodeRuntimeMetrics(
                    collected_at=utc_now(),
                    cpu_percent=1.0,
                    cpu_count=32,
                    memory_total_mb=98304,
                    memory_available_mb=90000,
                ),
            )
            try:
                self.assertIn(
                    "dynamic-active-admission-v1",
                    server.RequestHandlerClass.capabilities,
                )
            finally:
                server.server_close()

    def test_direct_image_warmup_materializes_rootfs_before_reporting_ready(
        self,
    ) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            provisioner, _, _, _, images, _ = self.make(root)
            service = DirectSandboxService(
                provisioner,
                process_runner=FakeProcessRunner(),
            )
            server = build_direct_node_agent_server(
                "127.0.0.1",
                0,
                service=service,
                image_file=root / "images.json",
                job_id="job",
                node_id="node",
            )
            thread = Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address
                outbound = request.Request(
                    f"http://{host}:{port}/v1/images/pull",
                    data=json.dumps({"image": "image"}).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with request.urlopen(outbound) as response:
                    pulled = json.load(response)
                heartbeat_request = request.Request(
                    f"http://{host}:{port}/v1/heartbeat"
                )
                with request.urlopen(heartbeat_request) as response:
                    heartbeat = json.load(response)["heartbeat"]
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=1)

            self.assertEqual(images.materialized_refs, ["image"])
            self.assertGreaterEqual(
                pulled["timings"]["rootfs_materialize_ms"],
                0,
            )
            self.assertIn("image", heartbeat["cached_images"])
            self.assertEqual(
                heartbeat["runtime_metrics"][
                    "rootfs_export_max_concurrent_operations"
                ],
                4,
            )

    def test_direct_node_park_and_explicit_wake_endpoints(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            provisioner, _, _, _, _, _ = self.make(root)
            service = DirectSandboxService(
                provisioner,
                process_runner=FakeProcessRunner(),
            )
            created = service.create(self.spec())
            server = build_direct_node_agent_server(
                "127.0.0.1",
                0,
                service=service,
                image_file=root / "images.json",
                job_id="job",
                node_id="node",
            )
            thread = Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address
                park_request = request.Request(
                    f"http://{host}:{port}/v1/sandboxes/{created.spec.id}/park",
                    data=json.dumps({"operation_id": "park:test"}).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with request.urlopen(park_request) as response:
                    parked = json.load(response)["sandbox"]
                self.assertEqual(parked["state"], HibernationState.PARKED.value)
                with self.assertRaises(RuntimeError):
                    service.wake(
                        created.spec.id,
                        generation=created.generation + 1,
                        operation_id="wake:stale",
                    )

                wake_request = request.Request(
                    f"http://{host}:{port}/v1/sandboxes/{created.spec.id}/wake",
                    data=json.dumps(
                        {
                            "generation": created.generation,
                            "operation_id": "wake:test",
                        }
                    ).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with request.urlopen(wake_request) as response:
                    woken = json.load(response)["sandbox"]
                self.assertEqual(woken["state"], HibernationState.RUNNING.value)
                self.assertEqual(
                    service.get(created.spec.id).state,
                    HibernationState.RUNNING.value,
                )
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=1)

    def test_direct_node_migrates_parked_sandbox_between_daemons(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            source_root = root / "source"
            destination_root = root / "destination"
            source_root.mkdir()
            destination_root.mkdir()
            source, _, _, _, source_images, source_warden = self.make(source_root)
            destination, destination_registry, _, _, _, _ = self.make(destination_root)
            source_service = DirectSandboxService(
                source,
                process_runner=FakeProcessRunner(),
            )
            destination_service = DirectSandboxService(
                destination,
                process_runner=FakeProcessRunner(),
            )
            created = source_service.create(self.spec())
            source_service.park(created.spec.id, operation_id="park:migration")
            registration = source.registry.get(created.spec.id)
            self.assertIsNotNone(registration)
            assert registration is not None
            incarnation = Path(registration.quota_path)
            generation = incarnation / "hibernate-1"
            generation.mkdir()
            (incarnation / "upper" / "payload").write_bytes(b"migrated")
            roles = {
                "application_memory.img": HibernationFileRole.MAIN_MEMORY,
                "checkpoint.img": HibernationFileRole.KERNEL_STATE,
                "pages_meta.img": HibernationFileRole.ALLOCATOR_METADATA,
            }
            for name in roles:
                (generation / name).write_bytes(name.encode("ascii"))
            runtime = replace(
                source_warden.config.runtime_fingerprint,
                rootfs_sha256=source_images.image.rootfs_identity_sha256,
            )
            manifest = HibernationManifest(
                sandbox_id=registration.sandbox_id,
                sandbox_generation=registration.sandbox_generation,
                hibernation_generation=1,
                operation_id="park:migration",
                spec_sha256=registration.spec_sha256,
                container_id=registration.container_id,
                created_ns=1,
                runtime=runtime,
                files=tuple(
                    HibernationArtifactFile.from_path(
                        generation / name,
                        role=role,
                    )
                    for name, role in roles.items()
                ),
            )
            source_warden.artifacts.publish_complete(manifest)
            source_warden.records[
                (registration.sandbox_id, registration.sandbox_generation)
            ] = SimpleNamespace(
                state=HibernationState.PARKED,
                hibernation_generation=1,
            )

            source_server = build_direct_node_agent_server(
                "127.0.0.1",
                0,
                service=source_service,
                image_file=source_root / "images.json",
                job_id="source-job",
                node_id="source-node",
                node_control_bearer_token="shared-secret",
            )
            destination_server = build_direct_node_agent_server(
                "127.0.0.1",
                0,
                service=destination_service,
                image_file=destination_root / "images.json",
                job_id="destination-job",
                node_id="destination-node",
                node_control_bearer_token="shared-secret",
            )
            source_thread = Thread(
                target=source_server.serve_forever,
                daemon=True,
            )
            destination_thread = Thread(
                target=destination_server.serve_forever,
                daemon=True,
            )
            source_thread.start()
            destination_thread.start()

            def post(base: str, path: str, payload: dict) -> dict:
                outbound = request.Request(
                    base + path,
                    data=json.dumps(payload).encode("utf-8"),
                    headers={
                        "Authorization": "Bearer shared-secret",
                        "Content-Type": "application/json",
                    },
                    method="POST",
                )
                with request.urlopen(outbound) as response:
                    return json.load(response)

            try:
                source_host, source_port = source_server.server_address
                destination_host, destination_port = destination_server.server_address
                source_base = f"http://{source_host}:{source_port}"
                destination_base = f"http://{destination_host}:{destination_port}"
                migration_id = "migration-http"
                prepared = post(
                    source_base,
                    f"/v1/sandboxes/{created.spec.id}/migration/prepare",
                    {"migration_id": migration_id},
                )["migration"]
                source_url = (
                    f"{source_base}/v1/sandboxes/{created.spec.id}"
                    f"/migration/archive?migration_id={migration_id}"
                )
                imported = post(
                    destination_base,
                    "/v1/migrations/import",
                    {
                        "archive_sha256": prepared["archive_sha256"],
                        "archive_token": prepared["archive_token"],
                        "migration_id": migration_id,
                        "sandbox_id": created.spec.id,
                        "source_url": source_url,
                    },
                )
                self.assertEqual(imported["sandbox"]["state"], "import_ready")
                activated = post(
                    destination_base,
                    f"/v1/sandboxes/{created.spec.id}/migration/activate",
                    {
                        "archive_sha256": prepared["archive_sha256"],
                        "migration_id": migration_id,
                    },
                )
                post(
                    source_base,
                    f"/v1/sandboxes/{created.spec.id}/migration/finalize",
                    {
                        "archive_sha256": prepared["archive_sha256"],
                        "migration_id": migration_id,
                    },
                )
            finally:
                source_server.shutdown()
                destination_server.shutdown()
                source_server.server_close()
                destination_server.server_close()
                source_thread.join(timeout=1)
                destination_thread.join(timeout=1)

            self.assertEqual(activated["sandbox"]["state"], "parked")
            self.assertIsNone(source.registry.get(created.spec.id))
            destination_registration = destination_registry.get(created.spec.id)
            self.assertIsNotNone(destination_registration)
            assert destination_registration is not None
            self.assertEqual(destination_registration.phase, "owned")
            self.assertEqual(
                (
                    Path(destination_registration.quota_path) / "upper" / "payload"
                ).read_bytes(),
                b"migrated",
            )


if __name__ == "__main__":
    unittest.main()
