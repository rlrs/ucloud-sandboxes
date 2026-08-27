import hashlib
import json
import shutil
import unittest
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Event, Lock, Thread
from time import monotonic, sleep
from types import SimpleNamespace
from unittest.mock import patch
from urllib import request

from ucloud_sandboxes.direct_oci import DirectOciConfigBuilder
from ucloud_sandboxes.direct_provisioner import DirectSandboxProvisioner
from ucloud_sandboxes.direct_registry import DirectSandboxRegistry
from ucloud_sandboxes.direct_service import DirectExecResult, DirectSandboxService
from ucloud_sandboxes.direct_warden import DirectSandbox
from ucloud_sandboxes.hibernation import (
    HibernationArtifactStore,
    HibernationRuntimeFingerprint,
    HibernationState,
)
from ucloud_sandboxes.image_rootfs import (
    DockerImageConfig,
    MaterializedRootfs,
    OverlayRootfsLease,
)
from ucloud_sandboxes.images import DockerImageRuntime
from ucloud_sandboxes.models import NodeRuntimeMetrics, ResourceQuantity, utc_now
from ucloud_sandboxes.node_agent import (
    build_direct_node_agent_server as _build_direct_node_agent_server,
)
from ucloud_sandboxes.node_runtime import DirectNodeRuntime
from ucloud_sandboxes.sandbox import (
    SandboxAdmissionClosedError,
    SandboxBusyError,
    SandboxCapacityUnavailableError,
    SandboxOperation,
    SandboxSecuritySpec,
    SandboxSpec,
    sandbox_spec_fingerprint,
)
from ucloud_sandboxes.storage_native_daemon import (
    StorageNativeConflictError,
    StorageNativeNodeError,
    StorageVolumeOwner,
    StorageVolumeRecord,
    StorageVolumeState,
)


def build_direct_node_agent_server(*args, **kwargs):
    explicit_auth = "node_control_bearer_token" in kwargs
    kwargs.setdefault("node_control_bearer_token", "test-node-secret")
    kwargs.setdefault("deployment_id", "test-deployment")
    kwargs.setdefault("image_runtime", DockerImageRuntime(dry_run=True))
    server = _build_direct_node_agent_server(*args, **kwargs)
    if not explicit_auth:
        server.RequestHandlerClass._check_node_control_authorized = lambda _self: True
    return server


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
        self.reconciled_roots: tuple[str, ...] = ()
        self.collected_image_ids: list[str] = []
        self.collection_reference_checks: list[bool] = []
        self.materialized_refs: list[str] = []
        self.fail_collect = False
        self.fail_reconcile = False

    def materialize(self, image_ref: str) -> MaterializedRootfs:
        if image_ref != "image":
            raise AssertionError("wrong image")
        self.materialized_refs.append(image_ref)
        return self.image

    @contextmanager
    def operation_lease(self, image_ref: str):
        yield self.materialize(image_ref)

    def reconcile_images(self, referenced_image_ids, *, is_referenced) -> None:
        self.reconciled_roots = tuple(referenced_image_ids)
        del is_referenced
        if self.fail_reconcile:
            raise OSError("injected image reconciliation failure")
        self.reconciled = True

    def collect_image(self, image_id, *, is_referenced) -> bool:
        self.collected_image_ids.append(image_id)
        if self.fail_collect:
            raise OSError("injected image collection failure")
        referenced = is_referenced(image_id)
        self.collection_reference_checks.append(referenced)
        return not referenced

    def warm(self, image_ref: str) -> None:
        self.materialize(image_ref)

    def operation_snapshot(self) -> dict[str, int]:
        return {
            "active_operations": 0,
            "waiting_operations": 0,
            "max_concurrent_operations": 4,
        }


class FakeStorage:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.records: dict[tuple[str, int], StorageVolumeRecord] = {}
        self.next_project_id = 200_000
        self.delete_operation_ids: list[str] = []

    @property
    def active_records(self) -> dict[tuple[str, int], StorageVolumeRecord]:
        return {
            key: record
            for key, record in self.records.items()
            if record.state != StorageVolumeState.DELETED
        }

    def prepare_volume(
        self,
        owner: StorageVolumeOwner,
        *,
        operation_id: str,
        virtual_size: int,
    ) -> StorageVolumeRecord:
        key = (owner.sandbox_id, owner.sandbox_generation)
        existing = self.records.get(key)
        if existing is not None:
            if (
                existing.state == StorageVolumeState.DELETED
                or existing.owner != owner
                or existing.virtual_size != virtual_size
            ):
                raise StorageNativeConflictError(
                    "replayed storage preparation changed its owner"
                )
            return existing
        project_id = self.next_project_id
        self.next_project_id += 1
        record = self._record(
            owner,
            operation_id=operation_id,
            virtual_size=virtual_size,
            accounting_id=project_id,
        )
        self.records[key] = record
        return record

    def get_volume(self, volume_id: str) -> StorageVolumeRecord:
        for record in self.records.values():
            if record.volume_id == volume_id:
                return record
        raise StorageNativeConflictError("storage-native volume does not exist")

    def delete_volume(
        self,
        owner: StorageVolumeOwner,
        *,
        operation_id: str,
        expected_accounting_id: int | None = None,
        expected_virtual_size: int | None = None,
    ) -> StorageVolumeRecord:
        record = self.get_volume(owner.volume_id)
        if (
            record.owner != owner
            or (
                expected_accounting_id is not None
                and record.accounting_id != expected_accounting_id
            )
            or (
                expected_virtual_size is not None
                and record.virtual_size != expected_virtual_size
            )
        ):
            raise StorageNativeNodeError(
                "storage-native volume belongs to another owner"
            )
        if record.state == StorageVolumeState.DELETED:
            return record
        self.delete_operation_ids.append(operation_id)
        path = Path(record.mount_path)
        if path.exists():
            shutil.rmtree(path)
        deleted = replace(
            record,
            revision=record.revision + 1,
            state=StorageVolumeState.DELETED,
            operation_id=operation_id,
        )
        self.records[(owner.sandbox_id, owner.sandbox_generation)] = deleted
        return deleted

    def get_metrics(self) -> dict:
        return {}

    def _record(
        self,
        owner: StorageVolumeOwner,
        *,
        operation_id: str,
        virtual_size: int,
        accounting_id: int,
    ) -> StorageVolumeRecord:
        path = self.root / owner.volume_id
        path.mkdir(mode=0o700, parents=True, exist_ok=True)
        runtime = self.root.parent / "storage-runtime" / owner.volume_id
        return StorageVolumeRecord(
            volume_id=owner.volume_id,
            sandbox_id=owner.sandbox_id,
            sandbox_generation=owner.sandbox_generation,
            revision=1,
            state=StorageVolumeState.MOUNTED,
            operation_id=operation_id,
            virtual_size=virtual_size,
            runtime_dir=str(runtime / "runtime"),
            mount_path=str(path),
            source_image_config=str(runtime / "source.json"),
            device_owner_id="",
            accounting_id=accounting_id,
        )


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
        image,
        config_template,
        spec_sha256,
        imported_parked=False,
    ):
        del config_template
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
    def __init__(self, root: Path, storage: FakeStorage) -> None:
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
            bundle_root=bundles,
            memory_root=quota,
            network="none",
            runtime_fingerprint=fingerprint,
        )
        self.artifacts = HibernationArtifactStore(quota)
        self.records = {}
        self.discarded = []
        self.alive = True
        self.storage = storage

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

    @staticmethod
    def _storage_record(_sandbox):
        return SimpleNamespace(state=StorageVolumeState.MOUNTED)

    def storage_records_snapshot(self, sandboxes):
        return {
            sandbox.memory_directory: SimpleNamespace(state=StorageVolumeState.MOUNTED)
            for sandbox in sandboxes
        }

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
        storage = FakeStorage(overlays.writable_root)
        warden = FakeWarden(root, storage)
        warden.rootfs_lifecycle = overlays
        registry = DirectSandboxRegistry((root / "registry.sqlite").resolve())
        provisioner = DirectSandboxProvisioner(
            registry=registry,
            image_store=images,
            overlays=overlays,
            oci=DirectOciConfigBuilder(),
            warden=warden,
        )
        return provisioner, registry, storage, images, warden

    @staticmethod
    def spec() -> SandboxSpec:
        return SandboxSpec(
            id="sandbox",
            image="image",
            memory_mb=1024,
            disk_mb=2048,
            security=SandboxSecuritySpec(init=False),
        )

    @staticmethod
    def create(
        service: DirectSandboxService,
        spec: SandboxSpec,
        *,
        generation: int = 7,
    ):
        return service.create(
            spec,
            operation=SandboxOperation(
                operation_id=f"create:{spec.id}:{generation}",
                generation=generation,
                kind="create",
                spec_hash=sandbox_spec_fingerprint(spec),
            ),
        )

    @contextmanager
    def running_server(self, root: Path, service: DirectSandboxService):
        server = build_direct_node_agent_server(
            "127.0.0.1",
            0,
            service=service,
            image_file=root / "images.json",
            job_id="job",
            node_id="node",
        )
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
            server.server_close()
            thread.join(timeout=1)

    def test_create_and_delete_order_all_owners(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            provisioner, registry, quota, _, warden = self.make(root)

            created = provisioner.create(
                spec=self.spec(),
                sandbox_generation=7,
                operation_id="create:7",
            )

            self.assertEqual(created.phase, "owned")
            self.assertEqual(len(quota.active_records), 1)
            self.assertIn(("sandbox", 7), warden.discarded)

            provisioner.delete("sandbox")

            self.assertIsNone(registry.get("sandbox"))
            self.assertEqual(quota.active_records, {})
            self.assertEqual(quota.delete_operation_ids, ["quota-delete:200000"])

    def test_delete_rolls_back_planned_create_without_advancing_image(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            provisioner, registry, quota, images, _ = self.make(root)
            planned = registry.plan(
                spec=self.spec(),
                sandbox_generation=7,
                operation_id="create:7",
                runtime_compatibility_sha256=(
                    provisioner.runtime_compatibility_sha256
                ),
            )
            total_mb = provisioner._quota_total_mb(planned)
            quota.prepare_volume(
                provisioner._storage_owner(planned),
                operation_id=planned.operation_id,
                virtual_size=total_mb * 1024 * 1024,
            )

            provisioner.delete(planned.sandbox_id)

            self.assertIsNone(registry.get(planned.sandbox_id))
            self.assertEqual(quota.active_records, {})
            self.assertEqual(images.materialized_refs, [])

    def test_delete_rolls_back_quota_ready_create_without_advancing_image(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            provisioner, registry, quota, images, _ = self.make(root)
            planned = registry.plan(
                spec=self.spec(),
                sandbox_generation=7,
                operation_id="create:7",
                runtime_compatibility_sha256=(
                    provisioner.runtime_compatibility_sha256
                ),
            )
            total_mb = provisioner._quota_total_mb(planned)
            prepared = quota.prepare_volume(
                provisioner._storage_owner(planned),
                operation_id=planned.operation_id,
                virtual_size=total_mb * 1024 * 1024,
            )
            quota_ready = registry.commit_quota(
                planned.sandbox_id,
                expected_revision=planned.revision,
                project_id=prepared.accounting_id,
                total_mb=total_mb,
                quota_path=Path(prepared.mount_path),
            )

            provisioner.delete(quota_ready.sandbox_id)

            self.assertIsNone(registry.get(quota_ready.sandbox_id))
            self.assertEqual(quota.active_records, {})
            self.assertEqual(images.materialized_refs, [])

    def test_create_rejects_noncanonical_storage_record(self) -> None:
        cases = (
            (
                "owner",
                lambda record, _root: replace(record, sandbox_id="other"),
                "another quota owner",
            ),
            (
                "path",
                lambda record, root: replace(
                    record,
                    mount_path=str(root / "unexpected"),
                ),
                "unexpected mount path",
            ),
            (
                "size",
                lambda record, _root: replace(
                    record,
                    virtual_size=record.virtual_size + 1,
                ),
                "another quota owner",
            ),
            (
                "accounting",
                lambda record, _root: replace(record, accounting_id=0),
                "invalid accounting ID",
            ),
        )
        for label, mutate, message in cases:
            with self.subTest(label=label), TemporaryDirectory() as raw:
                root = Path(raw).resolve()
                provisioner, _, storage, _, _ = self.make(root)
                prepare = storage.prepare_volume

                def invalid(owner, **kwargs):
                    return mutate(prepare(owner, **kwargs), root)

                storage.prepare_volume = invalid
                with self.assertRaisesRegex(StorageNativeNodeError, message):
                    provisioner.create(
                        spec=self.spec(),
                        sandbox_generation=7,
                        operation_id="create:7",
                    )

    def test_delete_collects_only_its_digest_and_preserves_shared_root(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            provisioner, _, _, images, _ = self.make(root)
            first = provisioner.create(
                spec=replace(self.spec(), id="sandbox-1"),
                sandbox_generation=1,
                operation_id="create:1",
            )
            second = provisioner.create(
                spec=replace(self.spec(), id="sandbox-2"),
                sandbox_generation=1,
                operation_id="create:2",
            )

            provisioner.delete(first.sandbox_id)
            provisioner.delete(second.sandbox_id)

            self.assertFalse(images.reconciled)
            self.assertEqual(
                images.collected_image_ids,
                [images.image.image_id, images.image.image_id],
            )
            self.assertEqual(images.collection_reference_checks, [True, False])

    def test_post_commit_image_collection_failure_is_deferred(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            provisioner, registry, _, images, _ = self.make(root)
            provisioner.start()
            created = provisioner.create(
                spec=self.spec(),
                sandbox_generation=7,
                operation_id="create:7",
            )
            images.fail_collect = True

            provisioner.delete(created.sandbox_id)

            self.assertIsNone(registry.get(created.sandbox_id))
            self.assertTrue(provisioner.image_cache_reconciliation_pending)
            images.fail_collect = False
            self.assertTrue(provisioner.reconcile_image_cache_if_pending())
            self.assertFalse(provisioner.image_cache_reconciliation_pending)

    def test_targeted_image_collection_does_not_serialize_other_digests(
        self,
    ) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            provisioner, _, _, images, _ = self.make(root)
            entered_two = Event()
            release = Event()
            active_guard = Lock()
            active = 0
            maximum_active = 0
            errors: list[BaseException] = []

            def blocking_collect(image_id, *, is_referenced):
                nonlocal active, maximum_active
                if is_referenced(image_id):
                    raise AssertionError("test digest unexpectedly became referenced")
                with active_guard:
                    active += 1
                    maximum_active = max(maximum_active, active)
                    if active == 2:
                        entered_two.set()
                try:
                    if not release.wait(timeout=5):
                        raise AssertionError("test did not release image collection")
                finally:
                    with active_guard:
                        active -= 1
                return True

            images.collect_image = blocking_collect

            def collect(image_id: str) -> None:
                try:
                    provisioner._collect_deleted_image(image_id)
                except BaseException as exc:
                    errors.append(exc)

            first = Thread(target=collect, args=("sha256:" + "a" * 64,))
            second = Thread(target=collect, args=("sha256:" + "b" * 64,))
            first.start()
            second.start()
            concurrent = entered_two.wait(timeout=2)
            release.set()
            first.join(timeout=5)
            second.join(timeout=5)

            self.assertTrue(concurrent)
            self.assertFalse(first.is_alive())
            self.assertFalse(second.is_alive())
            self.assertEqual(errors, [])
            self.assertEqual(maximum_active, 2)

    def test_restart_advances_quota_ready_registration(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            provisioner, registry, quota, images, _ = self.make(root)
            planned = registry.plan(
                spec=self.spec(),
                sandbox_generation=9,
                operation_id="create:9",
                runtime_compatibility_sha256=(provisioner.runtime_compatibility_sha256),
            )
            total_mb = provisioner._quota_total_mb(planned)
            record = quota.prepare_volume(
                provisioner._storage_owner(planned),
                operation_id=planned.operation_id,
                virtual_size=total_mb * 1024 * 1024,
            )
            registry.commit_quota(
                "sandbox",
                expected_revision=planned.revision,
                project_id=record.accounting_id,
                total_mb=total_mb,
                quota_path=Path(record.mount_path),
            )

            results = provisioner.start()

            self.assertTrue(images.reconciled)
            self.assertEqual(len(results), 1)
            self.assertEqual(results[0].phase, "owned")

    def test_restart_reuses_storage_id_after_prepare_before_registry_commit(
        self,
    ) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            provisioner, registry, quota, _, _ = self.make(root)
            planned = registry.plan(
                spec=self.spec(),
                sandbox_generation=9,
                operation_id="create:prepared",
                runtime_compatibility_sha256=(provisioner.runtime_compatibility_sha256),
            )
            total_mb = provisioner._quota_total_mb(planned)
            prepared = quota.prepare_volume(
                provisioner._storage_owner(planned),
                operation_id=planned.operation_id,
                virtual_size=total_mb * 1024 * 1024,
            )

            results = provisioner.start()

            self.assertEqual(len(results), 1)
            self.assertEqual(results[0].phase, "owned")
            self.assertEqual(
                results[0].quota_project_id,
                prepared.accounting_id,
            )
            self.assertEqual(quota.next_project_id, 200_001)

    def test_restart_never_turns_interrupted_import_into_new_sandbox(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            provisioner, registry, quota, _, warden = self.make(root)
            planned = registry.plan_import(
                spec=self.spec(),
                sandbox_generation=9,
                operation_id="create:9",
                runtime_compatibility_sha256=(provisioner.runtime_compatibility_sha256),
                migration_id="move:interrupted",
                migration_sha256="a" * 64,
            )
            total_mb = provisioner._quota_total_mb(planned)
            record = quota.prepare_volume(
                provisioner._storage_owner(planned),
                operation_id=planned.operation_id,
                virtual_size=total_mb * 1024 * 1024,
            )
            registry.commit_import_quota(
                "sandbox",
                expected_revision=planned.revision,
                project_id=record.accounting_id,
                total_mb=total_mb,
                quota_path=Path(record.mount_path),
            )

            results = provisioner.start()

            self.assertEqual(results, ())
            self.assertEqual(registry.get("sandbox").phase, "importing")
            self.assertNotIn(("sandbox", 9), warden.records)

    def test_delete_activated_import_preserves_migration_storage_fence(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            provisioner, registry, quota, _, warden = self.make(root)
            migration_id = "move:activated"
            migration_sha256 = "a" * 64
            planned = registry.plan_import(
                spec=self.spec(),
                sandbox_generation=9,
                operation_id="create:9",
                runtime_compatibility_sha256=(provisioner.runtime_compatibility_sha256),
                migration_id=migration_id,
                migration_sha256=migration_sha256,
            )
            total_mb = provisioner._quota_total_mb(planned)
            volume = quota.prepare_volume(
                provisioner._storage_owner(planned),
                operation_id=planned.operation_id,
                virtual_size=total_mb * 1024 * 1024,
            )
            importing = registry.commit_import_quota(
                planned.sandbox_id,
                expected_revision=planned.revision,
                project_id=volume.accounting_id,
                total_mb=total_mb,
                quota_path=Path(volume.mount_path),
            )
            bundle = (root / "bundles" / "sandbox.sandbox-9").resolve()
            bundle.mkdir()
            sandbox = DirectSandbox(
                sandbox_id=planned.sandbox_id,
                sandbox_generation=planned.sandbox_generation,
                container_id="c" * 64,
                spec_sha256=planned.spec_sha256,
                rootfs_sha256="d" * 64,
                bundle=bundle,
                memory_directory="sandbox.sandbox-9",
            )
            rootfs = registry.commit_import_rootfs(
                planned.sandbox_id,
                expected_revision=importing.revision,
                image_id="sha256:" + "e" * 64,
                sandbox=sandbox,
            )
            ready = registry.commit_import_ready(
                planned.sandbox_id,
                expected_revision=rootfs.revision,
                migration_id=migration_id,
                migration_sha256=migration_sha256,
            )
            owned = registry.activate_import(
                planned.sandbox_id,
                expected_revision=ready.revision,
                migration_id=migration_id,
                migration_sha256=migration_sha256,
            )
            warden.records[(owned.sandbox_id, owned.sandbox_generation)] = (
                SimpleNamespace(state=HibernationState.PARKED)
            )

            provisioner.delete(owned.sandbox_id)

            self.assertIsNone(registry.get(owned.sandbox_id))
            self.assertEqual(quota.active_records, {})
            self.assertEqual(
                quota.delete_operation_ids,
                [f"quota-delete:{volume.accounting_id}:{migration_id}"],
            )

    def test_restart_completes_delete_after_storage_delete_boundary(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            provisioner, registry, quota, _, warden = self.make(root)
            created = provisioner.create(
                spec=self.spec(),
                sandbox_generation=7,
                operation_id="create:7",
            )
            deleting = registry.begin_delete(
                created.sandbox_id,
                expected_revision=created.revision,
            )
            warden.delete(deleting.to_direct_sandbox())
            provisioner.overlays.release_sandbox(deleting.to_direct_sandbox())
            quota.delete_volume(
                provisioner._storage_owner(deleting),
                operation_id=f"quota-delete:{deleting.quota_project_id}",
                expected_accounting_id=deleting.quota_project_id,
                expected_virtual_size=deleting.quota_total_mb * 1024 * 1024,
            )

            self.assertEqual(registry.get(deleting.sandbox_id).phase, "deleting")
            self.assertEqual(quota.active_records, {})

            results = provisioner.start()

            self.assertEqual(results, ())
            self.assertIsNone(registry.get(deleting.sandbox_id))

    def test_delete_fails_closed_when_storage_authority_is_absent(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            provisioner, registry, quota, _, _ = self.make(root)
            created = provisioner.create(
                spec=self.spec(),
                sandbox_generation=7,
                operation_id="create:7",
            )
            identity = (
                created.sandbox_id,
                created.sandbox_generation,
            )
            quota.records.pop(identity)

            with self.assertRaisesRegex(RuntimeError, "quota owner is absent"):
                provisioner.delete(created.sandbox_id)

            deleting = registry.get(created.sandbox_id)
            assert deleting is not None
            self.assertEqual(deleting.phase, "deleting")

    def test_restart_reclaims_all_deletions_before_advancing_planned_work(
        self,
    ) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            provisioner, registry, quota, _, _ = self.make(root)
            created = provisioner.create(
                spec=replace(self.spec(), id="z-deleting"),
                sandbox_generation=7,
                operation_id="create:7",
            )
            registry.begin_delete(
                created.sandbox_id,
                expected_revision=created.revision,
            )
            registry.plan(
                spec=replace(self.spec(), id="a-planned"),
                sandbox_generation=9,
                operation_id="create:9",
                runtime_compatibility_sha256=(provisioner.runtime_compatibility_sha256),
            )
            original_prepare = quota.prepare_volume

            def prepare_only_after_reclaim(owner, **kwargs):
                if quota.active_records:
                    raise RuntimeError("storage hard capacity is exhausted")
                return original_prepare(owner, **kwargs)

            quota.prepare_volume = prepare_only_after_reclaim

            results = provisioner.start()

            self.assertIsNone(registry.get("z-deleting"))
            self.assertEqual(len(results), 1)
            self.assertEqual(results[0].sandbox_id, "a-planned")
            self.assertEqual(results[0].phase, "owned")

    def test_service_retries_durable_delete_without_node_restart(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            provisioner, registry, quota, _, _ = self.make(root)
            service = DirectSandboxService(
                provisioner,
                process_runner=FakeProcessRunner(),
                deletion_reconcile_interval_seconds=0.01,
            )
            service.start()
            try:
                created = self.create(service, self.spec())
                original_drop = quota.delete_volume
                failures_remaining = 1

                def transient_drop(owner, **kwargs):
                    nonlocal failures_remaining
                    if failures_remaining:
                        failures_remaining -= 1
                        raise OSError("injected transient quota delete failure")
                    return original_drop(owner, **kwargs)

                quota.delete_volume = transient_drop
                with self.assertRaisesRegex(OSError, "transient quota delete"):
                    service.delete(
                        created.spec.id,
                        generation=created.generation,
                    )
                self.assertEqual(registry.get(created.spec.id).phase, "deleting")

                deadline = monotonic() + 2
                while (
                    registry.get(created.spec.id) is not None and monotonic() < deadline
                ):
                    sleep(0.01)

                self.assertIsNone(registry.get(created.spec.id))
                self.assertEqual(quota.active_records, {})
            finally:
                service.stop()

    def test_start_serves_while_failed_warden_delete_retries(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            provisioner, registry, quota, _, warden = self.make(root)
            created = provisioner.create(
                spec=self.spec(),
                sandbox_generation=7,
                operation_id="create:7",
            )
            registry.begin_delete(
                created.sandbox_id,
                expected_revision=created.revision,
            )
            original_delete = warden.delete
            failures_remaining = 1

            def transient_warden_delete(sandbox):
                nonlocal failures_remaining
                if failures_remaining:
                    failures_remaining -= 1
                    raise OSError("injected mounted-volume cleanup failure")
                return original_delete(sandbox)

            warden.delete = transient_warden_delete
            service = DirectSandboxService(
                provisioner,
                process_runner=FakeProcessRunner(),
                deletion_reconcile_interval_seconds=0.01,
            )
            service.start()
            try:
                sandbox_id = created.sandbox_id
                self.assertEqual(registry.get(sandbox_id).phase, "deleting")
                deadline = monotonic() + 2
                while registry.get(sandbox_id) is not None and monotonic() < deadline:
                    sleep(0.01)

                self.assertIsNone(registry.get(sandbox_id))
                self.assertEqual(quota.active_records, {})
            finally:
                service.stop()

    def test_start_does_not_cross_audit_storage_owners(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            provisioner, _, quota, _, _ = self.make(root)
            quota.prepare_volume(
                StorageVolumeOwner(
                    volume_id="orphan.sandbox-1",
                    sandbox_id="orphan",
                    sandbox_generation=1,
                ),
                operation_id="create:orphan",
                virtual_size=4096 * 1024 * 1024,
            )

            self.assertEqual(provisioner.start(), ())
            self.assertIn(("orphan", 1), quota.active_records)

    def test_service_wakes_for_exec_and_supports_binary_file_input(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            provisioner, _, _, _, _ = self.make(root)
            runner = FakeProcessRunner()
            service = DirectSandboxService(provisioner, process_runner=runner)
            created = self.create(service, self.spec())
            service.park(created.spec.id, operation_id="park:exec")

            result = service.exec(created.spec.id, ("/bin/echo", "ok"))
            service.write_file(created.spec.id, "/workspace/payload", b"\0binary")

            self.assertEqual(result.stdout, b"ok\n")
            self.assertEqual(
                service.get(created.spec.id).state,
                HibernationState.RUNNING.value,
            )
            self.assertEqual(runner.calls[-1][1], b"\0binary")

    def test_evict_published_requires_exact_parked_registry_authority(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            provisioner, registry, quota, _, warden = self.make(root)
            service = DirectSandboxService(
                provisioner,
                process_runner=FakeProcessRunner(),
            )
            created = self.create(service, self.spec())
            service.park(created.spec.id, operation_id="park:test")
            digest = "sha256:" + "a" * 64
            warden._storage_record = lambda _sandbox: SimpleNamespace(
                state=StorageVolumeState.PUBLISHED,
                published_manifest_digest=digest,
            )

            with self.assertRaisesRegex(
                RuntimeError,
                "does not match the durable route",
            ):
                service.evict_published(
                    created.spec.id,
                    generation=created.generation,
                    snapshot_manifest_digest="sha256:" + "b" * 64,
                )

            self.assertIsNotNone(registry.get(created.spec.id))
            service.evict_published(
                created.spec.id,
                generation=created.generation,
                snapshot_manifest_digest=digest,
            )
            service.evict_published(
                created.spec.id,
                generation=created.generation,
                snapshot_manifest_digest=digest,
            )

            self.assertIsNone(registry.get(created.spec.id))
            self.assertEqual(quota.active_records, {})

    def test_publish_parked_requires_exact_owned_incarnation(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            provisioner, _, _, _, _ = self.make(root)
            service = DirectSandboxService(
                provisioner,
                process_runner=FakeProcessRunner(),
            )
            created = self.create(service, self.spec())
            sentinel = object()

            with patch.object(
                service,
                "_storage_native_snapshot_locked",
                return_value=sentinel,
            ) as publish:
                observed = service.publish_parked(
                    created.spec.id,
                    generation=created.generation,
                    create_operation_id=created.operation_id,
                    spec_hash=created.spec_hash,
                )
                with self.assertRaisesRegex(RuntimeError, "does not own"):
                    service.publish_parked(
                        created.spec.id,
                        generation=created.generation + 1,
                        create_operation_id=created.operation_id,
                        spec_hash=created.spec_hash,
                    )

            self.assertIs(observed, sentinel)
            publish.assert_called_once()

    def test_service_quarantines_dead_running_sentry_on_read(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            provisioner, _, _, _, warden = self.make(root)
            service = DirectSandboxService(
                provisioner,
                process_runner=FakeProcessRunner(),
            )
            created = self.create(service, self.spec())
            warden.alive = False

            observed = service.get(created.spec.id)

            self.assertIsNotNone(observed)
            self.assertEqual(observed.state, HibernationState.RECOVERY_REQUIRED.value)

    def test_active_admission_stops_on_live_pressure(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            provisioner, _, _, _, _ = self.make(root)
            service = DirectSandboxService(
                provisioner,
                process_runner=FakeProcessRunner(),
            )
            service.configure_active_capacity(
                ResourceQuantity(vcpu=4, memory_mb=8192),
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
                self.create(service, self.spec())

    def test_active_admission_fails_closed_without_metrics(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            provisioner, _, _, _, _ = self.make(root)
            service = DirectSandboxService(
                provisioner,
                process_runner=FakeProcessRunner(),
            )
            service.configure_active_capacity(
                ResourceQuantity(vcpu=4, memory_mb=8192),
                runtime_metrics_provider=lambda: None,
            )

            with self.assertRaisesRegex(
                SandboxCapacityUnavailableError,
                "no fresh runtime metrics",
            ):
                self.create(service, self.spec())

    def test_exec_capacity_uses_live_pressure_not_sandbox_shape(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            provisioner, _, _, _, _ = self.make(root)
            service = DirectSandboxService(
                provisioner,
                process_runner=FakeProcessRunner(),
            )
            runtime_metrics = [
                NodeRuntimeMetrics(
                    collected_at=utc_now(),
                    cpu_percent=10.0,
                    cpu_count=4,
                    memory_total_mb=4096,
                    memory_available_mb=4096,
                )
            ]
            service.configure_active_capacity(
                ResourceQuantity(vcpu=4, memory_mb=4096),
                runtime_metrics_provider=lambda: runtime_metrics[0],
            )
            records = [
                self.create(
                    service,
                    replace(
                        self.spec(),
                        id=f"sandbox-{index}",
                        cpus=2.0,
                    ),
                )
                for index in range(3)
            ]

            first = service.acquire_exec_capacity(
                records[0].spec.id,
                records[0].generation,
            )
            second = service.acquire_exec_capacity(
                records[1].spec.id,
                records[1].generation,
            )
            third = service.acquire_exec_capacity(
                records[2].spec.id,
                records[2].generation,
            )
            reservations, _epoch = service.active_reservations_snapshot()

            self.assertEqual(len(service.list_snapshot()), 3)
            self.assertEqual(len(reservations), 3)
            self.assertEqual(sum(item.vcpu for item in reservations.values()), 0)
            self.assertEqual(
                sum(item.memory_mb for item in reservations.values()),
                0,
            )

            runtime_metrics[0] = replace(runtime_metrics[0], cpu_percent=95.0)
            with self.assertRaisesRegex(
                SandboxCapacityUnavailableError,
                "CPU pressure",
            ):
                service.acquire_exec_capacity(
                    records[0].spec.id,
                    records[0].generation,
                )

            runtime_metrics[0] = replace(
                runtime_metrics[0],
                cpu_percent=10.0,
                memory_available_mb=1024,
            )
            with self.assertRaisesRegex(
                SandboxCapacityUnavailableError,
                "live memory headroom",
            ):
                service.acquire_exec_capacity(
                    records[0].spec.id,
                    records[0].generation,
                )

            service.release_exec_capacity(first)
            service.release_exec_capacity(second)
            service.release_exec_capacity(third)
            self.assertEqual(service.active_reservations_snapshot()[0], {})

    def test_exec_capacity_samples_live_pressure_in_parallel(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            provisioner, _, _, _, _ = self.make(root)
            service = DirectSandboxService(
                provisioner,
                process_runner=FakeProcessRunner(),
            )
            records = [
                self.create(
                    service,
                    replace(self.spec(), id=f"sandbox-{index}"),
                )
                for index in range(2)
            ]
            calls = 0
            calls_guard = Lock()
            both_sampling = Event()
            release_samples = Event()

            def runtime_metrics() -> NodeRuntimeMetrics:
                nonlocal calls
                with calls_guard:
                    calls += 1
                    if calls == 2:
                        both_sampling.set()
                release_samples.wait(timeout=2.0)
                return NodeRuntimeMetrics(
                    collected_at=utc_now(),
                    cpu_percent=10.0,
                    cpu_count=4,
                    memory_total_mb=4096,
                    memory_available_mb=4096,
                )

            service.configure_active_capacity(
                ResourceQuantity(vcpu=4, memory_mb=4096),
                runtime_metrics_provider=runtime_metrics,
            )
            tokens: list[str] = []
            errors: list[BaseException] = []

            def acquire(record) -> None:
                try:
                    token = service.acquire_exec_capacity(
                        record.spec.id,
                        record.generation,
                    )
                    with calls_guard:
                        tokens.append(token)
                except BaseException as exc:
                    with calls_guard:
                        errors.append(exc)

            threads = [Thread(target=acquire, args=(record,)) for record in records]
            for thread in threads:
                thread.start()
            sampled_in_parallel = both_sampling.wait(timeout=1.0)
            release_samples.set()
            for thread in threads:
                thread.join(timeout=2.0)

            self.assertTrue(sampled_in_parallel)
            self.assertEqual(errors, [])
            self.assertEqual(len(tokens), 2)
            for token in tokens:
                service.release_exec_capacity(token)

    def test_node_adapter_delete_preempts_attached_exec_but_park_does_not(
        self,
    ) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            provisioner, _, _, _, _ = self.make(root)
            service = DirectSandboxService(
                provisioner,
                process_runner=FakeProcessRunner(),
            )
            created = self.create(service, self.spec())
            manager = DirectNodeRuntime(service)

            manager.lifecycle.acquire_shared(created.spec.id)
            manager.runtime.exec_command(
                created.spec.id,
                ("/bin/sleep", "3600"),
                interactive=False,
            )
            manager.runtime.exec_started(created.spec.id)
            with self.assertRaisesRegex(SandboxBusyError, "active exec") as raised:
                manager.park(created.spec.id, operation_id="park-busy")
            self.assertIn("start_agent()", str(raised.exception))

            deleted = manager.delete(
                created.spec.id,
                generation=created.generation,
                operation_id="delete:test",
            )
            manager.lifecycle.release_shared(created.spec.id)

            self.assertEqual(deleted, created)
            self.assertIsNone(service.get(created.spec.id))

    def test_heartbeat_epoch_increases_after_highest_revision_is_deleted(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            provisioner, registry, _, _, _ = self.make(root)
            service = DirectSandboxService(
                provisioner,
                process_runner=FakeProcessRunner(),
            )
            highest = self.create(
                service,
                replace(self.spec(), id="sandbox-highest"),
                generation=1,
            )
            remaining = self.create(
                service,
                replace(self.spec(), id="sandbox-remaining"),
                generation=1,
            )
            record = registry.get(highest.spec.id)
            assert record is not None
            moving = registry.begin_move_out(
                highest.spec.id,
                expected_revision=record.revision,
                migration_id="move:highest-revision",
                migration_sha256="c" * 64,
            )
            registry.abort_move_out(
                highest.spec.id,
                expected_revision=moving.revision,
                migration_id=moving.migration_id,
                migration_sha256=moving.migration_sha256,
            )
            manager = DirectNodeRuntime(service)
            before = manager.heartbeat_snapshot(active_build_count=lambda: 0)

            service.delete(highest.spec.id, generation=highest.generation)
            after = manager.heartbeat_snapshot(active_build_count=lambda: 0)

            self.assertEqual(
                tuple(record.spec.id for record in after.activity.records),
                (remaining.spec.id,),
            )
            self.assertGreater(
                after.activity.activity_revision,
                before.activity.activity_revision,
            )

    def test_try_delete_retires_absent_mismatched_and_busy_lock_entries(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            provisioner, _, _, _, _ = self.make(root)
            service = DirectSandboxService(
                provisioner,
                process_runner=FakeProcessRunner(),
            )
            created = self.create(service, self.spec())

            self.assertTrue(service.try_delete("absent", generation=1))
            self.assertFalse(
                service.try_delete(created.spec.id, generation=created.generation + 1)
            )
            self.assertEqual(service._locks, {})

            acquired = Event()
            release = Event()

            def hold_lifecycle_lock() -> None:
                with service._lock(created.spec.id, created.generation):
                    acquired.set()
                    if not release.wait(timeout=5):
                        raise AssertionError("test did not release lifecycle lock")

            holder = Thread(target=hold_lifecycle_lock)
            holder.start()
            self.assertTrue(acquired.wait(timeout=2))
            self.assertFalse(
                service.try_delete(created.spec.id, generation=created.generation)
            )
            release.set()
            holder.join(timeout=5)

            self.assertFalse(holder.is_alive())
            self.assertEqual(service._locks, {})

    def test_drain_fences_create_before_rootfs_registration(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            provisioner, _, _, images, _ = self.make(root)
            service = DirectSandboxService(
                provisioner,
                process_runner=FakeProcessRunner(),
            )
            manager = DirectNodeRuntime(service)
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
            create_thread = Thread(
                target=lambda: created.append(self.create(service, self.spec()))
            )
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
                self.create(service, replace(self.spec(), id="rejected"))

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
            provisioner, _, _, _, _ = self.make(root)
            service = DirectSandboxService(
                provisioner,
                process_runner=FakeProcessRunner(),
            )
            manager = DirectNodeRuntime(service)
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
            provisioner, _, _, _, _ = self.make(root)
            service = DirectSandboxService(
                provisioner,
                process_runner=FakeProcessRunner(),
            )
            manager = DirectNodeRuntime(service)
            images = ImageOperations()

            with manager.image_operation(images):
                draining = manager.configure_drain(
                    "drain-image",
                    True,
                    active_build_count=lambda: images.active,
                )
                self.assertFalse(draining.ready)
                self.assertEqual(draining.active_image_builds, 1)

            ready = manager.heartbeat_snapshot(active_build_count=lambda: images.active)
            self.assertTrue(ready.ready)
            with self.assertRaises(SandboxAdmissionClosedError):
                with manager.image_operation(images):
                    pass

    def test_direct_node_drain_survives_adapter_restart(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            provisioner, _, _, _, _ = self.make(root)
            service = DirectSandboxService(
                provisioner,
                process_runner=FakeProcessRunner(),
            )
            manager = DirectNodeRuntime(service)

            drained = manager.configure_drain(
                "drain-test",
                True,
                active_build_count=lambda: 0,
            )
            restarted = DirectNodeRuntime(service)
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

    def test_direct_node_wire_uses_domain_results_without_docker_facades(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            provisioner, _, _, _, _ = self.make(root)
            service = DirectSandboxService(
                provisioner,
                process_runner=FakeProcessRunner(),
            )
            with self.running_server(root, service) as base:
                spec = self.spec()
                create_payload = {
                    **spec.to_dict(),
                    "_ucloud_operation": {
                        "generation": 1,
                        "kind": "create",
                        "operation_id": "create:wire-domain",
                        "spec_hash": sandbox_spec_fingerprint(spec),
                    },
                }
                create_request = request.Request(
                    f"{base}/v1/sandboxes",
                    data=json.dumps(create_payload).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with request.urlopen(create_request) as response:
                    created = json.load(response)

                upload_request = request.Request(
                    f"{base}/v1/sandboxes/{spec.id}/files?path=/workspace/data.txt",
                    data=b"content",
                    headers={"Content-Type": "application/octet-stream"},
                    method="PUT",
                )
                with request.urlopen(upload_request) as response:
                    uploaded = json.load(response)

                download_request = request.Request(
                    f"{base}/v1/sandboxes/{spec.id}/files?path=/workspace/data.txt"
                )
                with request.urlopen(download_request) as response:
                    downloaded = response.read()
                    download_headers = {
                        key.lower(): value for key, value in response.headers.items()
                    }

                delete_request = request.Request(
                    f"{base}/v1/sandboxes/{spec.id}",
                    headers={
                        "X-UCloud-Sandbox-Generation": "1",
                        "X-UCloud-Sandbox-Operation-Id": "delete:wire-domain",
                    },
                    method="DELETE",
                )
                with request.urlopen(delete_request) as response:
                    deleted = json.load(response)

            self.assertEqual(set(created), {"sandbox", "timings"})
            self.assertEqual(
                uploaded,
                {
                    "ok": True,
                    "path": "/workspace/data.txt",
                    "sandbox_id": spec.id,
                    "size": 7,
                },
            )
            self.assertEqual(downloaded, b"ok\n")
            self.assertEqual(download_headers["x-sandbox-id"], spec.id)
            self.assertEqual(
                download_headers["x-sandbox-path"],
                "/workspace/data.txt",
            )
            self.assertNotIn("x-docker-command", download_headers)
            self.assertNotIn("x-docker-exit-code", download_headers)
            self.assertEqual(set(deleted), {"deleted"})
            self.assertEqual(deleted["deleted"]["id"], spec.id)

    def test_direct_image_warmup_materializes_rootfs_before_reporting_ready(
        self,
    ) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            provisioner, _, _, images, _ = self.make(root)
            service = DirectSandboxService(
                provisioner,
                process_runner=FakeProcessRunner(),
            )
            with self.running_server(root, service) as base:
                outbound = request.Request(
                    f"{base}/v1/images/pull",
                    data=json.dumps({"image": "image"}).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with request.urlopen(outbound) as response:
                    pulled = json.load(response)
                heartbeat_request = request.Request(f"{base}/v1/heartbeat")
                with request.urlopen(heartbeat_request) as response:
                    heartbeat = json.load(response)["heartbeat"]

            self.assertEqual(images.materialized_refs, ["image"])
            self.assertGreaterEqual(
                pulled["timings"]["rootfs_materialize_ms"],
                0,
            )
            self.assertIn("image", heartbeat["cached_images"])
            self.assertEqual(
                heartbeat["runtime_metrics"][
                    "image_materialization_max_concurrent_operations"
                ],
                4,
            )

    def test_direct_node_park_and_explicit_wake_endpoints(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            provisioner, _, _, _, _ = self.make(root)
            service = DirectSandboxService(
                provisioner,
                process_runner=FakeProcessRunner(),
            )
            created = self.create(service, self.spec())
            with self.running_server(root, service) as base:
                park_request = request.Request(
                    f"{base}/v1/sandboxes/{created.spec.id}/park",
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
                    f"{base}/v1/sandboxes/{created.spec.id}/wake",
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


if __name__ == "__main__":
    unittest.main()
