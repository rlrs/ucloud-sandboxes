from dataclasses import replace
from contextlib import nullcontext
from io import BytesIO
import json
import shutil
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Lock
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from tests.test_storage_native_registry import FakeRegistry
from tests import test_storage_native_migration as migration_fixtures
from tests import test_direct_provisioner as provisioner_fixtures
from tests.support import make_dirs
from tests.test_split_memory_lifecycle import FakeQuota
from ucloud_sandboxes.checkpoint_components import WorkspaceCaptureRef
from ucloud_sandboxes.checkpoint_registry import (
    RegistryCheckpointStore,
    OCI_IMAGE,
    canonical_bytes,
    sparse_extents,
)
from ucloud_sandboxes.gateway.registry_refs import (
    RegistryReferences,
    release_registry_snapshot_reference,
)
from ucloud_sandboxes.direct_service import DirectSandboxService
from ucloud_sandboxes.direct_provisioner import DirectSandboxProvisioner
from ucloud_sandboxes.direct_registry import DirectSandboxRegistry
from ucloud_sandboxes.direct_oci import DirectOciConfigBuilder
from ucloud_sandboxes.managed_process import load_parked_managed_state
from ucloud_sandboxes.memory_backing import MemoryBackingStore
from ucloud_sandboxes.storage_native_daemon import StorageVolumeState
from ucloud_sandboxes.direct_warden import DirectWardenError
from ucloud_sandboxes.hibernation import HibernationArtifactStore, HibernationState
from ucloud_sandboxes.storage_native_migration import (
    StorageNativeMigration,
    StorageNativeMigrationStore,
)
from ucloud_sandboxes.storage_native_registry import (
    StorageSnapshotPublication,
    PublishedStorageLayer,
)

TEST_TIER = "contract"


class StreamRegistry(FakeRegistry):
    def open_blob(self, repository, digest):
        return BytesIO(self.blobs[digest])


class CheckpointRegistryTests(unittest.TestCase):
    managed_process = False

    def setUp(self):
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        fixture = migration_fixtures.StorageNativeMigrationTests()
        spec = replace(fixture.spec(), parkable=True, managed_process=self.managed_process)
        fixture.spec = lambda: spec
        registration, local, incarnation = fixture.make_source(self.root / "source")
        self.generation = incarnation / "hibernate-3"
        self.registration = replace(
            registration,
            version=4,
            workspace_directory="workspace-sandbox.sandbox-7",
            memory_allocation_id="sandbox.sandbox-7",
        )
        local = replace(
            local,
            version=3,
            workspace=WorkspaceCaptureRef("workspace-sandbox.sandbox-7", "d" * 64),
            memory=self.registration.memory_reference,
        )
        self.registration = replace(
            registration,
            version=4,
            workspace_directory=local.workspace.volume_id,
            memory_allocation_id=local.memory.allocation_id,
        )
        (self.generation / "COMPLETE").unlink()
        (self.generation / "manifest.json").unlink()
        self.artifacts = HibernationArtifactStore(self.root / "source")
        self.local = self.artifacts.publish_complete(local)
        self.registry = StreamRegistry()
        self.store = RegistryCheckpointStore(self.registry, repository="snapshots")
        workspace_payload = canonical_bytes(
            {"schemaVersion": 2, "mediaType": OCI_IMAGE, "config": {}, "layers": []}
        )
        digest = self.registry.put_manifest(
            "snapshots", "workspace", workspace_payload, media_type=OCI_IMAGE
        )
        self.workspace = StorageSnapshotPublication(
            digest,
            "workspace",
            "snapshots",
            "http://registry:5000/v2/snapshots/blobs",
            self.registration.spec.disk_mb * 1024**2,
            (PublishedStorageLayer("sha256:" + "2" * 64, 4096),),
        )
        self.record = SimpleNamespace(
            state=HibernationState.PARKED,
            revision=1,
            hibernation_generation=3,
            manifest_sha256=local.metadata_sha256,
        )
        self.storage = SimpleNamespace(
            state=SimpleNamespace(value="published"),
            capture_id=local.workspace.capture_id,
            volume_id=local.workspace.volume_id,
            publication=lambda: self.workspace,
            published_manifest_digest=self.workspace.manifest_digest,
        )
        self.service = object.__new__(DirectSandboxService)
        self.service.provisioner = SimpleNamespace(
            checkpoint_store=self.store,
            storage_migrations=StorageNativeMigrationStore(self.root / "publications"),
        )
        self.service.warden = SimpleNamespace(
            inspect=lambda sandbox: self.record,
            load_parked_manifest=lambda sandbox: self.local,
            artifacts=self.artifacts,
            workspace_record=lambda sandbox: self.storage,
            memory_backing=SimpleNamespace(read_lease=lambda *a, **kw: nullcontext()),
        )
        self.service._require_registration = lambda sandbox: self.registration
        self.guard = Lock()
        self.service._lock = lambda *args: self.guard
        self.service._published_snapshots = {}
        self.service._published_snapshots_guard = Lock()

    def publish(self):
        return self.service._split_storage_native_snapshot(self.registration)

    def test_complete_root_roundtrip_sparse_import_and_local_rebind(self):
        # Some development filesystems report sparse files as one dense extent.
        # Qualify the sparse wire format independently of that host behavior.
        def extents(fd, size):
            return (
                [(0, 5), (size - 3, 3)]
                if size == 16 * 1024 * 1024
                else sparse_extents(fd, size)
            )

        with patch(
            "ucloud_sandboxes.checkpoint_registry.sparse_extents", side_effect=extents
        ):
            snapshot = self.publish()
        self.assertEqual(StorageNativeMigration.from_dict(snapshot.to_dict()), snapshot)
        self.assertEqual(len(snapshot.references), 3)
        self.store.verify_root(
            snapshot.reference,
            snapshot.publication,
            snapshot.memory_publication,
            portable_manifest=snapshot.manifest.to_dict(),
        )
        memory = next(
            file
            for file in snapshot.memory_publication.files
            if file.name == "application_memory.img"
        )
        self.assertLess(memory.size, memory.logical_bytes // 2)
        destination_root = self.root / "destination"
        destination = (
            destination_root / self.generation.parent.name / self.generation.name
        )
        make_dirs(destination.parent)
        self.store.restore_memory(
            snapshot.memory_publication,
            destination,
            allowed_files={file.name for file in snapshot.memory_publication.files},
            check_current=lambda: None,
        )
        self.assertEqual(
            (destination / "application_memory.img").read_bytes(),
            (self.generation / "application_memory.img").read_bytes(),
        )
        rebound = StorageNativeMigrationStore(
            self.root / "migrations"
        ).rebind_mounted_snapshot(
            snapshot,
            expected_runtime=self.local.runtime,
            artifact_store=HibernationArtifactStore(destination_root),
            writable_incarnation=destination.parent,
        )
        self.assertEqual(rebound.version, 3)
        self.assertEqual(rebound.workspace, self.local.workspace)
        self.assertNotEqual(rebound.metadata_sha256, self.local.metadata_sha256)
        self.assertFalse((destination / ".store.lock").exists())

    def test_resume_and_repark_during_last_upload_chunk_never_promotes_root(self):
        upload = self.registry.upload_blob_chunk

        def restoring(location, chunk):
            result = upload(location, chunk)
            # Even an immediate repark cannot recover the captured revision.
            self.record = SimpleNamespace(**{**vars(self.record), "revision": 3})
            return result

        self.registry.upload_blob_chunk = restoring
        with self.assertRaisesRegex(DirectWardenError, "superseded"):
            self.publish()
        self.assertIsNone(self.service.cached_storage_native_snapshot("sandbox", 7))
        self.assertFalse(
            any(tag.startswith("checkpoint-") for tag in self.registry.manifests)
        )
        self.assertEqual(self.registry.uploads, {})

    def test_unlinked_or_replaced_source_never_promotes_root(self):
        for replace_file in (False, True):
            with self.subTest(replace=replace_file):
                upload = FakeRegistry.upload_blob_chunk.__get__(self.registry)
                path = self.generation / "checkpoint.img"
                original = path.read_bytes()

                def changed(location, chunk):
                    result = upload(location, chunk)
                    path.unlink()
                    if replace_file:
                        path.write_bytes(original)
                    return result

                self.registry.upload_blob_chunk = changed
                with self.assertRaises((DirectWardenError, FileNotFoundError)):
                    self.publish()
                if not path.exists():
                    path.write_bytes(original)
                self.assertIsNone(
                    self.service.cached_storage_native_snapshot("sandbox", 7)
                )

    def test_corrupt_blob_never_exposes_complete_destination_and_retry_recovers(self):
        snapshot = self.publish()
        file = snapshot.memory_publication.files[0]
        original = self.registry.blobs[file.digest]
        self.registry.blobs[file.digest] = original[:-1] + bytes([original[-1] ^ 1])
        destination = self.root / "imported"
        with self.assertRaises(ValueError):
            self.store.restore_memory(
                snapshot.memory_publication,
                destination,
                allowed_files={file.name for file in snapshot.memory_publication.files},
                check_current=lambda: None,
            )
        self.assertFalse(destination.exists())
        self.assertFalse((self.root / ".imported.importing").exists())
        self.registry.blobs[file.digest] = original
        self.store.restore_memory(
            snapshot.memory_publication,
            destination,
            allowed_files={file.name for file in snapshot.memory_publication.files},
            check_current=lambda: None,
        )
        self.assertTrue((destination / "COMPLETE").exists())

    def test_root_commit_failure_leaves_no_portable_pointer(self):
        put = self.registry.put_manifest

        def fail_root(repository, reference, payload, *, media_type):
            result = put(repository, reference, payload, media_type=media_type)
            if reference.startswith("checkpoint-"):
                raise OSError("ambiguous root commit")
            return result

        self.registry.put_manifest = fail_root
        with self.assertRaisesRegex(OSError, "ambiguous"):
            self.publish()
        self.assertIsNone(self.service.cached_storage_native_snapshot("sandbox", 7))
        self.registry.put_manifest = put
        self.assertIsNotNone(self.publish())

    def test_route_protects_every_component_and_successor_preserves_shared_dependencies(
        self,
    ):
        snapshot = self.publish()
        route = SimpleNamespace(
            sandbox_id="sandbox",
            generation=7,
            create_operation_id="create:7",
            node_id="node1",
            job_id="job1",
            storage_snapshot=snapshot.to_dict(),
            snapshot_repository=snapshot.reference.repository,
            snapshot_tag=snapshot.reference.tag,
        )
        usage = Mock()
        RegistryReferences(
            registry_url=None, registry_worker_url=None, usage_store=usage,
            deployment_id="test", dependency_resolver=None,
        ).ensure_snapshot_reference(
            route,
            repository=snapshot.reference.repository,
            tag=snapshot.reference.tag,
            digest=snapshot.reference.manifest_digest,
        )
        self.assertEqual(usage.acquire_reference.call_count, 3)
        release_registry_snapshot_reference(
            usage, route, deployment_id="test", keep_route=route
        )
        usage.release_lease.assert_not_called()
        release_registry_snapshot_reference(usage, route, deployment_id="test")
        self.assertEqual(usage.release_lease.call_count, 3)

    def test_changed_root_metadata_or_missing_component_rejected(self):
        snapshot = self.publish()
        with self.assertRaisesRegex(ValueError, "both exact components"):
            self.store.verify_root(
                snapshot.reference,
                snapshot.publication,
                snapshot.memory_publication,
                portable_manifest={**snapshot.manifest.to_dict(), "captured_ns": 1},
            )
        self.registry.manifests.pop(
            snapshot.memory_publication.reference.manifest_digest
        )
        with self.assertRaises(KeyError):
            self.store.verify_root(
                snapshot.reference,
                snapshot.publication,
                snapshot.memory_publication,
                portable_manifest=snapshot.manifest.to_dict(),
            )

    def import_provisioner(self):
        root = make_dirs(self.root / "destination")
        images = provisioner_fixtures.FakeImageStore(root)
        images.image = replace(
            images.image, rootfs_identity_sha256=self.local.runtime.rootfs_sha256
        )
        images.materialize = lambda ref: images.image
        overlays = provisioner_fixtures.FakeOverlays(images, root)
        overlays.park_sandbox = lambda sandbox: None
        storage = provisioner_fixtures.FakeStorage(overlays.writable_root)

        def prepare_import(owner, *, publication, operation_id, capture_id):
            record = storage.prepare_volume(
                owner, operation_id=operation_id, virtual_size=publication.virtual_size
            )
            record = replace(
                record,
                capture_id=capture_id,
                published_manifest_digest=publication.manifest_digest,
                published_tag=publication.tag,
                published_repository=publication.repository,
                published_repo_blob_url=publication.repo_blob_url,
                published_backend="registry",
                published_layers=tuple(layer.to_dict() for layer in publication.layers),
            )
            storage.records[(owner.sandbox_id, owner.sandbox_generation)] = record
            (Path(record.mount_path) / "upper").mkdir(exist_ok=True)
            return record

        storage.prepare_import = prepare_import

        def discard_resume(owner, *, operation_id):
            record = replace(
                storage.get_volume(owner.volume_id), state=StorageVolumeState.PUBLISHED
            )
            storage.records[(owner.sandbox_id, owner.sandbox_generation)] = record
            return record

        storage.discard_resume = discard_resume
        warden = provisioner_fixtures.FakeWarden(root, storage)
        warden.rootfs_lifecycle = overlays
        warden.config.runtime_fingerprint = self.local.runtime
        warden.config.memory_root.chmod(0o700)
        warden.memory_backing = MemoryBackingStore(
            warden.config.memory_root,
            root / "memory.sqlite",
            hard_capacity_bytes=8 * 1024**3,
            quota=FakeQuota(),
        )
        warden.workspace_record = lambda sandbox: storage.get_volume(
            sandbox.workspace_directory
        )
        return DirectSandboxProvisioner(
            registry=DirectSandboxRegistry(root / "registry.sqlite"),
            overlays=overlays,
            oci=DirectOciConfigBuilder(),
            warden=warden,
            checkpoint_store=self.store,
        )

    def test_provisioner_import_uses_separate_quota_and_exact_checkpoint_without_republishing(
        self,
    ):
        snapshot = self.publish()
        provisioner = self.import_provisioner()
        registration, imported = provisioner.stage_storage_native_import(
            snapshot, migration_id="import:1"
        )
        self.assertEqual(registration.phase, "import_ready")
        self.assertEqual(imported, snapshot)
        self.assertEqual(registration.memory_reference, snapshot.manifest.memory)
        self.assertNotEqual(
            Path(registration.quota_path).name, registration.memory_allocation_id
        )
        self.assertEqual(
            provisioner.warden.inspect(registration.to_direct_sandbox()).state,
            HibernationState.PARKED,
        )
        self.assertEqual(
            provisioner.stage_storage_native_import(snapshot, migration_id="import:1"),
            (registration, snapshot),
        )

    def test_interrupted_import_keeps_claim_and_resumes_exact_generation(self):
        snapshot = self.publish()
        provisioner = self.import_provisioner()
        open_blob = self.registry.open_blob
        self.registry.open_blob = Mock(side_effect=OSError("connection reset"))
        with self.assertRaisesRegex(OSError, "connection reset"):
            provisioner.stage_storage_native_import(snapshot, migration_id="import:1")
        partial = provisioner.registry.get("sandbox")
        self.assertEqual(partial.phase, "importing")
        self.assertEqual(
            provisioner.warden.memory_backing.metrics()[
                "memory_backing_hard_reserved_bytes"
            ],
            snapshot.manifest.memory.quota_bytes,
        )
        self.registry.open_blob = open_blob
        registration, imported = provisioner.stage_storage_native_import(
            snapshot, migration_id="import:1"
        )
        self.assertEqual(registration.phase, "import_ready")
        self.assertEqual(imported.reference, snapshot.reference)

    def test_restart_reuses_complete_publication_without_uploading_memory_again(self):
        snapshot = self.publish()
        self.service._published_snapshots.clear()
        self.registry.upload_blob_chunk = Mock(
            side_effect=AssertionError("unexpected reupload")
        )
        self.assertEqual(self.publish(), snapshot)
        self.assertEqual(len(list((self.root / "publications").glob("*.json"))), 1)

    def test_restore_after_root_upload_cannot_promote_even_a_complete_remote_root(self):
        put = self.registry.put_manifest

        def restore_after_put(repository, reference, payload, *, media_type):
            result = put(repository, reference, payload, media_type=media_type)
            if reference.startswith("checkpoint-"):
                self.record = SimpleNamespace(**{**vars(self.record), "revision": 2})
            return result

        self.registry.put_manifest = restore_after_put
        with self.assertRaisesRegex(DirectWardenError, "superseded"):
            self.publish()
        self.assertIsNone(self.service.cached_storage_native_snapshot("sandbox", 7))
        self.assertFalse((self.root / "publications").exists())


class ManagedStateImportTests(unittest.TestCase):
    """A move regenerates the OCI config: the state mount follows the new
    bundle, and only for a sandbox whose checkpoint was taken with it."""

    def imported_config(self, *, bound: bool) -> tuple[dict, Path]:
        fixture = CheckpointRegistryTests()
        fixture.managed_process = True
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        snapshot = fixture.publish()
        provisioner = fixture.import_provisioner()
        provisioner.oci = replace(
            provisioner.oci, managed_init_binary=Path("/bin/sh").resolve()
        )
        storage = provisioner.warden.storage
        prepare_import = storage.prepare_import

        def shipped_upper(owner, **kwargs):
            record = prepare_import(owner, **kwargs)
            if bound:
                # The source node created it host-side, in the upper it ships.
                managed = Path(record.mount_path) / "upper" / ".ucloud-managed"
                managed.mkdir(mode=0o700)
                (managed / "state.json").write_text(json.dumps({
                    "version": 1, "job_id": "agent", "spec_sha256": "e" * 64,
                    "state": "running", "pid": 4, "sequence": 3}))
                (managed / "stdout.log").write_bytes(b"x" * 321)
            return record

        storage.prepare_import = shipped_upper
        prepare_rootfs = provisioner.overlays.prepare

        def merged_upper(**kwargs):
            # FakeOverlays' rootfs is a plain directory: show the upper in it,
            # as the destination's overlay mount does.
            lease = prepare_rootfs(**kwargs)
            shutil.copytree(lease.upper, lease.merged, dirs_exist_ok=True)
            return lease

        provisioner.overlays.prepare = merged_upper
        registration, _ = provisioner.stage_storage_native_import(
            snapshot, migration_id="import:1"
        )
        self.assertEqual(registration.phase, "import_ready")
        bundle = registration.to_direct_sandbox().bundle
        return json.loads((bundle / "config.json").read_text(encoding="utf-8")), bundle

    def test_bound_sandbox_mount_follows_the_destination_bundle(self):
        config, bundle = self.imported_config(bound=True)
        mounts = [m for m in config["mounts"] if m["destination"] == "/.ucloud-managed"]
        self.assertEqual(mounts, [{
            "destination": "/.ucloud-managed",
            "options": ["rbind", "rw", "nosuid", "nodev", "noexec"],
            "source": str(bundle / "rootfs" / ".ucloud-managed"),
            "type": "bind",
        }])
        self.assertEqual(config["mounts"][-1], mounts[0])
        # The parked record is exact at the destination too: it is derived
        # from the same stopped guest's shipped files.
        record = load_parked_managed_state(bundle)
        self.assertEqual(
            {key: record[key] for key in ("sandbox_id", "sandbox_generation", "job_id",
                                         "stdout_bytes", "stderr_bytes", "cursor")},
            {"sandbox_id": "sandbox", "sandbox_generation": 7, "job_id": "agent",
             "stdout_bytes": 321, "stderr_bytes": 0, "cursor": "3.321.0"})

    def test_sandbox_parked_before_the_mount_stays_legacy_after_a_move(self):
        config, _bundle = self.imported_config(bound=False)
        self.assertEqual(
            config["annotations"]["dev.ucloud-sandboxes.managed-process"], "v1"
        )
        self.assertNotIn(
            "/.ucloud-managed", [m["destination"] for m in config["mounts"]]
        )
        self.assertIsNone(load_parked_managed_state(_bundle))
