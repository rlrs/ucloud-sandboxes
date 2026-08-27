from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
import hashlib
import shutil
import time
import unittest

from ucloud_sandboxes.storage_native_migration import (
    StorageNativeMigration,
    StorageNativeMigrationStore,
    StorageNativeSandboxManifest,
)
from ucloud_sandboxes.direct_registry import DirectSandboxRegistration
from ucloud_sandboxes.hibernation import (
    HibernationArtifactFile,
    HibernationArtifactStore,
    HibernationFileRole,
    HibernationManifest,
    HibernationRuntimeFingerprint,
    HibernationValidationError,
    LocalHibernationArtifactFile,
)
from ucloud_sandboxes.sandbox import SandboxSecuritySpec, SandboxSpec
from ucloud_sandboxes.storage_native_registry import (
    PublishedStorageLayer,
    StorageSnapshotPublication,
)


class StorageNativeMigrationTests(unittest.TestCase):
    @staticmethod
    def runtime() -> HibernationRuntimeFingerprint:
        return HibernationRuntimeFingerprint(
            runsc_sha256="a" * 64,
            runsc_commit="b" * 40,
            platform="systrap",
            architecture="x86_64",
            page_size=4096,
            cpu_features_sha256="c" * 64,
            boot_config_sha256="d" * 64,
            rootfs_sha256="e" * 64,
        )

    @staticmethod
    def spec() -> SandboxSpec:
        return SandboxSpec(
            id="sandbox",
            image="registry/image@sha256:" + "f" * 64,
            memory_mb=1024,
            disk_mb=2048,
            network="none",
            security=SandboxSecuritySpec(init=False),
        )

    def make_source(
        self,
        root: Path,
    ) -> tuple[DirectSandboxRegistration, HibernationManifest, Path]:
        spec = self.spec()
        runtime = self.runtime()
        container_id = hashlib.sha256(b"sandbox:7").hexdigest()
        incarnation = root / "sandbox.sandbox-7"
        generation = incarnation / "hibernate-3"
        upper = incarnation / "upper"
        generation.mkdir(parents=True)
        upper.mkdir()
        (upper / "workspace").mkdir()
        (upper / "workspace" / "answer.txt").write_text("portable\n")
        (upper / "relative-link").symlink_to("../workspace")

        memory = generation / "application_memory.img"
        with memory.open("wb") as handle:
            handle.write(b"begin")
            handle.seek(16 * 1024 * 1024 - 3)
            handle.write(b"end")
        checkpoint = generation / "checkpoint.img"
        checkpoint.write_bytes(b"kernel-state")
        allocator = generation / "pages_meta.img"
        allocator.write_bytes(b"allocator-state")
        files = (
            LocalHibernationArtifactFile.from_path(
                memory,
                role=HibernationFileRole.MAIN_MEMORY,
            ),
            LocalHibernationArtifactFile.from_path(
                checkpoint,
                role=HibernationFileRole.KERNEL_STATE,
            ),
            LocalHibernationArtifactFile.from_path(
                allocator,
                role=HibernationFileRole.ALLOCATOR_METADATA,
            ),
        )
        manifest = HibernationManifest(
            sandbox_id=spec.id,
            sandbox_generation=7,
            hibernation_generation=3,
            operation_id="park:test",
            spec_sha256=DirectSandboxRegistration(
                spec=spec,
                sandbox_generation=7,
                operation_id="create:7",
                runtime_compatibility_sha256=runtime.node_compatibility_sha256,
                phase="planned",
                revision=1,
                created_ns=time.time_ns(),
                updated_ns=time.time_ns(),
            ).spec_sha256,
            container_id=container_id,
            created_ns=time.time_ns(),
            runtime=runtime,
            files=files,
            managed_process_sha256="a" * 64,
        )
        HibernationArtifactStore(root).publish_complete(manifest)
        registration = DirectSandboxRegistration(
            spec=spec,
            sandbox_generation=7,
            operation_id="create:7",
            runtime_compatibility_sha256=runtime.node_compatibility_sha256,
            phase="owned",
            revision=4,
            created_ns=time.time_ns(),
            updated_ns=time.time_ns(),
            quota_project_id=200_007,
            quota_total_mb=4096,
            quota_path=str(incarnation),
            image_id="sha256:" + "2" * 64,
            rootfs_sha256=runtime.rootfs_sha256,
            container_id=container_id,
            bundle=str(root / "bundles" / "sandbox.sandbox-7"),
            memory_directory="sandbox.sandbox-7",
        )
        return registration, manifest, incarnation

    @staticmethod
    def portable_files() -> tuple[HibernationArtifactFile, ...]:
        return tuple(
            HibernationArtifactFile(name, role, 1, 1)
            for name, role in (
                ("application_memory.img", HibernationFileRole.MAIN_MEMORY),
                ("checkpoint.img", HibernationFileRole.KERNEL_STATE),
                ("pages_meta.img", HibernationFileRole.ALLOCATOR_METADATA),
            )
        )

    def test_storage_snapshot_metadata_is_durable_and_rebound_locally(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            source_root = root / "source"
            destination_root = root / "destination"
            source_root.mkdir()
            destination_root.mkdir()
            registration, source_manifest, source = self.make_source(source_root)
            portable = StorageNativeSandboxManifest.from_local(
                registration,
                source_manifest,
                source_guest_ip=None,
            )
            legacy = portable.to_dict()
            legacy["schema"] = "storage-native-runtime-v1"
            with self.assertRaisesRegex(ValueError, "unsupported"):
                StorageNativeSandboxManifest.from_dict(legacy)
            migration = StorageNativeMigration(
                manifest=portable,
                publication=StorageSnapshotPublication(
                    manifest_digest="sha256:" + "1" * 64,
                    tag="snapshot",
                    repository="snapshots",
                    repo_blob_url="https://registry/v2/snapshots/blobs",
                    virtual_size=4096,
                    layers=(
                        PublishedStorageLayer(
                            digest="sha256:" + "2" * 64,
                            size=4096,
                        ),
                    ),
                ),
            )
            store = StorageNativeMigrationStore(root / "migration-metadata")
            self.assertEqual(store.save("move:1", migration), migration)
            self.assertEqual(store.load("move:1"), migration)

            destination = destination_root / source.name
            shutil.copytree(source, destination, symlinks=True)
            with self.assertRaisesRegex(HibernationValidationError, "required runtime"):
                store.rebind_mounted_snapshot(
                    migration,
                    expected_runtime=replace(
                        self.runtime(), cpu_features_sha256="9" * 64
                    ),
                    artifact_store=HibernationArtifactStore(destination_root),
                    writable_incarnation=destination,
                )
            rebound = store.rebind_mounted_snapshot(
                migration,
                expected_runtime=self.runtime(),
                artifact_store=HibernationArtifactStore(destination_root),
                writable_incarnation=destination,
            )

            self.assertNotEqual(
                rebound.metadata_sha256,
                source_manifest.metadata_sha256,
            )
            self.assertEqual(
                HibernationArtifactStore(destination_root).load_complete(
                    sandbox_id="sandbox",
                    sandbox_generation=7,
                    hibernation_generation=3,
                ),
                rebound,
            )
            self.assertEqual(
                store.rebind_mounted_snapshot(
                    migration,
                    expected_runtime=self.runtime(),
                    artifact_store=HibernationArtifactStore(destination_root),
                    writable_incarnation=destination,
                ),
                rebound,
            )

    def test_storage_native_rebind_ignores_only_remounted_device_identity(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            registration, manifest, incarnation = self.make_source(root)
            generation = incarnation / "hibernate-3"
            (generation / HibernationArtifactStore.COMPLETE_NAME).unlink()
            (generation / HibernationArtifactStore.MANIFEST_NAME).unlink()
            remounted_manifest = replace(
                manifest,
                files=tuple(
                    replace(item, device=item.device + 10_000)
                    for item in manifest.files
                ),
            )
            artifacts = HibernationArtifactStore(
                root,
                require_stable_device=False,
            )
            artifacts.publish_complete(remounted_manifest)
            portable = StorageNativeSandboxManifest.from_local(
                registration,
                remounted_manifest,
                source_guest_ip=None,
            )
            migration = StorageNativeMigration(
                manifest=portable,
                publication=StorageSnapshotPublication(
                    manifest_digest="sha256:" + "1" * 64,
                    tag="snapshot",
                    repository="snapshots",
                    repo_blob_url="https://registry/v2/snapshots/blobs",
                    virtual_size=4096,
                    layers=(
                        PublishedStorageLayer(
                            digest="sha256:" + "2" * 64,
                            size=4096,
                        ),
                    ),
                ),
            )

            rebound, changed = StorageNativeMigrationStore(
                root / "migration-metadata"
            ).rebind_mounted_snapshot_with_status(
                migration,
                expected_runtime=self.runtime(),
                artifact_store=artifacts,
                writable_incarnation=incarnation,
            )

            self.assertFalse(changed)
            self.assertEqual(rebound, remounted_manifest)

    def test_networked_manifest_requires_disconnect_policy_and_source_ip(
        self,
    ) -> None:
        spec = SandboxSpec(
            id="sandbox",
            image="registry/image@sha256:" + "f" * 64,
            memory_mb=1024,
            disk_mb=2048,
            network="bridge",
            security=SandboxSecuritySpec(init=False),
        )
        with self.assertRaisesRegex(ValueError, "source guest IP"):
            StorageNativeSandboxManifest(
                spec=spec,
                sandbox_generation=7,
                create_operation_id="create:7",
                hibernation_generation=3,
                park_operation_id="park:test",
                captured_ns=time.time_ns(),
                runtime=self.runtime(),
                source_manifest_sha256="a" * 64,
                source_guest_ip=None,
                connection_policy="disconnect",
                files=self.portable_files(),
            )

    def test_runtime_manifest_requires_positive_sandbox_generation(self) -> None:
        runtime = self.runtime()
        with self.assertRaisesRegex(ValueError, "sandbox generation.*positive"):
            StorageNativeSandboxManifest(
                spec=self.spec(),
                sandbox_generation=0,
                create_operation_id="create:zero",
                hibernation_generation=1,
                park_operation_id="park:zero",
                captured_ns=1,
                runtime=runtime,
                source_manifest_sha256="a" * 64,
                source_guest_ip=None,
                connection_policy="none",
                files=self.portable_files(),
            )


if __name__ == "__main__":
    unittest.main()
