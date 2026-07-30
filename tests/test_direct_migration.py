from pathlib import Path
from tempfile import TemporaryDirectory
import hashlib
import io
import json
import tarfile
import time
import unittest

from ucloud_sandboxes.direct_migration import (
    DIRECT_MIGRATION_METADATA,
    DirectMigrationArchiveStore,
    DirectMigrationError,
    DirectMigrationManifest,
    PortableArtifactFile,
)
from ucloud_sandboxes.direct_registry import DirectSandboxRegistration
from ucloud_sandboxes.hibernation import (
    HibernationArtifactFile,
    HibernationArtifactStore,
    HibernationFileRole,
    HibernationManifest,
    HibernationRuntimeFingerprint,
)
from ucloud_sandboxes.runtime_identity import NodeRuntimeIdentity
from ucloud_sandboxes.sandbox import SandboxSecuritySpec, SandboxSpec


class DirectMigrationTests(unittest.TestCase):
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
            security=SandboxSecuritySpec(init=False),
        )

    def make_source(
        self,
        root: Path,
    ) -> tuple[
        DirectSandboxRegistration,
        HibernationManifest,
        NodeRuntimeIdentity,
        Path,
    ]:
        spec = self.spec()
        runtime = self.runtime()
        identity = NodeRuntimeIdentity.from_fingerprint(runtime)
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
            HibernationArtifactFile.from_path(
                memory,
                role=HibernationFileRole.MAIN_MEMORY,
            ),
            HibernationArtifactFile.from_path(
                checkpoint,
                role=HibernationFileRole.KERNEL_STATE,
            ),
            HibernationArtifactFile.from_path(
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
                runtime_identity_sha256=identity.digest,
                phase="planned",
                revision=1,
                created_ns=time.time_ns(),
                updated_ns=time.time_ns(),
            ).spec_sha256,
            container_id=container_id,
            created_ns=time.time_ns(),
            runtime=runtime,
            files=files,
        )
        HibernationArtifactStore(root).publish_complete(manifest)
        registration = DirectSandboxRegistration(
            spec=spec,
            sandbox_generation=7,
            operation_id="create:7",
            runtime_identity_sha256=identity.digest,
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
        return registration, manifest, identity, incarnation

    def test_archive_rebinds_local_inode_manifest_on_destination(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            source_root = root / "source"
            destination_root = root / "destination"
            source_root.mkdir()
            destination_root.mkdir()
            registration, source_manifest, identity, source = self.make_source(
                source_root
            )
            archive_path = root / "sandbox-move.tar"
            store = DirectMigrationArchiveStore()

            exported = store.export(
                registration=registration,
                local_manifest=source_manifest,
                runtime_identity=identity,
                writable_incarnation=source,
                archive_path=archive_path,
            )
            with tarfile.open(archive_path, "r:*") as archive:
                first = archive.next()
                self.assertIsNotNone(first)
                assert first is not None
                self.assertEqual(first.name, DIRECT_MIGRATION_METADATA)
            self.assertEqual(
                store.read_manifest(
                    archive_path,
                    expected_sha256=exported.sha256,
                ),
                exported.manifest,
            )
            destination = destination_root / source.name
            portable, imported = store.import_archive(
                archive_path,
                expected_sha256=exported.sha256,
                expected_runtime_identity=identity,
                expected_runtime=self.runtime(),
                artifact_store=HibernationArtifactStore(destination_root),
                writable_incarnation=destination,
            )

            self.assertEqual(portable.sandbox_id, "sandbox")
            self.assertNotEqual(
                source_manifest.metadata_sha256,
                imported.metadata_sha256,
            )
            self.assertNotEqual(
                source_manifest.files[0].inode,
                imported.files[0].inode,
            )
            self.assertEqual(
                (destination / "upper" / "workspace" / "answer.txt").read_text(),
                "portable\n",
            )
            self.assertEqual(
                (
                    destination
                    / "hibernate-3"
                    / "application_memory.img"
                ).stat().st_size,
                16 * 1024 * 1024,
            )
            reloaded = HibernationArtifactStore(destination_root).load_complete(
                sandbox_id="sandbox",
                sandbox_generation=7,
                hibernation_generation=3,
            )
            self.assertEqual(reloaded, imported)

    def test_archive_digest_is_required_before_extraction(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            source_root = root / "source"
            destination_root = root / "destination"
            source_root.mkdir()
            destination_root.mkdir()
            registration, manifest, identity, source = self.make_source(source_root)
            archive_path = root / "sandbox-move.tar"
            store = DirectMigrationArchiveStore()
            store.export(
                registration=registration,
                local_manifest=manifest,
                runtime_identity=identity,
                writable_incarnation=source,
                archive_path=archive_path,
            )

            with self.assertRaisesRegex(DirectMigrationError, "digest"):
                store.import_archive(
                    archive_path,
                    expected_sha256="0" * 64,
                    expected_runtime_identity=identity,
                    expected_runtime=self.runtime(),
                    artifact_store=HibernationArtifactStore(destination_root),
                    writable_incarnation=destination_root / source.name,
                )
            self.assertFalse((destination_root / source.name).exists())

    def test_rejects_archive_member_outside_portable_inventory(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            archive_path = root / "unsafe.tar"
            runtime = self.runtime()
            spec = self.spec()
            portable = DirectMigrationManifest(
                spec=spec,
                sandbox_generation=7,
                create_operation_id="create:7",
                runtime_identity=NodeRuntimeIdentity.from_fingerprint(runtime),
                hibernation_generation=3,
                park_operation_id="park:test",
                captured_ns=time.time_ns(),
                runtime=runtime,
                source_manifest_sha256="a" * 64,
                source_guest_ip=None,
                connection_policy="none",
                files=(
                    PortableArtifactFile(
                        "application_memory.img",
                        HibernationFileRole.MAIN_MEMORY,
                        1,
                        1,
                    ),
                    PortableArtifactFile(
                        "checkpoint.img",
                        HibernationFileRole.KERNEL_STATE,
                        1,
                        1,
                    ),
                    PortableArtifactFile(
                        "pages_meta.img",
                        HibernationFileRole.ALLOCATOR_METADATA,
                        1,
                        1,
                    ),
                ),
            )
            with tarfile.open(archive_path, "w") as archive:
                metadata = json.dumps(portable.to_dict()).encode("ascii")
                info = tarfile.TarInfo(DIRECT_MIGRATION_METADATA)
                info.size = len(metadata)
                archive.addfile(info, io.BytesIO(metadata))
                unsafe = tarfile.TarInfo("../../escape")
                unsafe.size = 1
                archive.addfile(unsafe, io.BytesIO(b"x"))

            with self.assertRaisesRegex(DirectMigrationError, "unsafe path"):
                DirectMigrationArchiveStore().inspect(archive_path)

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
            DirectMigrationManifest(
                spec=spec,
                sandbox_generation=7,
                create_operation_id="create:7",
                runtime_identity=NodeRuntimeIdentity.from_fingerprint(self.runtime()),
                hibernation_generation=3,
                park_operation_id="park:test",
                captured_ns=time.time_ns(),
                runtime=self.runtime(),
                source_manifest_sha256="a" * 64,
                source_guest_ip=None,
                connection_policy="disconnect",
                files=(
                    PortableArtifactFile(
                        "application_memory.img",
                        HibernationFileRole.MAIN_MEMORY,
                        1,
                        1,
                    ),
                    PortableArtifactFile(
                        "checkpoint.img",
                        HibernationFileRole.KERNEL_STATE,
                        1,
                        1,
                    ),
                    PortableArtifactFile(
                        "pages_meta.img",
                        HibernationFileRole.ALLOCATOR_METADATA,
                        1,
                        1,
                    ),
                ),
            )


if __name__ == "__main__":
    unittest.main()
