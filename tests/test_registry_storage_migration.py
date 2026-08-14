from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from ucloud_sandboxes.registry_storage_migration import (
    execute_filesystem_registry_to_s3,
    plan_filesystem_registry_to_s3,
)
from ucloud_sandboxes.storage_native_s3 import S3ObjectStat


class FakeMigrationS3:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.digests: dict[str, str] = {}

    def stat(self, key: str) -> S3ObjectStat | None:
        payload = self.objects.get(key)
        if payload is None:
            return None
        return S3ObjectStat(
            size=len(payload),
            sha256=self.digests.get(key, ""),
        )

    def put_file(self, key: str, path: Path, *, sha256: str) -> None:
        self.objects[key] = path.read_bytes()
        self.digests[key] = sha256


class RegistryStorageMigrationTests(unittest.TestCase):
    def test_fresh_target_uploads_and_then_becomes_idempotent(self) -> None:
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir) / "registry"
            first = root / "docker/registry/v2/blobs/sha256/a/data"
            second = root / "docker/registry/v2/repositories/example/_manifests/tag/current/link"
            first.parent.mkdir(parents=True)
            second.parent.mkdir(parents=True)
            first.write_bytes(b"layer")
            second.write_bytes(b"sha256:digest")
            client = FakeMigrationS3()

            plan = plan_filesystem_registry_to_s3(
                client,
                source_root=root,
                target_prefix="/production/oci/",
            )
            uploaded = execute_filesystem_registry_to_s3(
                client,
                plan,
                max_concurrency=2,
            )
            repeated = plan_filesystem_registry_to_s3(
                client,
                source_root=root,
                target_prefix="production/oci",
            )

        self.assertEqual(uploaded, 2)
        self.assertEqual(plan.to_dict()["uploadObjects"], 2)
        self.assertEqual(repeated.to_dict()["existingObjects"], 2)
        self.assertEqual(
            client.objects[
                "production/oci/docker/registry/v2/blobs/sha256/a/data"
            ],
            b"layer",
        )

    def test_conflicting_target_requires_explicit_overwrite(self) -> None:
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir) / "registry"
            path = root / "docker/registry/v2/repositories/example/link"
            path.parent.mkdir(parents=True)
            path.write_bytes(b"new")
            client = FakeMigrationS3()
            key = "production/oci/docker/registry/v2/repositories/example/link"
            client.objects[key] = b"old"
            client.digests[key] = "different"
            plan = plan_filesystem_registry_to_s3(
                client,
                source_root=root,
                target_prefix="production/oci",
            )

            with self.assertRaisesRegex(RuntimeError, "refusing to overwrite"):
                execute_filesystem_registry_to_s3(client, plan)
            uploaded = execute_filesystem_registry_to_s3(
                client,
                plan,
                allow_overwrite=True,
            )

        self.assertEqual(uploaded, 1)
        self.assertEqual(client.objects[key], b"new")

    def test_source_symbolic_links_are_rejected(self) -> None:
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir) / "registry"
            root.mkdir()
            target = Path(raw_dir) / "outside"
            target.write_bytes(b"outside")
            (root / "link").symlink_to(target)

            with self.assertRaisesRegex(ValueError, "symbolic link"):
                plan_filesystem_registry_to_s3(
                    FakeMigrationS3(),
                    source_root=root,
                    target_prefix="production/oci",
                )

    def test_execute_rejects_file_replaced_by_symlink_after_planning(self) -> None:
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir) / "registry"
            path = root / "docker/registry/v2/blob"
            path.parent.mkdir(parents=True)
            path.write_bytes(b"planned")
            outside = Path(raw_dir) / "outside"
            outside.write_bytes(b"planned")
            client = FakeMigrationS3()
            plan = plan_filesystem_registry_to_s3(
                client,
                source_root=root,
                target_prefix="production/oci",
            )
            path.unlink()
            path.symlink_to(outside)

            with self.assertRaisesRegex(RuntimeError, "unsafe"):
                execute_filesystem_registry_to_s3(client, plan)

        self.assertEqual(client.objects, {})

    def test_execute_rejects_directory_symlink_swap_after_planning(self) -> None:
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir) / "registry"
            directory = root / "docker"
            path = directory / "registry/v2/blob"
            path.parent.mkdir(parents=True)
            path.write_bytes(b"planned")
            outside_directory = Path(raw_dir) / "outside"
            outside_path = outside_directory / "registry/v2/blob"
            outside_path.parent.mkdir(parents=True)
            outside_path.write_bytes(b"planned")
            client = FakeMigrationS3()
            plan = plan_filesystem_registry_to_s3(
                client,
                source_root=root,
                target_prefix="production/oci",
            )
            moved = root / "original-docker"
            directory.rename(moved)
            directory.symlink_to(outside_directory, target_is_directory=True)

            with self.assertRaisesRegex(RuntimeError, "unsafe"):
                execute_filesystem_registry_to_s3(client, plan)

        self.assertEqual(client.objects, {})

    def test_execute_rejects_source_mutation_and_bad_remote_verification(self) -> None:
        class TruncatingMigrationS3(FakeMigrationS3):
            def put_file(self, key: str, path: Path, *, sha256: str) -> None:
                super().put_file(key, path, sha256=sha256)
                self.objects[key] = self.objects[key][:-1]

        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir) / "registry"
            path = root / "blob"
            path.parent.mkdir(parents=True)
            path.write_bytes(b"planned")
            plan = plan_filesystem_registry_to_s3(
                FakeMigrationS3(),
                source_root=root,
                target_prefix="production/oci",
            )
            path.write_bytes(b"changed")
            with self.assertRaisesRegex(RuntimeError, "changed before upload"):
                execute_filesystem_registry_to_s3(FakeMigrationS3(), plan)

            path.write_bytes(b"planned")
            fresh = plan_filesystem_registry_to_s3(
                FakeMigrationS3(),
                source_root=root,
                target_prefix="production/oci",
            )
            with self.assertRaisesRegex(RuntimeError, "post-upload verification"):
                execute_filesystem_registry_to_s3(TruncatingMigrationS3(), fresh)

    def test_plan_enforces_object_bound_and_normalized_prefix(self) -> None:
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir) / "registry"
            root.mkdir()
            (root / "one").write_bytes(b"1")
            (root / "two").write_bytes(b"2")

            with self.assertRaisesRegex(ValueError, "max_objects=1"):
                plan_filesystem_registry_to_s3(
                    FakeMigrationS3(),
                    source_root=root,
                    target_prefix="production/oci",
                    max_objects=1,
                )
            for prefix in ("", "/", "production/../oci", "production//oci"):
                with self.subTest(prefix=prefix), self.assertRaisesRegex(
                    ValueError,
                    "prefix",
                ):
                    plan_filesystem_registry_to_s3(
                        FakeMigrationS3(),
                        source_root=root,
                        target_prefix=prefix,
                    )


if __name__ == "__main__":
    unittest.main()
