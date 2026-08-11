from pathlib import Path
from tempfile import TemporaryDirectory
import json
import os
import unittest

from ucloud_sandboxes.direct_registry import (
    DirectRegistryConflictError,
    DirectRegistryError,
    DirectSandboxRegistry,
)
from ucloud_sandboxes.direct_warden import DirectSandbox
from ucloud_sandboxes.sandbox import SandboxSpec


class DirectRegistryTests(unittest.TestCase):
    def spec(self, sandbox_id: str = "sandbox") -> SandboxSpec:
        return SandboxSpec(
            id=sandbox_id,
            image="registry/image@sha256:" + "a" * 64,
            memory_mb=1024,
            disk_mb=2048,
        )

    def delete_registration(
        self,
        registry: DirectSandboxRegistry,
        root: Path,
        sandbox_id: str,
        generation: int,
    ) -> None:
        planned = registry.plan(
            spec=self.spec(sandbox_id),
            sandbox_generation=generation,
            operation_id=f"create:{generation}",
            runtime_identity_sha256="b" * 64,
        )
        quota = registry.commit_quota(
            sandbox_id,
            expected_revision=planned.revision,
            project_id=200_000 + generation,
            total_mb=4096,
            quota_path=(root / "quota" / sandbox_id).resolve(),
        )
        rootfs = registry.commit_rootfs(
            sandbox_id,
            expected_revision=quota.revision,
            image_id="sha256:" + "e" * 64,
            sandbox=DirectSandbox(
                sandbox_id=sandbox_id,
                sandbox_generation=generation,
                container_id=f"{generation:064x}",
                spec_sha256=quota.spec_sha256,
                rootfs_sha256="d" * 64,
                bundle=(root / "bundles" / sandbox_id).resolve(),
                memory_directory=f"{sandbox_id}.{generation}",
            ),
        )
        owned = registry.commit_owned(
            sandbox_id,
            expected_revision=rootfs.revision,
        )
        deleting = registry.begin_delete(
            sandbox_id,
            expected_revision=owned.revision,
        )
        registry.commit_deleted(
            sandbox_id,
            sandbox_generation=generation,
            expected_revision=deleting.revision,
        )

    def test_registration_survives_every_provisioning_boundary(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw)
            path = (root / "registry.json").resolve()
            registry = DirectSandboxRegistry(path)
            planned = registry.plan(
                spec=self.spec(),
                sandbox_generation=7,
                operation_id="create:7",
                runtime_identity_sha256="b" * 64,
            )
            quota = registry.commit_quota(
                "sandbox",
                expected_revision=planned.revision,
                project_id=200_000,
                total_mb=4096,
                quota_path=(root / "quota" / "sandbox.sandbox-7").resolve(),
            )
            sandbox = DirectSandbox(
                sandbox_id="sandbox",
                sandbox_generation=7,
                container_id="c" * 64,
                spec_sha256=quota.spec_sha256,
                rootfs_sha256="d" * 64,
                bundle=(root / "bundles" / "sandbox.sandbox-7").resolve(),
                memory_directory="sandbox.sandbox-7",
            )
            rootfs = registry.commit_rootfs(
                "sandbox",
                expected_revision=quota.revision,
                image_id="sha256:" + "e" * 64,
                sandbox=sandbox,
            )
            owned = registry.commit_owned(
                "sandbox",
                expected_revision=rootfs.revision,
            )

            reopened = DirectSandboxRegistry(path).get("sandbox")
            self.assertEqual(reopened, owned)
            assert reopened is not None
            self.assertEqual(reopened.to_direct_sandbox(), sandbox)

    def test_exact_plan_replay_is_idempotent_but_mismatch_conflicts(self) -> None:
        with TemporaryDirectory() as raw:
            registry = DirectSandboxRegistry(
                (Path(raw) / "registry.json").resolve()
            )
            first = registry.plan(
                spec=self.spec(),
                sandbox_generation=7,
                operation_id="create:7",
                runtime_identity_sha256="b" * 64,
            )
            replay = registry.plan(
                spec=self.spec(),
                sandbox_generation=7,
                operation_id="create:7",
                runtime_identity_sha256="b" * 64,
            )
            self.assertEqual(first, replay)
            with self.assertRaisesRegex(
                DirectRegistryConflictError,
                "another direct registration",
            ):
                registry.plan(
                    spec=self.spec(),
                    sandbox_generation=8,
                    operation_id="create:8",
                    runtime_identity_sha256="b" * 64,
                )

    def test_snapshot_is_resident_and_invalidates_after_external_commit(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw)
            path = (root / "registry.json").resolve()
            registry = DirectSandboxRegistry(path)
            planned = registry.plan(
                spec=self.spec(),
                sandbox_generation=7,
                operation_id="create:7",
                runtime_identity_sha256="b" * 64,
            )

            first = registry.snapshot()
            self.assertIs(registry.snapshot(), first)
            self.assertIs(first.get("sandbox"), planned)

            external = DirectSandboxRegistry(path)
            committed = external.commit_quota(
                "sandbox",
                expected_revision=planned.revision,
                project_id=200_000,
                total_mb=4096,
                quota_path=(root / "quota" / "sandbox.sandbox-7").resolve(),
            )
            refreshed = registry.snapshot()

            self.assertIsNot(refreshed, first)
            self.assertEqual(refreshed.get("sandbox"), committed)
            self.assertEqual(refreshed.activity_revision, committed.revision)

    def test_global_activity_revision_survives_last_record_deletion(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw)
            path = (root / "registry.json").resolve()
            registry = DirectSandboxRegistry(path)

            self.delete_registration(registry, root, "sandbox", 7)
            deleted = registry.snapshot()
            reopened = DirectSandboxRegistry(path).snapshot()
            payload = json.loads(path.read_text(encoding="utf-8"))

            self.assertEqual(deleted.records, ())
            self.assertGreater(deleted.activity_revision, 0)
            self.assertEqual(reopened.activity_revision, deleted.activity_revision)
            self.assertEqual(payload["version"], 3)
            self.assertEqual(
                payload["activity_revision"],
                deleted.activity_revision,
            )

    def test_version_two_registry_is_rejected_without_migration(self) -> None:
        with TemporaryDirectory() as raw:
            path = (Path(raw) / "registry.json").resolve()
            path.write_text(
                json.dumps(
                    {
                        "migration_tombstones": {},
                        "records": [],
                        "tombstones": {},
                        "version": 2,
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            os.chmod(path, 0o600)

            with self.assertRaisesRegex(DirectRegistryError, "schema is invalid"):
                DirectSandboxRegistry(path).snapshot()

    def test_rejects_noncanonical_registry_schema(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw)
            path = (root / "registry.json").resolve()
            registry = DirectSandboxRegistry(path)
            planned = registry.plan(
                spec=self.spec(),
                sandbox_generation=7,
                operation_id="create:7",
                runtime_identity_sha256="b" * 64,
            )
            noncanonical = planned.to_dict()
            noncanonical.pop("migration_id")
            noncanonical.pop("migration_sha256")
            noncanonical["version"] = 1
            path.write_text(
                json.dumps(
                    {
                        "records": [noncanonical],
                        "tombstones": {},
                        "version": 1,
                    }
                )
                + "\n"
            )
            os.chmod(path, 0o600)

            with self.assertRaisesRegex(
                DirectRegistryError,
                "schema is invalid",
            ):
                DirectSandboxRegistry(path).get("sandbox")

    def test_rejects_nonpositive_generations(self) -> None:
        with TemporaryDirectory() as raw:
            registry = DirectSandboxRegistry(
                (Path(raw) / "registry.json").resolve()
            )
            with self.assertRaisesRegex(ValueError, "positive"):
                registry.plan(
                    spec=self.spec(),
                    sandbox_generation=0,
                    operation_id="create:zero",
                    runtime_identity_sha256="b" * 64,
                )
            with self.assertRaisesRegex(ValueError, "positive"):
                registry.plan_import(
                    spec=self.spec(),
                    sandbox_generation=0,
                    operation_id="create:zero",
                    runtime_identity_sha256="b" * 64,
                    migration_id="move:zero",
                    migration_sha256="c" * 64,
                )

    def test_migration_phases_are_generation_and_digest_fenced(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw)
            registry = DirectSandboxRegistry(
                (root / "registry.json").resolve()
            )
            planned = registry.plan(
                spec=self.spec(),
                sandbox_generation=7,
                operation_id="create:7",
                runtime_identity_sha256="b" * 64,
            )
            quota = registry.commit_quota(
                "sandbox",
                expected_revision=planned.revision,
                project_id=200_000,
                total_mb=4096,
                quota_path=(root / "quota" / "sandbox.sandbox-7").resolve(),
            )
            importing = registry.begin_import(
                "sandbox",
                expected_revision=quota.revision,
                migration_id="move:1",
                migration_sha256="f" * 64,
            )
            sandbox = DirectSandbox(
                sandbox_id="sandbox",
                sandbox_generation=7,
                container_id="c" * 64,
                spec_sha256=quota.spec_sha256,
                rootfs_sha256="d" * 64,
                bundle=(root / "bundles" / "sandbox.sandbox-7").resolve(),
                memory_directory="sandbox.sandbox-7",
            )
            rootfs = registry.commit_import_rootfs(
                "sandbox",
                expected_revision=importing.revision,
                image_id="sha256:" + "e" * 64,
                sandbox=sandbox,
            )
            ready = registry.commit_import_ready(
                "sandbox",
                expected_revision=rootfs.revision,
                migration_id="move:1",
                migration_sha256="f" * 64,
            )
            with self.assertRaisesRegex(
                DirectRegistryConflictError,
                "ownership fence",
            ):
                registry.activate_import(
                    "sandbox",
                    expected_revision=ready.revision,
                    migration_id="move:stale",
                    migration_sha256="f" * 64,
                )
            owned = registry.activate_import(
                "sandbox",
                expected_revision=ready.revision,
                migration_id="move:1",
                migration_sha256="f" * 64,
            )
            self.assertEqual(owned.migration_id, "move:1")
            moving = registry.begin_move_out(
                "sandbox",
                expected_revision=owned.revision,
                migration_id="move:2",
                migration_sha256="a" * 64,
            )
            restored = registry.abort_move_out(
                "sandbox",
                expected_revision=moving.revision,
                migration_id="move:2",
                migration_sha256="a" * 64,
            )

            self.assertEqual(restored.phase, "owned")
            self.assertEqual(restored.migration_id, "")

    def test_delete_tombstone_fences_delayed_create(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw)
            registry = DirectSandboxRegistry(
                (root / "registry.json").resolve()
            )
            planned = registry.plan(
                spec=self.spec(),
                sandbox_generation=7,
                operation_id="create:7",
                runtime_identity_sha256="b" * 64,
            )
            quota = registry.commit_quota(
                "sandbox",
                expected_revision=planned.revision,
                project_id=200_000,
                total_mb=4096,
                quota_path=(root / "quota" / "sandbox.sandbox-7").resolve(),
            )
            rootfs = registry.commit_rootfs(
                "sandbox",
                expected_revision=quota.revision,
                image_id="sha256:" + "e" * 64,
                sandbox=DirectSandbox(
                    sandbox_id="sandbox",
                    sandbox_generation=7,
                    container_id="c" * 64,
                    spec_sha256=quota.spec_sha256,
                    rootfs_sha256="d" * 64,
                    bundle=(root / "bundle").resolve(),
                    memory_directory="sandbox.sandbox-7",
                ),
            )
            owned = registry.commit_owned(
                "sandbox",
                expected_revision=rootfs.revision,
            )
            deleting = registry.begin_delete(
                "sandbox",
                expected_revision=owned.revision,
            )
            registry.commit_deleted(
                "sandbox",
                sandbox_generation=7,
                expected_revision=deleting.revision,
            )

            self.assertEqual(registry.list(), ())
            with self.assertRaisesRegex(
                DirectRegistryConflictError,
                "tombstone",
            ):
                registry.plan(
                    spec=self.spec(),
                    sandbox_generation=7,
                    operation_id="create:7-retry",
                    runtime_identity_sha256="b" * 64,
                )
            returning = registry.plan_import(
                spec=self.spec(),
                sandbox_generation=7,
                operation_id="create:7",
                runtime_identity_sha256="b" * 64,
                migration_id="move:return",
                migration_sha256="f" * 64,
            )
            registry.abort_import_planned(
                "sandbox",
                expected_revision=returning.revision,
                migration_id="move:return",
                migration_sha256="f" * 64,
            )
            with self.assertRaisesRegex(
                DirectRegistryConflictError,
                "migration import is fenced",
            ):
                registry.plan_import(
                    spec=self.spec(),
                    sandbox_generation=7,
                    operation_id="create:7",
                    runtime_identity_sha256="b" * 64,
                    migration_id="move:return",
                    migration_sha256="f" * 64,
                )
            next_return = registry.plan_import(
                spec=self.spec(),
                sandbox_generation=7,
                operation_id="create:7",
                runtime_identity_sha256="b" * 64,
                migration_id="move:return-next",
                migration_sha256="e" * 64,
            )
            self.assertEqual(next_return.phase, "import_planned")

    def test_tombstone_compaction_preserves_exact_archive_fences(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw)
            path = (root / "registry.json").resolve()
            registry = DirectSandboxRegistry(
                path,
                max_bytes=32 * 1024,
                max_inline_tombstones=2,
                max_inline_migration_tombstones=2,
            )
            for index in range(5):
                self.delete_registration(
                    registry,
                    root,
                    f"sandbox-{index}",
                    7,
                )
            for index in range(5):
                migration_id = f"move:{index}"
                planned = registry.plan_import(
                    spec=self.spec("migration-sandbox"),
                    sandbox_generation=7,
                    operation_id=f"import:{index}",
                    runtime_identity_sha256="b" * 64,
                    migration_id=migration_id,
                    migration_sha256="f" * 64,
                )
                registry.abort_import_planned(
                    "migration-sandbox",
                    expected_revision=planned.revision,
                    migration_id=migration_id,
                    migration_sha256="f" * 64,
                )

            payload = json.loads(path.read_text())
            self.assertLessEqual(len(payload["tombstones"]), 2)
            self.assertLessEqual(
                sum(len(values) for values in payload["migration_tombstones"].values()),
                2,
            )
            self.assertLessEqual(path.stat().st_size, 32 * 1024)
            self.assertEqual(
                registry.tombstone_archive_path.stat().st_mode & 0o777,
                0o600,
            )

            reopened = DirectSandboxRegistry(
                path,
                max_bytes=32 * 1024,
                max_inline_tombstones=2,
                max_inline_migration_tombstones=2,
            )
            with self.assertRaisesRegex(
                DirectRegistryConflictError,
                "tombstone",
            ):
                reopened.plan(
                    spec=self.spec("sandbox-0"),
                    sandbox_generation=7,
                    operation_id="create:7-retry",
                    runtime_identity_sha256="b" * 64,
                )
            newer = reopened.plan(
                spec=self.spec("sandbox-0"),
                sandbox_generation=8,
                operation_id="create:8",
                runtime_identity_sha256="b" * 64,
            )
            reopened.abort_planned(
                "sandbox-0",
                expected_revision=newer.revision,
            )
            with self.assertRaisesRegex(
                DirectRegistryConflictError,
                "migration import is fenced",
            ):
                reopened.plan_import(
                    spec=self.spec("migration-sandbox"),
                    sandbox_generation=7,
                    operation_id="import:replay",
                    runtime_identity_sha256="b" * 64,
                    migration_id="move:0",
                    migration_sha256="f" * 64,
                )
            fresh = reopened.plan_import(
                spec=self.spec("migration-sandbox"),
                sandbox_generation=7,
                operation_id="import:fresh",
                runtime_identity_sha256="b" * 64,
                migration_id="move:fresh",
                migration_sha256="f" * 64,
            )
            self.assertEqual(fresh.phase, "import_planned")

    def test_oversized_update_preserves_readable_registry(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw)
            path = (root / "registry.json").resolve()
            registry = DirectSandboxRegistry(path)
            first = registry.plan(
                spec=self.spec("first"),
                sandbox_generation=1,
                operation_id="create:1",
                runtime_identity_sha256="b" * 64,
            )
            before = path.read_bytes()
            registry._max_bytes = len(before) + 1

            with self.assertRaisesRegex(
                DirectRegistryError,
                "update exceeds",
            ):
                registry.plan(
                    spec=self.spec("second"),
                    sandbox_generation=1,
                    operation_id="create:1",
                    runtime_identity_sha256="c" * 64,
                )

            self.assertEqual(path.read_bytes(), before)
            self.assertEqual(DirectSandboxRegistry(path).list(), (first,))

if __name__ == "__main__":
    unittest.main()
