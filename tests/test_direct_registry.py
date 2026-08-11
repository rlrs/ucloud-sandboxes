from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory
import json
import os
import sqlite3
import unittest

from ucloud_sandboxes.direct_registry import (
    DirectRegistryConflictError,
    DirectRegistryError,
    DirectSandboxRegistry,
)
from ucloud_sandboxes.direct_warden import DirectSandbox
from ucloud_sandboxes.sandbox import NodeDrainState, SandboxSpec


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
            runtime_compatibility_sha256="b" * 64,
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
            path = (root / "registry.sqlite3").resolve()
            registry = DirectSandboxRegistry(path)
            planned = registry.plan(
                spec=self.spec(),
                sandbox_generation=7,
                operation_id="create:7",
                runtime_compatibility_sha256="b" * 64,
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
            registry = DirectSandboxRegistry((Path(raw) / "registry.sqlite3").resolve())
            first = registry.plan(
                spec=self.spec(),
                sandbox_generation=7,
                operation_id="create:7",
                runtime_compatibility_sha256="b" * 64,
            )
            replay = registry.plan(
                spec=self.spec(),
                sandbox_generation=7,
                operation_id="create:7",
                runtime_compatibility_sha256="b" * 64,
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
                    runtime_compatibility_sha256="b" * 64,
                )

    def test_snapshot_observes_external_commit(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw)
            path = (root / "registry.sqlite3").resolve()
            registry = DirectSandboxRegistry(path)
            planned = registry.plan(
                spec=self.spec(),
                sandbox_generation=7,
                operation_id="create:7",
                runtime_compatibility_sha256="b" * 64,
            )

            first = registry.snapshot()
            self.assertEqual(registry.snapshot(), first)
            self.assertEqual(first.get("sandbox"), planned)

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
            path = (root / "registry.sqlite3").resolve()
            registry = DirectSandboxRegistry(path)

            self.delete_registration(registry, root, "sandbox", 7)
            deleted = registry.snapshot()
            reopened = DirectSandboxRegistry(path).snapshot()
            with closing(sqlite3.connect(path)) as connection:
                persisted_revision = connection.execute(
                    "SELECT activity_revision FROM registry_metadata"
                ).fetchone()

            self.assertEqual(deleted.records, ())
            self.assertGreater(deleted.activity_revision, 0)
            self.assertEqual(reopened.activity_revision, deleted.activity_revision)
            self.assertEqual(
                persisted_revision,
                (deleted.activity_revision,),
            )

    def test_legacy_json_registry_is_rejected_without_migration(self) -> None:
        with TemporaryDirectory() as raw:
            path = (Path(raw) / "registry.sqlite3").resolve()
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

            with self.assertRaisesRegex(DirectRegistryError, "unreadable"):
                DirectSandboxRegistry(path).snapshot()

    def test_runtime_compatibility_bind_is_durable_idempotent_and_atomic(self) -> None:
        with TemporaryDirectory() as raw:
            path = (Path(raw) / "registry.sqlite3").resolve()
            compatibilities = ("a" * 64, "d" * 64)

            def bind(compatibility: str) -> object:
                try:
                    return DirectSandboxRegistry(path).bind_runtime_compatibility(
                        compatibility
                    )
                except DirectRegistryError as exc:
                    return exc

            with ThreadPoolExecutor(max_workers=2) as pool:
                results = tuple(pool.map(bind, compatibilities))

            self.assertEqual(sum(isinstance(item, str) for item in results), 1)
            self.assertEqual(
                sum(isinstance(item, DirectRegistryError) for item in results), 1
            )
            bound = next(item for item in results if isinstance(item, str))
            self.assertEqual(
                DirectSandboxRegistry(path).bind_runtime_compatibility(bound), bound
            )
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)

    def test_compatibility_bind_rejects_incompatible_registration(self) -> None:
        with TemporaryDirectory() as raw:
            path = (Path(raw) / "registry.sqlite3").resolve()
            registry = DirectSandboxRegistry(path)
            registry.plan(
                spec=self.spec(),
                sandbox_generation=7,
                operation_id="create:7",
                runtime_compatibility_sha256="b" * 64,
            )

            with self.assertRaisesRegex(DirectRegistryError, "another runtime"):
                registry.bind_runtime_compatibility("a" * 64)
            with closing(sqlite3.connect(path)) as connection:
                persisted = connection.execute(
                    "SELECT runtime_compatibility_sha256 FROM registry_metadata"
                ).fetchone()
            self.assertEqual(persisted, (None,))

    def test_registry_metadata_rejects_noncanonical_types(self) -> None:
        with TemporaryDirectory() as raw:
            for name, column, payload, load in (
                (
                    "compatibility",
                    "runtime_compatibility_sha256",
                    "x" * 64,
                    lambda registry: registry.bind_runtime_compatibility("a" * 64),
                ),
                (
                    "drain",
                    "drain_json",
                    {
                        "admission_open": False,
                        "drain_activity_epoch": 0,
                        "draining": "true",
                        "token": "drain-test",
                    },
                    lambda registry: registry.load_drain(),
                ),
            ):
                with self.subTest(name=name):
                    path = (Path(raw) / f"{name}.sqlite3").resolve()
                    registry = DirectSandboxRegistry(path)
                    registry.bind_runtime_compatibility("a" * 64)
                    registry.save_drain(
                        NodeDrainState(
                            draining=True,
                            token="drain-test",
                            admission_open=False,
                        )
                    )
                    with closing(sqlite3.connect(path)) as connection:
                        connection.execute("PRAGMA ignore_check_constraints = ON")
                        connection.execute(
                            f"UPDATE registry_metadata SET {column} = ?",
                            (
                                json.dumps(
                                    payload, separators=(",", ":"), sort_keys=True
                                )
                                if not isinstance(payload, str)
                                else payload,
                            ),
                        )
                        connection.commit()
                    with self.assertRaisesRegex(DirectRegistryError, "invalid"):
                        load(DirectSandboxRegistry(path))

    def test_rejects_noncanonical_registration_encoding(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw)
            path = (root / "registry.sqlite3").resolve()
            registry = DirectSandboxRegistry(path)
            planned = registry.plan(
                spec=self.spec(),
                sandbox_generation=7,
                operation_id="create:7",
                runtime_compatibility_sha256="b" * 64,
            )
            with closing(sqlite3.connect(path)) as connection:
                connection.execute(
                    "UPDATE registrations SET record_json = ?",
                    (json.dumps(planned.to_dict(), indent=2, sort_keys=True),),
                )
                connection.commit()

            with self.assertRaisesRegex(
                DirectRegistryError,
                "encoding is invalid",
            ):
                DirectSandboxRegistry(path).get("sandbox")

    def test_rejects_wrong_or_extended_sqlite_schema(self) -> None:
        cases = {
            "old-version": ("PRAGMA user_version = 2",),
            "column": ("ALTER TABLE generation_tombstones ADD COLUMN obsolete TEXT",),
            "index": (
                "DROP INDEX registrations_image_id",
                "CREATE INDEX registrations_image_id " "ON registrations (sandbox_id)",
            ),
            "table": ("CREATE TABLE unexpected (value TEXT) STRICT",),
        }
        with TemporaryDirectory() as raw:
            for name, statements in cases.items():
                with self.subTest(name=name):
                    path = (Path(raw) / f"{name}.sqlite3").resolve()
                    DirectSandboxRegistry(path).snapshot()
                    with closing(sqlite3.connect(path)) as connection:
                        for statement in statements:
                            connection.execute(statement)
                        connection.commit()
                    with self.assertRaisesRegex(DirectRegistryError, "schema"):
                        DirectSandboxRegistry(path).snapshot()

    def test_rejects_nonpositive_generations(self) -> None:
        with TemporaryDirectory() as raw:
            registry = DirectSandboxRegistry((Path(raw) / "registry.sqlite3").resolve())
            with self.assertRaisesRegex(ValueError, "positive"):
                registry.plan(
                    spec=self.spec(),
                    sandbox_generation=0,
                    operation_id="create:zero",
                    runtime_compatibility_sha256="b" * 64,
                )
            with self.assertRaisesRegex(ValueError, "positive"):
                registry.plan_import(
                    spec=self.spec(),
                    sandbox_generation=0,
                    operation_id="create:zero",
                    runtime_compatibility_sha256="b" * 64,
                    migration_id="move:zero",
                    migration_sha256="c" * 64,
                )

    def test_migration_phases_are_generation_and_digest_fenced(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw)
            registry = DirectSandboxRegistry((root / "registry.sqlite3").resolve())
            planned = registry.plan_import(
                spec=self.spec(),
                sandbox_generation=7,
                operation_id="create:7",
                runtime_compatibility_sha256="b" * 64,
                migration_id="move:1",
                migration_sha256="f" * 64,
            )
            importing = registry.commit_import_quota(
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
                spec_sha256=importing.spec_sha256,
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
            registry = DirectSandboxRegistry((root / "registry.sqlite3").resolve())
            planned = registry.plan(
                spec=self.spec(),
                sandbox_generation=7,
                operation_id="create:7",
                runtime_compatibility_sha256="b" * 64,
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
                    runtime_compatibility_sha256="b" * 64,
                )
            returning = registry.plan_import(
                spec=self.spec(),
                sandbox_generation=7,
                operation_id="create:7",
                runtime_compatibility_sha256="b" * 64,
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
                    runtime_compatibility_sha256="b" * 64,
                    migration_id="move:return",
                    migration_sha256="f" * 64,
                )
            next_return = registry.plan_import(
                spec=self.spec(),
                sandbox_generation=7,
                operation_id="create:7",
                runtime_compatibility_sha256="b" * 64,
                migration_id="move:return-next",
                migration_sha256="e" * 64,
            )
            self.assertEqual(next_return.phase, "import_planned")

    def test_tombstones_share_database_and_preserve_exact_fences(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw)
            path = (root / "registry.sqlite3").resolve()
            registry = DirectSandboxRegistry(path)
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
                    runtime_compatibility_sha256="b" * 64,
                    migration_id=migration_id,
                    migration_sha256="f" * 64,
                )
                registry.abort_import_planned(
                    "migration-sandbox",
                    expected_revision=planned.revision,
                    migration_id=migration_id,
                    migration_sha256="f" * 64,
                )

            with closing(sqlite3.connect(path)) as connection:
                generations = connection.execute(
                    "SELECT COUNT(*) FROM generation_tombstones"
                ).fetchone()
                migrations = connection.execute(
                    "SELECT COUNT(*) FROM migration_tombstones"
                ).fetchone()
            self.assertEqual(generations, (5,))
            self.assertEqual(migrations, (5,))
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)

            reopened = DirectSandboxRegistry(path)
            with self.assertRaisesRegex(
                DirectRegistryConflictError,
                "tombstone",
            ):
                reopened.plan(
                    spec=self.spec("sandbox-0"),
                    sandbox_generation=7,
                    operation_id="create:7-retry",
                    runtime_compatibility_sha256="b" * 64,
                )
            newer = reopened.plan(
                spec=self.spec("sandbox-0"),
                sandbox_generation=8,
                operation_id="create:8",
                runtime_compatibility_sha256="b" * 64,
            )
            self.assertEqual(newer.phase, "planned")
            with self.assertRaisesRegex(
                DirectRegistryConflictError,
                "migration import is fenced",
            ):
                reopened.plan_import(
                    spec=self.spec("migration-sandbox"),
                    sandbox_generation=7,
                    operation_id="import:replay",
                    runtime_compatibility_sha256="b" * 64,
                    migration_id="move:0",
                    migration_sha256="f" * 64,
                )
            fresh = reopened.plan_import(
                spec=self.spec("migration-sandbox"),
                sandbox_generation=7,
                operation_id="import:fresh",
                runtime_compatibility_sha256="b" * 64,
                migration_id="move:fresh",
                migration_sha256="f" * 64,
            )
            self.assertEqual(fresh.phase, "import_planned")

    def test_concurrent_first_reads_initialize_once(self) -> None:
        with TemporaryDirectory() as raw:
            path = (Path(raw) / "registry.sqlite3").resolve()
            registry = DirectSandboxRegistry(path)

            with ThreadPoolExecutor(max_workers=4) as pool:
                results = tuple(
                    pool.map(
                        registry.references_image,
                        ("sha256:" + str(index) * 64 for index in range(4)),
                    )
                )

            self.assertEqual(results, (False,) * 4)
            self.assertEqual(registry.list(), ())

    def test_concurrent_cas_allows_one_transition(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw)
            path = (root / "registry.sqlite3").resolve()
            planned = DirectSandboxRegistry(path).plan(
                spec=self.spec("first"),
                sandbox_generation=1,
                operation_id="create:1",
                runtime_compatibility_sha256="b" * 64,
            )

            def commit(index: int) -> object:
                try:
                    return DirectSandboxRegistry(path).commit_quota(
                        "first",
                        expected_revision=planned.revision,
                        project_id=200_000 + index,
                        total_mb=4096,
                        quota_path=(root / f"quota-{index}").resolve(),
                    )
                except DirectRegistryConflictError as exc:
                    return exc

            with ThreadPoolExecutor(max_workers=2) as pool:
                results = tuple(pool.map(commit, range(2)))

            self.assertEqual(
                sum(not isinstance(result, Exception) for result in results),
                1,
            )
            self.assertEqual(
                sum(
                    isinstance(result, DirectRegistryConflictError)
                    for result in results
                ),
                1,
            )
            committed = DirectSandboxRegistry(path).get("first")
            assert committed is not None
            self.assertEqual(committed.phase, "quota_ready")


if __name__ == "__main__":
    unittest.main()
