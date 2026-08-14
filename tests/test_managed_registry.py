from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import multiprocessing
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
from threading import Event
import unittest
from unittest.mock import patch

from hypothesis import given, settings, strategies as st

from ucloud_sandboxes.managed_registry import (
    digest_protection_tag,
    RegistryRequestError,
    RegistryImageLeaseNotFound,
    RegistryUsageGenerationChanged,
    RegistryUsageStateError,
    RegistryUsageStore,
    RegistryTag,
    execute_registry_prune,
    list_registry_tags,
    registry_summary,
    select_prune_candidates,
)


LEASE_DIGEST = "sha256:" + "d" * 64


class DeletingRegistryClient:
    def __init__(self) -> None:
        self.deleted: list[tuple[str, str]] = []

    def delete_manifest(self, repository: str, digest: str) -> None:
        self.deleted.append((repository, digest))


def _acquire_lease_in_process(
    path: str,
    repository: str,
    tag: str,
    owner: str,
    result_queue: object,
    start_event: object | None = None,
) -> None:
    if start_event is not None:
        start_event.wait()  # type: ignore[attr-defined]
    lease = RegistryUsageStore(Path(path)).acquire_lease(
        repository,
        tag,
        owner,
        ttl_seconds=60,
        digest=LEASE_DIGEST,
    )
    result_queue.put(lease.owner)  # type: ignore[attr-defined]


class ManagedRegistryTests(unittest.TestCase):
    def test_registry_views_tolerate_catalog_entries_removed_during_scan(self) -> None:
        class FakeRegistryClient:
            base_url = "http://registry"

            def catalog(self) -> list[str]:
                return ["repo/a", "repo/missing"]

            def tags(self, repository: str) -> list[str]:
                if repository == "repo/missing":
                    raise RegistryRequestError(
                        404,
                        "GET",
                        "/v2/repo/missing/tags/list",
                        '{"errors":[{"code":"NAME_UNKNOWN"}]}',
                    )
                return ["v1"]

            def tag_record(self, repository: str, tag: str) -> RegistryTag | None:
                return RegistryTag(repository, tag, "sha256:1")

        client = FakeRegistryClient()
        summary = registry_summary(client)  # type: ignore[arg-type]
        records = list_registry_tags(client)  # type: ignore[arg-type]

        self.assertEqual(summary["unavailable_repositories"], ["repo/missing"])
        self.assertEqual(
            [(record.repository, record.tag) for record in records],
            [("repo/a", "v1")],
        )

    @settings(max_examples=100, deadline=None)
    @given(st.lists(st.booleans(), min_size=1, max_size=12))
    def test_prune_shared_digest_requires_every_alias_to_be_expired(
        self,
        expired_aliases: list[bool],
    ) -> None:
        now = datetime(2026, 6, 7, tzinfo=timezone.utc)
        digest = "sha256:" + "a" * 64
        records = [
            RegistryTag(
                "repo/a",
                f"alias-{index}",
                digest,
                "2026-06-01T00:00:00+00:00",
                (now - timedelta(days=10) if expired else now).isoformat(),
            )
            for index, expired in enumerate(expired_aliases)
        ]

        candidates = select_prune_candidates(
            records,
            keep_per_repository=0,
            max_age_days=3,
            use_last_used_at=True,
            now=now,
        )
        reversed_candidates = select_prune_candidates(
            list(reversed(records)),
            keep_per_repository=0,
            max_age_days=3,
            use_last_used_at=True,
            now=now,
        )
        expected = {record.tag for record in records} if all(expired_aliases) else set()

        self.assertEqual({record.tag for record in candidates}, expected)
        self.assertEqual(
            {record.tag for record in reversed_candidates},
            expected,
        )

    def test_registry_usage_generation_detects_stale_maintenance_snapshot(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = RegistryUsageStore(Path(raw_dir) / "usage.json")
            original = store.snapshot()
            store.touch_image(
                "localhost:5000/repo/image:v1",
                when=datetime(2026, 6, 7, tzinfo=timezone.utc),
            )

            with self.assertRaises(RegistryUsageGenerationChanged):
                store.save({}, expected_generation=original.generation)
            self.assertEqual(store.path.stat().st_mode & 0o777, 0o600)

    def test_registry_leases_acquire_renew_release_and_expire(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = RegistryUsageStore(Path(raw_dir) / "usage.json")
            started = datetime(2026, 7, 9, 10, 0, tzinfo=timezone.utc)
            identity = ("repo/a", "v1", "sandbox:one")

            acquired = store.acquire_lease(
                *identity,
                ttl_seconds=30,
                digest=LEASE_DIGEST,
                now=started,
            )
            renewed = store.renew_lease(
                *identity,
                ttl_seconds=60,
                digest=LEASE_DIGEST,
                now=started + timedelta(seconds=10),
            )
            self.assertEqual(acquired.acquired_at, renewed.acquired_at)
            self.assertNotEqual(acquired.expires_at, renewed.expires_at)
            self.assertTrue(
                store.release_lease(
                    *identity,
                    now=started + timedelta(seconds=11),
                )
            )
            with self.assertRaises(RegistryImageLeaseNotFound):
                store.renew_lease(
                    *identity,
                    ttl_seconds=60,
                    digest=LEASE_DIGEST,
                    now=started + timedelta(seconds=12),
                )

            store.acquire_lease(
                "repo/a",
                "v1",
                "sandbox:two",
                ttl_seconds=1,
                digest=LEASE_DIGEST,
                now=started + timedelta(seconds=20),
            )
            before_expiry = store.snapshot(now=started + timedelta(seconds=20))
            after_expiry = store.snapshot(now=started + timedelta(seconds=22))

            self.assertEqual(len(before_expiry.leases), 1)
            self.assertEqual(after_expiry.leases, {})
            self.assertEqual(after_expiry.generation, before_expiry.generation + 1)

    def test_registry_reference_does_not_expire_without_renewal(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = RegistryUsageStore(Path(raw_dir) / "usage.json")
            started = datetime(2026, 7, 9, 10, 0, tzinfo=timezone.utc)

            reference = store.acquire_reference(
                "repo/a",
                "v1",
                "sandbox:one",
                digest=LEASE_DIGEST,
                now=started,
            )
            loaded = RegistryUsageStore(store.path).snapshot(
                now=started + timedelta(days=3650)
            )

            self.assertEqual(reference.expires_at, "")
            self.assertIn(("repo/a", "v1", "sandbox:one"), loaded.leases)
            self.assertEqual(
                loaded.active_lease_digests(),
                {("repo/a", LEASE_DIGEST)},
            )

    def test_registry_lease_digest_is_required_and_immutable(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = RegistryUsageStore(Path(raw_dir) / "usage.json")
            with self.assertRaisesRegex(ValueError, "digest is required"):
                store.acquire_reference(
                    "repo/a",
                    "v1",
                    "sandbox:one",
                    digest="",
                )
            store.acquire_reference(
                "repo/a",
                "v1",
                "sandbox:one",
                digest=LEASE_DIGEST,
            )
            with self.assertRaisesRegex(ValueError, "digest is immutable"):
                store.acquire_reference(
                    "repo/a",
                    "v1",
                    "sandbox:one",
                    digest="sha256:" + "e" * 64,
                )

    def test_usage_store_rejects_legacy_and_incompatible_schemas(self) -> None:
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            legacy = root / "usage.json"
            legacy.write_text(
                '{"generation": 1, "images": [], "leases": []}',
                encoding="utf-8",
            )
            with self.assertRaises(RegistryUsageStateError):
                RegistryUsageStore(legacy)

            cases = (
                ("unversioned", False, "CREATE TABLE obsolete_state (value TEXT)"),
                ("wrong-version", False, "PRAGMA user_version = 2"),
                (
                    "wrong-columns",
                    True,
                    "ALTER TABLE registry_images "
                    "RENAME COLUMN last_used_at TO lastUsedAt",
                ),
            )
            for name, initialize, mutation in cases:
                with self.subTest(name=name):
                    path = root / f"{name}.sqlite"
                    if initialize:
                        RegistryUsageStore(path)
                    with sqlite3.connect(path) as conn:
                        conn.executescript(mutation)
                    with self.assertRaisesRegex(
                        RegistryUsageStateError, "invalid or unavailable"
                    ):
                        RegistryUsageStore(path)

    def test_usage_updates_preserve_active_leases(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = RegistryUsageStore(Path(raw_dir) / "usage.json")
            store.acquire_lease(
                "repo/a",
                "v1",
                "sandbox:one",
                ttl_seconds=60,
                digest=LEASE_DIGEST,
            )

            store.touch_image("localhost:5000/repo/a:v1")
            touched = store.snapshot()
            store.save(touched.records, expected_generation=touched.generation)
            saved = store.snapshot()

            self.assertIn(("repo/a", "v1", "sandbox:one"), touched.leases)
            self.assertIn(("repo/a", "v1", "sandbox:one"), saved.leases)

    def test_registry_lease_ttl_must_be_positive_finite_and_bounded(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = RegistryUsageStore(Path(raw_dir) / "usage.json")

            for ttl in (0, -1, float("nan"), float("inf"), 100_000):
                with self.subTest(ttl=ttl), self.assertRaises(ValueError):
                    store.acquire_lease(
                        "repo/a",
                        "v1",
                        "sandbox:one",
                        ttl_seconds=ttl,
                        digest=LEASE_DIGEST,
                    )

    def test_sqlite_busy_error_uses_registry_state_contract(self) -> None:
        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "usage.sqlite"
            store = RegistryUsageStore(path)
            blocker = sqlite3.connect(path, isolation_level=None)
            blocker.execute("BEGIN EXCLUSIVE")
            try:
                with (
                    patch.object(
                        store,
                        "_connect",
                        side_effect=lambda: sqlite3.connect(
                            path,
                            timeout=0,
                            isolation_level=None,
                        ),
                    ),
                    self.assertRaises(RegistryUsageStateError),
                ):
                    store.touch_image("localhost:5000/repo/a:v1")
            finally:
                blocker.rollback()
                blocker.close()

    def test_malformed_sqlite_rows_fail_closed(self) -> None:
        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "usage.sqlite"
            store = RegistryUsageStore(path)
            with sqlite3.connect(path) as conn:
                conn.execute(
                    "INSERT INTO registry_images VALUES (?, ?, ?, ?)",
                    ("registry/repo/a:v1", "repo/a", "v1", "not-a-timestamp"),
                )
            with self.assertRaisesRegex(ValueError, "invalid image"):
                store.snapshot()

            with sqlite3.connect(path) as conn:
                conn.execute("DELETE FROM registry_images")
                conn.execute(
                    "INSERT INTO registry_leases VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        "repo/a",
                        "v1",
                        "sandbox:one",
                        "2026-07-10T00:00:00+00:00",
                        "2026-07-10T00:00:00+00:00",
                        "",
                        "",
                    ),
                )
            with self.assertRaisesRegex(ValueError, "invalid lease"):
                store.snapshot()

    def test_concurrent_process_lease_acquisition_loses_no_owner(self) -> None:
        with TemporaryDirectory() as raw_dir:
            path = str(Path(raw_dir) / "usage.json")
            context = multiprocessing.get_context("spawn")
            results = context.Queue()
            start = context.Event()
            processes = [
                context.Process(
                    target=_acquire_lease_in_process,
                    args=(path, "repo/a", "v1", owner, results, start),
                )
                for owner in ("sandbox:one", "sandbox:two")
            ]

            for process in processes:
                process.start()
            start.set()
            for process in processes:
                process.join(timeout=10)
                if process.is_alive():
                    process.terminate()
                    process.join(timeout=2)

            self.assertEqual([process.exitcode for process in processes], [0, 0])
            self.assertEqual(
                {results.get(timeout=2), results.get(timeout=2)},
                {"sandbox:one", "sandbox:two"},
            )
            snapshot = RegistryUsageStore(Path(path)).snapshot()
            self.assertEqual(snapshot.generation, 2)
            self.assertEqual(len(snapshot.leases), 2)

    def test_cross_process_lease_acquired_after_plan_prevents_delete(self) -> None:
        class FakeRegistryClient:
            def __init__(self) -> None:
                self.deleted: list[tuple[str, str]] = []

            def delete_manifest(self, repository: str, digest: str) -> None:
                self.deleted.append((repository, digest))

        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "usage.json"
            store = RegistryUsageStore(path)
            records = [RegistryTag("repo/a", "v1", LEASE_DIGEST)]
            planned = select_prune_candidates(records, keep_per_repository=0)
            self.assertEqual(planned, records)

            context = multiprocessing.get_context("spawn")
            results = context.Queue()
            process = context.Process(
                target=_acquire_lease_in_process,
                args=(str(path), "repo/a", "v1", "sandbox:new", results),
            )
            process.start()
            process.join(timeout=10)
            if process.is_alive():
                process.terminate()
                process.join(timeout=2)
            self.assertEqual(process.exitcode, 0)
            self.assertEqual(results.get(timeout=2), "sandbox:new")

            client = FakeRegistryClient()
            deleted = execute_registry_prune(
                client,  # type: ignore[arg-type]
                planned,
                usage_store=store,
                all_records=records,
            )

            self.assertEqual(deleted, [])
            self.assertEqual(client.deleted, [])

    def test_lease_fence_serializes_reference_with_remote_delete(self) -> None:
        delete_started = Event()
        finish_delete = Event()
        reference_acquired = Event()

        class BlockingRegistryClient:
            def delete_manifest(self, repository: str, digest: str) -> None:
                self.deleted = (repository, digest)
                delete_started.set()
                if not finish_delete.wait(timeout=5):
                    raise TimeoutError("test did not release remote delete")

        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "usage.sqlite"
            store = RegistryUsageStore(path)
            records = [RegistryTag("repo/a", "v1", LEASE_DIGEST)]

            def acquire_reference():
                lease = RegistryUsageStore(path).acquire_reference(
                    "repo/a",
                    "v1",
                    "sandbox:new",
                    digest=LEASE_DIGEST,
                )
                reference_acquired.set()
                return lease

            client = BlockingRegistryClient()
            with ThreadPoolExecutor(max_workers=2) as executor:
                deletion = executor.submit(
                    execute_registry_prune,
                    client,  # type: ignore[arg-type]
                    records,
                    usage_store=store,
                    all_records=records,
                )
                self.assertTrue(delete_started.wait(timeout=5))
                acquisition = executor.submit(acquire_reference)
                self.assertFalse(reference_acquired.wait(timeout=0.1))
                finish_delete.set()

                self.assertEqual(deletion.result(timeout=5), records)
                self.assertEqual(acquisition.result(timeout=5).owner, "sandbox:new")

            self.assertEqual(client.deleted, ("repo/a", LEASE_DIGEST))

    def test_execute_registry_prune_revalidates_all_digest_aliases(self) -> None:
        client = DeletingRegistryClient()
        records = [
            RegistryTag("repo/a", "safe", "sha256:1"),
            RegistryTag("repo/a", "in-use", "sha256:1"),
            RegistryTag("repo/a", "old", "sha256:2"),
        ]
        deleted = execute_registry_prune(
            client,  # type: ignore[arg-type]
            records,
            revalidate=lambda record: record.tag != "in-use",
            all_records=records,
        )

        self.assertEqual(client.deleted, [("repo/a", "sha256:2")])
        self.assertEqual([record.tag for record in deleted], ["old"])

    def test_execute_registry_prune_requires_complete_digest_inventory(self) -> None:
        shared_digest = "sha256:" + "a" * 64
        records = [
            RegistryTag("repo/a", "old", shared_digest),
            RegistryTag("repo/a", "retained", shared_digest),
        ]
        for inventory in (records, []):
            with self.subTest(inventory=inventory):
                client = DeletingRegistryClient()
                deleted = execute_registry_prune(
                    client,  # type: ignore[arg-type]
                    [records[0]],
                    all_records=inventory,
                )
                self.assertEqual(deleted, [])
                self.assertEqual(client.deleted, [])

    def test_digest_lease_fences_all_aliases_in_plan_and_execution(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = RegistryUsageStore(Path(raw_dir) / "usage.json")
            shared_digest = "sha256:" + "6" * 64
            store.acquire_lease(
                "repo/a",
                "alias-v2",
                "sandbox:one",
                ttl_seconds=60,
                digest=shared_digest,
            )
            snapshot = store.snapshot()
            records = [
                RegistryTag("repo/a", "alias-v1", shared_digest),
                RegistryTag("repo/a", "alias-v2", shared_digest),
            ]

            planned = select_prune_candidates(
                records,
                keep_per_repository=0,
                active_leases=snapshot.leases,
            )
            client = DeletingRegistryClient()
            deleted = execute_registry_prune(
                client,  # type: ignore[arg-type]
                [records[0]],
                usage_store=store,
                all_records=records,
            )

            self.assertEqual(planned, [])
            self.assertEqual(deleted, [])
            self.assertEqual(client.deleted, [])

    def test_digest_lease_survives_tag_move_in_plan_and_execution(self) -> None:
        protected_digest = "sha256:" + "3" * 64
        moved_digest = "sha256:" + "4" * 64
        protection_tag = digest_protection_tag(protected_digest)
        with TemporaryDirectory() as raw_dir:
            store = RegistryUsageStore(Path(raw_dir) / "usage.json")
            store.acquire_reference(
                "repo/a",
                "v1",
                "sandbox:one",
                digest=protected_digest,
            )
            snapshot = store.snapshot()
            records = [
                RegistryTag("repo/a", "v1", moved_digest),
                RegistryTag("repo/a", protection_tag, protected_digest),
            ]

            planned = select_prune_candidates(
                records,
                keep_per_repository=0,
                active_leases=snapshot.leases,
            )
            client = DeletingRegistryClient()
            deleted = execute_registry_prune(
                client,  # type: ignore[arg-type]
                records,
                usage_store=store,
                all_records=records,
            )

        self.assertEqual(planned, [records[0]])
        self.assertEqual(deleted, [records[0]])
        self.assertEqual(client.deleted, [("repo/a", moved_digest)])

    def test_keep_floor_counts_distinct_digests_instead_of_alias_tags(self) -> None:
        records = [
            RegistryTag("repo/a", "new", "sha256:a", "2026-07-04T00:00:00+00:00"),
            RegistryTag(
                "repo/a",
                "new-alias",
                "sha256:a",
                "2026-07-03T00:00:00+00:00",
            ),
            RegistryTag("repo/a", "middle", "sha256:b", "2026-07-02T00:00:00+00:00"),
            RegistryTag("repo/a", "old", "sha256:c", "2026-07-01T00:00:00+00:00"),
        ]

        candidates = select_prune_candidates(records, keep_per_repository=2)

        self.assertEqual(candidates, [records[3]])


if __name__ == "__main__":
    unittest.main()
