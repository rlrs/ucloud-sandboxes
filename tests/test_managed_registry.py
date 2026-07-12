from datetime import datetime, timedelta, timezone
import json
import multiprocessing
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import call, patch

from ucloud_sandboxes.managed_registry import (
    MANIFEST_ACCEPT,
    canonical_image_digest_ref,
    digest_protection_tag,
    image_ref_with_manifest_digest,
    manifest_digest_from_image_ref,
    RegistryClient,
    RegistryRequestError,
    RegistryImageLeaseNotFound,
    RegistryUsageGenerationChanged,
    RegistryUsageStore,
    RegistryTag,
    _next_link_path,
    execute_registry_prune,
    list_registry_tags,
    registry_repository_tag_from_image_ref,
    registry_host_from_image_ref,
    registry_maintenance_lock,
    registry_prune_plan,
    registry_summary,
    select_prune_candidates,
)


def _acquire_lease_in_process(
    path: str,
    repository: str,
    tag: str,
    owner: str,
    result_queue: object,
) -> None:
    lease = RegistryUsageStore(Path(path)).acquire_lease(
        repository,
        tag,
        owner,
        ttl_seconds=60,
    )
    result_queue.put(lease.owner)  # type: ignore[attr-defined]


class ManagedRegistryTests(unittest.TestCase):
    def test_registry_client_reads_compressed_manifest_layers(self) -> None:
        manifest_digest = "sha256:" + "a" * 64
        first = "sha256:" + "1" * 64
        second = "sha256:" + "2" * 64
        client = RegistryClient("http://registry")
        with patch.object(
            client,
            "_json_request",
            return_value=(
                {
                    "schemaVersion": 2,
                    "layers": [
                        {"digest": first, "size": 100},
                        {"digest": second, "size": 250},
                    ],
                },
                {"Docker-Content-Digest": manifest_digest},
            ),
        ):
            result = client.manifest_layers("repo/a", manifest_digest)

        self.assertEqual(result.repository, "repo/a")
        self.assertEqual(result.manifest_digest, manifest_digest)
        self.assertEqual(
            [(layer.digest, layer.size) for layer in result.layers],
            [(first, 100), (second, 250)],
        )
        self.assertEqual(result.total_size, 350)

    def test_registry_client_selects_linux_amd64_manifest_from_index(self) -> None:
        index_digest = "sha256:" + "a" * 64
        amd64_digest = "sha256:" + "b" * 64
        arm64_digest = "sha256:" + "c" * 64
        layer_digest = "sha256:" + "d" * 64
        client = RegistryClient("http://registry")
        with patch.object(
            client,
            "_json_request",
            side_effect=(
                (
                    {
                        "schemaVersion": 2,
                        "manifests": [
                            {
                                "digest": arm64_digest,
                                "platform": {
                                    "os": "linux",
                                    "architecture": "arm64",
                                },
                            },
                            {
                                "digest": amd64_digest,
                                "platform": {
                                    "os": "linux",
                                    "architecture": "amd64",
                                },
                            },
                        ],
                    },
                    {"Docker-Content-Digest": index_digest},
                ),
                (
                    {
                        "schemaVersion": 2,
                        "layers": [{"digest": layer_digest, "size": 512}],
                    },
                    {"Docker-Content-Digest": amd64_digest},
                ),
            ),
        ) as request_manifest:
            result = client.manifest_layers("repo/a", index_digest)

        self.assertEqual(result.manifest_digest, amd64_digest)
        self.assertEqual(result.total_size, 512)
        self.assertEqual(
            request_manifest.call_args_list,
            [
                call(
                    f"/v2/repo/a/manifests/{index_digest}",
                    headers={"Accept": MANIFEST_ACCEPT},
                ),
                call(
                    f"/v2/repo/a/manifests/{amd64_digest}",
                    headers={"Accept": MANIFEST_ACCEPT},
                ),
            ],
        )

    def test_registry_client_creates_digest_protection_tag_from_exact_manifest(
        self,
    ) -> None:
        digest = "sha256:" + "1" * 64
        manifest = b'{"schemaVersion":2}'

        class FakeResponse:
            def __init__(
                self,
                body: bytes = b"",
                content_type: str = "application/json",
            ) -> None:
                self.body = body
                self.headers = {"Content-Type": content_type}

            def read(self, _limit: int = -1) -> bytes:
                return self.body

            def close(self) -> None:
                return None

        client = RegistryClient("http://registry")
        missing = RegistryRequestError(404, "HEAD", "/missing", "")
        with patch.object(
            client,
            "manifest_digest",
            side_effect=(missing, digest),
        ), patch.object(
            client,
            "_request",
            side_effect=(
                FakeResponse(
                    manifest,
                    "application/vnd.oci.image.manifest.v1+json",
                ),
                FakeResponse(),
            ),
        ) as request_manifest:
            tag = client.ensure_digest_protection_tag("repo/a", digest)

        expected_tag = digest_protection_tag(digest)
        self.assertEqual(tag, expected_tag)
        self.assertEqual(
            request_manifest.call_args_list,
            [
                call(
                    f"/v2/repo/a/manifests/{digest}",
                    headers={"Accept": MANIFEST_ACCEPT},
                ),
                call(
                    f"/v2/repo/a/manifests/{expected_tag}",
                    method="PUT",
                    headers={
                        "Content-Type": "application/vnd.oci.image.manifest.v1+json"
                    },
                    data=manifest,
                ),
            ],
        )

    def test_registry_usage_state_is_owner_only(self) -> None:
        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "registry-usage.json"
            RegistryUsageStore(path).touch_image("registry.test/models/demo:latest")

            self.assertEqual(path.stat().st_mode & 0o777, 0o600)

    def test_root_registry_writes_adopt_shared_state_owner(self) -> None:
        with TemporaryDirectory() as raw_dir:
            directory = Path(raw_dir)
            path = directory / "registry-usage.json"
            owner = directory.stat()
            with patch(
                "ucloud_sandboxes.managed_registry.os.geteuid",
                return_value=0,
            ), patch("ucloud_sandboxes.managed_registry.os.fchown") as fchown:
                RegistryUsageStore(path).touch_image("registry.test/models/demo:latest")

            self.assertGreaterEqual(fchown.call_count, 2)
            self.assertTrue(
                all(
                    item.args[1:] == (owner.st_uid, owner.st_gid)
                    for item in fchown.call_args_list
                )
            )

    def test_registry_catalog_rejects_repeated_pagination_link(self) -> None:
        client = RegistryClient("http://registry")
        repeated = '</v2/_catalog?n=1000>; rel="next"'
        with patch.object(
            client,
            "_json_request",
            return_value=({"repositories": ["repo/a"]}, {"Link": repeated}),
        ):
            with self.assertRaisesRegex(ValueError, "repeated pagination"):
                client.catalog()

    def test_registry_tags_follow_pagination_and_deduplicate(self) -> None:
        client = RegistryClient("http://registry")
        with patch.object(
            client,
            "_json_request",
            side_effect=(
                (
                    {"tags": ["v1", "v2"]},
                    {"Link": ('</v2/repo/a/tags/list?n=1000&last=v2>; rel="next"')},
                ),
                ({"tags": ["v2", "v3"]}, {}),
            ),
        ) as fetch:
            tags = client.tags("repo/a")

        self.assertEqual(tags, ["v1", "v2", "v3"])
        self.assertEqual(fetch.call_count, 2)

    def test_registry_summary_exposes_visible_tags_and_repository_metadata(
        self,
    ) -> None:
        class FakeRegistryClient:
            base_url = "http://registry"

            def catalog(self) -> list[str]:
                return ["repo/a", "plain"]

            def tags(self, repository: str) -> list[str]:
                return {
                    "plain": [],
                    "repo/a": ["v1", "v3", "v2"],
                }[repository]

        summary = registry_summary(
            FakeRegistryClient(),  # type: ignore[arg-type]
            max_tags_per_repository=2,
        )

        self.assertEqual(summary["repository_count"], 2)
        self.assertEqual(summary["scanned_tag_count"], 3)
        self.assertEqual(summary["visible_tag_count"], 2)
        repo = summary["repositories"][1]
        self.assertEqual(repo["repository"], "repo/a")
        self.assertEqual(repo["namespace"], "repo")
        self.assertEqual(repo["tag_count"], 3)
        self.assertEqual(repo["visible_tag_count"], 2)
        self.assertTrue(repo["tags_truncated"])
        self.assertEqual(repo["latest_tag"], "v3")
        self.assertEqual(repo["tags"], ["v2", "v3"])

    def test_registry_summary_tolerates_catalog_entry_with_missing_tags(self) -> None:
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

        summary = registry_summary(FakeRegistryClient())  # type: ignore[arg-type]

        self.assertTrue(summary["ok"])
        self.assertEqual(summary["repository_count"], 2)
        self.assertEqual(summary["scanned_repository_count"], 2)
        self.assertEqual(summary["scanned_tag_count"], 1)
        self.assertEqual(summary["unavailable_repository_count"], 1)
        self.assertEqual(summary["unavailable_repositories"], ["repo/missing"])
        missing = summary["repositories"][1]
        self.assertEqual(missing["repository"], "repo/missing")
        self.assertFalse(missing["available"])
        self.assertEqual(missing["tag_count"], 0)

    def test_list_registry_tags_skips_catalog_entries_with_missing_tags(self) -> None:
        class FakeRegistryClient:
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

        records = list_registry_tags(FakeRegistryClient())  # type: ignore[arg-type]

        self.assertEqual(
            [(record.repository, record.tag) for record in records],
            [("repo/a", "v1")],
        )

    def test_select_prune_candidates_keeps_newest_per_repository(self) -> None:
        records = [
            RegistryTag("repo/a", "v1", "sha256:1", "2026-06-01T00:00:00+00:00"),
            RegistryTag("repo/a", "v2", "sha256:2", "2026-06-02T00:00:00+00:00"),
            RegistryTag("repo/a", "v3", "sha256:3", "2026-06-03T00:00:00+00:00"),
            RegistryTag("repo/b", "v1", "sha256:4", "2026-06-01T00:00:00+00:00"),
            RegistryTag("repo/b", "v2", "sha256:5", "2026-06-02T00:00:00+00:00"),
        ]

        candidates = select_prune_candidates(records, keep_per_repository=2)

        self.assertEqual(
            [(item.repository, item.tag) for item in candidates],
            [("repo/a", "v1")],
        )

    def test_select_prune_candidates_uses_tag_order_when_created_at_missing(
        self,
    ) -> None:
        records = [
            RegistryTag("repo/a", "v1", "sha256:1"),
            RegistryTag("repo/a", "v3", "sha256:3"),
            RegistryTag("repo/a", "v2", "sha256:2"),
        ]

        candidates = select_prune_candidates(records, keep_per_repository=1)

        self.assertEqual(
            [(item.repository, item.tag) for item in candidates],
            [("repo/a", "v1"), ("repo/a", "v2")],
        )

    def test_select_prune_candidates_can_delete_old_single_tag_repositories(
        self,
    ) -> None:
        records = [
            RegistryTag("repo/old", "only", "sha256:1", "2026-06-01T00:00:00+00:00"),
            RegistryTag("repo/new", "only", "sha256:2", "2026-06-06T00:00:00+00:00"),
        ]

        candidates = select_prune_candidates(
            records,
            keep_per_repository=0,
            max_age_days=3,
            now=datetime(2026, 6, 7, tzinfo=timezone.utc),
        )

        self.assertEqual(
            [(item.repository, item.tag) for item in candidates],
            [("repo/old", "only")],
        )

    def test_select_prune_candidates_uses_last_usage_for_ttl(self) -> None:
        records = [
            RegistryTag(
                "repo/a",
                "old-but-used",
                "sha256:1",
                "2026-06-01T00:00:00+00:00",
                "2026-06-06T00:00:00+00:00",
            ),
            RegistryTag(
                "repo/a",
                "old-and-unused",
                "sha256:2",
                "2026-06-01T00:00:00+00:00",
                "2026-06-02T00:00:00+00:00",
            ),
        ]

        candidates = select_prune_candidates(
            records,
            keep_per_repository=0,
            max_age_days=3,
            use_last_used_at=True,
            now=datetime(2026, 6, 7, tzinfo=timezone.utc),
        )

        self.assertEqual(
            [(item.repository, item.tag) for item in candidates],
            [("repo/a", "old-and-unused")],
        )

    def test_select_prune_candidates_keeps_missing_usage_in_last_used_mode(
        self,
    ) -> None:
        records = [
            RegistryTag("repo/a", "old", "sha256:1", "2026-06-01T00:00:00+00:00")
        ]

        candidates = select_prune_candidates(
            records,
            keep_per_repository=0,
            max_age_days=3,
            use_last_used_at=True,
            now=datetime(2026, 6, 7, tzinfo=timezone.utc),
        )

        self.assertEqual(candidates, [])

    def test_select_prune_candidates_keeps_unknown_age_when_ttl_is_set(self) -> None:
        records = [
            RegistryTag("repo/a", "old", "sha256:1", "2026-06-01T00:00:00+00:00"),
            RegistryTag("repo/a", "unknown", "sha256:2"),
        ]

        candidates = select_prune_candidates(
            records,
            keep_per_repository=0,
            max_age_days=3,
            now=datetime(2026, 6, 7, tzinfo=timezone.utc),
        )

        self.assertEqual(
            [(item.repository, item.tag) for item in candidates],
            [("repo/a", "old")],
        )

    def test_registry_usage_store_touches_private_registry_image(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = RegistryUsageStore(Path(raw_dir) / "usage.json")

            record = store.touch_image(
                "ucloud-sandbox-registry:5000/prime-rl/tmax-mini-base:mswe-2.2.8-r5",
                when=datetime(2026, 6, 7, tzinfo=timezone.utc),
            )

            self.assertIsNotNone(record)
            self.assertEqual(record.repository, "prime-rl/tmax-mini-base")
            self.assertEqual(record.tag, "mswe-2.2.8-r5")
            loaded = store.load()
            self.assertIn(("prime-rl/tmax-mini-base", "mswe-2.2.8-r5"), loaded)

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

            self.assertEqual(store.snapshot().generation, 1)

    def test_registry_leases_acquire_renew_release_and_expire(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = RegistryUsageStore(Path(raw_dir) / "usage.json")
            started = datetime(2026, 7, 9, 10, 0, tzinfo=timezone.utc)

            acquired = store.acquire_lease(
                "repo/a",
                "v1",
                "sandbox:one",
                ttl_seconds=30,
                now=started,
            )
            self.assertEqual(store.snapshot(now=started).generation, 1)
            renewed = store.renew_lease(
                "repo/a",
                "v1",
                "sandbox:one",
                ttl_seconds=60,
                now=started + timedelta(seconds=10),
            )
            self.assertEqual(acquired.acquired_at, renewed.acquired_at)
            self.assertNotEqual(acquired.expires_at, renewed.expires_at)
            self.assertEqual(
                store.snapshot(now=started + timedelta(seconds=10)).generation,
                2,
            )
            self.assertTrue(
                store.release_lease(
                    "repo/a",
                    "v1",
                    "sandbox:one",
                    now=started + timedelta(seconds=11),
                )
            )
            self.assertEqual(
                store.snapshot(now=started + timedelta(seconds=11)).generation,
                3,
            )
            with self.assertRaises(RegistryImageLeaseNotFound):
                store.renew_lease(
                    "repo/a",
                    "v1",
                    "sandbox:one",
                    ttl_seconds=60,
                    now=started + timedelta(seconds=12),
                )

            store.acquire_lease(
                "repo/a",
                "v1",
                "sandbox:two",
                ttl_seconds=1,
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
                now=started,
            )
            loaded = RegistryUsageStore(store.path).snapshot(
                now=started + timedelta(days=3650)
            )

            self.assertEqual(reference.expires_at, "")
            self.assertIn(("repo/a", "v1", "sandbox:one"), loaded.leases)
            self.assertEqual(loaded.active_lease_tags(), {("repo/a", "v1")})

    def test_usage_updates_preserve_active_leases(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = RegistryUsageStore(Path(raw_dir) / "usage.json")
            store.acquire_lease(
                "repo/a",
                "v1",
                "sandbox:one",
                ttl_seconds=60,
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
                    )

    def test_old_usage_file_loads_without_lease_fields(self) -> None:
        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "usage.json"
            path.write_text(
                json.dumps(
                    {
                        "images": [
                            {
                                "image_ref": "registry/repo/a:v1",
                                "repository": "repo/a",
                                "tag": "v1",
                                "last_used_at": "2026-07-01T00:00:00+00:00",
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )

            snapshot = RegistryUsageStore(path).snapshot()

            self.assertEqual(snapshot.generation, 0)
            self.assertIn(("repo/a", "v1"), snapshot.records)
            self.assertEqual(snapshot.leases, {})

    def test_tag_only_lease_migrates_to_digest_protection_on_reacquire(self) -> None:
        digest = "sha256:" + "2" * 64
        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "usage.json"
            path.write_text(
                json.dumps(
                    {
                        "generation": 1,
                        "images": [],
                        "leases": [
                            {
                                "repository": "repo/a",
                                "tag": "v1",
                                "owner": "sandbox:one",
                                "acquired_at": "2026-07-10T00:00:00+00:00",
                                "renewed_at": "2026-07-10T00:00:00+00:00",
                                "expires_at": "",
                                "persistent": True,
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            store = RegistryUsageStore(path)
            legacy = store.snapshot()
            updated = store.acquire_reference(
                "repo/a",
                "v1",
                "sandbox:one",
                digest=digest,
            )
            migrated = store.snapshot()

        self.assertEqual(legacy.leases[("repo/a", "v1", "sandbox:one")].digest, "")
        self.assertEqual(updated.digest, digest)
        self.assertEqual(migrated.active_lease_digests(), {("repo/a", digest)})

    def test_malformed_lease_state_fails_closed(self) -> None:
        valid = {
            "repository": "repo/a",
            "tag": "v1",
            "owner": "sandbox:one",
            "acquired_at": "2026-07-10T00:00:00+00:00",
            "renewed_at": "2026-07-10T00:00:00+00:00",
            "expires_at": "2026-07-10T01:00:00+00:00",
        }
        cases = (
            {"generation": 1, "images": [], "leases": {}},
            {"generation": 1, "images": [], "leases": [None]},
            {"generation": 1, "images": [], "leases": [valid, valid]},
            {"generation": "broken", "images": [], "leases": []},
            {"generation": -1, "images": [], "leases": []},
        )
        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "usage.json"
            for payload in cases:
                with self.subTest(payload=payload):
                    path.write_text(json.dumps(payload), encoding="utf-8")
                    with self.assertRaises(ValueError):
                        RegistryUsageStore(path).snapshot()

    def test_usage_store_fsyncs_file_and_parent_directory(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = RegistryUsageStore(Path(raw_dir) / "usage.json")
            with patch("ucloud_sandboxes.managed_registry.os.fsync") as fsync:
                store.touch_image("localhost:5000/repo/a:v1")

            self.assertGreaterEqual(fsync.call_count, 2)

    def test_concurrent_process_lease_acquisition_loses_no_owner(self) -> None:
        with TemporaryDirectory() as raw_dir:
            path = str(Path(raw_dir) / "usage.json")
            context = multiprocessing.get_context("spawn")
            results = context.Queue()
            processes = [
                context.Process(
                    target=_acquire_lease_in_process,
                    args=(path, "repo/a", "v1", owner, results),
                )
                for owner in ("sandbox:one", "sandbox:two")
            ]

            for process in processes:
                process.start()
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
            records = [RegistryTag("repo/a", "v1", "sha256:1")]
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

    def test_registry_maintenance_lock_creates_cross_process_lock_file(self) -> None:
        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "registry-maintenance"

            with registry_maintenance_lock(path):
                self.assertTrue(path.with_name(path.name + ".lock").exists())

    def test_registry_repository_tag_from_image_ref(self) -> None:
        self.assertEqual(
            registry_repository_tag_from_image_ref(
                "ucloud-sandbox-registry:5000/prime-rl/tmax-mini-base:mswe-2.2.8-r5"
            ),
            ("prime-rl/tmax-mini-base", "mswe-2.2.8-r5"),
        )
        self.assertEqual(
            registry_repository_tag_from_image_ref("localhost:5000/repo/image"),
            ("repo/image", "latest"),
        )
        self.assertEqual(
            registry_host_from_image_ref(
                "ucloud-sandbox-registry:5000/prime-rl/tmax-mini-base:mswe-2.2.8-r5"
            ),
            "ucloud-sandbox-registry:5000",
        )
        self.assertEqual(registry_host_from_image_ref("ubuntu:latest"), "")

    def test_manifest_digest_helpers_preserve_tag_and_canonicalize_cache_key(
        self,
    ) -> None:
        digest = "sha256:" + "a" * 64
        tagged = "registry.example.org/team/image:v1"
        pinned = image_ref_with_manifest_digest(tagged, digest)

        self.assertEqual(pinned, f"{tagged}@{digest}")
        self.assertEqual(manifest_digest_from_image_ref(pinned), digest)
        self.assertEqual(
            canonical_image_digest_ref(pinned),
            f"registry.example.org/team/image@{digest}",
        )
        self.assertEqual(manifest_digest_from_image_ref(f"{tagged}@sha256:bad"), "")

    def test_execute_registry_prune_deletes_duplicate_digest_once(self) -> None:
        class FakeRegistryClient:
            def __init__(self) -> None:
                self.deleted: list[tuple[str, str]] = []

            def delete_manifest(self, repository: str, digest: str) -> None:
                self.deleted.append((repository, digest))

        client = FakeRegistryClient()

        execute_registry_prune(
            client,  # type: ignore[arg-type]
            [
                RegistryTag("repo/a", "v1", "sha256:1"),
                RegistryTag("repo/a", "v2", "sha256:1"),
                RegistryTag("repo/a", "v3", "sha256:2"),
            ],
        )

        self.assertEqual(
            client.deleted,
            [("repo/a", "sha256:1"), ("repo/a", "sha256:2")],
        )

    def test_execute_registry_prune_revalidates_all_digest_aliases(self) -> None:
        class FakeRegistryClient:
            def __init__(self) -> None:
                self.deleted: list[tuple[str, str]] = []

            def delete_manifest(self, repository: str, digest: str) -> None:
                self.deleted.append((repository, digest))

        client = FakeRegistryClient()
        deleted = execute_registry_prune(
            client,  # type: ignore[arg-type]
            [
                RegistryTag("repo/a", "safe", "sha256:1"),
                RegistryTag("repo/a", "in-use", "sha256:1"),
                RegistryTag("repo/a", "old", "sha256:2"),
            ],
            revalidate=lambda record: record.tag != "in-use",
        )

        self.assertEqual(client.deleted, [("repo/a", "sha256:2")])
        self.assertEqual([record.tag for record in deleted], ["old"])

    def test_alias_lease_fences_digest_in_plan_and_execution(self) -> None:
        class FakeRegistryClient:
            def __init__(self) -> None:
                self.deleted: list[tuple[str, str]] = []

            def delete_manifest(self, repository: str, digest: str) -> None:
                self.deleted.append((repository, digest))

        with TemporaryDirectory() as raw_dir:
            store = RegistryUsageStore(Path(raw_dir) / "usage.json")
            store.acquire_lease(
                "repo/a",
                "alias-v2",
                "sandbox:one",
                ttl_seconds=60,
            )
            snapshot = store.snapshot()
            records = [
                RegistryTag("repo/a", "alias-v1", "sha256:shared"),
                RegistryTag("repo/a", "alias-v2", "sha256:shared"),
            ]

            planned = select_prune_candidates(
                records,
                keep_per_repository=0,
                active_leases=snapshot.leases,
            )
            client = FakeRegistryClient()
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
        class FakeRegistryClient:
            def __init__(self) -> None:
                self.deleted: list[tuple[str, str]] = []

            def delete_manifest(self, repository: str, digest: str) -> None:
                self.deleted.append((repository, digest))

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
            client = FakeRegistryClient()
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

    def test_registry_summary_hides_internal_digest_protection_tags(self) -> None:
        protection_tag = digest_protection_tag("sha256:" + "5" * 64)

        class FakeRegistryClient:
            base_url = "http://registry"

            def catalog(self) -> list[str]:
                return ["repo/a"]

            def tags(self, repository: str) -> list[str]:
                del repository
                return ["v1", protection_tag]

        summary = registry_summary(FakeRegistryClient())  # type: ignore[arg-type]

        self.assertEqual(summary["scanned_tag_count"], 1)
        self.assertEqual(summary["internal_tag_count"], 1)
        self.assertEqual(summary["repositories"][0]["tags"], ["v1"])

    def test_prune_plan_reports_generation_and_excludes_active_lease(self) -> None:
        class FakeRegistryClient:
            def catalog(self) -> list[str]:
                return ["repo/a"]

            def tags(self, repository: str) -> list[str]:
                return ["v1"]

            def tag_record(self, repository: str, tag: str) -> RegistryTag:
                return RegistryTag(repository, tag, "sha256:1")

        with TemporaryDirectory() as raw_dir:
            store = RegistryUsageStore(Path(raw_dir) / "usage.json")
            store.acquire_lease(
                "repo/a",
                "v1",
                "sandbox:one",
                ttl_seconds=60,
            )
            snapshot = store.snapshot()

            plan = registry_prune_plan(
                FakeRegistryClient(),  # type: ignore[arg-type]
                keep_per_repository=0,
                active_leases=snapshot.leases,
                usage_generation=snapshot.generation,
            )

            self.assertEqual(plan["usage_generation"], snapshot.generation)
            self.assertEqual(plan["active_lease_count"], 1)
            self.assertEqual(plan["delete"], [])

    def test_next_link_path_extracts_registry_pagination_target(self) -> None:
        self.assertEqual(
            _next_link_path('</v2/_catalog?last=repo/a&n=1000>; rel="next"'),
            "/v2/_catalog?last=repo/a&n=1000",
        )


if __name__ == "__main__":
    unittest.main()
