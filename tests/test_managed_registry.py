from datetime import datetime, timezone
import unittest

from ucloud_sandboxes.managed_registry import (
    RegistryRequestError,
    RegistryTag,
    _next_link_path,
    execute_registry_prune,
    list_registry_tags,
    registry_summary,
    select_prune_candidates,
)


class ManagedRegistryTests(unittest.TestCase):
    def test_registry_summary_exposes_visible_tags_and_repository_metadata(self) -> None:
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

    def test_select_prune_candidates_uses_tag_order_when_created_at_missing(self) -> None:
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

    def test_select_prune_candidates_can_delete_old_single_tag_repositories(self) -> None:
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

    def test_next_link_path_extracts_registry_pagination_target(self) -> None:
        self.assertEqual(
            _next_link_path('</v2/_catalog?last=repo/a&n=1000>; rel="next"'),
            "/v2/_catalog?last=repo/a&n=1000",
        )


if __name__ == "__main__":
    unittest.main()
