import hashlib
import importlib.util
import io
import json
from pathlib import Path
import sqlite3
import unittest
import tempfile
from unittest.mock import patch
from urllib.error import HTTPError


spec = importlib.util.spec_from_file_location("prepare_image_pool", Path(__file__).parents[1] / "scripts/prepare_image_pool.py")
pool = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pool)
planner_spec = importlib.util.spec_from_file_location("plan_image_pool", Path(__file__).parents[1] / "scripts/plan_image_pool.py")
planner = importlib.util.module_from_spec(planner_spec)
planner_spec.loader.exec_module(planner)
qualification_spec = importlib.util.spec_from_file_location("qualify_image_pool", Path(__file__).parents[1] / "scripts/qualify_image_pool.py")
qualification = importlib.util.module_from_spec(qualification_spec)
qualification_spec.loader.exec_module(qualification)


class ImagePoolTests(unittest.TestCase):
    def test_source_resolution_shares_cooldown_without_hammering_registry(self):
        now = [1000.0]
        calls = []

        def resolve(source):
            calls.append(now[0])
            if len(calls) == 1:
                raise HTTPError("https://registry.invalid", 429, "rate limited", {"Retry-After": "120"}, None)
            return {"source": source}

        def sleep(seconds):
            now[0] += seconds

        with tempfile.TemporaryDirectory() as directory:
            resolver = pool.SourceResolver(Path(directory), resolve=resolve, clock=lambda: now[0], sleep=sleep)
            with patch("builtins.print"):
                self.assertEqual(resolver("ubuntu:22.04"), {"source": "ubuntu:22.04"})
            self.assertEqual(calls, [1000.0, 1120.0])
            # A new coordinator observes the same persisted host gate.
            other = pool.SourceResolver(Path(directory), resolve=resolve, clock=lambda: now[0], sleep=sleep)
            other("python:3.13")
            self.assertEqual(calls[-1], 1121.0)

    def test_source_resolution_defers_long_cooldown_and_preserves_nontransient_errors(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            limited = HTTPError("https://registry.invalid", 429, "rate limited", {"Retry-After": "7200"}, None)
            with patch.object(pool, "resolve_source", side_effect=limited) as resolve, patch("builtins.print"):
                resolver = pool.SourceResolver(root)
                with self.assertRaisesRegex(RuntimeError, "deferred: public registry cooldown"):
                    resolver("ubuntu:22.04")
                with self.assertRaisesRegex(RuntimeError, "deferred: public registry cooldown"):
                    resolver("python:3.13")
                self.assertEqual(resolve.call_count, 1)
            with patch.object(pool, "resolve_source", side_effect=HTTPError("url", 404, "missing", {}, None)):
                with self.assertRaises(HTTPError):
                    pool.SourceResolver(root)("ghcr.io/org/missing:latest")

    def test_retry_after_supports_http_dates_and_invalid_values(self):
        self.assertEqual(pool.retry_delay({"Retry-After": "Thu, 01 Jan 1970 00:02:00 GMT"}, 0, 0), 120)
        self.assertEqual(pool.retry_delay({"Retry-After": "invalid"}, 2, 0), 240)

    def test_result_journal_recovers_after_a_stale_catalog_snapshot(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            pool.save(root / "catalog.json", {"schema": 1, "initial_used_bytes": 123, "images": {}})
            pool.recover_catalog(root)
            result = {"source": "source", "status": "ready", "reference": "fixed"}
            pool.journal_result(root, result)
            recovered = pool.recover_catalog(root)
            self.assertEqual(recovered["initial_used_bytes"], 123)
            self.assertEqual(recovered["images"]["source"], result)
            pool.journal_result(root, {**result, "status": "failed"})
            self.assertEqual(pool.recover_catalog(root)["images"]["source"]["status"], "failed")

    def test_balanced_selection_does_not_starve_smaller_families(self):
        images = [{"source": str(n), "families": ["large"], "task_rows": 100} for n in range(100)]
        images += [{"source": "small-" + str(n), "families": ["small"], "task_rows": 1} for n in range(10)]
        selected = planner.select_images(images, 55, 0, balanced=True)
        self.assertEqual(sum("small" in x["families"] for x in selected), 5)
        self.assertEqual(len({x["source"] for x in selected}), 55)

    def test_successful_receipt_survives_missing_builder_history(self):
        published = {"id": "expected", "pushed": True, "manifest_digest": "sha256:" + "a" * 64}
        receipt = {"build": {"status": "succeeded", "image": published}}
        self.assertEqual(pool.receipt_image(receipt, "expected"), published)
        self.assertIsNone(pool.receipt_image(receipt, "different-preparation"))
        self.assertIsNone(pool.receipt_image({"build": {"status": "running", "image": published}}, "expected"))
        self.assertIsNone(pool.receipt_image({"build": {"status": "failed"}}, "expected"))
        self.assertEqual(pool.receipt_image({"published": published}, "expected"), published)

    def test_enriched_preparation_only_replaces_its_exact_recipe(self):
        reference = "registry/prepared@sha256:" + "a" * 64
        source = "docker.io/example/repo@sha256:" + "b" * 64
        mapping = {source: {"reference": reference, "image_id": "prepared", "preparation": "swesmith-v1"}}
        original = {"dockerfile": "FROM " + source + "\nRUN id\n"}
        self.assertEqual(planner.rewrite_bases(original, mapping), original)
        exact = {"dockerfile": pool.image_recipe(source, "swesmith-v1")}
        self.assertEqual(planner.complete_prepared_image(exact, mapping), "prepared")
        self.assertIn("FROM " + reference, planner.rewrite_bases(exact, mapping)["dockerfile"])
        changed = {"dockerfile": exact["dockerfile"] + "RUN echo task-specific\n"}
        self.assertEqual(planner.rewrite_bases(changed, mapping), changed)

    def test_catalog_pointer_recovery_requires_matching_validated_identity(self):
        key = "a" * 64
        item = {"status": "ready", "key": key, "image_id": "precomputed-" + key[:32],
                "reference": "registry/prepared@sha256:" + "b" * 64}
        self.assertEqual(pool.catalog_publication(item, key)["manifest_digest"], "sha256:" + "b" * 64)
        self.assertIsNone(pool.catalog_publication({**item, "status": "failed"}, key))
        self.assertIsNone(pool.catalog_publication(item, "c" * 64))
        with self.assertRaisesRegex(ValueError, "not pinned"):
            pool.catalog_publication({**item, "reference": "registry/prepared:latest"}, key)

    def test_index_audit_does_not_hide_cold_inputs_or_unverified_prepared_ids(self):
        reference = "registry/prepared@sha256:" + "a" * 64
        catalog = {"schema": 1, "images": {"base": {"status": "ready", "reference": reference, "image_id": "prepared"}}}
        recipes = [
            ("complete", "FROM anything\n", "prepared"),
            ("stale", "FROM anything\n", "unknown"),
            ("warm", "FROM " + reference + " AS build\nRUN touch /task\nFROM build\nCOPY --from=0 /task /task\n", None),
            ("cold", "FROM unprepared\n", None),
            ("hidden", "FROM " + reference + "\nCOPY --from=external /file /file\n", None),
            ("unknown", "FROM --platform=linux/amd64 " + reference + "\n", None),
        ]
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "images.sqlite"
            with sqlite3.connect(source) as conn:
                conn.execute("CREATE TABLE images(source TEXT,recipe TEXT,prepared_image TEXT)")
                conn.executemany("INSERT INTO images VALUES (?,?,?)",
                                 [(key, json.dumps({"dockerfile": text}), prepared) for key, text, prepared in recipes])
            before = source.read_bytes()
            result = planner.audit_index(source, [catalog])
            self.assertEqual(source.read_bytes(), before)
        self.assertEqual(result["prepared"], 1)
        self.assertEqual(result["unverified_prepared"], 1)
        self.assertEqual(result["live_builds"], 4)
        self.assertEqual(result["builds_with_prepared_bases"], 1)
        self.assertEqual(result["cold_or_unknown_builds"], 3)

    def test_index_shortcuts_only_complete_recipes_and_keeps_source_database(self):
        reference = "registry/prepared@sha256:" + "a" * 64
        catalog = {"schema": 1, "images": {"base": {"status": "ready", "reference": reference,
                   "source_reference": "docker.io/library/base@sha256:" + "b" * 64, "image_id": "prepared"}}}
        with tempfile.TemporaryDirectory() as directory:
            source, target = Path(directory) / "source.sqlite", Path(directory) / "target.sqlite"
            with sqlite3.connect(source) as conn:
                conn.execute("CREATE TABLE images(source TEXT PRIMARY KEY, recipe TEXT, prepared_image TEXT)")
                conn.execute("INSERT INTO images VALUES ('plain', ?, NULL)", (json.dumps({"dockerfile": "FROM base\n"}),))
                conn.execute("INSERT INTO images VALUES ('task', ?, NULL)", (json.dumps({"dockerfile": "FROM base\nRUN echo broken > /task\n"}),))
            before = source.read_bytes()
            self.assertEqual(planner.rewrite_index(source, target, [catalog]), {"rewritten": 2})
            self.assertEqual(source.read_bytes(), before)
            with sqlite3.connect(target) as conn:
                self.assertEqual(conn.execute("SELECT prepared_image FROM images WHERE source='plain'").fetchone()[0], "prepared")
                encoded, prepared = conn.execute("SELECT recipe,prepared_image FROM images WHERE source='task'").fetchone()
                self.assertIsNone(prepared)
                self.assertEqual(json.loads(encoded)["dockerfile"], "FROM " + reference + "\nRUN echo broken > /task\n")
            with self.assertRaises(ValueError):
                planner.rewrite_index(source, target, [catalog])

    def test_coverage_does_not_count_a_base_as_a_ready_task_image(self):
        inventory = {"images": [{"source": "base", "uses": [{"family": "terminal", "level": "base_only", "task_rows": 100}]},
                                {"source": "task", "uses": [{"family": "terminal", "level": "upstream_task_image", "task_rows": 1}]}]}
        catalogs = [{"schema": 1, "images": {"base": {"status": "ready", "reference": "base@digest",
                      "components": [{"digest": "shared", "bytes": 100}]}, "task": {"status": "failed"}}}]
        report = planner.coverage_report(inventory, catalogs)
        self.assertEqual(report["families"]["terminal"]["base_only_rows_ready"], 100)
        self.assertEqual(report["families"]["terminal"].get("upstream_image_rows_ready", 0), 0)
        self.assertEqual(report["unique_erofs_bytes"], 100)

    def test_selection_balances_families_before_fanout(self):
        images = [{"source": "popular", "families": ["swe"], "task_rows": 1000},
                  {"source": "second", "families": ["swe"], "task_rows": 500},
                  {"source": "eval", "families": ["eval"], "task_rows": 1}]
        self.assertEqual({x["source"] for x in planner.select_images(images, 2, 1)}, {"popular", "eval"})

    def test_family_floor_samples_distinct_repositories(self):
        images = [{"source": "same-1", "families": ["swe"], "repository": "large", "task_rows": 1},
                  {"source": "same-2", "families": ["swe"], "repository": "large", "task_rows": 1},
                  {"source": "unique", "families": ["swe"], "repository": "other", "task_rows": 1}]
        self.assertEqual([x["source"] for x in planner.select_images(images, 2, 2)], ["same-1", "unique"])

    def test_rewriter_preserves_stage_aliases_and_task_work(self):
        reference = "registry/prepared@sha256:" + "e" * 64
        mapping = {"ubuntu": {"reference": reference, "image_id": "prepared"}}
        recipe = {"dockerfile": "FROM ubuntu AS ubuntu\nRUN touch /task\nFROM ubuntu\nCOPY --from=ubuntu /task /task\n", "files": {"fixture": "broken"}}
        result = planner.rewrite_bases(recipe, mapping)
        self.assertEqual(result["dockerfile"], recipe["dockerfile"].replace("FROM ubuntu AS", "FROM " + reference + " AS", 1))
        self.assertEqual(result["files"], recipe["files"])
        self.assertIsNone(planner.complete_prepared_image(recipe, mapping))
        self.assertEqual(planner.complete_prepared_image({"dockerfile": "FROM ubuntu\n"}, mapping), "prepared")
        for text in ("FROM ubuntu AS ubuntu # comment\nFROM ubuntu\n", "# syntax=custom\nFROM ubuntu\n"):
            self.assertEqual(planner.rewrite_bases({"dockerfile": text}, mapping), {"dockerfile": text})

    def test_import_alias_is_immediately_resolvable_and_never_overwrites(self):
        from ucloud_sandboxes.images import ImageRecord, ImageStore
        from ucloud_sandboxes.image_import import import_image_id
        from ucloud_sandboxes.models import utc_now
        with tempfile.TemporaryDirectory() as directory:
            store = ImageStore(Path(directory) / "images.sqlite")
            record = ImageRecord(id="prepared", tag="registry/repo:tag", source="build:context", state="available",
                                 created_at=utc_now(), updated_at=utc_now(), pushed=True, manifest_digest="sha256:" + "a" * 64)
            self.assertEqual(pool.register_import_alias(store, record, "example/image:latest"), "registered")
            saved = store.get(import_image_id("example/image:latest"))
            self.assertEqual(saved.digest_ref, record.digest_ref)
            self.assertTrue(qualification.matching_alias(saved, record.tag + "@" + record.manifest_digest))
            self.assertFalse(qualification.matching_alias(saved, record.tag + "@sha256:" + "b" * 64))
            self.assertEqual(pool.register_import_alias(store, record, "example/image:latest"), "already-ready")
            changed = pool.replace(record, manifest_digest="sha256:" + "b" * 64)
            self.assertEqual(pool.register_import_alias(store, changed, "example/image:latest"), "preserved-existing")
            self.assertEqual(store.get(saved.id), saved)

    def test_only_public_registry_references_are_resolved(self):
        self.assertEqual(pool.source_parts("ubuntu:22.04"), ("library/ubuntu", "22.04"))
        self.assertEqual(pool.source_parts("docker.io/org/repo@sha256:" + "a" * 64), ("org/repo", "sha256:" + "a" * 64))
        for source in ("prime/primeintellect/task:1", "ghcr.io/org/repo:tag", "https://example.com/image", "x\nRUN false"):
            with self.subTest(source=source), self.assertRaises(ValueError):
                pool.source_parts(source)

    def test_budget_includes_other_inflight_images_and_survives_resume(self):
        limits = dict(growth_limit=100, free_floor=200, estimate=20)
        self.assertIsNone(pool.admission(300, 500, 300, 0, **limits))
        self.assertEqual(pool.admission(370, 500, 300, 20, **limits), "batch storage budget")
        self.assertEqual(pool.admission(300, 230, 300, 20, **limits), "free-space reserve")

    def test_mutable_tags_cannot_name_ready_artifacts(self):
        with self.assertRaises(ValueError):
            pool.image_identity("ubuntu:22.04")
        a = pool.image_identity("docker.io/library/ubuntu@sha256:" + "a" * 64)
        b = pool.image_identity("docker.io/library/ubuntu@sha256:" + "b" * 64)
        self.assertNotEqual(a, b)
        self.assertNotEqual(a, pool.image_identity("docker.io/library/ubuntu@sha256:" + "a" * 64, "swesmith-v1"))
        with self.assertRaises(ValueError):
            pool.image_recipe("docker.io/library/ubuntu@sha256:" + "a" * 64, "unknown")

    def test_registry_resolution_verifies_platform_and_content(self):
        config = json.dumps({"os": "linux", "architecture": "amd64", "rootfs": {"diff_ids": ["sha256:" + "c" * 64]}}).encode()
        config_digest = "sha256:" + hashlib.sha256(config).hexdigest()
        manifest = json.dumps({"config": {"digest": config_digest}, "layers": [{"digest": "sha256:" + "d" * 64, "size": 123}]}).encode()
        digest = "sha256:" + hashlib.sha256(manifest).hexdigest()

        def response(body):
            stream = io.BytesIO(body)
            stream.headers = {}
            return stream

        with patch.object(pool.request, "urlopen", side_effect=[response(b'{"token":"test-token"}'), response(manifest), response(config)]):
            resolved = pool.resolve_source("org/repo:latest")
        self.assertEqual(resolved["reference"], "docker.io/org/repo@" + digest)
        self.assertEqual(resolved["compressed_bytes"], 123)
        with patch.object(pool.request, "urlopen", side_effect=[response(b'{"token":"test-token"}'), response(manifest), response(config + b" ")]):
            with self.assertRaisesRegex(ValueError, "digest mismatch"):
                pool.resolve_source("org/repo:latest")


if __name__ == "__main__":
    unittest.main()
