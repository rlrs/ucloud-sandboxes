from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import json
import multiprocessing
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
import unittest

from ucloud_sandboxes.build_history import BuildHistoryStore, terminal_build_summary


NOW = datetime(2026, 9, 29, tzinfo=timezone.utc).timestamp()


def build(index=0, *, updated=0, finished=None, **changes):
    stamp = datetime.fromtimestamp(NOW + updated, timezone.utc).isoformat()
    completed = datetime.fromtimestamp(NOW + (updated if finished is None else finished),
                                       timezone.utc).isoformat()
    return {"build_id": f"build-{index}", "image_id": f"image-{index % 2}",
            "status": "succeeded", "created_at": stamp, "updated_at": stamp,
            "finished_at": completed, "timings": {"total_ms": 10}, **changes}


def writer(path, worker):
    store = BuildHistoryStore(Path(path), clock=lambda: NOW)
    for index in range(10):
        store.record(build(index, updated=worker))


class BuildHistoryTests(unittest.TestCase):
    def setUp(self):
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.path = Path(self.temporary.name) / "build-history.sqlite"

    def store(self, **kwargs):
        return BuildHistoryStore(self.path, clock=lambda: NOW, **kwargs)

    def test_allowlist_excludes_logs_commands_errors_context_and_invalid_numbers(self):
        raw = build(log_tail="private output", command=["private command"],
                    context_path="private path", error="private error", tag="private registry",
                    location="https://private/?token=secret", queued_at="not-a-timestamp")
        raw["timings"] = {
            "total_ms": 123, "queue_wait_ms": 45, "preparation_ms": 6, "end_to_end_ms": 174,
            "secret": 123, "phases": {"docker_build_ms": 100, "cache_prepare_ms": 2,
                                      "cache_mount_ms": 4, "finishing_wait_ms": 12, "cleanup_ms": float("nan"),
                                      "private command": 42},
            "environment": {"groups_reused": 3, "selective_subprocess_ms": 120, "groups_built": True,
                            "erofs_bytes_built": 10**1000, "mkfs_ms": -1,
                            "squash_ms": float("inf"), "docker_pull_ms": {"nested": 10}},
        }
        self.assertTrue(self.store().record(raw))
        saved = self.store().get("build-0")
        self.assertNotIn("private", json.dumps(saved))
        self.assertNotIn("secret", json.dumps(saved))
        self.assertNotIn("queued_at", saved)
        self.assertEqual(saved["timings"], {
            "total_ms": 123, "queue_wait_ms": 45, "preparation_ms": 6, "end_to_end_ms": 174,
            "phases": {"docker_build_ms": 100, "cache_prepare_ms": 2, "cache_mount_ms": 4, "finishing_wait_ms": 12},
            "environment": {"groups_reused": 3, "selective_subprocess_ms": 120},
        })
        self.assertEqual(self.path.stat().st_mode & 0o777, 0o600)

    def test_nonterminal_and_invalid_identity_are_not_recorded(self):
        store = self.store()
        for raw in (build(status="running"), build(build_id=""), build(image_id="a/b"),
                    build(build_id="x" * 257), build(image_id=None), build(status=[]), None):
            self.assertFalse(store.record(raw))
        self.assertEqual(store.list_builds(), [])

    def test_publication_diagnostics_keep_only_known_numeric_reason_counters(self):
        raw = build(timings={"environment": {
            "selective_fallback_compressed_budget": 1,
            "selective_fallback_parent_context": 2,
            "selective_fallback_private_payload": 1,
            "oci_transfer_ms": 30, "oci_decompress_ms": 15, "oci_extract_ms": 20,
            "oci_download_bytes_actual": 10_000_000,
        }})
        saved = terminal_build_summary(raw)["timings"]["environment"]
        self.assertEqual(saved, {key: value for key, value in raw["timings"]["environment"].items()
                                 if key != "selective_fallback_private_payload"})

    def test_idempotent_replay_and_newer_cleanup_revision_survive_restart(self):
        store = self.store()
        original = build()
        self.assertTrue(store.record(original))
        self.assertFalse(store.record(original))
        complete = build(updated=1, finished=0, timings={"total_ms": 20, "phases": {"cleanup_ms": 10}})
        self.assertTrue(store.record(complete))
        self.assertFalse(store.record(original))
        fresh = self.store()
        self.assertEqual(fresh.list_builds(), [terminal_build_summary(complete)])
        with self.assertRaisesRegex(ValueError, "another image"):
            fresh.record(build(updated=2, image_id="different"))
        self.assertEqual(fresh.get("build-0"), terminal_build_summary(complete))

    def test_retention_count_bytes_and_age_do_not_refresh_on_replay(self):
        store = self.store(max_records=3, max_bytes=4096, max_age_days=1)
        for index in range(5):
            store.record(build(index, updated=index))
        self.assertEqual([item["build_id"] for item in store.list_builds()],
                         ["build-4", "build-3", "build-2"])
        self.assertFalse(store.record(build(9, updated=-86401)))
        self.assertIsNone(store.get("build-9"))
        small = self.store(max_records=100, max_bytes=1024)
        for index in range(10, 20):
            small.record(build(index, updated=index))
        with sqlite3.connect(self.path) as db:
            self.assertLessEqual(db.execute("SELECT sum(payload_bytes) FROM terminal_builds").fetchone()[0], 1024)
        future = BuildHistoryStore(self.path, max_age_days=1, clock=lambda: NOW + 86421)
        self.assertEqual(future.list_builds(), [])
        self.assertFalse(future.record(build(19, updated=19)))

    def test_age_expiry_hides_results_even_without_another_write(self):
        now = [NOW]
        store = BuildHistoryStore(self.path, max_age_days=1, clock=lambda: now[0])
        store.record(build())
        now[0] += 86401
        self.assertIsNone(store.get("build-0"))
        self.assertEqual(store.list_builds(), [])

    def test_filters_are_parameterized_and_results_are_detached(self):
        store = self.store()
        for index in range(6):
            store.record(build(index, updated=index, status="failed" if index % 2 else "succeeded"))
        result = store.list_builds(image_id="image-1", status="failed", limit=2)
        self.assertEqual([r["build_id"] for r in result], ["build-5", "build-3"])
        result[0]["timings"]["total_ms"] = 999
        self.assertEqual(store.get("build-5")["timings"]["total_ms"], 10)
        self.assertEqual(store.list_builds(image_id="x' OR 1=1--"), [])
        for kwargs in ({"limit": 0}, {"limit": 1001}, {"limit": True}, {"status": "running"}):
            with self.assertRaises(ValueError):
                store.list_builds(**kwargs)

    def test_cross_process_writers_preserve_latest_revision_without_duplicates(self):
        self.store()
        context = multiprocessing.get_context("spawn")
        processes = [context.Process(target=writer, args=(str(self.path), i)) for i in range(3)]
        for process in processes:
            process.start()
        for process in processes:
            process.join(15)
            self.assertEqual(process.exitcode, 0)
            process.close()
        records = self.store().list_builds()
        self.assertEqual(len(records), 10)
        self.assertTrue(all(r["updated_at"] == build(updated=2)["updated_at"] for r in records))

    def test_concurrent_initialization_and_first_capture_are_idempotent(self):
        with ThreadPoolExecutor(6) as pool:
            outcomes = list(pool.map(lambda _: self.store().record(build()), range(12)))
        self.assertEqual(sum(outcomes), 1)
        self.assertEqual(len(self.store().list_builds()), 1)

    def test_unrelated_database_and_symlink_are_rejected(self):
        with sqlite3.connect(self.path) as db:
            db.execute("CREATE TABLE unrelated(value TEXT)")
        with self.assertRaises(sqlite3.DatabaseError):
            self.store()
        link = self.path.parent / "link.sqlite"
        link.symlink_to(self.path)
        with self.assertRaises(ValueError):
            BuildHistoryStore(link)
