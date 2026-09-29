import importlib.util
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
import unittest

from tests.test_image_build_admission import record
from ucloud_sandboxes.images import ImageBuildStore


SPEC = importlib.util.spec_from_file_location(
    "builder_admission_probe", Path(__file__).parents[1] / "scripts/sample_builder_admission.py",
)
probe = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(probe)


class AdmissionProbeTests(unittest.TestCase):
    def test_real_sqlite_counts_ownership_including_terminal_cleanup_without_writing(self):
        with TemporaryDirectory() as temporary:
            path = Path(temporary) / "images.sqlite"
            store = ImageBuildStore(path)
            for number in range(4):
                store.upsert(record(f"solve-{number}"))
            store.upsert(record("finish", phase="finishing"))
            store.upsert(record("cleanup", phase="finishing", status="succeeded"))
            store.upsert(record("old", phase="released", status="succeeded"))
            before = path.read_bytes()
            db = probe.connect(path)
            try:
                sample = probe.sample(db)
                self.assertEqual([sample[key] for key in (
                    "active_builds", "preparing_solving_builds", "finishing_builds", "terminal_cleanup_builds")],
                    [6, 4, 2, 1])
                self.assertEqual(sample["violations"], [])
                with self.assertRaises(sqlite3.OperationalError):
                    db.execute("DELETE FROM image_state_v1_builds")
                self.assertEqual(path.read_bytes(), before)
                store.upsert(record("extra"))
                self.assertEqual(set(probe.sample(db)["violations"]),
                                 {"active_builds", "preparing_solving_builds"})
            finally:
                db.close()

    def test_missing_or_legacy_unindexed_database_is_never_created_or_scanned(self):
        with TemporaryDirectory() as temporary:
            path = Path(temporary) / "absent.sqlite"
            with self.assertRaises(sqlite3.OperationalError):
                probe.connect(path)
            self.assertFalse(path.exists())
            store = ImageBuildStore(path)
            with store._transaction(write=True) as db:
                db.execute("DROP INDEX image_build_owned_phases")
            with self.assertRaisesRegex(ValueError, "index absent"):
                probe.connect(path)


if __name__ == "__main__":
    unittest.main()
