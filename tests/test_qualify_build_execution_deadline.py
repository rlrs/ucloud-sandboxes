from argparse import Namespace
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from scripts.qualify_build_execution_deadline import marked_processes, validate_args


class ExecutionDeadlineCanaryTests(unittest.TestCase):
    def arguments(self, **changes):
        return Namespace(**{
            "registry_url": "http://10.42.0.2:5000",
            "registry_authority": "10.42.0.2:5000", "builder": "ucloud-shared-cache",
            "owned_builder_id": "123", "base_image": "python@sha256:" + "a" * 64,
            "execution_timeout_seconds": 5, "normal_timeout_seconds": 120,
            **changes,
        })

    def test_canary_rejects_unpinned_or_mismatched_destination_and_unbounded_runtime(self):
        validate_args(self.arguments())
        for change in ({"registry_authority": "other:5000"},
                       {"registry_url": "http://user:secret@10.42.0.2:5000"},
                       {"base_image": "python:latest"},
                       {"execution_timeout_seconds": 60},
                       {"normal_timeout_seconds": 0},
                       {"execution_timeout_seconds": float("nan")},
                       {"builder": ""}, {"owned_builder_id": ""}):
            with self.subTest(change=list(change)), self.assertRaises(ValueError):
                validate_args(self.arguments(**change))

    def test_process_witness_matches_only_an_exact_environment_field(self):
        with TemporaryDirectory() as raw:
            root = Path(raw)
            for pid, env in {
                100: b"UNRELATED=value\0UCLOUD_BUILD_DEADLINE_WITNESS=owned\0",
                101: b"UCLOUD_BUILD_DEADLINE_WITNESS=owned-other\0",
                102: b"PREFIX_UCLOUD_BUILD_DEADLINE_WITNESS=owned\0",
                103: b"OTHER=UCLOUD_BUILD_DEADLINE_WITNESS=owned\0",
            }.items():
                process = root / str(pid)
                process.mkdir()
                (process / "environ").write_bytes(env)
            (root / "104").mkdir()  # Process vanished during the snapshot.
            self.assertEqual(marked_processes("owned", proc_root=root),
                             {"pids": [100], "unreadable_environments": 0})

    def test_oversized_environment_prevents_an_absence_proof(self):
        with TemporaryDirectory() as raw:
            process = Path(raw) / "100"
            process.mkdir()
            (process / "environ").write_bytes(b"x" * (1024 * 1024 + 1))
            self.assertEqual(marked_processes("owned", proc_root=Path(raw)),
                             {"pids": [], "unreadable_environments": 1})


if __name__ == "__main__":
    unittest.main()
