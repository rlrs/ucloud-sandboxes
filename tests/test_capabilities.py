import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from ucloud_sandboxes.capabilities import (
    DISK_QUOTA_CAPABILITY,
    RUNTIME_CONFORMANCE_CAPABILITY,
    TMPFS_QUOTA_PROBE,
    conformance_capabilities_from_file,
    conformance_results_from_file,
    merge_capabilities,
)


class CapabilityTests(unittest.TestCase):
    def test_derives_disk_quota_from_passing_storage_probe(self) -> None:
        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "runtime-conformance.json"
            path.write_text(
                json.dumps(
                    {
                        "ok": True,
                        "results": [
                            {
                                "name": "storage-opt-quota-enforced",
                                "ok": True,
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            capabilities = conformance_capabilities_from_file(path)

            self.assertIn(RUNTIME_CONFORMANCE_CAPABILITY, capabilities)
            self.assertIn(DISK_QUOTA_CAPABILITY, capabilities)

    def test_derives_probe_results_from_passing_conformance(self) -> None:
        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "runtime-conformance.json"
            path.write_text(
                json.dumps(
                    {
                        "ok": True,
                        "results": [
                            {"name": "storage-opt-quota-enforced", "ok": True},
                            {"name": TMPFS_QUOTA_PROBE, "ok": True},
                        ],
                    }
                ),
                encoding="utf-8",
            )

            results = conformance_results_from_file(path)

            self.assertTrue(results["storage-opt-quota-enforced"])
            self.assertTrue(results[TMPFS_QUOTA_PROBE])

    def test_failed_conformance_derives_no_capabilities(self) -> None:
        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "runtime-conformance.json"
            path.write_text(
                json.dumps(
                    {
                        "ok": False,
                        "results": [
                            {
                                "name": "storage-opt-quota-enforced",
                                "ok": True,
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            self.assertEqual(conformance_capabilities_from_file(path), ())

    def test_merge_capabilities_deduplicates(self) -> None:
        self.assertEqual(
            merge_capabilities(("sandbox", "disk-quota"), ("disk-quota", "snapshot")),
            ("sandbox", "disk-quota", "snapshot"),
        )


if __name__ == "__main__":
    unittest.main()
