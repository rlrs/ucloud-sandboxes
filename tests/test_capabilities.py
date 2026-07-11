import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from ucloud_sandboxes.capabilities import (
    DISK_QUOTA_CAPABILITY,
    FORK_LOCAL_CAPABILITY,
    GVISOR_LIVE_FORK_PROBE,
    RUNTIME_CONFORMANCE_CAPABILITY,
    TMPFS_QUOTA_PROBE,
    conformance_capabilities_from_file,
    conformance_results_from_file,
    merge_capabilities,
)

RUNTIME_FINGERPRINT = "a" * 64


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

    def test_derives_local_fork_capability_from_executed_live_probe(self) -> None:
        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "runtime-conformance.json"
            path.write_text(
                json.dumps(
                    {
                        "ok": True,
                        "results": [
                            {"name": "storage-opt-quota-enforced", "ok": True},
                            {"name": TMPFS_QUOTA_PROBE, "ok": True},
                            {
                                "name": GVISOR_LIVE_FORK_PROBE,
                                "ok": True,
                                "skipped": False,
                                "required": False,
                                "runtime_fingerprint": RUNTIME_FINGERPRINT,
                            },
                        ],
                    }
                ),
                encoding="utf-8",
            )

            capabilities = conformance_capabilities_from_file(path)

            self.assertIn(RUNTIME_CONFORMANCE_CAPABILITY, capabilities)
            self.assertIn(FORK_LOCAL_CAPABILITY, capabilities)

    def test_live_fork_requires_both_writable_quota_probes(self) -> None:
        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "runtime-conformance.json"
            path.write_text(
                json.dumps(
                    {
                        "ok": True,
                        "results": [
                            {"name": "storage-opt-quota-enforced", "ok": True},
                            {
                                "name": GVISOR_LIVE_FORK_PROBE,
                                "ok": True,
                                "runtime_fingerprint": RUNTIME_FINGERPRINT,
                            },
                        ],
                    }
                ),
                encoding="utf-8",
            )

            capabilities = conformance_capabilities_from_file(path)

            self.assertNotIn(FORK_LOCAL_CAPABILITY, capabilities)

    def test_live_fork_fails_closed_on_runtime_fingerprint_change(self) -> None:
        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "runtime-conformance.json"
            path.write_text(
                json.dumps(
                    {
                        "ok": True,
                        "results": [
                            {"name": "storage-opt-quota-enforced", "ok": True},
                            {"name": TMPFS_QUOTA_PROBE, "ok": True},
                            {
                                "name": GVISOR_LIVE_FORK_PROBE,
                                "ok": True,
                                "runtime_fingerprint": RUNTIME_FINGERPRINT,
                            },
                        ],
                    }
                ),
                encoding="utf-8",
            )

            capabilities = conformance_capabilities_from_file(
                path,
                expected_fork_runtime_fingerprint="b" * 64,
            )

            self.assertNotIn(FORK_LOCAL_CAPABILITY, capabilities)
            self.assertFalse(
                conformance_results_from_file(
                    path,
                    expected_fork_runtime_fingerprint="b" * 64,
                )[GVISOR_LIVE_FORK_PROBE]
            )

    def test_skipped_live_probe_does_not_advertise_fork_capability(self) -> None:
        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "runtime-conformance.json"
            path.write_text(
                json.dumps(
                    {
                        "ok": True,
                        "results": [
                            {
                                "name": GVISOR_LIVE_FORK_PROBE,
                                "ok": True,
                                "skipped": True,
                                "required": False,
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            capabilities = conformance_capabilities_from_file(path)

            self.assertIn(RUNTIME_CONFORMANCE_CAPABILITY, capabilities)
            self.assertNotIn(FORK_LOCAL_CAPABILITY, capabilities)

    def test_merge_capabilities_deduplicates(self) -> None:
        self.assertEqual(
            merge_capabilities(("sandbox", "disk-quota"), ("disk-quota", "snapshot")),
            ("sandbox", "disk-quota", "snapshot"),
        )


if __name__ == "__main__":
    unittest.main()
