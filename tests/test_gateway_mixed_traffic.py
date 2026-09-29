import hashlib
import json
from pathlib import Path
import tempfile
import time
import unittest

from scripts.gateway_mixed_traffic import Budget, digest_file, registry_manifest, upload_location, write_blob


class GatewayMixedTrafficTests(unittest.TestCase):
    def test_budget_bounds_both_directions_and_expired_work(self):
        budget = Budget(100, time.monotonic() + 10)
        self.assertTrue(budget.reserve(60))
        self.assertFalse(budget.reserve(60))
        self.assertEqual(budget.reserved, 60)
        budget.stopped.set()
        self.assertFalse(budget.reserve(1))
        self.assertFalse(Budget(100, time.monotonic() - 1).reserve(1))

    def test_deterministic_fixture_is_unique_and_manifest_binds_exact_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            first, second = Path(directory) / "one", Path(directory) / "two"
            digest = write_blob(first, b"run:1", 12345)
            self.assertEqual(digest, write_blob(second, b"run:1", 12345))
            self.assertEqual(digest, digest_file(first))
            self.assertNotEqual(digest, write_blob(second, b"run:2", 12345))
            config, manifest = registry_manifest(digest, 12345, "run", 1)
            document = json.loads(manifest)
            self.assertEqual(document["layers"][0]["digest"], digest)
            self.assertEqual(document["layers"][0]["size"], 12345)
            self.assertEqual(document["config"]["digest"], "sha256:" + hashlib.sha256(config).hexdigest())

    def test_upload_location_restricts_origin_repository_and_preserves_state(self):
        registry, repo = "http://10.42.0.2:5000", "ucloud-diagnostics/test"
        path = "/v2/" + repo + "/blobs/uploads/123?_state=opaque"
        self.assertIn("_state=opaque&digest=sha256%3Aabc", upload_location(registry, repo, path, "sha256:abc"))
        for path in ("http://other/v2/" + repo + "/blobs/uploads/123", "/v2/customer/blobs/uploads/123", ""):
            with self.assertRaises(ValueError):
                upload_location(registry, repo, path)


if __name__ == "__main__":
    unittest.main()
