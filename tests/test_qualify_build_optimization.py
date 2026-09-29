import hashlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from scripts.qualify_build_optimization import digest, verify_contexts


class FrozenContextTests(unittest.TestCase):
    def test_changed_bytes_and_expected_runtime_result_are_rejected(self):
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            context = root / "contexts" / "recipe" / "variant"
            context.mkdir(parents=True)
            payload = b"FROM frozen@sha256:example\n"
            dockerfile = context / "Dockerfile"
            dockerfile.write_bytes(payload)
            inventory_digest = hashlib.sha256(b"Dockerfile\0" + hashlib.sha256(payload).digest()).hexdigest()
            fixture = {"recipe": "recipe", "variant": "variant", "context_sha256": inventory_digest,
                       "context_files": 1, "context_bytes": len(payload), "smoke_expected_json": {"result": 7}}
            manifest = context / "fixture.json"
            manifest.write_text(json.dumps(fixture))
            inventory = root / "inventory.json"
            inventory.write_text(json.dumps([fixture]))

            def verify():
                return verify_contexts(root, inventory, digest(inventory), recipes=("recipe",), variants=("variant",))

            self.assertEqual(len(verify()), 1)
            dockerfile.write_bytes(payload + b"RUN true\n")
            with self.assertRaisesRegex(ValueError, "context bytes changed"):
                verify()
            dockerfile.write_bytes(payload)
            fixture["smoke_expected_json"]["result"] = 8
            manifest.write_text(json.dumps(fixture))
            with self.assertRaisesRegex(ValueError, "Fixture manifest changed"):
                verify()


if __name__ == "__main__":
    unittest.main()
