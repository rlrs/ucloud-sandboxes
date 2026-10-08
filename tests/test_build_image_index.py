"""scripts/build_image_index.py: how inventory rows become index names."""
import hashlib
import importlib.util
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "build_image_index.py"
spec = importlib.util.spec_from_file_location("build_image_index", SCRIPT)
index = importlib.util.module_from_spec(spec)
spec.loader.exec_module(index)
DIGEST = "sha256:" + "a" * 64


class BuildImageIndexTests(unittest.TestCase):
    def test_prepared_references_drop_the_host_and_tag(self):
        for reference, expected in (
                (f"10.42.0.2:5000/ucloud-managed/precomputed-abc:latest@{DIGEST}", f"ucloud-managed/precomputed-abc@{DIGEST}"),
                (f"10.42.0.2:5000/ucloud-managed/precomputed-abc@{DIGEST}", f"ucloud-managed/precomputed-abc@{DIGEST}"),
                (f"host/a/b:c@{DIGEST}", f"a/b@{DIGEST}")):
            self.assertEqual(index.prepared_reference(reference), expected)

    def test_bundle_recipes_hash_as_the_inventory_did(self):
        with TemporaryDirectory() as directory:
            context = Path(directory)
            (context / "Dockerfile").write_text("FROM x\n")
            (context / "verifier-bootstrap.sh").write_text("set -e\n")
            (context / "task_file").mkdir()
            self.assertEqual(index.bundle_recipe("tmax", context), {"dockerfile": "FROM x\n"})
            self.assertEqual(index.bundle_recipe("terminal-lego", context),
                             {"dockerfile": "FROM x\n", "files": {"verifier-bootstrap.sh": "set -e\n"},
                              "directories": ["task_file"]})
            (context / "task_file" / "input.txt").write_text("data")  # A task_file with content is the task's own.
            self.assertNotIn("directories", index.bundle_recipe("terminal-lego", context))
            # json.dumps(sort_keys=True), the inventory's own encoding.
            self.assertEqual(index.recipe_sha({"dockerfile": "FROM x\n"}),
                             hashlib.sha256(b'{"dockerfile": "FROM x\\n"}').hexdigest())


if __name__ == "__main__":
    unittest.main()
