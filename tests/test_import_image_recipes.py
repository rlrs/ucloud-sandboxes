"""scripts/import_image_recipes.py: exports from a git checkout, as training would see it."""
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "import_image_recipes.py"


def load():
    spec = importlib.util.spec_from_file_location("import_image_recipes", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module  # Its dataclasses look themselves up there.
    spec.loader.exec_module(module)
    return module


@unittest.skipIf(sys.version_info < (3, 11), "the importer reads task.toml with tomllib (Python 3.11+)")
class ImportImageRecipesTests(unittest.TestCase):
    def setUp(self):
        self.module = load()
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.repo = self.root / "prime-tasks"
        self.real = b"\x89PNG real bytes"
        pointer = (b"version https://git-lfs.github.com/spec/v1\noid sha256:"
                   + hashlib.sha256(self.real).hexdigest().encode() + b"\nsize " + str(len(self.real)).encode() + b"\n")
        files = {
            "datasets/tmax/README.md": b"not a task\n",
            "datasets/tmax/task_1/task.toml": b'[environment]\ndocker_image = "prime/tmax:task_1"\n',
            "datasets/tmax/task_1/environment/Dockerfile": b"FROM ubuntu:22.04\nCOPY _fixtures/a.png /a.png\n",
            "datasets/tmax/task_1/environment/_fixtures/a.png": pointer,
            "datasets/tmax/task_1/tests/test.sh": b"true\n",
            "datasets/tmax/task_2/task.toml": b'[verifier]\ntimeout_sec = 1\n',
            "datasets/tmax/task_2/environment/Dockerfile": b"FROM ubuntu:22.04\n",
        }
        for path, data in files.items():
            target = self.repo / path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)
        git = ["git", "-C", str(self.repo), "-c", "user.name=t", "-c", "user.email=t@t"]
        subprocess.run(["git", "init", "-q", str(self.repo)], check=True)
        subprocess.run([*git, "add", "-A"], check=True)
        subprocess.run([*git, "commit", "-qm", "tasks"], check=True)
        subprocess.run([*git, "remote", "add", "origin", "https://example.org/prime-tasks.git"], check=True)

    def lfs_server(self):
        real = self.real
        oid = hashlib.sha256(real).hexdigest()

        class Response(io.BytesIO):
            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

        def urlopen(req, timeout=None):
            if req.full_url.endswith("/info/lfs/objects/batch"):
                assert req.full_url == "https://example.org/prime-tasks.git/info/lfs/objects/batch"
                return Response(json.dumps({"objects": [{"oid": oid, "size": len(real), "actions": {
                    "download": {"href": "https://lfs.example.org/" + oid}}}]}).encode())
            return Response(real)
        return urlopen

    def test_tmax_export_names_from_task_toml_with_whole_trees_and_lfs_objects(self):
        out = self.root / "bundle"
        with patch.object(self.module.urllib.request, "urlopen", self.lfs_server()):
            self.module.main(["export", "tmax", "--dataset", str(self.repo), "--out", str(out),
                              "--lfs-cache", str(self.root / "lfs")])
        manifest = [json.loads(line) for line in (out / "manifest.jsonl").read_text().splitlines()]
        excluded = [json.loads(line) for line in (out / "excluded.jsonl").read_text().splitlines()]
        self.assertEqual([row["name"] for row in manifest], ["prime/tmax:task_1"])
        self.assertEqual(excluded, [{"task": "task_2", "name": None, "reason": "no [environment].docker_image"}])
        context = out / manifest[0]["context"]
        self.assertEqual((context / "_fixtures/a.png").read_bytes(), self.real)  # The object, not its pointer.
        self.assertIn(b"COPY _fixtures", (context / "Dockerfile").read_bytes())
        self.assertEqual(json.loads((out / "bundle.json").read_text())["counts"], {"exported": 1,
                                                                                  "no [environment].docker_image": 1})
        with self.assertRaises(FileExistsError):  # Never written into twice.
            self.module.main(["export", "tmax", "--dataset", str(self.repo), "--out", str(out),
                              "--lfs-cache", str(self.root / "lfs")])

    def test_an_lfs_object_that_fails_its_pointer_is_refused(self):
        bad = self.lfs_server()

        def tampered(req, timeout=None):
            response = bad(req, timeout)
            return response if req.full_url.endswith("/batch") else type(response)(b"other bytes, same size!")
        with patch.object(self.module.urllib.request, "urlopen", tampered), self.assertRaisesRegex(ValueError, "check"):
            self.module.main(["export", "tmax", "--dataset", str(self.repo), "--out", str(self.root / "b2"),
                              "--lfs-cache", str(self.root / "lfs2")])

    def test_context_rules(self):
        reason = self.module.context_reason
        self.assertIsNone(reason({"Dockerfile": (b"FROM x\n", "100644")}, set()))
        self.assertEqual(reason({"link": (b"target", "120000")}, set()), "context symlink")
        self.assertEqual(reason({".dockerignore": (b"*\n", "100644")}, set()), "context ignore file")
        self.assertEqual(reason({"big": (b"x" * (8 * 1024 ** 2 + 1), "100644")}, set()), "context too large")


if __name__ == "__main__":
    unittest.main()
