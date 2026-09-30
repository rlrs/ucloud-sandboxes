import unittest
import importlib.util
import json
from pathlib import Path
import sqlite3
import tempfile

from ucloud_sandboxes.image_foundations import openswe_foundation, tmax_foundation


BASE = "ubuntu:22.04@sha256:" + "a" * 64
READY = "registry:5000/base:latest@sha256:" + "b" * 64
PREFIX = ("FROM ubuntu:22.04\n\nENV LANG=C.UTF-8\n\n"
          "COPY base_install.sh /tmp/base_install.sh\n"
          "RUN bash /tmp/base_install.sh && rm /tmp/base_install.sh\n")


class ImageFoundationTests(unittest.TestCase):
    def load_script(self, name):
        spec = importlib.util.spec_from_file_location(name, Path(__file__).parents[1] / "scripts" / (name + ".py"))
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def test_preparer_rejects_changed_or_extra_context_files(self):
        preparer = self.load_script("prepare_image_foundations")
        foundation = tmax_foundation(PREFIX, b"install-v1", ubuntu_base=BASE)
        item = {"key": foundation.key, "image_id": foundation.image_id}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            context = root / foundation.image_id
            context.mkdir()
            (context / "Dockerfile").write_text(foundation.dockerfile)
            (context / "base_install.sh").write_bytes(foundation.installer)
            self.assertEqual(preparer.validate_context(root, item), context)
            (context / "base_install.sh").write_bytes(b"changed")
            with self.assertRaisesRegex(ValueError, "changed after planning"):
                preparer.validate_context(root, item)
            (context / "base_install.sh").write_bytes(foundation.installer)
            (context / "task-secret").write_text("must not enter a shared foundation")
            with self.assertRaisesRegex(ValueError, "only its dependency inputs"):
                preparer.validate_context(root, item)

    def test_openswe_index_rewrite_keeps_context_and_skips_other_prefixes(self):
        planner = self.load_script("plan_image_foundations")
        base = "continuumio/miniconda3:25.3.1-1@sha256:" + "c" * 64
        foundation = openswe_foundation("3.12", miniconda_base=base)
        tail = "COPY repo /testbed\nRUN pip install -e /testbed\n"
        recipe = {"dockerfile": "# Source task\n" + foundation.source_prefix + tail,
                  "source_repository": {"repo": "example/repo", "commit": "d" * 40}}
        catalog = {"schema": 1, "foundations": {foundation.key: {
            "family": "openswe", "python_version": "3.12", "key": foundation.key,
            "base": base, "reference": READY, "validated": True}}}
        with tempfile.TemporaryDirectory() as directory:
            source, target = Path(directory) / "source.sqlite", Path(directory) / "target.sqlite"
            with sqlite3.connect(source) as conn:
                conn.execute("CREATE TABLE images(source TEXT PRIMARY KEY, family TEXT, recipe TEXT, prepared_image TEXT)")
                conn.execute("INSERT INTO images VALUES ('task', 'openswe', ?, 'old')", (json.dumps(recipe),))
                conn.execute("INSERT INTO images VALUES ('custom', 'openswe', ?, 'keep')",
                             (json.dumps({"dockerfile": "# syntax=custom/frontend\n" + foundation.source_prefix}),))
                # An unrelated family must not need its original context available.
                conn.execute("INSERT INTO images VALUES ('tmax', 'tmax', ?, 'keep')",
                             (json.dumps({"dockerfile": PREFIX, "context_dir": "/nonexistent"}),))
            self.assertEqual(planner.rewrite_index(source, target, catalog)["rewritten"], 1)
            with sqlite3.connect(target) as conn:
                actual, prepared = conn.execute("SELECT recipe, prepared_image FROM images WHERE source='task'").fetchone()
                expected = {**recipe, "dockerfile": "# Source task\nFROM " + READY + "\n" + tail}
                self.assertEqual(json.loads(actual), expected)
                self.assertIsNone(prepared)
                self.assertEqual(conn.execute("SELECT count(*) FROM images WHERE prepared_image='keep'").fetchone()[0], 2)

    def test_openswe_preserves_source_dependent_installation_after_foundation(self):
        base = "continuumio/miniconda3:25.3.1-1@sha256:" + "c" * 64
        foundation = openswe_foundation("3.12", miniconda_base=base)
        tail = "RUN git clone https://example.org/repo /testbed\nRUN pip install -e /testbed\n"
        self.assertEqual(foundation.task_dockerfile(foundation.source_prefix + tail, READY),
                         "FROM " + READY + "\n" + tail)
        self.assertNotEqual(foundation.key, openswe_foundation("3.11", miniconda_base=base).key)
        commented = "# Base image specification\n\n" + foundation.source_prefix + tail
        self.assertEqual(foundation.task_dockerfile(commented, READY),
                         "# Base image specification\n\nFROM " + READY + "\n" + tail)
        self.assertIsNone(foundation.prefix_offset("ARG EXTRA=1\n" + foundation.source_prefix))
        self.assertIsNone(foundation.prefix_offset("# syntax=custom/frontend\n" + foundation.source_prefix))
        with self.assertRaises(ValueError):
            openswe_foundation("3.12; false", miniconda_base=base)

    def test_task_contents_do_not_invalidate_shared_dependencies(self):
        one = tmax_foundation(PREFIX + "COPY task-a /app/a\n", b"apt-get install -y gcc\n", ubuntu_base=BASE)
        two = tmax_foundation(PREFIX + "COPY task-b /app/b\n", one.installer, ubuntu_base=BASE)
        self.assertEqual(one.key, two.key)
        self.assertNotIn("task-a", one.dockerfile)
        self.assertEqual(one.task_dockerfile(PREFIX + "COPY task-a /app/a\n", READY),
                         "FROM " + READY + "\nCOPY task-a /app/a\n")

    def test_dependency_environment_and_base_changes_invalidate_foundation(self):
        initial = tmax_foundation(PREFIX, b"install-v1", ubuntu_base=BASE)
        variants = [tmax_foundation(PREFIX, b"install-v2", ubuntu_base=BASE),
                    tmax_foundation(PREFIX.replace("C.UTF-8", "C"), b"install-v1", ubuntu_base=BASE),
                    tmax_foundation(PREFIX, b"install-v1", ubuntu_base=BASE.replace("a" * 64, "c" * 64))]
        self.assertEqual(len({initial.key, *(v.key for v in variants)}), 4)

    def test_task_mutation_before_installer_is_rejected(self):
        with self.assertRaises(ValueError):
            tmax_foundation(PREFIX.replace("ENV LANG", "RUN rm /etc/ssl/cert.pem\nENV LANG"),
                            b"install", ubuntu_base=BASE)

    def test_mutable_base_and_mutable_result_are_rejected(self):
        with self.assertRaises(ValueError):
            tmax_foundation(PREFIX, b"install", ubuntu_base="ubuntu:22.04")
        foundation = tmax_foundation(PREFIX, b"install", ubuntu_base=BASE)
        with self.assertRaises(ValueError):
            foundation.task_dockerfile(PREFIX, "registry:5000/base:latest")

    def test_unknown_prefix_cannot_reuse_a_foundation(self):
        foundation = tmax_foundation(PREFIX, b"install", ubuntu_base=BASE)
        with self.assertRaises(ValueError):
            foundation.task_dockerfile(PREFIX.replace("C.UTF-8", "C"), READY)

    def test_index_rewrite_preserves_original_and_task_operations(self):
        spec = importlib.util.spec_from_file_location(
            "plan_foundations", Path(__file__).parents[1] / "scripts/plan_image_foundations.py")
        planner = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(planner)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "base_install.sh").write_bytes(b"install-v1")
            source, target = root / "source.sqlite", root / "factored.sqlite"
            suffix = "COPY broken-source /app/\nRUN rm /etc/ssl/cert.pem\n"
            recipe = {"context_dir": str(root), "dockerfile": PREFIX + suffix}
            encoded = json.dumps(recipe)
            with sqlite3.connect(source) as conn:
                conn.execute("CREATE TABLE images(source TEXT PRIMARY KEY, family TEXT, recipe TEXT, prepared_image TEXT)")
                conn.execute("INSERT INTO images VALUES ('task', 'tmax', ?, 'old-prepared')", (encoded,))
                conn.execute("INSERT INTO images VALUES ('other', 'openswe', ?, 'keep-prepared')", (encoded,))
                conn.execute("INSERT INTO images VALUES ('custom', 'tmax', ?, 'keep-prepared')",
                             (json.dumps({**recipe, "dockerfile": PREFIX.replace("ENV LANG", "RUN touch /task\nENV LANG") + suffix}),))
            foundation = tmax_foundation(recipe["dockerfile"], b"install-v1", ubuntu_base=BASE)
            catalog = {"schema": 1, "foundations": {foundation.key: {
                "key": foundation.key, "base": BASE, "reference": READY, "validated": True}}}
            self.assertEqual(planner.rewrite_index(source, target, catalog)["rewritten"], 1)
            with sqlite3.connect(source) as conn:
                self.assertEqual(conn.execute("SELECT recipe, prepared_image FROM images WHERE source='task'").fetchone(),
                                 (encoded, "old-prepared"))
            with sqlite3.connect(target) as conn:
                rewritten, prepared = conn.execute("SELECT recipe, prepared_image FROM images WHERE source='task'").fetchone()
                self.assertEqual(json.loads(rewritten)["dockerfile"], "FROM " + READY + "\n" + suffix)
                self.assertIsNone(prepared)
                self.assertEqual(conn.execute("SELECT prepared_image FROM images WHERE source='other'").fetchone()[0],
                                 "keep-prepared")
            with self.assertRaises(ValueError):
                planner.rewrite_index(source, target, catalog)
