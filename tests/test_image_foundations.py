import unittest
import importlib.util
import json
from pathlib import Path
import sqlite3
import tempfile
from contextlib import ExitStack
from types import SimpleNamespace
from unittest.mock import Mock, patch
import sys

from ucloud_sandboxes.image_foundations import openswe_foundation, terminal_foundation, tmax_foundation, tmax_inline_foundation


BASE = "ubuntu:22.04@sha256:" + "a" * 64
READY = "registry:5000/base:latest@sha256:" + "b" * 64
PREFIX = ("FROM ubuntu:22.04\n\nENV LANG=C.UTF-8\n\n"
          "COPY base_install.sh /tmp/base_install.sh\n"
          "RUN bash /tmp/base_install.sh && rm /tmp/base_install.sh\n")


class ImageFoundationTests(unittest.TestCase):
    def test_terminal_prefix_reuses_dependencies_and_preserves_task_bytes(self):
        base = {"reference": BASE, "onbuild": []}
        prefix = "FROM ubuntu:22.04\nWORKDIR /app\n# Packages\nRUN apt-get update && apt-get install -y bash\n"
        tail = "\n# Task data\nCOPY task_file /app/task_file\nRUN rm /app/task_file/config\n"
        text = "# Task canary A\n" + prefix + tail
        foundation = terminal_foundation(text, source_base="ubuntu:22.04", resolved_base=base)
        other = terminal_foundation(text.replace("canary A", "canary B").replace("# Packages\n", ""),
                                    source_base="ubuntu:22.04", resolved_base=base)
        self.assertEqual(foundation.key, other.key)
        self.assertNotIn("task_file", foundation.dockerfile)
        self.assertEqual(foundation.task_dockerfile(text, READY), "# Task canary A\nFROM " + READY + "\n" + tail)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            context = root / foundation.image_id
            context.mkdir()
            (context / "Dockerfile").write_text(foundation.dockerfile)
            self.load_script("prepare_image_foundations").validate_context(root, {
                "key": foundation.key, "family": "terminal-prefix", "image_id": foundation.image_id})

    def test_terminal_prefix_rejects_context_triggers_and_unsupported_layouts(self):
        prefix = "FROM ubuntu:22.04\nRUN apt-get update\n"
        for base in ({"reference": BASE}, {"reference": BASE, "onbuild": ["COPY . /task"]}):
            with self.assertRaises(ValueError):
                terminal_foundation(prefix, source_base="ubuntu:22.04", resolved_base=base)
        base = {"reference": BASE, "onbuild": []}
        for text in ("# syntax=custom/frontend\n" + prefix, prefix + "FROM other\n",
                     prefix.replace("ubuntu:22.04", "ubuntu:22.04 AS build"), "ARG BASE\n" + prefix,
                     prefix + "COPY . /app\n", prefix + 'COPY ["Dockerfile", "/app/Dockerfile"]\n',
                     prefix + "COPY task_file/../Dockerfile /app/Dockerfile\n",
                     "FROM ubuntu:22.04\nRUN --mount=type=bind,target=/input true\n"):
            with self.assertRaises(ValueError):
                terminal_foundation(text, source_base="ubuntu:22.04", resolved_base=base)
        tail = "ARG VERSION\nRUN --mount=type=bind,target=/input cat /input/file\n"
        foundation = terminal_foundation(prefix + tail, source_base="ubuntu:22.04", resolved_base=base)
        self.assertTrue(foundation.task_dockerfile(prefix + tail, READY).endswith(tail))

    def test_terminal_rewrite_keeps_context_overrides_and_unknown_families(self):
        planner = self.load_script("plan_image_foundations")
        base = {"reference": BASE, "onbuild": []}
        text = "FROM ubuntu:22.04\nRUN apt-get update\nCOPY task_file /app/\n"
        foundation = terminal_foundation(text, source_base="ubuntu:22.04", resolved_base=base)
        catalog = {"schema": 1, "foundations": {foundation.key: {"family": "terminal-prefix", "key": foundation.key,
                   "source_base": "ubuntu:22.04", "resolved_base": base, "reference": READY, "validated": True}}}
        with tempfile.TemporaryDirectory() as directory:
            source, output = Path(directory) / "source.sqlite", Path(directory) / "output.sqlite"
            recipe = {"dockerfile": text, "context_dir": "/task/context", "files": {"task_file/input": "unchanged"}}
            with sqlite3.connect(source) as db:
                db.execute("CREATE TABLE images(source TEXT, family TEXT, recipe TEXT, prepared_image TEXT)")
                for family in ("terminal-lego", "other"):
                    db.execute("INSERT INTO images VALUES(?,?,?,?)", (family, family, json.dumps(recipe), "old"))
            self.assertEqual(planner.rewrite_index(source, output, catalog)["rewritten"], 1)
            with sqlite3.connect(output) as db:
                encoded, prepared = db.execute("SELECT recipe,prepared_image FROM images WHERE family='terminal-lego'").fetchone()
                result = json.loads(encoded)
                self.assertEqual(result["context_dir"], recipe["context_dir"])
                self.assertEqual(result["files"], recipe["files"])
                self.assertEqual(result["dockerfile"], "FROM " + READY + "\nCOPY task_file /app/\n")
                self.assertIsNone(prepared)
                self.assertEqual(db.execute("SELECT prepared_image FROM images WHERE family='other'").fetchone()[0], "old")

    def test_preparer_isolates_failed_build_and_resumes_accepted_work(self):
        preparer = self.load_script("prepare_image_foundations")
        pool = self.load_script("prepare_image_pool")
        from ucloud_sandboxes.images import ImageRecord
        from ucloud_sandboxes.models import utc_now
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            foundations = [tmax_foundation(PREFIX, text, ubuntu_base=BASE) for text in (b"bad", b"good")]
            items = []
            for foundation in foundations:
                context = root / foundation.image_id
                context.mkdir()
                (context / "Dockerfile").write_text(foundation.dockerfile)
                (context / "base_install.sh").write_bytes(foundation.installer)
                items.append({"key": foundation.key, "image_id": foundation.image_id, "tasks": 1})
            (root / "plan.json").write_text(json.dumps({"schema": 1, "foundations": items}))
            (root / "token").write_text("test")
            (root / "config.json").write_text("{}")
            config = SimpleNamespace(control_state_file=lambda: root / "control",
                sandbox_api_token_file=lambda: root / "token", registry_usage_file=lambda: root / "usage",
                image_file=lambda: root / "images")
            client = Mock()
            client.list_images.return_value = []
            client.submit_image_build.side_effect = [{"build_id": "bad"}, {"build_id": "good"}]
            now = utc_now()
            image = ImageRecord(id=foundations[1].image_id, tag="registry/good:latest", source="build",
                state="available", created_at=now, updated_at=now, pushed=True,
                manifest_digest="sha256:" + "b" * 64).to_dict()
            client.wait_for_image_build.side_effect = lambda identity, **kwargs: (
                {"status": "failed", "error": "upstream package missing"} if identity == "bad"
                else {"status": "succeeded", "image": image})
            client.exec.return_value = SimpleNamespace(exit_code=0, stdout='{"cpu_only":true}', stderr="")
            sdk = SimpleNamespace(SandboxClient=Mock(return_value=client), Image=Mock(), SandboxSpec=Mock())
            store = Mock()
            store.get.return_value = None
            with ExitStack() as stack:
                stack.enter_context(patch.dict(sys.modules, {"ucloud_sandboxes_sdk": sdk, "prepare_image_pool": pool}))
                stack.enter_context(patch.object(sys, "path", list(sys.path)))
                stack.enter_context(patch.object(sys, "argv", ["prepare", "--root", str(root), "--sdk-wheel", "unused",
                    "--config", str(root / "config.json"), "--gateway", "https://example.invalid", "--limit", "2"]))
                replacements = {
                    "ucloud_sandboxes.config.DeploymentConfig.from_dict": config,
                    "ucloud_sandboxes.environment_config.environment_registry_from_deployment": Mock(),
                    "ucloud_sandboxes.managed_registry.RegistryUsageStore": Mock(),
                    "ucloud_sandboxes.environment_dependencies.EnvironmentDependencyResolver": Mock(),
                    "ucloud_sandboxes.images.ImageStore": store,
                    "ucloud_sandboxes.control_plane._persist_registry_image_protection": True,
                    "ucloud_sandboxes.environment_artifact.load_image_environment": ("root", SimpleNamespace(components=[])),
                    "ucloud_sandboxes.registry_disk.registry_disk_usage": SimpleNamespace(used_bytes=0, available_bytes=1024**4),
                    "ucloud_sandboxes.host_locks.HOST_LOCKS.configure": None,
                }
                for target, value in replacements.items():
                    stack.enter_context(patch(target, return_value=value))
                stack.enter_context(patch("builtins.print"))
                for _ in range(2):
                    with self.assertRaises(SystemExit) as exit_status:
                        preparer.main()
                    self.assertEqual(exit_status.exception.code, 1)
                    catalog = json.loads((root / "catalog.json").read_text())
                    self.assertEqual(set(catalog["foundations"]), {foundations[1].key})
                    self.assertEqual(catalog["failures"][foundations[0].key]["status"], "failed")
                self.assertEqual(client.submit_image_build.call_count, 2)
                self.assertEqual(client.create_sandbox.call_count, 1)
                self.assertEqual(len(list((root / "results").glob("*.json"))), 2)

    def test_inline_index_override_preserves_context_and_original_script(self):
        planner = self.load_script("plan_image_foundations")
        docker = "FROM ubuntu:22.04\nENV DEBIAN_FRONTEND=noninteractive\nCOPY post_install.sh /tmp/post_install.sh\nRUN bash /tmp/post_install.sh && rm /tmp/post_install.sh\n"
        script = b"apt-get update && apt-get install -y python3 python3-pip\npip3 install pytest\necho broken > /task\n"
        foundation, remaining = tmax_inline_foundation(docker, script, ubuntu_base=BASE)
        catalog = {"schema": 1, "foundations": {foundation.key: {"family": "tmax-inline", "key": foundation.key,
                   "base": BASE, "reference": READY, "validated": True}}}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "post_install.sh").write_bytes(script)
            source, target = root / "source.sqlite", root / "target.sqlite"
            recipe = {"dockerfile": docker, "context_dir": str(root), "files": {"fixture": "original"}}
            with sqlite3.connect(source) as conn:
                conn.execute("CREATE TABLE images(source TEXT PRIMARY KEY, family TEXT, recipe TEXT, prepared_image TEXT)")
                conn.execute("INSERT INTO images VALUES ('task','tmax',?,NULL)", (json.dumps(recipe),))
            self.assertEqual(planner.rewrite_index(source, target, catalog)["rewritten"], 1)
            self.assertEqual((root / "post_install.sh").read_bytes(), script)
            with sqlite3.connect(target) as conn:
                result = json.loads(conn.execute("SELECT recipe FROM images").fetchone()[0])
            self.assertEqual(result["files"], {"fixture": "original", "post_install.sh": remaining.decode()})
            self.assertEqual(result["context_dir"], str(root))
            self.assertIn("COPY post_install.sh", result["dockerfile"])

    def test_inline_dependencies_preserve_shell_options_and_task_bytes(self):
        docker = "FROM ubuntu:22.04\nENV DEBIAN_FRONTEND=noninteractive\n\nCOPY post_install.sh /tmp/post_install.sh\nRUN bash /tmp/post_install.sh && rm /tmp/post_install.sh\n"
        prefix = b"#!/bin/bash\nset -e\napt-get update && apt-get install -y python3 python3-pip\npip3 install pytest\n"
        tail = b"mkdir -p /app\nprintf broken > /app/task\nrm /etc/ssl/certs/ca-certificates.crt\n"
        foundation, remaining = tmax_inline_foundation(docker, prefix + tail, ubuntu_base=BASE)
        self.assertTrue(remaining.endswith(tail))
        self.assertTrue(remaining.startswith(b"#!/bin/bash\nset -e\n"))
        self.assertNotIn(b"pip3 install", remaining)
        other, _ = tmax_inline_foundation(docker, prefix + b"echo different task\n", ubuntu_base=BASE)
        self.assertEqual(foundation.key, other.key)
        changed, _ = tmax_inline_foundation(docker, prefix.replace(b"pytest", b"pytest==8.4.1") + tail, ubuntu_base=BASE)
        self.assertNotEqual(foundation.key, changed.key)
        _, remainder = tmax_inline_foundation(docker, prefix + b"set -eu\npip3 install newpackage\n", ubuntu_base=BASE)
        self.assertIn(b"set -eu\npip3 install newpackage", remainder)
        for script in (b"apt-get install -y $(cat packages)\n", prefix + b"echo $?\n"):
            with self.assertRaises(ValueError):
                tmax_inline_foundation(docker, script, ubuntu_base=BASE)
        _, remainder = tmax_inline_foundation(docker, prefix + b"pip3 install -r requirements.txt\n", ubuntu_base=BASE)
        self.assertIn(b"pip3 install -r requirements.txt", remainder)

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
