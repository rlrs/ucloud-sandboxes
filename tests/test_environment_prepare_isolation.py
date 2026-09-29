"""Real isolated OCI preparation against a local immutable-blob HTTP fixture."""
import hashlib
import math
import os
from pathlib import Path
import stat
import tarfile
from tempfile import TemporaryDirectory
from threading import Event
import time
import unittest
from unittest.mock import patch

from tests.test_oci_layer_materialize import MemoryRegistry, directory, layer, member, sha256
from tests.test_registry_client_contract import _RegistryHTTPServer
from ucloud_sandboxes import environment_prepare
from ucloud_sandboxes.environment_builder import squash_layer_diffs
from ucloud_sandboxes.environment_prepare import PreparationError, prepare_in_subprocess
from ucloud_sandboxes.managed_registry import RegistryClient
from ucloud_sandboxes.oci_layer_materialize import materialize_layers


REPOSITORY = "owned/isolation-fixture"


def snapshot(root):
    rows, links = {}, {}
    for path in [root, *sorted(root.rglob("*"))]:
        info = path.lstat()
        name = str(path.relative_to(root))
        item = {"mode": info.st_mode, "uid": info.st_uid, "gid": info.st_gid,
                "mtime_ns": info.st_mtime_ns,
                "xattrs": {key: os.getxattr(path, key, follow_symlinks=False)
                           for key in sorted(os.listxattr(path, follow_symlinks=False))}}
        if stat.S_ISREG(info.st_mode):
            item["bytes"] = hashlib.sha256(path.read_bytes()).hexdigest()
            links.setdefault((info.st_dev, info.st_ino), []).append(name)
        elif stat.S_ISLNK(info.st_mode):
            item["link"] = os.readlink(path)
        rows[name] = item
    return rows, sorted(sorted(paths) for paths in links.values() if len(paths) > 1)


class PreparationSubprocessIsolationTests(unittest.TestCase):
    def setUp(self):
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.ordinal = 0

    def invoke(self, layers, group_counts, *, blobs=None, response_status=200):
        self.ordinal += 1
        scratch = self.root / f"scratch-{self.ordinal}"
        scratch.mkdir(mode=0o700)
        blobs = {value[0]["digest"]: value[2] for value in layers} if blobs is None else blobs
        urls = {f"/v2/{REPOSITORY}/blobs/{digest}": body for digest, body in blobs.items()}

        def responder(method, path, headers, body):
            self.assertEqual(method, "GET")
            self.assertEqual(body, b"")
            self.assertIn(path, urls)
            return response_status, {"Content-Type": "application/octet-stream"}, urls[path]

        with _RegistryHTTPServer(responder) as server:
            result = prepare_in_subprocess(RegistryClient(server.base_url, timeout_seconds=5),
                REPOSITORY, [value[0] for value in layers], [value[1] for value in layers],
                group_counts, scratch, timeout_seconds=15)
        return result, scratch, server.requests

    def test_actual_child_matches_inline_two_groups_and_preserves_links_and_metadata(self):
        layers = [
            layer([directory("."), directory("app", mode=0o750),
                   member("app/main", b"original"),
                   member("app/retained", kind=tarfile.LNKTYPE, linkname="app/main")]),
            layer([directory("."), directory("app", mode=0o751),
                   member("app/main", b"replacement", mode=0o755),
                   member("app/inert", kind=tarfile.SYMTYPE, mode=0o777,
                          linkname="/outside-never-followed")]),
            layer([directory(".", mode=0o750), directory("second", mode=0o711),
                   member("second/run", b"#!/bin/sh\nexit 0\n", mode=0o751),
                   member("second/alias", kind=tarfile.LNKTYPE, mode=0o751, linkname="second/run")]),
        ]
        result, scratch, requests = self.invoke(layers, [2, 1])
        self.assertFalse(result.fallback)
        self.assertEqual(tuple(result.views), (scratch / "view-0", scratch / "view-1"))
        self.assertEqual(len(requests), 3)
        self.assertEqual({path for _, path, _, _ in requests},
                         {f"/v2/{REPOSITORY}/blobs/{value[0]['digest']}" for value in layers})
        directories = materialize_layers(MemoryRegistry(layers), REPOSITORY,
            [value[0] for value in layers], [value[1] for value in layers], self.root / "inline")
        expected = [self.root / "expected-0", self.root / "expected-1"]
        squash_layer_diffs(directories[:2], expected[0])
        squash_layer_diffs(directories[2:], expected[1])
        self.assertEqual([snapshot(path) for path in result.views], [snapshot(path) for path in expected])
        self.assertEqual((result.views[0] / "app/retained").read_bytes(), b"original")
        self.assertEqual((result.views[0] / "app/main").read_bytes(), b"replacement")
        self.assertEqual(os.readlink(result.views[0] / "app/inert"), "/outside-never-followed")
        self.assertEqual((result.views[1] / "second/run").stat().st_ino,
                         (result.views[1] / "second/alias").stat().st_ino)
        self.assertEqual(set(result.metrics),
                         {"selective_materialization_ms", "squash_ms", "selective_subprocess_ms",
                          "oci_transfer_ms", "oci_decompress_ms", "oci_extract_ms",
                          "oci_download_bytes_actual"})
        self.assertEqual(result.metrics["oci_download_bytes_actual"], sum(value[0]["size"] for value in layers))
        self.assertTrue(all(math.isfinite(value) and value >= 0 for value in result.metrics.values()))

    def test_actual_child_returns_fallback_for_unsupported_layer_semantics(self):
        cases = [
            layer([directory("."), member(".wh.deleted")]),
            layer([directory("."), member("app/missing-parent", b"not independently materializable")]),
        ]
        for value in cases:
            with self.subTest(diff_id=value[1]):
                result, scratch, requests = self.invoke([value], [1])
                self.assertTrue(result.fallback)
                self.assertEqual(tuple(result.views), ())
                self.assertFalse((scratch / "view-0").exists())
                self.assertEqual(len(requests), 1)

    def test_actual_child_digest_corruption_is_hard_failure_not_fallback(self):
        value = layer([directory("."), member("payload", b"authenticated bytes")])
        with self.subTest(binding="compressed"), self.assertRaises(PreparationError):
            self.invoke([value], [1], blobs={value[0]["digest"]: b"x" * len(value[2])})
        with self.subTest(binding="diff_id"), self.assertRaises(PreparationError):
            self.invoke([(value[0], sha256(b"different uncompressed bytes"), value[2])], [1])

    def test_actual_child_registry_http_failure_does_not_become_semantic_fallback(self):
        value = layer([directory("."), member("payload", b"fixture")])
        with self.assertRaises(PreparationError) as caught:
            self.invoke([value], [1], response_status=404,
                        blobs={value[0]["digest"]: b"private fixture error response"})
        self.assertNotIn("private fixture error response", str(caught.exception))

    def test_actual_child_uses_selected_package_despite_shadow_package_in_cwd(self):
        shadow = self.root / "shadow"
        package = shadow / "ucloud_sandboxes"
        package.mkdir(parents=True)
        marker = self.root / "wrong-package-executed"
        (package / "__init__.py").write_text("")
        (package / "environment_prepare.py").write_text(
            f"from pathlib import Path\nPath({str(marker)!r}).write_bytes(b'wrong')\nraise SystemExit(2)\n")
        previous = Path.cwd()
        try:
            os.chdir(shadow)
            value = layer([directory("."), member("payload", b"selected candidate")])
            result, _, _ = self.invoke([value], [1])
        finally:
            os.chdir(previous)
        self.assertFalse(marker.exists())
        self.assertFalse(result.fallback)
        self.assertEqual((result.views[0] / "payload").read_bytes(), b"selected candidate")

    def test_actual_child_timeout_during_blob_read_returns_before_cleanup(self):
        requested, release = Event(), Event()
        value = layer([directory("."), member("payload", b"slow fixture")])
        scratch = self.root / "timeout"
        scratch.mkdir(mode=0o700)
        children = []
        original_popen = environment_prepare.subprocess.Popen

        def launch(*args, **kwargs):
            child = original_popen(*args, **kwargs)
            children.append(child)
            return child

        def responder(method, path, headers, body):
            requested.set()
            release.wait(5)
            return 200, {}, b""

        with _RegistryHTTPServer(responder) as server:
            # A deliberately killed reader may close before the response; that
            # expected fixture-side disconnect is not a benchmark exception.
            server.server.handle_error = lambda *_: None
            started = time.monotonic()
            try:
                with patch.object(environment_prepare.subprocess, "Popen", side_effect=launch), \
                     self.assertRaisesRegex(PreparationError, "timed out"):
                    prepare_in_subprocess(RegistryClient(server.base_url, timeout_seconds=5),
                        REPOSITORY, [value[0]], [value[1]], [1], scratch, timeout_seconds=1)
                self.assertTrue(requested.is_set(), "child must reach the blob read before timeout")
                self.assertLess(time.monotonic() - started, 3)
                self.assertFalse((scratch / "view-0").exists())
                self.assertEqual(len(children), 1)
                self.assertIsNotNone(children[0].returncode)
                with self.assertRaises(ChildProcessError):
                    os.waitpid(children[0].pid, os.WNOHANG)
            finally:
                release.set()


if __name__ == "__main__":
    unittest.main()
