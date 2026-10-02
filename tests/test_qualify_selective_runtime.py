from copy import deepcopy
import gzip
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from scripts.qualify_selective_runtime import CHECK_PROGRAM, cleanup_owned, expected_filesystem, validate_receipt
from scripts.qualify_selective_semantics import fixture_layers

TEST_TIER = "contract"


class RuntimeSemanticTests(unittest.TestCase):
    def setUp(self):
        self.run_id = "0123456789abcdef"
        self.lower, self.cases = fixture_layers("ucloud-qual-" + self.run_id)

    def proof(self, case):
        return [{key: value for key, value in item.items() if key != "payload"}
                for item in (self.lower, self.cases[case])]

    def test_expected_semantics_keep_links_and_apply_actual_whiteouts(self):
        links = expected_filesystem(self.run_id, "links", self.proof("links"))
        self.assertEqual(links["hardlinks"], [["links/hard", "links/executable"]])
        self.assertEqual(links["execution_identity"], {"uid": 23123, "gid": 23124})
        self.assertEqual(links["entries"]["links/hard"]["mode"], 0o751)
        self.assertEqual(links["entries"]["links/absolute"]["link"], "/missing/qualification-target")
        opaque = expected_filesystem(self.run_id, "whiteout-opaque", self.proof("whiteout-opaque"))
        self.assertNotIn("opaque/old", opaque["entries"])
        self.assertNotIn("delete/gone", opaque["entries"])
        self.assertIn("opaque/new", opaque["entries"])
        self.assertIn("opaque/old", opaque["absent"])
        missing = expected_filesystem(self.run_id, "missing-parent", self.proof("missing-parent"))
        self.assertTrue(missing["entries"]["implicit"]["metadata_inferred"])
        self.assertEqual(missing["entries"]["implicit/child"]["uid"], 23123)
        compile(CHECK_PROGRAM, "guest-check", "exec")

    def test_receipt_header_tampering_is_rejected(self):
        proof = deepcopy(self.proof("links"))
        proof[1]["members"][0]["uid"] = 0
        with self.assertRaisesRegex(ValueError, "tar proof differs"):
            expected_filesystem(self.run_id, "links", proof)

    def test_guest_program_checks_real_files_links_and_detects_corruption(self):
        expected = expected_filesystem(self.run_id, "links", self.proof("links"))
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            # Materialize only this test's generated, reviewed link fixture;
            # this is independent of the runtime program that verifies it.
            for value in (self.lower, self.cases["links"]):
                with tarfile.open(fileobj=io.BytesIO(gzip.decompress(value["payload"]))) as archive:
                    for member in archive:
                        path = root / member.name
                        if member.isdir():
                            path.mkdir(exist_ok=True)
                        elif member.isreg():
                            path.write_bytes(archive.extractfile(member).read())
                        elif member.issym():
                            path.symlink_to(member.linkname)
                        elif member.islnk():
                            os.link(root / member.linkname, path)
            fixture = root / expected["prefix"]
            for relative, item in sorted(expected["entries"].items(), key=lambda pair: -pair[0].count("/")):
                path = fixture / relative
                item.update(uid=os.getuid(), gid=os.getgid())
                if item["kind"] != "symlink":
                    path.chmod(item["mode"])
                os.utime(path, ns=(0, 0), follow_symlinks=False)
            expected["prefix"] = str(fixture)
            expected["execution_identity"] = {"uid": os.geteuid(), "gid": os.getegid()}

            def check():
                return subprocess.run([sys.executable, "-c", CHECK_PROGRAM, json.dumps(expected)],
                                      capture_output=True, text=True, timeout=10)

            checked = check()
            self.assertEqual(checked.returncode, 0, checked.stderr)
            self.assertTrue(json.loads(checked.stdout)["verified"])
            executable = fixture / "links/executable"
            payload = executable.read_bytes()
            executable.write_bytes(b"!" + payload[1:])
            os.utime(executable, ns=(0, 0))
            corrupted = check()
            self.assertNotEqual(corrupted.returncode, 0)
            self.assertIn("content", corrupted.stderr)

    def test_incomplete_receipt_requires_explicit_successful_case(self):
        receipt = {"run_id": self.run_id, "source_repository": "ucloud-managed/owned-benchmark", "complete": False,
                   "cases": [{"case": "links", "tag": "qual-" + self.run_id + "-links",
                              "manifest": "sha256:" + "a" * 64, "equivalent": True},
                             {"case": "cross-layer-hardlink", "equivalent": False}]}
        with self.assertRaisesRegex(ValueError, "explicit --cases"):
            validate_receipt(receipt, None)
        self.assertEqual(len(validate_receipt(receipt, ["links"])), 1)
        with self.assertRaisesRegex(ValueError, "lacks a successful"):
            validate_receipt(receipt, ["cross-layer-hardlink"])

    def test_cleanup_refuses_foreign_id_and_fences_uncertain_creation(self):
        foreign = SimpleNamespace(get_sandbox=Mock(return_value={"spec": {"labels": {"qualification_owner": "foreign"}}}),
                                  delete_sandbox=Mock())
        self.assertFalse(cleanup_owned(foreign, "owned-id", "owner")["deleted"])
        foreign.delete_sandbox.assert_not_called()
        unknown = SimpleNamespace(get_sandbox=Mock(return_value=None), delete_sandbox=Mock())
        with patch("scripts.qualify_selective_runtime.time.sleep"):
            self.assertTrue(cleanup_owned(unknown, "owned-id", "owner")["deleted"])
        unknown.delete_sandbox.assert_called_once_with("owned-id")
        self.assertEqual(unknown.get_sandbox.call_count, 3)


if __name__ == "__main__":
    unittest.main()
