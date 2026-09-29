import hashlib
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest

from scripts.qualify_build_cache_affinity import MARKER, affinity_key, classify_run, fixture, verify_tar


class CacheAffinityProofTests(unittest.TestCase):
    def test_source_and_argument_both_change_affinity_identity(self):
        dockerfile, original = fixture("example/base@sha256:" + "a" * 64, 0)
        _, changed = fixture("example/base@sha256:" + "a" * 64, 1)
        self.assertNotEqual(affinity_key(dockerfile, original), affinity_key(dockerfile, changed))
        self.assertNotEqual(affinity_key(dockerfile, original), affinity_key(dockerfile, original, "changed"))

    def test_execution_marker_does_not_match_run_header(self):
        cached = f"#1 [3/3] RUN printf '{MARKER}\\n'\n#1 CACHED\n"
        result = classify_run(cached)
        self.assertFalse(result["execution_marker_seen"])
        self.assertTrue(result["run_cached_marker_seen"])
        executed = f"#1 [3/3] RUN printf '{MARKER}\\n'\n#1 0.120 {MARKER}\n#1 DONE 0.2s\n"
        self.assertTrue(classify_run(executed)["execution_marker_seen"])
        self.assertNotIn("printf", json.dumps(result))

    def test_exported_proof_files_are_verified_without_extracting_rootfs(self):
        source = b"test fixture\n"
        files = {"proof-input.txt": source, "proof-arg.txt": b"original\n",
                 "proof-sha256.txt": (hashlib.sha256(source).hexdigest() + "  /proof-input.txt\n").encode()}
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "output.tar"
            with tarfile.open(path, "w") as archive:
                for name, data in files.items():
                    member = tarfile.TarInfo(name)
                    member.size = len(data)
                    archive.addfile(member, io.BytesIO(data))
                link = tarfile.TarInfo("irrelevant-rootfs-link")
                link.type = tarfile.SYMTYPE
                link.linkname = "/outside"
                archive.addfile(link)
            self.assertEqual(len(verify_tar(path, source, "original")), 3)
            self.assertFalse((Path(tmp) / "proof-input.txt").exists())
            with self.assertRaises(ValueError):
                verify_tar(path, source, "changed")


if __name__ == "__main__":
    unittest.main()
