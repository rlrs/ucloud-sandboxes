import hashlib
from pathlib import Path
import tempfile
import unittest

from scripts.analyze_buildkit_progress import parse_progress
from scripts.qualify_buildkit_cache_concurrency import application_observation, context_identity


class BuildkitConcurrencyDiagnosticTests(unittest.TestCase):
    def test_mixed_materialization_and_command_output_counts_execution(self):
        parsed = parse_progress("#1 [8/8] RUN npm run lint && npm run build && npm test\n"
                                "#1 extracting sha256:aaa 1.0s done\n"
                                "#1 1.5 > lint\n#1 DONE 3.0s\n")
        vertices, executed, cached = application_observation(parsed)
        self.assertEqual(vertices[0]["timing_category"], "mixed_execution_materialization")
        self.assertTrue(executed)
        self.assertFalse(cached)

    def test_missing_or_ambiguous_application_progress_fails_closed(self):
        for text in ("#1 exporting to image\n#1 DONE 1.0s\n",
                     "#1 [8/8] RUN npm run lint && npm run build && npm test\n#1 DONE 1.0s\n"):
            with self.assertRaises(ValueError):
                application_observation(parse_progress(text))
        _, executed, cached = application_observation(parse_progress(
            "#1 [8/8] RUN npm run lint && npm run build && npm test\n#1 CACHED\n"))
        self.assertFalse(executed)
        self.assertTrue(cached)

    def test_context_hash_matches_frozen_inventory_definition(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "src").mkdir()
            (root / "src/revision.ts").write_bytes(b"revision=5\n")
            (root / "fixture.json").write_text("ignored metadata")
            expected = hashlib.sha256(b"src/revision.ts\0" + hashlib.sha256(b"revision=5\n").digest()).hexdigest()
            self.assertEqual(context_identity(root), expected)
            (root / "unexpected-link").symlink_to("src/revision.ts")
            with self.assertRaises(ValueError):
                context_identity(root)


if __name__ == "__main__":
    unittest.main()
