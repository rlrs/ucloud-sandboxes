import json
import unittest

from scripts.qualify_prime_tasksets import MANIFEST, verdict, runtime_preflight
from scripts.prime_validation_entrypoint import resolve_image


class PrimeQualificationTests(unittest.TestCase):
    def test_image_aliases_are_taskset_scoped_and_unambiguous(self):
        rules = json.loads(MANIFEST.with_name("prime-image-aliases.json").read_text())[
            "rules"
        ]
        image = "prime/primeintellect/elastic-synthetics:316-f52f0bf"
        self.assertEqual(
            resolve_image("swerebench-v2", image, rules),
            "docker.io/swerebenchv2/elastic-synthetics:316-f52f0bf",
        )
        self.assertEqual(resolve_image("tmax", image, rules), image)
        exact = [
            {
                "taskset": "tmax",
                "source": image,
                "target": "registry/task@sha256:example",
            }
        ]
        self.assertEqual(
            resolve_image("tmax", image, exact), "registry/task@sha256:example"
        )
        self.assertEqual(resolve_image("tmax", image + "extra", exact), image + "extra")
        with self.assertRaises(ValueError):
            resolve_image("swerebench-v2", image, rules + rules)

    def test_public_build_alias_requires_published_digest(self):
        from scripts.build_public_task_image import image_alias

        row = {"taskset": "senior-swe-bench", "source_image": "upstream/task:latest"}
        result = {
            "status": "succeeded",
            "image": {
                "state": "available",
                "available_to_sandboxes": True,
                "pushed": True,
                "tag": "registry:5000/task:latest",
                "manifest_digest": "sha256:" + "a" * 64,
            },
        }
        self.assertEqual(
            image_alias(row, result)["target"], "registry:5000/task@sha256:" + "a" * 64
        )
        for field, value in (
            ("pushed", False),
            ("available_to_sandboxes", False),
            ("manifest_digest", "sha256:invalid"),
        ):
            broken = {**result, "image": {**result["image"], field: value}}
            with self.assertRaises(ValueError):
                image_alias(row, broken)

    def test_known_network_gaps_block_before_dataset_setup(self):
        rows = json.loads(MANIFEST.read_text())["tasksets"]
        blocked = [
            row["taskset"]
            for row in rows
            if not runtime_preflight(row)["requirements_satisfied"]
        ]
        self.assertEqual(set(blocked), {"browsecomp-plus", "swebench-multilingual"})
        self.assertFalse(
            runtime_preflight({"required_runtime_features": ["unknown-feature"]})[
                "requirements_satisfied"
            ]
        )

    def test_manifest_covers_article_families(self):
        rows = json.loads(MANIFEST.read_text())["tasksets"]
        self.assertEqual(len({row["article_name"] for row in rows}), 23)
        self.assertEqual(
            {
                domain: sum(row["domain"] == domain for row in rows)
                for domain in ("swe", "terminal", "search")
            },
            {"swe": 11, "terminal": 4, "search": 8},
        )
        self.assertTrue(all(row["source_sha256"] for row in rows))

    def test_exit_success_and_unchecked_gold_are_not_qualification(self):
        summary = {
            "total": 1,
            "recorded": 1,
            "mode": "all",
            "outcomes": {"valid": 1},
            "checks": {"gold": {"unchecked": 1}, "setup": {"valid": 1}},
        }
        self.assertEqual(verdict(summary, mode="all"), "failed_or_incomplete")
        summary["checks"]["gold"] = {"valid": 1}
        self.assertEqual(verdict(summary, mode="all"), "sample_passed")
        summary["outcomes"]["missing"] = 1
        self.assertEqual(verdict(summary, mode="all"), "failed_or_incomplete")

    def test_search_setup_does_not_certify_grading_or_search_tools(self):
        summary = {"total": 1, "recorded": 1, "mode": "setup", "outcomes": {"valid": 1}}
        self.assertEqual(verdict(summary, mode="setup"), "setup_passed")
        self.assertEqual(verdict(summary, mode="all"), "failed_or_incomplete")
