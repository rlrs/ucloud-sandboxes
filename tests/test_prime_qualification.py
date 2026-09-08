import asyncio
import json
from pathlib import Path
import sys
import tempfile
from types import ModuleType, SimpleNamespace
import unittest
from unittest import mock

from scripts.qualify_prime_tasksets import MANIFEST, command, verdict, runtime_preflight
from scripts.prime_validation_entrypoint import (
    main as validation_main,
    parse_arguments,
    resolve_image,
    resource_evidence,
)


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

    def test_malformed_evidence_fails_without_aborting_other_tasksets(self):
        complete = {
            "total": 1,
            "recorded": 1,
            "mode": "all",
            "outcomes": {"valid": 1},
            "checks": {"gold": {"valid": 1}, "setup": {"valid": 1}},
        }
        malformed = [None, [], "done", True]
        for field, value in (
            ("total", True),
            ("recorded", True),
            ("outcomes", None),
            ("outcomes", {"valid": True}),
            ("outcomes", {"valid": 1, "new_failure": 1}),
            ("outcomes", {"valid": 1, "error": -1}),
            ("checks", None),
            ("checks", {"gold": None, "setup": {"valid": 1}}),
            ("checks", {"gold": {"valid": 1, "invalid": 1}, "setup": {"valid": 1}}),
            ("terminal", 0),
            ("owed", 1),
        ):
            malformed.append({**complete, field: value})
        for summary in malformed:
            with self.subTest(summary=summary):
                self.assertEqual(verdict(summary, mode="all"), "failed_or_incomplete")
        self.assertEqual(verdict(complete, mode="all"), "sample_passed")

    def test_setup_check_cannot_contradict_overall_outcome(self):
        summary = {
            "total": 1,
            "recorded": 1,
            "mode": "setup",
            "outcomes": {"valid": 1},
            "checks": {"setup": {"valid": 1, "error": 1}},
        }
        self.assertEqual(verdict(summary, mode="setup"), "failed_or_incomplete")

    def test_smaller_sample_than_requested_cannot_pass(self):
        summary = {"total": 1, "recorded": 1, "mode": "setup", "outcomes": {"valid": 1}}
        self.assertEqual(
            verdict(summary, mode="setup", expected_total=2), "failed_or_incomplete"
        )
        self.assertEqual(
            verdict(summary, mode="setup", expected_total=1), "setup_passed"
        )

    def test_saved_qualification_verdicts_remain_unchanged(self):
        evidence = json.loads(
            (
                MANIFEST.parent / "reviews/sandbox-qualification-2026-09-05.json"
            ).read_text()
        )
        modes = {
            row["taskset"]: row["mode"]
            for row in json.loads(MANIFEST.read_text())["tasksets"]
        }
        for row in evidence["tasksets"]:
            if "summary" not in row:
                continue
            with self.subTest(taskset=row["taskset"]):
                self.assertEqual(
                    verdict(
                        row["summary"], mode=modes[row["taskset"]], expected_total=1
                    ),
                    row["qualification"],
                )

    def test_qualification_forwards_resource_overrides_and_always_records_evidence(
        self,
    ):
        for overrides in (None, {"cpu": 0.25, "memory": 2.0, "disk": 5.0}):
            with self.subTest(overrides=overrides):
                argv = command(
                    "/python",
                    {"taskset": "scaleswe", "mode": "all"},
                    output=Path("/evidence"),
                    num_tasks=1,
                    resource_overrides=overrides,
                )
                args, remaining = parse_arguments(argv[2:])
                self.assertEqual(args.aliases, "-")
                self.assertEqual(args.taskset, "scaleswe")
                self.assertEqual(
                    args.qualification_evidence,
                    Path("/evidence/scaleswe-runtimes.jsonl"),
                )
                self.assertEqual(remaining[:2], ["--runtime.type", "ucloud"])
                for key, value in (overrides or {}).items():
                    self.assertEqual(getattr(args, f"qualification_{key}"), value)
                    self.assertEqual(
                        remaining[remaining.index(f"--runtime.{key}") + 1], str(value)
                    )

    def test_task_resource_default_collision_is_recorded_before_provisioning(self):
        for expected_memory in (2, 4):
            with (
                self.subTest(expected_memory=expected_memory),
                tempfile.TemporaryDirectory() as directory,
            ):

                class FakeRuntime:
                    def __init__(self):
                        self.name = "sample-runtime"
                        self.config = SimpleNamespace(
                            image="registry/task:latest",
                            cpu=4,
                            memory=4,
                            disk=10,
                            env={"API_TOKEN": "must-not-appear"},
                        )
                        self.info = SimpleNamespace(image=self.config.image)
                        self.created = False

                    async def start(self):
                        self.created = True

                module = ModuleType("verifiers_ucloud.runtime")
                module.UCloudRuntime = FakeRuntime
                runtime = FakeRuntime()
                evidence_path = Path(directory) / "resources.jsonl"
                argv = [
                    "entrypoint",
                    "-",
                    "scaleswe",
                    "--qualification-memory",
                    str(expected_memory),
                    "--qualification-evidence",
                    str(evidence_path),
                    "--runtime.type",
                    "ucloud",
                ]
                with (
                    mock.patch.dict(
                        sys.modules,
                        {
                            "verifiers_ucloud": ModuleType("verifiers_ucloud"),
                            "verifiers_ucloud.runtime": module,
                        },
                    ),
                    mock.patch.object(sys, "argv", argv),
                    mock.patch(
                        "scripts.prime_validation_entrypoint.runpy.run_module",
                        side_effect=lambda *args, **kwargs: asyncio.run(
                            runtime.start()
                        ),
                    ),
                ):
                    if expected_memory == 2:
                        with self.assertRaisesRegex(
                            ValueError, "task-resolved resources"
                        ):
                            validation_main()
                    else:
                        validation_main()
                self.assertEqual(runtime.created, expected_memory == 4)
                evidence = json.loads(evidence_path.read_text())
                self.assertEqual(
                    evidence["resources"], {"cpu": 4, "memory": 4, "disk": 10}
                )
                self.assertEqual(bool(evidence["mismatches"]), expected_memory == 2)
                self.assertNotIn("must-not-appear", evidence_path.read_text())

    def test_unspecified_resources_preserve_upstream_task_defaults(self):
        evidence = resource_evidence(SimpleNamespace(cpu=4, memory=8, disk=10), {})
        self.assertEqual(evidence["mismatches"], {})
        self.assertEqual(evidence["resources"]["memory"], 8)
