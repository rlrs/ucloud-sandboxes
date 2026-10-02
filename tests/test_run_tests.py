"""The parallel runner's tier convention and per-module result recording."""

from pathlib import Path
from tempfile import TemporaryDirectory
import json
import unittest
import warnings
from unittest.mock import patch

from scripts import run_tests


class TierMarkerTests(unittest.TestCase):
    def tier(self, source: str) -> str:
        with TemporaryDirectory() as raw:
            path = Path(raw) / "test_fixture.py"
            path.write_text(source)
            return run_tests.module_tier(path)

    def test_unmarked_module_is_unit_and_marker_selects_tier(self):
        self.assertEqual(self.tier("import unittest\n"), "unit")
        self.assertEqual(self.tier('TEST_TIER = "contract"\n'), "contract")

    def test_marker_must_be_one_known_literal(self):
        for source in (
            'TEST_TIER = "integration"\n',
            "TEST_TIER = TIER\n",
            'TEST_TIER = OTHER = "unit"\n',
            'TEST_TIER = "unit"\nTEST_TIER = "contract"\n',
        ):
            with self.subTest(source=source), self.assertRaises(SystemExit):
                self.tier(source)

    def test_every_checked_in_module_has_a_valid_tier(self):
        modules = run_tests.discover()
        self.assertIn("tests.test_run_tests", modules)
        self.assertTrue({tier for _, tier in modules.values()} <= set(run_tests.TIERS))


class ChildRecordingTests(unittest.TestCase):
    def test_child_records_outcomes_subtests_and_class_fixture_errors(self):
        class Fixture(unittest.TestCase):
            def test_pass(self):
                pass

            def test_skip(self):
                self.skipTest("absent dependency")

            def test_subtest_failure(self):
                with self.subTest(case=1):
                    self.fail("first case")

        class BrokenFixture(unittest.TestCase):
            @classmethod
            def setUpClass(cls):
                raise OSError("class fixture failed")

            def test_never_runs(self):
                pass

        suite = unittest.TestSuite([
            unittest.defaultTestLoader.loadTestsFromTestCase(Fixture),
            unittest.defaultTestLoader.loadTestsFromTestCase(BrokenFixture),
        ])
        with TemporaryDirectory() as raw:
            events = Path(raw) / "events.jsonl"
            with patch.object(run_tests, "_exit_with_runner"), patch.object(
                run_tests.unittest.defaultTestLoader, "loadTestsFromName",
                return_value=suite,
            ), patch.object(run_tests.sys, "stderr"):
                self.assertEqual(run_tests.run_child("fixture", str(events), 0), 1)
            lines = [json.loads(line) for line in events.read_text().splitlines()]
        outcomes = {line["test"]: line["outcome"] for line in lines if "test" in line}
        by_method = {test.rsplit(".", 1)[-1]: outcome for test, outcome in outcomes.items()}
        self.assertEqual(by_method["test_pass"], "pass")
        self.assertEqual(by_method["test_skip"], "skip")
        self.assertEqual(by_method["test_subtest_failure"], "fail")
        self.assertEqual(
            [outcome for test, outcome in outcomes.items() if "setUpClass" in test],
            ["error"],
        )
        self.assertEqual(lines[-1], {"done": 3})
        self.assertEqual(
            sum("start" in line for line in lines), 3,
            "class fixture errors are recorded without a start event",
        )

    def test_child_installs_the_warning_filter_of_python_m_unittest(self):
        class Fixture(unittest.TestCase):
            def test_records_deprecation(self):
                with warnings.catch_warnings(record=True) as seen:
                    warnings.warn("old", DeprecationWarning)
                self.assertEqual(len(seen), 1)

        suite = unittest.defaultTestLoader.loadTestsFromTestCase(Fixture)
        with TemporaryDirectory() as raw, warnings.catch_warnings():
            # The interpreter default ignores DeprecationWarning here.
            warnings.simplefilter("ignore")
            with patch.object(run_tests, "_exit_with_runner"), patch.object(
                run_tests.unittest.defaultTestLoader, "loadTestsFromName",
                return_value=suite,
            ), patch.object(run_tests.sys, "stderr"), patch.object(
                run_tests.sys, "warnoptions", [],
            ):
                events = str(Path(raw) / "events.jsonl")
                self.assertEqual(run_tests.run_child("fixture", events, 0), 0)


if __name__ == "__main__":
    unittest.main()
