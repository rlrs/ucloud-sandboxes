"""Fail when the test suite grows past its checked-in line budget.

The budget counts every *.py under tests/, harness and fixtures included. It
may only decrease. A PR that raises it must say why, and should name the
tests its new ones retire (docs/rl-scale-architecture-plan.md, C8.5). Lower
it when a deletion lands so the freed lines cannot regrow.
"""
from __future__ import annotations

from pathlib import Path
import unittest

from tests.test_package_budget import package_lines

# Set after C8.4 deleted the tests that the first local-fleet scenarios (S1-S4,
# S8, S10, S13) cover. The 60k target needs the remaining scenarios and the
# code deletions whose tests go with them (C1.1, C1.3, C1.4, C2.1, C2.9).
# 89,500 after the C8.4 consolidation, raised to 93,000 for the tests merged
# alongside it: C1.1 follow-ups, C3.1 commit, C4.4 sender, C5.1 guest agent
# and C5.2, then to 93,200 for the node-failure scenarios (reboot re-adoption
# and reaping, heartbeat pull, UCloud quarantine), then to 93,390 for the C2.15
# upstream mirror (test_upstream_mirror), then to 93,450 for the C9.2 rollout
# scenario tests, then to 93,490 for the backend EAGAIN tests. Lower it later.
SUITE_LINE_BUDGET = 93_490
SUITE = Path(__file__).resolve().parent


class SuiteBudgetTests(unittest.TestCase):
    def test_suite_stays_within_line_budget(self):
        lines = package_lines(SUITE)
        self.assertGreater(lines, 0, f"no Python sources under {SUITE}")
        self.assertLessEqual(
            lines, SUITE_LINE_BUDGET,
            f"tests/ has {lines} lines, over the budget of {SUITE_LINE_BUDGET}; "
            "delete tests that scenarios cover or justify raising the budget",
        )


if __name__ == "__main__":
    unittest.main()
