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
# scenario tests, then to 93,490 for the backend EAGAIN tests, then to 93,653
# for the rollout think modes, then 93,681 for composition sharing, then
# 94,939 (+1,258) for the C2.13 M1 chunk store: format tamper tests, the index
# over real SQLite and HTTP, SigV4 vectors, converter crash injection at every
# write-path step, the worker read path against an S3 stand-in and concurrent
# attach. They retire test_environment_layers and test_erofs_metadata with the
# EROFS builder after migration. Then +276 for the M1 gate driver
# (test_chunk_store_gate: no scenario covers an operator script) and +399 for
# the C2.6 store node (test_chunk_store_node: needs a store node, not a scenario), +9 adapter, +16 attach knob, +6 gate fixes,
# +24 verifier fixes from the M1 gate (replaced hardlink members, node locators),
# +17 path-ordered layers, +52 the convert race and the gate bench's node API
# (operation, unmount; test_chunk_store_gate: no scenario covers an operator script),
# +22 exact rollback symlinks (real mkfs.erofs images), +43 the gate's baseline worker,
# +53 M2 readers (dispatched roots, spec fingerprints, the RAFS flag), +113 the gateway side,
# +62 the M2 inventory and retention view, +31 the attach-timing diagnostic,
# +56 shared startup traces, +135 the nydusd spike's virtual blobs (fake and real
# nydus-image, test_nydusd_spike).
SUITE_LINE_BUDGET = 96_255
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
