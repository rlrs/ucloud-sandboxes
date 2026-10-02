"""Fail when the server package grows past its checked-in line budget.

The budget may only decrease. A PR that raises it must say why, and should
name the mechanism its new code retires (docs/rl-scale-architecture-plan.md,
C6.3). Lower it when a deletion lands so the freed lines cannot regrow.
"""
from __future__ import annotations

from pathlib import Path
import unittest

# Budget history from the 102,852-line baseline (90ce959), with what offsets it:
# + C2.2/C2.3 EROFS metadata and trace prefetch, C2.12 --MZ (the Python NBD
#   server and cache they extend go with C2.1);
# + C1.1 pause tier and its thaw-prefetch, reclaim-budget and escalation
#   follow-ups (warm_park goes once the pause flag is the only path);
# + C2.11 EROFS layout 2; C4.3 placement_choice (its wiring deletes the
#   whole-fleet scan, worker revisions and advisory turns); C5.2 netns pool;
#   C4.4 in-process heartbeat sender; C5.2 part 2 (durability levels and the
#   in-memory registry index);
# + C5.1 step 1 guest agent client (sandbox_exec's runsc-exec path and its
#   three threads per session go once exec and files are wired to it);
# + C3.1 commit worker and builder halves (the largest addition);
# - C4.7 shadow program scheduler, shared-control qualification store, and
#   the C6.1 extraction steps so far (PR4-PR6 net negative);
# + D1 reboot = process loss: exact park re-adoption, the retired-boot reaper
#   and registration-conflict codes (+187 net; the dead cross-boot reconcile
#   branch and three dead helpers went).
# + D2 heartbeat pull before any sandbox_worker_unreachable answer, and D3-D7
#   UCloud quarantine re-anchoring, provider-confirmed loss and replay
#   ordering (+151 net together; RoutingStore.delete_sandboxes_for_jobs went).
# + C2.15 upstream pull-through mirror (+265: config, mirror/trim helpers,
#   builder mirrors). What it retires, the campaign's upstream cooldowns, lives
#   in scripts/ outside this count and goes once production runs with it.
# + the environment backend's deep backlog and EAGAIN connect retry (+15),
#   and sharing a composition between config-only siblings (+1).
# Lower it whenever a deletion lands; storage-native (C1.3) alone is ~8k lines.
PACKAGE_LINE_BUDGET = 107_016
PACKAGE = Path(__file__).resolve().parents[1] / "ucloud_sandboxes"


def package_lines(root: Path = PACKAGE) -> int:
    total = 0
    for path in root.rglob("*.py"):
        data = path.read_bytes()
        total += data.count(b"\n") + (1 if data and not data.endswith(b"\n") else 0)
    return total


class PackageBudgetTests(unittest.TestCase):
    def test_package_stays_within_line_budget(self):
        lines = package_lines()
        self.assertGreater(lines, 0, f"no Python sources under {PACKAGE}")
        self.assertLessEqual(
            lines, PACKAGE_LINE_BUDGET,
            f"ucloud_sandboxes/ has {lines} lines, over the budget of "
            f"{PACKAGE_LINE_BUDGET}; delete code or justify raising the budget",
        )


if __name__ == "__main__":
    unittest.main()
