"""Node agents push their own heartbeats (plan C4.4) through the local fleet.

Tier: contract. The real sender thread in each real node agent posts to the
real gateway; no scenario step relays a heartbeat here.
"""

from datetime import datetime
import threading
import time
import unittest
from unittest.mock import patch

from ucloud_sandboxes.models import utc_now

from tests.harness import LocalFleet
from tests.harness import fleet as harness

TEST_TIER = "contract"


def senders() -> int:
    return sum(thread.name == "node-heartbeat" for thread in threading.enumerate())


def stored(fleet: LocalFleet, job_id: str) -> dict:
    nodes = fleet.request("GET", "/v1/nodes").json()["nodes"]
    return next(node for node in nodes if node["job_id"] == job_id)


class LocalFleetHeartbeatTests(unittest.TestCase):
    def test_node_agents_push_inventory_on_their_interval(self):
        with patch.object(harness, "HEARTBEAT_INTERVAL_SECONDS", 1), LocalFleet(nodes=2) as fleet:
            self.assertEqual(senders(), 2)
            fleet.create("alpha")
            created = utc_now()
            job_id = fleet.route("alpha").job_id
            deadline = time.monotonic() + 10
            while True:
                current = stored(fleet, job_id)
                if (datetime.fromisoformat(current["received_at"]) > created
                        and "alpha" in [item["sandbox_id"] for item in current["inventory"]]):
                    break
                self.assertLess(time.monotonic(), deadline, "no periodic heartbeat arrived")
                time.sleep(0.05)

    def test_restarted_agent_announces_itself_and_its_predecessor_stops_sending(self):
        with LocalFleet() as fleet:
            node = fleet.nodes[0]
            before = datetime.fromisoformat(stored(fleet, node.job_id)["received_at"])
            node.restart()
            self.assertEqual(senders(), 1)
            after = datetime.fromisoformat(stored(fleet, node.job_id)["received_at"])
            self.assertGreater(after, before, "the new agent's first heartbeat precedes start()")
            node.stop()
            self.assertEqual(senders(), 0)


if __name__ == "__main__":
    unittest.main()
