"""Silence is never loss (D2): a late push is answered by one shared pull.

Tier: contract. Before the gateway answers sandbox_worker_unreachable for
exec, files, park, DELETE or an exec session, it pulls the worker's
heartbeat once, shared by every concurrent request to that worker boot. A
pull that finds a new boot is ingested like a push and retires the old one.
"""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from tests.harness import LocalFleet
from tests.harness.fleet import DEFAULT_IMAGE
from ucloud_sandboxes.agent import build_heartbeat
from ucloud_sandboxes.gateway import heartbeats
from ucloud_sandboxes.gateway.heartbeats import PULL_TIMEOUT_SECONDS, HeartbeatIngest, HeartbeatOutcome

TEST_TIER = "contract"


def pulls(node) -> int:
    return node.requests.count(("GET", "/v1/heartbeat"))


def pull_outcomes(fleet) -> list[str]:
    metrics = fleet.gateway.RequestHandlerClass.metrics_store
    return [event.data["outcome"] for event in metrics.load_events(kinds=("node_heartbeat_pull",))]


class HeartbeatPullTests(unittest.TestCase):
    def test_late_push_is_not_silence(self):
        with LocalFleet() as fleet, patch.object(heartbeats, "PULL_INTERVAL_SECONDS", 0):
            node = fleet.nodes[0]
            fleet.create("alpha")
            fleet.create("doomed")
            # The worker is alive; only its push is late (2026-09-20).
            fleet.expire_heartbeat(node)
            self.assertEqual(fleet.start_exec("alpha", ["true"]).status, 201)
            self.assertEqual(fleet.write_file("alpha", "/workspace/a.txt", b"kept").status, 200)
            self.assertEqual(fleet.read_file("alpha", "/workspace/a.txt").body, b"kept")
            # The pulled receipt is fresh for every later request.
            self.assertEqual(pulls(node), 1)
            fleet.expire_heartbeat(node)
            self.assertEqual(fleet.park("alpha").status, 200)
            fleet.expire_heartbeat(node)
            self.assertEqual(fleet.delete("doomed").status, 200)
            self.assertIsNone(node.registration("doomed"))
            self.assertEqual((pulls(node), pull_outcomes(fleet)), (3, ["refreshed"] * 3))

    def test_create_replay_and_identity(self):
        with LocalFleet() as fleet, patch.object(heartbeats, "PULL_INTERVAL_SECONDS", 0):
            node = fleet.nodes[0]
            store = fleet.gateway.RequestHandlerClass.services.fleet.store
            routes = fleet.gateway.RequestHandlerClass.routing_store
            fleet.create("alpha")
            # An ambiguous create replays on its assigned node, late push or not.
            routes.upsert_sandbox(replace(routes.get_sandbox("alpha"), state="creating"))
            current = store.get_heartbeat(node.job_id)
            store.upsert_heartbeat(replace(current, labels={**current.labels, "pool": "a"}))
            fleet.expire_heartbeat(node)
            replay = fleet.request("POST", "/v1/sandboxes", token="sandbox", payload={
                "id": "alpha", "image": DEFAULT_IMAGE, "cpus": 1, "memory_mb": 256,
                "disk_mb": 1024, "network": "none"})
            self.assertEqual((replay.status, fleet.route("alpha").state), (200, "running"))
            # The sender's labels, which a pull cannot see, survive it.
            self.assertEqual(store.get_heartbeat(node.job_id).labels["pool"], "a")

            # Another agent at the stored URL is not this worker.
            served = node.server.RequestHandlerClass.node_heartbeat
            fleet.expire_heartbeat(node)
            stale = store.get_heartbeat(node.job_id)
            with patch.object(node.server.RequestHandlerClass, "node_heartbeat",
                              lambda handler: replace(served(handler), agent_version="other")):
                self.assertEqual(fleet.start_exec("alpha", ["true"]).status, 503)
            self.assertEqual(store.get_heartbeat(node.job_id).freshness_at, stale.freshness_at)
            self.assertEqual(pull_outcomes(fleet), ["refreshed", "identity_mismatch"])

    def test_unanswered_pulls_back_off(self):
        stale = build_heartbeat(job_id="job", node_id="node", node_url="http://node:8090",
                                node_epoch="boot", deployment_id="d")
        ingest = HeartbeatIngest(
            store=SimpleNamespace(get_heartbeat=lambda *_args, **_kwargs: stale),
            routing_store=None, metrics_store=None, deployment_id="d",
            registry_refs=None, layer_cache=None)
        outcomes = ["unreachable", "identity_mismatch", "unreachable", "refreshed", "unreachable"]
        ingest._pull = Mock(side_effect=outcomes)
        clock = [0.0]
        pulled = []
        with patch.object(heartbeats, "time", SimpleNamespace(monotonic=lambda: clock[0])):
            # Pulls are due after 2 s, then 4, 8 and 16 s while unanswered;
            # an answered pull restores the 2 s interval.
            for at in (0, 3.9, 4, 11.9, 12, 27.9, 28, 29.9, 30, 31):
                clock[0] = at
                calls = ingest._pull.call_count
                self.assertIs(ingest.refresh(stale, lambda _url: None), stale)
                if ingest._pull.call_count > calls:
                    pulled.append(at)
        self.assertEqual(pulled, [0, 4, 12, 28, 30])

    def test_silent_worker_is_unreachable_within_one_shared_pull(self):
        with LocalFleet() as fleet:
            node = fleet.nodes[0]
            fleet.create("alpha")
            released = threading.Event()

            def hang(_self):
                released.wait(30)
                raise RuntimeError("released")

            def timed(_):
                started = time.monotonic()
                response = fleet.start_exec("alpha", ["true"])
                return response, time.monotonic() - started

            fleet.expire_heartbeat(node)
            with patch.object(node.server.RequestHandlerClass, "node_heartbeat", hang):
                try:
                    with ThreadPoolExecutor(8) as pool:
                        answers = list(pool.map(timed, range(8)))
                    # Within the pull interval nothing asks the worker again.
                    answers.append(timed(None))
                finally:
                    released.set()
            for response, elapsed in answers:
                self.assertEqual((response.status, response.json()["error_code"]),
                                 (503, "sandbox_worker_unreachable"))
                self.assertLess(elapsed, PULL_TIMEOUT_SECONDS + 1.5)
            self.assertEqual((pulls(node), pull_outcomes(fleet)), (1, ["unreachable"]))

    def test_pull_that_finds_a_new_boot_retires_the_old_one(self):
        def first_exec(fleet, session):
            return fleet.start_exec("alpha", ["true"]), (410, "node_lost")

        def first_session(fleet, session):
            return fleet.events(session, after=0, wait_seconds=0), (410, "exec_worker_lost")

        def first_durable_session(fleet, session):
            self.assertTrue(session.startswith("exec-"))
            return first_session(fleet, session)

        def first_delete(fleet, session):
            # Retirement keeps the recorded delete for delivery (D1), so the
            # request that pulled the new boot answers one retryable 503 and
            # the retry, or the reboot reaper, delivers it.
            first = fleet.delete("alpha")
            self.assertEqual((first.status, first.json()["error_code"]), (503, "sandbox_worker_unreachable"))
            return fleet.delete("alpha"), (200, None)

        for first in (first_exec, first_session, first_durable_session, first_delete):
            with self.subTest(first.__name__), LocalFleet() as fleet:
                node = fleet.nodes[0]
                node.honor_exec_session_prefix = first is not first_durable_session
                store = fleet.gateway.RequestHandlerClass.services.fleet.store
                fleet.create("alpha")
                session = fleet.start_exec("alpha", ["sleep", "30"]).json()["session"]["id"]
                old_epoch = store.get_heartbeat(node.job_id).node_epoch
                # The rebooted worker's pushes never arrive; only a pull sees it.
                refused = HeartbeatOutcome(503, {"error": "push lost"})
                with patch.object(fleet.gateway.RequestHandlerClass.services.heartbeats, "receive",
                                  return_value=refused), patch.object(node, "post_heartbeat", lambda: None):
                    node.reboot()
                    fleet.expire_heartbeat(node)
                    response, expected = first(fleet, session)
                self.assertEqual((response.status, response.json().get("error_code")), expected)
                stored = store.get_heartbeat(node.job_id)
                self.assertIn(old_epoch, stored.retired_node_epochs)
                self.assertTrue(stored.is_fresh(stored.freshness_at, fleet.heartbeat_ttl_seconds))
                self.assertIsNone(fleet.route("alpha"))
                self.assertEqual(pull_outcomes(fleet), ["epoch_changed"])


if __name__ == "__main__":
    unittest.main()
