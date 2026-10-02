"""S13: node drain and admission fences, over the local fleet harness.

Tier: contract. Drain closes a worker to new creates, wakes, execs and file
traffic, is owned by its token, and waits out work admitted before it. Live
admission reads the host sample (``FleetNode.sample_metrics``): an exec
without headroom is deferred at once, a create waits for headroom only until
its deadline, and an unknown sample never waits.

Not covered: drain during an image pull or a memory wait, and the
empty-inventory stop proof, which belong to the autoscaler (S9).
"""

from dataclasses import replace
import threading
import time
import unittest

from tests.harness import LocalFleet
from tests.harness.assembly import fixed_metrics

SPEC = {"image": "harness/base:1", "cpus": 1, "memory_mb": 256, "disk_mb": 1024, "network": "none"}
WAIT = 1.5


def pressured(available_mb: int = 100):
    return lambda: replace(fixed_metrics(), memory_available_mb=available_mb)


class DrainAdmissionTests(unittest.TestCase):
    def assert_retryable(self, response, error_code: str) -> None:
        self.assertEqual(response.status, 503, response.body)
        self.assertEqual((response.json()["error_code"], response.json()["retryable"]), (error_code, True))

    def test_drain_fences_new_work_and_reopens_with_its_token(self):
        with LocalFleet() as fleet:
            node = fleet.nodes[0]
            fleet.create("alpha")
            fleet.create("resting", parkable=True)
            self.assertEqual(fleet.park("resting").status, 200)

            drained = node.drain("ops-1")
            self.assertEqual(drained.status, 200, drained.body)
            self.assertEqual(
                {key: drained.json()["drain"][key] for key in ("token", "draining", "admission_open", "ready")},
                {"token": "ops-1", "draining": True, "admission_open": False, "ready": False},
            )
            fleet.heartbeat()
            for response in (fleet.start_exec("alpha", ["true"]), fleet.read_file("alpha", "/etc/hostname")):
                self.assert_retryable(response, "node_admission_closed")
            # The gateway knows from the heartbeat; a local park has no other destination.
            self.assert_retryable(fleet.wake("resting", generation=fleet.route("resting").generation),
                                  "wake_destination_unavailable")
            refused = fleet.request("POST", "/v1/sandboxes", payload={"id": "beta", **SPEC}, token="sandbox")
            self.assert_retryable(refused, "no_ready_node")
            # Pending demand now holds this create as well as the waiting wake.
            pending = refused.json()["pending_resources"]
            self.assertTrue(pending["vcpu"] >= 2 and pending["memory_mb"] >= 512 and pending["disk_mb"] >= 2048,
                            pending)

            # Drain is durable across an agent restart, and only its own token ends it.
            node.restart()
            fleet.heartbeat()
            self.assert_retryable(fleet.start_exec("alpha", ["true"]), "node_admission_closed")
            self.assertEqual(node.drain("ops-2").json(), {"error": "node is draining with another token"})
            self.assertEqual(node.drain("ops-2", draining=False).status, 409)
            reopened = node.drain("ops-1", draining=False)
            self.assertEqual((reopened.status, reopened.json()["drain"]["admission_open"]), (200, True))
            fleet.heartbeat()
            self.assertEqual(fleet.exec("alpha", ["cat", "/opt/greeting"]).exit_code, 0)
            self.assertEqual(fleet.wake("resting", generation=fleet.route("resting").generation).status, 200)
            self.assertEqual(fleet.create("beta")["state"], "running")

    def test_drain_waits_out_a_create_admitted_before_it(self):
        with LocalFleet() as fleet:
            node = fleet.nodes[0]
            # Blocked mounting its rootfs: admitted, quota prepared but committed
            # only with the rootfs (C5.2), not yet active.
            node.arm_fault("mount", "hang")
            result: dict = {}
            create = threading.Thread(target=lambda: result.setdefault("response", fleet.request(
                "POST", "/v1/sandboxes", payload={"id": "alpha", **SPEC}, token="sandbox")), daemon=True)
            create.start()
            node.wait_hung("mount")

            drained = node.drain("ops-1").json()["drain"]
            self.assertEqual((drained["ready"], drained["active_sandboxes"], drained["reserved_resources"]),
                             (False, 0, {"vcpu": 1.0, "memory_mb": 256, "disk_mb": 1024}))
            during = node.post_heartbeat()["node"]
            self.assertEqual(
                (during["draining"], during["active_sandbox_creates"],
                 [(item["sandbox_id"], item["state"]) for item in during["inventory"]]),
                (True, 1, [("alpha", "planned")]),
            )
            refused = fleet.request("POST", "/v1/sandboxes", payload={"id": "beta", **SPEC}, token="sandbox")
            self.assert_retryable(refused, "no_ready_node")

            node.release("mount")
            create.join(15)
            self.assertEqual(result["response"].status, 201, result["response"].body)
            after = node.post_heartbeat()["node"]
            self.assertEqual((after["draining"], after["active_sandbox_creates"], after["active_sandboxes"]),
                             (True, 0, 1))
            self.assertIsNone(node.registration("beta"))

    def test_live_admission_reads_the_host_sample(self):
        with LocalFleet(admission_wait_seconds=WAIT) as fleet:
            node = fleet.nodes[0]
            fleet.create("alpha")

            def timed(call):
                started = time.monotonic()
                return call(), time.monotonic() - started

            def create(name):
                return fleet.request("POST", "/v1/sandboxes", payload={"id": name, **SPEC}, token="sandbox")

            # CPU load is advice, not a veto, for commands in a resident sandbox.
            node.sample_metrics = lambda: replace(fixed_metrics(), cpu_percent=95.0, load_average_1m=16.0)
            self.assertEqual(fleet.exec("alpha", ["true"]).exit_code, 0)

            # No headroom: an exec is deferred at once, a create only after
            # waiting out its deadline, and neither runs later.
            node.sample_metrics = pressured()
            response, elapsed = timed(lambda: fleet.start_exec("alpha", ["true"]))
            self.assert_retryable(response, "node_active_exec_deferred")
            self.assertLess(elapsed, WAIT)
            response, elapsed = timed(lambda: create("late"))
            self.assert_retryable(response, "node_active_admission_deferred")
            # It waited (the node may give up just short of its deadline).
            self.assertGreater(elapsed, WAIT / 2)

            # An unknown sample never waits.
            node.sample_metrics = lambda: None
            for call, error_code in ((lambda: fleet.start_exec("alpha", ["true"]), "node_active_exec_deferred"),
                                     (lambda: create("unknown"), "node_active_admission_deferred")):
                response, elapsed = timed(call)
                self.assert_retryable(response, error_code)
                self.assertIn("no fresh runtime metrics", response.json()["error"])
                self.assertLess(elapsed, WAIT)

            # Headroom that arrives within the deadline admits the create. It
            # arrives 0.2 s after the create first sees pressure, however late.
            relieved_at: list[float] = []

            def relieving():
                if not relieved_at:
                    relieved_at.append(time.monotonic() + 0.2)
                return pressured(100 if time.monotonic() < relieved_at[0] else 60000)()

            node.sample_metrics = relieving
            response, elapsed = timed(lambda: create("patient"))
            self.assertEqual(response.status, 201, response.body)
            self.assertGreaterEqual(elapsed, 0.2)
            self.assertLess(elapsed, WAIT + 1)

            node.sample_metrics = fixed_metrics
            self.assertEqual(fleet.exec("alpha", ["true"]).exit_code, 0)
            fleet.heartbeat()
            for refused in ("late", "unknown"):
                self.assertIsNone(fleet.route(refused))
                self.assertIsNone(node.registration(refused))


if __name__ == "__main__":
    unittest.main()
