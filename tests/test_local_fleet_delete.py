"""S10: delete races and generation fencing, over the local fleet harness.

Tier: contract. Traffic to a slow create is fenced until it is owned; a
delete that overlaps it wins, and the create reports that its ownership
ended. A failed node delete keeps the route as a durable delete intent that
fences traffic until a retry completes it. A delayed delete for an old
generation cannot remove its replacement.

Not covered: publication readers, detached (portable) routes, the queued
PostgreSQL create path and registry references, which the harness does not
model.
"""

import threading
import unittest

from tests.harness import LocalFleet, process_alive

SPEC = {"image": "harness/base:1", "cpus": 1, "memory_mb": 256, "disk_mb": 1024, "network": "none"}


def in_background(call):
    result: dict = {}
    thread = threading.Thread(target=lambda: result.setdefault("response", call()), daemon=True)
    thread.start()
    return thread, result


class DeleteRaceTests(unittest.TestCase):
    def assert_gone(self, fleet: LocalFleet, node, name: str) -> None:
        self.assertIsNone(fleet.route(name))
        self.assertIsNone(node.registration(name))
        self.assertEqual(node.live_sentries(), [])
        self.assertEqual(node.storage_backend.devices, {})
        # Only host lock files may outlive a sandbox's volume.
        self.assertEqual([path for path in node.volumes.iterdir() if path.suffix != ".lock"], [])
        self.assertEqual(fleet.status(name).json()["sandboxes"], [])

    def test_delete_during_slow_create_wins(self):
        with LocalFleet() as fleet:
            node = fleet.nodes[0]
            node.arm_fault("create", "hang", sandbox_id="alpha")
            create, created = in_background(lambda: fleet.request(
                "POST", "/v1/sandboxes", payload={"id": "alpha", **SPEC}, token="sandbox"))
            node.wait_hung("create")
            self.assertEqual((fleet.route("alpha").state, node.registration("alpha").phase),
                             ("creating", "rootfs_ready"))
            # Traffic is fenced until the create is owned and never reaches
            # the worker: a fresh one is only asked whether the create
            # finished, a silent one is not asked at all.
            for silent, asked in ((False, ["/v1/sandboxes?sandbox_id=alpha"]), (True, [])):
                if silent:
                    fleet.expire_heartbeat(node)
                before = len(node.requests)
                fenced = fleet.start_exec("alpha", ["true"])
                self.assertEqual((fenced.status, fenced.json()["retryable"]), (503, True), fenced.body)
                self.assertIn("creation is already in progress", fenced.json()["error"])
                self.assertEqual([path for _, path in node.requests[before:] if "alpha" in path], asked)
            fleet.heartbeat()

            delete, deleted = in_background(lambda: fleet.delete("alpha"))
            # The delete reaches the worker and waits there for the create.
            while ("DELETE", "/v1/sandboxes/alpha") not in node.requests:
                self.assertTrue(delete.is_alive())
                delete.join(0.005)
            node.release("create")
            create.join(15)
            delete.join(15)

            self.assertEqual(deleted["response"].status, 200, deleted["response"].body)
            superseded = created["response"]
            self.assertEqual((superseded.status, superseded.json()), (410, {
                "error": "sandbox ownership ended before worker confirmation",
                "error_code": "sandbox_observation_superseded",
                "retryable": False,
            }))
            self.assert_gone(fleet, node, "alpha")

    def test_failed_node_delete_is_durable_intent_until_retried(self):
        with LocalFleet() as fleet:
            node = fleet.nodes[0]
            fleet.create("alpha")
            pid = node.sentry_pid("alpha")
            session = fleet.start_exec("alpha", ["sleep", "30"]).json()["session"]["id"]
            node.arm_fault("delete", "fail", sandbox_id="alpha")

            failed = fleet.delete("alpha")
            self.assertEqual(failed.status, 503, failed.body)
            self.assertIn("injected delete failure", failed.json()["error"])
            route = fleet.route("alpha")
            self.assertEqual(route.state, "running")
            self.assertTrue(route.delete_operation_id)
            self.assertEqual(node.registration("alpha").phase, "deleting")
            for response in (fleet.start_exec("alpha", ["true"]), fleet.read_file("alpha", "/etc/hostname")):
                self.assertEqual(response.status, 409, response.body)
                self.assertEqual(response.json(), {
                    "error": "sandbox deletion is in progress",
                    "error_code": "sandbox_delete_pending",
                    "retryable": False,
                })

            retried = fleet.delete("alpha")
            self.assertEqual(retried.status, 200, retried.body)
            self.assertFalse(process_alive(pid))
            # The deleted sandbox's command was killed with it.
            exit_event = fleet.events(session, after=0, wait_seconds=0).json()["events"][-1]
            self.assertEqual(exit_event["stream"], "exit")
            self.assertNotEqual(exit_event["exit_code"], 0)
            self.assert_gone(fleet, node, "alpha")
            again = fleet.delete("alpha")
            self.assertEqual((again.status, again.json()), (200, {"ok": True, "deleted": False}))

    def test_delayed_delete_cannot_remove_a_replacement(self):
        with LocalFleet() as fleet:
            node = fleet.nodes[0]
            fleet.create("alpha", parkable=True)
            self.assertEqual(fleet.park("alpha").status, 200)
            # A parked sandbox's storage is released with it.
            self.assertEqual(fleet.delete("alpha").status, 200)
            self.assert_gone(fleet, node, "alpha")
            fleet.create("alpha")
            self.assertEqual(fleet.route("alpha").generation, 2)

            # The first incarnation's delete arrives late at the worker.
            stale = node.request("DELETE", "/v1/sandboxes/alpha", headers={
                "X-UCloud-Sandbox-Generation": "1", "X-UCloud-Sandbox-Operation-Id": "delete-late"})
            self.assertEqual((stale.status, stale.json()),
                             (409, {"error": "delete generation does not own direct sandbox"}))
            unfenced = node.request("DELETE", "/v1/sandboxes/alpha")
            self.assertEqual(unfenced.status, 400, unfenced.body)
            self.assertEqual((node.registration("alpha").phase, node.registration("alpha").sandbox_generation),
                             ("owned", 2))
            self.assertEqual(fleet.exec("alpha", ["cat", "/opt/greeting"]).exit_code, 0)


if __name__ == "__main__":
    unittest.main()
