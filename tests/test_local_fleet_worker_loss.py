"""S3: worker silence, reboot and quarantine, over the local fleet harness.

Tier: contract. A silent worker's routes are retryable and never proxied. A
new boot epoch retires the old boot's routes and sessions as node_lost; a
restart within one boot keeps them. A quarantined worker keeps serving
existing work but admits none, and its inventory cannot retire routes.

The autoscaler half of S3 (provider-reported suspension or power-off, and
no destructive stop replay) needs the S9 fake provider and is not covered.
"""

import unittest

from tests.harness import LocalFleet, process_alive
from ucloud_sandboxes.control_state import QUARANTINE_REASON

SPEC = {"image": "harness/base:1", "cpus": 1, "memory_mb": 256, "disk_mb": 1024, "network": "none"}


class WorkerLossTests(unittest.TestCase):
    def assert_unreachable(self, response, node) -> None:
        self.assertEqual(response.status, 503, response.body)
        self.assertEqual(response.json(), {
            "error": "sandbox worker heartbeat is stale or unavailable",
            "error_code": "sandbox_worker_unreachable",
            "retryable": True,
            "node_id": node.node_id,
            "job_id": node.job_id,
        })
        self.assertEqual(response.headers["Retry-After"], "1")

    def test_silent_worker_is_retryable_and_resumes_within_its_boot(self):
        with LocalFleet() as fleet:
            node = fleet.nodes[0]
            fleet.create("alpha")
            fleet.create("doomed")
            fleet.create("resting", parkable=True)
            self.assertEqual(fleet.write_file("alpha", "/workspace/a.txt", b"kept").status, 200)
            self.assertEqual(fleet.park("resting").status, 200)
            generations = {name: fleet.route(name).generation for name in ("alpha", "doomed", "resting")}

            # The agent is down and its last heartbeat expired. Each answer is
            # the gateway's own: a proxy attempt would be a transport error.
            node.stop()
            fleet.expire_heartbeat(node)
            for response in (
                fleet.start_exec("alpha", ["true"]),
                fleet.read_file("alpha", "/workspace/a.txt"),
                fleet.write_file("alpha", "/workspace/b.txt", b"lost"),
                fleet.park("alpha"),
                fleet.delete("doomed"),
            ):
                self.assert_unreachable(response, node)
            # A local park can wake only on its silent worker.
            woken = fleet.wake("resting", generation=generations["resting"])
            self.assertEqual(woken.status, 503, woken.body)
            self.assertEqual((woken.json()["error_code"], woken.json()["retryable"]),
                             ("wake_destination_unavailable", True))
            status = {item["id"]: item for item in fleet.request("GET", "/v1/sandboxes?view=status").json()["sandboxes"]}
            self.assertEqual(
                {name: (item["state"], item["cached_state"], item["node"]["fresh"]) for name, item in status.items()},
                {"alpha": ("unknown", "running", False), "doomed": ("unknown", "running", False),
                 "resting": ("unknown", "parked", False)},
            )

            # A restart within the same boot proves continuity. The refused
            # delete stays durable intent: it fences traffic until retried.
            node.start()
            fleet.heartbeat()
            self.assertEqual(fleet.exec("alpha", ["cat", "/workspace/a.txt"]).stdout, "kept")
            self.assertEqual(fleet.wake("resting", generation=generations["resting"]).status, 200)
            self.assertEqual({name: fleet.route(name).generation for name in generations}, generations)
            fenced = fleet.start_exec("doomed", ["true"])
            self.assertEqual((fenced.status, fenced.json()["error_code"]), (409, "sandbox_delete_pending"))
            self.assertEqual(fleet.delete("doomed").status, 200)
            self.assertIsNone(fleet.route("doomed"))
            self.assertIsNone(node.registration("doomed"))

    def test_reboot_retires_the_old_boot_as_node_lost(self):
        with LocalFleet() as fleet:
            node = fleet.nodes[0]
            fleet.create("alpha")
            fleet.create("resting", parkable=True)
            self.assertEqual(fleet.park("resting").status, 200)
            pid = node.sentry_pid("alpha")

            node.reboot()
            fleet.heartbeat()
            self.assertFalse(process_alive(pid))
            # A local park dies with its worker's boot, like a running sandbox.
            self.assertEqual((fleet.route("alpha"), fleet.route("resting")), (None, None))
            self.assertEqual(fleet.status("alpha").json()["sandboxes"], [])
            for name, response in (
                ("alpha", fleet.request("GET", "/v1/sandboxes/alpha")),
                ("alpha", fleet.request("GET", "/v1/sandboxes/alpha/jobs/job-1")),
                ("alpha", fleet.start_exec("alpha", ["true"])),
                ("alpha", fleet.park("alpha")),
                ("alpha", fleet.read_file("alpha", "/etc/hostname")),
                ("resting", fleet.wake("resting", generation=1)),
            ):
                self.assertEqual(response.status, 410, response.body)
                self.assertEqual(
                    {key: response.json()[key]
                     for key in ("error_code", "retryable", "sandbox_id", "sandbox_generation")},
                    {"error_code": "node_lost", "retryable": False, "sandbox_id": name, "sandbox_generation": 1},
                )
            for _ in range(2):
                deleted = fleet.delete("alpha")
                self.assertEqual((deleted.status, deleted.json()), (200, {"ok": True, "deleted": False}))
            # The rebooted worker still reports what its old boot owned, the
            # dead runtime as quarantined; the gateway no longer routes to it.
            inventory = {item["sandbox_id"]: item["state"] for item in node.post_heartbeat()["node"]["inventory"]}
            self.assertEqual((sorted(inventory), inventory["alpha"]), (["alpha", "resting"], "recovery-required"))
            self.assertIsNotNone(node.registration("alpha"))
            # Current contract: nothing deletes those registrations, so the
            # same name cannot be created on this worker again.
            again = fleet.request("POST", "/v1/sandboxes", payload={"id": "alpha", **SPEC}, token="sandbox")
            self.assertEqual((again.status, again.json()),
                             (503, {"error": "sandbox already has another direct registration"}))
            self.assertEqual(fleet.create("beta")["state"], "running")

    def test_quarantined_worker_serves_existing_work_and_admits_none(self):
        with LocalFleet() as fleet:
            node = fleet.nodes[0]
            for name in ("alpha", "beta", "omega"):
                fleet.create(name)
            store = fleet.gateway.RequestHandlerClass.services.fleet.store

            def remove_out_of_band(name: str) -> None:
                removed = node.request("DELETE", f"/v1/sandboxes/{name}", headers={
                    "X-UCloud-Sandbox-Generation": "1", "X-UCloud-Sandbox-Operation-Id": f"delete-{name}"})
                self.assertEqual(removed.status, 200, removed.body)

            # Normally a complete inventory retires an absent route.
            remove_out_of_band("omega")
            fleet.heartbeat()
            self.assertIsNone(fleet.route("omega"))

            # What the autoscaler does when provider readiness is unverified.
            store.quarantine_node(node.job_id, "provider_readiness_unverified")
            refused = fleet.request("POST", "/v1/sandboxes", payload={"id": "gamma", **SPEC}, token="sandbox")
            self.assertEqual(refused.status, 503, refused.body)
            self.assertEqual((refused.json()["error_code"], refused.json()["retryable"]), ("no_ready_node", True))
            self.assertEqual(fleet.exec("alpha", ["cat", "/opt/greeting"]).stdout, "hello from the image\n")

            # The worker's own heartbeats cannot lift quarantine, and its
            # inventory cannot retire a route while it lasts.
            remove_out_of_band("beta")
            fleet.heartbeat()
            self.assertEqual(store.get_heartbeat(node.job_id).labels[QUARANTINE_REASON],
                             "provider_readiness_unverified")
            self.assertEqual(fleet.route("beta").state, "running")


if __name__ == "__main__":
    unittest.main()
