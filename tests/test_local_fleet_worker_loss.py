"""S3: worker silence, reboot and quarantine, over the local fleet harness.

Tier: contract. A silent worker's routes are retryable and never proxied. A
new boot epoch loses the old boot's processes as node_lost (reason
rebooted), over SQLite and PostgreSQL routing, but keeps its complete local
parks; fenced deletes then free every other old-boot registration, recorded
client deletes included. A restart within one boot keeps everything. A
quarantined worker keeps serving existing work but admits none, and its
inventory cannot retire routes, but it still wakes its own parks. The
autoscaler's continuity step (a fake provider job, the real probe) retires
what a verified same-boot inventory omits, and quarantines a historical
suspension once.

The rest of the autoscaler half of S3 (provider power-off, no destructive
stop replay) needs the full S9 fake provider and is not covered.
"""

from datetime import timedelta
import threading
import time
import unittest

from tests.harness import LocalFleet, process_alive
from ucloud_sandboxes.control_state import QUARANTINE_REASON
from ucloud_sandboxes.models import utc_now

SPEC = {"image": "harness/base:1", "cpus": 1, "memory_mb": 256, "disk_mb": 1024, "network": "none"}


def in_background(call) -> threading.Thread:
    thread = threading.Thread(target=call, daemon=True)
    thread.start()
    return thread


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

    def assert_rebooted(self, response, name: str, generation: int = 1) -> None:
        self.assertEqual(response.status, 410, response.body)
        self.assertEqual(
            {key: response.json()[key]
             for key in ("error_code", "reason", "retryable", "sandbox_id", "sandbox_generation")},
            {"error_code": "node_lost", "reason": "rebooted", "retryable": False,
             "sandbox_id": name, "sandbox_generation": generation},
        )

    def wait_reaped(self, node, *names: str) -> None:
        # The gateway reaps off the heartbeat thread.
        deadline = time.monotonic() + 15
        while any(node.registration(name) is not None for name in names):
            self.assertLess(time.monotonic(), deadline, f"{names} were not reaped")
            time.sleep(0.02)

    def test_reboot_loses_processes_and_keeps_complete_parks(self):
        self._reboot_loses_processes_and_keeps_complete_parks(postgres=False)

    def test_reboot_loses_processes_and_keeps_complete_parks_with_postgres_routing(self):
        self._reboot_loses_processes_and_keeps_complete_parks(postgres=True)

    def _reboot_loses_processes_and_keeps_complete_parks(self, *, postgres: bool) -> None:
        with LocalFleet(postgres=postgres) as fleet:
            node = fleet.nodes[0]
            store = fleet.gateway.RequestHandlerClass.services.fleet.store
            fleet.create("alpha")
            fleet.create("doomed")
            fleet.create("resting", parkable=True)
            self.assertEqual(fleet.write_file("resting", "/workspace/marker", b"kept").status, 200)
            self.assertEqual(fleet.park("resting").status, 200)
            pid = node.sentry_pid("alpha")
            # A delete recorded while the worker is silent, then the reboot.
            node.stop()
            fleet.expire_heartbeat(node)
            self.assert_unreachable(fleet.delete("doomed"), node)
            old_epoch = store.get_heartbeat(node.job_id).node_epoch

            node.reboot()
            new_epoch = store.get_heartbeat(node.job_id).node_epoch
            self.assertNotEqual(new_epoch, old_epoch)
            self.assertFalse(process_alive(pid))
            # Measured, not guessed: one event per proven boot change.
            metrics = fleet.gateway.RequestHandlerClass.metrics_store
            getattr(metrics, "flush", lambda: True)()
            (retired,) = [e.data for e in metrics.load_events() if e.kind == "node_epoch_retired"]
            self.assertEqual((retired["job_id"], retired["node_epoch"], retired["retiring"]),
                             (node.job_id, node.boot_id.hex, False))
            self.assertGreaterEqual(retired["downtime_seconds"], 0)
            # The old boot's processes are lost.
            self.assertIsNone(fleet.route("alpha"))
            self.assertEqual(fleet.status("alpha").json()["sandboxes"], [])
            for response in (
                fleet.request("GET", "/v1/sandboxes/alpha"),
                fleet.request("GET", "/v1/sandboxes/alpha/jobs/job-1"),
                fleet.start_exec("alpha", ["true"]),
                fleet.park("alpha"),
                fleet.read_file("alpha", "/etc/hostname"),
            ):
                self.assert_rebooted(response, "alpha")
            for _ in range(2):
                deleted = fleet.delete("alpha")
                self.assertEqual((deleted.status, deleted.json()), (200, {"ok": True, "deleted": False}))
            # The complete local park moved to the new boot, durably.
            self.assertEqual(
                (fleet.route("resting").state, fleet.route("resting").node_epoch), ("parked", new_epoch))

            # Fenced deletes free what the old boot still held: the lost
            # registration and the recorded delete, delivered, not dropped.
            self.wait_reaped(node, "alpha", "doomed")
            self.assertIsNone(fleet.route("doomed"))
            self.assertEqual(fleet.request("GET", "/v1/sandboxes/doomed").status, 404)
            heartbeat = node.post_heartbeat()["node"]
            self.assertEqual({item["sandbox_id"]: item["state"] for item in heartbeat["inventory"]},
                             {"resting": "parked"})
            self.assertEqual(heartbeat["reserved_resources"], {"vcpu": 0.0, "memory_mb": 0, "disk_mb": 0})
            self.assertEqual(set(node.service.provisioner.registry.disk_claims_mb()), {("resting", 1)})

            self.assertEqual(fleet.wake("resting", generation=1).status, 200)
            self.assertEqual(fleet.exec("resting", ["cat", "/workspace/marker"]).stdout, "kept")
            # The id is free again, and a later heartbeat never reaps the
            # newer incarnation or the re-adopted one.
            again = fleet.create("alpha")
            self.assertEqual((again["state"], fleet.route("alpha").generation), ("running", 2))
            fleet.heartbeat()
            self.assertEqual(fleet.exec("alpha", ["true"]).exit_code, 0)
            self.assertEqual(fleet.exec("resting", ["cat", "/workspace/marker"]).stdout, "kept")

    def test_reboot_settles_every_lifecycle_state(self):
        with LocalFleet(node_processes=True) as fleet:
            node = fleet.nodes[0]
            fleet.create("capturing", parkable=True)
            fleet.create("waking", parkable=True)
            self.assertEqual(fleet.park("waking").status, 200)
            # Mid-HIBERNATING: the checkpoint wrote its image but never returned.
            node.arm_fault("checkpoint", "hang-after", sandbox_id="capturing")
            park = in_background(lambda: fleet.park("capturing"))
            node.wait_hung("checkpoint")
            # Mid-RESTORING: the restore never ran.
            node.arm_fault("restore", "hang", sandbox_id="waking")
            wake = in_background(lambda: fleet.wake("waking", generation=1))
            node.wait_hung("restore")

            node.reboot()
            park.join(15)
            wake.join(15)
            fleet.heartbeat()
            # A capture without COMPLETE is lost; a restore rolls back to an
            # intact park, which the new boot keeps.
            self.assert_rebooted(fleet.request("GET", "/v1/sandboxes/capturing"), "capturing")
            self.wait_reaped(node, "capturing")
            self.assertEqual(fleet.wake("waking", generation=1).status, 200)
            self.assertEqual(fleet.exec("waking", ["true"]).exit_code, 0)

    def test_registration_conflict_is_a_definite_409(self):
        with LocalFleet() as fleet:
            fleet.create("ghost")
            # The gateway forgets a route whose registration the worker keeps.
            fleet.gateway.RequestHandlerClass.routing_store.delete_sandbox("ghost")
            again = fleet.request("POST", "/v1/sandboxes", payload={"id": "ghost", **SPEC}, token="sandbox")
            self.assertEqual(again.status, 409, again.body)
            self.assertEqual({key: again.json()[key] for key in ("error_code", "retryable")},
                             {"error_code": "sandbox_registration_conflict", "retryable": False})
            self.assertIsNone(fleet.route("ghost"))

    def test_quarantined_worker_serves_existing_work_and_admits_none(self):
        with LocalFleet() as fleet:
            node = fleet.nodes[0]
            for name in ("alpha", "beta", "omega"):
                fleet.create(name)
            fleet.create("resting", parkable=True)
            self.assertEqual(fleet.park("resting").status, 200)
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
            # Its boot is unchanged, so it still owns and wakes its own parks.
            self.assertEqual(fleet.wake("resting", generation=1).status, 200)

            # A same-boot probe with complete inventory retires beta by the
            # normal rules, so one vanished sandbox cannot pin quarantine.
            suspended_at = utc_now() - timedelta(minutes=5)
            job, heartbeat, retired = fleet.continuity_cycle(node, interrupted_at=suspended_at)
            self.assertEqual(([r.sandbox_id for r in retired], job.phase.value),
                             (["beta"], "running"))
            self.assertIsNone(fleet.route("beta"))
            self.assertNotIn(QUARANTINE_REASON, heartbeat.labels)
            self.assertEqual(fleet.create("gamma")["state"], "running")
            # That suspension is verified: a failing probe does not quarantine
            # it again every cycle. A newer suspension does.
            node.stop()
            for interrupted_at, quarantined in ((suspended_at, None),
                                                (utc_now(), "provider_readiness_unverified")):
                job, heartbeat, _ = fleet.continuity_cycle(node, interrupted_at=interrupted_at)
                self.assertEqual(heartbeat.labels.get(QUARANTINE_REASON), quarantined)
                self.assertEqual(job.phase.value, "unavailable" if quarantined else "running")

    def test_one_proven_reboot_does_not_pin_quarantine(self):
        # A UCloud power-off is quarantined before the new boot reports. Once
        # ingest has retired the old boot, quarantine fences the new one, so
        # the rebooted worker rejoins the pool instead of leaking forever.
        with LocalFleet() as fleet:
            node = fleet.nodes[0]
            fleet.create("alpha")
            fleet.create("resting", parkable=True)
            self.assertEqual(fleet.park("resting").status, 200)
            _, heartbeat, _ = fleet.continuity_cycle(node, state="SUSPENDED")
            self.assertEqual(heartbeat.labels[QUARANTINE_REASON], "provider_readiness_unverified")
            node.reboot()
            fleet.heartbeat()
            job, heartbeat, _ = fleet.continuity_cycle(node, interrupted_at=utc_now())
            self.assertEqual((job.phase.value, heartbeat.labels.get(QUARANTINE_REASON)),
                             ("running", None))
            self.assertEqual(fleet.create("beta")["state"], "running")


if __name__ == "__main__":
    unittest.main()
