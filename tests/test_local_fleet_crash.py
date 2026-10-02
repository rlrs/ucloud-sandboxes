"""S2: node-agent crash and restart replay, over the local fleet harness.

Tier: contract. Node agents run as their own processes
(``LocalFleet(node_processes=True)``) and crash by SIGKILL while a create,
delete, park or wake is blocked inside a fake runtime or storage boundary.
The in-flight invocation then dies too (or, for the storage daemon, which
outlives the agent, completes). The restarted agent replays the registry
and the Warden journal: creates finish exactly once with no orphan sentry,
deletes complete, abandoned captures resume and leave no checkpoint,
restores settle, and a sentry that died while the agent was down is
quarantined. The gateway reports each crash as a retryable transport error
and recovers the route from the next heartbeat or the client's retry.

Not covered: a crash between the Warden journal commit and the registry's
owned commit, which has no fake boundary to block in.
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


class AgentCrashReplayTests(unittest.TestCase):
    def assert_transport_error(self, response) -> None:
        self.assertEqual(response.status, 502, response.body)
        self.assertEqual((response.json()["code"], response.json()["retryable"]), ("node_transport_error", True))

    def test_create_replays_after_a_crash_in_each_phase(self):
        with LocalFleet(node_processes=True) as fleet:
            node = fleet.nodes[0]
            fleet.create("bystander")
            survivors = {"bystander": node.sentry_pid("bystander")}
            for name, phase, block, unblock in (
                # Storage prepares the volume; the registry still says planned.
                ("planned", "planned",
                 lambda name: node.storage_host.hold("mount"), lambda hold: hold.release.set()),
                # The overlay mount runs. Quota is prepared but commits only with
                # the rootfs (C5.2), so the registry still says planned.
                ("quota", "planned",
                 lambda name: node.arm_fault("mount", "hang"), lambda _: node.kill_hung()),
                # runsc create made a sentry, but nothing journaled it.
                ("created", "rootfs_ready",
                 lambda name: node.arm_fault("create", "hang-after", sandbox_id=name), lambda _: node.kill_hung()),
                # The container exists but was never started.
                ("unstarted", "rootfs_ready",
                 lambda name: node.arm_fault("start", "hang", sandbox_id=name), lambda _: node.kill_hung()),
            ):
                with self.subTest(phase=name):
                    blocker = block(name)
                    thread, result = in_background(lambda name=name: fleet.request(
                        "POST", "/v1/sandboxes", payload={"id": name, **SPEC}, token="sandbox"))
                    if name == "planned":
                        self.assertTrue(blocker.reached.wait(15))
                    else:
                        node.wait_hung({"quota": "mount", "created": "create", "unstarted": "start"}[name])
                    self.assertEqual(node.registration(name).phase, phase)
                    orphan = node.sentry_pid(name) if node.runsc_state(name) is not None else None

                    node.crash()
                    thread.join(15)
                    self.assert_transport_error(result["response"])
                    self.assertEqual(fleet.route(name).state, "creating")
                    unblock(blocker)
                    node.start()

                    self.assertEqual(node.registration(name).phase, "owned")
                    # Replay reuses the volume storage prepared before the crash.
                    self.assertEqual(len([device for device in node.storage_backend.devices.values()
                                          if device.volume_root.name == f"{name}.sandbox-1"]), 1)
                    pid = node.sentry_pid(name)
                    if orphan is not None:
                        self.assertNotEqual(pid, orphan)
                        self.assertFalse(process_alive(orphan))
                    survivors[name] = pid
                    self.assertEqual(sorted(node.live_sentries()), sorted(survivors.values()))
                    fleet.heartbeat()
                    self.assertEqual(fleet.route(name).state, "running")
                    retried = fleet.request("POST", "/v1/sandboxes", payload={"id": name, **SPEC}, token="sandbox")
                    self.assertEqual((retried.status, retried.json()["recovered"]), (200, True))
                    self.assertEqual(fleet.route(name).generation, 1)
                    self.assertEqual(fleet.exec(name, ["cat", "/opt/greeting"]).stdout, "hello from the image\n")
            self.assertEqual(node.sentry_pid("bystander"), survivors["bystander"])

    def test_delete_completes_and_a_dead_sentry_is_quarantined_at_restart(self):
        with LocalFleet(node_processes=True) as fleet:
            node = fleet.nodes[0]
            for name in ("doomed", "orphaned", "healthy"):
                fleet.create(name)
            doomed, healthy = node.sentry_pid("doomed"), node.sentry_pid("healthy")
            node.arm_fault("delete", "hang", sandbox_id="doomed")
            thread, result = in_background(lambda: fleet.delete("doomed"))
            node.wait_hung("delete")
            self.assertEqual(node.registration("doomed").phase, "deleting")

            node.crash()
            thread.join(15)
            self.assert_transport_error(result["response"])
            node.kill_hung()
            # Its runtime dies while no agent is watching.
            node.kill_sentry("orphaned")
            node.start()

            # Durable deletion finishes at startup.
            self.assertIsNone(node.registration("doomed"))
            self.assertFalse(process_alive(doomed))
            self.assertNotIn("doomed.sandbox-1",
                             [device.volume_root.name for device in node.storage_backend.devices.values()])
            inventory = {item["sandbox_id"]: item["state"] for item in node.post_heartbeat()["node"]["inventory"]}
            self.assertEqual(inventory, {"orphaned": "recovery-required", "healthy": "running"})
            self.assertIsNone(fleet.route("doomed"))
            again = fleet.delete("doomed")
            self.assertEqual((again.status, again.json()), (200, {"ok": True, "deleted": False}))

            # Quarantine refuses traffic but blocks neither its neighbour nor its deletion.
            refused = fleet.start_exec("orphaned", ["true"])
            self.assertEqual(refused.status, 503, refused.body)
            self.assertIn("recovery-required", refused.json()["error"])
            self.assertEqual(fleet.exec("healthy", ["cat", "/opt/greeting"]).exit_code, 0)
            self.assertEqual(node.sentry_pid("healthy"), healthy)
            self.assertEqual(fleet.delete("orphaned").status, 200)
            self.assertIsNone(node.registration("orphaned"))
            self.assertEqual(node.live_sentries(), [healthy])
            self.assertEqual([device.volume_root.name for device in node.storage_backend.devices.values()],
                             ["healthy.sandbox-1"])

    def test_interrupted_park_and_wake_settle_at_restart(self):
        with LocalFleet(node_processes=True) as fleet:
            node = fleet.nodes[0]
            fleet.create("resting", parkable=True)
            self.assertEqual(fleet.write_file("resting", "/workspace/disk.txt", b"disk").status, 200)
            self.assertEqual(fleet.write_file("resting", "/tmp/memory.txt", b"memory").status, 200)

            def crash_during(command: str, action: str, call) -> None:
                node.arm_fault(command, action, sandbox_id="resting")
                thread, result = in_background(call)
                node.wait_hung(command)
                node.crash()
                thread.join(15)
                self.assert_transport_error(result["response"])
                node.kill_hung()
                node.start()
                fleet.heartbeat()

            def state_intact() -> str:
                return fleet.exec("resting", ["cat", "/tmp/memory.txt", "/workspace/disk.txt"]).stdout

            # Before or after the checkpoint is written, an unfinished capture
            # is discarded and the original, paused sentry resumes.
            original = node.sentry_pid("resting")
            for action in ("hang", "hang-after"):
                with self.subTest(park=action):
                    crash_during("checkpoint", action, lambda: fleet.park("resting"))
                    self.assertEqual(fleet.route("resting").state, "running")
                    self.assertEqual(node.live_sentries(), [original])
                    self.assertEqual(list((node.volumes / "resting.sandbox-1").glob("hibernate-*")), [])
                    self.assertEqual(state_intact(), "memorydisk")

            # A restore that never ran leaves the sandbox parked for a retry;
            # one that made its sentry is adopted. Either way the checkpoint's
            # state is intact and exactly one sentry runs.
            for action, settled in (("hang", "parked"), ("hang-after", "running")):
                with self.subTest(wake=action):
                    self.assertEqual(fleet.park("resting").status, 200)
                    self.assertEqual(node.live_sentries(), [])
                    generation = fleet.route("resting").generation
                    crash_during("restore", action, lambda: fleet.wake("resting", generation=generation))
                    self.assertEqual(fleet.route("resting").state, settled)
                    self.assertEqual(len(node.live_sentries()), int(settled == "running"))
                    if settled == "parked":
                        self.assertEqual(fleet.wake("resting", generation=generation).status, 200)
                    self.assertEqual(state_intact(), "memorydisk")
                    self.assertEqual(len(node.live_sentries()), 1)


if __name__ == "__main__":
    unittest.main()
