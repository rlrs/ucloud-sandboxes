"""End-to-end scenarios over the local fleet harness (tests/harness).

Tier: contract. Every request goes through the real gateway and node-agent
HTTP servers; the node runs the real direct service, provisioner, Warden,
overlay manager and storage-native daemon. Only the runtime binary, overlay
mounts, block devices and image source are fakes; see tests/harness/README.md.
"""

import io
import signal
import tarfile
import unittest

from tests.harness import LocalFleet, process_alive
from tests.harness.fleet import _request


class LocalFleetScenarioTests(unittest.TestCase):
    def _create_exec_files_delete(self, fleet: LocalFleet) -> None:
        created = fleet.create("alpha")
        self.assertEqual(created["state"], "running")
        node = fleet.node_for("alpha")
        pid = node.sentry_pid("alpha")
        self.assertTrue(process_alive(pid))

        image = fleet.exec("alpha", ["cat", "/opt/greeting"])
        self.assertEqual((image.exit_code, image.stdout), (0, "hello from the image\n"))
        uploaded = fleet.write_file("alpha", "/workspace/notes.txt", b"uploaded\x00bytes")
        self.assertEqual(uploaded.status, 200, uploaded.body)
        downloaded = fleet.read_file("alpha", "/workspace/notes.txt")
        self.assertEqual((downloaded.status, downloaded.body), (200, b"uploaded\x00bytes"))
        generated = fleet.exec(
            "alpha", ["/bin/sh", "-c", "wc -c < notes.txt > size.txt; echo oops >&2; exit 3"],
            working_dir="/workspace",
        )
        self.assertEqual((generated.exit_code, generated.stdout, generated.stderr), (3, "", "oops\n"))
        self.assertEqual(fleet.read_file("alpha", "/workspace/size.txt").body.strip(), b"14")
        # Current contract: a missing file is indistinguishable from a failed
        # helper and surfaces as 503, not 404.
        missing = fleet.read_file("alpha", "/workspace/absent.txt")
        self.assertEqual(
            (missing.status, missing.json()), (503, {"error": "sandbox file read failed with exit 1"})
        )

        status = fleet.status("alpha").json()["sandboxes"]
        self.assertEqual([(item["id"], item["state"]) for item in status], [("alpha", "running")])

        deleted = fleet.delete("alpha")
        self.assertEqual(deleted.status, 200, deleted.body)
        self.assertIsNone(fleet.route("alpha"))
        self.assertFalse(process_alive(pid))
        self.assertEqual(node.live_sentries(), [])
        self.assertIsNone(node.runsc_state("alpha"))
        self.assertFalse((node.proc_root / str(pid)).exists())
        self.assertEqual(list(node.volumes.iterdir()), [])
        self.assertEqual(node.storage_backend.devices, {})
        self.assertEqual(list((node.state_root / "bundles").iterdir()), [])
        self.assertEqual(list((node.state_root / "journals").iterdir()), [])
        self.assertEqual(list((node.root / "mounts").iterdir()), [])
        fleet.heartbeat()
        self.assertEqual(fleet.status("alpha").json()["sandboxes"], [])
        self.assertEqual(fleet.request("GET", "/v1/sandboxes/alpha").status, 404)

    def test_create_exec_files_delete(self):
        with LocalFleet() as fleet:
            self._create_exec_files_delete(fleet)

    def test_create_exec_files_delete_with_postgres_routing(self):
        with LocalFleet(postgres=True) as fleet:
            self._create_exec_files_delete(fleet)

    def test_archive_upload_writes_a_harness_in_one_request(self):
        def archive(*members) -> bytes:
            buffer = io.BytesIO()
            with tarfile.open(fileobj=buffer, mode="w:gz") as tar:
                for name, data, kind, mode in members:
                    info = tarfile.TarInfo(name)
                    info.type, info.mode, info.size, info.linkname = kind, mode, len(data), "/etc/passwd"
                    tar.addfile(info, io.BytesIO(data))
            return buffer.getvalue()

        with LocalFleet() as fleet:
            fleet.create("alpha")
            path = "/v1/sandboxes/alpha/archive?path=/workspace/harness"
            harness = archive(("run.sh", b"echo ran\n", tarfile.REGTYPE, 0o755),
                              ("lib/notes.txt", b"notes", tarfile.REGTYPE, 0o644))
            uploaded = fleet.request("PUT", path, body=harness, token="sandbox")
            self.assertEqual((uploaded.status, uploaded.json()["files"]), (200, 2), uploaded.body)
            result = fleet.exec("alpha", ["/bin/sh", "-c", "./run.sh; cat lib/notes.txt; stat -c %a run.sh"],
                                working_dir="/workspace/harness")
            self.assertEqual((result.exit_code, result.stdout), (0, "ran\nnotes755\n"))
            unsafe = fleet.request("PUT", path, body=archive(("link", b"", tarfile.SYMTYPE, 0o777)), token="sandbox")
            self.assertEqual(unsafe.status, 400, unsafe.body)
            # The gateway names the generation it routed to; a replaced one conflicts.
            node = fleet.node_for("alpha")
            stale = _request(node.url, "PUT", path, fleet.tokens.node_control, body=harness,
                             headers={"X-UCloud-Sandbox-Generation": "99"})
            self.assertEqual(stale.status, 409, stale.body)

    def test_exec_session_events_stdin_and_signal(self):
        with LocalFleet() as fleet:
            fleet.create("beta")
            started = fleet.start_exec(
                "beta", ["/bin/sh", "-c", "echo first; sleep 0.3; echo second >&2; exit 7"]
            )
            self.assertEqual(started.status, 201, started.body)
            payload = started.json()
            session_id = payload["session"]["id"]
            self.assertIn("events", payload)
            # Polls, stdin and signals below must all reach the owning worker,
            # however the gateway routes a session (durable row or signed ID).
            result = fleet.wait_exec(session_id, payload["events"])
            self.assertEqual((result.exit_code, result.stdout, result.stderr), (7, "first\n", "second\n"))
            sequences = [event["sequence"] for event in result.events]
            self.assertEqual(sequences, list(range(sequences[0], sequences[0] + len(sequences))))
            self.assertEqual(result.events[-1]["stream"], "exit")
            replay = fleet.events(session_id, after=0, wait_seconds=0).json()["events"]
            self.assertEqual([event["sequence"] for event in replay], sequences)

            # Without an initial wait the start response carries no events.
            unwaited = fleet.start_exec("beta", ["echo", "later"], initial_wait_seconds=None)
            self.assertEqual(unwaited.status, 201, unwaited.body)
            self.assertNotIn("events", unwaited.json())
            later = fleet.wait_exec(unwaited.json()["session"]["id"])
            self.assertEqual((later.exit_code, later.stdout), (0, "later\n"))

            reader = fleet.start_exec("beta", ["cat"], stdin=True)
            self.assertEqual(reader.status, 201, reader.body)
            reader_id = reader.json()["session"]["id"]
            written = fleet.exec_stdin(reader_id, "ping\n", eof=True)
            self.assertEqual(written.status, 200, written.body)
            echoed = fleet.wait_exec(reader_id)
            self.assertEqual((echoed.exit_code, echoed.stdout), (0, "ping\n"))

            sleeper = fleet.start_exec("beta", ["/bin/sh", "-c", "echo ready; exec sleep 30"])
            sleeper_id = sleeper.json()["session"]["id"]
            fleet.wait_output(sleeper_id, "ready\n", sleeper.json().get("events", ()))
            signalled = fleet.exec_signal(sleeper_id, int(signal.SIGTERM))
            self.assertEqual(signalled.status, 200, signalled.body)
            stopped = fleet.wait_exec(sleeper_id, timeout=5)
            # runsc exec forwards the signal and exits like a shell would.
            self.assertEqual(stopped.exit_code, 128 + signal.SIGTERM)

    def test_park_and_wake_preserve_disk_and_memory_state(self):
        with LocalFleet() as fleet:
            fleet.create("gamma", parkable=True)
            node = fleet.node_for("gamma")
            first = node.sentry_pid("gamma")
            cid = node.container_id("gamma")
            self.assertEqual(fleet.write_file("gamma", "/workspace/disk.txt", b"disk").status, 200)
            self.assertEqual(fleet.write_file("gamma", "/tmp/memory.txt", b"memory").status, 200)
            self.assertTrue((node.runtime_root / "fake-memory" / cid).is_dir())

            parked = fleet.park("gamma")
            self.assertEqual(parked.status, 200, parked.body)
            self.assertEqual(fleet.route("gamma").state, "parked")
            self.assertFalse(process_alive(first))
            self.assertIsNone(node.runsc_state("gamma"))
            # Memory exists only in the checkpoint; the volume is released, so
            # its mountpoint is empty and the overlay is unmounted.
            self.assertFalse((node.runtime_root / "fake-memory" / cid).exists())
            self.assertEqual(list((node.volumes / "gamma.sandbox-1").iterdir()), [])
            self.assertEqual(list((node.root / "mounts").iterdir()), [])

            woken = fleet.wake("gamma", generation=fleet.route("gamma").generation)
            self.assertEqual(woken.status, 200, woken.body)
            second = node.sentry_pid("gamma")
            self.assertNotEqual(second, first)
            self.assertEqual(fleet.read_file("gamma", "/workspace/disk.txt").body, b"disk")
            self.assertEqual(fleet.read_file("gamma", "/tmp/memory.txt").body, b"memory")

            # A second cycle wakes implicitly on exec, from the second checkpoint.
            self.assertEqual(fleet.write_file("gamma", "/run/second.txt", b"again").status, 200)
            self.assertEqual(fleet.park("gamma").status, 200)
            result = fleet.exec("gamma", ["cat", "/tmp/memory.txt", "/run/second.txt", "/workspace/disk.txt"])
            self.assertEqual((result.exit_code, result.stdout), (0, "memoryagaindisk"))
            self.assertNotIn(node.sentry_pid("gamma"), {first, second})

    def test_lost_runtime_is_quarantined_reported_and_deletable(self):
        with LocalFleet() as fleet:
            fleet.create("delta")
            fleet.create("bystander")
            node = fleet.node_for("delta")
            node.kill_sentry("delta")

            def reported() -> dict[str, str]:
                inventory = node.post_heartbeat()["node"]["inventory"]
                return {item["sandbox_id"]: item["state"] for item in inventory}

            # Heartbeat inventory reads the journal without probing liveness,
            # so the loss is found by the next request that touches it.
            self.assertEqual(reported()["delta"], "running")
            refused = fleet.start_exec("delta", ["true"])
            self.assertEqual(refused.status, 503, refused.body)
            self.assertIn("recovery-required", refused.json()["error"])
            self.assertNotIn("retryable", refused.json())
            self.assertEqual(reported(), {"delta": "recovery-required", "bystander": "running"})
            # Recovery states prove presence but never become route state, so
            # the gateway's status view keeps reporting the lost runtime.
            self.assertEqual(fleet.route("delta").state, "running")
            status = fleet.status("delta").json()["sandboxes"]
            self.assertEqual([(item["id"], item["state"]) for item in status], [("delta", "running")])
            self.assertEqual(fleet.exec("bystander", ["true"]).exit_code, 0)

            deleted = fleet.delete("delta")
            self.assertEqual(deleted.status, 200, deleted.body)
            self.assertIsNone(fleet.route("delta"))
            self.assertIsNone(node.runsc_state("delta"))
            self.assertEqual(sorted(path.name for path in node.volumes.iterdir()), ["bystander.sandbox-1"])

    def test_node_agent_restart_keeps_running_and_parked_sandboxes(self):
        with LocalFleet() as fleet:
            fleet.create("running")
            fleet.create("resting", parkable=True)
            self.assertEqual(fleet.write_file("running", "/workspace/a.txt", b"live").status, 200)
            self.assertEqual(fleet.write_file("resting", "/tmp/b.txt", b"asleep").status, 200)
            self.assertEqual(fleet.park("resting").status, 200)
            node = fleet.nodes[0]
            pid = node.sentry_pid("running")

            node.restart()
            fleet.heartbeat()

            # The new agent re-verifies and keeps the surviving sentry from
            # durable state, and restores the parked one from its journal.
            self.assertEqual(fleet.exec("running", ["cat", "/workspace/a.txt"]).stdout, "live")
            self.assertEqual(node.sentry_pid("running"), pid)
            self.assertEqual(fleet.exec("resting", ["cat", "/tmp/b.txt"]).stdout, "asleep")
            self.assertEqual(
                sorted((item["id"], item["state"]) for item in fleet.status("running").json()["sandboxes"]),
                [("running", "running")],
            )


if __name__ == "__main__":
    unittest.main()
