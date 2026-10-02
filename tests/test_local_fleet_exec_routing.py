"""S4: signed exec session routing and loss, over the local fleet harness.

Tier: contract. A worker that honors the gateway's signed prefix names each
session ``xr1.<route>.<random>``; the gateway then routes polls, stdin and
signals by heartbeat alone. When that worker is silent the gateway answers
for it: 503 while the incarnation is still its own, 404 once deleted or
replaced, 410 once the incarnation was lost. A fresh worker is the authority
on its own sessions. Workers without the prefix keep the durable exec route.
"""

import signal
import unittest

from tests.harness import LocalFleet
from ucloud_sandboxes import routing


def _exec_paths(node) -> list[tuple[str, str]]:
    return [request for request in node.requests if request[1].startswith("/v1/exec/")]


class SignedExecSessionTests(unittest.TestCase):
    def test_signed_session_routes_by_heartbeat_without_routing_reads(self):
        with LocalFleet() as fleet:
            fleet.create("alpha")
            started = fleet.start_exec(
                "alpha", ["/bin/sh", "-c", "echo ready; read x; echo got $x; exec sleep 30"],
                stdin=True, initial_wait_seconds=None,
            )
            self.assertEqual(started.status, 201, started.body)
            session_id = started.json()["session"]["id"]
            self.assertTrue(session_id.startswith("xr1."), session_id)
            self.assertIsNone(routing.open_routing_store(fleet.routing_file).get_exec(session_id))

            with fleet.routing_calls() as calls:
                fleet.wait_output(session_id, "ready\n")
                self.assertEqual(fleet.exec_stdin(session_id, "v\n").status, 200)
                fleet.wait_output(session_id, "ready\ngot v\n")
                self.assertEqual(fleet.exec_signal(session_id, int(signal.SIGTERM)).status, 200)
                result = fleet.wait_exec(session_id)
            self.assertEqual(result.exit_code, 128 + signal.SIGTERM)
            self.assertEqual(calls, [])

    def test_answers_after_silence_deletion_replacement_and_loss(self):
        with LocalFleet() as fleet:
            node = fleet.nodes[0]
            fleet.create("alpha")
            first = fleet.start_exec("alpha", ["sleep", "30"]).json()["session"]["id"]

            # Silent, but the incarnation is still this worker's: retryable,
            # and nothing reaches the worker.
            fleet.expire_heartbeat(node)
            before = len(_exec_paths(node))
            for response in (
                fleet.events(first, after=0, wait_seconds=0),
                fleet.exec_stdin(first, "x"),
                fleet.exec_signal(first, int(signal.SIGTERM)),
            ):
                self.assertEqual(response.status, 503, response.body)
                self.assertEqual(response.json()["error_code"], "sandbox_worker_unreachable")
                self.assertTrue(response.json()["retryable"])
                self.assertEqual(response.headers["Retry-After"], "1")
            self.assertEqual(len(_exec_paths(node)), before)
            fleet.heartbeat()
            self.assertEqual(fleet.events(first, after=0, wait_seconds=0).status, 200)

            # Deleted: the fresh worker still replays the killed command's
            # events, even though its inventory is now empty; once silent,
            # the gateway knows the incarnation is gone.
            self.assertEqual(fleet.delete("alpha").status, 200)
            fleet.heartbeat()
            replay = fleet.events(first, after=0, wait_seconds=0)
            self.assertEqual(replay.status, 200, replay.body)
            exit_event = replay.json()["events"][-1]
            self.assertEqual(exit_event["stream"], "exit")
            self.assertNotEqual(exit_event["exit_code"], 0)
            fleet.expire_heartbeat(node)
            gone = fleet.events(first, after=0, wait_seconds=0)
            self.assertEqual((gone.status, gone.json()), (404, {"error": "exec route not found", "retryable": False}))

            # Replaced: the old generation's session is unknown, the new one
            # is retryable while its worker is silent.
            fleet.heartbeat()
            fleet.create("alpha")
            self.assertEqual(fleet.route("alpha").generation, 2)
            second = fleet.start_exec("alpha", ["sleep", "30"]).json()["session"]["id"]
            fleet.expire_heartbeat(node)
            self.assertEqual(fleet.events(first, after=0, wait_seconds=0).status, 404)
            self.assertEqual(fleet.events(second, after=0, wait_seconds=0).status, 503)

            # Lost with its boot: the rebooted worker does not know the
            # session, and once it is silent the gateway reports the loss.
            node.reboot()
            fleet.heartbeat()
            self.assertIsNone(fleet.route("alpha"))
            unknown = fleet.events(second, after=0, wait_seconds=0)
            self.assertEqual(unknown.status, 404, unknown.body)
            self.assertIn("exec session not found", unknown.json()["error"])
            fleet.expire_heartbeat(node)
            lost = fleet.events(second, after=0, wait_seconds=0)
            self.assertEqual(lost.status, 410, lost.body)
            self.assertEqual(
                {key: lost.json()[key] for key in ("error_code", "retryable", "sandbox_id", "sandbox_generation")},
                {"error_code": "exec_worker_lost", "retryable": False, "sandbox_id": "alpha",
                 "sandbox_generation": 2},
            )
            self.assertTrue(lost.json()["lost_at"])

    def test_unsigned_worker_sessions_keep_the_durable_route(self):
        with LocalFleet() as fleet:
            node = fleet.nodes[0]
            node.honor_exec_session_prefix = False
            store = routing.open_routing_store(fleet.routing_file)
            fleet.create("alpha")
            started = fleet.start_exec("alpha", ["/bin/sh", "-c", "echo ready; exec sleep 30"],
                                       initial_wait_seconds=None)
            session_id = started.json()["session"]["id"]
            self.assertTrue(session_id.startswith("exec-"), session_id)
            self.assertEqual(store.get_exec(session_id).sandbox_id, "alpha")
            with fleet.routing_calls() as calls:
                fleet.wait_output(session_id, "ready\n")
            self.assertIn("get_exec", calls)

            # A silent worker keeps its durable session: retryable, unproxied.
            fleet.expire_heartbeat(node)
            before = len(_exec_paths(node))
            silent = fleet.events(session_id, after=0, wait_seconds=0)
            self.assertEqual((silent.status, silent.json()["error_code"]), (503, "sandbox_worker_unreachable"))
            self.assertEqual(len(_exec_paths(node)), before)
            self.assertIsNotNone(store.get_exec(session_id))
            self.assertIsNone(store.get_exec_loss(session_id))
            fleet.heartbeat()

            # Deletion removes the durable row: the gateway answers alone.
            self.assertEqual(fleet.delete("alpha").status, 200)
            self.assertIsNone(store.get_exec(session_id))
            before = len(_exec_paths(node))
            gone = fleet.events(session_id, after=0, wait_seconds=0)
            self.assertEqual((gone.status, gone.json()), (404, {"error": "exec route not found", "retryable": False}))
            self.assertEqual(len(_exec_paths(node)), before)

            # A retired boot records the loss, so it is final even though the
            # rebooted worker is fresh.
            fleet.create("alpha")
            survivor = fleet.start_exec("alpha", ["sleep", "30"]).json()["session"]["id"]
            node.reboot()
            fleet.heartbeat()
            before = len(_exec_paths(node))
            for response in (
                fleet.request("GET", f"/v1/exec/{survivor}", token="sandbox"),
                fleet.events(survivor, after=0, wait_seconds=0),
                fleet.exec_stdin(survivor, "x"),
                fleet.exec_signal(survivor, 15),
            ):
                self.assertEqual(response.status, 410, response.body)
                self.assertEqual(
                    (response.json()["error_code"], response.json()["retryable"],
                     response.json()["sandbox_generation"]),
                    ("exec_worker_lost", False, 2),
                )
            self.assertEqual(len(_exec_paths(node)), before)
            unknown = fleet.events("exec-" + "0" * 32, after=0, wait_seconds=0)
            self.assertEqual((unknown.status, unknown.json()), (404, {"error": "exec route not found", "retryable": False}))


if __name__ == "__main__":
    unittest.main()
