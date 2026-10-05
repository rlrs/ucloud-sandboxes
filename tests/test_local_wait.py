"""Node-local model waits: relay flows, NFLOG parsing and the pause/thaw scheduler."""
import ipaddress
from pathlib import Path
import json
import socket
import struct
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import threading
import unittest

from ucloud_sandboxes.direct_registry import DirectRegistryConflictError
from unittest.mock import patch

from ucloud_sandboxes import local_wait
from ucloud_sandboxes.local_wait import (Flow, LocalWaitScheduler, WaitCandidate, nft_script, parse_messages,
                                         parse_packet, relay_endpoints)
from ucloud_sandboxes.node_runtime import DirectNodeRuntime
from ucloud_sandboxes.pause_tier import PauseStats
from ucloud_sandboxes.relay_network import NetworkRelay
from ucloud_sandboxes.sandbox import SandboxBusyError

TEST_TIER = "contract"
NETWORK = ipaddress.IPv4Network("100.96.0.0/16")
GUEST, RELAY = "100.96.0.3", "10.42.0.2"


def tcp(source, destination, *, payload=0, flags=0x10, sport=40000, dport=8092):
    header = struct.pack("!HHIIBBHHH", sport, dport, 1, 1, 5 << 4, flags, 65535, 0, 0)
    ip = struct.pack("!BBHHHBBH4s4s", 0x45, 0, 20 + len(header) + payload, 0, 0, 64, 6, 0,
                     socket.inet_aton(source), socket.inet_aton(destination))
    return ip + header  # Snapped: the payload itself is never logged.


def nflog(*packets):
    messages = b""
    for packet in packets:
        attribute = struct.pack("=HH", 4 + len(packet), local_wait.NFULA_PAYLOAD) + packet
        attribute += b"\0" * (-len(attribute) % 4)
        body = struct.pack("=BBH", socket.AF_INET, 0, socket.htons(local_wait.NFLOG_GROUP)) + attribute
        messages += struct.pack("=IHHII", 16 + len(body), local_wait.NFNL_SUBSYS_ULOG << 8, 0, 0, 0) + body
    return messages


class RelayFlowTests(unittest.TestCase):
    def test_only_private_literal_relays_take_the_local_path(self):
        relays = {"default": NetworkRelay.parse("default", "10.42.0.2:8092"),
                  "ingress": NetworkRelay("ingress", "relay.example.org", 443),
                  "public": NetworkRelay("public", "77.42.92.27", 443)}
        self.assertEqual(relay_endpoints(relays), (("10.42.0.2", 8092),))
        script = nft_script(relay_endpoints(relays), NETWORK)
        self.assertIn("type filter hook forward priority -150; policy accept;", script)
        self.assertIn("ip saddr 100.96.0.0/16 ip daddr 10.42.0.2 tcp dport 8092 log group 4207 snaplen 64 "
                      "queue-threshold 1", script)
        self.assertIn("ip saddr 10.42.0.2 tcp sport 8092 ip daddr 100.96.0.0/16 log group", script)
        self.assertNotIn("77.42.92.27", script)

    def test_headers_give_direction_payload_and_wake(self):
        request = parse_packet(tcp(GUEST, RELAY, payload=300, flags=0x18), NETWORK)
        self.assertEqual((request.guest, request.outbound, request.payload, request.wakes), (GUEST, True, 300, False))
        answer = parse_packet(tcp(RELAY, GUEST, payload=1368, sport=8092, dport=40000), NETWORK)
        self.assertEqual((answer.outbound, answer.payload, answer.wakes), (False, 1368, True))
        self.assertFalse(parse_packet(tcp(RELAY, GUEST, sport=8092, dport=40000), NETWORK).wakes)  # A bare ACK.
        self.assertTrue(parse_packet(tcp(RELAY, GUEST, flags=0x11, sport=8092), NETWORK).wakes)  # FIN.
        self.assertIsNone(parse_packet(tcp(RELAY, "10.42.0.9"), NETWORK))
        self.assertIsNone(parse_packet(b"\x60" + b"\0" * 39, NETWORK))  # IPv6.

    def test_netlink_messages_yield_each_logged_header(self):
        first, second = tcp(GUEST, RELAY, payload=10), tcp(RELAY, GUEST, payload=20, sport=8092)
        error = struct.pack("=IHHII", 36, local_wait.NLMSG_ERROR, 0, 1, 0) + b"\0" * 20
        self.assertEqual(parse_messages(error + nflog(first, second)), [first, second])
        self.assertEqual(parse_messages(b"\x05\0\0"), [])


class ImmediateExecutor:
    def submit(self, function, *args):
        function(*args)


class SchedulerTests(unittest.TestCase):
    def setUp(self):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.cpu = Path(directory.name) / "cpu.stat"
        self.usage(0)
        self.now, self.paused, self.calls = 100.0, set(), []
        key = ("s1", 1)

        def pause(key):
            self.calls.append(("pause", key))
            self.paused.add(key)
            for packet in self.during_pause:
                self.scheduler.observe(packet, now=self.now)

        def thaw(key):
            self.calls.append(("thaw", key))
            self.paused.discard(key)
        self.during_pause = []
        self.scheduler = LocalWaitScheduler(
            endpoints=[(RELAY, 8092)], network=NETWORK,
            candidates=lambda: [WaitCandidate(key, GUEST, str(self.cpu))],
            pause=pause, thaw=thaw, is_paused=lambda key: key in self.paused, executor=ImmediateExecutor(),
            clock=lambda: self.now)

    def usage(self, usec):
        self.cpu.write_text(f"usage_usec {usec}\nuser_usec 0\n")

    def run_for(self, seconds, step=0.01):
        end = self.now + seconds
        while self.now < end - 1e-9:
            self.scheduler.tick(now=self.now)
            self.now = round(self.now + step, 6)

    def send(self, payload=300):
        self.scheduler.observe(parse_packet(tcp(GUEST, RELAY, payload=payload), NETWORK), now=self.now)

    def answer(self, payload=900):
        self.scheduler.observe(parse_packet(tcp(RELAY, GUEST, payload=payload, sport=8092), NETWORK), now=self.now)

    def test_an_idle_outstanding_call_pauses_once_and_its_answer_thaws(self):
        self.run_for(0.02)
        self.send()
        self.run_for(0.04)
        self.assertEqual(self.calls, [])  # Not yet settled, and not yet a full idle window.
        self.run_for(0.04)
        self.assertEqual(self.calls, [("pause", ("s1", 1))])
        self.run_for(1.0)
        self.assertEqual(len(self.calls), 1)
        self.scheduler.observe(parse_packet(tcp(RELAY, GUEST, sport=8092), NETWORK), now=self.now)
        self.assertEqual(len(self.calls), 1)  # A bare ACK does not thaw.
        self.answer()
        self.assertEqual(self.calls[-1], ("thaw", ("s1", 1)))
        self.run_for(0.5)
        self.assertEqual(len(self.calls), 2)  # Answered: nothing outstanding.

    def test_a_busy_sandbox_is_not_paused(self):
        self.run_for(0.01)
        self.send()
        for _ in range(40):
            self.usage(int(self.now * 1e6))  # A background job keeps running while it waits.
            self.run_for(0.01)
        self.assertEqual(self.calls, [])
        self.run_for(0.2)
        self.assertEqual(self.calls, [("pause", ("s1", 1))])  # Once it settles.

    def test_an_answer_before_the_decision_or_during_the_pause_leaves_it_running(self):
        self.send()
        self.run_for(0.03)
        self.answer()
        self.run_for(0.5)
        self.assertEqual(self.calls, [])
        self.send()
        self.during_pause = [parse_packet(tcp(RELAY, GUEST, payload=50, sport=8092), NETWORK)]
        self.run_for(0.2)
        self.assertEqual(self.calls, [("pause", ("s1", 1)), ("thaw", ("s1", 1))])
        self.assertEqual(self.paused, set())

    def test_a_pause_landing_on_an_answered_call_is_undone(self):
        # A status read thawed the paused sandbox (keep_paused), the answer
        # arrived meanwhile, then the read paused it again with the answer inside.
        self.send()
        self.run_for(0.2)
        self.paused.discard(("s1", 1))  # The read's thaw.
        self.answer()
        self.assertEqual(self.calls, [("pause", ("s1", 1))])
        self.paused.add(("s1", 1))  # The read's re-pause.
        self.run_for(0.02)
        self.assertEqual(self.calls[-1], ("thaw", ("s1", 1)))
        self.send()  # The next request consumes the watch.
        self.run_for(0.02)
        self.paused.add(("s1", 1))
        self.run_for(0.03)
        self.assertEqual(self.calls[-1], ("thaw", ("s1", 1)))  # Nothing new until the policy pauses again.

    def test_a_failed_thaw_of_an_answered_call_is_retried(self):
        self.send()
        self.run_for(0.2)
        failures = []

        def refuse(key):
            failures.append(key)
            raise RuntimeError("busy: a status read holds the request lock")
        thaw, self.scheduler._thaw = self.scheduler._thaw, refuse
        with self.assertLogs("ucloud_sandboxes.local_wait", "WARNING"):
            self.answer()
            self.run_for(0.03)
        self.assertEqual(len(failures), 4)  # The packet's attempt, then every tick.
        self.scheduler._thaw = thaw
        self.run_for(0.01)
        self.assertEqual((self.calls[-1], self.paused), (("thaw", ("s1", 1)), set()))

    def test_an_answered_call_reports_itself_to_escalation(self):
        self.send()
        self.run_for(0.2)
        self.assertFalse(self.scheduler.answered(("s1", 1), now=self.now))
        self.answer()
        self.assertTrue(self.scheduler.answered(("s1", 1), now=self.now))
        self.assertFalse(self.scheduler.answered(("s1", 1), now=self.now + local_wait.ANSWERED_WATCH_SECONDS))

    def test_flows_follow_the_candidates(self):
        self.send()
        self.scheduler._candidates = lambda: []
        self.run_for(0.2)
        self.assertEqual((self.calls, self.scheduler.flows), ([], {}))

    def test_cpu_window_needs_its_full_span(self):
        flow = Flow()
        flow.sample(0.0, 10)
        flow.sample(0.03, 10)
        self.assertFalse(flow.idle(0.03))
        flow.sample(0.06, 10)
        self.assertTrue(flow.idle(0.06))
        flow.sample(0.07, 5000)
        self.assertFalse(flow.idle(0.07))


class RuntimeTests(unittest.TestCase):
    def runtime(self, *, local_model_waits=True, managed=True):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        bundle = Path(directory.name)
        (bundle / "config.json").write_text(json.dumps({"linux": {"cgroupsPath": "/ucloud-sandboxes/abc"}}))
        warden = SimpleNamespace(config=SimpleNamespace(pause_tier=True, application_memory_root=None,
                                                        local_model_waits=local_model_waits),
                                 paused=set(), pause_stats=PauseStats())
        warden.is_paused = lambda sandbox_id, generation: (sandbox_id, generation) in warden.paused
        registration = SimpleNamespace(
            phase="owned", sandbox_id="agent", sandbox_generation=1,
            spec=SimpleNamespace(parkable=True, managed_process=managed),
            to_direct_sandbox=lambda: SimpleNamespace(bundle=bundle))
        registry = SimpleNamespace(
            load_drain=lambda: SimpleNamespace(draining=False), get=lambda _id: registration,
            snapshot=lambda: SimpleNamespace(records=(registration,)))
        parks = []

        def park(sandbox_id, *, operation_id, pause):
            parks.append((operation_id.split("-")[0], pause))
            warden.paused.add((sandbox_id, 1))
        network = SimpleNamespace(relays={"default": NetworkRelay.parse("default", "10.42.0.2:8092")},
                                  leases=lambda: {("agent", 1): SimpleNamespace(guest_ip=GUEST)})
        service = SimpleNamespace(
            warden=warden, provisioner=SimpleNamespace(registry=registry, network_manager=network),
            open_admission=lambda: None, close_admission=lambda: None, idle_park_seconds=0, park=park,
            advance_lifecycle_activity_revision=lambda: 7)
        return DirectNodeRuntime(service), parks

    def test_a_model_wait_pauses_through_the_pause_tier_and_fails_fast_on_activity(self):
        runtime, parks = self.runtime()
        runtime.pause_model_wait(("agent", 1))
        runtime.pause_model_wait(("agent", 1))  # Already paused: nothing more.
        self.assertEqual(parks, [("local", True)])
        self.assertIn(("agent", 1), runtime._paused)  # Reclaim and escalation see it.
        runtime.pause_model_wait(("agent", 2))  # Another incarnation: never.
        self.assertEqual(len(parks), 1)
        runtime.service.warden.paused.clear()
        with runtime.lifecycle.exclusive("agent"), self.assertRaises(SandboxBusyError):
            runtime.pause_model_wait(("agent", 1))  # Another lifecycle operation holds it: skip, never wait.

    def test_a_local_wait_suspends_the_growth_forecast_and_its_thaw_resumes_it(self):
        runtime, _ = self.runtime()
        calls = []
        runtime.service.observe_managed_wait = lambda *args: calls.append(("wait", *args))
        runtime.service.resume_managed_continuation = lambda *args: calls.append(("resume", *args))
        runtime.wake_with_activity_revision = lambda *args, **kwargs: calls.append(("wake", args[0]))
        written = threading.Event()  # A slow registry write blocks neither the pause nor the thaw.
        runtime.service.observe_managed_wait = lambda *args: (written.wait(5), calls.append(("wait", *args)))
        runtime.pause_model_wait(("agent", 1))
        request_id = runtime._paused[("agent", 1)].local_request_id
        self.assertTrue(request_id.startswith("local-wait-"))
        runtime._thaw_model_wait(("agent", 1))
        self.assertEqual(calls, [("wake", "agent")])
        written.set()
        runtime._growth_executor.shutdown(wait=True)  # In order: the wait before its resume.
        self.assertEqual(calls, [("wake", "agent"), ("wait", "agent", 1, request_id),
                                 ("resume", "agent", 1, request_id)])
        def refuse(*_args):
            raise DirectRegistryConflictError("growth wait was superseded by wake")
        runtime._growth_executor = None
        runtime.service.resume_managed_continuation = refuse  # Bookkeeping never fails a thaw.
        runtime._thaw_model_wait(("agent", 1))
        runtime._growth_executor.shutdown(wait=True)

    def test_candidates_are_managed_sandboxes_with_their_lease_and_cgroup(self):
        runtime, _ = self.runtime()
        self.assertEqual(runtime._local_wait_candidates(),
                         [WaitCandidate(("agent", 1), GUEST, "/sys/fs/cgroup/ucloud-sandboxes/abc/cpu.stat")])
        runtime, _ = self.runtime(managed=False)
        self.assertEqual(runtime._local_wait_candidates(), [])

    def test_the_scheduler_runs_only_with_the_switch(self):
        with patch.object(LocalWaitScheduler, "start", lambda scheduler: scheduler):
            runtime, _ = self.runtime(local_model_waits=False)
            runtime._start_local_waits()
            self.assertIsNone(runtime._local_waits)
            runtime, _ = self.runtime()
            runtime._start_local_waits()
            self.assertEqual(runtime._local_waits.endpoints, (("10.42.0.2", 8092),))


if __name__ == "__main__":
    unittest.main()
