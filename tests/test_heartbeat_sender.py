"""The node agent's heartbeat sender thread (plan C4.4, node side).

Tier: contract. The gateway is a real localhost HTTP server that records each
POST and answers from a script; the node-agent test runs a real builder agent.
"""

from __future__ import annotations

import contextlib
from dataclasses import replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import json
import os
from pathlib import Path
import random
import re
import shlex
import subprocess
import tempfile
import threading
import time
import unittest

from ucloud_sandboxes import cli
from ucloud_sandboxes.agent import HeartbeatPostResult, build_heartbeat, fetch_node_agent_heartbeat
from ucloud_sandboxes.heartbeat_sender import HeartbeatSenderConfig, NodeHeartbeatSender
from ucloud_sandboxes.images import DockerImageRuntime
from ucloud_sandboxes.models import NodeRuntimeMetrics, ResourceQuantity, utc_now
from ucloud_sandboxes.node_agent import build_builder_node_agent_server
from ucloud_sandboxes.registry import heartbeat_to_dict
from ucloud_sandboxes.vm_init import render_vm_init_script

from tests import test_vm_init

TEST_TIER = "contract"
TOKEN = "heartbeat-secret"


class FakeGateway:
    """Answers heartbeat POSTs from ``statuses`` (then 200); clear ``open`` to hang them."""

    def __init__(self) -> None:
        self.statuses: list[int] = []
        self.location = ""  # sent as Location when set
        self.arrivals: list[float] = []
        self.requests: list[tuple[str, dict[str, str], dict]] = []
        self.open = threading.Event()
        self.open.set()
        self.changed = threading.Condition()
        gateway = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                with gateway.changed:
                    gateway.arrivals.append(time.monotonic())
                    gateway.changed.notify_all()
                gateway.open.wait(10)
                with gateway.changed:
                    gateway.requests.append((self.path, dict(self.headers), body))
                    status = gateway.statuses.pop(0) if gateway.statuses else 200
                    gateway.changed.notify_all()
                payload = json.dumps({"ok": status == 200, "node": body}).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                if gateway.location:
                    self.send_header("Location", gateway.location)
                self.end_headers()
                self.wfile.write(payload)

            def do_GET(self) -> None:  # where a followed 302/303 would land
                with gateway.changed:
                    gateway.requests.append((self.path, dict(self.headers), None))
                self.send_response(200)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def log_message(self, *_args: object) -> None:
                return

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        threading.Thread(
            target=self.server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True
        ).start()
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}/v1/nodes/heartbeat"

    def wait(self, attribute: str, count: int, timeout: float = 10.0) -> list:
        with self.changed:
            if not self.changed.wait_for(lambda: len(getattr(self, attribute)) >= count, timeout):
                raise AssertionError(f"{count} {attribute} expected, saw {getattr(self, attribute)}")
            return list(getattr(self, attribute))

    def close(self) -> None:
        self.open.set()
        self.server.shutdown()
        self.server.server_close()


def sample_source():
    """A heartbeat source whose activity epoch counts its samples."""
    calls = []

    def source():
        calls.append(None)
        return build_heartbeat(
            job_id="job-1", node_id="node-1", deployment_id="deployment-1",
            node_url="http://node-1:8090", activity_epoch=len(calls),
            labels={"own": "node", "shared": "node"},
        )

    return source, calls


def sender_threads() -> list[threading.Thread]:
    return [thread for thread in threading.enumerate() if thread.name == "node-heartbeat"]


class HeartbeatSenderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.gateway = FakeGateway()
        self.addCleanup(self.gateway.close)

    def sender(self, source, *, interval: float = 60, retry: float = 0.05, **kwargs) -> NodeHeartbeatSender:
        config = HeartbeatSenderConfig(
            url=self.gateway.url, bearer_token=TOKEN, interval_seconds=interval,
            retry_initial_seconds=retry, **kwargs,
        )
        sender = NodeHeartbeatSender(source, config)
        self.addCleanup(sender.stop)
        return sender

    def test_config_is_strict_and_owns_its_labels(self) -> None:
        good = {"url": "http://gateway:8080/v1/nodes/heartbeat", "bearer_token": TOKEN}
        for bad in (
            {"url": "gateway:8080/v1/nodes/heartbeat"}, {"url": "ftp://gateway/x"},
            {"url": "http://user:secret@gateway/v1/nodes/heartbeat"},
            {"bearer_token": ""}, {"bearer_token": " padded"}, {"bearer_token": "two\nlines"},
            {"interval_seconds": 0}, {"interval_seconds": float("inf")}, {"interval_seconds": float("nan")},
            {"jitter": 1.0}, {"jitter": -0.1}, {"retry_initial_seconds": 0},
            {"interval_seconds": 5, "retry_initial_seconds": 6}, {"labels": {"": "x"}},
        ):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                HeartbeatSenderConfig(**{**good, **bad})
        labels = {"k": "v"}
        config = HeartbeatSenderConfig(**good, labels=labels)
        labels["k"] = "changed"
        self.assertEqual(config.labels, {"k": "v"})
        self.assertNotIn(TOKEN, repr(config))

    def test_backoff_doubles_to_the_interval_and_rejections_wait_a_full_period(self) -> None:
        config = HeartbeatSenderConfig(
            url=self.gateway.url, bearer_token=TOKEN, interval_seconds=20, jitter=0
        )
        sender = NodeHeartbeatSender(lambda: None, config)
        ok, busy = HeartbeatPostResult(200, {}), HeartbeatPostResult(503, {})
        outcomes = [RuntimeError("refused"), busy, HeartbeatPostResult(429, {}),
                    HeartbeatPostResult(408, {}), ValueError("bad sample"),
                    HeartbeatPostResult(500, {}), busy, ok, busy, HeartbeatPostResult(401, {}),
                    busy, ok, HeartbeatPostResult(409, {}), ok]
        with self.assertLogs("ucloud_sandboxes.heartbeat_sender", "WARNING") as logs:
            delays = [sender._next_delay(outcome) for outcome in outcomes]
        self.assertEqual(delays, [1, 2, 4, 8, 16, 20, 20, 20, 1, 20, 4, 20, 20, 20])
        self.assertIn("(7 in a row, next in 20.0 s): HTTP 503", logs.output[6])
        self.assertIn("delivered after 7 failed attempts", logs.output[7])
        self.assertFalse(any(TOKEN in line for line in logs.output))

        jittered = NodeHeartbeatSender(lambda: None, replace(config, jitter=0.2), rng=random.Random(7))
        periods = [jittered._next_delay(ok) for _ in range(500)]
        self.assertTrue(all(16 <= delay <= 24 for delay in periods))
        self.assertAlmostEqual(sum(periods) / len(periods), 20, delta=0.5)
        self.assertGreater(max(periods) - min(periods), 6)
        first_retry = jittered._next_delay(busy)
        self.assertTrue(0.8 <= first_retry <= 1.2)

    def test_first_heartbeat_is_immediate_and_carries_the_wire_contract(self) -> None:
        source, _ = sample_source()
        sender = self.sender(source, labels={"shared": "config", "provider": "label"})
        sender.start()
        [(path, headers, body)] = self.gateway.wait("requests", 1)
        self.assertEqual(path, "/v1/nodes/heartbeat")
        self.assertEqual(headers["Authorization"], f"Bearer {TOKEN}")
        self.assertEqual(headers["Content-Type"], "application/json")
        expected = heartbeat_to_dict(replace(
            source(), labels={"own": "node", "shared": "config", "provider": "label"}))
        volatile = {"updated_at", "reported_at", "activity_epoch"}
        self.assertEqual({k: v for k, v in body.items() if k not in volatile},
                         {k: v for k, v in expected.items() if k not in volatile})
        self.assertEqual(body["activity_epoch"], 1)
        time.sleep(0.3)
        self.assertEqual(len(self.gateway.requests), 1, "the next heartbeat waits an interval")

    def test_transient_failures_retry_fresh_samples_with_backoff(self) -> None:
        source, _ = sample_source()
        self.gateway.statuses = [503, 503, 200]
        sender = self.sender(source, retry=0.1, jitter=0)
        started = time.monotonic()
        sender.start()
        requests = self.gateway.wait("requests", 3)
        arrivals = self.gateway.arrivals
        # Each attempt is a new sample; waits double after each failure.
        self.assertEqual([body["activity_epoch"] for *_, body in requests], [1, 2, 3])
        self.assertLess(arrivals[0] - started, 5)
        self.assertGreaterEqual(arrivals[1] - arrivals[0], 0.09)
        self.assertGreaterEqual(arrivals[2] - arrivals[1], 0.19)
        time.sleep(0.4)
        self.assertEqual(len(self.gateway.requests), 3, "delivery restores the interval")

    def test_a_rejection_retrying_cannot_fix_waits_a_full_interval(self) -> None:
        source, _ = sample_source()
        self.gateway.statuses = [403]
        self.sender(source, retry=0.01).start()
        self.gateway.wait("requests", 1)
        time.sleep(0.3)
        self.assertEqual(len(self.gateway.requests), 1)

    def test_sampling_and_transport_failures_never_end_the_thread(self) -> None:
        source, calls = sample_source()
        posted = []

        def flaky_source():
            if not calls:
                calls.append(None)
                raise OSError("disk usage unavailable")
            return source()

        def flaky_post(url, heartbeat, headers):
            posted.append(heartbeat.activity_epoch)
            if len(posted) == 1:
                raise RuntimeError("Could not post heartbeat: connection refused")
            return HeartbeatPostResult(200, {})

        config = HeartbeatSenderConfig(url=self.gateway.url, bearer_token=TOKEN,
                                       interval_seconds=60, retry_initial_seconds=0.01)
        sender = NodeHeartbeatSender(flaky_source, config, post=flaky_post)
        self.addCleanup(sender.stop)
        sender.start()
        deadline = time.monotonic() + 10
        while len(posted) < 2 and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertEqual(posted, [2, 3])
        self.assertEqual(sender.send_now().status, 200)

    def test_send_now_answers_for_a_sample_taken_after_the_call(self) -> None:
        source, calls = sample_source()
        sender = self.sender(source)
        # Asked before the thread runs, as the harness may: it is served once it does.
        early = []
        asker = threading.Thread(target=lambda: early.append(sender.send_now()))
        asker.start()
        time.sleep(0.05)
        sender.start()
        asker.join(10)
        self.assertEqual(early[0].status, 200)
        before = len(calls)
        answer = sender.send_now()
        self.assertEqual(answer.status, 200)
        self.assertEqual(answer.payload["node"]["activity_epoch"], before + 1)
        # A gateway error is an answer, not an exception.
        self.gateway.statuses = [500]
        self.assertEqual(sender.send_now().status, 500)

    def test_a_redirect_is_an_answer_and_the_token_stays(self) -> None:
        elsewhere = FakeGateway()
        self.addCleanup(elsewhere.close)
        # urllib would follow these for a POST, as a GET with every header.
        self.gateway.statuses, self.gateway.location = [302, 303], elsewhere.url
        source, _ = sample_source()
        sender = self.sender(source, retry=0.01)
        sender.start()
        self.gateway.wait("requests", 1)
        self.assertEqual(sender.send_now().status, 303)
        self.assertEqual(elsewhere.requests, [])

    def test_stop_is_prompt_when_idle_and_final(self) -> None:
        source, _ = sample_source()
        sender = self.sender(source)
        sender.start()
        self.gateway.wait("requests", 1)
        started = time.monotonic()
        sender.stop()
        self.assertLess(time.monotonic() - started, 2)
        self.assertEqual(sender_threads(), [])
        sender.stop()
        with self.assertRaisesRegex(RuntimeError, "stopped"):
            sender.send_now()
        with self.assertRaisesRegex(RuntimeError, "starts once"):
            sender.start()

    def test_stop_waits_for_the_attempt_in_flight_and_sends_nothing_after(self) -> None:
        source, _ = sample_source()
        self.gateway.open.clear()
        sender = self.sender(source, retry=0.01)
        sender.start()
        self.gateway.wait("arrivals", 1)
        stopper = threading.Thread(target=sender.stop)
        stopper.start()
        stopper.join(0.2)
        self.assertTrue(stopper.is_alive(), "stop returns only after the in-flight POST")
        self.gateway.open.set()
        stopper.join(10)
        self.assertFalse(stopper.is_alive())
        self.assertEqual(sender_threads(), [])
        time.sleep(0.2)
        self.assertEqual(len(self.gateway.arrivals), 1)

    def test_a_sample_taken_while_stopping_is_never_sent(self) -> None:
        source, _ = sample_source()
        sampling, release = threading.Event(), threading.Event()

        def slow_source():
            sampling.set()
            release.wait(10)
            return source()

        sender = self.sender(slow_source)
        sender.start()
        self.assertTrue(sampling.wait(10))
        stopper = threading.Thread(target=sender.stop)
        stopper.start()
        deadline = time.monotonic() + 10
        while not sender._stopping and time.monotonic() < deadline:
            time.sleep(0.01)
        release.set()
        stopper.join(10)
        self.assertFalse(stopper.is_alive())
        self.assertEqual(sender_threads(), [])
        self.assertEqual(self.gateway.arrivals, [], "a stopped node advertises nothing")


class NodeAgentHeartbeatTests(unittest.TestCase):
    def test_node_agent_pushes_its_get_heartbeat_without_blocking_requests(self) -> None:
        gateway = FakeGateway()
        self.addCleanup(gateway.close)
        gateway.open.clear()
        labels = {"ucloud.provider": "label"}
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            server = build_builder_node_agent_server(
                "127.0.0.1", 0, state_file=root / "state.json", image_file=root / "images.json",
                job_id="job-b", node_id="builder-1", node_url="http://builder-1:8090",
                deployment_id="deployment-1", image_runtime=DockerImageRuntime(dry_run=True),
                node_control_bearer_token="node-secret",
                total_resources=ResourceQuantity(vcpu=4, memory_mb=8192, disk_mb=10_000),
                runtime_metrics_provider=lambda: NodeRuntimeMetrics(collected_at=utc_now(), cpu_count=4),
                heartbeat=HeartbeatSenderConfig(url=gateway.url, bearer_token=TOKEN, labels=labels),
            )
            serving = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01})
            serving.start()
            try:
                gateway.wait("arrivals", 1)
                # The first POST hangs at the gateway; request threads still answer.
                url = f"http://127.0.0.1:{server.server_address[1]}"
                started = time.monotonic()
                fetched = fetch_node_agent_heartbeat(url, bearer_token="node-secret", timeout_seconds=5)
                self.assertLess(time.monotonic() - started, 2)
                gateway.open.set()
                [(_, headers, pushed)] = gateway.wait("requests", 1)
            finally:
                server.shutdown()
                serving.join(5)
                stopped_with_serving = sender_threads() == []
                server.server_close()
            self.assertTrue(stopped_with_serving, "the sender stops when serving does")
            with self.assertRaisesRegex(RuntimeError, "stopped"):
                server.heartbeat_sender.send_now()
        self.assertEqual(headers["Authorization"], f"Bearer {TOKEN}")
        # What the retired oneshot posted: the GET heartbeat with labels merged.
        expected = heartbeat_to_dict(replace(fetched, labels={**fetched.labels, **labels}))
        self.assertIn("ucloud.image-build-admission-capacity", pushed["labels"])
        self.assertEqual(sorted(pushed), sorted(expected))
        volatile = {"updated_at", "reported_at", "runtime_metrics", "physical_disk_free_mb"}
        self.assertEqual({k: v for k, v in pushed.items() if k not in volatile},
                         {k: v for k, v in expected.items() if k not in volatile})
        del pushed["runtime_metrics"]["collected_at"], expected["runtime_metrics"]["collected_at"]
        self.assertEqual(pushed["runtime_metrics"], expected["runtime_metrics"])


class NodeHeartbeatDeploymentTests(unittest.TestCase):
    def test_cli_heartbeat_flags_are_strict(self) -> None:
        parser = cli.build_parser()
        base = ["serve-builder-agent", "--deployment-id", "d", "--state-file", "s",
                "--image-file", "i", "--node-control-bearer-token-file", "t"]
        with tempfile.TemporaryDirectory() as raw:
            token = Path(raw) / "heartbeat-token"
            token.write_text(f"{TOKEN}\n", encoding="utf-8")
            config = cli.heartbeat_sender_config_from_args(parser.parse_args(base + [
                "--heartbeat-url", "http://gateway:8080/v1/nodes/heartbeat",
                "--heartbeat-bearer-token-file", str(token), "--heartbeat-interval-seconds", "7.5",
                "--heartbeat-label", "a=1", "--heartbeat-label", "b = two",
            ]))
            self.assertEqual((config.url, config.bearer_token, config.interval_seconds, config.labels),
                             ("http://gateway:8080/v1/nodes/heartbeat", TOKEN, 7.5, {"a": "1", "b": "two"}))
            self.assertIsNone(cli.heartbeat_sender_config_from_args(parser.parse_args(base)))
            defaulted = cli.heartbeat_sender_config_from_args(parser.parse_args(base + [
                "--heartbeat-url", "http://gateway/v1/nodes/heartbeat",
                "--heartbeat-bearer-token-file", str(token)]))
            self.assertEqual((defaulted.interval_seconds, defaulted.labels), (20, {}))
            for flags in (["--heartbeat-url", "http://gateway/v1/nodes/heartbeat"],
                          ["--heartbeat-bearer-token-file", str(token)],
                          ["--heartbeat-label", "a=1"], ["--heartbeat-interval-seconds", "5"]):
                with self.subTest(flags=flags), self.assertRaises(ValueError):
                    cli.heartbeat_sender_config_from_args(parser.parse_args(base + flags))
        # The oneshot command of the retired timer is gone.
        with self.assertRaises(SystemExit), contextlib.redirect_stderr(io.StringIO()):
            parser.parse_args(["agent-heartbeat", "--deployment-id", "d"])

    def test_node_units_carry_the_sender_and_no_timer_is_installed(self) -> None:
        labels = {"ucloud.role": "worker pool", "zone": "a"}
        for role in ("sandbox", "builder"):
            with self.subTest(role=role), tempfile.TemporaryDirectory() as raw:
                script = render_vm_init_script(test_vm_init.VmInitTests._options(
                    role=role, heartbeat_interval_seconds=7, labels=labels))
                self.assertNotIn("agent-heartbeat", script)
                self.assertNotIn("OnUnitActiveSec", script)
                self.assertNotIn("enable --now ucloud-sandbox-heartbeat", script)
                unit = script.split("<<NODE_SERVICE\n", 1)[1].split("\nNODE_SERVICE", 1)[0]
                [exec_start] = [line for line in unit.splitlines() if line.startswith("ExecStart=")]
                # Expand as bash does when it writes the unit, then parse with the CLI.
                token = Path(raw) / "heartbeat-token"
                token.write_text(TOKEN, encoding="utf-8")
                values = {"UCLOUD_HEARTBEAT_URL": "http://gateway:8080/v1/nodes/heartbeat",
                          "UCLOUD_HEARTBEAT_BEARER_TOKEN_FILE": str(token),
                          "UCLOUD_DIRECT_NETWORK": "sandbox"}
                expanded = re.sub(r"\$\{(\w+)\}", lambda m: values.get(m.group(1), "1"), exec_start)
                argv = shlex.split(expanded.removeprefix("ExecStart="))[1:]
                config = cli.heartbeat_sender_config_from_args(cli.build_parser().parse_args(argv))
                self.assertEqual((config.url, config.bearer_token, config.interval_seconds, config.labels),
                                 (values["UCLOUD_HEARTBEAT_URL"], TOKEN, 7, labels))

    def test_reinit_retires_an_older_releases_timer_before_the_agent_restarts(self) -> None:
        script = render_vm_init_script(test_vm_init.VmInitTests._options())
        start = script.index("# The node agent sends its own heartbeats.")
        end = script.index("\n", script.index("$SUDO rm -f", start))
        # After the node unit is written, before systemd reloads units and
        # before the new agent starts.
        written = script.index("<<NODE_SERVICE")
        self.assertLess(written, start)
        self.assertLess(end, script.index("$SUDO systemctl daemon-reload", written))
        self.assertLess(end, script.index("systemctl restart ucloud-sandbox-node.service"))
        for present, failing in ((("timer", "service"), ""), ((), ""), (("service",), ""),
                                 (("timer", "service"), "disable")):
            with self.subTest(present=present, failing=failing), tempfile.TemporaryDirectory() as raw:
                root = Path(raw)
                units, bin_dir, log = root / "units", root / "bin", root / "systemctl.log"
                units.mkdir()
                bin_dir.mkdir()
                for kind in present:
                    (units / f"ucloud-sandbox-heartbeat.{kind}").write_text("[Unit]\n")
                fake = bin_dir / "systemctl"
                fake.write_text(f'#!/bin/sh\necho "$*" >> {shlex.quote(str(log))}\n'
                                f'case "$1" in {failing or "never"}) exit 1;; esac\n')
                fake.chmod(0o755)
                snippet = script[start:end].replace("/etc/systemd/system", str(units))
                result = subprocess.run(
                    ["bash", "-c", "set -euo pipefail\nSUDO=\n" + snippet],
                    env={**os.environ, "PATH": f"{bin_dir}:{os.environ['PATH']}"},
                    capture_output=True, text=True,
                )
                calls = log.read_text().splitlines() if log.exists() else []
                if failing:
                    self.assertNotEqual(result.returncode, 0)
                    self.assertTrue((units / "ucloud-sandbox-heartbeat.timer").exists(),
                                    "a timer systemd could not stop is left for the operator")
                    continue
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(list(units.iterdir()), [])
                expected = (["disable --now ucloud-sandbox-heartbeat.timer"] if "timer" in present else [])
                if "service" in present:
                    expected += ["stop ucloud-sandbox-heartbeat.service",
                                 "reset-failed ucloud-sandbox-heartbeat.service"]
                self.assertEqual(calls, expected)


if __name__ == "__main__":
    unittest.main()
