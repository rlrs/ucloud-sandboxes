"""Gateway job reads: held reads, their deadline and the record cache.

docs/managed-process-reads.md, "Gateway".
"""

from contextlib import ExitStack, contextmanager
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Condition, Event, Thread
import unittest
from unittest.mock import patch

from ucloud_sandboxes.control_plane import build_server
from ucloud_sandboxes.gateway import job_reads
from ucloud_sandboxes.gateway.node_rpc import DEFAULT_PROXY_TIMEOUT_SECONDS, ProxiedResponse
from ucloud_sandboxes.http_contract import match_sandbox_http_route
from ucloud_sandboxes.managed_process import ManagedProcessRecord
from ucloud_sandboxes.models import NodeHeartbeat, ResourceQuantity, utc_now
from ucloud_sandboxes.routing import SandboxRoute

TEST_TIER = "contract"

SPEC_SHA = "a" * 64


def job_record(**overrides):
    record = {
        "sandbox_id": "s", "sandbox_generation": 7, "job_id": "job-1",
        "spec_sha256": SPEC_SHA, "state": "running", "pid": 42,
        "stdout_bytes": 0, "stderr_bytes": 0, "sequence": 2,
        "updated_at": "2026-10-11T00:00:00+00:00",
    }
    record.update(overrides)
    return ManagedProcessRecord.from_dict(record).to_dict()


@contextmanager
def running(server):
    thread = Thread(target=server.serve_forever, kwargs={"poll_interval": 0.005}, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        thread.join(3)


class NodeServer(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 256


class GatewayJobReadTests(unittest.TestCase):
    async_proxy_responses = True

    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.release = Event()
        self.entered = Condition()
        self.calls = []
        # path -> (status, payload); the default answers as a capable worker.
        self.answers = {}
        parent = self

        class Node(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *_args):
                pass

            def do_GET(self):
                with parent.entered:
                    parent.calls.append((self.path, self.headers.get("Authorization")))
                    parent.entered.notify_all()
                parent.release.wait(5)
                status, payload = parent.answers.get(self.path) or parent.default_answer(self.path)
                body = json.dumps(payload).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                try:
                    self.wfile.write(body)
                except OSError:
                    pass

        self.node = NodeServer(("127.0.0.1", 0), Node)
        self.stack.enter_context(running(self.node))
        self.addCleanup(self.release.set)
        self.gateway = build_server(
            "127.0.0.1", 0, self.root / "control.sqlite",
            gateway_bearer_token="operator", sandbox_api_token="public",
            heartbeat_bearer_token="heartbeat", node_control_bearer_token="node-private",
            deployment_id="test", routing_file=self.root / "routing.sqlite",
            image_file=self.root / "images.json", metrics_file=self.root / "metrics.sqlite",
            max_http_request_threads=2, async_proxy_responses=self.async_proxy_responses,
        )
        self.stack.enter_context(running(self.gateway))
        self.handler = self.gateway.RequestHandlerClass
        heartbeat = NodeHeartbeat(
            node_id="node", job_id="job", updated_at=utc_now(), received_at=utc_now(),
            node_url=f"http://127.0.0.1:{self.node.server_port}", active_sandboxes=1,
            deployment_id="test", total_resources=ResourceQuantity(vcpu=32, memory_mb=98304),
        )
        self.handler.services.heartbeats.store.receive_heartbeat(heartbeat)
        self.route = self.handler.routing_store.upsert_sandbox(SandboxRoute(
            sandbox_id="s", node_id="node", job_id="job", node_url=heartbeat.node_url,
            resources=ResourceQuantity(vcpu=1, memory_mb=1024),
            spec={"id": "s", "parkable": True, "managed_process": True},
            state="running", generation=7, create_operation_id="test-create",
            spec_hash="b" * 64,
        ))

    def default_answer(self, path):
        if "/logs/" in path:
            return 200, {"stream": "stdout", "offset": 0, "next_offset": 0, "data": "",
                         "eof": True, "job": job_record(stdout_bytes=20)}
        return 200, {"job": job_record(stdout_bytes=10)}

    def get(self, path, *, token="public"):
        connection = HTTPConnection("127.0.0.1", self.gateway.server_port, timeout=10)
        self.addCleanup(connection.close)
        connection.request("GET", path, headers={"Authorization": "Bearer " + token})
        return connection

    def read(self, connection):
        response = connection.getresponse()
        return response.status, response.headers, json.loads(response.read() or b"{}")

    def wait_calls(self, count):
        with self.entered:
            self.assertTrue(self.entered.wait_for(lambda: len(self.calls) >= count, timeout=5))

    def cached(self):
        return self.handler.routing_store.get_managed_process("s", "job-1", sandbox_generation=7)

    def cache(self, **overrides):
        self.handler.routing_store.upsert_managed_process(
            self.route, ManagedProcessRecord.from_dict(job_record(**overrides)))

    def park(self):
        from dataclasses import replace
        self.route = self.handler.routing_store.upsert_sandbox(replace(self.route, state="parked"))

    @contextmanager
    def recording_wakes(self):
        wakes = []

        def prepare_wake(handler, route):
            wakes.append(handler.path)
            handler._write_json({"error": "wake attempted"}, status=503)
            return None

        with patch.object(self.handler, "_prepare_wake_placement", prepare_wake):
            yield wakes


class HeldJobReadTests(GatewayJobReadTests):
    def test_held_reads_release_handler_threads_and_keep_health_responsive(self):
        responses = self.handler.async_responses
        deadlines = []
        start = responses.start

        def recording_start(request, **kwargs):
            deadlines.append((kwargs.get("timeout"), kwargs.get("held_read")))
            return start(request, **kwargs)

        responses.start = recording_start
        paths = []
        for index in range(64):
            if index % 2:
                paths.append(f"/v1/sandboxes/s/jobs/job-1/logs/stdout?offset=0&limit=65536&wait_ms={index}")
            else:
                paths.append(f"/v1/sandboxes/s/jobs/job-1?wait_ms=20000&after=2.0.{index}")
        connections = []
        for index, path in enumerate(paths):
            connections.append(self.get(path))
            self.wait_calls(index + 1)
        # 64 held reads exceed the two-thread HTTP admission pool.
        status, _headers, _body = self.read(self.get("/healthz"))
        self.assertEqual(status, 200)
        self.assertEqual(len(responses._pending), 64)
        self.assertEqual(len(deadlines), 64)
        self.assertIn((DEFAULT_PROXY_TIMEOUT_SECONDS + 20, True), deadlines)
        self.assertIn((DEFAULT_PROXY_TIMEOUT_SECONDS + 0.001, True), deadlines)
        # Query strings reach the worker verbatim, with its private credential.
        self.assertEqual(sorted(path for path, _ in self.calls), sorted(paths))
        self.assertEqual({token for _, token in self.calls}, {"Bearer node-private"})
        self.release.set()
        for path, connection in zip(paths, connections):
            status, headers, body = self.read(connection)
            self.assertEqual(status, 200)
            self.assertEqual(headers["Connection"], "close")
            self.assertEqual(body["job"]["job_id"], "job-1")
            self.assertEqual("/logs/" in path, "stream" in body)
        # Log responses' records are cached; an older status read finishing
        # later does not shrink the counts.
        self.assertEqual(self.cached().stdout_bytes, 20)

    def test_wait_is_clamped_and_unparseable_waits_are_not_held(self):
        route = match_sandbox_http_route("GET", "/v1/sandboxes/s/jobs/j")
        logs = match_sandbox_http_route("GET", "/v1/sandboxes/s/jobs/j/logs/stderr")
        self.assertEqual(job_reads.job_read_wait_ms(route, "/v1/sandboxes/s/jobs/j?wait_ms=99999"), 25000)
        self.assertEqual(job_reads.job_read_wait_ms(logs, "/x?offset=1&wait_ms=-5"), 0)
        self.assertIsNone(job_reads.job_read_wait_ms(route, "/v1/sandboxes/s/jobs/j"))
        self.assertIsNone(job_reads.job_read_wait_ms(route, "/v1/sandboxes/s/jobs/j?wait_ms=soon"))
        exec_route = match_sandbox_http_route("POST", "/v1/sandboxes/s/exec")
        self.assertIsNone(job_reads.job_read_wait_ms(exec_route, "/x?wait_ms=5"))
        self.assertEqual(job_reads.job_read_timeout_seconds(None), DEFAULT_PROXY_TIMEOUT_SECONDS)
        self.assertEqual(job_reads.job_read_timeout_seconds(25000), DEFAULT_PROXY_TIMEOUT_SECONDS + 25)
        # The worker, not the gateway, rejects a malformed wait.
        self.release.set()
        self.answers["/v1/sandboxes/s/jobs/job-1?wait_ms=soon"] = (400, {"error": "wait_ms"})
        status, _headers, body = self.read(self.get("/v1/sandboxes/s/jobs/job-1?wait_ms=soon"))
        self.assertEqual((status, body), (400, {"error": "wait_ms"}))
        self.assertEqual(self.handler.async_responses._pending, {})

    def test_held_log_records_are_validated_like_status_records(self):
        self.release.set()
        legacy = "/v1/sandboxes/s/jobs/job-1/logs/stdout?offset=0&wait_ms=10"
        self.answers[legacy] = (200, {"stream": "stdout", "offset": 0, "next_offset": 0,
                                      "data": "", "eof": True})
        status, _headers, body = self.read(self.get(legacy))
        self.assertEqual((status, body["eof"]), (200, True))
        self.assertIsNone(self.cached())
        other = "/v1/sandboxes/s/jobs/job-1/logs/stdout?offset=0&wait_ms=11"
        self.answers[other] = (200, {"stream": "stdout", "offset": 0, "next_offset": 0, "data": "",
                                     "eof": True, "job": job_record(job_id="job-2")})
        status, _headers, body = self.read(self.get(other))
        self.assertEqual(status, 502)
        self.assertIn("another managed process", body["error"])
        self.assertIsNone(self.cached())

    def test_worker_transition_answers_waiting_reads_from_the_cache(self):
        self.release.set()
        self.cache(stdout_bytes=20)
        transition = (409, {"error": "sandbox lifecycle transition is in progress: s"})
        logs = "/v1/sandboxes/s/jobs/job-1/logs/stdout?offset=20&wait_ms=20000"
        status_path = "/v1/sandboxes/s/jobs/job-1?wait_ms=20000"
        below = "/v1/sandboxes/s/jobs/job-1/logs/stdout?offset=5&wait_ms=20000"
        for path in (logs, status_path, below):
            self.answers[path] = transition
        status, headers, body = self.read(self.get(logs))
        self.assertEqual((status, headers["Retry-After"]), (200, "1"))
        self.assertEqual((body["next_offset"], body["data"], body["job"]["stdout_bytes"]), (20, "", 20))
        status, headers, body = self.read(self.get(status_path))
        self.assertEqual((status, headers["Retry-After"], body["job"]["cursor"]), (200, "1", "2.20.0"))
        # Bytes below the cached count exist only in the sandbox.
        status, _headers, body = self.read(self.get(below))
        self.assertEqual(status, 409)


class SynchronousJobReadTests(GatewayJobReadTests):
    async_proxy_responses = False

    def test_synchronous_reads_get_the_wait_in_their_deadline(self):
        self.release.set()
        deadlines = []
        proxy = self.handler._proxy_request

        def recording_proxy(handler, node_url, path, **kwargs):
            deadlines.append((path, kwargs.get("timeout_seconds")))
            return proxy(handler, node_url, path, **kwargs)

        with patch.object(self.handler, "_proxy_request", recording_proxy):
            for path in ("/v1/sandboxes/s/jobs/job-1?wait_ms=99999&after=2.0.0",
                         "/v1/sandboxes/s/jobs/job-1/logs/stdout?offset=0&limit=10&wait_ms=1500",
                         "/v1/sandboxes/s/jobs/job-1"):
                status, _headers, body = self.read(self.get(path))
                self.assertEqual(status, 200, body)
        self.assertEqual(deadlines, [
            ("/v1/sandboxes/s/jobs/job-1?wait_ms=99999&after=2.0.0", DEFAULT_PROXY_TIMEOUT_SECONDS + 25),
            ("/v1/sandboxes/s/jobs/job-1/logs/stdout?offset=0&limit=10&wait_ms=1500",
             DEFAULT_PROXY_TIMEOUT_SECONDS + 1.5),
            ("/v1/sandboxes/s/jobs/job-1", DEFAULT_PROXY_TIMEOUT_SECONDS),
        ])
        self.assertEqual([path for path, _ in self.calls], [path for path, _ in deadlines])
        self.assertEqual(self.cached().stdout_bytes, 20)

    def test_synchronous_log_reads_cache_their_record(self):
        self.release.set()
        status, _headers, body = self.read(self.get("/v1/sandboxes/s/jobs/job-1/logs/stdout?offset=0"))
        self.assertEqual(status, 200)
        self.assertEqual(self.cached().stdout_bytes, 20)
        self.answers["/v1/sandboxes/s/jobs/job-1/logs/stderr?offset=0"] = (
            200, {"stream": "stderr", "offset": 0, "next_offset": 0, "data": "", "eof": True,
                  "job": {"job_id": "job-1"}})
        status, _headers, body = self.read(self.get("/v1/sandboxes/s/jobs/job-1/logs/stderr?offset=0"))
        self.assertEqual(status, 502)
        self.assertIn("invalid managed process state", body["error"])


class ParkedJobReadTests(GatewayJobReadTests):
    def setUp(self):
        super().setUp()
        self.release.set()
        self.cache(stdout_bytes=20, stderr_bytes=0)
        self.park()

    def test_waiting_status_read_returns_the_cached_record_and_backs_off(self):
        status, headers, body = self.read(self.get("/v1/sandboxes/s/jobs/job-1?wait_ms=20000&after=2.20.0"))
        self.assertEqual((status, headers["Retry-After"]), (200, "1"))
        self.assertEqual(body["job"], {**job_record(stdout_bytes=20), "cursor": "2.20.0"})
        status, headers, body = self.read(self.get("/v1/sandboxes/s/jobs/job-1"))
        self.assertEqual(status, 200)
        self.assertIsNone(headers["Retry-After"])
        self.assertEqual(body["job"]["state"], "running")
        self.assertEqual(self.calls, [])

    def test_waiting_log_read_at_or_past_the_cached_bytes_does_not_wake(self):
        with self.recording_wakes() as wakes:
            for stream, offset in (("stdout", 20), ("stdout", 25), ("stderr", 0)):
                with self.subTest(stream=stream, offset=offset):
                    status, headers, body = self.read(self.get(
                        f"/v1/sandboxes/s/jobs/job-1/logs/{stream}?offset={offset}&limit=4096&wait_ms=20000"))
                    self.assertEqual((status, headers["Retry-After"]), (200, "1"))
                    self.assertEqual(body, {
                        "stream": stream, "offset": offset, "next_offset": offset, "data": "",
                        "eof": True, "job": {**job_record(stdout_bytes=20), "cursor": "2.20.0"},
                    })
        self.assertEqual(wakes, [])
        self.assertEqual(self.calls, [])

    def test_other_parked_log_reads_still_wake(self):
        with self.recording_wakes() as wakes:
            for path in (
                # Bytes below the cached count exist only in the parked image.
                "/v1/sandboxes/s/jobs/job-1/logs/stdout?offset=19&wait_ms=20000",
                # Reads that do not wait keep today's behaviour.
                "/v1/sandboxes/s/jobs/job-1/logs/stdout?offset=20",
                # The worker rejects malformed reads, as today.
                "/v1/sandboxes/s/jobs/job-1/logs/stdout?offset=20&limit=0&wait_ms=1",
                "/v1/sandboxes/s/jobs/job-1/logs/stdout?offset=x&wait_ms=1",
                # No cached record for this job.
                "/v1/sandboxes/s/jobs/job-2/logs/stdout?offset=20&wait_ms=1",
            ):
                with self.subTest(path=path):
                    status, _headers, body = self.read(self.get(path))
                    self.assertEqual((status, body["error"]), (503, "wake attempted"))
        self.assertEqual(len(wakes), 5)


class ManagedProcessCacheOrderTests(GatewayJobReadTests):
    def test_an_older_read_within_a_sequence_does_not_shrink_cached_counts(self):
        self.cache(stdout_bytes=20, stderr_bytes=5)
        self.cache(stdout_bytes=10, stderr_bytes=5)
        self.assertEqual((self.cached().stdout_bytes, self.cached().stderr_bytes), (20, 5))
        self.cache(stdout_bytes=20, stderr_bytes=7)
        self.assertEqual(self.cached().stderr_bytes, 7)
        self.cache(stdout_bytes=0, stderr_bytes=0, sequence=3, state="exited", exit_code=0,
                   completed_at="2026-10-11T00:00:01+00:00")
        self.assertEqual((self.cached().sequence, self.cached().stdout_bytes), (3, 0))


class ProjectionTests(unittest.TestCase):
    def test_non_job_routes_are_relayed(self):
        route = match_sandbox_http_route("POST", "/v1/sandboxes/s/exec")
        response = ProxiedResponse(409, {}, b'{"error":"lifecycle transition is in progress"}')
        self.assertIsNone(job_reads.project_job_response(None, None, route, "/", response, wait_ms=1))


if __name__ == "__main__":
    unittest.main()
