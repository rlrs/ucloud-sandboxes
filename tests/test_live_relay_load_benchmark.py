import json
import io
import asyncio
from contextlib import redirect_stderr
import os
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
import time
import unittest

from scripts.live_relay_load_benchmark import AGENT, with_lease_renewal, parse_args, response_window, retry_control, safe_error, summary, meets_wake_slo


class ResponseWindowTests(unittest.IsolatedAsyncioTestCase):
    async def test_forced_park_runs_during_model_wait_without_hiding_overrun(self):
        before = time.monotonic()
        inventory = {}
        async def park():
            await asyncio.sleep(.04)
            inventory['s'] = ('parked', time.monotonic())
        observer = asyncio.create_task(asyncio.sleep(60))
        try:
            ready, parked = await response_window(
                claimed_at=before, model_seconds=.01, mode='forced', sandbox_id='s',
                inventory=inventory, inventory_task=observer, park=park,
            )
            self.assertEqual(ready, before + .01)
            self.assertGreaterEqual(parked, .04)
        finally:
            observer.cancel()
            await asyncio.gather(observer, return_exceptions=True)

    async def test_deliberate_wait_renews_lease_and_stops_on_lease_loss(self):
        from unittest.mock import AsyncMock
        from types import SimpleNamespace
        renewed = object()
        relay = SimpleNamespace(renew_request=AsyncMock(return_value=renewed))
        self.assertEqual(await with_lease_renewal(
            asyncio.sleep(.04, result="ready"), relay, object(), interval=.01,
        ), "ready")
        self.assertGreaterEqual(relay.renew_request.await_count, 2)
        calls = relay.renew_request.await_count
        await asyncio.sleep(.02)
        self.assertEqual(relay.renew_request.await_count, calls)
        relay.renew_request.side_effect = RuntimeError("lease lost")
        with self.assertRaisesRegex(RuntimeError, "lease lost"):
            await with_lease_renewal(asyncio.sleep(60), relay, object(), interval=.01)

    async def test_poll_transport_retry_is_visible_and_ownership_errors_are_not_replayed(self):
        attempts, retries = [], []

        async def operation():
            attempts.append(1)
            if len(attempts) == 1:
                error = RuntimeError('ingress unavailable')
                error.status_code = 503
                raise error
            return 'request'

        self.assertEqual(await retry_control(
            operation, deadline=time.monotonic()+2,
            on_retry=lambda a, s: retries.append((a, s)),
        ), 'request')
        self.assertEqual(retries, [(1, 503)])

        async def conflict():
            error = RuntimeError('generation changed')
            error.status_code = 409
            raise error

        with self.assertRaisesRegex(RuntimeError, 'generation changed'):
            await retry_control(conflict, deadline=time.monotonic()+2,
                                active_delete=True, on_retry=lambda *_: self.fail('unsafe retry'))

    async def test_natural_readiness_does_not_wait_for_parking(self):
        inventory_task = asyncio.create_task(asyncio.sleep(60))
        try:
            before = time.monotonic()
            ready, parked = await asyncio.wait_for(response_window(
                claimed_at=before, model_seconds=0, mode='natural', sandbox_id='s',
                inventory={}, inventory_task=inventory_task,
            ), .5)
            self.assertEqual(ready, before)
            self.assertIsNone(parked)
        finally:
            inventory_task.cancel()
            await asyncio.gather(inventory_task, return_exceptions=True)

    async def test_forced_park_wait_does_not_reset_response_ready_timer(self):
        inventory = {}
        before = time.monotonic()
        async def observe():
            await asyncio.sleep(.01)
            inventory['s'] = ('parked', time.monotonic())
        task = asyncio.create_task(observe())
        ready, parked = await response_window(
            claimed_at=before, model_seconds=0, mode='forced', sandbox_id='s',
            inventory=inventory, inventory_task=task,
        )
        await task
        self.assertEqual(ready, before)
        self.assertGreater(parked, .01)


class RelayLoadBenchmarkTests(unittest.TestCase):
    def test_slo_counts_tail_and_rejects_invalid_working_sets(self):
        self.assertEqual(summary(list(range(1, 101)))['p95'], 95)
        self.assertIsNone(summary([])['p95'])
        base = ['--gateway-url', 'http://gateway', '--relay-url', 'http://relay',
                '--sandbox-token-file', '/tmp/sandbox-token', '--relay-worker-token-file', '/tmp/worker-token',
                '--image', 'python@sha256:' + 'a' * 64, '--output', '/tmp/report.json']
        for extra in (['--dirty-mb', '256'], ['--warmup-cycles', '8'], ['--model-seconds', 'nan'], ['--cycles', '0'], ['--fleet-pollers', '0'],
                      ['--startup-mode', 'rolling', '--start-signal-file', '/tmp/signal']):
            with self.subTest(extra=extra), redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                parse_args(base + extra)
        self.assertNotIn('secret', safe_error(OSError('http://relay/_relay/secret/chat/completions')))

    def test_fast_steady_state_cannot_hide_failed_or_missing_overlap_coverage(self):
        result = {
            'correct': True, 'configuration': {'startup_mode': 'rolling'},
            'response_ready_to_usable_exec_seconds': summary([.1] * 100),
            'phase_latency_seconds': {
                'during_provisioning': summary([5.] * 10),
                'after_provisioning': summary([.1] * 100),
            },
        }
        self.assertFalse(meets_wake_slo(result, .8))
        result['phase_latency_seconds']['during_provisioning'] = summary([])
        self.assertFalse(meets_wake_slo(result, .8))
        result['phase_latency_seconds']['during_provisioning'] = summary([.2] * 10)
        self.assertTrue(meets_wake_slo(result, .8))
        result['correct'] = False
        self.assertFalse(meets_wake_slo(result, .8))

    def test_guest_exercises_memory_files_relay_and_tool_with_stable_identity(self):
        requests = []
        class Echo(BaseHTTPRequestHandler):
            def do_POST(self):
                body = self.rfile.read(int(self.headers['Content-Length']))
                requests.append((json.loads(body), self.headers['X-UCloud-Relay-Request-Id']))
                self.send_response(200)
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            def log_message(self, *_args):
                pass
        server = ThreadingHTTPServer(('127.0.0.1', 0), Echo)
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with TemporaryDirectory() as directory:
                root = Path(directory)
                for cycle in range(2):
                    (root / f'go-{cycle}').touch()
                config = dict(resident_mb=1, dirty_mb=1, cycles=2, files=2, file_kib=4, payload_kib=1, cpu_ms=1)
                process = subprocess.Popen([sys.executable, '-c', AGENT], env={
                    **os.environ, 'BENCH_CONFIG': json.dumps(config), 'BENCH_ROOT': directory,
                    'RELAY_URL': f'http://127.0.0.1:{server.server_port}/relay',
                }, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                try:
                    deadline = time.monotonic() + 10
                    while not (root / 'result-1.json').exists() and time.monotonic() < deadline and process.poll() is None:
                        time.sleep(.01)
                    self.assertTrue((root / 'result-1.json').exists())
                    results = [json.loads((root / f'result-{i}.json').read_text()) for i in range(2)]
                    self.assertEqual(results[0]['nonce'], results[1]['nonce'])
                    self.assertEqual(results[0]['pid'], results[1]['pid'])
                    self.assertNotEqual(results[0]['digest'], results[1]['digest'])
                    self.assertEqual([r['tool'] for r in results], ['42', '42'])
                    self.assertEqual([r['transport_retries'] for r in results], [0, 0])
                    for result in results:
                        self.assertGreaterEqual(result['verification_seconds'], 0)
                        self.assertGreater(result['tool_seconds'], 0)
                    self.assertEqual(len(requests), 2)
                    self.assertNotEqual(requests[0][1], requests[1][1])
                finally:
                    process.terminate()
                    process.communicate(timeout=5)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)
