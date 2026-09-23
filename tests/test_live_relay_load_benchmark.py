import json
import hashlib
import io
import asyncio
from contextlib import closing, redirect_stderr
import os
from pathlib import Path
import subprocess
import shutil
import sqlite3
import sys
from tempfile import TemporaryDirectory
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
import time
import unittest
from datetime import datetime, timedelta, timezone

from scripts.live_relay_load_benchmark import AGENT, uploaded_tool_probe, with_lease_renewal, parse_args, ContinuationObserver, response_window, retry_control, safe_error, summary, meets_useful_action_slo
from scripts.live_relay_load_benchmark import resource_sample, resource_summary, FleetHealthQualification
from scripts.live_relay_load_benchmark import finish_primary


class PrimaryCompletionTests(unittest.IsolatedAsyncioTestCase):
    def record(self, **changes):
        from ucloud_sandboxes.managed_process import ManagedProcessRecord
        return ManagedProcessRecord(
            sandbox_id='agent', sandbox_generation=1, job_id='primary',
            spec_sha256='a' * 64, sequence=2,
            **({'state': 'exited', 'exit_code': 0, 'signal': 0} | changes),
        )

    async def test_waits_for_authoritative_normal_exit(self):
        from types import SimpleNamespace
        from unittest.mock import AsyncMock
        job = SimpleNamespace(job_id='primary', refresh=AsyncMock(side_effect=[
            self.record(state='running', exit_code=None), self.record(),
        ]))
        result = await finish_primary(job, deadline=time.monotonic() + 2,
                                      on_retry=lambda *_: None)
        self.assertEqual(result.state, 'exited')
        self.assertEqual(job.refresh.await_count, 2)

    async def test_rejects_signal_failure_nonzero_or_malformed_exit(self):
        from types import SimpleNamespace
        from unittest.mock import AsyncMock
        for changes in ({'state': 'signaled', 'signal': 7, 'exit_code': None},
                        {'state': 'failed', 'exit_code': None},
                        {'exit_code': 1}, {'exit_code': None}, {'exit_code': False},
                        {'signal': None}, {'state': 'unknown'}):
            with self.subTest(changes=changes):
                job = SimpleNamespace(job_id='primary', refresh=AsyncMock(
                    return_value=self.record(**changes)))
                with self.assertRaises(RuntimeError):
                    await finish_primary(job, deadline=time.monotonic() + 1,
                                         on_retry=lambda *_: None)

    async def test_rejects_wrong_primary_and_bounded_nonterminal(self):
        from types import SimpleNamespace
        from unittest.mock import AsyncMock
        job = SimpleNamespace(job_id='different', refresh=AsyncMock(return_value=self.record()))
        with self.assertRaisesRegex(RuntimeError, 'invalid completion'):
            await finish_primary(job, deadline=time.monotonic() + 1,
                                 on_retry=lambda *_: None)
        job.job_id = 'primary'
        job.refresh.return_value = self.record(state='running', exit_code=None)
        with self.assertRaises(TimeoutError):
            await finish_primary(job, deadline=time.monotonic() + .01,
                                 on_retry=lambda *_: None)


class FleetHealthQualificationTests(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 9, 23, 9, tzinfo=timezone.utc)
        self.gate = FleetHealthQualification(enabled=True)
        self.gate.placed('run-sandbox', 'used', monotonic_now=0)

    def node(self, *, job='used', heartbeat_age=0, metric_age=0):
        return {'job_id': job, 'updated_at': (self.now - timedelta(seconds=heartbeat_age)).isoformat(),
                'runtime_metrics': {'collected_at': (self.now - timedelta(seconds=metric_age)).isoformat()}}

    def observe(self, nodes, *, elapsed=31, error=None):
        self.gate.observe(nodes, now=self.now, monotonic_now=elapsed, probe_error=error)

    def test_fresh_active_worker_passes_and_unused_stale_worker_does_not_fail(self):
        self.observe([self.node(), self.node(job='unused', heartbeat_age=300, metric_age=300)])
        self.assertTrue(self.gate.summary()['passed'])
        self.assertEqual(self.gate.summary()['observed_healthy_jobs'], ['used'])
        self.assertEqual(self.gate.failures, [])

    def test_stale_active_heartbeat_or_resources_fails_even_after_recovery(self):
        for kwargs, kind in (({'heartbeat_age': 31}, 'stale_heartbeat'),
                             ({'metric_age': 31}, 'stale_resource_metrics')):
            with self.subTest(kind=kind):
                gate = FleetHealthQualification(enabled=True)
                gate.placed('run-sandbox', 'used', monotonic_now=0)
                gate.observe([self.node(**kwargs)], now=self.now, monotonic_now=31)
                gate.observe([self.node()], now=self.now, monotonic_now=36)
                self.assertFalse(gate.summary()['passed'])
                self.assertEqual(gate.failures[0]['kind'], kind)

    def test_bootstrap_grace_is_bounded_and_repeated_placement_cannot_reset_it(self):
        self.observe([], elapsed=29)
        self.assertEqual(self.gate.failures, [])
        self.assertEqual(self.gate.summary()['status'], 'insufficient_observation')
        self.gate.placed('run-sandbox', 'used', monotonic_now=29)
        self.observe([], elapsed=30)
        self.assertEqual(self.gate.failures[0]['kind'], 'missing_node')
        self.assertFalse(self.gate.summary()['passed'])

    def test_probe_failure_and_missing_metrics_are_explicit_not_zero_cost(self):
        self.observe(None, error='TimeoutError')
        self.assertEqual(self.gate.failures[0]['kind'], 'resource_probe_failed')
        self.gate.observe([{'job_id': 'used', 'updated_at': self.now.isoformat()}],
                          now=self.now, monotonic_now=36)
        self.assertEqual(self.gate.failures[-1]['kind'], 'missing_or_invalid_resource_metrics')

    def test_migration_tracks_current_owned_jobs_and_preserves_new_job_grace(self):
        self.observe([self.node()])
        self.gate.placed('run-sandbox', 'replacement', monotonic_now=40)
        self.observe([], elapsed=41)
        self.assertEqual(self.gate.failures, [])
        self.assertFalse(self.gate.summary()['passed'])
        self.observe([self.node(job='replacement')], elapsed=71)
        self.assertTrue(self.gate.summary()['passed'])

    def test_nonadmin_run_reports_unknown_without_claiming_healthy_fleet(self):
        gate = FleetHealthQualification(enabled=False)
        gate.placed('s', 'worker', monotonic_now=0)
        gate.observe(None, now=self.now, monotonic_now=60, probe_error='forbidden')
        self.assertIsNone(gate.summary()['passed'])
        self.assertEqual(gate.summary()['status'], 'unknown')
        self.assertEqual(gate.failures, [])


class ResourceEvidenceTests(unittest.TestCase):
    def test_stale_nodes_and_sensitive_registration_fields_are_not_retained(self):
        now = datetime(2026, 9, 23, 9, tzinfo=timezone.utc)
        node = {'job_id': 'worker', 'node_epoch': 'boot', 'node_url': 'secret',
                'drain_token': 'secret', 'labels': {'token': 'secret'},
                'runtime_metrics': {'collected_at': '2026-09-23T08:59:50+00:00'}}
        result = resource_sample([node, {**node, 'runtime_metrics': {
            'collected_at': '2026-09-23T08:58:00+00:00'}}], now)
        self.assertEqual(len(result['nodes']), 1)
        self.assertNotIn('secret', json.dumps(result))

    def test_costs_do_not_bridge_gaps_reboots_or_duplicate_heartbeats(self):
        def row(second, epoch='a', cpu=2):
            return {'nodes': [{'job_id': 'worker', 'node_epoch': epoch,
                'runtime_metrics': {
                    'collected_at': datetime.fromtimestamp(second, timezone.utc).isoformat(),
                    'cpu_vcpu': cpu, 'memory_working_set_mb': 512}}]}
        result = resource_summary([row(0), row(5), row(5), row(60), row(65, 'b'), row(70, 'b', None)])
        self.assertEqual(result['observed_worker_seconds'], 10)
        self.assertEqual(result['cpu_coverage_worker_seconds'], 5)
        self.assertEqual(result['sampled_cpu_seconds'], 10)
        self.assertEqual(result['sampled_host_memory_mib_seconds'], 5120)

    def test_cpu_counters_capture_bursts_and_reject_resets_or_reboots(self):
        def row(second, total, epoch='a'):
            return {'nodes': [{'job_id': 'worker', 'node_epoch': epoch, 'runtime_metrics': {
                'collected_at': datetime.fromtimestamp(second, timezone.utc).isoformat(),
                'cpu_vcpu': 0, 'resource_evidence': {'host_cpu_usage_usec': total,
                                                   'host_cpu_steal_usec': total // 10}}}]}
        result = resource_summary([row(0, 1_000_000), row(5, 3_000_000),
                                   row(10, 100), row(15, 500, 'b')])
        self.assertEqual(result['sampled_cpu_seconds'], 0)
        self.assertEqual(result['host_cpu_usage_seconds'], 2)
        self.assertEqual(result['host_cpu_steal_seconds'], .2)
        self.assertEqual(result['host_cpu_usage_coverage_worker_seconds'], 5)

    def test_io_uses_counters_and_excludes_device_replacement_or_reset(self):
        def row(second, total, identity='disk'):
            return {'nodes': [{'job_id': 'worker', 'node_epoch': 'boot', 'runtime_metrics': {
                'collected_at': datetime.fromtimestamp(second, timezone.utc).isoformat(),
                'resource_evidence': {'devices': [{'identity': identity,
                    'read_bytes': total, 'write_bytes': total * 2,
                    'write_bytes_per_second': 999999}]}}}]}
        result = resource_summary([row(0, 100), row(5, 200), row(10, 50), row(15, 100, 'replacement')])
        self.assertEqual(result['device_read_bytes'], 100)
        self.assertEqual(result['device_write_bytes'], 200)
        self.assertEqual(result['device_write_bytes_coverage_worker_seconds'], 5)


class ResponseWindowTests(unittest.IsolatedAsyncioTestCase):
    async def test_extended_observation_is_still_bounded_by_run_deadline(self):
        observer = ContinuationObserver(None, 'observer')
        future = asyncio.get_running_loop().create_future()
        task = asyncio.create_task(asyncio.Event().wait())
        try:
            with self.assertRaises(TimeoutError):
                await asyncio.wait_for(
                    observer.wait(future, task, timeout_seconds=1800), .01,
                )
            self.assertFalse(future.done())
            self.assertFalse(task.done())
            future.set_result(42)
            self.assertEqual(await observer.wait(future, task, timeout_seconds=.1), 42)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_continuation_observer_cannot_complete_without_guest_evidence(self):
        from types import SimpleNamespace
        queue = asyncio.Queue()
        commits = []
        class Relay:
            async def poll(self, *_args, **_kwargs):
                return SimpleNamespace(requests=[await queue.get()])
            async def commit_response_bytes_to(self, request, *_args, **_kwargs):
                commits.append(request)
        observer = ContinuationObserver(Relay(), 'unbound-observer')
        payload = {'nonce': 'process', 'cycle': 0, 'digest': 'hash'}
        future = observer.expect(payload)
        task = asyncio.create_task(observer.run())
        try:
            with self.assertRaises(TimeoutError):
                await observer.wait(future, task, timeout_seconds=.01)
            self.assertFalse(future.done())
            receipt = SimpleNamespace(body_bytes=json.dumps({**payload, 'tool': '42'}).encode())
            await queue.put(receipt)
            self.assertGreater(await observer.wait(future, task), 0)
            self.assertEqual(commits, [receipt])
            # Lost receipt ACK may replay the same observation without changing
            # its first observed time or waking another sandbox.
            observed = future.result()
            await queue.put(receipt)
            await asyncio.sleep(0)
            self.assertEqual(future.result(), observed)
            wrong = SimpleNamespace(body_bytes=json.dumps({**payload, 'digest': 'wrong', 'tool': '42'}).encode())
            await queue.put(wrong)
            with self.assertRaisesRegex(RuntimeError, 'unexpected guest continuation'):
                await task
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_observer_batch_timestamps_do_not_include_other_receipt_commits(self):
        from types import SimpleNamespace
        ack_gate = asyncio.Event()
        poll_gate = asyncio.Event()
        values = [{'nonce': 'guest', 'cycle': i, 'digest': str(i), 'tool': '42'} for i in range(2)]
        class Relay:
            async def poll(self, *_args, **_kwargs):
                await poll_gate.wait()
                poll_gate.clear()
                return SimpleNamespace(requests=[SimpleNamespace(body_bytes=json.dumps(v).encode()) for v in values])
            async def commit_response_bytes_to(self, *_args, **_kwargs):
                await ack_gate.wait()
        observer = ContinuationObserver(Relay(), 'unbound')
        futures = [observer.expect(v) for v in values]
        task = asyncio.create_task(observer.run())
        try:
            poll_gate.set()
            await asyncio.wait_for(asyncio.gather(*futures), 1)
            self.assertEqual(futures[0].result(), futures[1].result())
            self.assertFalse(ack_gate.is_set())
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

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
                      ['--startup-mode', 'rolling', '--start-signal-file', '/tmp/signal'],
                      ['--parking-mode', 'forced'], ['--sqlite-transactions', '-1'],
                      ['--sqlite-transactions', '10001'], ['--sqlite-payload-bytes', '0'],
                      ['--sqlite-payload-bytes', '1048577'],
                      ['--sandbox-request-timeout-seconds', '0'],
                      ['--sandbox-request-timeout-seconds', '-1'],
                      ['--sandbox-request-timeout-seconds', 'nan'],
                      ['--sandbox-request-timeout-seconds', 'inf'],
                      ['--continuation-timeout-seconds', '0'],
                      ['--continuation-timeout-seconds', '-1'],
                      ['--continuation-timeout-seconds', 'nan'],
                      ['--continuation-timeout-seconds', 'inf']):
            with self.subTest(extra=extra), redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                parse_args(base + extra)
        forced = parse_args(base + ['--parking-mode', 'forced', '--gateway-token-file', '/tmp/control-token'])
        self.assertEqual(parse_args(base).sqlite_transactions, 0)
        self.assertEqual(parse_args(base).sandbox_request_timeout_seconds, 180)
        self.assertEqual(parse_args(base).continuation_timeout_seconds, 180)
        self.assertEqual(parse_args(base + [
            '--continuation-timeout-seconds', '1800',
        ]).continuation_timeout_seconds, 1800)
        self.assertEqual(parse_args(base + [
            '--sandbox-request-timeout-seconds', '1800',
        ]).sandbox_request_timeout_seconds, 1800)
        self.assertEqual(forced.gateway_token_file, Path('/tmp/control-token'))
        self.assertNotIn('secret', safe_error(OSError('http://relay/_relay/secret/chat/completions')))

    def test_uploaded_tool_executes_and_rejects_corrupt_bytes(self):
        self.assertEqual(len(uploaded_tool_probe(64)), 64 * 1024)
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'usable-0.json').write_text('{"tool":"42"}')
            # Substitute only the guest path so this runs as a normal Linux test.
            body = uploaded_tool_probe(64).replace(b'/workspace/relay-bench/',
                                                   (directory + '/').encode())
            tool = root / 'tool.py'
            tool.write_bytes(body)
            digest = hashlib.sha256(body).hexdigest()
            command = [sys.executable, str(tool), '0', 'usable', digest]
            result = subprocess.run(command, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(json.loads(result.stdout), {'tool': '42'})
            tool.write_bytes(body + b'# corrupted\n')
            result = subprocess.run(command, capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn('tool upload corrupted', result.stderr)

    def test_fast_steady_state_cannot_hide_failed_or_missing_overlap_coverage(self):
        result = {
            'correct': True, 'configuration': {'startup_mode': 'rolling'},
            'response_ready_to_usable_exec_seconds': summary([.1] * 100),
            'response_ready_to_guest_continuation_seconds': summary([.1] * 100),
            'phase_continuation_seconds': {'during_provisioning': summary([.1]), 'after_provisioning': summary([.1])},
            'phase_latency_seconds': {
                'during_provisioning': summary([5.] * 10),
                'after_provisioning': summary([.1] * 100),
            },
        }
        self.assertFalse(meets_useful_action_slo(result, .8))
        result['phase_latency_seconds']['during_provisioning'] = summary([])
        self.assertFalse(meets_useful_action_slo(result, .8))
        result['phase_latency_seconds']['during_provisioning'] = summary([.2] * 10)
        self.assertTrue(meets_useful_action_slo(result, .8))
        result['fleet_health'] = {'enabled': True, 'passed': False}
        self.assertFalse(meets_useful_action_slo(result, .8))
        self.assertTrue(result['correct'], 'workload correctness is independent of fleet qualification')
        result['fleet_health'] = {'enabled': True, 'passed': True}
        self.assertTrue(meets_useful_action_slo(result, .8))
        result['phase_continuation_seconds']['during_provisioning'] = summary([.9])
        self.assertFalse(meets_useful_action_slo(result, 1))
        result['phase_continuation_seconds']['during_provisioning'] = summary([.1])
        result['response_ready_to_guest_continuation_seconds'] = summary([.9] * 100)
        self.assertFalse(meets_useful_action_slo(result, 1))
        result['correct'] = False
        self.assertFalse(meets_useful_action_slo(result, .8))

    def test_guest_exercises_memory_files_relay_and_tool_with_stable_identity(self):
        self._exercise_guest()

    def test_sqlite_wal_connections_content_and_recoverable_files_survive_cycles(self):
        self._exercise_guest(sqlite_transactions=4)

    def test_sqlite_content_change_fails_integrity_even_when_rows_survive(self):
        self._exercise_guest(sqlite_transactions=4, corrupt_sqlite=True)

    def _exercise_guest(self, sqlite_transactions=0, corrupt_sqlite=False):
        requests = []
        class Echo(BaseHTTPRequestHandler):
            def do_POST(self):
                body = self.rfile.read(int(self.headers['Content-Length']))
                requests.append((self.path, json.loads(body), self.headers['X-UCloud-Relay-Request-Id']))
                if corrupt_sqlite and self.path == '/relay' and json.loads(body)['cycle'] == 1:
                    with closing(sqlite3.connect(root / 'repository.sqlite')) as corruptor:
                        corruptor.execute("UPDATE changes SET payload=? WHERE cycle=0 AND transaction_id=0", (b'corrupted',))
                        corruptor.commit()
                if sqlite_transactions and self.path == '/observe' and json.loads(body)['cycle'] == 1:
                    # Capture while the final response is blocked: the guest's
                    # writer and old reader are still open and WAL is pinned.
                    shutil.copyfile(root / 'repository.sqlite', root / 'captured.sqlite')
                    shutil.copyfile(root / 'repository.sqlite-wal', root / 'captured.sqlite-wal')
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
                config = dict(resident_mb=1, dirty_mb=1, cycles=2, files=2, file_kib=4, payload_kib=1, cpu_ms=1,
                              sqlite_transactions=sqlite_transactions, sqlite_payload_bytes=8192)
                process = subprocess.Popen([sys.executable, '-c', AGENT], env={
                    **os.environ, 'BENCH_CONFIG': json.dumps(config), 'BENCH_ROOT': directory,
                    'RELAY_URL': f'http://127.0.0.1:{server.server_port}/relay',
                    'OBSERVER_URL': f'http://127.0.0.1:{server.server_port}/observe',
                }, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                try:
                    deadline = time.monotonic() + 10
                    while not (root / 'result-1.json').exists() and time.monotonic() < deadline and process.poll() is None:
                        time.sleep(.01)
                    if corrupt_sqlite:
                        _stdout, stderr = process.communicate(timeout=5)
                        self.assertNotEqual(process.returncode, 0)
                        self.assertIn(b'SQLite payload changed during park', stderr)
                        self.assertFalse((root / 'result-1.json').exists())
                        return
                    self.assertTrue((root / 'result-1.json').exists())
                    _stdout, stderr = process.communicate(timeout=5)
                    self.assertEqual(process.returncode, 0, stderr.decode())
                    results = [json.loads((root / f'result-{i}.json').read_text()) for i in range(2)]
                    usable = [json.loads((root / f'usable-{i}.json').read_text()) for i in range(2)]
                    self.assertEqual([r['tool'] for r in usable], ['42', '42'])
                    self.assertEqual([r['nonce'] for r in usable], [r['nonce'] for r in results])
                    self.assertEqual(results[0]['nonce'], results[1]['nonce'])
                    self.assertEqual(results[0]['pid'], results[1]['pid'])
                    self.assertNotEqual(results[0]['digest'], results[1]['digest'])
                    self.assertEqual([r['tool'] for r in results], ['42', '42'])
                    self.assertEqual([r['transport_retries'] for r in results], [0, 0])
                    for result in results:
                        self.assertGreaterEqual(result['verification_seconds'], 0)
                        self.assertGreater(result['tool_seconds'], 0)
                    self.assertEqual([r['sqlite_rows'] for r in results], [sqlite_transactions, 2 * sqlite_transactions])
                    if sqlite_transactions:
                        database = root / 'captured.sqlite'
                        self.assertGreater(Path(str(database) + '-wal').stat().st_size, 0)
                        # Reopen the exact files captured at the last model
                        # continuation, before normal process exit closes SQLite.
                        with TemporaryDirectory() as restored:
                            target = Path(restored) / database.name
                            shutil.copyfile(database, target)
                            with closing(sqlite3.connect(target)) as database_only:
                                self.assertEqual(database_only.execute('SELECT count(*) FROM changes').fetchone()[0], 0)
                            shutil.copyfile(str(database) + '-wal', str(target) + '-wal')
                            with closing(sqlite3.connect(target)) as reopened:
                                self.assertEqual(reopened.execute('PRAGMA integrity_check').fetchall(), [('ok',)])
                                rows = reopened.execute('SELECT cycle, transaction_id, payload FROM changes ORDER BY cycle, transaction_id').fetchall()
                            self.assertEqual(len(rows), 2 * sqlite_transactions)
                            digest = hashlib.sha256()
                            for cycle, transaction, value in rows:
                                expected = hashlib.shake_256(f"{results[0]['nonce']}:{cycle}:{transaction}".encode()).digest(8192)
                                self.assertEqual(value, expected)
                                digest.update(value)
                            self.assertEqual(digest.hexdigest(), results[-1]['sqlite_digest'])
                    else:
                        self.assertFalse((root / 'repository.sqlite').exists())
                    self.assertEqual([r[0] for r in requests], ['/relay', '/observe', '/relay', '/observe'])
                    self.assertNotEqual(requests[0][2], requests[2][2])
                    for model, receipt in zip(requests[::2], requests[1::2]):
                        self.assertEqual(receipt[1]['nonce'], model[1]['nonce'])
                        self.assertEqual(receipt[1]['digest'], model[1]['digest'])
                        self.assertEqual(receipt[1]['tool'], '42')
                finally:
                    if process.poll() is None:
                        process.terminate()
                    process.communicate(timeout=5)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)


class ProbeResponseDiagnosticTests(unittest.TestCase):
    def diagnostic(self, body, headers=None):
        from types import SimpleNamespace
        from scripts.live_relay_load_benchmark import probe_response_diagnostic

        async def run():
            stream = asyncio.StreamReader()
            stream.feed_data(body)
            stream.feed_eof()
            response = SimpleNamespace(status=503, headers=headers or {}, content=stream)
            result = await probe_response_diagnostic(response, redact=('private-token',))
            return result, len(await stream.read())
        return asyncio.run(run())

    def test_typed_error_and_trace_are_retained_without_auth_headers(self):
        evidence, remaining = self.diagnostic(
            b'{"error":"private-token","error_code":"http_request_capacity_exhausted","retryable":true}',
            {'X-Trace-Id': 'trace', 'Retry-After': '1', 'Server': 'nginx',
             'Authorization': 'Bearer private-token', 'Set-Cookie': 'private-token'},
        )
        self.assertEqual(evidence['status'], 503)
        self.assertEqual(evidence['error_code'], 'http_request_capacity_exhausted')
        self.assertTrue(evidence['retryable'])
        self.assertEqual(evidence['headers'], {'Server': 'nginx', 'Retry-After': '1', 'X-Trace-Id': 'trace'})
        self.assertNotIn('private-token', json.dumps(evidence))
        self.assertFalse(evidence['body_truncated'])
        self.assertEqual(remaining, 0)

    def test_non_json_ingress_failure_is_bounded_and_not_reinterpreted(self):
        evidence, remaining = self.diagnostic(b'<html>unavailable</html>' + b'x' * 8192)
        self.assertEqual(len(evidence['body_preview']), 4096)
        self.assertTrue(evidence['body_truncated'])
        self.assertNotIn('retryable', evidence)
        self.assertNotIn('error_code', evidence)
        self.assertEqual(remaining, len(b'<html>unavailable</html>') + 8192 - 4097)

    def test_stalled_preview_is_bounded_and_cancellation_propagates(self):
        from types import SimpleNamespace
        from scripts.live_relay_load_benchmark import probe_response_diagnostic

        async def run():
            response = SimpleNamespace(status=503, headers={}, content=asyncio.StreamReader())
            evidence = await probe_response_diagnostic(response)
            self.assertEqual(evidence['status'], 503)
            self.assertTrue(evidence['body_truncated'])
            self.assertIn('TimeoutError', evidence['body_error'])
            task = asyncio.create_task(probe_response_diagnostic(response))
            await asyncio.sleep(0)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        asyncio.run(asyncio.wait_for(run(), 1))
