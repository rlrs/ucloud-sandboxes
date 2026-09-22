from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
import os
import sqlite3
import unittest
from unittest.mock import patch

from ucloud_sandboxes.routing import ExecRoute, RoutingStore, SandboxRouteConflictError
from tests.test_routing import sandbox_route


class RoutingBatchTests(unittest.TestCase):
    def test_shared_batch_keeps_reads_committed_and_conflicts_isolated(self):
        with TemporaryDirectory() as tmp, ThreadPoolExecutor(max_workers=8) as pool:
            store = RoutingStore(Path(tmp) / 'routing.sqlite')
            peer = RoutingStore(store.path)
            batch = store._write_batches
            self.assertIs(batch, peer._write_batches)
            batch.delay = 10
            route = ExecRoute('same', 'sandbox', 'node', 'job', 'http://node')
            first = pool.submit(store.upsert_exec, route)
            with batch._condition:
                self.assertTrue(batch._condition.wait_for(
                    lambda: batch._batch and batch._batch.operations == 1, timeout=3))
            conflict = pool.submit(peer.upsert_exec,
                ExecRoute('same', 'other', 'node', 'job', 'http://node'))
            others = [pool.submit(peer.upsert_exec,
                ExecRoute(str(i), 'sandbox', 'node', 'job', 'http://node')) for i in range(6)]
            with batch._condition:
                self.assertTrue(batch._condition.wait_for(
                    lambda: batch._batch and batch._batch.operations == 8, timeout=3))
                self.assertIsNone(peer.get_exec('same'))
                self.assertFalse(any(f.done() for f in [first, conflict, *others]))
                batch._batch.deadline = 0
                batch._condition.notify_all()
            first.result(timeout=3)
            with self.assertRaises(SandboxRouteConflictError):
                conflict.result(timeout=3)
            for future in others:
                future.result(timeout=3)
            self.assertEqual(peer.get_exec('same').sandbox_id, 'sandbox')
            self.assertEqual(batch.commits, 1)
            with closing(sqlite3.connect(store.path)) as conn:
                self.assertEqual(conn.execute('SELECT count(*) FROM exec_sessions').fetchone()[0], 7)
                self.assertEqual(conn.execute('PRAGMA integrity_check').fetchone()[0], 'ok')
            batch.delay = 0.001
            peer.delete_exec('same')
            self.assertIsNone(store.get_exec('same'))

    def test_writer_rejects_fork_and_replaced_database(self):
        with TemporaryDirectory() as tmp:
            store = RoutingStore(Path(tmp) / 'routing.sqlite')
            route = ExecRoute('session', 'sandbox', 'node', 'job', 'http://node')
            with patch('ucloud_sandboxes.routing.os.getpid', return_value=-1):
                with self.assertRaisesRegex(sqlite3.DatabaseError, 'after fork'):
                    store.upsert_exec(route)
            replacement = Path(tmp) / 'replacement.sqlite'
            replacement.touch()
            os.replace(replacement, store.path)
            with self.assertRaisesRegex(sqlite3.DatabaseError, 'replaced'):
                store.upsert_exec(route)


    def test_program_transition_uses_generation_fence_without_decoding_large_route(self):
        with TemporaryDirectory() as tmp:
            store = RoutingStore(Path(tmp) / 'routing.sqlite')
            route = store.upsert_sandbox(sandbox_route(
                sandbox_id='agent', node_id='node', job_id='job', node_url='http://node',
                spec={'id': 'agent', 'environment': {'large': 'x' * 131072}}))
            with patch('ucloud_sandboxes.routing._sandbox_route_from_row',
                       side_effect=AssertionError('unnecessary full route decoding')):
                state, changed = store.upsert_program_request_transition_with_change(
                    route, request_id='request', rollout_id='rollout', state='waking')
                self.assertTrue(changed)
                self.assertEqual(state.sandbox_generation, route.generation)
                for stale in (replace(route, generation=route.generation + 1),
                              replace(route, sandbox_id='absent', spec={'id': 'absent'})):
                    with self.assertRaises(SandboxRouteConflictError):
                        store.upsert_program_request_transition_with_change(
                            stale, request_id='request', rollout_id='rollout', state='acting')
            self.assertEqual(store.program_request_readonly('request').state, 'waking')
