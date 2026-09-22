from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
import os
import sqlite3
import threading
import time
import unittest
from unittest.mock import patch

from ucloud_sandboxes.routing import ExecRoute, RoutingStore, SandboxRouteConflictError
from ucloud_sandboxes.durable_batch import DurableSqliteBatch
from tests.test_routing import sandbox_route


class RoutingBatchTests(unittest.TestCase):
    def test_queued_writers_enter_in_order_across_commits(self):
        with TemporaryDirectory() as tmp, ThreadPoolExecutor(max_workers=9) as pool:
            path = Path(tmp) / 'fifo.sqlite'
            with closing(sqlite3.connect(path)) as conn:
                conn.execute('CREATE TABLE entries (value INTEGER)')
                conn.commit()
            committing, release = threading.Event(), threading.Event()
            class BlockedCommit(sqlite3.Connection):
                def commit(self):
                    committing.set()
                    if not release.wait(10):
                        raise TimeoutError('test did not release commit')
                    return super().commit()
            batch = DurableSqliteBatch(
                lambda: sqlite3.connect(path, check_same_thread=False, factory=BlockedCommit),
                lambda: None, delay_seconds=0, max_operations=1,
            )
            def write(value):
                with batch.transaction() as conn:
                    conn.execute('INSERT INTO entries VALUES (?)', (value,))
            first = pool.submit(write, -1)
            futures = []
            try:
                self.assertTrue(committing.wait(3))
                for i in range(8):
                    futures.append(pool.submit(write, i))
                    deadline = time.monotonic() + 3
                    while True:
                        with batch._writers_lock:
                            queued = len(batch._writers)
                        if queued == i + 1:
                            break
                        self.assertLess(time.monotonic(), deadline, 'writer did not queue')
                        time.sleep(0.001)
                self.assertFalse(any(f.done() for f in [first, *futures]))
            finally:
                release.set()
            for future in [first, *futures]:
                future.result(timeout=3)
            with closing(sqlite3.connect(path)) as conn:
                self.assertEqual(
                    [r[0] for r in conn.execute('SELECT value FROM entries ORDER BY rowid')],
                    [-1, *range(8)],
                )

    def wait_for_operations(self, batch, count):
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            with batch._condition:
                if batch._batch and batch._batch.operations == count:
                    return
            time.sleep(0.005)
        self.fail("batch did not accept operations")

    def test_shared_batch_keeps_reads_committed_and_conflicts_isolated(self):
        with TemporaryDirectory() as tmp, ThreadPoolExecutor(max_workers=8) as pool:
            store = RoutingStore(Path(tmp) / 'routing.sqlite')
            peer = RoutingStore(store.path)
            batch = store._write_batches
            self.assertIs(batch, peer._write_batches)
            batch.delay = 10
            route = ExecRoute('same', 'sandbox', 'node', 'job', 'http://node')
            first = pool.submit(store.upsert_exec, route)
            self.wait_for_operations(batch, 1)
            conflict = pool.submit(peer.upsert_exec,
                ExecRoute('same', 'other', 'node', 'job', 'http://node'))
            others = [pool.submit(peer.upsert_exec,
                ExecRoute(str(i), 'sandbox', 'node', 'job', 'http://node')) for i in range(6)]
            self.wait_for_operations(batch, 8)
            with batch._condition:
                self.assertIsNone(peer.get_exec('same'))
                self.assertFalse(any(f.done() for f in [first, conflict, *others]))
                batch._batch.deadline = 0
                batch._flush_condition.notify()
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
