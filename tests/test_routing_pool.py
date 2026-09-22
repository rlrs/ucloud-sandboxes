from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from tempfile import TemporaryDirectory
import os
import sqlite3
import threading
import unittest
from tests.test_control_plane import _sandbox_route
from ucloud_sandboxes.routing import ExecRoute, RoutingStore


class RoutingPoolTests(unittest.TestCase):
    def test_exec_commit_does_not_wait_for_fleet_projection_and_external_delete_wins(self):
        with TemporaryDirectory() as tmp:
            store = RoutingStore(Path(tmp) / 'routing.sqlite')
            route = ExecRoute('session', 'sandbox', 'node', 'job', 'http://node')
            with ThreadPoolExecutor(max_workers=1) as pool:
                with store._lock:
                    pool.submit(store.upsert_exec, route).result(timeout=2)
                    self.assertEqual(store.get_exec('session').sandbox_id, 'sandbox')
            other = RoutingStore(store.path)
            other.delete_exec('session')
            self.assertIsNone(store.get_exec('session'))
            self.assertIsNone(store.delete_exec('session'))

    def test_concurrent_readers_do_not_share_connections_or_weaken_durability(self):
        with TemporaryDirectory() as tmp:
            store = RoutingStore(Path(tmp) / 'routing.sqlite')
            barrier = threading.Barrier(24)
            def read(_):
                with store._connect() as conn:
                    identity = id(conn)
                    self.assertEqual(conn.execute('PRAGMA synchronous').fetchone()[0], 2)
                    barrier.wait(timeout=10)
                    return identity
            with ThreadPoolExecutor(max_workers=24) as pool:
                identities = list(pool.map(read, range(24)))
            self.assertEqual(len(set(identities)), 24)
            self.assertLessEqual(len(store._connections), 16)

    def test_returned_connections_have_no_uncommitted_state_or_read_snapshot(self):
        with TemporaryDirectory() as tmp:
            store = RoutingStore(Path(tmp) / 'routing.sqlite')
            route = store.upsert_sandbox(_sandbox_route(sandbox_id='local', state='parked', node_id='node', job_id='job', node_url='http://node'))
            with store._connect() as conn:
                conn.execute('BEGIN')
                conn.execute("UPDATE sandboxes SET state='running' WHERE sandbox_id='local'")
            self.assertEqual(store.get_sandbox('local').state, 'parked')
            with store._connect() as conn:
                conn.execute('BEGIN')
                self.assertEqual(conn.execute("SELECT state FROM sandboxes").fetchone()[0], 'parked')
            # A separately opened store's committed mutation is visible on the
            # next reused connection; no WAL snapshot escapes its context.
            RoutingStore(store.path).reserve_sandbox_wake(route, pending_id='none')
            self.assertEqual(store.get_sandbox('local').state, 'waking')

    def test_failed_transaction_does_not_poison_next_borrower(self):
        with TemporaryDirectory() as tmp:
            store = RoutingStore(Path(tmp) / 'routing.sqlite')
            store.upsert_sandbox(_sandbox_route(sandbox_id='local', state='parked', node_id='node', job_id='job', node_url='http://node'))
            with self.assertRaisesRegex(RuntimeError, 'injected'):
                with store._transaction() as conn:
                    conn.execute("UPDATE sandboxes SET state='running'")
                    raise RuntimeError('injected')
            self.assertEqual(store.get_sandbox('local').state, 'parked')

    def test_database_replacement_is_rejected(self):
        with TemporaryDirectory() as tmp:
            store = RoutingStore(Path(tmp) / 'routing.sqlite')
            other = Path(tmp) / 'other.sqlite'
            other.touch(mode=0o600)
            os.replace(other, store.path)
            with self.assertRaisesRegex(sqlite3.DatabaseError, 'replaced'):
                store.get_sandbox('absent')
