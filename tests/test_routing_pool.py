from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from tempfile import TemporaryDirectory
import os
import sqlite3
import threading
import unittest
from unittest.mock import patch
from tests.test_control_plane import _sandbox_route
from ucloud_sandboxes.routing import ExecRoute, RoutingStore


class RoutingPoolTests(unittest.TestCase):
    def test_new_and_reopened_sqlite_sidecars_remain_owner_only_with_open_umask(self):
        with TemporaryDirectory() as tmp:
            previous_umask = os.umask(0)
            try:
                path = Path(tmp) / 'routing.sqlite'
                store = RoutingStore(path)
                for _ in range(2):
                    with store._transaction() as conn:
                        conn.execute('CREATE TABLE IF NOT EXISTS permission_probe (value TEXT)')
                        for suffix in ('', '-wal', '-shm'):
                            self.assertEqual(Path(str(path) + suffix).stat().st_mode & 0o777, 0o600)
                    flusher = store._write_batches._thread
                    if flusher is not None:
                        flusher.join(timeout=3)
                        self.assertFalse(flusher.is_alive())
                    store._connection_finalizer()
                    self.assertFalse(Path(str(path) + '-wal').exists())
                    # Closing every connection deletes the sidecars; creating
                    # them again must retain the database's owner-only mode.
                    store = RoutingStore(path)
                path.chmod(0o644)
                store.get_sandbox('absent')
                self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            finally:
                os.umask(previous_umask)

    def test_lifecycle_update_preserves_spec_generation_and_storage_dependency(self):
        with TemporaryDirectory() as tmp:
            store = RoutingStore(Path(tmp) / 'routing.sqlite')
            route = store.upsert_sandbox(_sandbox_route(
                sandbox_id='live', state='running', node_id='node', job_id='job',
                node_url='http://node', node_epoch='boot',
            ))
            with store._transaction() as conn:
                conn.execute('CREATE TABLE redundant_writes (kind TEXT)')
                conn.execute("CREATE TRIGGER spec_rewrite AFTER UPDATE OF spec_json ON sandboxes BEGIN INSERT INTO redundant_writes VALUES ('spec'); END")
                conn.execute("CREATE TRIGGER generation_rewrite AFTER UPDATE ON sandbox_generation_hwm BEGIN INSERT INTO redundant_writes VALUES ('generation'); END")
                conn.execute('INSERT INTO sandbox_storage_dependencies VALUES (?, ?, ?)', ('live', route.generation, '{"marker":"keep"}'))
            updated = store.set_sandbox_state_if_current(
                route, expected_states={'running'}, state='running', node_epoch='boot',
                activity_epoch=route.activity_epoch + 1,
            )
            self.assertEqual(updated.spec, route.spec)
            self.assertEqual(store.get_sandbox('live'), updated)
            with store._connect() as conn:
                self.assertEqual(conn.execute('SELECT * FROM redundant_writes').fetchall(), [])
                self.assertEqual(conn.execute('SELECT storage_snapshot_json FROM sandbox_storage_dependencies').fetchone()[0], '{"marker":"keep"}')

    def test_combined_warm_readiness_and_dispatch_preserve_first_timestamps(self):
        with TemporaryDirectory() as tmp:
            store = RoutingStore(Path(tmp) / 'routing.sqlite')
            route = store.upsert_sandbox(_sandbox_route(
                sandbox_id='live', state='running', node_id='node', job_id='job', node_url='http://node',
            ))
            first, changed = store.upsert_program_request_transition_with_change(
                route, request_id='request', rollout_id='rollout', state='waking',
                response_ready_at='2026-09-22T00:00:00+00:00',
                transition_at='2026-09-22T00:00:01+00:00',
            )
            self.assertTrue(changed)
            self.assertEqual(first, store.program_request_readonly('request'))
            self.assertEqual(first.response_ready_at, '2026-09-22T00:00:00+00:00')
            self.assertEqual(first.wake_started_at, '2026-09-22T00:00:01+00:00')
            retry, changed = store.upsert_program_request_transition_with_change(
                route, request_id='request', rollout_id='rollout', state='waking',
                response_ready_at='2026-09-22T01:00:00+00:00',
            )
            self.assertFalse(changed)
            self.assertEqual(retry, first)

    def test_inventory_batches_unchanged_routes_but_keeps_fences_and_dependencies(self):
        from dataclasses import replace
        from ucloud_sandboxes.models import SandboxInventoryEntry, utc_now
        with TemporaryDirectory() as tmp:
            store = RoutingStore(Path(tmp) / 'routing.sqlite')
            routes = [store.upsert_sandbox(_sandbox_route(
                sandbox_id=f'live-{i}', state='running', node_id='node',
                job_id='job', node_url='http://node', node_epoch='boot',
            )) for i in range(64)]
            store.upsert_pending('live-0', routes[0].resources)
            observations = [SandboxInventoryEntry(
                r.sandbox_id, r.generation, r.create_operation_id, r.spec_hash,
                'parked' if i == 0 else 'running', r.resources, storage_dependency={},
            ) for i, r in enumerate(routes)]
            observed_at = utc_now().isoformat()
            kwargs = dict(node_id='node', job_id='job', node_epoch='boot',
                          activity_epoch=12, observed_at=observed_at,
                          reported_sandbox_ids=[r.sandbox_id for r in routes])
            with patch.object(store, '_write_sandbox', wraps=store._write_sandbox) as write:
                self.assertEqual(store.reconcile_sandboxes_for_node(
                    'http://node', observations, **kwargs,
                ), ([], []))
                self.assertEqual(write.call_count, 1)
            for i, route in enumerate(routes):
                self.assertEqual(store.get_sandbox(route.sandbox_id), replace(
                    route, state='parked' if i == 0 else 'running',
                    activity_epoch=12, updated_at=observed_at,
                ))
            self.assertEqual(store.pending_demand().pending_count, 0)
            with store._connect() as conn:
                self.assertEqual(conn.execute('SELECT COUNT(*) FROM sandbox_storage_dependencies').fetchone()[0], 64)
                self.assertEqual(conn.execute('SELECT COUNT(*) FROM sandbox_generation_hwm').fetchone()[0], 64)
            # A stale report must not move either state or watermark backwards.
            kwargs['activity_epoch'] = 11
            stale = [replace(o, state='running') for o in observations]
            store.reconcile_sandboxes_for_node('http://node', stale, **kwargs)
            self.assertEqual(store.get_sandbox('live-0').state, 'parked')
            self.assertEqual(store.get_sandbox('live-0').activity_epoch, 12)

    def test_repeated_image_warmup_observation_skips_writer_and_new_nodes_merge(self):
        from ucloud_sandboxes.models import ResourceQuantity
        with TemporaryDirectory() as tmp:
            store = RoutingStore(Path(tmp) / 'routing.sqlite')
            store.upsert_image_warmup(
                'warmup', 'image', ResourceQuantity(vcpu=1, memory_mb=512),
                count=256, ttl_seconds=600,
            )
            first = store.mark_image_warmup_node('warmup', 'node-1', expected_image='image')
            with ThreadPoolExecutor(max_workers=2) as pool:
                with store._transaction():
                    self.assertEqual(pool.submit(
                        store.mark_image_warmup_node, 'warmup', 'node-1', expected_image='image',
                    ).result(timeout=2), first)
                    self.assertIsNone(pool.submit(
                        store.mark_image_warmup_node, 'warmup', 'node-1', expected_image='stale-image',
                    ).result(timeout=2))
                with store._lock:
                    updates = [pool.submit(store.mark_image_warmup_node, 'warmup', node)
                               for node in ('node-2', 'node-3')]
                    for update in updates:
                        update.result(timeout=2)
            self.assertEqual(set(store.image_warmups()[0].warmed_node_ids), {'node-1', 'node-2', 'node-3'})

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

    def test_inventory_and_create_do_not_wait_for_fleet_projection(self):
        from ucloud_sandboxes.models import SandboxInventoryEntry, utc_now
        from ucloud_sandboxes.routing import SandboxRouteAllocation
        with TemporaryDirectory() as tmp:
            store = RoutingStore(Path(tmp) / 'routing.sqlite')
            route = store.upsert_sandbox(_sandbox_route(
                sandbox_id='live', state='running', node_id='node',
                job_id='job', node_url='http://node', node_epoch='boot',
            ))
            observation = SandboxInventoryEntry(
                route.sandbox_id, route.generation, route.create_operation_id,
                route.spec_hash, 'parked', route.resources,
            )
            with ThreadPoolExecutor(max_workers=2) as pool:
                with store._lock:
                    reconcile = pool.submit(
                        store.reconcile_sandboxes_for_node, route.node_url,
                        [observation], node_id=route.node_id, job_id=route.job_id,
                        reported_sandbox_ids=['live'], observed_at=utc_now().isoformat(),
                        node_epoch='boot', activity_epoch=1,
                    )
                    create = pool.submit(
                        store.allocate_sandbox_create_with_pending,
                        SandboxRouteAllocation(
                            sandbox_id='new', node_id='node', job_id='job',
                            node_url='http://node', resources=route.resources,
                            spec={'id': 'new'}, node_epoch='boot',
                        ), spec_hash='b' * 64,
                    )
                    self.assertEqual(reconcile.result(timeout=2), ([], []))
                    created, _ = create.result(timeout=2)
            self.assertEqual(store.get_sandbox('live').state, 'parked')
            self.assertEqual(store.get_sandbox('new'), created)

    def test_fleet_scan_queue_does_not_block_lifecycle_or_cache_old_routes(self):
        with TemporaryDirectory() as tmp:
            store = RoutingStore(Path(tmp) / 'routing.sqlite')
            route = store.upsert_sandbox(_sandbox_route(
                sandbox_id='live', state='parked', node_id='node',
                job_id='job', node_url='http://node',
            ))
            with ThreadPoolExecutor(max_workers=2) as pool:
                with store._fleet_read_lock:
                    listing = pool.submit(store.sandbox_routes_readonly, background=True)
                    pool.submit(store.reserve_sandbox_wake, route, pending_id='none').result(timeout=2)
                    self.assertEqual(store.get_sandbox('live').state, 'waking')
                    self.assertEqual(pool.submit(store.sandbox_routes_readonly).result(timeout=2)[0].state, 'waking')
                self.assertEqual(listing.result(timeout=2)[0].state, 'waking')
            RoutingStore(store.path).delete_sandbox('live')
            self.assertEqual(store.sandbox_routes_readonly(), [])

    def test_repeated_program_projection_reads_committed_generation_without_writer(self):
        from ucloud_sandboxes.routing import SandboxRouteConflictError
        with TemporaryDirectory() as tmp:
            store = RoutingStore(Path(tmp) / 'routing.sqlite')
            route = store.upsert_sandbox(_sandbox_route(
                sandbox_id='live', state='running', node_id='node',
                job_id='job', node_url='http://node',
            ))
            expected, changed = store.upsert_program_request_transition_with_change(
                route, request_id='request', rollout_id='rollout', state='model_wait',
                last_error='retry',
            )
            self.assertTrue(changed)
            before = store._write_batches.operations
            with ThreadPoolExecutor(max_workers=1) as pool:
                with store._transaction():
                    observed, changed = pool.submit(
                        store.upsert_program_request_transition_with_change,
                        route, request_id='request', rollout_id='rollout', state='model_wait',
                    ).result(timeout=2)
                    self.assertFalse(changed)
                    self.assertEqual(observed, expected)
            self.assertEqual(store._write_batches.operations, before + 1)
            cleared, changed = store.upsert_program_request_transition_with_change(
                route, request_id='request', rollout_id='rollout', state='model_wait',
                clear_error=True,
            )
            self.assertTrue(changed)
            self.assertEqual(cleared.last_error, '')
            store.delete_sandbox(route.sandbox_id)
            with self.assertRaises(SandboxRouteConflictError):
                store.upsert_program_request_transition_with_change(
                    route, request_id='request', rollout_id='rollout', state='model_wait',
                )
