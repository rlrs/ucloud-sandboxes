from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
import sqlite3
import time
import unittest
from unittest.mock import patch

from ucloud_sandboxes.models import ResourceQuantity, SandboxInventoryEntry, utc_now
from ucloud_sandboxes.routing import RoutingStore, SandboxRoute, SandboxRouteAllocation, ExecRoute, SandboxRouteConflictError
from ucloud_sandboxes.routing_writer import RoutingWriteProcess


class RoutingWriterTests(unittest.TestCase):
    def setUp(self):
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store = RoutingStore(Path(self.tmp.name) / 'routes.sqlite')
        self.route = SandboxRoute(
            sandbox_id='s', node_id='n', job_id='j', node_url='http://node',
            resources=ResourceQuantity(memory_mb=1024), spec={'id': 's'},
            state='running', generation=1, create_operation_id='create',
            spec_hash='a' * 64, node_epoch='boot', activity_epoch=1,
        )
        self.store.upsert_sandbox(self.route)
        self.writer = RoutingWriteProcess(self.store)
        self.addCleanup(self.writer.close)

    def test_acknowledgement_is_durable_and_wake_projection_is_atomic(self):
        updated, (program, changed) = self.writer.confirm_sandbox_wake(
            self.route, node_epoch='boot', activity_epoch=2,
            program_transition=dict(request_id='request', rollout_id='rollout', state='acting'),
        )
        external = RoutingStore(self.store.path)
        self.assertEqual(external.get_sandbox_readonly('s'), updated)
        self.assertEqual(updated.activity_epoch, 2)
        self.assertTrue(changed)
        self.assertEqual(program.state, 'acting')
        with self.assertRaises(ValueError):
            self.writer.confirm_sandbox_wake(
                updated, node_epoch='boot', activity_epoch=3,
                program_transition=dict(request_id='', rollout_id='rollout', state='acting'),
            )
        self.assertEqual(external.get_sandbox_readonly('s').activity_epoch, 2)

    def test_create_confirmation_and_wake_share_durable_writer_fences(self):
        allocation = SandboxRouteAllocation(
            sandbox_id='created', node_id='n', job_id='j', node_url='http://node',
            resources=self.route.resources, spec={'id': 'created'}, node_epoch='boot',
        )
        created, pending = self.writer.allocate_sandbox_create_with_pending(
            allocation, spec_hash='b' * 64,
        )
        self.assertIsNone(pending)
        external = RoutingStore(self.store.path)
        self.assertEqual(external.get_sandbox_readonly('created'), created)
        again, _ = self.writer.allocate_sandbox_create_with_pending(allocation, spec_hash='b' * 64)
        self.assertEqual(again, created)
        parked = self.writer.upsert_sandbox(replace(created, state='parked'))
        self.assertEqual(external.get_sandbox_readonly('created'), parked)
        waking = self.writer.reserve_sandbox_wake(parked, pending_id='wake-created')
        self.assertEqual(external.get_sandbox_readonly('created'), waking)
        self.assertEqual(waking.state, 'waking')
        external.delete_sandbox('created')
        recreated, _ = self.writer.allocate_sandbox_create_with_pending(allocation, spec_hash='b' * 64)
        self.assertGreater(recreated.generation, created.generation)
        self.assertIsNone(self.writer.reserve_sandbox_wake(parked, pending_id='stale'))
        self.assertEqual(external.get_sandbox_readonly('created'), recreated)

    def test_external_generation_change_is_seen_by_child(self):
        self.store.delete_sandbox('s')
        self.store.upsert_sandbox(replace(self.route, generation=2, create_operation_id='new'))
        self.assertEqual(self.store.get_sandbox_readonly('s').generation, 2)
        updated, _ = self.writer.confirm_sandbox_wake(
            self.route, node_epoch='boot', activity_epoch=2,
        )
        self.assertIsNone(updated)
        with self.assertRaises(SandboxRouteConflictError):
            self.writer.upsert_program_request_transition_with_change(
                self.route, request_id='request', rollout_id='rollout', state='acting',
            )

    def test_inventory_iterators_reconcile_in_child_and_reject_stale_generations(self):
        for generation, activity in ((1, 3), (2, 5)):
            observed = SandboxInventoryEntry(
                sandbox_id='s', generation=generation, operation_id='create',
                spec_hash='a' * 64, state='parked', resources=self.route.resources,
            )
            removed, stale = self.writer.reconcile_sandboxes_for_node(
                'http://node', (item for item in [observed]),
                node_id='n', job_id='j', reported_sandbox_ids=(value for value in ['s']),
                observed_at=utc_now().isoformat(), node_epoch='boot', activity_epoch=activity,
            )
            self.assertEqual((removed, stale), ([], []))
            stored = RoutingStore(self.store.path).get_sandbox_readonly('s')
            self.assertEqual((stored.state, stored.generation, stored.activity_epoch), ('parked', 1, 3))

    def test_concurrent_exec_writes_survive_one_conflict(self):
        first = ExecRoute(session_id='e0', sandbox_id='s', node_id='n', job_id='j', node_url='http://node')
        self.writer.upsert_exec(first)
        with self.assertRaises(SandboxRouteConflictError):
            self.writer.upsert_exec(replace(first, sandbox_id='wrong'))
        with ThreadPoolExecutor(max_workers=12) as executor:
            list(executor.map(lambda i: self.writer.upsert_exec(replace(first, session_id=f'e{i}')), range(32)))
        external = RoutingStore(self.store.path)
        self.assertTrue(all(external.get_exec(f'e{i}').sandbox_id == 's' for i in range(32)))

    def test_queued_writes_share_ipc_without_acknowledging_before_commit(self):
        first = ExecRoute(session_id='existing', sandbox_id='s', node_id='n', job_id='j', node_url='http://node')
        self.writer.upsert_exec(first)
        # Hold SQLite's writer fence so arrivals collect behind one in-flight
        # command. Neither queuing nor IPC submission may acknowledge a write.
        with sqlite3.connect(self.store.path) as blocked, patch.object(
            self.writer._executor, 'submit', wraps=self.writer._executor.submit,
        ) as submit, ThreadPoolExecutor(max_workers=34) as pool:
            blocked.execute('BEGIN IMMEDIATE')
            futures = []
            try:
                futures = [pool.submit(self.writer.upsert_exec, replace(first, session_id=f'b{i}')) for i in range(33)]
                conflict = pool.submit(self.writer.upsert_exec, replace(first, sandbox_id='wrong'))
                deadline = time.monotonic() + 5
                while time.monotonic() < deadline:
                    with self.writer._guard:
                        if len(self.writer._pending) >= 2:
                            break
                    time.sleep(.005)
                self.assertFalse(any(f.done() for f in futures))
                self.assertIsNone(self.store.get_exec('b0'))
            finally:
                blocked.rollback()
            for future in futures:
                future.result(5)
            with self.assertRaises(SandboxRouteConflictError):
                conflict.result(5)
            envelopes = [call.args[1] for call in submit.call_args_list]
            self.assertTrue(any(len(batch) > 1 for batch in envelopes))
            self.assertLess(len(envelopes), 34)
        external = RoutingStore(self.store.path)
        self.assertTrue(all(external.get_exec(f'b{i}').sandbox_id == 's' for i in range(33)))
        self.assertEqual(external.get_exec('existing').sandbox_id, 's')

    def test_missing_or_replaced_file_fails_closed(self):
        self.store.path.rename(self.store.path.with_suffix('.saved'))
        with self.assertRaises(FileNotFoundError):
            self.writer.upsert_exec(ExecRoute(session_id='e', sandbox_id='s', node_id='n', job_id='j', node_url='http://node'))
        self.assertFalse(self.store.path.exists())
        self.store.path.write_text('replacement')
        with self.assertRaises(sqlite3.DatabaseError):
            self.writer.confirm_sandbox_wake(self.route, node_epoch='boot', activity_epoch=2)
        self.assertEqual(self.store.path.read_text(), 'replacement')

    def test_process_loss_does_not_acknowledge_or_replay(self):
        child = next(iter(self.writer._executor._processes.values()))
        child.kill()
        child.join(5)
        with self.assertRaises(sqlite3.DatabaseError):
            self.writer.confirm_sandbox_wake(self.route, node_epoch='boot', activity_epoch=2)
        self.assertEqual(self.store.get_sandbox_readonly('s').activity_epoch, 1)
        self.assertTrue(self.writer.health_error())
        self.writer.close()
        with self.assertRaises(RuntimeError):
            self.writer.confirm_sandbox_wake(self.route, node_epoch='boot', activity_epoch=2)

    def test_process_loss_fails_inflight_and_queued_callers_without_replay(self):
        route = ExecRoute(session_id='lost', sandbox_id='s', node_id='n', job_id='j', node_url='http://node')
        with sqlite3.connect(self.store.path) as blocked, ThreadPoolExecutor(max_workers=16) as pool:
            blocked.execute('BEGIN IMMEDIATE')
            try:
                futures = [pool.submit(self.writer.upsert_exec, replace(route, session_id=f'lost{i}')) for i in range(16)]
                deadline = time.monotonic() + 5
                while time.monotonic() < deadline:
                    with self.writer._guard:
                        if self.writer._pending:
                            break
                    time.sleep(.005)
                child = next(iter(self.writer._executor._processes.values()))
                child.kill()
                child.join(5)
                for future in futures:
                    with self.assertRaises(sqlite3.DatabaseError):
                        future.result(5)
            finally:
                blocked.rollback()
        self.assertTrue(self.writer.health_error())
        self.assertTrue(all(self.store.get_exec(f'lost{i}') is None for i in range(16)))


class GatewayRoutingWriterTests(unittest.TestCase):
    def test_gateway_lifecycle_and_exec_with_process_writer(self):
        from unittest.mock import patch
        from tests import test_control_plane as cases
        original = cases.build_server
        def build(*args, **kwargs):
            kwargs['isolate_routing_writes'] = True
            return original(*args, **kwargs)
        with patch.object(cases, 'build_server', side_effect=build):
            for name in (
                'test_gateway_persists_route_before_node_create_finishes',
                'test_gateway_fences_tool_traffic_until_direct_create_is_owned',
                'test_relay_lifecycle_persists_program_request_transitions',
                'test_successful_exec_implicitly_commits_parked_route_wake',
                'test_gateway_stamps_heartbeat_receipt_time_and_enforces_deployment',
                'test_heartbeat_identity_is_bound_to_authoritative_route',
            ):
                with self.subTest(name=name):
                    case = cases.ControlPlaneTests(name)
                    result = unittest.TestResult()
                    case.run(result)
                    self.assertFalse(result.errors + result.failures, result.errors + result.failures)
