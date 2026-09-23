from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
import sqlite3
import unittest

from ucloud_sandboxes.models import ResourceQuantity
from ucloud_sandboxes.routing import RoutingStore, SandboxRoute, ExecRoute, SandboxRouteConflictError
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

    def test_concurrent_exec_writes_survive_one_conflict(self):
        first = ExecRoute(session_id='e0', sandbox_id='s', node_id='n', job_id='j', node_url='http://node')
        self.writer.upsert_exec(first)
        with self.assertRaises(SandboxRouteConflictError):
            self.writer.upsert_exec(replace(first, sandbox_id='wrong'))
        with ThreadPoolExecutor(max_workers=12) as executor:
            list(executor.map(lambda i: self.writer.upsert_exec(replace(first, session_id=f'e{i}')), range(32)))
        external = RoutingStore(self.store.path)
        self.assertTrue(all(external.get_exec(f'e{i}').sandbox_id == 's' for i in range(32)))

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
                'test_relay_lifecycle_persists_program_request_transitions',
                'test_successful_exec_implicitly_commits_parked_route_wake',
            ):
                with self.subTest(name=name):
                    case = cases.ControlPlaneTests(name)
                    result = unittest.TestResult()
                    case.run(result)
                    self.assertFalse(result.errors + result.failures, result.errors + result.failures)
