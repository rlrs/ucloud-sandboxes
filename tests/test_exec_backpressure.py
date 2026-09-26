"""Lossless bounded output, including the real pipe/HTTP/client boundary."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
import sys
import time
import unittest

from tests.test_sandbox_exec import FakeSandboxManager, BlockingStdin, _install_session
from ucloud_sandboxes.sandbox_exec import ExecSessionManager, SandboxExecSpec


class ExecBackpressureTests(unittest.TestCase):
    def wait_for(self, predicate):
        deadline = time.monotonic() + 3
        while not predicate() and time.monotonic() < deadline:
            time.sleep(.005)
        self.assertTrue(predicate())

    def test_slow_reader_replays_lost_response_without_sequence_gaps(self):
        manager = ExecSessionManager(FakeSandboxManager(), max_events_per_session=8,
                                     output_idle_timeout_seconds=2)
        session = _install_session(manager, BlockingStdin())
        other = _install_session(manager, BlockingStdin())
        def produce():
            for i in range(1077):
                if not manager._append_stream_chunk(session.id, 'stdout', str(i)):
                    return
        with ThreadPoolExecutor(max_workers=1) as pool:
            producer = pool.submit(produce)
            self.wait_for(lambda: session.output_waiters)
            self.assertEqual(len(session.events), 8)
            first = manager.events_after(session.id, limit=4)
            # Simulate losing the HTTP response. Reading is not acknowledgment.
            self.assertEqual(manager.events_after(session.id, limit=4), first)
            self.assertTrue(manager._append_stream_chunk(other.id, 'stdout', 'independent'))
            self.assertEqual(manager.events_after(other.id)[0].data, 'independent')
            received = list(first)
            while len(received) < 1077:
                received.extend(manager.events_after(session.id, after=received[-1].sequence,
                                                      limit=4, wait_seconds=1))
                self.assertLessEqual(len(session.events), 8)
            producer.result(2)
        self.assertEqual([e.sequence for e in received], list(range(1, 1078)))
        self.assertEqual([e.data for e in received], [str(i) for i in range(1077)])

    def test_expiring_terminal_session_releases_a_late_output_waiter(self):
        from datetime import timedelta
        manager = ExecSessionManager(FakeSandboxManager(), max_sessions=1,
                                     max_events_per_session=1)
        session = _install_session(manager, BlockingStdin())
        manager._append_stream_chunk(session.id, 'stdout', 'retained')
        with manager._lock:
            session.status = 'exited'
            manager._touch_locked(session)
        with ThreadPoolExecutor(max_workers=1) as pool:
            late = pool.submit(manager._append_stream_chunk, session.id, 'stdout', 'late')
            self.wait_for(lambda: session.output_waiters)
            with manager._lock:
                session.updated_at -= timedelta(seconds=31)
                manager._make_session_room_locked()
            self.assertFalse(late.result(1))
        self.assertIsNone(manager.get(session.id))

    def test_child_exit_is_not_output_completion_while_reader_is_blocked(self):
        manager = ExecSessionManager(FakeSandboxManager(), max_events_per_session=2,
                                     output_idle_timeout_seconds=5)
        payload = 'x' * 24000
        session = manager.start(SandboxExecSpec('one', (sys.executable, '-c',
                                    "import sys; sys.stdout.write('x'*24000)")))
        self.wait_for(lambda: session.output_waiters)
        # Exceeds the old 2-second pump join: no premature terminal/empty read.
        time.sleep(2.2)
        self.assertEqual(session.status, 'running')
        self.assertIsNone(session.final_sequence)
        events = []
        deadline = time.monotonic() + 5
        while session.final_sequence is None or not events or events[-1].sequence < session.final_sequence:
            self.assertLess(time.monotonic(), deadline)
            events.extend(manager.events_after(session.id, after=events[-1].sequence if events else 0,
                                               wait_seconds=.1))
        self.assertEqual(''.join(e.data for e in events if e.stream == 'stdout'), payload)
        self.assertEqual([e.sequence for e in events], list(range(1, session.final_sequence + 1)))
        self.assertEqual(session.exit_code, 0)
        self.assertEqual(events[-1].stream, 'exit')

    def test_abandoned_reader_fails_explicitly_and_releases_leases(self):
        owner = FakeSandboxManager()
        manager = ExecSessionManager(owner, max_events_per_session=2,
                                     output_idle_timeout_seconds=.1)
        session = manager.start(SandboxExecSpec('one', (sys.executable, '-c',
            "import sys; sys.stdout.write('x'*1000000); sys.stdout.flush()")))
        self.wait_for(lambda: session.final_sequence is not None)
        events = manager.events_after(session.id)
        self.assertTrue(session.output_aborted)
        self.assertEqual(session.exit_code, 1)
        self.assertLessEqual(len(events), 4)  # bounded data plus error and exit
        self.assertEqual([e.sequence for e in events], list(range(1, len(events) + 1)))
        self.assertEqual(len([e for e in events if e.stream == 'error']), 1)
        self.assertIn('backpressure timed out', next(e.data for e in events if e.stream == 'error'))
        self.assertEqual(owner.capacity_released, ['capacity:one'])
        self.assertEqual(owner.lifecycle.released, ['one'])


class ExecDuplexHttpTests(unittest.TestCase):
    def test_sync_and_async_sdk_feed_stdin_while_draining_bounded_output(self):
        try:
            from ucloud_sandboxes_sdk import SandboxClient, AsyncSandboxClient
        except ImportError:
            self.skipTest('run with PYTHONPATH=ucloud-sandboxes-sdk/src for SDK integration')
        from types import SimpleNamespace
        from tests.test_control_plane import _running_server
        from ucloud_sandboxes.http_server import HighBacklogThreadingHTTPServer
        from ucloud_sandboxes.node_agent import NodeAgentHandler
        from ucloud_sandboxes.telemetry import Telemetry
        class Handler(NodeAgentHandler):
            def _check_node_control_authorized(self):
                return True
        Handler.exec_manager = ExecSessionManager(FakeSandboxManager(), max_events_per_session=4,
                                                  output_idle_timeout_seconds=5)
        Handler.manager = SimpleNamespace(consume_exec_start_timings=lambda: {})
        Handler.telemetry = Telemetry.disabled('test')
        Handler.node_control_bearer_token = ''
        Handler.sandboxes_enabled = True
        payload = 'price: €\n' * 50000
        # Emit stdout AND stderr before consuming more input. Sequential stdin
        # then output deadlocks once both the pipe and event buffer fill.
        command = [sys.executable, '-c',
            'import os\nwhile True:\n b=os.read(0,4096)\n if not b: break\n os.write(1,b)\n os.write(2,b)']
        server = HighBacklogThreadingHTTPServer(('127.0.0.1', 0), Handler)
        with _running_server(server) as url:
            client = SandboxClient(url)
            result = client.exec('one', command, input=payload, timeout_seconds=10)
            self.assertEqual(result.stdout, payload)
            self.assertEqual(result.stderr, payload)
            self.assertEqual(result.exit_code, 0)
            async def run():
                async with AsyncSandboxClient(url) as client:
                    return await client.exec('one', command, input=payload, timeout_seconds=10)
            result = asyncio.run(run())
            self.assertEqual(result.stdout, payload)
            self.assertEqual(result.stderr, payload)
            self.assertEqual(result.exit_code, 0)
