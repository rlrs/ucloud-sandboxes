from ucloud_sandboxes.transition_admission import MemoryDemand
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path
import socket
import sqlite3
import struct
import tempfile
import threading
import time
import unittest

from ucloud_sandboxes.background_io import BackgroundPacer, Pressure, PressureSampler
from ucloud_sandboxes.durable_batch import DurableSqliteBatch
from ucloud_sandboxes.storage_native import AgentEnvUblkClient
from ucloud_sandboxes.warm_park import WarmParkPolicy


class GroupCommitTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / "journal"
        with closing(self.connect()) as conn:
            conn.execute("CREATE TABLE values_test (id INTEGER PRIMARY KEY)")

    def connect(self):
        c = sqlite3.connect(self.path, isolation_level=None, check_same_thread=False)
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("PRAGMA synchronous=FULL")
        return c

    def read(self):
        with closing(self.connect()) as c:
            return [
                row[0] for row in c.execute("SELECT id FROM values_test ORDER BY id")
            ]

    def wait_for_operations(self, batch, count):
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            with batch._condition:
                if batch._batch is not None and batch._batch.operations == count:
                    return
            time.sleep(0.005)
        self.fail("batch did not accept operations")

    def batch_ready(self, batch, count):
        self.wait_for_operations(batch, count)
        with batch._condition:
            self.assertEqual(self.read(), [])
            batch._batch.deadline = 0
            batch._flush_condition.notify()

    def test_concurrent_closed_batches_make_durable_progress(self):
        batch = DurableSqliteBatch(self.connect, lambda: None, max_operations=2)
        barrier = threading.Barrier(64)

        def write(i):
            barrier.wait(10)
            for j in range(4):
                with batch.transaction() as conn:
                    conn.execute("INSERT INTO values_test VALUES (?)", (i * 4 + j,))
                with closing(self.connect()) as reader:
                    self.assertIsNotNone(reader.execute("SELECT id FROM values_test WHERE id=?", (i * 4 + j,)).fetchone())

        with ThreadPoolExecutor(max_workers=64) as pool:
            futures = [pool.submit(write, i) for i in range(64)]
            for future in futures:
                future.result(20)
        self.assertEqual(self.read(), list(range(256)))

    def test_batch_is_invisible_until_durable_and_bad_operation_is_isolated(self):
        batch = DurableSqliteBatch(self.connect, lambda: None, delay_seconds=10)

        def write(i):
            with batch.transaction() as conn:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute("INSERT INTO values_test VALUES (?)", (i,))
                if i == 3:
                    raise ValueError("bad operation")
                conn.commit()

        with ThreadPoolExecutor(max_workers=8) as pool:
            futures = [pool.submit(write, i) for i in range(8)]
            self.batch_ready(batch, 8)
            for i, future in enumerate(futures):
                if i == 3:
                    with self.assertRaisesRegex(ValueError, "bad operation"):
                        future.result(3)
                else:
                    future.result(3)
        self.assertEqual(self.read(), [0, 1, 2, 4, 5, 6, 7])
        self.assertEqual(batch.commits, 1)
        self.assertEqual(batch.operations, 8)

    def test_commit_failure_fails_all_waiters_and_next_batch_can_recover(self):
        class FailingConnection:
            def __init__(self, connection):
                self.connection = connection

            def __getattr__(self, name):
                return getattr(self.connection, name)

            def commit(self):
                raise sqlite3.OperationalError("injected fsync failure")

        batch = DurableSqliteBatch(
            lambda: FailingConnection(self.connect()), lambda: None, delay_seconds=10
        )

        def write(i):
            with batch.transaction() as conn:
                conn.execute("INSERT INTO values_test VALUES (?)", (i,))

        with ThreadPoolExecutor(max_workers=4) as pool:
            futures = [pool.submit(write, i) for i in range(4)]
            self.batch_ready(batch, 4)
            for future in futures:
                with self.assertRaisesRegex(sqlite3.OperationalError, "fsync"):
                    future.result(3)
        self.assertEqual(self.read(), [])
        batch.connect, batch.delay = self.connect, 0
        write(9)
        self.assertEqual(self.read(), [9])

    def test_sqlite_transaction_abort_fails_other_unacknowledged_writes(self):
        batch = DurableSqliteBatch(self.connect, lambda: None, delay_seconds=10)

        def write(rollback=False):
            with batch.transaction() as conn:
                conn.execute(
                    "INSERT OR ROLLBACK INTO values_test VALUES (1)"
                    if rollback
                    else "INSERT INTO values_test VALUES (1)"
                )

        with ThreadPoolExecutor() as pool:
            first = pool.submit(write)
            self.wait_for_operations(batch, 1)
            with self.assertRaises(sqlite3.Error):
                write(True)
            with self.assertRaises(sqlite3.Error):
                first.result(2)
        self.assertEqual(self.read(), [])
        batch.delay = 0
        write()
        self.assertEqual(self.read(), [1])

    def test_ack_waits_for_commit_and_metrics_do_not(self):
        entered, release = threading.Event(), threading.Event()

        class SlowConnection:
            def __init__(self, connection):
                self.connection = connection

            def __getattr__(self, name):
                return getattr(self.connection, name)

            def commit(self):
                entered.set()
                if not release.wait(3):
                    raise TimeoutError("test release")
                self.connection.commit()

        batch = DurableSqliteBatch(lambda: SlowConnection(self.connect()), lambda: None)

        def write():
            with batch.transaction() as conn:
                conn.execute("INSERT INTO values_test VALUES (1)")

        with ThreadPoolExecutor() as pool:
            future = pool.submit(write)
            try:
                self.assertTrue(entered.wait(2))
                self.assertFalse(future.done())
                self.assertEqual(self.read(), [])
                self.assertEqual(batch.metrics()["journal_batch_commits"], 0)
            finally:
                release.set()
            future.result(3)
        self.assertEqual(self.read(), [1])

    def test_uncommitted_explicit_operation_is_rolled_back(self):
        batch = DurableSqliteBatch(self.connect, lambda: None)
        with batch.transaction() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute("INSERT INTO values_test VALUES (1)")
        self.assertEqual(self.read(), [])


class BackgroundSchedulingTests(unittest.TestCase):
    def test_pacing_tracks_pressure_and_resumes_without_a_rate_ceiling(self):
        now, waits, observed = [0.0], [], [Pressure(io_stall=75)]

        def sleep(value):
            waits.append(value)
            now[0] += value

        pacer = BackgroundPacer(lambda: observed[0], clock=lambda: now[0], sleep=sleep)
        now[0] += 0.01
        pacer.pace()
        self.assertAlmostEqual(waits[0], 0.03)
        observed[0] = Pressure(io_stall=0)
        now[0] += 0.01
        pacer.pace()
        self.assertEqual(len(waits), 1)
        observed[0] = Pressure(io_stall=100)
        now[0] += 5
        pacer.pace()
        self.assertLessEqual(waits[-1], 0.1)

    def test_foreground_work_receives_priority_before_psi_catches_up(self):
        now, waits = [0.0], []
        pacer = BackgroundPacer(
            lambda: Pressure(io_stall=0),
            foreground=lambda: True,
            clock=lambda: now[0],
            sleep=waits.append,
        )
        now[0] = 0.02
        pacer.pace()
        self.assertEqual(waits, [0.02])



    def test_wake_cancels_grace_and_generation_does_not_cross(self):
        policy = WarmParkPolicy(lambda: Pressure(0.8, 0))
        key = ("sandbox", 1, "request")
        ready = threading.Event()

        def park():
            with policy.defer(key) as event:
                ready.set()
                return event.is_set()

        with ThreadPoolExecutor() as pool:
            future = pool.submit(park)
            deadline = time.monotonic() + 2
            while key not in policy._pending and time.monotonic() < deadline:
                time.sleep(0.001)
            self.assertIn(key, policy._pending)
            policy.wake(("sandbox", 2, "request"))
            self.assertFalse(ready.is_set())
            policy.wake(key)
            self.assertTrue(future.result(2))
        self.assertFalse(policy._pending)


    def test_pressure_ends_grace_and_missing_pressure_does_not_delay_reclaim(self):
        state = [Pressure(0.8, 0)]
        policy = WarmParkPolicy(lambda: state[0])

        def park():
            with policy.defer("request") as event:
                return event.is_set()

        with ThreadPoolExecutor() as pool:
            future = pool.submit(park)
            deadline = time.monotonic() + 2
            while "request" not in policy._pending and time.monotonic() < deadline:
                time.sleep(0.001)
            state[0] = Pressure(0.02, 10)
            self.assertFalse(future.result(1))
        with tempfile.TemporaryDirectory() as directory:
            missing = PressureSampler(Path(directory))
            policy = WarmParkPolicy(missing.sample)
            self.assertTrue(policy._needs_reclaim(missing.sample(), MemoryDemand()))

    def test_native_control_wait_uses_export_progress_but_stalls_expire(self):
        for progressing in [True, False]:
            with (
                self.subTest(progressing=progressing),
                tempfile.TemporaryDirectory() as directory,
            ):
                path = Path(directory) / "socket"
                last = [time.monotonic()]
                server = socket.socket(socket.AF_UNIX)
                self.addCleanup(server.close)
                server.bind(str(path))
                server.listen()

                def serve():
                    conn, _ = server.accept()
                    with conn:
                        size = struct.unpack(
                            ">I", AgentEnvUblkClient._recv_exact(conn, 4)
                        )[0]
                        AgentEnvUblkClient._recv_exact(conn, size)
                        for _ in range(10):
                            time.sleep(0.015)
                            if progressing:
                                last[0] = time.monotonic()
                        try:
                            conn.sendall(struct.pack(">I", 15) + b'{"status":"ok"}')
                        except BrokenPipeError:
                            pass

                thread = threading.Thread(target=serve)
                thread.start()
                try:
                    client = AgentEnvUblkClient(path, timeout_seconds=0.05)
                    if progressing:
                        self.assertEqual(
                            client._call({"kind": "test"}, progress=lambda: last[0]),
                            {"status": "ok"},
                        )
                    else:
                        with self.assertRaises(socket.timeout):
                            client._call({"kind": "test"}, progress=lambda: last[0])
                finally:
                    thread.join(2)


class WarmDemandTests(unittest.TestCase):

    def test_queued_wake_demand_ends_an_existing_warm_wait(self):
        incoming = [0]
        policy = WarmParkPolicy(lambda: Pressure(.8, 0, 0, 1024),
                                demand=lambda: MemoryDemand(incoming[0], incoming[0]))
        def wait():
            with policy.defer('sandbox') as event:
                return event.is_set()
        with ThreadPoolExecutor() as pool:
            waiting = pool.submit(wait)
            deadline = time.monotonic()+2
            while 'sandbox' not in policy._pending and time.monotonic()<deadline:
                time.sleep(.001)
            self.assertIn('sandbox', policy._pending)
            incoming[0] = 1024
            self.assertFalse(waiting.result(timeout=1))


class PressureDrivenWarmParkTests(unittest.TestCase):
    def test_spare_memory_retains_long_model_wait_without_an_infinite_retry(self):
        from unittest.mock import patch
        from ucloud_sandboxes.warm_park import WarmParkDeferred
        pressure = [Pressure(.8, 0, 0, 80 * 1024**3)]
        incoming = [0]
        policy = WarmParkPolicy(lambda: pressure[0], demand=lambda: MemoryDemand(incoming[0], incoming[0]))
        for now in (0, 20, 60, 600):
            with patch('ucloud_sandboxes.warm_park.time.monotonic', return_value=now):
                with self.assertRaises(WarmParkDeferred) as deferred:
                    with policy.defer('request', memory_bytes=1024**3, blocking=False):
                        self.fail('elapsed wall time alone must not force a checkpoint')
                self.assertEqual(deferred.exception.seconds, 3)
        incoming[0] = 80 * 1024**3
        with policy.defer('request', memory_bytes=1024**3, blocking=False):
            pass

    def test_reclaim_or_low_headroom_releases_retention_and_wake_is_fenced(self):
        from unittest.mock import patch
        from ucloud_sandboxes.warm_park import WarmParkDeferred
        pressure = [Pressure(.8, 0, 0, 80 * 1024**3)]
        policy = WarmParkPolicy(lambda: pressure[0])
        key = ('sandbox', 1, 'request')
        with patch('ucloud_sandboxes.warm_park.time.monotonic', return_value=10):
            with self.assertRaises(WarmParkDeferred):
                with policy.defer(key, blocking=False):
                    pass
        policy.wake(('sandbox', 2, 'request'))
        self.assertIn(key, policy._waiting_since)
        for low in (Pressure(.05, 0, 0, 1024), Pressure(.8, 10, 0, 80 * 1024**3)):
            pressure[0] = low
            policy._settle_until = 0
            policy._retry_after.clear()
            with policy.defer(key, blocking=False):
                pass
        policy.wake(key)
        self.assertNotIn(key, policy._waiting_since)


class ResidentWaitReclaimTests(unittest.TestCase):
    GIB = 1024**3

    def retain(self, policy, key, memory_bytes=0):
        from ucloud_sandboxes.warm_park import WarmParkDeferred
        with self.assertRaises(WarmParkDeferred):
            with policy.defer(key, memory_bytes=memory_bytes, blocking=False):
                self.fail('wait should stay resident')

    def test_512_old_waits_use_available_memory_without_checkpoint_expiry(self):
        from unittest.mock import patch
        policy = WarmParkPolicy(lambda: Pressure(.20, 1, 0, 20*self.GIB))
        with patch('ucloud_sandboxes.warm_park.time.monotonic', return_value=10):
            for key in range(512):
                self.retain(policy, key, 4*self.GIB)
        # Large configured limits are not another resident-memory charge.
        with patch('ucloud_sandboxes.warm_park.time.monotonic', return_value=3610):
            for key in range(512):
                self.assertFalse(policy.ready(key, memory_bytes=4*self.GIB))
        self.assertEqual(policy.snapshot()['resident_waits'], 512)
        self.assertEqual(policy.snapshot()['checkpoints_completed'], 0)

    def test_memory_hysteresis_and_completed_reclaim_stop_at_recovered_headroom(self):
        from contextlib import ExitStack
        from unittest.mock import patch
        pressure = [Pressure(.20, 0, 0, 20*self.GIB)]
        policy = WarmParkPolicy(lambda: pressure[0])
        for key in range(8):
            self.retain(policy, key, self.GIB)
        pressure[0] = Pressure(.04, 0, 0, 4*self.GIB)
        # To recover 7.5 GiB headroom from 4 GiB needs four 1 GiB
        # projected releases, not all eight and not a fixed concurrency cap.
        with ExitStack() as stack:
            for key in range(4):
                stack.enter_context(policy.defer(key, memory_bytes=self.GIB, blocking=False))
            self.retain(policy, 4, self.GIB)
            self.assertEqual(policy.snapshot()['checkpoint_inflight'], 4)
            for key in range(4):
                policy.parked(key)
        # Recovery above the entrance floor but below the exit floor remains
        # latched; once it reaches 8 GiB remaining waits stay resident.
        pressure[0] = Pressure(.06, 0, 0, 6*self.GIB)
        self.assertTrue(policy._needs_reclaim(pressure[0], MemoryDemand()))
        pressure[0] = Pressure(.08, 0, 0, 8*self.GIB)
        with patch('ucloud_sandboxes.warm_park.time.monotonic', return_value=time.monotonic()+1):
            self.assertFalse(policy.ready(4, memory_bytes=self.GIB))
        self.assertEqual(policy.snapshot()['checkpoints_completed'], 4)
        self.assertEqual(policy.snapshot()['resident_waits'], 4)
        pressure[0] = Pressure(.06, 0, 0, 6*self.GIB)
        self.assertFalse(policy._needs_reclaim(pressure[0], MemoryDemand()))

    def test_saturated_storage_does_not_turn_reclaim_psi_into_more_checkpoints(self):
        pressure = [Pressure(.20, 30, 75, 20*self.GIB)]
        demand = [0]
        policy = WarmParkPolicy(lambda: pressure[0], demand=lambda: MemoryDemand(demand[0], demand[0]))
        self.retain(policy, 'model', self.GIB)
        self.assertEqual(policy.snapshot()['reason'], 'storage_backpressure')
        # Real foreground memory demand still makes progress under disk PSI.
        demand[0] = 20*self.GIB
        with policy.defer('model', memory_bytes=self.GIB, blocking=False):
            self.assertEqual(policy.snapshot()['reason'], 'queued_demand')
            self.assertLessEqual(policy.snapshot()['reclaim_target_bytes'], 5*self.GIB)
            policy.parked('model')

    def test_failed_or_busy_oldest_wait_does_not_block_other_safe_points(self):
        from unittest.mock import patch
        pressure = [Pressure(.8, 0)]
        policy = WarmParkPolicy(lambda: pressure[0])
        self.retain(policy, 'busy')
        self.retain(policy, 'ready')
        pressure[0] = Pressure(.01, 20)
        with self.assertRaisesRegex(RuntimeError, 'busy'):
            with policy.defer('busy', blocking=False):
                raise RuntimeError('busy')
        with patch('ucloud_sandboxes.warm_park.time.monotonic', return_value=time.monotonic()+.3):
            with policy.defer('ready', blocking=False):
                policy.parked('ready')
        policy.forget('busy')
        policy.wake('ready')
        self.assertEqual(policy.snapshot()['resident_waits'], 0)
        self.assertEqual(policy.snapshot()['checkpoint_inflight'], 0)

    def test_cancellation_returns_projected_credit_without_faking_reclaimed_memory(self):
        from unittest.mock import patch
        policy = WarmParkPolicy(lambda: Pressure(.01, 20, 50, self.GIB))
        with self.assertRaises(KeyboardInterrupt):
            with policy.defer('cancelled', memory_bytes=self.GIB, blocking=False):
                self.assertGreater(policy.snapshot()['projected_reclaim_bytes'], 0)
                raise KeyboardInterrupt()
        self.assertEqual(policy.snapshot()['projected_reclaim_bytes'], 0)
        self.assertEqual(policy.snapshot()['checkpoints_completed'], 0)
        with patch('ucloud_sandboxes.warm_park.time.monotonic', return_value=time.monotonic()+.3):
            with policy.defer('next', memory_bytes=self.GIB, blocking=False):
                policy.parked('next')
        self.assertEqual(policy.snapshot()['checkpoints_completed'], 1)
