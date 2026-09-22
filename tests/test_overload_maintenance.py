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

    def batch_ready(self, batch, count):
        with batch._condition:
            self.assertTrue(
                batch._condition.wait_for(
                    lambda: (
                        batch._batch is not None and batch._batch.operations == count
                    ),
                    timeout=3,
                )
            )
            self.assertEqual(self.read(), [])
            batch._batch.deadline = 0
            batch._condition.notify_all()

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
            with batch._condition:
                self.assertTrue(
                    batch._condition.wait_for(
                        lambda: (
                            batch._batch is not None and batch._batch.operations == 1
                        ),
                        timeout=2,
                    )
                )
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

    def test_nonblocking_park_retries_preserve_deadline_and_recheck_pressure(self):
        from unittest.mock import patch
        from ucloud_sandboxes.warm_park import WarmParkDeferred
        pressure = [Pressure(.8, 0)]
        policy = WarmParkPolicy(lambda: pressure[0], max_delay=15)
        for now, remaining in ((10, 15), (15, 10)):
            with patch("ucloud_sandboxes.warm_park.time.monotonic", return_value=now):
                with self.assertRaises(WarmParkDeferred) as caught:
                    with policy.defer("request", blocking=False):
                        self.fail("warm request should defer")
                self.assertEqual(caught.exception.seconds, remaining)
                self.assertFalse(policy._pending)
        pressure[0] = Pressure(.05, 0)
        with patch("ucloud_sandboxes.warm_park.time.monotonic", return_value=16):
            with policy.defer("request", blocking=False):
                pass

    def test_wake_cancels_grace_and_generation_does_not_cross(self):
        policy = WarmParkPolicy(lambda: Pressure(0.8, 0), max_delay=1)
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

    def test_response_after_grace_informs_prediction_and_retry_does_not_reset_clock(
        self,
    ):
        from unittest.mock import patch

        policy = WarmParkPolicy(lambda: Pressure(0.8, 0), max_delay=2)
        policy._waiting_since["request"] = 10.0
        with patch("ucloud_sandboxes.warm_park.time.monotonic", return_value=15.0):
            with policy.defer("request") as event:
                self.assertFalse(event.is_set())
            policy.wake("request")
        self.assertEqual(list(policy._responses), [5.0])
        self.assertEqual(policy._budget(), 2)

    def test_pressure_ends_grace_and_missing_pressure_does_not_delay_reclaim(self):
        state = [Pressure(0.8, 0)]
        policy = WarmParkPolicy(lambda: state[0], max_delay=10)

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
            policy = WarmParkPolicy(missing.sample, max_delay=10)
            self.assertEqual(policy._budget(), 0)

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
    def test_memory_demand_and_large_footprint_shorten_retention(self):
        gib = 1024**3
        incoming = [0]
        policy = WarmParkPolicy(lambda: Pressure(.8, 0, 0, 8*gib),
                                demand_bytes=lambda: incoming[0])
        self.assertEqual(policy._budget(gib), 15)
        self.assertLess(policy._budget(4*gib), policy._budget(gib))
        incoming[0] = 4*gib
        self.assertEqual(policy._budget(gib), 7.5)
        incoming[0] = 8*gib
        self.assertEqual(policy._budget(gib), 0)

    def test_queued_wake_demand_ends_an_existing_warm_wait(self):
        incoming = [0]
        policy = WarmParkPolicy(lambda: Pressure(.8, 0, 0, 1024),
                                demand_bytes=lambda: incoming[0])
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

    def test_learning_can_retain_waits_longer_than_two_seconds(self):
        policy = WarmParkPolicy(lambda: Pressure(.8, 0))
        policy._responses.extend([4, 7, 9, 12])
        self.assertEqual(policy._budget(), 12)
