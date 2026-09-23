from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
from threading import Condition, Event, Lock
import time
import unittest
from unittest.mock import patch

from ucloud_sandboxes.sqlite_pool import SqliteConnectionPool


class SqliteLeaseTests(unittest.TestCase):
    def test_saturated_fifo_wakes_only_admitted_readers(self):
        wakeups = []
        guard = Lock()
        class CountedCondition(Condition):
            def wait(self, timeout=None):
                result = super().wait(timeout)
                with guard:
                    wakeups.append(1)
                return result
        with patch("ucloud_sandboxes.sqlite_pool.Condition", CountedCondition):
            pool = SqliteConnectionPool(1)
            self.addCleanup(pool.close)
            def connect():
                return sqlite3.connect(":memory:", check_same_thread=False)
            order = []
            def read(index):
                with pool.connection(connect) as conn:
                    self.assertEqual(conn.execute("SELECT 1").fetchone(), (1,))
                    order.append(index)
            with ThreadPoolExecutor(64) as threads:
                with pool.connection(connect):
                    futures = []
                    for index in range(64):
                        futures.append(threads.submit(read, index))
                        deadline = time.monotonic() + 5
                        while True:
                            with pool._condition:
                                queued = len(pool._waiters)
                            if queued == index + 1:
                                break
                            self.assertLess(time.monotonic(), deadline)
                            time.sleep(.001)
                for future in futures:
                    future.result(5)
            self.assertEqual(order, list(range(64)))
            # A broadcast implementation wakes O(n²) threads to deliver n
            # leases. Allow an occasional spurious wake, not a stampede.
            self.assertLessEqual(len(wakeups), 128)

    def test_factory_failure_releases_slot_and_shutdown_wakes_queued_reader(self):
        pool = SqliteConnectionPool(1)
        self.addCleanup(pool.close)
        def connect():
            return sqlite3.connect(":memory:", check_same_thread=False)
        def fail():
            raise OSError("injected open failure")
        with self.assertRaisesRegex(OSError, "injected"):
            with pool.connection(fail):
                pass
        entered = Event()
        def queued():
            entered.set()
            with pool.connection(connect):
                self.fail("closed pool admitted a waiter")
        with ThreadPoolExecutor(1) as threads:
            with pool.connection(connect) as active:
                waiter = threads.submit(queued)
                self.assertTrue(entered.wait(1))
                pool.close()
                with self.assertRaisesRegex(sqlite3.DatabaseError, "pool is closed"):
                    waiter.result(1)
                # Shutdown does not interrupt an owned transaction.
                self.assertEqual(active.execute("SELECT 1").fetchone(), (1,))
            with self.assertRaises(sqlite3.ProgrammingError):
                active.execute("SELECT 1")

    def test_waiting_reader_observes_external_commit_after_snapshot_return(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "state.sqlite"
            writer = sqlite3.connect(path)
            self.addCleanup(writer.close)
            writer.execute("PRAGMA journal_mode=WAL")
            writer.execute("CREATE TABLE probe (value INTEGER)")
            writer.execute("INSERT INTO probe VALUES (1)")
            writer.commit()
            pool = SqliteConnectionPool(1)
            self.addCleanup(pool.close)
            def connect():
                return sqlite3.connect(path, check_same_thread=False)
            entered = Event()
            def queued():
                entered.set()
                with pool.connection(connect) as reader:
                    self.assertFalse(reader.in_transaction)
                    return reader.execute("SELECT value FROM probe").fetchone()[0]
            with ThreadPoolExecutor(1) as threads:
                with pool.connection(connect) as reader:
                    reader.execute("BEGIN")
                    self.assertEqual(reader.execute("SELECT value FROM probe").fetchone(), (1,))
                    future = threads.submit(queued)
                    self.assertTrue(entered.wait(1))
                    # WAL writer must progress while all reader leases are held.
                    writer.execute("UPDATE probe SET value=2")
                    writer.commit()
                self.assertEqual(future.result(1), 2)
