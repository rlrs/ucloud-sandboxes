from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
from threading import Barrier
import unittest
from unittest.mock import patch

from ucloud_sandboxes.storage_native_daemon import StorageNativeJournal, StorageNativeNodeError


class StorageJournalPoolTests(unittest.TestCase):
    def setUp(self):
        self.directory = TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name).resolve() / 'journal.sqlite'
        self.journal = StorageNativeJournal(self.path)
        self.addCleanup(self.journal._close_idle_connections, self.journal._idle_connections)

    def test_reuse_preserves_durability_and_releases_read_snapshots(self):
        with self.journal._connection() as first:
            self.assertEqual(first.execute('PRAGMA synchronous').fetchone()[0], 2)
            self.assertEqual(first.execute('PRAGMA foreign_keys').fetchone()[0], 1)
            first.execute('BEGIN')
            self.assertEqual(first.execute('SELECT next_value FROM counters').fetchone()[0], 200000)
        with sqlite3.connect(self.path) as external:
            external.execute('UPDATE counters SET next_value = 200007')
        with self.journal._connection() as second:
            self.assertIs(first, second)
            self.assertFalse(second.in_transaction)
            self.assertEqual(second.execute('SELECT next_value FROM counters').fetchone()[0], 200007)

    def test_failed_write_is_rolled_back_and_discarded(self):
        with self.assertRaisesRegex(RuntimeError, 'injected'):
            with self.journal._write_connection() as first:
                first.execute('BEGIN IMMEDIATE')
                first.execute('UPDATE counters SET next_value = 200123')
                raise RuntimeError('injected')
        with self.journal._connection() as second:
            self.assertIsNot(first, second)
            self.assertEqual(second.execute('SELECT next_value FROM counters').fetchone()[0], 200000)

    def test_active_connections_are_exclusive_and_not_limited_by_idle_retention(self):
        barrier = Barrier(24)

        def read(_):
            with self.journal._connection() as connection:
                barrier.wait(5)
                return id(connection), connection.execute('SELECT next_value FROM counters').fetchone()[0]

        with ThreadPoolExecutor(max_workers=24) as pool:
            results = list(pool.map(read, range(24)))
        self.assertEqual(len({item[0] for item in results}), 24)
        self.assertEqual({item[1] for item in results}, {200000})
        self.assertEqual(len(self.journal._idle_connections), 16)

    def test_replaced_database_and_fork_fail_closed(self):
        with self.journal._connection():
            pass
        with patch('ucloud_sandboxes.storage_native_daemon.os.getpid', return_value=-1):
            with self.assertRaisesRegex(StorageNativeNodeError, 'after fork'):
                self.journal.load('absent')
        self.path.rename(self.path.with_suffix('.old'))
        self.path.touch()
        with self.assertRaisesRegex(StorageNativeNodeError, 'replaced'):
            self.journal.load('absent')
