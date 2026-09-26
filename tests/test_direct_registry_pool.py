from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory
import os
import sqlite3
import threading
import unittest
from unittest.mock import patch

from ucloud_sandboxes.direct_registry import (
    _IDLE_CONNECTIONS,
    DirectRegistryError,
    DirectSandboxRegistry,
)


class DirectRegistryPoolTests(unittest.TestCase):
    def test_reuses_validation_but_detects_schema_and_metadata_changes(self):
        changes = [
            "CREATE TABLE extra (value TEXT)",
            "CREATE VIEW extra AS SELECT 1",
            "CREATE TRIGGER extra AFTER UPDATE ON registry_metadata BEGIN SELECT 1; END",
            "PRAGMA user_version = 2",
            "DELETE FROM registry_metadata",
        ]
        for change in changes:
            with self.subTest(change=change), TemporaryDirectory() as tmp:
                registry = DirectSandboxRegistry(Path(tmp) / 'registry.sqlite')
                with patch.object(registry, '_ensure_schema', wraps=registry._ensure_schema) as validate:
                    for _ in range(10):
                        self.assertIsNone(registry.get('absent'))
                    validate.assert_called_once()
                with closing(sqlite3.connect(registry.path)) as conn:
                    conn.execute(change)
                    conn.commit()
                with self.assertRaises(DirectRegistryError):
                    registry.get('absent')

    def test_new_connections_reuse_validation_but_detect_later_changes(self):
        with TemporaryDirectory() as tmp:
            registry = DirectSandboxRegistry(Path(tmp) / 'registry.sqlite')
            registry.snapshot()
            barrier = threading.Barrier(8)

            def read(_):
                with registry._borrow():
                    barrier.wait(timeout=10)
                    return registry.get('absent')

            with patch.object(registry, '_ensure_schema', wraps=registry._ensure_schema) as validate:
                with ThreadPoolExecutor(max_workers=8) as pool:
                    self.assertEqual(list(pool.map(read, range(8))), [None] * 8)
                validate.assert_not_called()
            with closing(sqlite3.connect(registry.path)) as conn:
                conn.execute("CREATE TABLE extra (value TEXT)")
                conn.commit()
            for entry in registry._connections:
                entry.connection.close()
            registry._connections.clear()
            with self.assertRaises(DirectRegistryError):
                registry.get('absent')

    def test_replacement_file_does_not_reuse_old_connection(self):
        with TemporaryDirectory() as tmp:
            registry = DirectSandboxRegistry(Path(tmp) / 'registry.sqlite')
            registry.snapshot()
            replacement = Path(tmp) / 'replacement.sqlite'
            replacement.touch(mode=0o600)
            os.replace(replacement, registry.path)
            with self.assertRaisesRegex(DirectRegistryError, 'replaced'):
                registry.snapshot()

    def test_connections_are_exclusive_and_idle_retention_is_not_admission(self):
        with TemporaryDirectory() as tmp:
            registry = DirectSandboxRegistry(Path(tmp) / 'registry.sqlite')
            registry.snapshot()
            workers = _IDLE_CONNECTIONS + 8
            barrier = threading.Barrier(workers)
            ids = []
            guard = threading.Lock()
            def read(_):
                with registry._transaction(write=False) as conn:
                    with guard:
                        ids.append(id(conn))
                    barrier.wait(timeout=10)
                    self.assertEqual(conn.execute('PRAGMA synchronous').fetchone()[0], 2)
                    self.assertEqual(conn.execute('PRAGMA trusted_schema').fetchone()[0], 0)
            with ThreadPoolExecutor(max_workers=workers) as pool:
                list(pool.map(read, range(workers)))
            self.assertEqual(len(set(ids)), workers)
            self.assertEqual(len(registry._connections), _IDLE_CONNECTIONS)
            self.assertTrue(all(not e.connection.in_transaction for e in registry._connections))

    def test_exception_rolls_back_and_discards_connection(self):
        with TemporaryDirectory() as tmp:
            registry = DirectSandboxRegistry(Path(tmp) / 'registry.sqlite')
            initial = registry.activity_revision()
            with self.assertRaisesRegex(RuntimeError, 'injected'):
                with registry._transaction(write=True) as conn:
                    registry._bump_activity(conn)
                    raise RuntimeError('injected')
            self.assertEqual(registry.activity_revision(), initial)
