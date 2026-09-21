from pathlib import Path
import os
import sqlite3
import stat
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from ucloud_sandboxes.control_state import ControlStateStore
from ucloud_sandboxes.routing import RoutingStore


class SqlitePermissionTests(unittest.TestCase):
    def test_reads_skip_metadata_writes_but_repair_changed_permissions(self):
        for store_type in (RoutingStore, ControlStateStore):
            with self.subTest(store=store_type.__name__), TemporaryDirectory() as directory:
                path = Path(directory) / "state.sqlite"
                store = store_type(path)
                def read():
                    if isinstance(store, RoutingStore):
                        return store.get_sandbox_readonly("absent")
                    return store.get_heartbeat("absent")
                # Keep SQLite's real WAL and shared-memory files alive between
                # requests, as they are during overlapping production traffic.
                connection = sqlite3.connect(path)
                try:
                    connection.execute("SELECT name FROM sqlite_schema").fetchall()
                    files = [Path(f"{path}{suffix}") for suffix in ("", "-wal", "-shm")]
                    self.assertTrue(all(candidate.exists() for candidate in files))
                    read()
                    with patch("os.chmod", wraps=os.chmod) as chmod:
                        for _ in range(3):
                            read()
                        chmod.assert_not_called()

                    # Do not cache the security decision: repair external mode
                    # changes, including on sidecars, on the very next read.
                    for candidate in files:
                        candidate.chmod(0o644)
                    read()
                    self.assertEqual([stat.S_IMODE(f.stat().st_mode) for f in files], [0o600] * 3)
                finally:
                    connection.close()

                # A later SQLite connection recreates the sidecars; those must
                # also be checked, even after previous successful reads.
                connection = sqlite3.connect(path)
                try:
                    connection.execute("SELECT name FROM sqlite_schema").fetchall()
                    for candidate in files[1:]:
                        candidate.chmod(0o640)
                    read()
                    self.assertEqual([stat.S_IMODE(f.stat().st_mode) for f in files], [0o600] * 3)
                finally:
                    connection.close()
