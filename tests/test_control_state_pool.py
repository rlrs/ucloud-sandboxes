from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
import os
import threading
import unittest
from unittest.mock import patch

from tests.test_registry import build_heartbeat
from ucloud_sandboxes.control_state import ControlStateStore
from ucloud_sandboxes.models import utc_now


class ControlStatePoolTests(unittest.TestCase):
    def test_concurrent_readers_are_exclusive_and_keep_full_durability(self):
        with TemporaryDirectory() as directory:
            store = ControlStateStore(Path(directory) / 'state.sqlite')
            barrier = threading.Barrier(24)
            active, peak, identities = set(), [0], set()
            guard = threading.Lock()
            def read(_):
                import time
                barrier.wait(timeout=10)
                with store._transaction(write=False) as conn:
                    self.assertEqual(conn.execute('PRAGMA synchronous').fetchone()[0], 2)
                    with guard:
                        self.assertNotIn(id(conn), active)
                        active.add(id(conn))
                        identities.add(id(conn))
                        peak[0] = max(peak[0], len(active))
                    time.sleep(.02)
                    with guard:
                        active.remove(id(conn))
            with ThreadPoolExecutor(max_workers=24) as pool:
                list(pool.map(read, range(24)))
            self.assertLessEqual(peak[0], 16)
            self.assertLessEqual(len(identities), 16)
            self.assertFalse(active)

    def test_failed_write_rolls_back_and_reused_reader_observes_external_commit(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / 'state.sqlite'
            reader, writer = ControlStateStore(path), ControlStateStore(path)
            heartbeat = replace(
                build_heartbeat(job_id='job', node_id='node', node_epoch='boot'),
                received_at=utc_now(),
            )
            writer.receive_heartbeat(heartbeat)
            self.assertEqual(reader.get_heartbeat('job').node_epoch, 'boot')
            with self.assertRaisesRegex(RuntimeError, 'injected'):
                with reader._transaction(write=True) as conn:
                    conn.execute('DELETE FROM control_records')
                    raise RuntimeError('injected')
            self.assertEqual(reader.get_heartbeat('job').node_epoch, 'boot')
            writer.receive_heartbeat(replace(heartbeat, node_epoch='new-boot', received_at=utc_now()))
            self.assertEqual(reader.get_heartbeat('job').node_epoch, 'new-boot')
            with reader._connection() as conn:
                conn.execute('BEGIN')
                conn.execute('SELECT * FROM control_records').fetchall()
            with reader._connection() as conn:
                self.assertFalse(conn.in_transaction)

    def test_replaced_database_and_fork_are_rejected(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / 'state.sqlite'
            store = ControlStateStore(path)
            with patch('ucloud_sandboxes.control_state.os.getpid', return_value=-1):
                with self.assertRaisesRegex(ValueError, 'control state is unreadable'):
                    store.get_heartbeat('absent')
            replacement = Path(directory) / 'replacement.sqlite'
            ControlStateStore(replacement)
            os.replace(replacement, path)
            with self.assertRaisesRegex(ValueError, 'control state is unreadable'):
                store.get_heartbeat('absent')
