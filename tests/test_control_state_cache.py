from dataclasses import replace
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from tests.test_registry import build_heartbeat
from ucloud_sandboxes import control_state
from ucloud_sandboxes.control_state import ControlStateStore
from ucloud_sandboxes.models import SandboxInventoryEntry, utc_now


class ControlStateCacheTests(unittest.TestCase):
    def test_header_read_omits_inventory_but_rechecks_external_authority(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "control.sqlite"
            reader, writer = ControlStateStore(path), ControlStateStore(path)
            writer.receive_heartbeat(replace(self.heartbeat(), received_at=utc_now()))
            header = reader.get_heartbeat("job", include_inventory=False)
            self.assertEqual(header.inventory, ())
            self.assertFalse(header.inventory_complete)
            self.assertEqual(header.node_epoch, "boot-1")
            header.labels["pool"] = "changed"
            full = reader.get_heartbeat("job")
            self.assertTrue(full.inventory_complete)
            self.assertEqual(len(full.inventory), 1)
            self.assertEqual(full.labels["pool"], "workers")
            writer.quarantine_node("job", "test")
            self.assertFalse(reader.get_heartbeat("job", include_inventory=False).admission_open)
            writer.receive_heartbeat(replace(
                self.heartbeat(), node_epoch="boot-2", received_at=utc_now(),
            ))
            self.assertEqual(reader.get_heartbeat("job", include_inventory=False).node_epoch, "boot-2")
            with sqlite3.connect(path) as connection:
                connection.execute("UPDATE control_records SET payload = ' ' || payload")
            with self.assertRaisesRegex(ValueError, "invalid heartbeat"):
                reader.get_heartbeat("job", include_inventory=False)
            with sqlite3.connect(path) as connection:
                connection.execute("DELETE FROM control_records")
            self.assertIsNone(reader.get_heartbeat("job", include_inventory=False))

    def heartbeat(self, job_id="job"):
        return build_heartbeat(
            job_id=job_id, node_id=f"node-{job_id}",
            node_url=f"http://node-{job_id}:8090", node_epoch="boot-1",
            labels={"pool": "workers"}, inventory_complete=True,
            inventory=(SandboxInventoryEntry(
                sandbox_id="sandbox", generation=1, operation_id="create-1",
                spec_hash="a" * 64, state="running",
                storage_dependency={"layers": [{"digest": "original"}]},
            ),),
        )

    def test_reuses_decode_but_returns_isolated_mutable_inventory(self):
        with TemporaryDirectory() as directory:
            store = ControlStateStore(Path(directory) / "control.sqlite")
            store.upsert_heartbeat(self.heartbeat())
            with patch.object(store, "_decode_heartbeat", wraps=store._decode_heartbeat) as decode:
                first = store.get_heartbeat("job")
                first.labels["pool"] = "changed"
                first.inventory[0].storage_dependency["layers"][0]["digest"] = "changed"
                first.inventory[0].storage_snapshot["unexpected"] = True
                second = store.load_heartbeats()["job"]
                third = store.get_heartbeat("job")
                self.assertEqual(decode.call_count, 1)
            for heartbeat in (second, third):
                self.assertEqual(heartbeat.labels, {"pool": "workers"})
                self.assertEqual(heartbeat.inventory[0].storage_snapshot, {})
                self.assertEqual(heartbeat.inventory[0].storage_dependency,
                                 {"layers": [{"digest": "original"}]})

    def test_other_connection_quarantine_reboot_delete_and_corruption_are_immediate(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "control.sqlite"
            reader, writer = ControlStateStore(path), ControlStateStore(path)
            heartbeat = replace(self.heartbeat(), received_at=utc_now())
            writer.receive_heartbeat(heartbeat)
            self.assertTrue(reader.get_heartbeat("job").admission_open)
            writer.quarantine_node("job", "test")
            self.assertFalse(reader.get_heartbeat("job").admission_open)
            writer.receive_heartbeat(replace(
                heartbeat, node_epoch="boot-2", received_at=utc_now(),
            ))
            current = reader.get_heartbeat("job")
            self.assertEqual(current.node_epoch, "boot-2")
            self.assertEqual(current.retired_node_epochs, ("boot-1",))
            self.assertFalse(current.admission_open)
            with sqlite3.connect(path) as connection:
                connection.execute("UPDATE control_records SET payload = ' ' || payload")
            for load in (lambda: reader.get_heartbeat("job"), reader.load_heartbeats):
                with self.assertRaisesRegex(ValueError, "invalid heartbeat"):
                    load()
            with sqlite3.connect(path) as connection:
                connection.execute("DELETE FROM control_records")
            self.assertIsNone(reader.get_heartbeat("job"))
            self.assertEqual(reader.load_heartbeats(), {})

    def test_eviction_and_oversized_rows_do_not_limit_fleet_reads(self):
        with TemporaryDirectory() as directory:
            store = ControlStateStore(Path(directory) / "control.sqlite")
            with patch.object(control_state, "_HEARTBEAT_CACHE_ENTRIES", 2):
                for index in range(4):
                    store.upsert_heartbeat(self.heartbeat(str(index)))
                self.assertEqual(len(store.load_heartbeats()), 4)
                self.assertLessEqual(len(store._heartbeat_cache), 2)
            # Force byte eviction separately from entry eviction.
            with patch.object(control_state, "_HEARTBEAT_CACHE_BYTES", 1):
                store.upsert_heartbeat(self.heartbeat("large"))
                self.assertEqual(store.get_heartbeat("large").node_id, "node-large")
                self.assertNotIn("large", store._heartbeat_cache)
            self.assertEqual(len(store.load_heartbeats()), 5)

    def test_file_identity_check_does_not_block_returning_another_reader(self):
        from threading import Event, Thread, current_thread
        with TemporaryDirectory() as raw:
            store = ControlStateStore(Path(raw) / 'control.sqlite')
            entered, release, returned = Event(), Event(), Event()
            original = Path.stat
            errors = []
            def slow_stat(path, *args, **kwargs):
                if path == store.path and current_thread().name == 'slow-reader':
                    entered.set()
                    release.wait(2)
                return original(path, *args, **kwargs)
            held = store._connection()
            held.__enter__()
            def read():
                try:
                    store.load_heartbeats()
                except BaseException as exc:
                    errors.append(exc)
            def give_back():
                try:
                    held.__exit__(None, None, None)
                finally:
                    returned.set()
            with patch.object(Path, 'stat', slow_stat):
                reader = Thread(target=read, name='slow-reader')
                reader.start()
                self.assertTrue(entered.wait(2))
                returning = Thread(target=give_back)
                returning.start()
                try:
                    self.assertTrue(returned.wait(.5), 'identity stat held the pool mutex')
                finally:
                    release.set()
                    reader.join(3)
                    returning.join(3)
            self.assertFalse(errors)

    def test_exec_absence_check_only_loads_inventory_for_an_empty_worker(self):
        from ucloud_sandboxes.control_plane import ControlPlaneHandler
        from ucloud_sandboxes.routing import ExecRoute
        with TemporaryDirectory() as raw:
            store = ControlStateStore(Path(raw) / 'control.sqlite')
            heartbeat = replace(self.heartbeat(), received_at=utc_now(), active_sandboxes=1)
            store.receive_heartbeat(heartbeat)
            handler = object.__new__(ControlPlaneHandler)
            handler.store = store
            handler.heartbeat_ttl_seconds = 120
            route = ExecRoute(session_id='exec', sandbox_id='sandbox', node_id=heartbeat.node_id,
                              job_id=heartbeat.job_id, node_url=heartbeat.node_url,
                              created_at='2026-01-01T00:00:00+00:00', updated_at='2026-01-01T00:00:00+00:00')
            with patch.object(store, 'get_heartbeat', wraps=store.get_heartbeat) as get:
                self.assertFalse(handler._exec_route_is_proven_stale(route))
                get.assert_called_once_with('job', include_inventory=False)
            # Zero active count does not mean the parked sandbox disappeared.
            store.receive_heartbeat(replace(heartbeat, active_sandboxes=0, received_at=utc_now()))
            self.assertFalse(handler._exec_route_is_proven_stale(route))
            store.receive_heartbeat(replace(heartbeat, active_sandboxes=0, inventory=(), received_at=utc_now()))
            self.assertTrue(handler._exec_route_is_proven_stale(route))
