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
