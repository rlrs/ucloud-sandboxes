from contextlib import closing
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from tests import test_storage_native_daemon as fixtures
from ucloud_sandboxes.storage_native_daemon import (
    StorageNativeJournal, StorageVolumeOwner, StorageVolumeState,
)


def add_history(journal, template, count):
    """Keep valid tombstones, as long-running workers do for replay fencing."""
    with journal._write_connection() as connection:
        connection.execute("BEGIN IMMEDIATE")
        for index in range(count):
            journal._upsert_record(connection, replace(
                template, volume_id=f"history-{index}", operation_id=f"old-{index}",
                accounting_id=300000 + index, state=StorageVolumeState.DELETED,
                device_owner_id="", device_id=None, device_path="", runtime_image_config="",
                sealed_layer_paths=(), cached_layer_paths=(),
            ))
        connection.execute("UPDATE counters SET next_value = ?", (400000 + count,))
        connection.commit()


class StorageJournalHotpathTests(unittest.TestCase):
    def fixture(self, root, history=2000):
        service, backend, host = fixtures.StorageNativeNodeServiceTests()._service(root)
        owner = StorageVolumeOwner("live", "sandbox", 1)
        live = service.converge_volume(owner, action="prepare", operation_id="create", virtual_size=1 << 30)
        add_history(service.journal, live, history)
        return service, backend, host, owner

    def test_create_and_wake_capacity_work_does_not_grow_with_deleted_history(self):
        with TemporaryDirectory() as raw:
            service, _, _, owner = self.fixture(Path(raw))
            service.converge_volume(owner, action="release", operation_id="park")
            connect = service.journal._connect
            steps = 0

            def progress():
                nonlocal steps
                steps += 100
                # Work budget, not a wall-clock assertion: scanning 2,000
                # tombstones would exceed this during either admission query.
                return int(steps > 2500)

            def bounded_connect():
                connection = connect()
                connection.set_progress_handler(progress, 100)
                return connection

            with patch.object(service.journal, "_connect", side_effect=bounded_connect):
                created = service.converge_volume(
                    StorageVolumeOwner("new", "new", 1), action="prepare",
                    operation_id="new", virtual_size=1 << 30,
                )
                self.assertEqual(created.state, StorageVolumeState.MOUNTED)
                steps = 0
                woke = service.converge_volume(owner, action="mount", operation_id="wake")
                self.assertEqual(woke.state, StorageVolumeState.MOUNTED)
            self.assertEqual(service.metrics()["hard_reserved_bytes"], 2 << 30)

    def test_metrics_preserve_history_count_without_decoding_tombstones(self):
        with TemporaryDirectory() as raw:
            service, _, _, _ = self.fixture(Path(raw))
            decode = service.journal._decode_record_row
            with patch.object(service.journal, "_decode_record_row", wraps=decode) as decoded:
                metrics = service.metrics()
            self.assertEqual(decoded.call_count, 1)
            self.assertEqual(metrics["volume_count"], 2001)
            self.assertEqual(metrics["hard_reserved_bytes"], 1 << 30)
            self.assertEqual(metrics["error_volumes"], 0)
            self.assertEqual(metrics["published_volumes"], 0)
            # Historical records still exist and retain their replay identity.
            self.assertEqual(service.journal.load("history-0").state, StorageVolumeState.DELETED)

    def test_scoped_retirement_checks_bound_work_and_preserve_local_layers(self):
        with TemporaryDirectory() as raw:
            service, backend, _, owner = self.fixture(Path(raw), history=0)
            journal = service.journal
            live = journal.load(owner.volume_id)
            with journal._write_connection() as connection:
                connection.execute("BEGIN IMMEDIATE")
                connection.executemany("INSERT INTO retired_devices VALUES (?, ?, ?, ?)",
                                       [(f"retired-{i}", i + 100, f"other-{i}", 4096) for i in range(2000)])
                connection.commit()
            root = service._volume_root(owner.volume_id)
            layer = root / "cleanup.commit"
            layer.write_bytes(b"checkpoint")
            journal.retire_device(backend.owners[live.device_owner_id], live)
            with patch.object(journal, "retired_devices", side_effect=AssertionError("full retirement scan")):
                service._remove_local_layers((layer,))
                self.assertTrue(layer.exists())
                journal.forget_retired_device(live.device_owner_id)
                service._remove_local_layers((layer,))
                self.assertFalse(layer.exists())
            # The lookup remains bounded even with a large unrelated backlog.
            connect = journal._connect

            def bounded_connect():
                connection = connect()
                connection.set_progress_handler(lambda: 1, 100)
                return connection

            with patch.object(journal, "_connect", side_effect=bounded_connect):
                self.assertTrue(journal.has_retired_devices("other-1999"))
                self.assertFalse(journal.has_retired_devices("absent"))
            with patch.object(journal, "_connect", side_effect=AssertionError("empty cleanup touched journal")):
                service._remove_local_layers(())

    def test_existing_journal_gets_indexes_without_changing_records(self):
        with TemporaryDirectory() as raw:
            service, _, _, owner = self.fixture(Path(raw), history=3)
            journal = service.journal
            before = journal.load(owner.volume_id)
            with closing(journal._connect()) as connection:
                connection.execute("DROP INDEX volumes_live_capacity")
                connection.execute("DROP INDEX retired_devices_volume")
            reopened = StorageNativeJournal(journal.path)
            self.assertEqual(reopened.load(owner.volume_id), before)
            self.assertEqual(len(reopened.list()), 4)
            self.assertEqual(reopened.metrics_inventory()[1], 4)
