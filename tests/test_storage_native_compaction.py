from dataclasses import replace
import hashlib
import json
import os
from pathlib import Path
import socket
from tempfile import TemporaryDirectory
import threading
import unittest
from unittest.mock import patch

from tests import test_storage_native_daemon as fixtures
from ucloud_sandboxes.storage_native import StorageNativeLayer
from ucloud_sandboxes.storage_native_compaction import LocalCheckpointCompactor
from ucloud_sandboxes.storage_native_daemon import StorageVolumeOwner, StorageVolumeState


class LocalCompactionTests(unittest.TestCase):
    def fixture(self, root, *, blocked=False):
        service, backend, host = fixtures.StorageNativeNodeServiceTests()._service(root)
        entered, resume = threading.Event(), threading.Event()
        if not blocked:
            resume.set()

        def export(*, source_image_config, global_config, stream_socket_path):
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
                connection.connect(str(stream_socket_path))
                entered.set()
                if not resume.wait(5):
                    raise TimeoutError("test exporter was not released")
                paths = [Path(item["file"]) for item in json.loads(source_image_config.read_text())["lowers"]]
                payload = b"".join(path.read_bytes() for path in paths)
                connection.sendall(payload)
            return StorageNativeLayer("sha256:" + hashlib.sha256(payload).hexdigest(), len(payload))

        backend.export_compacted_image = export
        service._local_compactor = LocalCheckpointCompactor(
            root=service.config.runtime_root, global_config=service.global_config_path,
            exporter=backend, load=service.journal.load, remove_layers=service._remove_local_layers,
            max_layers=2, timeout_seconds=2,
        )
        owner = StorageVolumeOwner("volume", "sandbox", 1)
        service.converge_volume(owner, action="prepare", operation_id="create", virtual_size=1 << 30)
        return service, owner, entered, resume

    def park_cycles(self, service, owner, count=3):
        for index in range(count):
            if index:
                service.converge_volume(owner, action="mount", operation_id=f"wake-{index}")
            result = service.converge_volume(owner, action="release", operation_id=f"park-{index}")
        return result

    def test_compaction_survives_wake_and_appended_delta_then_adopts_on_next_mount(self):
        with TemporaryDirectory(dir="/tmp") as raw:
            service, owner, entered, resume = self.fixture(Path(raw).resolve(), blocked=True)
            old = self.park_cycles(service, owner)
            compactor = service._local_compactor
            try:
                self.assertTrue(entered.wait(2))
                mounted = service.converge_volume(owner, action="mount", operation_id="wake-during-compaction")
                self.assertEqual(mounted.sealed_layer_paths, old.sealed_layer_paths)
                parked = service.converge_volume(owner, action="release", operation_id="append-during-compaction")
                self.assertEqual(len(parked.sealed_layer_paths), 4)
                resume.set()
                compactor.wait(3)
                self.assertEqual(compactor.metrics()["local_compaction_completed"], 1)
                self.assertEqual(service.journal.load(owner.volume_id).sealed_layer_paths, parked.sealed_layer_paths)
                adopted = service.converge_volume(owner, action="mount", operation_id="adopt")
                self.assertEqual(len(adopted.sealed_layer_paths), 2)
                self.assertEqual(adopted.sealed_layer_paths[-1], parked.sealed_layer_paths[-1])
                self.assertEqual(Path(adopted.sealed_layer_paths[0]).read_bytes(), b"sealed-delta" * 3)
                source = json.loads(Path(adopted.source_image_config).read_text())
                self.assertEqual([layer["file"] for layer in source["lowers"]], list(adopted.sealed_layer_paths))
                self.assertTrue(all(not Path(path).exists() for path in old.sealed_layer_paths))
                self.assertEqual(service.journal.load(owner.volume_id), adopted)
            finally:
                resume.set()
                compactor.wait(5)

    def test_ready_candidate_survives_restart_and_failed_journal_adoption(self):
        with TemporaryDirectory(dir="/tmp") as raw:
            service, owner, _, _ = self.fixture(Path(raw).resolve())
            old = self.park_cycles(service, owner)
            service._local_compactor.wait(3)
            restarted = LocalCheckpointCompactor(
                root=service.config.runtime_root, global_config=service.global_config_path,
                exporter=service.backend, load=service.journal.load, remove_layers=service._remove_local_layers,
                max_layers=2,
            )
            with self.assertRaisesRegex(OSError, "commit failed"):
                restarted.adopt(replace(old, state=StorageVolumeState.ACQUIRING),
                                lambda _: (_ for _ in ()).throw(OSError("commit failed")))
            self.assertTrue(all(Path(path).exists() for path in old.sealed_layer_paths))
            service._local_compactor = restarted
            adopted = service.converge_volume(owner, action="mount", operation_id="restart-adopt")
            self.assertEqual(len(adopted.sealed_layer_paths), 1)

    def test_deleted_or_replaced_owner_cannot_adopt_candidate(self):
        with TemporaryDirectory(dir="/tmp") as raw:
            service, owner, _, _ = self.fixture(Path(raw).resolve())
            old = self.park_cycles(service, owner)
            compactor = service._local_compactor
            compactor.wait(3)
            old = replace(old, state=StorageVolumeState.ACQUIRING)
            for changed in (replace(old, sandbox_generation=2),
                            replace(old, state=StorageVolumeState.DELETED),
                            replace(old, sealed_layer_paths=old.sealed_layer_paths[1:])):
                with self.subTest(changed=changed.state), patch.object(service.journal, "update_pending") as persist:
                    self.assertEqual(compactor.adopt(changed, persist), changed)
                    persist.assert_not_called()

    def test_publication_adopts_ready_compaction_and_recovers_failed_adoption(self):
        for fail in (False, True):
            with self.subTest(fail=fail), TemporaryDirectory(dir="/tmp") as raw:
                service, owner, _, _ = self.fixture(Path(raw).resolve())
                service.publisher = fixtures.FakePublisher()
                old = self.park_cycles(service, owner)
                service._local_compactor.wait(3)
                update = service.journal.update_pending

                def persist(record):
                    if fail:
                        raise OSError("journal adoption failed")
                    update(record)

                with patch.object(service.journal, "update_pending", side_effect=persist):
                    if fail:
                        with self.assertRaisesRegex(OSError, "journal adoption failed"):
                            service.converge_volume(owner, action="publish", operation_id="publish")
                        recovered = service.journal.load(owner.volume_id)
                        self.assertEqual(recovered.state, StorageVolumeState.RELEASED)
                        self.assertEqual(recovered.sealed_layer_paths, old.sealed_layer_paths)
                        self.assertTrue(all(Path(path).exists() for path in old.sealed_layer_paths))
                    else:
                        result = service.converge_volume(owner, action="publish", operation_id="publish")
                        self.assertEqual(result.state, StorageVolumeState.PUBLISHED)
                        self.assertEqual(service._local_compactor.metrics()["local_compaction_adopted"], 1)

    def test_retired_devices_keep_original_input_names_after_adoption(self):
        with TemporaryDirectory(dir="/tmp") as raw:
            service, owner, _, _ = self.fixture(Path(raw).resolve())
            self.park_cycles(service, owner, count=2)
            mounted = service.converge_volume(owner, action="mount", operation_id="third-mount")
            service.host.busy_devices.add(Path(mounted.device_path))
            old = service.converge_volume(owner, action="release", operation_id="third-release")
            service._local_compactor.wait(3)
            adopted = service.converge_volume(owner, action="mount", operation_id="retired-adopt")
            self.assertEqual(len(adopted.sealed_layer_paths), 1)
            self.assertTrue(all(Path(path).exists() for path in old.sealed_layer_paths))
            service.host.busy_devices.clear()
            self.assertEqual(service._reap_retired_devices(), 1)
            self.assertTrue(all(not Path(path).exists() for path in old.sealed_layer_paths))
            self.assertTrue(Path(adopted.sealed_layer_paths[0]).exists())

    def test_delete_during_export_does_not_recreate_volume_or_publish_candidate(self):
        with TemporaryDirectory(dir="/tmp") as raw:
            service, owner, entered, resume = self.fixture(Path(raw).resolve(), blocked=True)
            compactor = service._local_compactor
            with patch("ucloud_sandboxes.storage_native_compaction.LOGGER.exception"):
                try:
                    self.park_cycles(service, owner)
                    self.assertTrue(entered.wait(2))
                    service.converge_volume(owner, action="delete", operation_id="delete-during-export")
                    resume.set()
                    compactor.wait(3)
                    self.assertEqual(service.journal.load(owner.volume_id).state, StorageVolumeState.DELETED)
                    self.assertFalse((service.config.runtime_root / owner.volume_id).exists())
                    self.assertEqual(compactor.metrics()["local_compaction_completed"], 0)
                finally:
                    resume.set()
                    compactor.wait(5)

    def test_export_digest_failure_keeps_original_layers(self):
        with TemporaryDirectory(dir="/tmp") as raw:
            service, owner, _, _ = self.fixture(Path(raw).resolve())
            export = service.backend.export_compacted_image

            def corrupt(**kwargs):
                descriptor = export(**kwargs)
                return replace(descriptor, digest="sha256:" + "0" * 64)

            service.backend.export_compacted_image = corrupt
            with patch("ucloud_sandboxes.storage_native_compaction.LOGGER.exception"):
                old = self.park_cycles(service, owner)
                service._local_compactor.wait(3)
            self.assertEqual(service._local_compactor.metrics()["local_compaction_failed"], 1)
            self.assertTrue(all(Path(path).exists() for path in old.sealed_layer_paths))
            self.assertFalse((service.config.runtime_root / owner.volume_id / "local-compaction.json").exists())
            self.assertEqual(list((service.config.runtime_root / owner.volume_id).glob("local-compact-*.commit")), [])

    def test_reconciliation_cleans_crash_left_pins_but_skips_live_export(self):
        with TemporaryDirectory(dir="/tmp") as raw:
            service, owner, entered, resume = self.fixture(Path(raw).resolve(), blocked=True)
            compactor = service._local_compactor
            try:
                old = self.park_cycles(service, owner)
                self.assertTrue(entered.wait(2))
                volume = service.config.runtime_root / owner.volume_id
                live = set(volume.glob(".local-compact-*"))
                abandoned = volume / ".local-compact-abandoned"
                abandoned.mkdir()
                os.link(old.sealed_layer_paths[0], abandoned / "input.commit")
                unused = volume / "local-compact-abandoned.commit"
                unused.write_bytes(b"incomplete")
                (abandoned / "output-name").write_text(unused.name)
                service.reconcile()
                self.assertTrue(all(work.exists() for work in live))
                self.assertFalse(abandoned.exists())
                self.assertFalse(unused.exists())
                self.assertTrue(all(Path(path).exists() for path in old.sealed_layer_paths))
                resume.set()
                compactor.wait(3)
                self.assertEqual(compactor.metrics()["local_compaction_completed"], 1)
            finally:
                resume.set()
                compactor.wait(5)

    def test_disk_headroom_and_damaged_candidate_keep_original_checkpoint(self):
        with TemporaryDirectory(dir="/tmp") as raw:
            service, owner, _, _ = self.fixture(Path(raw).resolve())
            with patch("ucloud_sandboxes.storage_native_compaction.shutil.disk_usage") as usage:
                usage.return_value.free = 1024
                old = self.park_cycles(service, owner)
                service._local_compactor.wait(3)
            self.assertEqual(service._local_compactor.metrics()["local_compaction_deferred"], 1)
            manifest = service.config.runtime_root / owner.volume_id / "local-compaction.json"
            manifest.write_text('{"damaged": true}')
            mounted = service.converge_volume(owner, action="mount", operation_id="damaged-ready")
            self.assertEqual(mounted.sealed_layer_paths, old.sealed_layer_paths)

    def test_dominant_local_base_is_retained_without_reading_it(self):
        with TemporaryDirectory(dir="/tmp") as raw:
            service, owner, _, _ = self.fixture(Path(raw).resolve())
            compactor = service._local_compactor
            service._local_compactor = None
            old = self.park_cycles(service, owner)
            Path(old.sealed_layer_paths[0]).write_bytes(b"base" * 32768)
            service._local_compactor = compactor
            compactor.submit(old)
            compactor.wait(3)
            adopted = service.converge_volume(owner, action="mount", operation_id="keep-base")
            self.assertEqual(len(adopted.sealed_layer_paths), 2)
            self.assertEqual(adopted.sealed_layer_paths[0], old.sealed_layer_paths[0])
            self.assertEqual(Path(adopted.sealed_layer_paths[1]).read_bytes(), b"sealed-delta" * 2)
            self.assertTrue(Path(old.sealed_layer_paths[0]).exists())
