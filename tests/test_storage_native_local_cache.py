import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from tests import test_storage_native_daemon as fixtures
from tests.storage_native_publisher_support import FakeExporter
from tests.test_storage_native_registry import FakeRegistry
from tests.test_storage_native_s3 import FakeS3
from ucloud_sandboxes.storage_native_daemon import StorageVolumeOwner, StorageVolumeState
from ucloud_sandboxes.storage_native_local_cache import PublishedLocalCache
from ucloud_sandboxes.storage_native_registry import RegistrySnapshotPublisher
from ucloud_sandboxes.storage_native_s3 import S3SnapshotPublisher


class PublishedLocalCacheTests(unittest.TestCase):
    def test_unavailable_cache_does_not_prevent_remote_fallback(self):
        with TemporaryDirectory() as raw:
            root = Path(raw)
            source = root / "source"
            source.write_bytes(b"checkpoint")
            with patch.object(Path, "mkdir", side_effect=OSError("disk full")), \
                 patch("ucloud_sandboxes.storage_native_local_cache.LOGGER.warning"):
                cache = PublishedLocalCache(root / "cache", capacity_bytes=4096)
            cache.remember("origin", "digest", source)
            self.assertIsNone(cache.pin("origin", "digest", root))
            self.assertEqual(cache.metrics()["published_local_cache_entries"], 0)

    def test_eviction_and_restart_preserve_active_mount_pins(self):
        with TemporaryDirectory() as raw:
            root = Path(raw)
            source = root / "source"
            source.write_bytes(b"immutable checkpoint")
            volume = root / "volume"
            volume.mkdir()
            cache = PublishedLocalCache(root / "cache", capacity_bytes=4096)
            cache.remember("origin", "first", source)
            pin = cache.pin("origin", "first", volume)
            self.assertEqual(pin.stat().st_ino, source.stat().st_ino)
            source.unlink()
            replacement = root / "replacement"
            replacement.write_bytes(b"x" * 4096)
            cache.remember("origin", "second", replacement)
            self.assertIsNone(cache.pin("origin", "first", volume))
            self.assertEqual(pin.read_bytes(), b"immutable checkpoint")
            restarted = PublishedLocalCache(root / "cache", capacity_bytes=4096)
            self.assertIsNone(restarted.pin("origin", "second", volume))
            self.assertEqual(pin.read_bytes(), b"immutable checkpoint")

    def test_pressure_or_disabled_cache_falls_back_without_retaining_data(self):
        with TemporaryDirectory() as raw:
            root = Path(raw)
            source = root / "source"
            source.write_bytes(b"checkpoint")
            for capacity in (0, 4096):
                cache = PublishedLocalCache(root / "cache", capacity_bytes=capacity)
                with patch.object(cache, "_has_headroom", return_value=False):
                    cache.remember("origin", "digest", source)
                    self.assertIsNone(cache.pin("origin", "digest", root))
                self.assertEqual(cache.metrics()["published_local_cache_bytes"], 0)
            cache.remember("origin", "digest", source)
            self.assertIsNone(cache.pin("other-origin", "digest", root))
            with patch.object(cache, "_has_headroom", return_value=False):
                cache.maintain()
            self.assertEqual(cache.metrics()["published_local_cache_entries"], 0)

    def test_publish_wake_uses_local_equivalent_and_release_reclaims_pins(self):
        for backend in ("registry", "s3"):
            with self.subTest(backend=backend), TemporaryDirectory() as raw:
                root = Path(raw)
                service, native, host = fixtures.StorageNativeNodeServiceTests()._service(root)
                if backend == "registry":
                    service.publisher = RegistrySnapshotPublisher(FakeRegistry(), repository="snapshots", stream_socket_root=root)
                else:
                    client = FakeS3()
                    service.publisher = S3SnapshotPublisher(endpoint="https://s3.example", bucket="test", region="test",
                        prefix="test", credential_process="/bin/false", client_factory=lambda: client, stream_socket_root=root)
                # Use the real streaming publisher while the fixture supplies
                # lifecycle/device operations. The native format is qualified separately.
                native.export_dense_layer = lambda **kw: FakeExporter({kw["source_layer_path"]: kw["source_layer_path"].read_bytes()}).export_dense_layer(**kw)
                owner = StorageVolumeOwner("volume", "sandbox", 1)
                service.converge_volume(owner, action="prepare", operation_id="create", virtual_size=1 << 30)
                released = service.converge_volume(owner, action="release", operation_id="park")
                inode = Path(released.sealed_layer_paths[0]).stat().st_ino
                published = service.converge_volume(owner, action="publish", operation_id="publish")
                self.assertEqual(published.state, StorageVolumeState.PUBLISHED)
                self.assertFalse(Path(released.sealed_layer_paths[0]).exists())
                mounted = service.converge_volume(owner, action="mount", operation_id="wake")
                lowers = json.loads(Path(mounted.source_image_config).read_text())["lowers"]
                self.assertEqual(len(lowers), 1)
                pin = Path(lowers[0]["file"])
                self.assertEqual(pin.stat().st_ino, inode)
                self.assertIn(str(pin), mounted.cached_layer_paths)
                self.assertEqual(mounted.published_layers, published.published_layers)
                self.assertEqual(service.metrics()["cache_bytes"], pin.stat().st_size)
                orphan = pin.parent / "published-local-orphan.commit"
                orphan.write_bytes(b"crash-left pin")
                service.reconcile()
                self.assertFalse(orphan.exists())
                self.assertTrue(pin.exists())
                # Busy retired device still needs its local source names.
                host.busy_devices.add(Path(mounted.device_path))
                service.converge_volume(owner, action="release", operation_id="park-again")
                self.assertTrue(pin.exists())
                next_mount = service.converge_volume(owner, action="mount", operation_id="wake-again")
                next_pin = Path(json.loads(Path(next_mount.source_image_config).read_text())["lowers"][0]["file"])
                host.busy_devices.clear()
                self.assertEqual(service._reap_retired_devices(), 1)
                self.assertFalse(pin.exists())
                self.assertTrue(next_pin.exists())
                service.converge_volume(owner, action="release", operation_id="final-park")
                self.assertFalse(next_pin.exists())

    def test_missing_cache_and_failed_pin_use_remote_descriptors(self):
        with TemporaryDirectory() as raw:
            root = Path(raw)
            service, _, _ = fixtures.StorageNativeNodeServiceTests()._service(root, publisher=True)
            owner = StorageVolumeOwner("volume", "sandbox", 1)
            service.converge_volume(owner, action="prepare", operation_id="create", virtual_size=1 << 30)
            service.converge_volume(owner, action="release", operation_id="park")
            published = service.converge_volume(owner, action="publish", operation_id="publish")
            mounted = service.converge_volume(owner, action="mount", operation_id="wake")
            self.assertEqual(json.loads(Path(mounted.source_image_config).read_text())["lowers"], list(published.published_layers))

    def test_retention_failure_does_not_fail_committed_publication(self):
        with TemporaryDirectory() as raw:
            service, _, _ = fixtures.StorageNativeNodeServiceTests()._service(Path(raw), publisher=True)
            service.publisher.local_layer_sources = lambda _: (_ for _ in ()).throw(OSError("cache unavailable"))
            owner = StorageVolumeOwner("volume", "sandbox", 1)
            service.converge_volume(owner, action="prepare", operation_id="create", virtual_size=1 << 30)
            service.converge_volume(owner, action="release", operation_id="park")
            with patch("ucloud_sandboxes.storage_native_daemon.LOGGER.warning"):
                published = service.converge_volume(owner, action="publish", operation_id="publish")
            self.assertEqual(published.state, StorageVolumeState.PUBLISHED)

    def test_compacted_blob_is_not_mapped_to_a_single_unmerged_input(self):
        with TemporaryDirectory() as raw:
            root = Path(raw)
            a, b, config = root / "a", root / "b", root / "config"
            a.write_bytes(b"a")
            b.write_bytes(b"b")
            config.write_text("{}")
            publisher = RegistrySnapshotPublisher(FakeRegistry(), repository="snapshots", stream_socket_root=root, compact_after_layers=1)
            publisher.publish(exporter=FakeExporter({a: b"a", b: b"b"}), source_layer_paths=(a,b), virtual_size=4096, global_config_path=config)
            self.assertEqual(publisher.local_layer_sources((a,b)), {})
