from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from tests.storage_native_publisher_support import FakeExporter
from tests.test_storage_native_registry import FakeRegistry
from tests.test_storage_native_s3 import FakeS3
from ucloud_sandboxes.storage_native_registry import RegistrySnapshotPublisher
from ucloud_sandboxes.storage_native_s3 import S3SnapshotPublisher


class StorageCompactionPolicyTests(unittest.TestCase):
    def publisher(self, backend, root, *, depth=4):
        options = dict(stream_socket_root=root, compact_after_layers=depth,
                       compact_after_bytes=32 * 1024)
        if backend == "registry":
            return RegistrySnapshotPublisher(FakeRegistry(), repository="snapshots", **options)
        client = FakeS3()
        return S3SnapshotPublisher(
            endpoint="https://s3.example", bucket="test-bucket", region="test",
            prefix="test", credential_process="/bin/false",
            client_factory=lambda: client, **options,
        )

    def test_large_base_and_sparse_small_deltas_append_until_depth_requires_compaction(self):
        for backend in ("registry", "s3"):
            with self.subTest(backend=backend), TemporaryDirectory() as directory:
                root = Path(directory)
                publisher = self.publisher(backend, root)
                base = root / "base.commit"
                base.write_bytes(b"b" * (64 * 1024))
                exporter = FakeExporter({base: base.read_bytes()}, compact_payload=b"flattened")
                # A single base larger than the threshold needs no flatten or
                # global config; dense export already emits one exact layer.
                publication = publisher.publish(
                    exporter=exporter, source_layer_paths=(base,), virtual_size=16 * 1024**3,
                )
                base_layer = publication.layers[0]
                for index in range(3):
                    delta = root / f"delta-{index}.commit"
                    with delta.open("wb") as stream:
                        stream.write(b"header")
                        stream.seek(8 * 1024**3)
                        stream.write(b"footer")
                    if getattr(delta.stat(), "st_blocks", delta.stat().st_size) * 512 > 32 * 1024:
                        self.skipTest("filesystem does not expose small sparse allocations")
                    exporter.payloads[delta] = f"delta-{index}".encode()
                    publication = publisher.publish(
                        exporter=exporter, source_layer_paths=(delta,),
                        virtual_size=16 * 1024**3, existing_layers=publication.layers,
                        existing_repo_blob_url=publisher.repo_blob_url,
                    )
                    self.assertEqual(len(publication.layers), index + 2)
                    self.assertEqual(publication.layers[0], base_layer)
                    self.assertEqual(publisher.verify(publication), publication)
                self.assertEqual(exporter.compact_calls, [])
                self.assertEqual(publisher.metrics()["snapshot_uploaded_bytes"], 64 * 1024 + 21)
                config = root / "global.json"
                config.write_text("{}")
                delta = root / "last.commit"
                delta.write_bytes(b"last")
                publication = publisher.publish(
                    exporter=exporter, source_layer_paths=(delta,),
                    virtual_size=16 * 1024**3, existing_layers=publication.layers,
                    existing_repo_blob_url=publisher.repo_blob_url, global_config_path=config,
                )
                self.assertEqual(len(exporter.compact_calls), 1)
                self.assertEqual(len(publication.layers), 1)
                self.assertEqual(publisher.verify(publication), publication)

    def test_accumulated_delta_bytes_still_trigger_compaction(self):
        for backend in ("registry", "s3"):
            with self.subTest(backend=backend), TemporaryDirectory() as directory:
                root = Path(directory)
                publisher = self.publisher(backend, root, depth=8)
                base = root / "base.commit"
                base.write_bytes(b"b" * (64 * 1024))
                first = root / "first.commit"
                first.write_bytes(b"x" * (20 * 1024))
                second = root / "second.commit"
                second.write_bytes(b"y" * (20 * 1024))
                config = root / "global.json"
                config.write_text("{}")
                exporter = FakeExporter({base: base.read_bytes(), first: first.read_bytes()})
                publication = publisher.publish(
                    exporter=exporter, source_layer_paths=(base, first), virtual_size=1024**2,
                )
                self.assertEqual(len(publication.layers), 2)
                publication = publisher.publish(
                    exporter=exporter, source_layer_paths=(second,), virtual_size=1024**2,
                    existing_layers=publication.layers,
                    existing_repo_blob_url=publisher.repo_blob_url, global_config_path=config,
                )
                self.assertEqual(len(exporter.compact_calls), 1)
                self.assertEqual(len(publication.layers), 1)
                self.assertEqual(publisher.verify(publication), publication)
