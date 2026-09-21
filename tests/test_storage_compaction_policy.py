from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from tests.storage_native_publisher_support import FakeExporter
from tests.test_storage_native_registry import FakeRegistry
from tests.test_storage_native_s3 import FakeS3
from ucloud_sandboxes.storage_native_publication import snapshot_compaction_start
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
                self.assertEqual(len(publication.layers), 2)
                self.assertEqual(publication.layers[0], base_layer)
                merged_inputs = exporter.compact_calls[0][0]["lowers"]
                self.assertEqual(len(merged_inputs), 4)
                self.assertNotIn(base_layer.to_dict(), merged_inputs)
                self.assertEqual(publisher.verify(publication), publication)
                # Retaining a descriptor does not upload the base again.
                self.assertEqual(publisher.metrics()["snapshot_uploaded_bytes"],
                                 64 * 1024 + 21 + len(b"flattened"))

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

    def test_failed_delta_merge_preserves_prior_publication(self):
        for backend in ("registry", "s3"):
            with self.subTest(backend=backend), TemporaryDirectory() as directory:
                root = Path(directory)
                publisher = self.publisher(backend, root, depth=2)
                base, delta, newest = [root / name for name in ("base", "delta", "newest")]
                base.write_bytes(b"b" * (64 * 1024))
                delta.write_bytes(b"delta")
                newest.write_bytes(b"newest")
                config = root / "global.json"
                config.write_text("{}")
                exporter = FakeExporter({base: base.read_bytes(), delta: delta.read_bytes()})
                previous = publisher.publish(exporter=exporter, source_layer_paths=(base, delta),
                                             virtual_size=1024**2)
                exporter.wrong_digest = True
                with self.assertRaises(ValueError):
                    publisher.publish(exporter=exporter, source_layer_paths=(newest,),
                        virtual_size=1024**2, existing_layers=previous.layers,
                        existing_repo_blob_url=publisher.repo_blob_url, global_config_path=config)
                self.assertEqual(publisher.verify(previous), previous)
                self.assertTrue(newest.exists())
                self.assertEqual(publisher.metrics()["snapshot_publications"], 1)
                self.assertEqual(publisher.metrics()["snapshot_compactions"], 0)


class CompactionSelectionTests(unittest.TestCase):
    def test_full_merge_fallbacks_and_append(self):
        cases = [
            ((1000, 10), 4, 100, True, False, None),
            ((1000, 10, 10), 2, 100, True, False, 1),
            ((1000, 10, 10), 1, 100, True, False, 0),
            ((1000, 60, 60), 4, 100, True, False, 0),
            ((100, 60, 60), 2, 1000, True, False, 0),
            ((1000, 10, 10), 2, 100, False, False, 0),
            ((1000, 10), 4, 100, True, True, 0),
            ((1000, 10, 10), 2, 100, True, True, 0),
        ]
        for sizes, depth, budget, reusable, changed, expected in cases:
            with self.subTest(sizes=sizes, depth=depth, reusable=reusable, changed=changed):
                self.assertEqual(snapshot_compaction_start(
                    sizes, max_layers=depth, max_delta_bytes=budget,
                    reusable_base=reusable, origin_changed=changed,
                ), expected)

    def test_repeated_depth_merges_keep_bounded_layers_then_growth_merges_base(self):
        layers = (1000,)
        partial = 0
        full = 0
        for _ in range(30):
            layers = (*layers, 10)
            start = snapshot_compaction_start(
                layers, max_layers=4, max_delta_bytes=100, reusable_base=True,
            )
            if start is not None:
                partial += start == 1
                full += start == 0
                layers = (*layers[:start], sum(layers[start:]))
            self.assertLessEqual(len(layers), 4)
        self.assertGreater(partial, 1)
        self.assertGreater(full, 1)
