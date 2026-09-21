from pathlib import Path
import os
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from tests.storage_native_publisher_support import FakeExporter, digest
from tests.test_storage_native_registry import FakeRegistry
from tests.test_storage_native_s3 import FakeS3
from ucloud_sandboxes.storage_native_publication import CompletedLayerUploads, local_layer_identity
from ucloud_sandboxes.storage_native_registry import PublishedStorageLayer, RegistrySnapshotPublisher
from ucloud_sandboxes.storage_native_s3 import S3SnapshotPublisher


class CompletedUploadTests(unittest.TestCase):
    def test_hardlink_pins_during_export_and_retry_preserve_completed_upload(self):
        for backend in ("registry", "s3"):
            for compact in (False, True):
                with self.subTest(backend=backend, compact=compact), TemporaryDirectory() as raw:
                    publisher, _, exporter, kwargs = self.fixture(Path(raw), backend, compact)
                    source = kwargs["source_layer_paths"][0]
                    pin = source.with_suffix(".pin")
                    method = "export_compacted_image" if compact else "export_dense_layer"
                    original = getattr(exporter, method)

                    def pin_during_export(**request):
                        # Background compaction and the published-local cache
                        # pin immutable inputs without changing their contents.
                        os.link(source, pin)
                        return original(**request)

                    with patch.object(exporter, method, side_effect=pin_during_export):
                        publication = publisher.publish(**kwargs)
                    pin.unlink()
                    with patch.object(exporter, method, side_effect=AssertionError("re-exported")):
                        reused = publisher.publish(**kwargs)
                    self.assertEqual(reused, publication)
                    self.assertEqual(publisher.verify(reused), publication)
                    self.assertEqual(publisher.metrics()["snapshot_reused_layers"], 1)
                    if not compact:
                        self.assertEqual(publisher._completed_uploads.completed_dense_sources((source,)),
                                         {publication.layers[0].digest: source})

    def test_content_mutation_during_export_is_still_rejected(self):
        with TemporaryDirectory() as raw:
            source = Path(raw) / "sealed.commit"
            source.write_bytes(b"old")
            # Make detection independent of the filesystem's clock resolution.
            os.utime(source, ns=(1_000_000_000, 1_000_000_000))
            cache = CompletedLayerUploads()
            layer = PublishedStorageLayer("sha256:" + "1" * 64, 3)

            def upload():
                source.write_bytes(b"new")
                return layer

            with self.assertRaisesRegex(ValueError, "inputs changed during upload"):
                cache.publish(identity=lambda: local_layer_identity(source), upload=upload,
                              exists=lambda _: True, check_current=None)
            self.assertEqual(cache.completed_dense_sources((source,)), {})

    def fixture(self, root, backend, compact=False):
        source, config = root / "sealed.commit", root / "global.json"
        source.write_bytes(b"old layer")
        config.write_text("{}")
        options = dict(stream_socket_root=root, compact_after_layers=1 if compact else 8)
        if backend == "registry":
            client = FakeRegistry()
            publisher = RegistrySnapshotPublisher(client, repository="snapshots", **options)
        else:
            client = FakeS3()
            publisher = S3SnapshotPublisher(
                endpoint="https://s3.example", bucket="test", region="test", prefix="test",
                credential_process="/bin/false", client_factory=lambda: client, **options,
            )
        paths = (source,)
        if compact:
            delta = root / "delta.commit"
            delta.write_bytes(b"delta")
            paths = (*paths, delta)
        exporter = FakeExporter({path: path.read_bytes() for path in paths})
        kwargs = dict(exporter=exporter, source_layer_paths=paths, virtual_size=4096,
                      global_config_path=config)
        return publisher, client, exporter, kwargs

    def test_metadata_failure_reuses_completed_dense_and_compacted_uploads(self):
        for backend in ("registry", "s3"):
            for compact in (False, True):
                with self.subTest(backend=backend, compact=compact), TemporaryDirectory() as raw:
                    publisher, _, exporter, kwargs = self.fixture(Path(raw), backend, compact)
                    with patch.object(RegistrySnapshotPublisher, "_snapshot_config", side_effect=RuntimeError("metadata failed")):
                        with self.assertRaisesRegex(RuntimeError, "metadata failed"):
                            publisher.publish(**kwargs)
                    with patch.object(exporter, "export_dense_layer", side_effect=AssertionError("re-exported")), \
                         patch.object(exporter, "export_compacted_image", side_effect=AssertionError("re-compacted")):
                        publication = publisher.publish(**kwargs)
                    self.assertEqual(publisher.verify(publication), publication)
                    self.assertEqual(publisher.metrics()["snapshot_reused_layers"], 1)
                    self.assertEqual(publisher.metrics()["snapshot_reused_layer_bytes"], publication.layers[0].size)
                    self.assertEqual(publisher.metrics()["snapshot_uploaded_bytes"], 0)

    def test_missing_remote_blob_is_exported_again(self):
        for backend in ("registry", "s3"):
            with self.subTest(backend=backend), TemporaryDirectory() as raw:
                publisher, client, exporter, kwargs = self.fixture(Path(raw), backend)
                original = publisher.publish(**kwargs)
                if backend == "registry":
                    del client.blobs[original.layers[0].digest]
                else:
                    del client.objects[publisher._layer_key(original.layers[0].digest)]
                with patch.object(exporter, "export_dense_layer", wraps=exporter.export_dense_layer) as export:
                    restored = publisher.publish(**kwargs)
                export.assert_called_once()
                self.assertEqual(publisher.verify(restored), original)
                self.assertEqual(publisher.metrics()["snapshot_reused_layers"], 0)

    def test_changed_input_and_compaction_context_do_not_reuse_old_upload(self):
        for backend in ("registry", "s3"):
            for compact in (False, True):
                with self.subTest(backend=backend, compact=compact), TemporaryDirectory() as raw:
                    publisher, _, exporter, kwargs = self.fixture(Path(raw), backend, compact)
                    publisher.publish(**kwargs)
                    if compact:
                        # Same layers but changed backend configuration must not
                        # inherit an export prepared under an earlier context.
                        kwargs["global_config_path"].write_text('{"changed": true}')
                        exporter.compact_payload = b"new compact"
                        method = "export_compacted_image"
                    else:
                        source = kwargs["source_layer_paths"][0]
                        replacement = source.with_suffix(".replacement")
                        replacement.write_bytes(b"new layer")
                        replacement.replace(source)
                        exporter.payloads[source] = b"new layer"
                        method = "export_dense_layer"
                    with patch.object(exporter, method, wraps=getattr(exporter, method)) as export:
                        publication = publisher.publish(**kwargs)
                    export.assert_called_once()
                    self.assertEqual(publication.layers[0].digest, digest(b"new compact" if compact else b"new layer"))
                    self.assertEqual(publisher.metrics()["snapshot_reused_layers"], 0)

    def test_failed_second_layer_reuses_first_and_exports_new_delta(self):
        for backend in ("registry", "s3"):
            with self.subTest(backend=backend), TemporaryDirectory() as raw:
                root = Path(raw)
                publisher, _, exporter, kwargs = self.fixture(root, backend)
                second, third = root / "second.commit", root / "third.commit"
                for path in (second, third):
                    path.write_bytes(path.name.encode())
                    exporter.payloads[path] = path.read_bytes()
                kwargs["source_layer_paths"] += (second,)
                original_export = exporter.export_dense_layer

                def interrupted(**request):
                    if request["source_layer_path"] == second:
                        raise RuntimeError("second interrupted")
                    return original_export(**request)

                with patch.object(exporter, "export_dense_layer", side_effect=interrupted):
                    with self.assertRaisesRegex(RuntimeError, "second interrupted"):
                        publisher.publish(**kwargs)
                kwargs["source_layer_paths"] += (third,)
                with patch.object(exporter, "export_dense_layer", wraps=original_export) as export:
                    publication = publisher.publish(**kwargs)
                self.assertEqual([call.kwargs["source_layer_path"] for call in export.call_args_list], [second, third])
                self.assertEqual(publisher.verify(publication), publication)
                self.assertEqual(publisher.metrics()["snapshot_reused_layers"], 1)

    def test_cache_hits_still_obey_ownership_after_remote_check(self):
        cache = CompletedLayerUploads()
        layer = PublishedStorageLayer("sha256:" + "1" * 64, 1024)
        canceled = False

        def check():
            if canceled:
                raise RuntimeError("superseded")

        def exists(_):
            nonlocal canceled
            canceled = True
            return True

        kwargs = dict(identity=lambda: ("fixed",), upload=lambda: layer, exists=exists, check_current=check)
        cache.publish(**kwargs)
        with self.assertRaisesRegex(RuntimeError, "superseded"):
            cache.publish(**kwargs)
        self.assertEqual(cache.metrics()["snapshot_reused_layers"], 0)

    def test_eviction_only_causes_reupload_and_mutated_inputs_are_not_cached(self):
        cache = CompletedLayerUploads(capacity=1)
        layer = PublishedStorageLayer("sha256:" + "1" * 64, 1024)
        version = 0

        def upload():
            nonlocal version
            version += 1
            return layer

        with self.assertRaisesRegex(ValueError, "inputs changed"):
            cache.publish(identity=lambda: (version,), upload=upload, exists=lambda _: True, check_current=None)
        for key in (1, 2, 1):
            _, uploaded = cache.publish(identity=lambda: (key,), upload=lambda: layer,
                                        exists=lambda _: True, check_current=None)
            self.assertEqual(uploaded, 1024)
        self.assertEqual(cache.metrics()["snapshot_reused_layers"], 0)
