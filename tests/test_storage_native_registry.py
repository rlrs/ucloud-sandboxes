from __future__ import annotations

import hashlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from tests.storage_native_publisher_support import FakeExporter
from ucloud_sandboxes.storage_native import StorageNativeLayer
from ucloud_sandboxes.storage_native_registry import (
    PublishedStorageLayer,
    RegistrySnapshotPublisher,
    SNAPSHOT_SCHEMA,
)


class FakeRegistry:
    base_url = "http://registry:5000"

    def __init__(self) -> None:
        self.uploads: dict[str, bytearray] = {}
        self.blobs: dict[str, bytes] = {}
        self.manifests: dict[str, bytes] = {}
        self.next_upload = 0

    def start_blob_upload(self, _repository: str) -> str:
        self.next_upload += 1
        location = f"/v2/snapshots/blobs/uploads/{self.next_upload}"
        self.uploads[location] = bytearray()
        return location

    def upload_blob_chunk(self, location: str, chunk: bytes) -> str:
        self.uploads[location].extend(chunk)
        return location

    def finish_blob_upload(self, location: str, digest: str) -> str:
        payload = bytes(self.uploads.pop(location))
        observed = f"sha256:{hashlib.sha256(payload).hexdigest()}"
        if observed != digest:
            raise ValueError("digest mismatch")
        self.blobs[digest] = payload
        return digest

    def blob_exists(self, _repository: str, digest: str) -> bool:
        return digest in self.blobs

    def put_manifest(
        self,
        _repository: str,
        reference: str,
        payload: bytes,
        *,
        media_type: str,
    ) -> str:
        digest = f"sha256:{hashlib.sha256(payload).hexdigest()}"
        self.manifests[reference] = payload
        self.manifests[digest] = payload
        return digest

    def manifest_document(
        self,
        _repository: str,
        reference: str,
    ) -> tuple[dict, dict[str, str]]:
        payload = self.manifests[reference]
        digest = f"sha256:{hashlib.sha256(payload).hexdigest()}"
        return (
            json.loads(payload.decode("ascii")),
            {"Docker-Content-Digest": digest},
        )

    def blob_bytes(
        self,
        _repository: str,
        digest: str,
        *,
        max_bytes: int,
    ) -> bytes:
        payload = self.blobs[digest]
        if len(payload) > max_bytes:
            raise ValueError("too large")
        return payload


class StorageNativeRegistryTests(unittest.TestCase):
    def test_streams_layers_and_publishes_content_addressed_manifest(self) -> None:
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            first = (root / "first.commit").resolve()
            second = (root / "second.commit").resolve()
            payloads = {
                first: b"first-dense-layer" * 17,
                second: b"second-dense-layer" * 9,
            }
            for path, payload in payloads.items():
                path.write_bytes(payload)
            registry = FakeRegistry()
            publisher = RegistrySnapshotPublisher(
                registry,  # type: ignore[arg-type]
                repository="ucloud/sandbox-snapshots",
                stream_socket_root=root,
                upload_chunk_bytes=31,
            )

            publication = publisher.publish(
                exporter=FakeExporter(payloads),
                source_layer_paths=(first, second),
                virtual_size=1 << 30,
            )

            self.assertEqual(
                publication.repo_blob_url,
                "http://registry:5000/v2/ucloud/sandbox-snapshots/blobs",
            )
            self.assertEqual(len(publication.layers), 2)
            for layer, payload in zip(
                publication.layers,
                payloads.values(),
                strict=True,
            ):
                self.assertEqual(registry.blobs[layer.digest], payload)
            manifest = json.loads(registry.manifests[publication.tag].decode("ascii"))
            config_digest = manifest["config"]["digest"]
            config = json.loads(registry.blobs[config_digest].decode("ascii"))
            self.assertEqual(config["schema"], SNAPSHOT_SCHEMA)
            self.assertEqual(config["virtualSize"], 1 << 30)
            self.assertEqual(
                [item["digest"] for item in config["layers"]],
                [layer.digest for layer in publication.layers],
            )
            self.assertEqual(publisher.verify(publication), publication)

    def test_rejects_stream_that_disagrees_with_backend_descriptor(self) -> None:
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            source = (root / "layer.commit").resolve()
            source.write_bytes(b"payload")
            publisher = RegistrySnapshotPublisher(
                FakeRegistry(),  # type: ignore[arg-type]
                repository="snapshots",
                stream_socket_root=root,
            )
            with self.assertRaisesRegex(ValueError, "does not match"):
                publisher.publish(
                    exporter=FakeExporter(
                        {source: b"payload"},
                        wrong_digest=True,
                    ),
                    source_layer_paths=(source,),
                    virtual_size=4096,
                )

    def test_export_failure_before_connect_is_propagated(self) -> None:
        class FailingExporter(FakeExporter):
            def export_dense_layer(self, **_kwargs) -> StorageNativeLayer:
                raise RuntimeError("injected export failure")

        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            source = (root / "layer.commit").resolve()
            source.write_bytes(b"payload")
            publisher = RegistrySnapshotPublisher(
                FakeRegistry(),  # type: ignore[arg-type]
                repository="snapshots",
                stream_socket_root=root,
            )

            with self.assertRaisesRegex(RuntimeError, "injected export failure"):
                publisher.publish(
                    exporter=FailingExporter({}),
                    source_layer_paths=(source,),
                    virtual_size=4096,
                )

    def test_compacts_mixed_remote_and_local_chain_at_layer_threshold(self) -> None:
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            delta = (root / "delta.commit").resolve()
            delta.write_bytes(b"local-delta")
            global_config = (root / "global.json").resolve()
            global_config.write_text("{}\n", encoding="ascii")
            exporter = FakeExporter({}, compact_payload=b"flattened-chain")
            registry = FakeRegistry()
            publisher = RegistrySnapshotPublisher(
                registry,  # type: ignore[arg-type]
                repository="snapshots",
                stream_socket_root=root,
                compact_after_layers=2,
                compact_after_bytes=1 << 30,
            )
            existing = (
                PublishedStorageLayer("sha256:" + "1" * 64, 100),
                PublishedStorageLayer("sha256:" + "2" * 64, 200),
            )

            publication = publisher.publish(
                exporter=exporter,
                source_layer_paths=(delta,),
                virtual_size=1 << 30,
                existing_layers=existing,
                global_config_path=global_config,
            )

            self.assertEqual(len(publication.layers), 1)
            self.assertEqual(
                registry.blobs[publication.layers[0].digest],
                b"flattened-chain",
            )
            self.assertEqual(len(exporter.compact_calls), 1)
            source, observed_global = exporter.compact_calls[0]
            self.assertEqual(observed_global, global_config)
            self.assertEqual(source["repoBlobUrl"], publisher.repo_blob_url)
            self.assertEqual(
                source["lowers"][:2], [layer.to_dict() for layer in existing]
            )
            self.assertEqual(source["lowers"][2], {"file": str(delta)})
            metrics = publisher.metrics()
            self.assertEqual(
                {
                    key: metrics[key]
                    for key in (
                        "snapshot_publications",
                        "snapshot_compactions",
                        "snapshot_uploaded_bytes",
                        "snapshot_compaction_input_layers",
                        "snapshot_compaction_input_bytes",
                        "snapshot_compaction_output_bytes",
                        "snapshot_compact_after_layers",
                        "snapshot_compact_after_bytes",
                        "snapshot_publication_limit",
                        "snapshot_publication_active",
                        "snapshot_publication_waiting",
                    )
                },
                {
                    "snapshot_publications": 1,
                    "snapshot_compactions": 1,
                    "snapshot_uploaded_bytes": len(b"flattened-chain"),
                    "snapshot_compaction_input_layers": 3,
                    "snapshot_compaction_input_bytes": 311,
                    "snapshot_compaction_output_bytes": len(b"flattened-chain"),
                    "snapshot_compact_after_layers": 2,
                    "snapshot_compact_after_bytes": 1 << 30,
                    "snapshot_publication_limit": 4,
                    "snapshot_publication_active": 0,
                    "snapshot_publication_waiting": 0,
                },
            )
            self.assertGreaterEqual(metrics["snapshot_publication_duration_ms_total"], 0)
            self.assertGreaterEqual(metrics["snapshot_publication_duration_ms_max"], 0)
            self.assertGreaterEqual(
                metrics["snapshot_publication_queue_wait_ms_total"], 0
            )
            self.assertGreaterEqual(metrics["snapshot_publication_queue_wait_ms_max"], 0)


if __name__ == "__main__":
    unittest.main()
