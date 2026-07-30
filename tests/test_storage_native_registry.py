from __future__ import annotations

import hashlib
import json
from pathlib import Path
import socket
from tempfile import TemporaryDirectory
import unittest

from ucloud_sandboxes.storage_native import StorageNativeLayer
from ucloud_sandboxes.storage_native_registry import (
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


class FakeExporter:
    def __init__(self, payloads: dict[Path, bytes], *, wrong_digest: bool = False):
        self.payloads = payloads
        self.wrong_digest = wrong_digest

    def export_dense_layer(
        self,
        *,
        source_layer_path: Path,
        stream_socket_path: Path,
    ) -> StorageNativeLayer:
        payload = self.payloads[source_layer_path]
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
            connection.connect(str(stream_socket_path))
            for offset in range(0, len(payload), 7):
                connection.sendall(payload[offset : offset + 7])
        digest = hashlib.sha256(payload).hexdigest()
        if self.wrong_digest:
            digest = "0" * 64
        return StorageNativeLayer(
            digest=f"sha256:{digest}",
            size=len(payload),
        )


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
            manifest = json.loads(
                registry.manifests[publication.tag].decode("ascii")
            )
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


if __name__ == "__main__":
    unittest.main()
