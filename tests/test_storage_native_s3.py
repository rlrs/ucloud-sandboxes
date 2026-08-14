from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from tests.storage_native_publisher_support import FakeExporter, digest
from ucloud_sandboxes.storage_native_registry import PublishedStorageLayer
from ucloud_sandboxes.storage_native_s3 import (
    S3ObjectStat,
    S3SnapshotPublisher,
)


class FakeS3:
    def __init__(
        self,
        *,
        lose_complete_response: bool = False,
    ) -> None:
        self.objects: dict[str, bytes] = {}
        self.uploads: dict[tuple[str, str], dict[int, bytes]] = {}
        self.aborted: list[tuple[str, str]] = []
        self.next_upload = 0
        self.modified_at: dict[str, float] = {}
        self.lose_complete_response = lose_complete_response

    def create_multipart_upload(self, key: str) -> str:
        self.next_upload += 1
        upload_id = str(self.next_upload)
        self.uploads[(key, upload_id)] = {}
        return upload_id

    def upload_part(
        self, key: str, upload_id: str, part_number: int, payload: bytes
    ) -> str:
        self.uploads[(key, upload_id)][part_number] = payload
        return f"etag-{part_number}"

    def complete_multipart_upload(self, key, upload_id, parts) -> None:
        upload = self.uploads.pop((key, upload_id))
        self.objects[key] = b"".join(upload[number] for number, _etag in parts)
        if self.lose_complete_response:
            raise TimeoutError("injected lost completion response")

    def abort_multipart_upload(self, key: str, upload_id: str) -> None:
        self.uploads.pop((key, upload_id), None)
        self.aborted.append((key, upload_id))

    def stat(self, key: str) -> S3ObjectStat | None:
        payload = self.objects.get(key)
        return (
            None
            if payload is None
            else S3ObjectStat(
                size=len(payload), modified_at=self.modified_at.get(key, 0.0)
            )
        )

    def put_bytes(self, key: str, payload: bytes, *, sha256: str) -> None:
        if digest(payload) != sha256:
            raise ValueError("bad digest")
        self.objects[key] = payload

    def get_bytes(self, key: str, *, max_bytes: int) -> bytes:
        payload = self.objects[key]
        if len(payload) > max_bytes:
            raise ValueError("too large")
        return payload

    def copy(self, source_key: str, destination_key: str, *, size: int) -> None:
        payload = self.objects[source_key]
        if len(payload) != size:
            raise ValueError("bad size")
        self.objects[destination_key] = payload

    def delete(self, key: str) -> None:
        self.objects.pop(key, None)

    def list_objects(self, prefix: str):
        return tuple(
            (key, self.stat(key))
            for key in sorted(self.objects)
            if key.startswith(prefix) and self.stat(key) is not None
        )


class StorageNativeS3Tests(unittest.TestCase):
    def _publisher(self, root: Path, s3: FakeS3) -> S3SnapshotPublisher:
        return S3SnapshotPublisher(
            endpoint="https://fsn1.example",
            bucket="sandbox-bucket",
            region="fsn1",
            prefix="ucloud/test",
            credential_process="/bin/false",
            stream_socket_root=root,
            upload_chunk_bytes=5 * 1024 * 1024,
            client_factory=lambda: s3,
        )

    def test_streams_to_temporary_object_then_commits_content_addressed_state(self):
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            source = (root / "delta.commit").resolve()
            payload = b"snapshot-layer" * 100
            source.write_bytes(payload)
            s3 = FakeS3()
            publisher = self._publisher(root, s3)

            publication = publisher.publish(
                exporter=FakeExporter({source: payload}),
                source_layer_paths=(source,),
                virtual_size=1 << 30,
            )

            self.assertEqual(publication.backend, "s3")
            self.assertEqual(
                publication.repo_blob_url,
                "s3://sandbox-bucket/ucloud/test/managed-layers",
            )
            self.assertEqual(
                s3.objects[
                    f"ucloud/test/managed-layers/{publication.layers[0].digest}"
                ],
                payload,
            )
            self.assertFalse(any("/.uploads/" in key for key in s3.objects))
            self.assertEqual(publisher.verify(publication), publication)

    def test_backend_switch_forces_compaction_using_old_blob_origin(self):
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            source = (root / "delta.commit").resolve()
            source.write_bytes(b"delta")
            global_config = (root / "global.json").resolve()
            global_config.write_text("{}\n", encoding="ascii")
            s3 = FakeS3()
            exporter = FakeExporter({source: b"delta"}, compact_payload=b"flattened")
            publisher = self._publisher(root, s3)

            publication = publisher.publish(
                exporter=exporter,
                source_layer_paths=(source,),
                virtual_size=4096,
                existing_layers=(PublishedStorageLayer("sha256:" + "1" * 64, 9),),
                existing_repo_blob_url="http://old-registry/v2/snapshots/blobs",
                global_config_path=global_config,
            )

            self.assertEqual(len(publication.layers), 1)
            self.assertEqual(
                exporter.compact_calls[0][0]["repoBlobUrl"],
                "http://old-registry/v2/snapshots/blobs",
            )
            self.assertEqual(publisher.metrics()["snapshot_compactions"], 1)

    def test_export_failure_aborts_multipart_and_commits_no_manifest(self):
        class FailingExporter(FakeExporter):
            def export_dense_layer(self, **kwargs):
                raise RuntimeError("injected")

        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            source = (root / "delta.commit").resolve()
            source.write_bytes(b"delta")
            s3 = FakeS3()
            publisher = self._publisher(root, s3)

            with self.assertRaisesRegex(RuntimeError, "injected"):
                publisher.publish(
                    exporter=FailingExporter({}),
                    source_layer_paths=(source,),
                    virtual_size=4096,
                )

            self.assertTrue(s3.aborted)
            self.assertFalse(s3.objects)
            self.assertFalse(s3.uploads)

    def test_lost_multipart_completion_response_is_resolved_by_stat(self):
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            source = (root / "delta.commit").resolve()
            payload = b"durable-after-timeout"
            source.write_bytes(payload)
            s3 = FakeS3(lose_complete_response=True)
            publisher = self._publisher(root, s3)

            publication = publisher.publish(
                exporter=FakeExporter({source: payload}),
                source_layer_paths=(source,),
                virtual_size=4096,
            )

            self.assertEqual(publication.layers[0].digest, digest(payload))
            self.assertFalse(any("/.uploads/" in key for key in s3.objects))
            self.assertFalse(s3.aborted)

    def test_verify_rejects_missing_layer(self):
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            source = (root / "delta.commit").resolve()
            source.write_bytes(b"delta")
            s3 = FakeS3()
            publisher = self._publisher(root, s3)
            publication = publisher.publish(
                exporter=FakeExporter({source: b"delta"}),
                source_layer_paths=(source,),
                virtual_size=4096,
            )
            del s3.objects[f"ucloud/test/managed-layers/{publication.layers[0].digest}"]

            with self.assertRaisesRegex(ValueError, "missing"):
                publisher.verify(publication)


if __name__ == "__main__":
    unittest.main()
