from __future__ import annotations

import hashlib
import json
from pathlib import Path
import socket
from tempfile import TemporaryDirectory
import threading
import time
import unittest

from ucloud_sandboxes.storage_native import StorageNativeLayer
from ucloud_sandboxes.storage_native_registry import PublishedStorageLayer
from ucloud_sandboxes.storage_native_s3 import (
    HETZNER_TRANSIENT_RETRY_ATTEMPTS,
    S3ObjectStat,
    S3SnapshotPublisher,
    _retry_transient_hetzner_gateway,
    _strip_expect_header,
)


class FakeS3:
    def __init__(
        self,
        *,
        upload_delay: float = 0.0,
        stat_delay: float = 0.0,
        lose_complete_response: bool = False,
    ) -> None:
        self.objects: dict[str, bytes] = {}
        self.uploads: dict[tuple[str, str], dict[int, bytes]] = {}
        self.aborted: list[tuple[str, str]] = []
        self.next_upload = 0
        self.modified_at: dict[str, float] = {}
        self.upload_delay = upload_delay
        self.stat_delay = stat_delay
        self.lose_complete_response = lose_complete_response
        self.active_uploads = 0
        self.max_active_uploads = 0
        self.upload_lock = threading.Lock()
        self.active_stats = 0
        self.max_active_stats = 0

    def create_multipart_upload(self, key: str) -> str:
        self.next_upload += 1
        upload_id = str(self.next_upload)
        self.uploads[(key, upload_id)] = {}
        return upload_id

    def upload_part(
        self, key: str, upload_id: str, part_number: int, payload: bytes
    ) -> str:
        with self.upload_lock:
            self.active_uploads += 1
            self.max_active_uploads = max(
                self.max_active_uploads, self.active_uploads
            )
        try:
            if self.upload_delay:
                time.sleep(self.upload_delay)
            self.uploads[(key, upload_id)][part_number] = payload
            return f"etag-{part_number}"
        finally:
            with self.upload_lock:
                self.active_uploads -= 1

    def complete_multipart_upload(self, key, upload_id, parts) -> None:
        upload = self.uploads.pop((key, upload_id))
        self.objects[key] = b"".join(upload[number] for number, _etag in parts)
        if self.lose_complete_response:
            raise TimeoutError("injected lost completion response")

    def abort_multipart_upload(self, key: str, upload_id: str) -> None:
        self.uploads.pop((key, upload_id), None)
        self.aborted.append((key, upload_id))

    def stat(self, key: str) -> S3ObjectStat | None:
        with self.upload_lock:
            self.active_stats += 1
            self.max_active_stats = max(self.max_active_stats, self.active_stats)
        try:
            if self.stat_delay:
                time.sleep(self.stat_delay)
            payload = self.objects.get(key)
            return (
                None
                if payload is None
                else S3ObjectStat(
                    size=len(payload), modified_at=self.modified_at.get(key, 0.0)
                )
            )
        finally:
            with self.upload_lock:
                self.active_stats -= 1

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


class FakeExporter:
    def __init__(self, payloads: dict[Path, bytes], compacted: bytes = b"compact"):
        self.payloads = payloads
        self.compacted = compacted
        self.compaction_configs: list[dict] = []

    def export_dense_layer(self, *, source_layer_path, stream_socket_path):
        payload = self.payloads[source_layer_path]
        return self._send(payload, stream_socket_path)

    def export_compacted_image(
        self, *, source_image_config, global_config, stream_socket_path
    ):
        self.compaction_configs.append(
            json.loads(source_image_config.read_text(encoding="ascii"))
        )
        return self._send(self.compacted, stream_socket_path)

    @staticmethod
    def _send(payload: bytes, stream_socket_path: Path) -> StorageNativeLayer:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
            connection.connect(str(stream_socket_path))
            connection.sendall(payload)
        return StorageNativeLayer(digest=digest(payload), size=len(payload))


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
                s3.objects[f"ucloud/test/managed-layers/{publication.layers[0].digest}"],
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
            exporter = FakeExporter({source: b"delta"}, compacted=b"flattened")
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
                exporter.compaction_configs[0]["repoBlobUrl"],
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

    def test_uploads_bounded_multipart_parts_concurrently(self):
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            source = (root / "delta.commit").resolve()
            payload = b"x" * (15 * 1024 * 1024)
            source.write_bytes(payload)
            s3 = FakeS3(upload_delay=0.05)
            publisher = self._publisher(root, s3)

            publication = publisher.publish(
                exporter=FakeExporter({source: payload}),
                source_layer_paths=(source,),
                virtual_size=1 << 30,
            )

            self.assertGreaterEqual(s3.max_active_uploads, 2)
            self.assertEqual(publication.layers[0].size, len(payload))
            self.assertEqual(
                publisher.metrics()["snapshot_upload_part_concurrency"], 4
            )

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
            del s3.objects[
                f"ucloud/test/managed-layers/{publication.layers[0].digest}"
            ]

            with self.assertRaisesRegex(ValueError, "missing"):
                publisher.verify(publication)

    def test_verify_checks_layer_objects_concurrently(self):
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            sources = tuple(
                (root / f"delta-{index}.commit").resolve() for index in range(3)
            )
            payloads = {
                source: f"delta-{index}".encode()
                for index, source in enumerate(sources)
            }
            for source, payload in payloads.items():
                source.write_bytes(payload)
            s3 = FakeS3(stat_delay=0.05)
            publisher = self._publisher(root, s3)
            publication = publisher.publish(
                exporter=FakeExporter(payloads),
                source_layer_paths=sources,
                virtual_size=4096,
            )
            s3.max_active_stats = 0

            self.assertEqual(publisher.verify(publication), publication)

            self.assertGreaterEqual(s3.max_active_stats, 2)

    def test_hetzner_compatibility_retry_is_narrow_and_bounded(self):
        class Response:
            status_code = 403

        response = (Response(), {"Error": {"Code": "AccessDenied"}})
        self.assertEqual(
            _retry_transient_hetzner_gateway(response=response, attempts=1),
            0.1,
        )
        self.assertIsNone(
            _retry_transient_hetzner_gateway(
                response=response,
                attempts=HETZNER_TRANSIENT_RETRY_ATTEMPTS,
            )
        )
        Response.status_code = 404
        self.assertIsNone(
            _retry_transient_hetzner_gateway(
                response=(Response(), {"Error": {"Code": "NoSuchKey"}}),
                attempts=1,
            )
        )

    def test_hetzner_compatibility_removes_expect_header_only(self):
        class Request:
            headers = {"Expect": "100-continue", "X-Amz-Date": "value"}

        request = Request()
        _strip_expect_header(request)
        self.assertEqual(request.headers, {"X-Amz-Date": "value"})


def digest(payload: bytes) -> str:
    return f"sha256:{hashlib.sha256(payload).hexdigest()}"


if __name__ == "__main__":
    unittest.main()
