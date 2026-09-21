from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from tempfile import TemporaryDirectory
import threading
import unittest
from unittest.mock import patch

from tests.test_storage_native_registry import FakeRegistry
from tests.test_storage_native_s3 import FakeS3
from ucloud_sandboxes.storage_native_publication import PublicationGate
from ucloud_sandboxes.storage_native_registry import RegistrySnapshotPublisher
from ucloud_sandboxes.storage_native_s3 import S3SnapshotPublisher
from ucloud_sandboxes.telemetry import Telemetry


class PublicationCancellationTests(unittest.TestCase):
    def test_stream_chunks_remain_immutable_across_buffer_reuse_and_partial_tail(self):
        import hashlib
        import socket
        from ucloud_sandboxes.storage_native import StorageNativeLayer
        from ucloud_sandboxes.storage_native_registry import consume_export_stream

        payload = bytes(range(251)) * 401
        chunk_size = 8192
        expected = StorageNativeLayer('sha256:' + hashlib.sha256(payload).hexdigest(), len(payload))
        retained = []

        def export(path):
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
                sock.connect(str(path))
                for offset in range(0, len(payload), 997):
                    sock.sendall(payload[offset:offset + 997])
            return expected

        with TemporaryDirectory() as raw:
            observed = consume_export_stream(
                export, stream_socket_root=Path(raw), chunk_bytes=chunk_size,
                timeout_seconds=2, consume=retained.append,
            )
        self.assertEqual(observed, expected)
        self.assertEqual(retained, [payload[offset:offset + chunk_size]
                                   for offset in range(0, len(payload), chunk_size)])
        self.assertTrue(all(isinstance(chunk, bytes) for chunk in retained))

    def test_superseded_queued_upload_exits_without_waiting_for_active_upload(self):
        with TemporaryDirectory() as raw:
            root = Path(raw)
            publishers = (
                RegistrySnapshotPublisher(registry=FakeRegistry(), repository="snapshots", stream_socket_root=root),
                S3SnapshotPublisher(endpoint="https://s3.example", bucket="test", region="test", prefix="test", credential_process="/bin/false", stream_socket_root=root, client_factory=FakeS3),
            )
            for publisher in publishers:
                with self.subTest(backend=type(publisher).__name__):
                    gate = PublicationGate(1)
                    publisher._publication_gate = gate
                    waiting, canceled = threading.Event(), threading.Event()

                    def check():
                        waiting.set()
                        if canceled.is_set():
                            raise RuntimeError("superseded")

                    with patch.object(publisher, "_publish_locked") as export, gate.acquire(Telemetry.disabled("test")), ThreadPoolExecutor(max_workers=1) as pool:
                        future = pool.submit(publisher.publish, exporter=object(), source_layer_paths=(), virtual_size=1, check_current=check)
                        self.assertTrue(waiting.wait(2))
                        canceled.set()
                        with self.assertRaisesRegex(RuntimeError, "superseded"):
                            future.result(timeout=2)
                        export.assert_not_called()
                        self.assertEqual(gate.metrics()["snapshot_publication_waiting"], 0)
                        self.assertEqual(gate.metrics()["snapshot_publication_active"], 1)
                    with gate.acquire(Telemetry.disabled("test")):
                        self.assertEqual(gate.metrics()["snapshot_publication_active"], 1)
                    self.assertEqual(gate.metrics()["snapshot_publication_active"], 0)

    def test_superseded_active_export_stops_upload_and_cleans_partial_objects(self):
        import hashlib
        import socket
        from ucloud_sandboxes.storage_native import StorageNativeLayer
        from ucloud_sandboxes.storage_native_registry import SnapshotPublisherRouter

        payload = b'x' * (20 * 1024 * 1024)
        for backend in ('registry', 's3'):
            for compact in (False, True):
                with self.subTest(backend=backend, compact=compact), TemporaryDirectory() as raw:
                    root = Path(raw)
                    source = root / 'source'
                    source.write_bytes(b'immutable source')
                    other = root / 'other'
                    other.write_bytes(b'delta')
                    config = root / 'global.json'
                    config.write_text('{}')
                    canceled = threading.Event()
                    ended = threading.Event()
                    uploaded = []

                    class Exporter:
                        def send(self, path):
                            try:
                                with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
                                    sock.connect(str(path))
                                    for offset in range(0, len(payload), 65536):
                                        sock.sendall(payload[offset:offset + 65536])
                                return StorageNativeLayer('sha256:' + hashlib.sha256(payload).hexdigest(), len(payload))
                            finally:
                                ended.set()

                        def export_dense_layer(self, *, source_layer_path, stream_socket_path):
                            return self.send(stream_socket_path)

                        def export_compacted_image(self, *, source_image_config, global_config, stream_socket_path):
                            return self.send(stream_socket_path)

                    def check():
                        if canceled.is_set():
                            raise RuntimeError('superseded during export')

                    options = dict(stream_socket_root=root, upload_chunk_bytes=5 * 1024 * 1024,
                                   compact_after_layers=1 if compact else 8, stream_timeout_seconds=2)
                    if backend == 'registry':
                        client = FakeRegistry()
                        original = client.upload_blob_chunk

                        def upload(location, chunk):
                            result = original(location, chunk)
                            uploaded.append(len(chunk))
                            canceled.set()
                            return result

                        client.upload_blob_chunk = upload
                        publisher = RegistrySnapshotPublisher(client, repository='snapshots', **options)
                    else:
                        client = FakeS3()
                        original = client.upload_part

                        def upload(key, upload_id, number, chunk):
                            result = original(key, upload_id, number, chunk)
                            uploaded.append(len(chunk))
                            canceled.set()
                            return result

                        client.upload_part = upload
                        publisher = S3SnapshotPublisher(
                            endpoint='https://s3.example', bucket='test', region='test', prefix='test',
                            credential_process='/bin/false', client_factory=lambda: client,
                            upload_part_concurrency=1, **options,
                        )
                    # Exercise the same authority forwarding used with mixed backends.
                    router = SnapshotPublisherRouter(publisher, verifiers={backend: publisher})
                    with self.assertRaisesRegex(RuntimeError, 'superseded during export'):
                        router.publish(exporter=Exporter(), source_layer_paths=(source, other) if compact else (source,),
                                       virtual_size=32 * 1024**2, global_config_path=config, check_current=check)
                    self.assertTrue(ended.wait(2))
                    self.assertLessEqual(sum(uploaded), 2 * options['upload_chunk_bytes'])
                    self.assertEqual(client.uploads, {})
                    self.assertEqual(client.manifests if backend == 'registry' else client.objects, {})
                    self.assertEqual(publisher.metrics()['snapshot_publications'], 0)
                    self.assertEqual(publisher.metrics()['snapshot_publication_active'], 0)
                    self.assertEqual(source.read_bytes(), b'immutable source')
                    with publisher._publication_gate.acquire(Telemetry.disabled('test')):
                        pass

    def test_superseded_stream_without_output_closes_exporter_connection(self):
        import socket
        from ucloud_sandboxes.storage_native_registry import consume_export_stream

        connected, ended = threading.Event(), threading.Event()

        def export(path):
            try:
                with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
                    sock.connect(str(path))
                    connected.set()
                    # No output: cancellation must be observed during recv polling.
                    try:
                        self.assertEqual(sock.recv(1), b'')
                    except ConnectionResetError:
                        pass
            finally:
                ended.set()

        def check():
            if connected.is_set():
                raise RuntimeError('superseded stalled stream')

        with TemporaryDirectory() as raw, ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(consume_export_stream, export, stream_socket_root=Path(raw),
                                 chunk_bytes=1024, timeout_seconds=5, consume=lambda chunk: None,
                                 check_current=check)
            with self.assertRaisesRegex(RuntimeError, 'superseded stalled stream'):
                future.result(timeout=2)
            self.assertTrue(ended.wait(1))
