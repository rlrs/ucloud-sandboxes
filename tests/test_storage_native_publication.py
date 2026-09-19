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
