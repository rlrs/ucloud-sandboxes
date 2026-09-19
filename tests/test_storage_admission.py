from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Event, Thread
import unittest
from unittest.mock import patch

from tests import test_storage_native_daemon as fixtures
from ucloud_sandboxes.storage_native_daemon import (
    StorageNativeNodeClient, StorageNativeNodeServer, StorageVolumeOwner, StorageVolumeState,
)


class StorageAdmissionIsolationTests(unittest.TestCase):
    def test_blocked_publication_does_not_block_metadata_or_local_restore(self):
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            service, *_ = fixtures.StorageNativeNodeServiceTests()._service(root, publisher=True)
            service.config = replace(service.config, max_concurrent_operations=1)
            server = StorageNativeNodeServer(root / 'socket' / 'storage.sock', service, require_root_peer=False)
            thread = Thread(target=server.serve_forever, daemon=True)
            thread.start()
            client = StorageNativeNodeClient(server.socket_path, timeout_seconds=2)
            entered, release = Event(), Event()
            try:
                client.wait_ready(timeout_seconds=2)
                for name in ('publishing', 'restoring'):
                    owner = StorageVolumeOwner(name, name, 1)
                    client.prepare_volume(owner, operation_id='create:'+name, virtual_size=1 << 30)
                    client.ensure_released(owner, operation_id='park:'+name)
                original = service.publisher.publish
                def slow_publish(**kwargs):
                    entered.set()
                    if not release.wait(5):
                        raise TimeoutError('test did not release publication')
                    return original(**kwargs)
                with patch.object(service.publisher, 'publish', side_effect=slow_publish), ThreadPoolExecutor(max_workers=1) as pool:
                    pending = pool.submit(client.ensure_published, StorageVolumeOwner('publishing', 'publishing', 1), operation_id='publish:one')
                    try:
                        self.assertTrue(entered.wait(2))
                        self.assertEqual(client.get_metrics()['active_operations'], 0)
                        self.assertEqual(len(client.list_volumes()), 2)
                        self.assertEqual(client.get_volume('publishing').state, StorageVolumeState.PUBLISHING)
                        # Read-only inventory also bypasses a genuinely occupied
                        # local-operation slot, not only the publication lane.
                        with server._server.operation_slots:
                            self.assertEqual(len(client.list_volumes()), 2)
                            self.assertEqual(client.get_volume('restoring').state, StorageVolumeState.RELEASED)
                        restored = client.ensure_mounted(StorageVolumeOwner('restoring', 'restoring', 1), operation_id='wake:other')
                        self.assertEqual(restored.state, StorageVolumeState.MOUNTED)
                        self.assertFalse(pending.done())
                    finally:
                        release.set()
                    self.assertEqual(pending.result(timeout=2).state, StorageVolumeState.PUBLISHED)
            finally:
                release.set()
                server.shutdown()
                thread.join(2)
