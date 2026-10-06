from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Event, Semaphore, Thread
import time
import unittest
from unittest.mock import patch

from tests import test_storage_native_daemon as fixtures
from ucloud_sandboxes.storage_native_daemon import (
    StorageNativeConflictError, StorageNativeNodeClient, StorageNativeNodeServer, StorageVolumeOwner, StorageVolumeState,
)

TEST_TIER = "contract"


class StorageAdmissionIsolationTests(unittest.TestCase):
    def test_blocked_publication_does_not_block_metadata_or_local_restore(self):
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            service, *_ = fixtures.StorageNativeNodeServiceTests()._service(root, publisher=True)
            service.config = replace(service.config, max_concurrent_operations=1)
            server = StorageNativeNodeServer(root / 'socket' / 'storage.sock', service, require_root_peer=False)
            thread = Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
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
                    pending = pool.submit(client.ensure_published, StorageVolumeOwner('publishing', 'publishing', 1), operation_id='publish:one', expected_revision=client.get_volume('publishing').revision)
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
                        resumed = client.ensure_mounted(StorageVolumeOwner('publishing', 'publishing', 1), operation_id='wake:same')
                        self.assertEqual(resumed.state, StorageVolumeState.MOUNTED)
                    finally:
                        release.set()
                    with self.assertRaises(StorageNativeConflictError):
                        pending.result(timeout=2)
                    self.assertEqual(client.get_volume('publishing'), resumed)
            finally:
                release.set()
                server.shutdown()
                thread.join(2)


class CreateAdmissionClassTests(unittest.TestCase):
    """New-workspace prepares and lifecycle work have separate bounds."""

    def _serve(self, root, **config):
        service, backend, host = fixtures.StorageNativeNodeServiceTests()._service(root, capacity=64 << 30)
        service.config = replace(service.config, **config)
        server = StorageNativeNodeServer(root / 'socket' / 'storage.sock', service, require_root_peer=False)
        thread = Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
        thread.start()
        self.addCleanup(thread.join, 2)
        self.addCleanup(server.shutdown)
        client = StorageNativeNodeClient(server.socket_path, timeout_seconds=10)
        client.wait_ready(timeout_seconds=2)
        return service, backend, host, server, client

    @staticmethod
    def _until(predicate, timeout=5.0):
        deadline = time.monotonic() + timeout
        while not predicate():
            if time.monotonic() > deadline:
                raise AssertionError('condition not reached')
            time.sleep(0.005)

    def test_creates_do_not_wait_behind_eight_long_parks(self):
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            service, backend, _host, _server, client = self._serve(root, max_concurrent_operations=8)
            owners = [StorageVolumeOwner(f'park-{i}', f'park-{i}', 1) for i in range(8)]
            for owner in owners:
                client.prepare_volume(owner, operation_id='create:' + owner.volume_id, virtual_size=1 << 30)
            entered, release = Semaphore(0), Event()
            original = backend.restack_snapshot
            def slow_restack(*args, **kwargs):
                entered.release()
                if not release.wait(10):
                    raise TimeoutError('test did not release parks')
                return original(*args, **kwargs)
            with patch.object(backend, 'restack_snapshot', side_effect=slow_restack), ThreadPoolExecutor(max_workers=10) as pool:
                try:
                    parks = [pool.submit(client.ensure_released, owner, operation_id='park:' + owner.volume_id) for owner in owners]
                    for _ in owners:
                        self.assertTrue(entered.acquire(timeout=5))
                    self.assertEqual(client.get_metrics()['active_operations'], 8)
                    fresh = StorageVolumeOwner('fresh', 'fresh', 1)
                    created = pool.submit(client.prepare_volume, fresh, operation_id='create:fresh', virtual_size=1 << 30).result(timeout=5)
                    self.assertEqual(created.state, StorageVolumeState.MOUNTED)
                    self.assertFalse(any(park.done() for park in parks))
                    # The split is real: lifecycle work still queues behind them.
                    queued = pool.submit(client.ensure_released, fresh, operation_id='park:fresh')
                    self._until(lambda: client.get_metrics()['waiting_operations'] == 1)
                    time.sleep(0.05)
                    self.assertFalse(queued.done())
                finally:
                    release.set()
                for park in [*parks, queued]:
                    self.assertEqual(park.result(timeout=10).state, StorageVolumeState.RELEASED)
            metrics = client.get_metrics()
            self.assertEqual((metrics['active_operations'], metrics['waiting_operations']), (0, 0))
            self.assertEqual((metrics['active_prepares'], metrics['waiting_prepares']), (0, 0))
            self.assertEqual(metrics['max_concurrent_operations'], 8)
            self.assertEqual(metrics['prepare_admissions'], 9)
            self.assertEqual(metrics['lifecycle_admissions'], 9)
            self.assertGreaterEqual(metrics['lifecycle_queue_wait_ms_max'], 40)
            self.assertGreaterEqual(metrics['lifecycle_queue_wait_ms_total'], metrics['lifecycle_queue_wait_ms_max'])

    def test_wakes_do_not_wait_behind_a_create_burst(self):
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            service, _backend, host, _server, client = self._serve(root, max_concurrent_prepares=2)
            sleeper = StorageVolumeOwner('sleeper', 'sleeper', 1)
            client.prepare_volume(sleeper, operation_id='create:sleeper', virtual_size=1 << 30)
            client.ensure_released(sleeper, operation_id='park:sleeper')
            entered, release = Semaphore(0), Event()
            original = host.format_xfs
            def slow_format(*args, **kwargs):
                entered.release()
                if not release.wait(10):
                    raise TimeoutError('test did not release creates')
                return original(*args, **kwargs)
            burst = [StorageVolumeOwner(f'burst-{i}', f'burst-{i}', 1) for i in range(4)]
            with patch.object(host, 'format_xfs', side_effect=slow_format), ThreadPoolExecutor(max_workers=6) as pool:
                try:
                    creates = [pool.submit(client.prepare_volume, owner, operation_id='create:' + owner.volume_id, virtual_size=1 << 30) for owner in burst]
                    for _ in range(2):
                        self.assertTrue(entered.acquire(timeout=5))
                    self._until(lambda: client.get_metrics()['waiting_prepares'] == 2)
                    metrics = client.get_metrics()
                    self.assertEqual((metrics['active_prepares'], metrics['max_concurrent_prepares']), (2, 2))
                    self.assertEqual(metrics['active_operations'], 0)
                    # A wake through PrepareVolume of the parked volume is
                    # lifecycle work, not a create, and does not queue.
                    woken = pool.submit(client.prepare_volume, sleeper, operation_id='wake:sleeper', virtual_size=1 << 30).result(timeout=5)
                    self.assertEqual(woken.state, StorageVolumeState.MOUNTED)
                    self.assertFalse(any(create.done() for create in creates))
                    time.sleep(0.05)
                finally:
                    release.set()
                for create in creates:
                    self.assertEqual(create.result(timeout=10).state, StorageVolumeState.MOUNTED)
            metrics = client.get_metrics()
            self.assertEqual(metrics['prepare_admissions'], 5)
            self.assertEqual(metrics['lifecycle_admissions'], 2)
            self.assertGreaterEqual(metrics['prepare_queue_wait_ms_max'], 40)
            self.assertEqual((metrics['active_prepares'], metrics['waiting_prepares']), (0, 0))

    def test_only_a_prepare_of_a_missing_volume_is_a_create(self):
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            _service, _backend, _host, server, client = self._serve(root, max_concurrent_operations=1)
            existing = StorageVolumeOwner('existing', 'existing', 1)
            client.prepare_volume(existing, operation_id='create:existing', virtual_size=1 << 30)
            with ThreadPoolExecutor(max_workers=1) as pool:
                with server._server.operation_slots:
                    # An idempotent retry of a mounted volume is lifecycle work.
                    retry = pool.submit(client.prepare_volume, existing, operation_id='create:existing', virtual_size=1 << 30)
                    self._until(lambda: client.get_metrics()['waiting_operations'] == 1)
                    other = StorageVolumeOwner('other', 'other', 1)
                    self.assertEqual(client.prepare_volume(other, operation_id='create:other', virtual_size=1 << 30).state, StorageVolumeState.MOUNTED)
                    self.assertFalse(retry.done())
                self.assertEqual(retry.result(timeout=5).state, StorageVolumeState.MOUNTED)


class PrepareBoundFlagTests(unittest.TestCase):
    def test_daemon_flag_defaults_to_the_production_create_target(self):
        from ucloud_sandboxes.storage_native_daemon import DEFAULT_MAX_CONCURRENT_PREPARES, StorageNativeNodeConfig
        from ucloud_sandboxes.storage_native_service import parse_args
        required = [
            '--socket', '/run/s.sock', '--deployment-id', 'd', '--node-id', 'n',
            '--backend-socket', '/run/b.sock', '--backend-global-config', '/etc/g.json',
            '--journal', '/var/j.sqlite', '--runtime-root', '/var/r', '--mount-root', '/var/m',
            '--hard-capacity-bytes', '1',
        ]
        self.assertEqual(DEFAULT_MAX_CONCURRENT_PREPARES, 32)
        self.assertEqual(parse_args(required).max_concurrent_prepares, 32)
        self.assertEqual(parse_args([*required, '--max-concurrent-prepares', '48']).max_concurrent_prepares, 48)
        paths = dict(journal_path=Path('/j'), runtime_root=Path('/r'), mount_root=Path('/m'), hard_capacity_bytes=1)
        self.assertEqual(StorageNativeNodeConfig(**paths).max_concurrent_prepares, 32)
        with self.assertRaisesRegex(ValueError, 'max_concurrent_prepares'):
            StorageNativeNodeConfig(**paths, max_concurrent_prepares=0)
