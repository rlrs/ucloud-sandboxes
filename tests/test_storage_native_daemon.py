from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import threading
import time
import unittest

from ucloud_sandboxes.hibernation import (
    HibernationDiskReservation,
    hibernation_memory_backing_reservation_mb,
)
from ucloud_sandboxes.storage_native import StorageNativeDevice
from ucloud_sandboxes.storage_native_daemon import (
    StorageNativeConflictError,
    StorageNativeNodeConfig,
    StorageNativeNodeClient,
    StorageNativeNodeServer,
    StorageNativeNodeService,
    StorageVolumeRecord,
    StorageVolumeState,
)
from ucloud_sandboxes.storage_native_quota import StorageNativeQuotaBackend
from ucloud_sandboxes.storage_native_registry import (
    PublishedStorageLayer,
    StorageSnapshotPublication,
)


@dataclass(frozen=True)
class FakeLayer:
    digest: str
    size: int
    uuid: str = ""


class FakeBlockBackend:
    def __init__(self, *, descriptor: bool = False) -> None:
        self.descriptor = descriptor
        self.live: set[int] = set()
        self.create_calls = 0
        self.restack_calls = 0
        self.delete_calls: list[int] = []

    def create_runtime_device(
        self,
        *,
        source_image_config: Path,
        global_config: Path,
        runtime_dir: Path,
        virtual_size: int,
        upper_mode: str,
    ) -> StorageNativeDevice:
        self.create_calls += 1
        device_id = self.create_calls
        self.live.add(device_id)
        runtime_dir.mkdir(mode=0o700)
        image = runtime_dir / "image.json"
        image.write_text("{}\n", encoding="ascii")
        return StorageNativeDevice(
            device_id=device_id,
            device_path=Path(f"/dev/ublkb{device_id}"),
            virtual_size=virtual_size,
            image_config_path=image,
        )

    def restack_snapshot(self, device_id: int, output_layer_path: Path):
        if device_id not in self.live:
            raise RuntimeError("device is missing")
        self.restack_calls += 1
        output_layer_path.write_bytes(b"sealed-delta")
        if not self.descriptor:
            return None
        return FakeLayer(
            digest="sha256:" + "a" * 64,
            size=len(b"sealed-delta"),
        )

    def delete(self, device_id: int) -> None:
        self.delete_calls.append(device_id)
        self.live.discard(device_id)


class FakePublisher:
    def verify(
        self,
        publication: StorageSnapshotPublication,
    ) -> StorageSnapshotPublication:
        return publication

    def publish(
        self,
        *,
        exporter: object,
        source_layer_paths: tuple[Path, ...],
        virtual_size: int,
        existing_layers: tuple[PublishedStorageLayer, ...] = (),
    ) -> StorageSnapshotPublication:
        new_layers = tuple(
            PublishedStorageLayer(
                digest="sha256:" + f"{index + 1:064x}",
                size=path.stat().st_size,
            )
            for index, path in enumerate(
                source_layer_paths,
                start=len(existing_layers),
            )
        )
        layers = (*existing_layers, *new_layers)
        return StorageSnapshotPublication(
            manifest_digest="sha256:" + "f" * 64,
            tag="ucloud-storage-v1-test",
            repository="snapshots",
            repo_blob_url="http://registry/v2/snapshots/blobs",
            virtual_size=virtual_size,
            layers=layers,
        )


class FakeHost:
    def __init__(self, backend: FakeBlockBackend) -> None:
        self.backend = backend
        self.mounted: set[Path] = set()
        self.frozen: set[Path] = set()
        self.formatted: list[Path] = []
        self.fail_next_unmount = False
        self.detached: list[Path] = []

    def format_xfs(self, device: Path) -> None:
        self.formatted.append(device)

    def mount(self, device: Path, target: Path) -> None:
        self.mounted.add(target)

    def sync(self, target: Path) -> None:
        if target not in self.mounted:
            raise RuntimeError("not mounted")

    def freeze(self, target: Path) -> None:
        self.frozen.add(target)

    def unfreeze(self, target: Path) -> None:
        self.frozen.remove(target)

    def unmount(self, target: Path) -> None:
        if self.fail_next_unmount:
            self.fail_next_unmount = False
            raise OSError("injected unreachable mount")
        self.mounted.remove(target)

    def detach(self, target: Path) -> None:
        self.detached.append(target)
        self.mounted.discard(target)

    def is_mounted(self, target: Path) -> bool:
        return target in self.mounted

    def ublk_device_ids(self) -> set[int]:
        return set(self.backend.live)


class StorageNativeNodeServiceTests(unittest.TestCase):
    def _service(
        self,
        root: Path,
        *,
        capacity: int = 8 << 30,
        descriptor: bool = False,
        publisher: bool = False,
    ) -> tuple[StorageNativeNodeService, FakeBlockBackend, FakeHost]:
        backend = FakeBlockBackend(descriptor=descriptor)
        host = FakeHost(backend)
        global_config = root / "global.json"
        global_config.write_text("{}\n", encoding="ascii")
        service = StorageNativeNodeService(
            StorageNativeNodeConfig(
                journal_path=root / "journal" / "storage.sqlite",
                runtime_root=root / "runtime",
                mount_root=root / "mounts",
                hard_capacity_bytes=capacity,
            ),
            backend=backend,
            global_config_path=global_config,
            host=host,
            publisher=FakePublisher() if publisher else None,
        )
        return service, backend, host

    def test_create_seal_release_is_fenced_and_idempotent(self) -> None:
        with TemporaryDirectory() as raw:
            service, backend, host = self._service(Path(raw), descriptor=True)
            create = service.create_volume(
                sandbox_id="sandbox-1",
                sandbox_generation=3,
                volume_id="volume-1",
                operation_id="create:1",
                virtual_size=1 << 30,
            )
            self.assertEqual(create["record"]["revision"], 1)
            self.assertEqual(create["record"]["state"], "mounted")
            replay = service.create_volume(
                sandbox_id="sandbox-1",
                sandbox_generation=3,
                volume_id="volume-1",
                operation_id="create:1",
                virtual_size=1 << 30,
            )
            self.assertEqual(replay, create)
            self.assertEqual(backend.create_calls, 1)

            with self.assertRaisesRegex(
                StorageNativeConflictError,
                "stale storage revision",
            ):
                service.freeze_and_seal(
                    sandbox_id="sandbox-1",
                    sandbox_generation=3,
                    volume_id="volume-1",
                    operation_id="seal:stale",
                    expected_revision=0,
                )

            sealed = service.freeze_and_seal(
                sandbox_id="sandbox-1",
                sandbox_generation=3,
                volume_id="volume-1",
                operation_id="seal:1",
                expected_revision=1,
            )
            self.assertEqual(sealed["record"]["revision"], 2)
            self.assertEqual(sealed["record"]["state"], "sealed")
            self.assertEqual(sealed["layer"]["size"], len(b"sealed-delta"))
            self.assertFalse(host.frozen)
            self.assertEqual(
                service.freeze_and_seal(
                    sandbox_id="sandbox-1",
                    sandbox_generation=3,
                    volume_id="volume-1",
                    operation_id="seal:1",
                    expected_revision=1,
                ),
                sealed,
            )
            self.assertEqual(backend.restack_calls, 1)

            released = service.release_runtime(
                sandbox_id="sandbox-1",
                sandbox_generation=3,
                volume_id="volume-1",
                operation_id="release:1",
                expected_revision=2,
            )
            self.assertEqual(released["record"]["revision"], 3)
            self.assertEqual(released["record"]["state"], "released")
            self.assertEqual(backend.delete_calls, [1])
            self.assertFalse(host.mounted)

            resumed = service.mount_snapshot_cow(
                sandbox_id="sandbox-1",
                sandbox_generation=3,
                volume_id="volume-1",
                operation_id="acquire:1",
                expected_revision=3,
            )
            self.assertEqual(resumed["record"]["revision"], 4)
            self.assertEqual(resumed["record"]["state"], "mounted")
            self.assertEqual(backend.create_calls, 2)
            second_seal = service.freeze_and_seal(
                sandbox_id="sandbox-1",
                sandbox_generation=3,
                volume_id="volume-1",
                operation_id="seal:2",
                expected_revision=4,
            )
            self.assertEqual(
                len(second_seal["record"]["sealed_layer_paths"]),
                2,
            )
            second_release = service.release_runtime(
                sandbox_id="sandbox-1",
                sandbox_generation=3,
                volume_id="volume-1",
                operation_id="release:2",
                expected_revision=5,
            )
            self.assertEqual(backend.delete_calls, [1, 2])
            deleted = service.delete_volume(
                sandbox_id="sandbox-1",
                sandbox_generation=3,
                volume_id="volume-1",
                operation_id="delete:1",
                expected_revision=second_release["record"]["revision"],
            )
            self.assertEqual(deleted["record"]["state"], "deleted")
            self.assertFalse((Path(raw) / "runtime" / "volume-1").exists())

    def test_failed_local_wake_discards_cow_back_to_released_snapshot(self) -> None:
        with TemporaryDirectory() as raw:
            service, backend, _host = self._service(Path(raw))
            created = service.create_volume(
                sandbox_id="sandbox-1",
                sandbox_generation=3,
                volume_id="volume-1",
                operation_id="create:1",
                virtual_size=1 << 30,
            )
            sealed = service.freeze_and_seal(
                sandbox_id="sandbox-1",
                sandbox_generation=3,
                volume_id="volume-1",
                operation_id="seal:1",
                expected_revision=int(created["record"]["revision"]),
            )
            released = service.release_runtime(
                sandbox_id="sandbox-1",
                sandbox_generation=3,
                volume_id="volume-1",
                operation_id="release:1",
                expected_revision=int(sealed["record"]["revision"]),
            )
            mounted = service.mount_snapshot_cow(
                sandbox_id="sandbox-1",
                sandbox_generation=3,
                volume_id="volume-1",
                operation_id="wake:1",
                expected_revision=int(released["record"]["revision"]),
            )

            discarded = service.discard_mounted_cow(
                sandbox_id="sandbox-1",
                sandbox_generation=3,
                volume_id="volume-1",
                operation_id="wake:1:rollback",
                expected_revision=int(mounted["record"]["revision"]),
            )

            self.assertEqual(discarded["record"]["state"], "released")
            self.assertTrue(discarded["record"]["sealed_layer_paths"])
            self.assertEqual(backend.delete_calls, [1, 2])

    def test_destination_acquires_published_registration_without_capacity(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw)
            service, _, _ = self._service(
                root,
                capacity=1,
                publisher=True,
            )
            publication = StorageSnapshotPublication(
                manifest_digest="sha256:" + "f" * 64,
                tag="ucloud-storage-v1-test",
                repository="snapshots",
                repo_blob_url="http://registry/v2/snapshots/blobs",
                virtual_size=1 << 30,
                layers=(
                    PublishedStorageLayer(
                        digest="sha256:" + "1" * 64,
                        size=4096,
                    ),
                ),
            )
            acquired = service.acquire_snapshot(
                sandbox_id="sandbox-1",
                sandbox_generation=9,
                volume_id="volume-1",
                operation_id="import:1",
                publication_raw=publication.to_dict(),
            )
            self.assertEqual(acquired["record"]["state"], "published")
            self.assertEqual(acquired["record"]["virtual_size"], 1 << 30)
            with self.assertRaisesRegex(
                StorageNativeConflictError,
                "hard capacity",
            ):
                service.mount_snapshot_cow(
                    sandbox_id="sandbox-1",
                    sandbox_generation=9,
                    volume_id="volume-1",
                    operation_id="mount:1",
                    expected_revision=1,
                )

    def test_deleted_import_tombstone_allows_same_incarnation_retry(self) -> None:
        with TemporaryDirectory() as raw:
            service, _, _ = self._service(Path(raw), publisher=True)
            publication = StorageSnapshotPublication(
                manifest_digest="sha256:" + "f" * 64,
                tag="ucloud-storage-v1-test",
                repository="snapshots",
                repo_blob_url="http://registry/v2/snapshots/blobs",
                virtual_size=1 << 30,
                layers=(
                    PublishedStorageLayer(
                        digest="sha256:" + "1" * 64,
                        size=4096,
                    ),
                ),
            )
            first = service.acquire_snapshot(
                sandbox_id="sandbox-1",
                sandbox_generation=9,
                volume_id="volume-1",
                operation_id="import:1",
                publication_raw=publication.to_dict(),
            )
            service.delete_volume(
                sandbox_id="sandbox-1",
                sandbox_generation=9,
                volume_id="volume-1",
                operation_id="delete:1",
                expected_revision=int(first["record"]["revision"]),
            )

            retried = service.acquire_snapshot(
                sandbox_id="sandbox-1",
                sandbox_generation=9,
                volume_id="volume-1",
                operation_id="import:2",
                publication_raw=publication.to_dict(),
            )

            self.assertEqual(retried["record"]["state"], "published")
            self.assertEqual(retried["record"]["revision"], 3)
            mounted = service.mount_snapshot_cow(
                sandbox_id="sandbox-1",
                sandbox_generation=9,
                volume_id="volume-1",
                operation_id="mount:2",
                expected_revision=3,
            )
            self.assertEqual(mounted["record"]["state"], "mounted")
            discarded = service.discard_mounted_cow(
                sandbox_id="sandbox-1",
                sandbox_generation=9,
                volume_id="volume-1",
                operation_id="discard:2",
                expected_revision=int(mounted["record"]["revision"]),
            )
            self.assertEqual(discarded["record"]["state"], "published")
            self.assertEqual(
                discarded["record"]["published_manifest_digest"],
                publication.manifest_digest,
            )

    def test_hard_capacity_is_not_overallocated_and_is_reclaimed(self) -> None:
        with TemporaryDirectory() as raw:
            service, _, _ = self._service(
                Path(raw),
                capacity=1 << 30,
                publisher=True,
            )
            service.create_volume(
                sandbox_id="sandbox-1",
                sandbox_generation=1,
                volume_id="volume-1",
                operation_id="create:1",
                virtual_size=1 << 30,
            )
            with self.assertRaisesRegex(
                StorageNativeConflictError,
                "hard capacity",
            ):
                service.create_volume(
                    sandbox_id="sandbox-2",
                    sandbox_generation=1,
                    volume_id="volume-2",
                    operation_id="create:2",
                    virtual_size=1,
                )
            service.freeze_and_seal(
                sandbox_id="sandbox-1",
                sandbox_generation=1,
                volume_id="volume-1",
                operation_id="seal:1",
                expected_revision=1,
            )
            service.release_runtime(
                sandbox_id="sandbox-1",
                sandbox_generation=1,
                volume_id="volume-1",
                operation_id="release:1",
                expected_revision=2,
            )
            with self.assertRaisesRegex(
                StorageNativeConflictError,
                "hard capacity",
            ):
                service.create_volume(
                    sandbox_id="sandbox-2",
                    sandbox_generation=1,
                    volume_id="volume-blocked",
                    operation_id="create:blocked",
                    virtual_size=1,
                )
            service.publish_snapshot(
                sandbox_id="sandbox-1",
                sandbox_generation=1,
                volume_id="volume-1",
                operation_id="publish:1",
                expected_revision=3,
            )
            second = service.create_volume(
                sandbox_id="sandbox-2",
                sandbox_generation=1,
                volume_id="volume-2",
                operation_id="create:3",
                virtual_size=1 << 30,
            )
            self.assertEqual(second["record"]["state"], "mounted")

    def test_publish_releases_capacity_and_remote_layers_resume(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw)
            service, backend, _ = self._service(
                root,
                capacity=1 << 30,
                publisher=True,
            )
            service.create_volume(
                sandbox_id="sandbox-1",
                sandbox_generation=1,
                volume_id="volume-1",
                operation_id="create:1",
                virtual_size=1 << 30,
            )
            service.freeze_and_seal(
                sandbox_id="sandbox-1",
                sandbox_generation=1,
                volume_id="volume-1",
                operation_id="seal:1",
                expected_revision=1,
            )
            released = service.release_runtime(
                sandbox_id="sandbox-1",
                sandbox_generation=1,
                volume_id="volume-1",
                operation_id="release:1",
                expected_revision=2,
            )
            local_layer = Path(released["record"]["sealed_layer_paths"][0])
            self.assertTrue(local_layer.exists())

            published = service.publish_snapshot(
                sandbox_id="sandbox-1",
                sandbox_generation=1,
                volume_id="volume-1",
                operation_id="publish:1",
                expected_revision=3,
            )
            self.assertEqual(published["record"]["state"], "published")
            self.assertEqual(len(published["record"]["published_layers"]), 1)
            self.assertFalse(local_layer.exists())

            mounted = service.mount_snapshot_cow(
                sandbox_id="sandbox-1",
                sandbox_generation=1,
                volume_id="volume-1",
                operation_id="mount:remote",
                expected_revision=4,
            )
            self.assertEqual(mounted["record"]["state"], "mounted")
            source = json.loads(
                Path(mounted["record"]["source_image_config"]).read_text(
                    encoding="ascii"
                )
            )
            self.assertEqual(
                source["repoBlobUrl"],
                "http://registry/v2/snapshots/blobs",
            )
            self.assertEqual(len(source["lowers"]), 1)
            self.assertEqual(backend.create_calls, 2)

    def test_reconcile_deletes_orphans_and_terminally_fences_missing_device(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw)
            service, backend, _ = self._service(root)
            service.create_volume(
                sandbox_id="sandbox-1",
                sandbox_generation=1,
                volume_id="volume-1",
                operation_id="create:1",
                virtual_size=1 << 30,
            )
            backend.live.remove(1)
            backend.live.add(99)
            result = service.reconcile()
            self.assertEqual(result["deleted_orphan_device_ids"], [99])
            self.assertEqual(result["terminal_records"][0]["record"]["state"], "error")
            record = service.journal.load("volume-1")
            assert record is not None
            self.assertEqual(record.state, StorageVolumeState.ERROR)

    def test_delete_detaches_unreachable_mount_and_reclaims_error_volume(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw)
            service, backend, host = self._service(root)
            created = service.create_volume(
                sandbox_id="sandbox-1",
                sandbox_generation=1,
                volume_id="volume-1",
                operation_id="create:1",
                virtual_size=1 << 30,
            )
            mount_path = Path(created["record"]["mount_path"])
            backend.live.remove(1)
            service.reconcile()
            failed = service.journal.load("volume-1")
            assert failed is not None
            self.assertEqual(failed.state, StorageVolumeState.ERROR)
            host.fail_next_unmount = True

            deleted = service.delete_volume(
                sandbox_id="sandbox-1",
                sandbox_generation=1,
                volume_id="volume-1",
                operation_id="delete:1",
                expected_revision=failed.revision,
            )

            self.assertEqual(deleted["record"]["state"], "deleted")
            self.assertEqual(host.detached, [mount_path])
            self.assertNotIn(mount_path, host.mounted)
            self.assertEqual(service.metrics()["hard_reserved_bytes"], 0)

    def test_interrupted_create_is_not_blindly_replayed(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw)
            service, backend, _ = self._service(root)
            volume_root = root / "runtime" / "volume-1"
            record = StorageVolumeRecord(
                volume_id="volume-1",
                sandbox_id="sandbox-1",
                sandbox_generation=1,
                revision=1,
                state=StorageVolumeState.CREATING,
                operation_id="create:1",
                virtual_size=1 << 30,
                runtime_dir=str(volume_root / "runtime"),
                mount_path=str(root / "mounts" / "volume-1"),
                source_image_config=str(volume_root / "source.json"),
                device_id=7,
                device_path="/dev/ublkb7",
                updated_ns=time.time_ns(),
            )
            request = {
                "kind": "CreateVolume",
                "operation_id": "create:1",
                "sandbox_generation": 1,
                "sandbox_id": "sandbox-1",
                "virtual_size": 1 << 30,
                "volume_id": "volume-1",
            }
            service.journal.reserve_create(
                request=request,
                record=record,
                hard_capacity_bytes=service.config.hard_capacity_bytes,
            )
            backend.live.add(7)
            result = service.reconcile()
            self.assertEqual(backend.delete_calls, [7])
            self.assertEqual(result["terminal_records"][0]["record"]["state"], "error")

    def test_versioned_unix_socket_protocol(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw)
            service, backend, _ = self._service(root)
            socket_path = root / "service" / "storage.sock"
            server = StorageNativeNodeServer(
                socket_path,
                service,
                require_root_peer=False,
            )
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            client = StorageNativeNodeClient(socket_path, timeout_seconds=2)
            deadline = time.monotonic() + 2
            while True:
                try:
                    features = client.get_features()
                    break
                except OSError:
                    if time.monotonic() >= deadline:
                        raise
                    time.sleep(0.01)
            self.assertEqual(features["protocol_schema"], 1)
            self.assertEqual(features["upper_mode"], "hybridLogStructured")
            created = client.create_volume(
                sandbox_id="sandbox-1",
                sandbox_generation=4,
                volume_id="volume-1",
                operation_id="create:1",
                virtual_size=1 << 30,
            )
            self.assertEqual(created["record"]["state"], "mounted")
            metrics = client.get_metrics()
            self.assertEqual(metrics["hard_reserved_bytes"], 1 << 30)
            self.assertEqual(metrics["active_operations"], 0)
            self.assertEqual(metrics["waiting_operations"], 0)
            self.assertEqual(
                client.get_volume("volume-1")["record"]["revision"],
                1,
            )
            self.assertEqual(backend.create_calls, 1)
            server.shutdown()
            thread.join(timeout=2)
            self.assertFalse(thread.is_alive())

    def test_direct_quota_adapter_preserves_exact_hard_ownership(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw)
            service, _, _ = self._service(root)
            socket_path = root / "service" / "storage.sock"
            server = StorageNativeNodeServer(
                socket_path,
                service,
                require_root_peer=False,
            )
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            client = StorageNativeNodeClient(socket_path, timeout_seconds=2)
            deadline = time.monotonic() + 2
            while True:
                try:
                    client.get_features()
                    break
                except OSError:
                    if time.monotonic() >= deadline:
                        raise
                    time.sleep(0.01)
            reservation = HibernationDiskReservation(
                sandbox_id="quota-sandbox",
                sandbox_generation=2,
                project_id=17,
                memory_mb=256,
                writable_disk_mb=512,
                memory_backing_mb=hibernation_memory_backing_reservation_mb(256),
                private_pages_mb=256,
                fixed_overhead_mb=64,
                created_ns=time.time_ns(),
            )
            quota = StorageNativeQuotaBackend(
                client,
                mount_root=root / "mounts",
            )
            prepared = quota.prepare(reservation)
            self.assertEqual(prepared["hard_limit_mb"], reservation.total_mb)
            self.assertEqual(prepared["project_id"], 17)
            self.assertEqual(quota.inventory(), (prepared,))
            dropped = quota.drop(reservation)
            self.assertTrue(dropped["removed"])
            self.assertEqual(quota.inventory(), ())
            server.shutdown()
            thread.join(timeout=2)
            self.assertFalse(thread.is_alive())

    def test_direct_quota_adapter_acquires_remote_snapshot_before_mount(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw)
            service, _, _ = self._service(root, publisher=True)
            socket_path = root / "service" / "storage.sock"
            server = StorageNativeNodeServer(
                socket_path,
                service,
                require_root_peer=False,
            )
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            client = StorageNativeNodeClient(socket_path, timeout_seconds=2)
            deadline = time.monotonic() + 2
            while True:
                try:
                    client.get_features()
                    break
                except OSError:
                    if time.monotonic() >= deadline:
                        raise
                    time.sleep(0.01)
            reservation = HibernationDiskReservation(
                sandbox_id="quota-import",
                sandbox_generation=3,
                project_id=18,
                memory_mb=256,
                writable_disk_mb=512,
                memory_backing_mb=hibernation_memory_backing_reservation_mb(256),
                private_pages_mb=256,
                fixed_overhead_mb=64,
                created_ns=time.time_ns(),
            )
            publication = StorageSnapshotPublication(
                manifest_digest="sha256:" + "f" * 64,
                tag="ucloud-storage-v1-test",
                repository="snapshots",
                repo_blob_url="http://registry/v2/snapshots/blobs",
                virtual_size=reservation.total_mb * 1024 * 1024,
                layers=(
                    PublishedStorageLayer(
                        digest="sha256:" + "1" * 64,
                        size=4096,
                    ),
                ),
            )
            quota = StorageNativeQuotaBackend(
                client,
                mount_root=root / "mounts",
            )

            prepared = quota.prepare_import(
                reservation,
                publication,
                migration_id="move:1",
            )

            self.assertEqual(prepared["hard_limit_mb"], reservation.total_mb)
            stored = client.get_volume("quota-import.sandbox-3")["record"]
            self.assertEqual(stored["state"], "mounted")
            self.assertEqual(
                stored["published_manifest_digest"],
                publication.manifest_digest,
            )
            server.shutdown()
            thread.join(timeout=2)
            self.assertFalse(thread.is_alive())


if __name__ == "__main__":
    unittest.main()
