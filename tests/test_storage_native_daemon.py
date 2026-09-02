from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass, replace
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
import threading
import time
import unittest

from ucloud_sandboxes.storage_native import (
    StorageNativeDevice,
    StorageNativeDeviceOwner,
)
from ucloud_sandboxes.storage_native_daemon import (
    StorageNativeCapacityError,
    StorageNativeConflictError,
    StorageNativeNodeConfig,
    StorageNativePendingOperation,
    StorageNativeNodeClient,
    StorageNativeNodeError,
    StorageNativeJournal,
    StorageNativeNodeServer,
    StorageNativeNodeService,
    StorageVolumeOwner,
    StorageVolumeRecord,
    StorageVolumeState,
)
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
    def __init__(self, *, descriptor: bool = False, pooled: bool = False) -> None:
        self.descriptor = descriptor
        self.pooled = pooled
        self.live: set[int] = set()
        self.idle: set[int] = set()
        self.next_device_id = 1
        self.create_calls = 0
        self.restack_calls = 0
        self.delete_calls: list[int] = []
        self.release_calls: list[int] = []
        self.fail_next_release = False
        self.owners: dict[str, StorageNativeDeviceOwner] = {}

    def create_runtime_device(
        self,
        *,
        source_image_config: Path,
        global_config: Path,
        runtime_dir: Path,
        virtual_size: int,
        upper_mode: str,
        owner_id: str,
    ) -> StorageNativeDevice:
        existing = self.owners.get(owner_id)
        if existing is not None:
            return StorageNativeDevice(
                device_id=existing.device_id,
                device_path=existing.device_path,
                virtual_size=virtual_size,
                image_config_path=existing.image_config_path,
            )
        self.create_calls += 1
        if self.pooled and self.idle:
            device_id = min(self.idle)
            self.idle.remove(device_id)
        else:
            device_id = self.next_device_id
            self.next_device_id += 1
        self.live.add(device_id)
        runtime_dir.mkdir(mode=0o700)
        image = runtime_dir / "image.json"
        image.write_text("{}\n", encoding="ascii")
        device = StorageNativeDevice(
            device_id=device_id,
            device_path=Path(f"/dev/ublkb{device_id}"),
            virtual_size=virtual_size,
            image_config_path=image,
        )
        self.owners[owner_id] = StorageNativeDeviceOwner(
            owner_id=owner_id,
            device_id=device.device_id,
            device_path=device.device_path,
            image_config_path=device.image_config_path,
        )
        return device

    def list_runtime_device_owners(self) -> tuple[StorageNativeDeviceOwner, ...]:
        return tuple(self.owners.values())

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
        self.idle.discard(device_id)
        self.live.discard(device_id)
        self.owners = {
            owner_id: owner
            for owner_id, owner in self.owners.items()
            if owner.device_id != device_id
        }

    def release(self, device_id: int) -> None:
        self.release_calls.append(device_id)
        if self.fail_next_release:
            self.fail_next_release = False
            raise RuntimeError("injected release failure")
        if device_id not in self.live:
            raise RuntimeError("device is missing")
        self.idle.add(device_id)
        self.owners = {
            owner_id: owner
            for owner_id, owner in self.owners.items()
            if owner.device_id != device_id
        }


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
        existing_repo_blob_url: str = "",
        global_config_path: Path | None = None,
    ) -> StorageSnapshotPublication:
        if global_config_path is None:
            raise AssertionError("service did not supply its global config")
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

    def metrics(self) -> dict[str, int]:
        return {"snapshot_publications": 0, "snapshot_compactions": 0}


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
        pooled: bool = False,
        max_ublk_devices: int = 0,
    ) -> tuple[StorageNativeNodeService, FakeBlockBackend, FakeHost]:
        backend = FakeBlockBackend(descriptor=descriptor, pooled=pooled)
        host = FakeHost(backend)
        global_config = root / "global.json"
        global_config.write_text("{}\n", encoding="ascii")
        service = StorageNativeNodeService(
            StorageNativeNodeConfig(
                journal_path=root / "journal" / "storage.sqlite",
                runtime_root=root / "runtime",
                mount_root=root / "mounts",
                hard_capacity_bytes=capacity,
                device_pool_enabled=pooled,
                device_pool_low_watermark=(
                    min(2, max_ublk_devices) if max_ublk_devices > 0 else 2
                ),
                device_pool_high_watermark=(
                    min(16, max_ublk_devices) if max_ublk_devices > 0 else 16
                ),
                max_ublk_devices=max_ublk_devices,
            ),
            backend=backend,
            global_config_path=global_config,
            host=host,
            publisher=FakePublisher() if publisher else None,
        )
        return service, backend, host

    def test_ublk_limit_rejects_before_journaling_and_reuses_idle_device(self) -> None:
        with TemporaryDirectory() as raw:
            service, backend, _ = self._service(
                Path(raw),
                pooled=True,
                max_ublk_devices=1,
            )
            first = service.create_volume(
                sandbox_id="sandbox-1",
                sandbox_generation=1,
                volume_id="volume-1",
                operation_id="create:1",
                virtual_size=1 << 30,
            )

            with self.assertRaisesRegex(
                StorageNativeConflictError,
                "ublk device capacity is exhausted",
            ):
                service.create_volume(
                    sandbox_id="sandbox-2",
                    sandbox_generation=1,
                    volume_id="volume-2",
                    operation_id="create:2",
                    virtual_size=1 << 30,
                )

            self.assertIsNone(service.journal.load("volume-2"))
            self.assertEqual(service.metrics()["ublk_max_devices"], 1)
            service.delete_volume(
                sandbox_id="sandbox-1",
                sandbox_generation=1,
                volume_id="volume-1",
                operation_id="delete:1",
                expected_revision=first.revision,
            )
            second = service.create_volume(
                sandbox_id="sandbox-2",
                sandbox_generation=1,
                volume_id="volume-2",
                operation_id="create:2",
                virtual_size=1 << 30,
            )

            self.assertEqual(second.device_id, first.device_id)
            self.assertEqual(backend.live, {first.device_id})

    def test_ublk_limit_rejects_wake_before_changing_released_volume(self) -> None:
        with TemporaryDirectory() as raw:
            service, _, _ = self._service(
                Path(raw),
                pooled=True,
                max_ublk_devices=1,
            )
            created = service.create_volume(
                sandbox_id="sandbox-1",
                sandbox_generation=1,
                volume_id="volume-1",
                operation_id="create:1",
                virtual_size=1 << 30,
            )
            sealed = service.freeze_and_seal(
                sandbox_id="sandbox-1",
                sandbox_generation=1,
                volume_id="volume-1",
                operation_id="seal:1",
                expected_revision=created.revision,
            )
            released = service.release_runtime(
                sandbox_id="sandbox-1",
                sandbox_generation=1,
                volume_id="volume-1",
                operation_id="release:1",
                expected_revision=sealed.revision,
            )
            service.create_volume(
                sandbox_id="sandbox-2",
                sandbox_generation=1,
                volume_id="volume-2",
                operation_id="create:2",
                virtual_size=1 << 30,
            )

            with self.assertRaisesRegex(
                StorageNativeConflictError,
                "ublk device capacity is exhausted",
            ):
                service.mount_snapshot_cow(
                    sandbox_id="sandbox-1",
                    sandbox_generation=1,
                    volume_id="volume-1",
                    operation_id="wake:1",
                    expected_revision=released.revision,
                )

            unchanged = service.journal.load("volume-1")
            self.assertIsNotNone(unchanged)
            assert unchanged is not None
            self.assertEqual(unchanged.state, StorageVolumeState.RELEASED)
            self.assertEqual(unchanged.revision, released.revision)
            self.assertEqual(unchanged.error, "")

    def test_accounting_ids_are_transactional_and_monotonic_across_restart(
        self,
    ) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw)
            service, _, _ = self._service(root)

            def reserve(index: int, *, journal=service.journal) -> int:
                volume_id = f"volume-{index}"
                operation_id = f"create:{index}"
                volume_root = root / "runtime" / volume_id
                record = StorageVolumeRecord(
                    volume_id=volume_id,
                    sandbox_id=f"sandbox-{index}",
                    sandbox_generation=1,
                    revision=1,
                    state=StorageVolumeState.CREATING,
                    operation_id=operation_id,
                    virtual_size=1 << 20,
                    runtime_dir=str(volume_root / "runtime"),
                    mount_path=str(root / "mounts" / volume_id),
                    source_image_config=str(volume_root / "source.json"),
                    device_owner_id=f"device:accounting-{index}",
                    updated_ns=time.time_ns(),
                )
                reserved = journal.reserve_create(
                    request={
                        "kind": "CreateVolume",
                        "operation_id": operation_id,
                        "sandbox_generation": 1,
                        "sandbox_id": f"sandbox-{index}",
                        "virtual_size": 1 << 20,
                        "volume_id": volume_id,
                    },
                    record=record,
                    hard_capacity_bytes=service.config.hard_capacity_bytes,
                )
                assert isinstance(reserved, StorageVolumeRecord)
                return reserved.accounting_id

            with ThreadPoolExecutor(max_workers=8) as pool:
                accounting_ids = tuple(pool.map(reserve, range(12)))

            self.assertEqual(
                sorted(accounting_ids),
                list(range(200_000, 200_012)),
            )
            restarted = type(service.journal)(service.config.journal_path)
            self.assertEqual(reserve(12, journal=restarted), 200_012)

    def test_persisted_volume_requires_positive_accounting_id(self) -> None:
        with TemporaryDirectory() as raw:
            service, _, _ = self._service(Path(raw))
            created = service.create_volume(
                sandbox_id="sandbox-1",
                sandbox_generation=1,
                volume_id="volume-1",
                operation_id="create:1",
                virtual_size=1 << 20,
            ).to_json()

            missing = dict(created)
            missing.pop("accounting_id")
            with self.assertRaisesRegex(ValueError, "invalid schema"):
                StorageVolumeRecord.from_json(missing)

            zero = {**created, "accounting_id": 0}
            with self.assertRaisesRegex(ValueError, "positive accounting ID"):
                StorageVolumeRecord.from_json(zero)

            service.create_volume(
                sandbox_id="sandbox-2",
                sandbox_generation=1,
                volume_id="volume-2",
                operation_id="create:2",
                virtual_size=1 << 20,
            )
            with closing(sqlite3.connect(service.config.journal_path)) as connection:
                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(
                        "UPDATE volumes SET accounting_id = 200000 "
                        "WHERE volume_id = 'volume-2'"
                    )
                connection.execute(
                    "UPDATE volumes SET accounting_id = 300000 "
                    "WHERE volume_id = 'volume-1'"
                )
                connection.commit()
            with self.assertRaisesRegex(
                StorageNativeNodeError,
                "columns are inconsistent",
            ):
                service.journal.load("volume-1")

    def test_existing_unversioned_journal_is_rejected(self) -> None:
        with TemporaryDirectory() as raw:
            path = Path(raw) / "journal" / "storage.sqlite"
            path.parent.mkdir(mode=0o700)
            with closing(sqlite3.connect(path)) as connection:
                connection.execute("CREATE TABLE volumes (volume_id TEXT PRIMARY KEY)")
                connection.commit()

            with self.assertRaisesRegex(
                StorageNativeNodeError,
                "schema is incompatible",
            ):
                StorageNativeJournal(path)

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
                expected_revision=created.revision,
            )
            released = service.release_runtime(
                sandbox_id="sandbox-1",
                sandbox_generation=3,
                volume_id="volume-1",
                operation_id="release:1",
                expected_revision=sealed.revision,
            )
            mounted = service.mount_snapshot_cow(
                sandbox_id="sandbox-1",
                sandbox_generation=3,
                volume_id="volume-1",
                operation_id="wake:1",
                expected_revision=released.revision,
            )

            discarded = service.discard_mounted_cow(
                sandbox_id="sandbox-1",
                sandbox_generation=3,
                volume_id="volume-1",
                operation_id="wake:1:rollback",
                expected_revision=mounted.revision,
            )

            self.assertEqual(discarded.state, StorageVolumeState.RELEASED)
            self.assertTrue(discarded.sealed_layer_paths)
            self.assertEqual(backend.delete_calls, [1, 2])

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
            local_layer = Path(released.sealed_layer_paths[0])
            self.assertTrue(local_layer.exists())

            published = service.publish_snapshot(
                sandbox_id="sandbox-1",
                sandbox_generation=1,
                volume_id="volume-1",
                operation_id="publish:1",
                expected_revision=3,
            )
            self.assertEqual(published.state, StorageVolumeState.PUBLISHED)
            self.assertEqual(len(published.published_layers), 1)
            self.assertFalse(local_layer.exists())

            mounted = service.mount_snapshot_cow(
                sandbox_id="sandbox-1",
                sandbox_generation=1,
                volume_id="volume-1",
                operation_id="mount:remote",
                expected_revision=4,
            )
            self.assertEqual(mounted.state, StorageVolumeState.MOUNTED)
            source = json.loads(
                Path(mounted.source_image_config).read_text(encoding="ascii")
            )
            self.assertEqual(
                source["repoBlobUrl"],
                "http://registry/v2/snapshots/blobs",
            )
            self.assertEqual(len(source["lowers"]), 1)
            self.assertEqual(backend.create_calls, 2)

    def test_reconcile_deletes_orphans_and_terminally_fences_missing_device(
        self,
    ) -> None:
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
            backend.owners["device:rogue"] = StorageNativeDeviceOwner(
                owner_id="device:rogue",
                device_id=99,
                device_path=Path("/dev/ublkb99"),
                image_config_path=root / "rogue" / "image.json",
            )
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
            mount_path = Path(created.mount_path)
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

            self.assertEqual(deleted.state, StorageVolumeState.DELETED)
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
                device_owner_id="device:test-interrupted-create",
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
            backend.owners[record.device_owner_id] = StorageNativeDeviceOwner(
                owner_id=record.device_owner_id,
                device_id=7,
                device_path=Path("/dev/ublkb7"),
                image_config_path=volume_root / "runtime" / "image.json",
            )
            result = service.reconcile()
            self.assertEqual(backend.delete_calls, [7])
            self.assertEqual(result["terminal_records"][0]["record"]["state"], "error")

    def test_interrupted_cow_acquire_recovers_backend_owner_before_cleanup(
        self,
    ) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw)
            service, backend, _ = self._service(root)
            volume_root = root / "runtime" / "volume-1"
            volume_root.mkdir(parents=True)
            source = volume_root / "source.json"
            source.write_text("{}\n", encoding="ascii")
            sealed = volume_root / "sealed.commit"
            sealed.write_bytes(b"sealed")
            record = StorageVolumeRecord(
                volume_id="volume-1",
                sandbox_id="sandbox-1",
                sandbox_generation=1,
                revision=1,
                state=StorageVolumeState.RELEASED,
                operation_id="seed:1",
                virtual_size=1 << 30,
                runtime_dir=str(volume_root / "runtime-seed"),
                mount_path=str(root / "mounts" / "volume-1"),
                source_image_config=str(source),
                device_owner_id="",
                sealed_layer_paths=(str(sealed),),
                updated_ns=time.time_ns(),
            )
            service.journal.reserve_create(
                request={
                    "kind": "CreateVolume",
                    "operation_id": "seed:1",
                    "sandbox_generation": 1,
                    "sandbox_id": "sandbox-1",
                    "virtual_size": 1 << 30,
                    "volume_id": "volume-1",
                },
                record=record,
                hard_capacity_bytes=service.config.hard_capacity_bytes,
            )
            pending = service.journal.begin_transition(
                request={
                    "expected_revision": 1,
                    "kind": "MountSnapshotCow",
                    "operation_id": "mount:1",
                    "sandbox_generation": 1,
                    "sandbox_id": "sandbox-1",
                    "volume_id": "volume-1",
                },
                operation_id="mount:1",
                kind="MountSnapshotCow",
                volume_id="volume-1",
                sandbox_id="sandbox-1",
                sandbox_generation=1,
                expected_revision=1,
                allowed_states={StorageVolumeState.RELEASED},
                next_state=StorageVolumeState.ACQUIRING,
                reserve_capacity=True,
                hard_capacity_bytes=service.config.hard_capacity_bytes,
            )
            assert isinstance(pending, StorageVolumeRecord)
            runtime_dir = volume_root / "runtime-2"
            pending = replace(
                pending,
                runtime_dir=str(runtime_dir),
                device_owner_id="device:test-interrupted-cow",
            )
            service.journal.update_pending(pending)
            backend.create_runtime_device(
                source_image_config=source,
                global_config=service.global_config_path,
                runtime_dir=runtime_dir,
                virtual_size=1 << 30,
                upper_mode=service.config.upper_mode,
                owner_id=pending.device_owner_id,
            )

            result = service.reconcile()

            recovered = service.journal.load("volume-1")
            assert recovered is not None
            self.assertEqual(recovered.state, StorageVolumeState.ERROR)
            self.assertEqual(result["terminal_records"][0]["record"]["state"], "error")
            self.assertEqual(backend.delete_calls, [1])
            self.assertEqual(backend.owners, {})

    def test_versioned_unix_socket_protocol(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw)
            service, backend, _ = self._service(root, capacity=2 << 30)
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
            self.assertEqual(features["protocol_schema"], 4)
            self.assertEqual(features["upper_mode"], "hybridLogStructured")
            with self.assertRaisesRegex(StorageNativeNodeError, "invalid schema"):
                client._call({"operation": "GetFeatures", "unexpected": True})
            created = client.prepare_volume(
                StorageVolumeOwner("volume-1", "sandbox-1", 4),
                operation_id="create:1",
                virtual_size=1 << 30,
            )
            self.assertEqual(created.state, StorageVolumeState.MOUNTED)
            other = client.prepare_volume(
                StorageVolumeOwner("volume-2", "sandbox-2", 4),
                operation_id="create:1",
                virtual_size=1 << 30,
            )
            self.assertEqual(other.accounting_id, created.accounting_id + 1)
            with self.assertRaisesRegex(
                StorageNativeCapacityError,
                "hard capacity is exhausted",
            ):
                client.prepare_volume(
                    StorageVolumeOwner("volume-3", "sandbox-3", 4),
                    operation_id="create:3",
                    virtual_size=1 << 30,
                )
            metrics = client.get_metrics()
            self.assertEqual(metrics["hard_reserved_bytes"], 2 << 30)
            self.assertEqual(metrics["active_operations"], 0)
            self.assertEqual(metrics["waiting_operations"], 0)
            self.assertEqual(
                client.get_volume("volume-1").revision,
                1,
            )
            released = client.ensure_released(
                created.owner,
                operation_id="park:1",
            )
            self.assertEqual(released.state, StorageVolumeState.RELEASED)
            mounted = client.ensure_mounted(
                created.owner,
                operation_id="wake:1",
            )
            self.assertEqual(mounted.state, StorageVolumeState.MOUNTED)
            self.assertEqual(backend.create_calls, 3)
            server.shutdown()
            thread.join(timeout=2)
            self.assertFalse(thread.is_alive())

    def test_pooled_delete_retries_backend_release_before_forgetting_device(
        self,
    ) -> None:
        with TemporaryDirectory() as raw:
            service, backend, _ = self._service(Path(raw), pooled=True)
            created = service.create_volume(
                sandbox_id="sandbox-1",
                sandbox_generation=1,
                volume_id="volume-1",
                operation_id="create:1",
                virtual_size=1 << 30,
            )
            backend.fail_next_release = True

            with self.assertRaisesRegex(
                StorageNativePendingOperation,
                "waiting for backend device release",
            ):
                service.delete_volume(
                    sandbox_id="sandbox-1",
                    sandbox_generation=1,
                    volume_id="volume-1",
                    operation_id="delete:1",
                    expected_revision=created.revision,
                )

            waiting = service.journal.load("volume-1")
            assert waiting is not None
            self.assertEqual(waiting.state, StorageVolumeState.DELETING)
            self.assertEqual(waiting.device_id, 1)
            self.assertEqual(backend.live, {1})
            self.assertEqual(backend.idle, set())
            self.assertEqual(service.metrics()["device_pool_idle_devices"], 0)

            reconciled = service.reconcile()
            deleted = service.journal.load("volume-1")
            assert deleted is not None
            self.assertEqual(reconciled["terminal_records"], [])
            self.assertEqual(deleted.state, StorageVolumeState.DELETED)
            self.assertIsNone(deleted.device_id)
            self.assertEqual(backend.idle, {1})
            self.assertEqual(service.metrics()["device_pool_idle_devices"], 1)
            replay = service.delete_volume(
                sandbox_id="sandbox-1",
                sandbox_generation=1,
                volume_id="volume-1",
                operation_id="delete:1",
                expected_revision=created.revision,
            )
            self.assertEqual(replay.state, StorageVolumeState.DELETED)


if __name__ == "__main__":
    unittest.main()
