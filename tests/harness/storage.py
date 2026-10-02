"""Directory-backed block devices for the real storage-native node service.

The journal, fencing, unix-socket protocol and client stay production code.
Only ``StorageBlockBackend`` (ublk/overlaybd) and ``StorageHostOperations``
(mkfs/mount/freeze) are replaced:

- A device's filesystem is a directory, ``<volume root>/.fake-device-<id>``.
  ``mount`` renames its entries into the mount target and ``unmount`` renames
  them back, so the target is empty whenever it is unmounted, like a real
  mountpoint.
- ``restack_snapshot`` (seal) records the live tree as a hard-link snapshot,
  ``<volume root>/.fake-sealed``, and writes a small layer file naming it.
  A device created from sealed layers starts as hard links of that snapshot,
  so file inodes survive release and remount exactly as they do on XFS; the
  Warden's checkpoint manifests depend on that. Anything not sealed before a
  device is released is lost, which models COW discard.
- Snapshots are full trees, not deltas, and only the newest one is kept. A
  device built from an older or a published layer is refused.

Hard links mean an in-place write to a mounted file after a seal would also
change the snapshot. The Warden, the fake runsc and the fake overlay replace
files instead of rewriting them.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import shutil
import stat
import threading
import uuid

from ucloud_sandboxes.storage_native import StorageNativeDevice, StorageNativeDeviceOwner

LAYER_FORMAT = "fake-storage-layer-v1"
SEALED = ".fake-sealed"


@dataclass
class _Device:
    device_id: int
    owner_id: str
    volume_root: Path
    image_config_path: Path
    virtual_size: int
    filesystem_bytes: int

    @property
    def path(self) -> Path:
        return Path(f"/dev/ublkb{self.device_id}")

    @property
    def content(self) -> Path:
        return self.volume_root / f".fake-device-{self.device_id}"


def hardlink_tree(source: Path, target: Path) -> None:
    """Mirror ``source`` into new directory ``target`` sharing file inodes."""
    target.mkdir(mode=stat.S_IMODE(source.lstat().st_mode))
    for child in source.iterdir():
        info = child.lstat()
        destination = target / child.name
        if stat.S_ISDIR(info.st_mode):
            hardlink_tree(child, destination)
        elif stat.S_ISLNK(info.st_mode):
            os.symlink(os.readlink(child), destination)
        else:
            os.link(child, destination, follow_symlinks=False)
    shutil.copystat(source, target, follow_symlinks=False)


class FakeBlockBackend:
    """In-memory device ownership over on-disk directory filesystems.

    Ownership is process state, like ublk devices that vanish with their
    daemon: scenarios restart node agents, never the storage service.
    """

    def __init__(self, runtime_root: Path) -> None:
        self.runtime_root = runtime_root
        self.host: FakeStorageHost | None = None
        self._guard = threading.Lock()
        self._next_device_id = 1
        self.devices: dict[int, _Device] = {}

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
        del global_config, upper_mode
        with self._guard:
            existing = next(
                (device for device in self.devices.values() if device.owner_id == owner_id),
                None,
            )
            if existing is not None:
                existing.virtual_size = virtual_size
                return self._descriptor(existing)
            volume_root = runtime_dir.parent
            if volume_root.parent != self.runtime_root:
                raise RuntimeError("runtime device escaped the storage runtime root")
            device = _Device(
                device_id=self._next_device_id,
                owner_id=owner_id,
                volume_root=volume_root,
                image_config_path=runtime_dir / "image.json",
                virtual_size=virtual_size,
                filesystem_bytes=virtual_size,
            )
            self._next_device_id += 1
            lowers = json.loads(source_image_config.read_text(encoding="utf-8"))["lowers"]
            runtime_dir.mkdir(mode=0o700, exist_ok=True)
            device.image_config_path.write_text(
                json.dumps({"fake_device": device.device_id, "lowers": lowers}) + "\n",
                encoding="ascii",
            )
            if lowers:
                self._require_newest_seal(volume_root, lowers[-1])
                hardlink_tree(volume_root / SEALED, device.content)
            else:
                device.content.mkdir(mode=0o700)
            self.devices[device.device_id] = device
            return self._descriptor(device)

    @staticmethod
    def _require_newest_seal(volume_root: Path, lower: dict) -> None:
        if set(lower) != {"file"}:
            raise NotImplementedError("published storage layers are not modeled")
        layer = json.loads(Path(lower["file"]).read_text(encoding="ascii"))
        marker = (volume_root / f"{SEALED}.id").read_text(encoding="ascii").strip()
        if layer.get("format") != LAYER_FORMAT or layer.get("seal") != marker:
            raise NotImplementedError("only the newest sealed layer can be mounted")

    def list_runtime_device_owners(self) -> tuple[StorageNativeDeviceOwner, ...]:
        with self._guard:
            return tuple(
                StorageNativeDeviceOwner(
                    owner_id=device.owner_id,
                    device_id=device.device_id,
                    device_path=device.path,
                    image_config_path=device.image_config_path,
                )
                for device in self.devices.values()
            )

    def restack_snapshot(self, device_id: int, output_layer_path: Path) -> None:
        device = self._device(device_id)
        assert self.host is not None
        live = self.host.mounted_target(device.path) or device.content
        volume_root = device.volume_root
        seal = uuid.uuid4().hex
        staging = volume_root / f"{SEALED}.new-{seal}"
        hardlink_tree(live, staging)
        retired = volume_root / f"{SEALED}.old-{seal}"
        if (volume_root / SEALED).exists():
            os.rename(volume_root / SEALED, retired)
        os.rename(staging, volume_root / SEALED)
        (volume_root / f"{SEALED}.id").write_text(seal + "\n", encoding="ascii")
        if retired.exists():
            shutil.rmtree(retired)
        files = sum(len(names) for _root, _dirs, names in os.walk(live))
        output_layer_path.write_text(
            json.dumps({"format": LAYER_FORMAT, "seal": seal, "files": files}) + "\n",
            encoding="ascii",
        )
        return None

    def delete(self, device_id: int) -> None:
        self.release(device_id)

    def release(self, device_id: int) -> None:
        device = self._device(device_id)
        assert self.host is not None
        if self.host.mounted_target(device.path) is not None:
            raise RuntimeError("cannot release a mounted fake device")
        shutil.rmtree(device.content, ignore_errors=True)
        with self._guard:
            self.devices.pop(device_id, None)

    def device_for_path(self, path: Path) -> _Device:
        with self._guard:
            for device in self.devices.values():
                if device.path == path:
                    return device
        raise RuntimeError(f"unknown fake block device: {path}")

    def _device(self, device_id: int) -> _Device:
        with self._guard:
            device = self.devices.get(device_id)
        if device is None:
            raise RuntimeError(f"fake block device {device_id} is missing")
        return device

    @staticmethod
    def _descriptor(device: _Device) -> StorageNativeDevice:
        return StorageNativeDevice(
            device_id=device.device_id,
            device_path=device.path,
            virtual_size=device.virtual_size,
            image_config_path=device.image_config_path,
        )


class Hold:
    """A one-shot pause: ``reached`` is set on arrival, then waits for ``release``."""

    def __init__(self) -> None:
        self.reached = threading.Event()
        self.release = threading.Event()


class FakeStorageHost:
    """mkfs/mount/freeze over the fake devices; mounts move directory entries."""

    def __init__(self, backend: FakeBlockBackend) -> None:
        self.backend = backend
        backend.host = self
        self._guard = threading.Lock()
        self.mounts: dict[Path, Path] = {}
        self._holds: dict[str, Hold] = {}
        self._armed: list[Hold] = []

    def hold(self, operation: str) -> Hold:
        """Pause the next ``operation`` (a method name), as a slow device would."""
        hold = self._holds[operation] = Hold()
        self._armed.append(hold)
        return hold

    def release_holds(self) -> None:
        self._holds.clear()
        for hold in self._armed:
            hold.release.set()

    def _pause(self, operation: str) -> None:
        hold = self._holds.pop(operation, None)
        if hold is not None:
            hold.reached.set()
            hold.release.wait(30)

    def mounted_target(self, device: Path) -> Path | None:
        with self._guard:
            return next((target for target, item in self.mounts.items() if item == device), None)

    def device_is_unused(self, device: Path) -> bool:
        return self.mounted_target(device) is None

    def format_xfs(self, device: Path, *, size_bytes: int | None = None) -> None:
        fake = self.backend.device_for_path(device)
        if self.mounted_target(device) is not None:
            raise RuntimeError("cannot format a mounted fake device")
        shutil.rmtree(fake.content)
        fake.content.mkdir(mode=0o700)
        fake.filesystem_bytes = size_bytes or fake.virtual_size

    def mount(self, device: Path, target: Path) -> None:
        self._pause("mount")
        fake = self.backend.device_for_path(device)
        with self._guard:
            if target in self.mounts or device in self.mounts.values():
                raise RuntimeError("fake device or mountpoint is already mounted")
            if target.is_symlink() or not target.is_dir() or any(target.iterdir()):
                raise RuntimeError(f"fake mountpoint must be an empty directory: {target}")
            for child in fake.content.iterdir():
                os.rename(child, target / child.name)
            os.chmod(target, stat.S_IMODE(fake.content.lstat().st_mode))
            self.mounts[target] = device

    def unmount(self, target: Path) -> None:
        with self._guard:
            device = self.mounts.pop(target, None)
            if device is None:
                raise RuntimeError(f"fake mountpoint is not mounted: {target}")
            fake = self.backend.device_for_path(device)
            for child in target.iterdir():
                os.rename(child, fake.content / child.name)

    def detach(self, target: Path) -> None:
        self.unmount(target)

    def is_mounted(self, target: Path) -> bool:
        with self._guard:
            return target in self.mounts

    def sync(self, target: Path) -> None:
        if not self.is_mounted(target):
            raise RuntimeError(f"cannot sync an unmounted fake filesystem: {target}")

    def freeze(self, target: Path) -> None:
        self.sync(target)

    def unfreeze(self, target: Path) -> None:
        self.sync(target)

    def trim(self, target: Path) -> None:
        self.sync(target)

    def filesystem_bytes(self, target: Path) -> int:
        with self._guard:
            device = self.mounts[target]
        return self.backend.device_for_path(device).filesystem_bytes

    def grow_xfs(self, target: Path, size_bytes: int) -> None:
        with self._guard:
            device = self.mounts[target]
        self.backend.device_for_path(device).filesystem_bytes = size_bytes

    def ublk_device_ids(self) -> set[int]:
        with self.backend._guard:
            return set(self.backend.devices)
