"""Provision one owned, direct-I/O XFS memory filesystem on a fresh worker.

Existing worker data is never moved, covered or formatted. The durable record
names an exact newly-created image inode; an ambiguous interrupted format stops
bootstrap. Ordinary restarts reattach that inode without formatting it again.
"""

from __future__ import annotations

import argparse
import fcntl
import json
import os
from pathlib import Path
import stat
import subprocess
import tempfile
import uuid

from .memory_backing import XfsMemoryQuota


class MemoryFilesystemError(RuntimeError):
    pass


def _run(*argv: str) -> str:
    return subprocess.run(
        argv, text=True, capture_output=True, check=True
    ).stdout.strip()


def _sync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _save(path: Path, record: dict) -> None:
    descriptor, name = tempfile.mkstemp(prefix=".memory-filesystem-", dir=path.parent)
    temporary = Path(name)
    try:
        with os.fdopen(descriptor, "w") as stream:
            json.dump(record, stream, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(path)
        _sync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _private(path: Path, *, directory: bool) -> os.stat_result:
    info = path.lstat()
    correct_type = (
        stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode)
    )
    if not correct_type or info.st_uid != os.geteuid() or info.st_mode & 0o077:
        raise MemoryFilesystemError(f"memory filesystem ownership is invalid: {path}")
    return info


def _mount(path: Path) -> dict:
    return json.loads(
        _run(
            "findmnt", "-J", "-o", "TARGET,SOURCE,FSTYPE,OPTIONS", "--target", str(path)
        )
    )["filesystems"][0]


def provision_memory_filesystem(mount_root: Path, *, hard_capacity_bytes: int) -> dict:
    if os.geteuid() != 0 or not mount_root.is_absolute() or hard_capacity_bytes <= 0:
        raise ValueError(
            "memory filesystem requires root, an absolute mount root and positive capacity"
        )
    if any(character.isspace() for character in str(mount_root)):
        raise ValueError("memory filesystem mount path cannot contain whitespace")
    parent = mount_root.parent
    _private(parent, directory=True)
    mount_root.mkdir(mode=0o700, exist_ok=True)
    _private(mount_root, directory=True)
    lock_fd = os.open(
        parent / "memory-filesystem.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600
    )
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX)
        return _provision_locked(mount_root, hard_capacity_bytes)
    finally:
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
        os.close(lock_fd)


def _provision_locked(mount_root: Path, capacity: int) -> dict:
    parent = mount_root.parent
    image = parent / "memory-backing.xfs"
    receipt = parent / "memory-filesystem.json"
    # This allowance is outside the sandbox guarantee, for filesystem metadata.
    overhead = max(512 * 1024**2, (capacity + 19) // 20)
    image_size = (capacity + overhead + 4095) // 4096 * 4096
    mounted = _mount(mount_root)
    if not receipt.exists():
        if mounted["fstype"] == "xfs" and {"pquota", "prjquota"} & set(
            mounted["options"].split(",")
        ):
            XfsMemoryQuota().validate_root(mount_root)
            space = os.statvfs(mount_root)
            if space.f_blocks * space.f_frsize < capacity:
                raise MemoryFilesystemError(
                    "preprovisioned memory filesystem is smaller than its budget"
                )
            return {"schema": 1, "external": True, "mount_root": str(mount_root)}
        # A worker upgrade must never cover its old volume mounts or artifacts.
        nested = _run("findmnt", "-rn", "-o", "TARGET").splitlines()
        if any(mount_root.iterdir()) or any(
            p == str(mount_root) or p.startswith(str(mount_root) + "/") for p in nested
        ):
            raise MemoryFilesystemError(
                "split memory layout requires an empty fresh-worker mount root"
            )
        runtime = parent / "runtime"
        if runtime.exists() and any(runtime.iterdir()):
            raise MemoryFilesystemError(
                "cannot enable split memory layout on an existing worker"
            )
        if (parent / "journal.sqlite").exists() or image.exists():
            raise MemoryFilesystemError(
                "unrecorded storage artifacts require explicit recovery"
            )
        available = os.statvfs(parent)
        if available.f_bavail * available.f_frsize < image_size:
            raise MemoryFilesystemError(
                "physical disk lacks shared capacity plus memory-filesystem metadata headroom"
            )
        descriptor = os.open(
            image, os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600
        )
        try:
            os.ftruncate(descriptor, image_size)
            os.fsync(descriptor)
            info = os.fstat(descriptor)
        finally:
            os.close(descriptor)
        _sync_directory(parent)
        record = {
            "schema": 1,
            "phase": "created",
            "image": str(image),
            "mount_root": str(mount_root),
            "capacity_bytes": capacity,
            "image_bytes": image_size,
            "device": info.st_dev,
            "inode": info.st_ino,
            "uuid": str(uuid.uuid4()),
        }
        _save(receipt, record)
    else:
        _private(receipt, directory=False)
        record = json.loads(receipt.read_text())
        expected = {
            "schema",
            "phase",
            "image",
            "mount_root",
            "capacity_bytes",
            "image_bytes",
            "device",
            "inode",
            "uuid",
        }
        if (
            set(record) != expected
            or record["schema"] != 1
            or record["phase"] not in {"created", "formatted"}
        ):
            raise MemoryFilesystemError("memory filesystem record schema is invalid")
        if (
            record["image"],
            record["mount_root"],
            record["capacity_bytes"],
            record["image_bytes"],
        ) != (str(image), str(mount_root), capacity, image_size):
            raise MemoryFilesystemError(
                "memory filesystem configuration differs from its durable owner"
            )
        if str(uuid.UUID(record["uuid"])) != record["uuid"]:
            raise MemoryFilesystemError("memory filesystem UUID is invalid")
    info = _private(image, directory=False)
    if (info.st_dev, info.st_ino, info.st_size) != (
        record["device"],
        record["inode"],
        image_size,
    ):
        raise MemoryFilesystemError("memory filesystem image identity changed")
    if info.st_dev != parent.stat().st_dev:
        raise MemoryFilesystemError(
            "memory and workspace backing must share the physical disk budget"
        )
    if record["phase"] == "created":
        if info.st_blocks:
            raise MemoryFilesystemError(
                "interrupted memory filesystem format needs explicit recovery"
            )
        _run(
            "mkfs.xfs",
            "-m",
            "reflink=1,uuid=" + record["uuid"],
            "-n",
            "ftype=1",
            str(image),
        )
        descriptor = os.open(image, os.O_RDONLY | os.O_NOFOLLOW)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        record["phase"] = "formatted"
        _save(receipt, record)
    if (
        _run("blkid", "-p", "-s", "UUID", "-o", "value", str(image)) != record["uuid"]
        or _run("blkid", "-p", "-s", "TYPE", "-o", "value", str(image)) != "xfs"
    ):
        raise MemoryFilesystemError("recorded memory filesystem has another identity")
    if mounted["target"] == str(mount_root):
        loop = mounted["source"]
    else:
        if any(mount_root.iterdir()):
            raise MemoryFilesystemError(
                "refusing to cover files below memory mount root"
            )
        loop = _run(
            "losetup", "--find", "--show", "--nooverlap", "--direct-io=on", str(image)
        )
    devices = json.loads(
        _run(
            "losetup",
            "--json",
            "--output",
            "NAME,BACK-FILE,DIO",
            "--associated",
            str(image),
        )
    )["loopdevices"]
    if (
        len(devices) != 1
        or devices[0]["name"] != loop
        or Path(devices[0]["back-file"]).resolve() != image.resolve()
    ):
        raise MemoryFilesystemError(
            "memory loop device does not own the recorded image"
        )
    if not devices[0]["dio"]:
        raise MemoryFilesystemError("memory backing requires direct loop I/O")
    if mounted["target"] != str(mount_root):
        _run(
            "mount",
            "-t",
            "xfs",
            "-o",
            "noatime,prjquota,discard",
            loop,
            str(mount_root),
        )
        mount_root.chmod(0o700)
    XfsMemoryQuota().validate_root(mount_root)
    mounted = _mount(mount_root)
    if mounted["target"] != str(mount_root) or mounted["source"] != loop:
        raise MemoryFilesystemError("memory filesystem did not mount at its owned root")
    space = os.statvfs(mount_root)
    if space.f_blocks * space.f_frsize < capacity:
        raise MemoryFilesystemError(
            "memory filesystem usable capacity is below its guarantee"
        )
    return record


def provision_ram_filesystem(root: Path, *, capacity_bytes: int) -> dict:
    """Retain active memory in bounded unswappable shmem on qualified workers.

    This mount reserves no RAM in advance. Per-sandbox cgroups enforce actual
    charges; admission and resident-wait policy leave host headroom. Reattaching
    an existing live mount must never resize it or hide ordinary files.
    """
    if (os.geteuid() != 0 or not root.is_absolute() or root.resolve() != root
            or type(capacity_bytes) is not int or capacity_bytes <= 0):
        raise ValueError("RAM backing requires root, a canonical path and positive capacity")
    counters = dict(line.split(":", 1) for line in Path("/proc/meminfo").read_text().splitlines())
    total = int(counters["MemTotal"].split()[0]) * 1024
    # Provider-advertised memory can exceed guest MemTotal. Bound by the guest
    # and leave five percent outside application backing for kernel/services.
    size = min(capacity_bytes, total) * 95 // 100 // 4096 * 4096
    if size < 4096:
        raise ValueError("RAM backing capacity is too small")
    root.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    _private(root.parent, directory=True)
    root.mkdir(mode=0o700, exist_ok=True)
    _private(root, directory=True)
    descriptor = os.open(root.parent / "ram-filesystem.lock",
                         os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        mounted = _mount(root)
        if mounted["target"] != str(root):
            if any(root.iterdir()):
                raise MemoryFilesystemError("refusing to cover existing application-memory files")
            _run("mount", "-t", "tmpfs", "-o", f"size={size},noswap,nodev,nosuid,mode=0700",
                 "ucloud-application-memory", str(root))
            mounted = _mount(root)
        space = os.statvfs(root)
        if (mounted["target"] != str(root) or mounted["fstype"] != "tmpfs"
                or mounted["source"] != "ucloud-application-memory"
                or "noswap" not in mounted["options"].split(",")
                or space.f_blocks * space.f_frsize != size):
            raise MemoryFilesystemError("RAM backing mount differs from its unswappable capacity contract")
        _private(root, directory=True)
        return {"schema": 1, "mount_root": str(root), "capacity_bytes": size, "noswap": True}
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mount-root", type=Path, required=True)
    parser.add_argument("--hard-capacity-bytes", type=int, required=True)
    parser.add_argument("--ram-root", type=Path)
    parser.add_argument("--ram-capacity-bytes", type=int)
    args = parser.parse_args()
    if (args.ram_root is None) != (args.ram_capacity_bytes is None):
        parser.error("RAM root and capacity must be supplied together")
    provision_memory_filesystem(
        args.mount_root, hard_capacity_bytes=args.hard_capacity_bytes
    )
    if args.ram_root is not None:
        provision_ram_filesystem(args.ram_root, capacity_bytes=args.ram_capacity_bytes)


if __name__ == "__main__":
    main()
