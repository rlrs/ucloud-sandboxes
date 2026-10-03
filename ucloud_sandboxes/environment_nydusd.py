"""Spike: stock nydusd as a worker's RAFS block device (C2.1 candidate).

docs/benchmarks/nydusd-spike-2026-10-03. Opt-in with UCLOUD_ENVIRONMENT_NYDUSD
naming a nydusd v2.4.5 built with ``--features block-nbd``. Each attached
chunk-store image gets one ``nydusd nbd`` process on the selected device,
exporting the same unified address space as environment_rafs (bootstrap, then
each blob at its mapped_blkaddr). nydusd reads the store node's virtual blobs
(chunk_store_node.VirtualBlobs) with the read token as its registry bearer,
and checks every chunk against the digests that the TOC pinned by the signed
bootstrap carries; a mismatch or a dead daemon reads as EIO, never as zeros.
Everything else (today's EROFS components) keeps the Python export.
"""
from __future__ import annotations

import errno
import fcntl
import json
import os
from pathlib import Path
import shutil
import signal
import stat
import subprocess
import time
from urllib.parse import urlsplit

from .environment_nbd import CLEAR_SOCK, DISCONNECT, ReadOnlyEnvironmentDevice
from .environment_rafs import RafsImage

READY_SECONDS = 30.0


class NydusdFactory:
    """The backend's device factory: nydusd for RAFS images, else the Python export."""

    def __init__(self, nydusd, store_url, token, root, *, threads=4):
        self.nydusd, self.store_url, self.token, self.threads = nydusd, store_url.rstrip("/"), token, threads
        self.root = Path(root)
        shutil.rmtree(self.root, ignore_errors=True)  # No daemon survives a backend restart.
        self.root.mkdir(mode=0o700, parents=True)

    def __call__(self, device, component, cache, workers, *, trusted_keys):
        if not isinstance(component, RafsImage):
            return ReadOnlyEnvironmentDevice(device, component, cache, workers, trusted_keys=trusted_keys)
        return NydusdDevice(device, component, self, trusted_keys=trusted_keys)


class NydusdDevice:
    def __init__(self, device: Path, component, factory, *, trusted_keys):
        component.authenticate(trusted_keys)
        if not device.is_absolute() or component.bootstrap.path is None:
            raise ValueError("nydusd needs an absolute NBD device and a bootstrap file")
        self.path, self.process, self._work, self._sysfs = device, None, None, None
        self._fd = os.open(device, os.O_RDONLY | os.O_NOFOLLOW)
        try:
            info = os.fstat(self._fd)
            if not stat.S_ISBLK(info.st_mode) or os.major(info.st_rdev) != 43:
                raise ValueError("nydusd export requires a real Linux NBD device")
            try:  # The same inode lease as the Python export: one owner per device.
                fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise OSError(errno.EBUSY, "environment NBD device is leased") from exc
            self._sysfs = Path("/sys/dev/block") / f"{os.major(info.st_rdev)}:{os.minor(info.st_rdev)}"
            if self._owner():
                raise OSError(errno.EBUSY, "NBD device has an existing kernel owner")
            hexdigest = component.digest[7:]
            self._work = factory.root / f"{hexdigest}-{device.name}"
            shutil.rmtree(self._work, ignore_errors=True)
            (self._work / "cache").mkdir(mode=0o700, parents=True)
            url = urlsplit(factory.store_url)
            config = {"type": "bootstrap", "id": hexdigest[:16], "domain_id": "block-nbd", "config_v2": {
                "version": 2, "id": hexdigest[:16],
                "backend": {"type": "registry", "registry": {
                    "scheme": url.scheme, "host": url.netloc, "repo": f"virtual/{hexdigest}",
                    "registry_token": factory.token, "timeout": 30, "connect_timeout": 5, "retry_limit": 2}},
                "cache": {"type": "filecache", "validate": True, "filecache": {"work_dir": str(self._work / "cache")}},
                "metadata_path": str(component.bootstrap.path)}}
            descriptor = os.open(self._work / "config.json", os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                                 0o600)
            with os.fdopen(descriptor, "w") as stream:  # Holds the read token.
                json.dump(config, stream)
            with open(self._work / "nydusd.log", "ab") as log:
                self.process = subprocess.Popen(
                    [factory.nydusd, "nbd", str(device), "--config", str(self._work / "config.json"),
                     "--threads", str(factory.threads), "--log-level", "warn"],
                    stdin=subprocess.DEVNULL, stdout=log, stderr=log, start_new_session=True)
            self._await_ready(component.image_size)
        except BaseException:
            self.close()
            raise

    def _owner(self):
        if self._sysfs is None:
            return ""
        try:
            return (self._sysfs / "pid").read_text().strip()
        except FileNotFoundError:
            return ""

    def _await_ready(self, image_size):
        deadline = time.monotonic() + READY_SECONDS
        while True:
            try:
                if int((self._sysfs / "size").read_text()) * 512 >= image_size and self._owner():
                    return
            except (OSError, ValueError):
                pass
            if self.process.poll() is not None or time.monotonic() >= deadline:
                log = (self._work / "nydusd.log").read_bytes()[-400:].decode("utf-8", "replace")
                raise RuntimeError(f"nydusd export did not become ready: {log}")
            time.sleep(.005)

    @property
    def healthy(self):
        return self.process is not None and self.process.poll() is None and bool(self._owner())

    def close(self):
        if self.process is not None:
            if self.process.poll() is None:
                self.process.send_signal(signal.SIGTERM)  # nydusd disconnects its device on SIGTERM.
                try:
                    self.process.wait(10)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    self.process.wait(5)
            self.process = None
        if self._fd is not None:
            if self._owner():  # A daemon that died mid-setup: never leave the device bound.
                for command in (DISCONNECT, CLEAR_SOCK):
                    try:
                        fcntl.ioctl(self._fd, command)
                    except OSError:
                        pass
            os.close(self._fd)
            self._fd = None
        if self._work is not None:
            shutil.rmtree(self._work, ignore_errors=True)
            self._work = None
