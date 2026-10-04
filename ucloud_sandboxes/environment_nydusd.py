"""Stock nydusd as a worker's RAFS block device (C2.1).

docs/benchmarks/nydusd-spike-2026-10-03. On with ``chunk_store.nydusd``: a
nydusd v2.4.5 built with ``--features block-nbd``, pinned by sha256. Each attached
chunk-store image gets one ``nydusd nbd`` process on the selected device,
exporting the same unified address space as environment_rafs (bootstrap, then
each blob at its mapped_blkaddr). nydusd reads the store node's virtual blobs
(chunk_store_node.VirtualBlobs) with the read token as its registry bearer,
and checks every chunk against the digests that the TOC pinned by the signed
bootstrap carries; a mismatch or a dead daemon reads as EIO, never as zeros.
Everything else (today's EROFS components) keeps the Python export.
"""
from __future__ import annotations

from contextlib import contextmanager
import errno
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import stat
import subprocess
import threading
import time
from urllib.parse import urlsplit

from .environment_nbd import CLEAR_SOCK, DISCONNECT, ReadOnlyEnvironmentDevice
from .environment_rafs import NydusdImage, RafsImage

READY_SECONDS = 30.0
# One read must never reach the kernel's NBD timeout: each expiry marks one of
# nydusd's connections dead for good, and the last one fails the device (M2
# wave 1). nydusd sets 60 s; a stalled S3 key outlasts that (Hetzner: ~2% of
# GETs stall 6-60 s, often again on retry). So nydusd retries the store node
# for up to BACKEND_TIMEOUT * (BACKEND_RETRIES + 1) = 270 s, every retry joins
# the node's in-flight fill, and the device waits NBD_TIMEOUT_SECONDS: a stall
# is latency, not EIO, and a dead store node still fails within the bound.
BACKEND_TIMEOUT, BACKEND_RETRIES, NBD_TIMEOUT_SECONDS = 30, 8, 600
NBD_SET_TIMEOUT = 0xAB09  # _IO(0xab, 9)


class NydusdFactory:
    """The backend's device factory: nydusd for RAFS images, else the Python export."""

    def __init__(self, nydusd, sha256, store_url, token, root, *, threads=4):
        digest = hashlib.sha256()
        with open(nydusd, "rb") as binary:  # Never run an unpinned daemon as root.
            while block := binary.read(1 << 20):
                digest.update(block)
        if digest.hexdigest() != sha256:
            raise ValueError("nydusd does not match its pinned sha256")
        self.nydusd, self.store_url, self.token, self.threads = nydusd, store_url.rstrip("/"), token, threads
        self.root = Path(root)
        shutil.rmtree(self.root, ignore_errors=True)  # No daemon survives a backend restart.
        self.root.mkdir(mode=0o700, parents=True)
        # One cache for every daemon, so images share layers (half the bytes
        # of a cache per image). nydusd's chunk bitmap is a shared mmap of
        # atomics and cache hits are re-validated, but a daemon starting on a
        # new blob writes its TOC and digests through one fixed temporary
        # name: a blob's first daemon starts alone, later ones need no lock.
        # (One lock for every start cost a 20-sandbox burst about 14 s, M2 wave 1.)
        self.cache = self.root / "cache"
        self._guard, self._users, self._ready, self._preparing = threading.Lock(), {}, set(), {}
        self.cache.mkdir(mode=0o700)

    def acquire(self, blobs):
        with self._guard:
            for blob in blobs:
                self._users[blob] = self._users.get(blob, 0) + 1

    def cache_bytes(self):
        """Bytes the shared cache holds on disk (its files are sparse)."""
        total = 0
        for entry in os.scandir(self.cache):
            try:
                total += entry.stat(follow_symlinks=False).st_blocks * 512
            except FileNotFoundError:
                pass  # Released meanwhile.
        return total

    @contextmanager
    def starting(self, blobs):
        """Held while a daemon starts: the locks of its blobs no daemon has
        prepared yet, in one order. Ready means prepared."""
        with self._guard:
            locks = [self._preparing.setdefault(blob, threading.Lock())
                     for blob in sorted(set(blobs) - self._ready)]
        for lock in locks:
            lock.acquire()
        try:
            yield
            with self._guard:
                self._ready.update(blobs)
        finally:
            for lock in reversed(locks):
                lock.release()

    def release(self, blobs):
        """Drop a blob's cache files once no attached image uses it."""
        with self._guard:
            for blob in blobs:
                self._users[blob] -= 1
                if not self._users[blob]:
                    del self._users[blob]
                    self._ready.discard(blob)  # Its files go: the next daemon prepares it again.
                    for path in self.cache.glob(blob + ".*"):
                        path.unlink(missing_ok=True)

    def __call__(self, device, component, cache, workers, *, trusted_keys):
        if not isinstance(component, (RafsImage, NydusdImage)):
            return ReadOnlyEnvironmentDevice(device, component, cache, workers, trusted_keys=trusted_keys)
        return NydusdDevice(device, component, self, trusted_keys=trusted_keys)


class NydusdDevice:
    def __init__(self, device: Path, component, factory, *, trusted_keys):
        component.authenticate(trusted_keys)
        if not device.is_absolute() or component.bootstrap.path is None:
            raise ValueError("nydusd needs an absolute NBD device and a bootstrap file")
        self.path, self.process, self._work, self._sysfs = device, None, None, None
        self._factory, self._blobs = factory, ()
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
            self._work.mkdir(mode=0o700, parents=True)
            # Held before the daemon starts: a closing image never deletes what this one reads.
            self._blobs = tuple(region[0] for region in component.map.regions)
            factory.acquire(self._blobs)
            url = urlsplit(factory.store_url)
            config = {"type": "bootstrap", "id": hexdigest[:16], "domain_id": "block-nbd", "config_v2": {
                "version": 2, "id": hexdigest[:16],
                "backend": {"type": "registry", "registry": {
                    "scheme": url.scheme, "host": url.netloc, "repo": f"virtual/{hexdigest}",
                    "registry_token": factory.token, "timeout": BACKEND_TIMEOUT, "connect_timeout": 5,
                    "retry_limit": BACKEND_RETRIES}},
                "cache": {"type": "filecache", "validate": True, "filecache": {"work_dir": str(factory.cache)}},
                "metadata_path": str(component.bootstrap.path)}}
            descriptor = os.open(self._work / "config.json", os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                                 0o600)
            with os.fdopen(descriptor, "w") as stream:  # Holds the read token.
                json.dump(config, stream)
            with open(self._work / "nydusd.log", "ab") as log, factory.starting(self._blobs):
                self.process = subprocess.Popen(
                    [factory.nydusd, "nbd", str(device), "--config", str(self._work / "config.json"),
                     "--threads", str(factory.threads), "--log-level", "warn"],
                    stdin=subprocess.DEVNULL, stdout=log, stderr=log, start_new_session=True)
                self._await_ready(component.image_size)
            fcntl.ioctl(self._fd, NBD_SET_TIMEOUT, NBD_TIMEOUT_SECONDS)  # After nydusd's own 60 s.
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
        if self._blobs:
            self._factory.release(self._blobs)
            self._blobs = ()
