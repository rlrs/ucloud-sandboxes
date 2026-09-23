"""Read-only Linux NBD transport for already-authenticated environment bytes.

This adapter is owned by the long-lived storage process, not the node agent.
There is no network listener, writable export, or independent storage authority.
"""
from concurrent.futures import CancelledError, ThreadPoolExecutor
import errno
import fcntl
import os
from pathlib import Path
import socket
import stat
import struct
import time
from threading import BoundedSemaphore, Event, Lock, Thread

REQUEST = struct.Struct("!II8sQI")
REPLY = struct.Struct("!II8s")
REQUEST_MAGIC = 0x25609513
REPLY_MAGIC = 0x67446698
MAX_READ = 32 * 1024 ** 2
# Linux UAPI _IO(0xab, command), from linux/nbd.h.
SET_SOCK, SET_BLKSIZE, SET_SIZE, DO_IT, CLEAR_SOCK = (0xab00 + n for n in range(5))
DISCONNECT, SET_TIMEOUT, SET_FLAGS = 0xab08, 0xab09, 0xab0a
READ_ONLY_FLAGS = 1 | 2


class EnvironmentReadWorkers:
    """One bounded read execution pool shared by all immutable components."""
    def __init__(self, concurrency=16):
        self._slots = BoundedSemaphore(concurrency)
        self._pool = ThreadPoolExecutor(max_workers=concurrency, thread_name_prefix="environment-read")

    def submit(self, function, cancel):
        while not self._slots.acquire(timeout=.05):
            if cancel.is_set():
                raise CancelledError()
        try:
            future = self._pool.submit(function)
        except BaseException:
            self._slots.release()
            raise
        future.add_done_callback(lambda _done: self._slots.release())
        return future

    def close(self):
        self._pool.shutdown(wait=True, cancel_futures=True)


class ReadOnlyNbdExport:
    def __init__(self, stream, component, cache, workers):
        self.stream, self.component, self.cache, self.workers = stream, component, cache, workers
        self.cancel = Event()
        self._writes = Lock()
        self._thread = Thread(target=self._serve, name="environment-nbd", daemon=True)
        self._thread.start()

    def _receive(self, count):
        chunks = []
        while count:
            data = self.stream.recv(count)
            if not data:
                raise EOFError()
            chunks.append(data)
            count -= len(data)
        return b"".join(chunks)

    def _reply(self, handle, error, data=b""):
        with self._writes:
            if not self.cancel.is_set():
                self.stream.sendall(REPLY.pack(REPLY_MAGIC, error, handle) + data)

    def _read(self, handle, offset, length):
        try:
            data = self.cache.read(self.component, offset, length, cancel=self.cancel)
            self._reply(handle, 0, data)
        except (OSError, ValueError, CancelledError):
            try:
                self._reply(handle, errno.EIO)
            except OSError:
                pass

    def _serve(self):
        try:
            while not self.cancel.is_set():
                magic, command, handle, offset, length = REQUEST.unpack(self._receive(REQUEST.size))
                if magic != REQUEST_MAGIC or length > MAX_READ:
                    break
                if command == 2:  # disconnect has no reply
                    break
                if command & 0xffff == 1:  # Drain bounded write payload; never apply it.
                    remaining = length
                    while remaining:
                        count = min(remaining, 64 * 1024)
                        self._receive(count)
                        remaining -= count
                    self._reply(handle, errno.EROFS)
                elif command != 0:
                    self._reply(handle, errno.EOPNOTSUPP)
                elif offset + length > self.component.image_size:
                    self._reply(handle, errno.EINVAL)
                else:
                    self.workers.submit(lambda h=handle, o=offset, n=length: self._read(h, o, n), self.cancel)
        except (OSError, EOFError, CancelledError):
            pass
        finally:
            self.cancel.set()
            try:
                self.stream.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

    def close(self):
        self.cancel.set()
        try:
            self.stream.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self._thread.join(5)
        if self._thread.is_alive():
            raise RuntimeError("environment NBD reader did not stop")
        self.stream.close()


class ReadOnlyEnvironmentDevice:
    """An owned kernel export; authenticate before invoking privileged ioctls."""
    def __init__(self, device: Path, component, cache, workers, *, trusted_keys):
        component.authenticate(trusted_keys)
        if not device.is_absolute():
            raise ValueError("environment block device path must be absolute")
        self.path = device
        self._fd = os.open(device, os.O_RDWR | os.O_NOFOLLOW)
        self._kernel_socket = self._export = self._kernel_thread = None
        self._bound = False
        try:
            device_info = os.fstat(self._fd)
            if not stat.S_ISBLK(device_info.st_mode) or os.major(device_info.st_rdev) != 43:
                raise ValueError("environment export requires a real Linux NBD device")
            # SET_SOCK permits multi-connection attachment, so it is NOT an
            # ownership fence. Hold the device inode lock for the export's
            # entire lifetime and reject any existing kernel owner first.
            try:
                fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise OSError(errno.EBUSY, "environment NBD device is leased") from exc
            owner_path = Path("/sys/dev/block") / f"{os.major(device_info.st_rdev)}:{os.minor(device_info.st_rdev)}" / "pid"
            try:
                if owner_path.read_text().strip():
                    raise OSError(errno.EBUSY, "NBD device has an existing kernel owner")
            except FileNotFoundError:
                pass
            kernel_socket, stream = socket.socketpair()
            self._kernel_socket = kernel_socket
            try:
                # This node's dedicated artifact-device pool uses the held
                # inode lease; never reconfigure a device with a kernel owner.
                fcntl.ioctl(self._fd, SET_SOCK, kernel_socket.fileno())
                self._bound = True
                fcntl.ioctl(self._fd, SET_BLKSIZE, 4096)
                fcntl.ioctl(self._fd, SET_SIZE, component.image_size)
                fcntl.ioctl(self._fd, SET_FLAGS, READ_ONLY_FLAGS)
                fcntl.ioctl(self._fd, SET_TIMEOUT, 30)
                self._export = ReadOnlyNbdExport(stream, component, cache, workers)
                self._kernel_thread = Thread(target=self._run_kernel, name="environment-nbd-kernel", daemon=True)
                self._kernel_thread.start()
                self._await_ready(owner_path.parent, component.image_size)
            except BaseException:
                stream.close()
                raise
        except BaseException:
            self.close()
            raise

    @property
    def healthy(self):
        return (self._kernel_thread is not None and self._kernel_thread.is_alive()
                and self._export is not None and not self._export.cancel.is_set())

    def _await_ready(self, sysfs_device, image_size):
        # SET_SIZE does not publish capacity synchronously on every kernel.
        # Mounting before DO_IT finishes setup can read a zero-sized device.
        deadline = time.monotonic() + 5
        while True:
            try:
                ready = (int((sysfs_device / "size").read_text()) * 512 == image_size
                         and bool((sysfs_device / "pid").read_text().strip()))
            except (OSError, ValueError):
                ready = False
            if ready and self.healthy:
                return
            if not self.healthy or time.monotonic() >= deadline:
                raise RuntimeError("environment NBD export did not become ready")
            self._export.cancel.wait(.005)

    def _run_kernel(self):
        try:
            fcntl.ioctl(self._fd, DO_IT)
        except OSError:
            pass  # Disconnect is expected; failed reads already return EIO.

    def close(self):
        if self._export is not None:
            self._export.close()
            self._export = None
        if self._fd is not None and self._bound:
            try:
                fcntl.ioctl(self._fd, DISCONNECT)
            except OSError:
                pass
        if self._kernel_thread is not None:
            self._kernel_thread.join(5)
            if self._kernel_thread.is_alive():
                raise RuntimeError("environment kernel export did not disconnect")
            self._kernel_thread = None
        if self._fd is not None:
            try:
                if self._bound:
                    fcntl.ioctl(self._fd, CLEAR_SOCK)
            finally:
                os.close(self._fd)
                self._fd = None
                self._bound = False
        if self._kernel_socket is not None:
            self._kernel_socket.close()
            self._kernel_socket = None
