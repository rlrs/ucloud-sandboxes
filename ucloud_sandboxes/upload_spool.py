"""Stage uploads on local disk without retaining their payloads in Python RAM."""

from contextlib import contextmanager
import errno
import os
from pathlib import Path
from tempfile import TemporaryFile
from threading import Lock
from typing import BinaryIO, Iterator

from .http_server import TRANSFER_CHUNK_BYTES
from .sandbox import SandboxStartupBusyError


class UploadSpool:
    def __init__(self, directory: Path, *, min_free_bytes: int = 1024**3) -> None:
        self.directory = directory
        self.min_free_bytes = min_free_bytes
        self._lock = Lock()
        self._unwritten_bytes = 0

    @contextmanager
    def receive(self, source: BinaryIO, length: int) -> Iterator[BinaryIO]:
        if length < 0:
            raise ValueError("upload length cannot be negative")
        self.directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        with self._lock:
            space = os.statvfs(self.directory)
            available = space.f_bavail * space.f_frsize
            # Already-written bytes are reflected in statvfs. Reserve only the
            # bytes still to come, so concurrent receivers cannot promise the
            # same free space. Keep room for node metadata and cleanup.
            if available - self._unwritten_bytes - length < self.min_free_bytes:
                raise SandboxStartupBusyError("node upload staging is waiting for disk space")
            self._unwritten_bytes += length
        remaining = length
        staged = None
        try:
            try:
                # TemporaryFile is unlinked: disconnect, exceptions and process
                # exit all reclaim the staging file without a cleanup daemon.
                staged = TemporaryFile(dir=self.directory, mode="w+b", buffering=0)
                while remaining:
                    chunk = source.read(min(remaining, TRANSFER_CHUNK_BYTES))
                    if not chunk:
                        raise ValueError("request body ended before Content-Length bytes were read")
                    pending = memoryview(chunk)
                    while pending:
                        written = staged.write(pending)
                        if not written:
                            raise OSError(errno.EIO, "upload staging write made no progress")
                        pending = pending[written:]
                    with self._lock:
                        self._unwritten_bytes -= len(chunk)
                        remaining -= len(chunk)
                staged.seek(0)
            except OSError as exc:
                if exc.errno in {errno.ENOSPC, errno.EDQUOT}:
                    raise SandboxStartupBusyError("node upload staging is waiting for disk space") from exc
                raise
            # Never reclassify an exception after command dispatch as a safe
            # admission retry. Only the receive phase above can do that.
            yield staged
        finally:
            try:
                if staged is not None:
                    staged.close()
            finally:
                with self._lock:
                    self._unwritten_bytes -= remaining
