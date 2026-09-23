"""Bounded disposable cache of independently authenticated environment chunks."""
from collections import OrderedDict
from concurrent.futures import CancelledError, Future, ThreadPoolExecutor, TimeoutError as FutureTimeout
from dataclasses import dataclass
from http.client import IncompleteRead
import math
import os
from pathlib import Path
import stat
from ssl import SSLCertVerificationError
import tempfile
from threading import Condition, Event
import time
from urllib.error import URLError

from .environment_artifact import CHUNK_BYTES, content_digest
from .managed_registry import RegistryRequestError


@dataclass
class _ChunkFetch:
    cancel: Event
    future: Future[bytes] | None = None
    readers: int = 0


class VerifiedEnvironmentCache:
    def __init__(self, root: Path, registry, *, max_bytes=1024 ** 3, concurrent_misses=8,
                 fetch_timeout_seconds=30.0):
        if not root.is_absolute() or max_bytes < CHUNK_BYTES or concurrent_misses < 1:
            raise ValueError("invalid environment cache bounds")
        if not math.isfinite(fetch_timeout_seconds) or fetch_timeout_seconds <= 0:
            raise ValueError("environment fetch timeout must be positive and finite")
        root.mkdir(mode=0o700, parents=True, exist_ok=True)
        info = root.lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o022:
            raise ValueError("environment cache must be a private owned directory")
        self.root, self.registry, self.max_bytes = root, registry, max_bytes
        self._guard = Condition()
        self._concurrent_misses = concurrent_misses
        self._fetch_timeout_seconds = fetch_timeout_seconds
        self._executor = ThreadPoolExecutor(max_workers=concurrent_misses, thread_name_prefix="environment-fetch")
        self._closed = False
        self._pending = {}
        self._lru = OrderedDict()
        self._bytes = 0
        self._metrics = {"hits": 0, "misses": 0, "downloaded_bytes": 0, "corruptions": 0, "fetch_retries": 0}
        # Cache files are disposable, not authority. Rebuild bounded LRU metadata;
        # actual bytes are authenticated on every use, including after restart.
        for path in sorted(root.iterdir(), key=lambda p: p.name):
            if len(path.name) != 64 or any(c not in "0123456789abcdef" for c in path.name):
                continue
            info = path.lstat()
            if stat.S_ISREG(info.st_mode) and info.st_uid == os.geteuid():
                self._lru[path.name] = info.st_size
                self._bytes += info.st_size
        self._evict()

    @staticmethod
    def _cancelled(cancel):
        if cancel is not None and cancel.is_set():
            raise CancelledError("environment read cancelled")

    def _evict(self):
        while self._bytes > self.max_bytes and self._lru:
            name, size = self._lru.popitem(last=False)
            self._bytes -= size
            try:
                (self.root / name).unlink()
            except FileNotFoundError:
                pass

    def _cached(self, chunk):
        path = self.root / chunk.digest.removeprefix("sha256:")
        try:
            descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        except FileNotFoundError:
            return None
        with os.fdopen(descriptor, "rb") as source:
            info = os.fstat(source.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_size != chunk.size:
                data = b""
            else:
                data = source.read(chunk.size + 1)
        if len(data) != chunk.size or content_digest(data) != chunk.digest:
            with self._guard:
                self._metrics["corruptions"] += 1
                self._bytes -= self._lru.pop(path.name, 0)
                path.unlink(missing_ok=True)
            return None
        with self._guard:
            self._metrics["hits"] += 1
            if path.name in self._lru:
                self._lru.move_to_end(path.name)
        return data

    def chunk(self, chunk, *, cancel=None):
        self._cancelled(cancel)
        with self._guard:
            if self._closed:
                raise CancelledError("environment cache is closed")
        data = self._cached(chunk)
        if data is not None:
            return data
        with self._guard:
            while True:
                self._cancelled(cancel)
                if self._closed:
                    raise CancelledError("environment cache is closed")
                pending = self._pending.get(chunk.digest)
                if pending is not None and not pending.cancel.is_set():
                    pending.readers += 1
                    break
                if pending is None and len(self._pending) < self._concurrent_misses:
                    pending = _ChunkFetch(Event(), readers=1)
                    pending.future = self._executor.submit(self._fetch, chunk, pending.cancel)
                    self._pending[chunk.digest] = pending
                    pending.future.add_done_callback(lambda done: self._finished(chunk.digest, done))
                    break
                self._guard.wait(.05)
        # Cancelling one sandbox's read must not cancel a shared miss needed by
        # another sandbox. At most concurrent_misses HTTP operations survive,
        # each uses the fetch retry budget and existing registry socket timeout.
        assert pending.future is not None
        try:
            while True:
                self._cancelled(cancel)
                try:
                    data = pending.future.result(timeout=.05)
                    if len(data) != chunk.size:
                        raise ValueError("shared chunk has inconsistent size")
                    return data
                except FutureTimeout:
                    if pending.future.done():
                        raise
        finally:
            with self._guard:
                pending.readers -= 1
                if pending.readers == 0:
                    # A cancelled sole reader cannot keep retrying useless work.
                    # A sibling reader retains the same shared fetch obligation.
                    pending.cancel.set()

    def _finished(self, digest, pending):
        with self._guard:
            entry = self._pending.get(digest)
            if entry is not None and entry.future is pending:
                self._pending.pop(digest)
            self._guard.notify_all()

    def _fetch(self, chunk, cancel):
        deadline = time.monotonic() + self._fetch_timeout_seconds
        backoff = .05
        while True:
            self._cancelled(cancel)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("immutable environment fetch deadline exceeded")
            try:
                data = self.registry.client.blob_bytes(
                    self.registry.repository, chunk.digest, max_bytes=chunk.size,
                    timeout_seconds=remaining)
                break
            except RegistryRequestError as exc:
                if exc.status_code not in {408, 429, 500, 502, 503, 504}:
                    raise
            except SSLCertVerificationError:
                raise
            except URLError as exc:
                if isinstance(exc.reason, SSLCertVerificationError):
                    raise
            except (OSError, IncompleteRead):
                pass
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("immutable environment fetch deadline exceeded")
            if cancel.wait(min(backoff, remaining)):
                raise CancelledError("environment fetch cancelled")
            self._cancelled(cancel)
            if time.monotonic() >= deadline:
                raise TimeoutError("immutable environment fetch deadline exceeded")
            with self._guard:
                self._metrics["fetch_retries"] += 1
            backoff = min(1.0, backoff * 2)
        self._cancelled(cancel)
        if time.monotonic() >= deadline:
            raise TimeoutError("immutable environment fetch deadline exceeded")
        if len(data) != chunk.size or content_digest(data) != chunk.digest:
            raise ValueError("environment chunk content identity mismatch")
        descriptor, name = tempfile.mkstemp(prefix=".chunk-", dir=self.root)
        temporary = Path(name)
        try:
            with os.fdopen(descriptor, "wb") as target:
                target.write(data)
            temporary.chmod(0o400)
            destination = self.root / chunk.digest.removeprefix("sha256:")
            with self._guard:
                self._cancelled(cancel)
                if time.monotonic() >= deadline:
                    raise TimeoutError("immutable environment fetch deadline exceeded")
                temporary.replace(destination)
                self._bytes -= self._lru.pop(destination.name, 0)
                self._lru[destination.name] = len(data)
                self._bytes += len(data)
                self._metrics["misses"] += 1
                self._metrics["downloaded_bytes"] += len(data)
                self._evict()
            return data
        finally:
            temporary.unlink(missing_ok=True)

    def close(self):
        with self._guard:
            self._closed = True
            for pending in self._pending.values():
                pending.cancel.set()
            self._guard.notify_all()
        self._executor.shutdown(wait=True, cancel_futures=True)

    def read(self, component, offset, length, *, cancel=None):
        if (type(offset) is not int or type(length) is not int or offset < 0
                or length < 0 or length > 32 * 1024 ** 2 or offset + length > component.image_size):
            raise ValueError("environment read exceeds its authenticated bounds")
        result = []
        end = offset + length
        while offset < end:
            index, within = divmod(offset, CHUNK_BYTES)
            data = self.chunk(component.chunks[index], cancel=cancel)
            take = min(len(data) - within, end - offset)
            result.append(data[within:within + take])
            offset += take
        self._cancelled(cancel)
        return b"".join(result)

    def metrics(self):
        with self._guard:
            return self._metrics | {"cached_bytes": self._bytes, "pending_misses": len(self._pending)}
