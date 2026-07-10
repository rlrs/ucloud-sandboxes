from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import fcntl
import hashlib
import math
import os
from pathlib import Path
import re
import stat
from threading import RLock, get_ident
import time
from typing import BinaryIO, Collection, Iterator


_DIGEST_RE = re.compile(r"sha256:([0-9a-f]{64})")
_BLOB_NAME_RE = re.compile(r"[0-9a-f]{64}")
_LOCAL_LOCKS_GUARD = RLock()
_LOCAL_LOCKS: dict[Path, RLock] = {}


@dataclass(frozen=True)
class BlobGCResult:
    removed_entries: int
    removed_bytes: int
    remaining_entries: int
    remaining_bytes: int


@dataclass(frozen=True)
class _BlobEntry:
    digest: str
    path: Path
    size: int
    mtime: float
    mtime_ns: int


class BuildContextBlobStore:
    """A small, process-safe store for content-addressed build archives."""

    def __init__(
        self,
        root: Path,
        *,
        max_blob_bytes: int,
        max_total_bytes: int | None = None,
        max_entries: int | None = None,
        max_age_seconds: float | None = None,
    ) -> None:
        self.root = Path(root)
        self.blob_dir = self.root / "sha256"
        self.max_blob_bytes = _nonnegative_int(max_blob_bytes, "max_blob_bytes")
        self.max_total_bytes = _optional_nonnegative_int(
            max_total_bytes,
            "max_total_bytes",
        )
        self.max_entries = _optional_nonnegative_int(max_entries, "max_entries")
        self.max_age_seconds = _optional_nonnegative_number(
            max_age_seconds,
            "max_age_seconds",
        )
        self.blob_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        self._lock_path = self.root / ".store.lock"

    def path(self, digest: str) -> Path:
        return self.blob_dir / _digest_hex(digest)

    def put(
        self,
        digest: str,
        reader: BinaryIO,
        *,
        content_length: int,
    ) -> Path:
        digest_hex = _digest_hex(digest)
        length = _nonnegative_int(content_length, "content_length")
        if length > self.max_blob_bytes:
            raise ValueError(
                f"build context is {length} bytes; limit is {self.max_blob_bytes}"
            )

        target = self.blob_dir / digest_hex
        temporary = self.blob_dir / (
            f".{digest_hex}.{os.getpid()}.{get_ident()}.{time.monotonic_ns()}.tmp"
        )
        descriptor = os.open(
            temporary,
            os.O_CREAT | os.O_EXCL | os.O_WRONLY,
            0o600,
        )
        hasher = hashlib.sha256()
        try:
            remaining = length
            while remaining:
                chunk = reader.read(min(1024 * 1024, remaining))
                if not chunk:
                    raise ValueError(
                        f"build context ended early with {remaining} bytes missing"
                    )
                if not isinstance(chunk, (bytes, bytearray, memoryview)):
                    raise TypeError("build context reader must return bytes")
                if len(chunk) > remaining:
                    raise ValueError("build context contains data past Content-Length")
                hasher.update(chunk)
                _write_all(descriptor, chunk)
                remaining -= len(chunk)

            trailing = reader.read(1)
            if trailing:
                raise ValueError("build context contains data past Content-Length")
            if not isinstance(trailing, (bytes, bytearray, memoryview)):
                raise TypeError("build context reader must return bytes")
            actual_digest = hasher.hexdigest()
            if actual_digest != digest_hex:
                raise ValueError(
                    "build context digest mismatch: "
                    f"expected sha256:{digest_hex}, got sha256:{actual_digest}"
                )

            os.fsync(descriptor)
            os.close(descriptor)
            descriptor = -1
            with self._hold_lock():
                if target.exists() and _file_matches_digest(target, digest_hex):
                    os.chmod(target, 0o600)
                    return target
                os.replace(temporary, target)
                os.chmod(target, 0o600)
                _fsync_directory(self.blob_dir)
            return target
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass

    def open(self, digest: str) -> BinaryIO:
        path = self.path(digest)
        with self._hold_lock():
            flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
            descriptor = os.open(path, flags)
            try:
                if not stat.S_ISREG(os.fstat(descriptor).st_mode):
                    raise FileNotFoundError(path)
                return os.fdopen(descriptor, "rb")
            except Exception:
                os.close(descriptor)
                raise

    def size(self, digest: str) -> int:
        path = self.path(digest)
        with self._hold_lock():
            return _regular_stat(path).st_size

    def touch(self, digest: str) -> None:
        path = self.path(digest)
        with self._hold_lock():
            _regular_stat(path)
            os.utime(path, follow_symlinks=False)

    def gc(
        self,
        *,
        protected: Collection[str] = (),
        now: float | None = None,
    ) -> BlobGCResult:
        protected_hex = {_digest_hex(digest) for digest in protected}
        current_time = time.time() if now is None else float(now)
        removed_entries = 0
        removed_bytes = 0

        with self._hold_lock():
            entries = self._entries_unlocked()
            survivors: list[_BlobEntry] = []
            for entry in entries:
                expired = (
                    self.max_age_seconds is not None
                    and entry.mtime < current_time - self.max_age_seconds
                )
                if entry.digest not in protected_hex and expired:
                    if _unlink_regular(entry.path):
                        removed_entries += 1
                        removed_bytes += entry.size
                    continue
                survivors.append(entry)

            total_bytes = sum(entry.size for entry in survivors)
            total_entries = len(survivors)
            candidates = sorted(
                (entry for entry in survivors if entry.digest not in protected_hex),
                key=lambda entry: (entry.mtime_ns, entry.digest),
            )
            for entry in candidates:
                over_bytes = (
                    self.max_total_bytes is not None
                    and total_bytes > self.max_total_bytes
                )
                over_entries = (
                    self.max_entries is not None and total_entries > self.max_entries
                )
                if not over_bytes and not over_entries:
                    break
                if _unlink_regular(entry.path):
                    removed_entries += 1
                    removed_bytes += entry.size
                    total_entries -= 1
                    total_bytes -= entry.size

            if removed_entries:
                _fsync_directory(self.blob_dir)
            remaining = self._entries_unlocked()

        return BlobGCResult(
            removed_entries=removed_entries,
            removed_bytes=removed_bytes,
            remaining_entries=len(remaining),
            remaining_bytes=sum(entry.size for entry in remaining),
        )

    def _entries_unlocked(self) -> list[_BlobEntry]:
        entries: list[_BlobEntry] = []
        for path in self.blob_dir.iterdir():
            if _BLOB_NAME_RE.fullmatch(path.name) is None:
                continue
            try:
                file_stat = path.stat(follow_symlinks=False)
            except FileNotFoundError:
                continue
            if not stat.S_ISREG(file_stat.st_mode):
                continue
            entries.append(
                _BlobEntry(
                    digest=path.name,
                    path=path,
                    size=file_stat.st_size,
                    mtime=file_stat.st_mtime,
                    mtime_ns=file_stat.st_mtime_ns,
                )
            )
        return entries

    @contextmanager
    def _hold_lock(self) -> Iterator[None]:
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        resolved = self._lock_path.resolve()
        with _LOCAL_LOCKS_GUARD:
            local_lock = _LOCAL_LOCKS.get(resolved)
            if local_lock is None:
                local_lock = RLock()
                _LOCAL_LOCKS[resolved] = local_lock
        with local_lock:
            with self._lock_path.open("a+b") as lock_file:
                os.chmod(self._lock_path, 0o600)
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
                try:
                    yield
                finally:
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def _digest_hex(digest: str) -> str:
    if not isinstance(digest, str):
        raise ValueError("build context digest must be a string")
    match = _DIGEST_RE.fullmatch(digest)
    if match is None:
        raise ValueError("build context digest must be normalized sha256:<64hex>")
    return match.group(1)


def _nonnegative_int(value: int, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{name} must be a non-negative integer")
    return value


def _optional_nonnegative_int(value: int | None, name: str) -> int | None:
    if value is None:
        return None
    return _nonnegative_int(value, name)


def _optional_nonnegative_number(
    value: float | None,
    name: str,
) -> float | None:
    if value is None:
        return None
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value < 0
    ):
        raise ValueError(f"{name} must be a non-negative number")
    return float(value)


def _write_all(descriptor: int, payload: bytes | bytearray | memoryview) -> None:
    remaining = memoryview(payload)
    while remaining:
        written = os.write(descriptor, remaining)
        if written <= 0:
            raise OSError("failed to write build context")
        remaining = remaining[written:]


def _file_matches_digest(path: Path, digest_hex: str) -> bool:
    try:
        file_stat = path.stat(follow_symlinks=False)
    except FileNotFoundError:
        return False
    if not stat.S_ISREG(file_stat.st_mode):
        return False
    hasher = hashlib.sha256()
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError:
        return False
    with os.fdopen(descriptor, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest() == digest_hex


def _regular_stat(path: Path) -> os.stat_result:
    file_stat = path.stat(follow_symlinks=False)
    if not stat.S_ISREG(file_stat.st_mode):
        raise FileNotFoundError(path)
    return file_stat


def _unlink_regular(path: Path) -> bool:
    try:
        file_stat = path.stat(follow_symlinks=False)
    except FileNotFoundError:
        return False
    if not stat.S_ISREG(file_stat.st_mode):
        return False
    try:
        path.unlink()
    except FileNotFoundError:
        return False
    return True


def _fsync_directory(path: Path) -> None:
    try:
        descriptor = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        try:
            os.fsync(descriptor)
        except OSError:
            pass
    finally:
        os.close(descriptor)
