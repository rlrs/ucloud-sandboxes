"""Conservative OCI diff materialization for small, partially cached images.

This accepts only layers whose filesystem view can be produced without looking
through a lower layer. Everything else remains the Docker adapter's job. Never
use tarfile.extract/extractall on these untrusted archives.
"""
from contextlib import nullcontext
import errno
import gzip
import hashlib
import os
from pathlib import Path
import shutil
import tarfile
import time
import zlib

from .build_deadline import remaining_build_execution_seconds
from .environment_artifact import require_digest


FALLBACK_REASONS = frozenset({
    "unsupported", "layer_set", "descriptors", "media_type", "compressed_budget",
    "unpacked_budget", "scratch_root", "member_limit", "member_path", "whiteout",
    "member_type", "member_metadata", "parent_context", "hardlink_context",
    "collection_budget", "request_budget", "io", "archive",
})


class UnsupportedLayer(ValueError):
    """A valid image may need Docker; reason is bounded, non-payload metadata."""

    def __init__(self, *args, reason="unsupported"):
        if reason not in FALLBACK_REASONS:
            raise ValueError("invalid selective layer fallback reason")
        super().__init__(*args)
        self.reason = reason


GZIP_TYPES = frozenset({"application/vnd.oci.image.layer.v1.tar+gzip",
                        "application/vnd.docker.image.rootfs.diff.tar.gzip"})
TAR_TYPES = frozenset({"application/vnd.oci.image.layer.v1.tar",
                       "application/vnd.docker.image.rootfs.diff.tar"})
MAX_MEMBERS = 50_000
BLOCK = 1024 * 1024
# Bound aggregate selected input, not each layer separately. Real dependency
# updates can exceed the old 128-MiB/1-GiB limits despite cached base components.
# The compressed spool is eliminated; publication admission bounds concurrency.
MAX_COMPRESSED_BYTES = 512 * 1024**2
MAX_UNPACKED_BYTES = 2 * 1024**3


def _path(value, *, root=False):
    if not isinstance(value, str) or len(value) > 4096 or "\0" in value:
        raise UnsupportedLayer("unsupported OCI member path", reason="member_path")
    # Match PurePosixPath's removal of empty and '.' components without
    # allocating path objects for every member. Absolute paths and every '..'
    # component remain forbidden before any destination path is constructed.
    parts = tuple(part for part in value.split('/') if part and part != '.')
    if value.startswith('/') or ".." in parts:
        raise UnsupportedLayer("OCI member escapes its diff directory", reason="member_path")
    if not parts:
        if root:
            return ()
        raise UnsupportedLayer("OCI member has an empty path", reason="member_path")
    if any(part.startswith('.wh.') for part in parts):
        raise UnsupportedLayer("OCI whiteouts require lower-layer context", reason="whiteout")
    return parts


def _members(archive):
    entries = {}
    for member in archive:
        if len(entries) >= MAX_MEMBERS:
            raise UnsupportedLayer("too many OCI members for selective materialization", reason="member_limit")
        parts = _path(member.name, root=member.isdir())
        if parts in entries:
            raise UnsupportedLayer("duplicate OCI member path", reason="member_path")
        if (member.issparse() or not (member.isdir() or member.isreg() or member.issym() or member.islnk())
                or any(key not in {'path', 'linkpath', 'mtime', 'atime', 'ctime', 'size', 'uid', 'gid',
                                   'uname', 'gname'} for key in member.pax_headers)):
            raise UnsupportedLayer("unsupported OCI member type or extended metadata", reason="member_type")
        if (type(member.uid) is not int or type(member.gid) is not int
                or not 0 <= member.uid < 2**32 - 1 or not 0 <= member.gid < 2**32 - 1
                or member.mode & ~0o7777 or member.size < 0):
            raise UnsupportedLayer("unsupported OCI member metadata", reason="member_metadata")
        if member.issym() and ("\0" in member.linkname or len(member.linkname) > 4096):
            raise UnsupportedLayer("unsupported OCI symlink", reason="member_metadata")
        entries[parts] = member
    for parts, member in entries.items():
        for depth in range(1, len(parts)):
            parent = entries.get(parts[:depth])
            if parent is None or not parent.isdir():
                raise UnsupportedLayer("OCI parent metadata requires lower-layer context", reason="parent_context")
        if member.islnk():
            target = entries.get(_path(member.linkname))
            if (target is None or not target.isreg()
                    or (member.mode, member.uid, member.gid, member.mtime)
                    != (target.mode, target.uid, target.gid, target.mtime)):
                raise UnsupportedLayer("OCI hardlink requires lower-layer context", reason="hardlink_context")
    return entries


def _metadata(path, member):
    # chown first, since it clears setuid and capability bits. No symlink is
    # followed, and all ancestors were validated as explicit directories.
    os.chown(path, member.uid, member.gid, follow_symlinks=False)
    if not member.issym():
        os.chmod(path, member.mode, follow_symlinks=False)
    os.utime(path, (member.mtime, member.mtime), follow_symlinks=False)


def _copy_member(archive, member, target):
    """Copy one validated regular payload, never its surrounding tar bytes."""
    copied = 0
    sendfile = getattr(os, 'sendfile', None)
    if sendfile is not None:
        while copied < member.size:
            try:
                count = sendfile(target.fileno(), archive.fileobj.fileno(),
                                 member.offset_data + copied, min(BLOCK, member.size - copied))
            except OSError as exc:
                if exc.errno not in {errno.ENOSYS, errno.EXDEV, errno.EINVAL, errno.EOPNOTSUPP, errno.ENOTSOCK}:
                    raise
                break
            if not count:
                # Some filesystems do not support range copies even though the
                # syscall exists. The bounded tar reader also detects truncation.
                break
            copied += count
    if copied < member.size:
        with archive.extractfile(member) as source:
            source.seek(copied)
            shutil.copyfileobj(source, target, BLOCK)


def _extract(archive_path, destination):
    with tarfile.open(archive_path, mode='r:') as archive:
        entries = _members(archive)
        # All names/ancestors were validated, including duplicate canonical
        # paths and symlink parents. Reuse each resulting filename for creation
        # and metadata instead of repeatedly reparsing it through pathlib.
        paths = {parts: os.path.join(os.fspath(destination), *parts) for parts in entries}
        destination.mkdir(mode=0o700)
        for parts, member in sorted(entries.items(), key=lambda item: (len(item[0]), item[0])):
            if parts and member.isdir():
                os.mkdir(paths[parts], mode=0o700)
        for parts, member in entries.items():
            path = paths[parts]
            if member.isreg():
                descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
                with os.fdopen(descriptor, 'wb') as target:
                    _copy_member(archive, member, target)
            elif member.issym():
                os.symlink(member.linkname, path)
        for parts, member in entries.items():
            if member.islnk():
                os.link(paths[_path(member.linkname)], paths[parts],
                        follow_symlinks=False)
        for parts, member in sorted(entries.items(), key=lambda item: (-len(item[0]), item[0])):
            _metadata(paths[parts], member)
        if () not in entries:
            os.chown(destination, 0, 0, follow_symlinks=False)
            os.chmod(destination, 0o755)


class _AuthenticatedBlobReader:
    """Bounded forward-only HTTP reader; authenticate all bytes, including tails.

    Coalescing short reads preserves gzip's file-like read(n) contract. The
    extra byte beyond the descriptor detects overlong responses without an
    unbounded read. This object does not own/close the response.
    """

    def __init__(self, response, size, digest):
        self.response, self.expected_size, self.digest = response, size, digest
        self.size, self.checksum, self.eof = 0, hashlib.sha256(), False
        self.transfer_ms = 0.0

    def read(self, size=-1):
        remaining_build_execution_seconds()
        if self.eof or size == 0:
            return b""
        limit = min(BLOCK, self.expected_size - self.size + 1)
        if size >= 0:
            limit = min(limit, size)
        chunks, received = [], 0
        while received < limit:
            remaining_build_execution_seconds()
            started = time.monotonic()
            try:
                chunk = self.response.read(limit - received)
            finally:
                self.transfer_ms += (time.monotonic() - started) * 1000
            if chunk:
                self.size += len(chunk)
            remaining_build_execution_seconds()
            if not chunk:
                self.eof = True
                break
            if self.size > self.expected_size:
                raise ValueError("OCI blob exceeds its descriptor size")
            self.checksum.update(chunk)
            chunks.append(chunk)
            received += len(chunk)
        return b"".join(chunks)

    def verify(self):
        # gzip can stop on invalid metadata before consuming the whole HTTP
        # body. Authentication still takes precedence over semantic fallback.
        while self.read(BLOCK):
            pass
        if self.size != self.expected_size or 'sha256:' + self.checksum.hexdigest() != self.digest:
            raise ValueError("OCI blob content identity mismatch")


def _metric(metrics, name, value):
    if metrics is not None:
        metrics[name] = metrics.get(name, 0.0) + value


def _read_unpacked(source, compressed, size, *, gzip_encoded, metrics):
    started, transfer_before = time.monotonic(), compressed.transfer_ms
    try:
        return source.read(size)
    finally:
        if gzip_encoded:
            # Exclude nested HTTP wait; includes gzip processing and compressed
            # hashing/framing overhead, not a CPU-only profiler measurement.
            elapsed_ms = (time.monotonic() - started) * 1000
            _metric(metrics, "oci_decompress_ms", max(0, elapsed_ms - (compressed.transfer_ms - transfer_before)))


def materialize_layers(client, repository, layers, diff_ids, root: Path, *,
                       max_compressed_bytes=MAX_COMPRESSED_BYTES,
                       max_unpacked_bytes=MAX_UNPACKED_BYTES, metrics=None):
    """Authenticate and extract a bounded collection of self-contained diffs.

    The caller supplies a private temporary root and cleans it on every exit.
    Both the registry blob identity and uncompressed OCI diff ID are verified
    before an archive can supply a publishable directory. No content is reused
    across requests and no registry references are modified here. Optional metrics
    count actual response bytes, including attempts that later fail validation.
    """
    if len(layers) != len(diff_ids) or not layers:
        raise UnsupportedLayer("invalid selective OCI layer set", reason="layer_set")
    if any(not isinstance(layer, dict) or type(layer.get('size')) is not int
           or layer['size'] < 0 for layer in layers):
        raise UnsupportedLayer("unsupported OCI layer descriptors", reason="descriptors")
    if any(layer.get('mediaType') not in GZIP_TYPES | TAR_TYPES for layer in layers):
        raise UnsupportedLayer("unsupported OCI layer media type", reason="media_type")
    if sum(layer['size'] for layer in layers) > max_compressed_bytes:
        raise UnsupportedLayer("selective OCI compressed byte budget exceeded", reason="compressed_budget")
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    info = root.lstat()
    if root.is_symlink() or not root.is_dir() or info.st_uid != os.geteuid() or info.st_mode & 0o077:
        raise UnsupportedLayer("selective OCI scratch root must be a private owned directory", reason="scratch_root")
    unpacked, directories = 0, []
    for index, (layer, diff_id) in enumerate(zip(layers, diff_ids)):
        digest, diff_id = require_digest(layer.get('digest')), require_digest(diff_id)
        archive_path = root / f'{index}.tar'
        checksum = hashlib.sha256()
        remaining_build_execution_seconds()
        started = time.monotonic()
        try:
            response = client.open_blob(repository, digest)
        finally:
            _metric(metrics, "oci_transfer_ms", (time.monotonic() - started) * 1000)
        compressed = _AuthenticatedBlobReader(response, layer['size'], digest)
        try:
            # Keep only the uncompressed quarantine tar. No filesystem member
            # is extracted until both compressed and uncompressed identities
            # have authenticated, including gzip trailers/concatenated members.
            gzip_encoded = layer['mediaType'] in GZIP_TYPES
            source = (gzip.GzipFile(fileobj=compressed, mode='rb')
                      if gzip_encoded else nullcontext(compressed))
            try:
                with source as unpacked_source, archive_path.open('xb') as target:
                    while chunk := _read_unpacked(unpacked_source, compressed,
                            min(BLOCK, max_unpacked_bytes - unpacked + 1), gzip_encoded=gzip_encoded, metrics=metrics):
                        remaining_build_execution_seconds()
                        unpacked += len(chunk)
                        if unpacked > max_unpacked_bytes:
                            raise UnsupportedLayer("selective OCI unpacked byte budget exceeded", reason="unpacked_budget")
                        checksum.update(chunk)
                        target.write(chunk)
            except (gzip.BadGzipFile, EOFError, zlib.error, UnsupportedLayer):
                # Previously the compressed spool was verified before gzip or
                # expansion limits ran. Preserve that hard-corruption boundary
                # while avoiding decompression beyond the unpacked byte budget.
                compressed.verify()
                raise
            compressed.verify()
        finally:
            response.close()
            _metric(metrics, "oci_transfer_ms", compressed.transfer_ms)
            _metric(metrics, "oci_download_bytes_actual", compressed.size)
        if 'sha256:' + checksum.hexdigest() != diff_id:
            raise ValueError("OCI uncompressed layer identity mismatch")
        destination = root / f'{index}.diff'
        started = time.monotonic()
        try:
            _extract(archive_path, destination)
        finally:
            _metric(metrics, "oci_extract_ms", (time.monotonic() - started) * 1000)
        archive_path.unlink()
        directories.append(destination)
    return directories
