"""Conservative OCI diff materialization for small, partially cached images.

This accepts only layers whose filesystem view can be produced without looking
through a lower layer. Everything else remains the Docker adapter's job. Never
use tarfile.extract/extractall on these untrusted archives.
"""
import gzip
import hashlib
import os
from pathlib import Path, PurePosixPath
import shutil
import tarfile

from .environment_artifact import require_digest


class UnsupportedLayer(ValueError):
    """A valid image may need the general Docker materialization path."""


GZIP_TYPES = frozenset({"application/vnd.oci.image.layer.v1.tar+gzip",
                        "application/vnd.docker.image.rootfs.diff.tar.gzip"})
TAR_TYPES = frozenset({"application/vnd.oci.image.layer.v1.tar",
                       "application/vnd.docker.image.rootfs.diff.tar"})
MAX_MEMBERS = 50_000
BLOCK = 1024 * 1024


def _path(value, *, root=False):
    if not isinstance(value, str) or len(value) > 4096 or "\0" in value:
        raise UnsupportedLayer("unsupported OCI member path")
    path = PurePosixPath(value)
    if path.is_absolute() or ".." in path.parts:
        raise UnsupportedLayer("OCI member escapes its diff directory")
    if not path.parts:
        if root:
            return ()
        raise UnsupportedLayer("OCI member has an empty path")
    if any(part.startswith('.wh.') for part in path.parts):
        raise UnsupportedLayer("OCI whiteouts require lower-layer context")
    return path.parts


def _members(archive):
    entries = {}
    for member in archive:
        if len(entries) >= MAX_MEMBERS:
            raise UnsupportedLayer("too many OCI members for selective materialization")
        parts = _path(member.name, root=member.isdir())
        if parts in entries:
            raise UnsupportedLayer("duplicate OCI member path")
        if (member.issparse() or not (member.isdir() or member.isreg() or member.issym() or member.islnk())
                or any(key not in {'path', 'linkpath', 'mtime', 'atime', 'ctime', 'size', 'uid', 'gid',
                                   'uname', 'gname'} for key in member.pax_headers)):
            raise UnsupportedLayer("unsupported OCI member type or extended metadata")
        if (type(member.uid) is not int or type(member.gid) is not int
                or not 0 <= member.uid < 2**32 - 1 or not 0 <= member.gid < 2**32 - 1
                or member.mode & ~0o7777 or member.size < 0):
            raise UnsupportedLayer("unsupported OCI member metadata")
        if member.issym() and ("\0" in member.linkname or len(member.linkname) > 4096):
            raise UnsupportedLayer("unsupported OCI symlink")
        entries[parts] = member
    for parts, member in entries.items():
        for depth in range(1, len(parts)):
            parent = entries.get(parts[:depth])
            if parent is None or not parent.isdir():
                raise UnsupportedLayer("OCI parent metadata requires lower-layer context")
        if member.islnk():
            target = entries.get(_path(member.linkname))
            if (target is None or not target.isreg()
                    or (member.mode, member.uid, member.gid, member.mtime)
                    != (target.mode, target.uid, target.gid, target.mtime)):
                raise UnsupportedLayer("OCI hardlink requires lower-layer context")
    return entries


def _metadata(path, member):
    # chown first, since it clears setuid and capability bits. No symlink is
    # followed, and all ancestors were validated as explicit directories.
    os.chown(path, member.uid, member.gid, follow_symlinks=False)
    if not member.issym():
        os.chmod(path, member.mode, follow_symlinks=False)
    os.utime(path, (member.mtime, member.mtime), follow_symlinks=False)


def _extract(archive_path, destination):
    with tarfile.open(archive_path, mode='r:') as archive:
        entries = _members(archive)
        destination.mkdir(mode=0o700)
        for parts, member in sorted(entries.items(), key=lambda item: (len(item[0]), item[0])):
            if parts and member.isdir():
                destination.joinpath(*parts).mkdir(mode=0o700)
        for parts, member in entries.items():
            path = destination.joinpath(*parts)
            if member.isreg():
                descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
                with archive.extractfile(member) as source, os.fdopen(descriptor, 'wb') as target:
                    shutil.copyfileobj(source, target, BLOCK)
            elif member.issym():
                path.symlink_to(member.linkname)
        for parts, member in entries.items():
            if member.islnk():
                os.link(destination.joinpath(*_path(member.linkname)), destination.joinpath(*parts),
                        follow_symlinks=False)
        for parts, member in sorted(entries.items(), key=lambda item: (-len(item[0]), item[0])):
            _metadata(destination.joinpath(*parts), member)
        if () not in entries:
            os.chown(destination, 0, 0, follow_symlinks=False)
            os.chmod(destination, 0o755)


def materialize_layers(client, repository, layers, diff_ids, root: Path, *,
                       max_compressed_bytes=128 * 1024**2, max_unpacked_bytes=1024**3):
    """Authenticate and extract a bounded collection of self-contained diffs.

    The caller supplies a private temporary root and cleans it on every exit.
    Both the registry blob identity and uncompressed OCI diff ID are verified
    before an archive can supply a publishable directory. No content is reused
    across requests and no registry references are modified here.
    """
    if len(layers) != len(diff_ids) or not layers:
        raise UnsupportedLayer("invalid selective OCI layer set")
    if any(not isinstance(layer, dict) or type(layer.get('size')) is not int
           or layer['size'] < 0 or layer.get('mediaType') not in GZIP_TYPES | TAR_TYPES for layer in layers):
        raise UnsupportedLayer("unsupported OCI layer descriptors")
    if sum(layer['size'] for layer in layers) > max_compressed_bytes:
        raise UnsupportedLayer("selective OCI compressed byte budget exceeded")
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    info = root.lstat()
    if root.is_symlink() or not root.is_dir() or info.st_uid != os.geteuid() or info.st_mode & 0o077:
        raise UnsupportedLayer("selective OCI scratch root must be a private owned directory")
    unpacked, directories = 0, []
    for index, (layer, diff_id) in enumerate(zip(layers, diff_ids)):
        digest, diff_id = require_digest(layer.get('digest')), require_digest(diff_id)
        blob, archive_path = root / f'{index}.blob', root / f'{index}.tar'
        size, checksum = 0, hashlib.sha256()
        response = client.open_blob(repository, digest)
        try:
            with blob.open('xb') as output:
                while chunk := response.read(min(BLOCK, layer['size'] - size + 1)):
                    size += len(chunk)
                    if size > layer['size']:
                        raise ValueError("OCI blob exceeds its descriptor size")
                    checksum.update(chunk)
                    output.write(chunk)
        finally:
            response.close()
        if size != layer['size'] or 'sha256:' + checksum.hexdigest() != digest:
            raise ValueError("OCI blob content identity mismatch")
        checksum = hashlib.sha256()
        opener = gzip.open if layer['mediaType'] in GZIP_TYPES else open
        with opener(blob, 'rb') as source, archive_path.open('xb') as target:
            while chunk := source.read(min(BLOCK, max_unpacked_bytes - unpacked + 1)):
                unpacked += len(chunk)
                if unpacked > max_unpacked_bytes:
                    raise UnsupportedLayer("selective OCI unpacked byte budget exceeded")
                checksum.update(chunk)
                target.write(chunk)
        if 'sha256:' + checksum.hexdigest() != diff_id:
            raise ValueError("OCI uncompressed layer identity mismatch")
        destination = root / f'{index}.diff'
        _extract(archive_path, destination)
        blob.unlink()
        archive_path.unlink()
        directories.append(destination)
    return directories
