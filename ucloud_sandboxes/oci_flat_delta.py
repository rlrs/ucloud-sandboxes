"""Conservative, offline delta planning for two complete OCI filesystem tars.

This is an experimental preparation tool, not an automatic image-import path.
It never extracts an archive or changes an existing image/alias. Unsupported
filesystem layouts fail closed so callers can retain the original source.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import posixpath
import tarfile


class UnsupportedFlatImage(ValueError):
    pass


def archive_path(value):
    if not isinstance(value, str) or not value or value.startswith('/') or '\x00' in value:
        raise UnsupportedFlatImage('archive path must be relative')
    if '..' in value.split('/'):
        raise UnsupportedFlatImage('archive parent traversal')
    result = posixpath.normpath(value)
    if any(part.startswith('.wh.') for part in result.split('/')):
        raise UnsupportedFlatImage('input must be a complete filesystem without whiteouts')
    return result


@dataclass(frozen=True)
class FileEntry:
    path: str
    kind: bytes
    size: int
    digest: str
    mode: int
    uid: int
    gid: int
    mtime: int | float
    linkname: str
    pax: tuple[tuple[str, str], ...]

    def filesystem_identity(self):
        # PAX path/linkpath are archive encoding, not additional filesystem data.
        return (self.kind, self.size, self.digest, self.mode, self.uid, self.gid,
                self.mtime, self.linkname, tuple((k, v) for k, v in self.pax
                                               if k not in {'path', 'linkpath', 'mtime'}))


def index_flat_tar(stream, *, max_entries=200_000, max_file_bytes=64 * 1024**3):
    """Index uncompressed tar bytes; retain only metadata and content hashes."""
    entries = {}
    total = 0
    with tarfile.open(fileobj=stream, mode='r|') as archive:
        for member in archive:
            path = archive_path(member.name)
            if path in entries or len(entries) >= max_entries:
                raise UnsupportedFlatImage('duplicate archive path or entry bound exceeded')
            if member.type not in {tarfile.REGTYPE, tarfile.DIRTYPE, tarfile.SYMTYPE, tarfile.LNKTYPE}:
                raise UnsupportedFlatImage('unsupported filesystem entry type')
            if member.sparse is not None or any(k not in {'path', 'linkpath', 'mtime'}
                                              and not k.startswith('SCHILY.xattr.') for k in member.pax_headers):
                raise UnsupportedFlatImage('unsupported sparse/PAX metadata')
            if path == '.' and not member.isdir():
                raise UnsupportedFlatImage('root entry must be a directory')
            linkname = archive_path(member.linkname) if member.islnk() else member.linkname
            if member.islnk() and (linkname not in entries or entries[linkname].kind != tarfile.REGTYPE):
                raise UnsupportedFlatImage('hardlink must name a preceding regular file')
            digest = hashlib.sha256()
            if member.isfile():
                total += member.size
                if total > max_file_bytes:
                    raise UnsupportedFlatImage('filesystem byte bound exceeded')
                reader = archive.extractfile(member)
                count = 0
                while chunk := reader.read(1024 * 1024):
                    digest.update(chunk)
                    count += len(chunk)
                if count != member.size:
                    raise UnsupportedFlatImage('truncated regular file')
            entries[path] = FileEntry(path, member.type, member.size if member.isfile() else 0,
                digest.hexdigest(), member.mode, member.uid, member.gid, member.mtime,
                linkname, tuple(sorted(member.pax_headers.items())))
    validate_flat_index(entries)
    return entries


def ancestors(path):
    while path != '.':
        path = posixpath.dirname(path) or '.'
        yield path


def validate_flat_index(entries):
    for path, entry in entries.items():
        if path != entry.path or archive_path(path) != path:
            raise UnsupportedFlatImage('index path identity mismatch')
        for parent in ancestors(path):
            if parent == '.' and parent not in entries:
                continue
            if parent not in entries or entries[parent].kind != tarfile.DIRTYPE:
                raise UnsupportedFlatImage('filesystem has an implicit or non-directory ancestor')
        if entry.kind == tarfile.LNKTYPE:
            if entry.linkname not in entries or entries[entry.linkname].kind != tarfile.REGTYPE:
                raise UnsupportedFlatImage('invalid hardlink target')


def hardlink_groups(entries):
    groups = {}
    for path, entry in entries.items():
        if entry.kind == tarfile.LNKTYPE:
            groups.setdefault(entry.linkname, {entry.linkname}).add(path)
    return {member: frozenset(group) for group in groups.values() for member in group}


@dataclass(frozen=True)
class FlatDelta:
    changed: frozenset[str]
    removed: tuple[str, ...]
    directories: frozenset[str]
    regular_file_bytes: int


def plan_flat_delta(base, target):
    """Plan exact path changes, including hardlink membership and directory times."""
    validate_flat_index(base)
    validate_flat_index(target)
    changed = {p for p, entry in target.items() if p not in base
               or entry.filesystem_identity() != base[p].filesystem_identity()}
    base_links, target_links = hardlink_groups(base), hardlink_groups(target)
    # Recreate a link group when any member changes or disappears. Otherwise a
    # copied-up regular file could leave an unchanged-looking lower hardlink
    # pointing to old bytes, or preserve a hidden lower inode's link count.
    for path in (set(base_links) | set(target_links)) & set(target):
        new_group = target_links.get(path, frozenset({path}))
        old_group = base_links.get(path, frozenset({path}))
        if new_group != old_group or changed.intersection(new_group):
            changed.update(new_group)
    removed = (set(base) - set(target)) | {p for p in base.keys() & target.keys()
                                          if base[p].kind != target[p].kind}
    removed.discard('.')
    # A whiteout of a directory already removes all of its lower descendants.
    roots = tuple(sorted(p for p in removed if not removed.intersection(ancestors(p))))
    directories = {p for p in changed if target[p].kind == tarfile.DIRTYPE}
    for path in changed | removed:
        directories.update(p for p in ancestors(path) if p in target and target[p].kind == tarfile.DIRTYPE)
    return FlatDelta(frozenset(changed), roots, frozenset(directories),
                     sum(target[p].size for p in changed if target[p].kind == tarfile.REGTYPE))


def write_flat_delta(target_stream, output_stream, target_index, plan):
    """Copy selected target members into a new OCI diff tar, never the host FS.

    Re-index and verify the target stream while copying, so a stale metadata
    plan cannot silently publish different input. Callers must authenticate the
    original compressed OCI blob as well before retaining the generated diff.
    """
    validate_flat_index(target_index)
    if not plan.changed <= target_index.keys() or not plan.directories <= target_index.keys():
        raise UnsupportedFlatImage('delta names an unknown target entry')
    if any(archive_path(path) != path for path in plan.removed):
        raise UnsupportedFlatImage('invalid removal path')
    if any(target_index[path].kind != tarfile.DIRTYPE for path in plan.directories):
        raise UnsupportedFlatImage('directory restoration names a non-directory')
    seen = set()
    with tarfile.open(fileobj=output_stream, mode='w|', format=tarfile.PAX_FORMAT) as output:
        for path in plan.removed:
            parent, name = posixpath.split(path)
            whiteout = tarfile.TarInfo(posixpath.join(parent, '.wh.' + name))
            whiteout.mode = 0
            output.addfile(whiteout)
        with tarfile.open(fileobj=target_stream, mode='r|') as archive:
            for member in archive:
                path = archive_path(member.name)
                if path in seen or path not in target_index:
                    raise UnsupportedFlatImage('target archive changed after planning')
                seen.add(path)
                expected = target_index[path]
                linkname = archive_path(member.linkname) if member.islnk() else member.linkname
                actual = FileEntry(path, member.type, member.size if member.isfile() else 0,
                    expected.digest, member.mode, member.uid, member.gid, member.mtime,
                    linkname, tuple(sorted(member.pax_headers.items())))
                if actual != expected:
                    raise UnsupportedFlatImage('target metadata changed after planning')
                if path in plan.directories:
                    output.addfile(member)
                reader = archive.extractfile(member) if member.isfile() else None
                hashed = _HashingReader(reader) if reader is not None else None
                if path in plan.changed and not member.isdir():
                    output.addfile(member, hashed)
                if hashed is not None:
                    while hashed.read(1024 * 1024):
                        pass
                    if hashed.digest.hexdigest() != expected.digest or hashed.count != expected.size:
                        raise UnsupportedFlatImage('target contents changed after planning')
        if seen != set(target_index):
            raise UnsupportedFlatImage('target archive lost entries after planning')
        # Each path occurs once. OCI unpackers defer directory metadata until
        # children have been applied; duplicate restoration headers are invalid.



class _HashingReader:
    def __init__(self, stream):
        self.stream = stream
        self.digest = hashlib.sha256()
        self.count = 0

    def read(self, size=-1):
        value = self.stream.read(size)
        self.digest.update(value)
        self.count += len(value)
        return value
