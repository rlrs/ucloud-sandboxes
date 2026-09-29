#!/usr/bin/env python3
"""Hash and inspect one explicitly pinned OCI gzip layer, without extraction.

Output contains aggregate metadata only. Member names, links, payloads and raw
exceptions are never emitted. The input is opened read-only without symlinks.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import stat
import tarfile
import time


BLOCK = 1024 * 1024
ALLOWED_PAX = {'path', 'linkpath', 'mtime', 'atime', 'ctime', 'size', 'uid', 'gid', 'uname', 'gname'}


class InspectionLimit(ValueError):
    pass


def digest(value):
    if not re.fullmatch(r'sha256:[0-9a-f]{64}', value):
        raise ValueError('Expected a SHA256 digest')
    return value


class CountedReader:
    def __init__(self, source, limit, deadline):
        self.source, self.limit, self.deadline = source, limit, deadline
        self.bytes = 0
        self.hash = hashlib.sha256()

    def read(self, size=-1):
        if time.monotonic() >= self.deadline:
            raise InspectionLimit('timeout')
        block = self.source.read(min(BLOCK, size if size >= 0 else BLOCK, self.limit - self.bytes + 1))
        self.bytes += len(block)
        if self.bytes > self.limit:
            raise InspectionLimit('byte_limit')
        self.hash.update(block)
        return block


def path_parts(name):
    if not isinstance(name, str) or len(name) > 4096 or '\0' in name:
        return None
    parts = tuple(part for part in name.split('/') if part and part != '.')
    return None if name.startswith('/') or '..' in parts else parts


def scan_archive(stream, *, max_members):
    entries, counts, reasons, first_reason = {}, Counter(), Counter(), None
    payload_bytes = 0

    def reject(reason, index):
        nonlocal first_reason
        reasons[reason] += 1
        if first_reason is None:
            first_reason = {'reason': reason, 'member_index': index}

    with tarfile.open(fileobj=stream, mode='r|') as archive:
        for index, member in enumerate(archive):
            if index >= max_members:
                raise InspectionLimit('member_limit')
            counts['members'] += 1
            kind = ('directory' if member.isdir() else 'regular' if member.isreg() else
                    'symlink' if member.issym() else 'hardlink' if member.islnk() else 'special')
            counts[kind] += 1
            payload_bytes += member.size if member.isreg() else 0
            parts = path_parts(member.name)
            if parts is None or (not parts and not member.isdir()):
                reject('unsupported_path', index)
                continue
            if any(part.startswith('.wh.') for part in parts):
                reject('whiteout_requires_lower_context', index)
                counts['opaque_whiteouts' if parts[-1] == '.wh..wh..opq' else 'whiteouts'] += 1
            if parts in entries:
                reject('duplicate_path', index)
            if member.issparse() or kind == 'special' or set(member.pax_headers) - ALLOWED_PAX:
                reject('unsupported_type_or_extended_metadata', index)
            counts['extended_metadata_members'] += bool(set(member.pax_headers) - ALLOWED_PAX)
            counts['xattr_members'] += any(key.startswith(('SCHILY.xattr.', 'LIBARCHIVE.xattr.')) for key in member.pax_headers)
            if (type(member.uid) is not int or type(member.gid) is not int
                    or not 0 <= member.uid < 2**32 - 1 or not 0 <= member.gid < 2**32 - 1
                    or member.mode & ~0o7777 or member.size < 0):
                reject('unsupported_metadata', index)
            if member.issym() and ('\0' in member.linkname or len(member.linkname) > 4096):
                reject('unsupported_symlink', index)
            entries[parts] = (index, kind, member.mode, member.uid, member.gid,
                              member.mtime, member.linkname if member.islnk() else None)
    for parts, entry in entries.items():
        if time.monotonic() >= stream.deadline:
            raise InspectionLimit('timeout')
        index, kind, mode, uid, gid, mtime, link = entry
        if any(entries.get(parts[:depth], (None, None))[1] != 'directory' for depth in range(1, len(parts))):
            reject('parent_requires_lower_context', index)
        if kind == 'hardlink':
            target = entries.get(path_parts(link))
            if target is None or target[1] != 'regular' or (mode, uid, gid, mtime) != target[2:6]:
                reject('hardlink_requires_lower_context', index)
    # Runtime refuses the 50,001st member, while its 50,000th is accepted.
    if counts['members'] > 50_000:
        reject('runtime_member_limit', 50_000)
    return {'counts': dict(counts), 'regular_payload_bytes': payload_bytes,
            'incompatibility_counts': dict(reasons), 'first_semantic_reason': first_reason,
            'semantically_supported_by_current_extractor': not reasons}


def inspect(args):
    started = time.monotonic()
    deadline = started + args.timeout_seconds
    fd = os.open(args.blob, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, 'rb') as source:
        info = os.fstat(source.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_size != args.compressed_bytes:
            raise ValueError('Expected pinned regular blob size')
        compressed = CountedReader(source, args.compressed_bytes, deadline)
        while compressed.read(BLOCK):
            pass
        if 'sha256:' + compressed.hash.hexdigest() != args.compressed_digest:
            raise ValueError('Compressed identity mismatch')
        source.seek(0)
        with gzip.GzipFile(fileobj=source, mode='rb') as uncompressed:
            counted = CountedReader(uncompressed, args.max_unpacked_bytes, deadline)
            result = scan_archive(counted, max_members=args.max_members)
            # Hash trailing tar padding too; tar iteration stops at its end marker.
            while counted.read(BLOCK):
                pass
        if 'sha256:' + counted.hash.hexdigest() != args.diff_id:
            raise ValueError('Uncompressed identity mismatch')
    result.update(captured_at=datetime.now(timezone.utc).isoformat(),
                  compressed_digest=args.compressed_digest, diff_id=args.diff_id,
                  compressed_bytes=compressed.bytes, unpacked_bytes=counted.bytes,
                  compressed_digest_verified=True, diff_id_verified=True,
                  exceeds_current_compressed_limit=compressed.bytes > 128 * 1024**2,
                  exceeds_current_unpacked_limit=counted.bytes > 1024**3,
                  wall_seconds=time.monotonic() - started, extracted_files=0,
                  registry_mutations=0, helper_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest())
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--blob', type=Path, required=True)
    parser.add_argument('--compressed-digest', type=digest, required=True)
    parser.add_argument('--compressed-bytes', type=int, required=True)
    parser.add_argument('--diff-id', type=digest, required=True)
    parser.add_argument('--max-unpacked-bytes', type=int, default=4 * 1024**3)
    parser.add_argument('--max-members', type=int, default=100_000)
    parser.add_argument('--timeout-seconds', type=float, default=120)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if (not 0 < args.compressed_bytes <= 1024**3 or not 0 < args.max_unpacked_bytes <= 4 * 1024**3
            or not 0 < args.max_members <= 100_000 or not 0 < args.timeout_seconds <= 120
            or args.output.exists()):
        parser.error('Inspection bounds or new output requirement violated')

    def timed_out(_signal, _frame):
        raise InspectionLimit('timeout')

    signal.signal(signal.SIGALRM, timed_out)
    signal.setitimer(signal.ITIMER_REAL, args.timeout_seconds)
    try:
        result = inspect(args)
    except Exception as error:
        # Fixed error types/codes only; never serialize member names or tar errors.
        result = {'complete': False, 'error_type': type(error).__name__}
        if isinstance(error, InspectionLimit):
            result['limit'] = str(error)
    else:
        result['complete'] = True
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
    with args.output.open('x') as stream:
        stream.write(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result), flush=True)
    if not result['complete']:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
