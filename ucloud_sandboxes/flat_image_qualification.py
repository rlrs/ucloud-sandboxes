"""Offline source-to-sandbox qualification for the layout-1 and layout-2 EROFS formats.

Layout 1 (mkfs -T 0) zeroes every inode time. Layout 2 (-T 0 --mkfs-time --MZ) keeps
file and symlink mtimes, which this checks against the source tar headers. It
validates the platform filesystem and does not change any import/alias path.
"""
from __future__ import annotations

import hashlib
import json
import math
import stat
import tarfile

from .oci_flat_delta import hardlink_groups, validate_flat_index

# DirectOciConfigBuilder always mounts a fresh tmpfs over /tmp and /run.
RUNTIME_TREES = frozenset({'/dev', '/proc', '/sys', '/run', '/tmp'})
RUNTIME_FILES = frozenset({'/etc/hostname', '/etc/hosts', '/etc/resolv.conf', '/.ucloud-init'})
RUNTIME_MTIMES = frozenset({'/', '/etc'})
SCAN_OUTPUT = '/tmp/ucloud-filesystem-proof.json.gz'
TIMESTAMP_CONTRACTS = {
    1: 'layout-1 mkfs -T 0; startup modifies root, etc and a newly created workspace',
    2: 'layout-2 mkfs -T 0 --mkfs-time --MZ; whole-second file and symlink mtimes match the source; '
       'directory times are not compared',
}


def excluded(path):
    return (path in RUNTIME_FILES or path == SCAN_OUTPUT
            or any(path == root or path.startswith(root + '/') for root in RUNTIME_TREES))


def qualified_layout(layer_formats):
    """The one layout, with a known timestamp contract, of every component."""
    layouts = {f.get('layout') for f in layer_formats}
    if (len(layouts) != 1 or any(type(value) is not int or value not in TIMESTAMP_CONTRACTS for value in layouts)
            or any(set(f.get('excludes', [])) != {'dev', 'proc', 'sys', 'run'} for f in layer_formats)):
        raise ValueError('qualification requires one known EROFS layout')
    return layouts.pop()


def _times(layout, kind, mtime):
    # Layout 2 compares whole seconds, the precision a .pyc check reads;
    # extractors round fractional PAX times differently. Builder-owned views
    # zero directory times, and borrowed Docker diffs keep Docker's.
    if layout == 1:
        return {'mtime_ns': 0}
    return {} if kind == tarfile.DIRTYPE else {'mtime_ns': math.floor(mtime) * 10**9}


def _observed(row, layout):
    if layout == 1 or 'mtime_ns' not in row:
        return row
    if stat.S_ISDIR(row.get('mode', 0)):
        return {k: v for k, v in row.items() if k != 'mtime_ns'}
    return row | {'mtime_ns': row['mtime_ns'] // 10**9 * 10**9}


def expected_filesystem(index, *, layout):
    """Derive contents/metadata independently of the delta selection algorithm."""
    validate_flat_index(index)
    if SCAN_OUTPUT.lstrip('/') in index:
        raise ValueError('source occupies the qualification output path')
    links = hardlink_groups(index)
    kinds = {tarfile.REGTYPE: stat.S_IFREG, tarfile.LNKTYPE: stat.S_IFREG,
             tarfile.SYMTYPE: stat.S_IFLNK, tarfile.DIRTYPE: stat.S_IFDIR}
    expected = {}
    for path, entry in index.items():
        absolute = '/' if path == '.' else '/' + path
        if excluded(absolute):
            continue
        inode = index[entry.linkname] if entry.kind == tarfile.LNKTYPE else entry
        row = {'mode': kinds[entry.kind] | inode.mode, 'uid': inode.uid, 'gid': inode.gid,
               **_times(layout, entry.kind, inode.mtime), 'xattrs': {
                   key[len('SCHILY.xattr.'):]: value.encode('utf-8', 'surrogateescape').hex()
                   for key, value in inode.pax if key.startswith('SCHILY.xattr.')}}
        if entry.kind in {tarfile.REGTYPE, tarfile.LNKTYPE}:
            group = links.get(path, frozenset({path}))
            row.update(size=inode.size, sha256=inode.digest, nlink=len(group))
            if len(group) > 1:
                row['hardlinks'] = sorted('/' + name for name in group if not excluded('/' + name))
        elif entry.kind == tarfile.SYMTYPE:
            row['link'] = entry.linkname
        expected[absolute] = row
    # Complete filesystem exports can omit the default root header.
    directory = _times(layout, tarfile.DIRTYPE, 0)
    expected.setdefault('/', {'mode': stat.S_IFDIR | 0o755, 'uid': 0, 'gid': 0, **directory, 'xattrs': {}})
    # The default workspace is created by DirectOciConfigBuilder only when
    # absent. Existing source workspace contents and metadata remain checked.
    expected.setdefault('/workspace', {'mode': stat.S_IFDIR | 0o1777, 'uid': 0, 'gid': 0,
                                      **directory, 'xattrs': {}})
    return expected


def startup_mtimes(index):
    return RUNTIME_MTIMES | ({'/workspace'} if 'workspace' not in index else set())


def compare_snapshot(index, snapshot, *, layer_formats):
    layout = qualified_layout(layer_formats)
    if snapshot.get('errors'):
        raise ValueError('filesystem scan had unreadable entries')
    expected_exclusions = RUNTIME_TREES | (RUNTIME_FILES - {'/.ucloud-init'}) | {SCAN_OUTPUT}
    if set(snapshot.get('excluded', [])) != expected_exclusions:
        raise ValueError('filesystem scanner exclusions changed')
    expected = expected_filesystem(index, layout=layout)
    actual = {path: _observed(row, layout) for path, row in snapshot['entries'].items() if not excluded(path)}
    count, sample = 0, []
    volatile_mtimes = startup_mtimes(index)
    for path in sorted(expected.keys() | actual.keys()):
        left, right = expected.get(path), actual.get(path)
        if left and right and path in volatile_mtimes:
            left = {k: v for k, v in left.items() if k != 'mtime_ns'}
            right = {k: v for k, v in right.items() if k != 'mtime_ns'}
        if left != right:
            count += 1
            if len(sample) < 50:
                sample.append({'path': path, 'expected': left, 'actual': right})
    return {'equivalent': count == 0, 'expected_entries': len(expected), 'actual_entries': len(actual),
            'difference_count': count, 'difference_sample': sample,
            'runtime_contract_version': 3,
            'created_runtime_workspace': 'workspace' not in index,
            'timestamp_contract': TIMESTAMP_CONTRACTS[layout],
            'excluded_runtime_trees': sorted(RUNTIME_TREES),
            'excluded_runtime_files': sorted(RUNTIME_FILES), 'scanner_output': SCAN_OUTPUT}


def qualification_key(index, components, runtime_config, worker_bundle_digest):
    """Reuse a full scan only for identical source expectations and mounted bytes.

    Callers must authenticate components and bind their source-layer chains to
    the final OCI config before using this key. Source layer IDs and parent IDs
    describe provenance, not filesystem bytes; component order, formats, sizes,
    range hashes, producer identity and runtime configuration remain in the key.
    """
    layout = qualified_layout([component.unsigned().get('format', {}) for component in components])
    expected = expected_filesystem(index, layout=layout)
    for path in startup_mtimes(index):
        if path in expected:
            expected[path] = {k: v for k, v in expected[path].items() if k != 'mtime_ns'}
    physical = []
    for component in components:
        profile = component.unsigned()
        profile.pop('source_layers', None)
        profile.pop('parent', None)
        physical.append(profile)
    payload = {'schema': 1, 'runtime_contract': 3, 'filesystem': expected, 'components': physical,
               'runtime_config': runtime_config, 'worker_bundle_digest': worker_bundle_digest,
               'scanner_digest': hashlib.sha256(SCANNER.encode()).hexdigest()}
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


SCANNER = r'''
import gzip,hashlib,json,os,stat,time
excluded={'/proc','/sys','/dev','/run','/tmp','/etc/hostname','/etc/hosts','/etc/resolv.conf','/tmp/ucloud-filesystem-proof.json.gz'}
rows={};links={};errors=[];stack=['/'];start=time.monotonic();byte_count=0
while stack:
    path=stack.pop()
    if path in excluded:continue
    try:
        s=os.lstat(path)
        attrs={name:os.getxattr(path,name,follow_symlinks=False).hex() for name in sorted(os.listxattr(path,follow_symlinks=False))}
        row={'mode':s.st_mode,'uid':s.st_uid,'gid':s.st_gid,'mtime_ns':s.st_mtime_ns,'xattrs':attrs}
        if stat.S_ISDIR(s.st_mode):
            stack.extend(entry.path for entry in os.scandir(path))
        elif stat.S_ISREG(s.st_mode):
            h=hashlib.sha256()
            with open(path,'rb') as f:
                while chunk:=f.read(1024*1024):h.update(chunk)
            row.update(size=s.st_size,sha256=h.hexdigest(),nlink=s.st_nlink)
            byte_count+=s.st_size
            links.setdefault((s.st_dev,s.st_ino),[]).append(path)
        elif stat.S_ISLNK(s.st_mode):row['link']=os.readlink(path)
        else:row['rdev']=s.st_rdev
        rows[path]=row
    except Exception as exc:errors.append({'path':path,'error':repr(exc)})
for paths in links.values():
    if len(paths)>1:
        for path in paths:rows[path]['hardlinks']=sorted(paths)
result={'entries':rows,'errors':errors,'excluded':sorted(excluded),'bytes_hashed':byte_count,'seconds':time.monotonic()-start}
with gzip.open('/tmp/ucloud-filesystem-proof.json.gz','wt') as f:json.dump(result,f,sort_keys=True,separators=(',',':'))
print(json.dumps({'entries':len(rows),'errors':errors[:10],'bytes_hashed':byte_count,'seconds':result['seconds']}))
'''
