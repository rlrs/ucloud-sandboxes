#!/usr/bin/env python3
"""Export a pinned OCI filesystem through an isolated, non-executing unpacker.

Root-only offline tool. Downloads verified blobs into a fresh work directory,
unpacks inside a minimal read-only chroot, and exports contents plus xattrs.
Neither image commands nor image binaries are executed. No registry writes.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import gzip
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tarfile
from urllib import request
from urllib.parse import quote, urlparse
from uuid import uuid4

from prepare_image_pool import public_registry_headers, registry_parts
from prepare_shared_task_image import copy_verified, encode
from stage_source_image import PublicBlobRedirect
from ucloud_sandboxes.environment_artifact import require_digest
from ucloud_sandboxes.oci_flat_delta import archive_path

EXCLUDED = frozenset({'dev', 'proc', 'sys', 'run'})


@contextmanager
def bounded_output(directory, backing_file, size_bytes):
    """A source cannot fill the gateway root disk with an expanding layer."""
    with backing_file.open('xb') as stream:
        stream.truncate(size_bytes)
    try:
        subprocess.run(['mkfs.ext4', '-q', '-F', str(backing_file)], check=True, timeout=60)
        subprocess.run(['mount', '-o', 'loop,nodev,nosuid,noexec', str(backing_file), str(directory)],
                       check=True, timeout=30)
        yield
    finally:
        if os.path.ismount(directory):
            # If unmount fails, preserve the backing file and fail the job.
            subprocess.run(['umount', str(directory)], check=True, timeout=30)
        backing_file.unlink()


def oci_manifest(document):
    result = dict(document)
    result['mediaType'] = 'application/vnd.oci.image.manifest.v1+json'
    result['config'] = {**document['config'], 'mediaType': 'application/vnd.oci.image.config.v1+json'}
    allowed = {'application/vnd.docker.image.rootfs.diff.tar.gzip', 'application/vnd.oci.image.layer.v1.tar+gzip'}
    layers = []
    for descriptor in document['layers']:
        if descriptor.get('mediaType') not in allowed:
            raise ValueError('unsupported layer compression/media type')
        require_digest(descriptor['digest'])
        if not isinstance(descriptor['size'], int) or descriptor['size'] < 0:
            raise ValueError('invalid layer size')
        layers.append({**descriptor, 'mediaType': 'application/vnd.oci.image.layer.v1.tar+gzip'})
    result['layers'] = layers
    return result


def export_tar(rootfs, destination):
    """Preserve numeric ownership, hardlinks, symlinks, times and xattrs."""
    count, size = 0, 0
    def include(member):
        nonlocal count, size
        name = archive_path(member.name)
        if name.split('/')[0] in EXCLUDED:
            return None
        if member.type not in {tarfile.REGTYPE, tarfile.DIRTYPE, tarfile.SYMTYPE, tarfile.LNKTYPE}:
            raise ValueError('unsupported source filesystem entry')
        count += 1
        size += member.size
        if count > 200_000 or size > 64 * 1024**3:
            raise ValueError('filesystem export bound exceeded')
        path = rootfs if name == '.' else rootfs / name
        for key in os.listxattr(path, follow_symlinks=False):
            member.pax_headers['SCHILY.xattr.' + key] = os.getxattr(path, key, follow_symlinks=False).decode('utf-8', 'surrogateescape')
        return member
    with destination.open('xb') as raw, gzip.GzipFile(fileobj=raw, mode='wb', filename='', mtime=0, compresslevel=1) as compressed:
        with tarfile.open(fileobj=compressed, mode='w|', format=tarfile.PAX_FORMAT, dereference=False) as archive:
            archive.add(rootfs, arcname='.', filter=include)
    return {'entries': count, 'regular_file_bytes': size}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--reference', required=True)
    parser.add_argument('--resolved', type=Path, help='authenticated public-source receipt, or omitted for a private pin')
    parser.add_argument('--root', required=True, type=Path)
    parser.add_argument('--umoci', required=True, type=Path)
    parser.add_argument('--umoci-sha256', required=True)
    parser.add_argument('--max-unpacked-gib', type=int, default=16)
    parser.add_argument('--config', type=Path, default=Path('/etc/ucloud-sandboxes/deployment.json'))
    args = parser.parse_args()
    if not 1 <= args.max_unpacked_gib <= 32:
        parser.error('unpacked filesystem bound must be 1..32 GiB')
    if os.getuid() != 0 or not args.root.is_absolute() or args.root.exists():
        parser.error('requires root and a fresh absolute work directory')
    if '@sha256:' not in args.reference:
        parser.error('immutable image reference required')
    if hashlib.sha256(args.umoci.read_bytes()).hexdigest() != args.umoci_sha256:
        raise ValueError('unpacker binary identity mismatch')
    from ucloud_sandboxes.config import DeploymentConfig
    from ucloud_sandboxes.managed_registry import MANIFEST_ACCEPT, RegistryClient, registry_repository_tag_from_image_ref
    config = DeploymentConfig.from_dict(json.loads(args.config.read_text()))
    client = RegistryClient(config.registry_url)
    original_digest = args.reference.split('@')[1]
    require_digest(original_digest)
    if args.resolved:
        receipt = json.loads(args.resolved.read_text())
        resolved = receipt.get('resolved', receipt)
        if resolved['reference'] != args.reference:
            raise ValueError('receipt belongs to another source')
        original = resolved['manifest_json'].encode()
        config_data = resolved['config_json'].encode()
        host, repository, _ = registry_parts(args.reference)
    else:
        if args.reference.split('/')[0] != urlparse(config.registry_worker_url).netloc:
            raise ValueError('private reference must use the configured registry')
        repository, _ = registry_repository_tag_from_image_ref(args.reference)
        with client._request(f'/v2/{quote(repository, safe="/")}/manifests/{original_digest}',
                             headers={'Accept': MANIFEST_ACCEPT}) as response:
            original = response.read(256 * 1024 + 1)
        if len(original) > 256 * 1024:
            raise ValueError('source manifest exceeds bound')
        document = json.loads(original)
        config_data = client.blob_bytes(repository, document['config']['digest'])
        host = None
    if hashlib.sha256(original).hexdigest() != original_digest.split(':')[1]:
        raise ValueError('source manifest identity mismatch')
    document = json.loads(original)
    descriptor = document['config']
    if 'sha256:' + hashlib.sha256(config_data).hexdigest() != descriptor['digest'] or len(config_data) != descriptor['size']:
        raise ValueError('source config identity mismatch')
    image_config = json.loads(config_data)
    if (image_config.get('os') != 'linux' or image_config.get('architecture') != 'amd64'
            or len(image_config['rootfs']['diff_ids']) != len(document['layers'])):
        raise ValueError('unsupported source platform or layer chain')
    manifest = oci_manifest(document)
    compressed_bytes = sum(d['size'] for d in manifest['layers'])
    if compressed_bytes > 5 * 1024**3 or len(manifest['layers']) > 64:
        raise ValueError('source exceeds bounded unpacking input')
    unpacked_bound = args.max_unpacked_gib * 1024**3
    if shutil.disk_usage(args.root.parent).free < max(32 * 1024**3, compressed_bytes * 3 + unpacked_bound * 2):
        raise ValueError('insufficient isolated scratch space')
    args.root.mkdir(mode=0o700)
    jail = args.root / 'jail'
    blobs = jail / 'image/blobs/sha256'
    blobs.mkdir(parents=True)
    (jail / 'output').mkdir()
    shutil.copyfile(args.umoci, jail / 'umoci')
    (jail / 'umoci').chmod(0o755)
    # The distro binary has only the glibc loader and libc as dynamic dependencies.
    for source, target in [('/lib64/ld-linux-x86-64.so.2', 'lib64/ld-linux-x86-64.so.2'),
                           ('/usr/lib/x86_64-linux-gnu/libc.so.6', 'lib/x86_64-linux-gnu/libc.so.6')]:
        destination = jail / target
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, destination)
        destination.chmod(0o755)
    (blobs / descriptor['digest'].split(':')[1]).write_bytes(config_data)
    opener = request.build_opener(PublicBlobRedirect()).open
    headers = None
    for layer in manifest['layers']:
        digest = layer['digest']
        key = digest.split(':')[1]
        destination = blobs / key
        if destination.exists():
            continue
        local = config.registry_data_dir() / 'docker/registry/v2/blobs/sha256' / key[:2] / key / 'data'
        if local.is_file() and local.stat().st_size == layer['size']:
            with local.open('rb') as stream:
                copy_verified(stream, destination, layer)
        elif host is None:
            with client.open_blob(repository, digest) as stream:
                copy_verified(stream, destination, layer)
        else:
            if headers is None:
                headers = public_registry_headers(host, repository)
            endpoint = 'registry-1.docker.io' if host == 'docker.io' else host
            with opener(request.Request(f'https://{endpoint}/v2/{repository}/blobs/{digest}', headers=headers), timeout=120) as stream:
                copy_verified(stream, destination, layer)
    data = encode(manifest)
    digest = hashlib.sha256(data).hexdigest()
    (blobs / digest).write_bytes(data)
    (jail / 'image/oci-layout').write_bytes(encode({'imageLayoutVersion': '1.0.0'}))
    (jail / 'image/index.json').write_bytes(encode({'schemaVersion': 2, 'manifests': [{
        'mediaType': manifest['mediaType'], 'digest': 'sha256:' + digest, 'size': len(data),
        'annotations': {'org.opencontainers.image.ref.name': 'source'}}]}))
    command = ['systemd-run', '--wait', '--collect', '--unit=uc-oci-unpack-' + uuid4().hex[:16],
               '--property=RootDirectory=' + str(jail), '--property=ProtectSystem=strict',
               '--property=ReadWritePaths=+/output', '--property=NoExecPaths=+/output',
               '--property=PrivateNetwork=yes', '--property=PrivateDevices=yes', '--property=NoNewPrivileges=yes',
               '--property=CapabilityBoundingSet=CAP_CHOWN CAP_DAC_OVERRIDE CAP_FOWNER CAP_FSETID CAP_SETFCAP',
               '--property=MemoryMax=2G', '--property=CPUQuota=100%', '--property=Nice=19',
               '--property=RuntimeMaxSec=1200', '/umoci', 'raw', 'unpack', '--image', '/image:source', '/output/rootfs']
    destination = args.root / 'filesystem.tar.gz'
    with bounded_output(jail / 'output', args.root / 'unpack.ext4', unpacked_bound):
        subprocess.run(command, check=True, timeout=1260)
        counts = export_tar(jail / 'output/rootfs', destination)
    digest = hashlib.sha256()
    with destination.open('rb') as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    report = {'schema': 1, 'reference': args.reference, 'source_config': document['config']['digest'],
              'source_layers': document['layers'], 'diff_ids': image_config['rootfs']['diff_ids'],
              'unpacker_sha256': args.umoci_sha256, 'export_sha256': digest.hexdigest(),
              'export_bytes': destination.stat().st_size, 'unpacked_limit_bytes': unpacked_bound,
              'excluded_runtime_trees': sorted(EXCLUDED), **counts}
    (args.root / 'export.json').write_bytes(encode(report))
    # This directory was created exclusively by this invocation. Keep only the
    # authenticated export/receipt; thousands of preparations must not retain
    # another unpacked rootfs and a full copy of every source blob on the gateway.
    # rmtree does not follow links from the untrusted unpacked filesystem.
    shutil.rmtree(jail)
    print(json.dumps(report), flush=True)


if __name__ == '__main__':
    main()
