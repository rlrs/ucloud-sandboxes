#!/usr/bin/env python3
"""Prepare one flat task image as a verified shared base plus a small delta.

Original upstream layers are temporary local inputs, never published to the
registry. Read-back qualification precedes catalog/alias registration. Existing
aliases are insert-only. Unsupported images fail without replacing any image.
"""
from __future__ import annotations

import argparse
from dataclasses import asdict
import fcntl
import gzip
import hashlib
import json
import os
from pathlib import Path
import shutil
import stat
import sys
from tempfile import TemporaryDirectory
import time
from urllib import request
from uuid import uuid4

from prepare_image_pool import SourceResolver, public_registry_headers, register_import_alias, registry_parts
from plan_flat_image_delta import CheckedWriter, verified_tar
from stage_source_image import PublicBlobRedirect, mount_source_layers, publication_repositories
from ucloud_sandboxes.flat_image_qualification import SCANNER, compare_snapshot, qualification_key
from ucloud_sandboxes.oci_flat_delta import FileEntry, index_flat_tar, plan_flat_delta, validate_flat_index, write_flat_delta


def encode(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':')).encode()


def source_identity(source, anchor, pinned_source=None, exports=None, compression_level=9, anchor_filesystem_source=None):
    if type(compression_level) is not int or not 1 <= compression_level <= 9:
        raise ValueError('invalid delta compression level')
    identity = {'source': source, 'anchor': anchor, 'version': 1}
    if compression_level != 9:
        identity['compression_level'] = compression_level
    if anchor_filesystem_source:
        from ucloud_sandboxes.environment_artifact import require_digest
        require_digest(registry_parts(anchor_filesystem_source)[2])
        identity['anchor_filesystem_source'] = anchor_filesystem_source
    if pinned_source:
        host, repository, selector = registry_parts(pinned_source)
        from ucloud_sandboxes.environment_artifact import require_digest
        require_digest(selector)
        if registry_parts(source)[:2] != (host, repository):
            raise ValueError('pinned source belongs to another repository')
        identity['pinned_source'] = pinned_source
    if exports:
        identity['filesystem_exports'] = {name: row['export_sha256'] for name, row in exports.items()}
    return identity


def read_filesystem_exports(root):
    """Accept only root-owned, immutable-input-bound offline unpacker receipts."""
    if root is None:
        return None
    result = {}
    for name in ('anchor', 'target'):
        directory = root / name
        receipt, archive = directory / 'export.json', directory / 'filesystem.tar.gz'
        for path in (root, directory, receipt, archive):
            info = path.lstat()
            if info.st_uid != 0 or info.st_mode & 0o022 or stat.S_ISLNK(info.st_mode):
                raise ValueError('filesystem exports require protected root-owned inputs')
        if not receipt.is_file() or not archive.is_file() or receipt.stat().st_size > 256 * 1024:
            raise ValueError('invalid filesystem export inputs')
        row = json.loads(receipt.read_text())
        from ucloud_sandboxes.environment_artifact import require_digest
        require_digest('sha256:' + row['export_sha256'])
        require_digest('sha256:' + row['unpacker_sha256'])
        if (row.get('schema') != 1 or set(row.get('excluded_runtime_trees', [])) != {'dev', 'proc', 'sys', 'run'}
                or row['export_bytes'] != archive.stat().st_size):
            raise ValueError('filesystem export contract mismatch')
        result[name] = row
    return result


def validate_filesystem_export(row, reference, manifest, config):
    if (row['reference'] != reference or row['source_config'] != manifest['config']['digest']
            or row['source_layers'] != manifest['layers'] or row['diff_ids'] != config['rootfs']['diff_ids']):
        raise ValueError('filesystem export does not match authenticated OCI input')


def save(path, value):
    temporary = path.with_suffix(path.suffix + '.partial')
    temporary.write_bytes(encode(value))
    temporary.replace(path)


def copy_verified(stream, path, descriptor):
    digest, size = hashlib.sha256(), 0
    with path.open('xb') as output:
        while chunk := stream.read(1024 * 1024):
            size += len(chunk)
            if size > descriptor['size']:
                raise ValueError('source exceeded declared size')
            digest.update(chunk)
            output.write(chunk)
    if size != descriptor['size'] or 'sha256:' + digest.hexdigest() != descriptor['digest']:
        raise ValueError('source blob identity mismatch')


def deserialize(rows):
    entries = {}
    for raw in rows:
        row = dict(raw)
        row['kind'] = row['kind'].encode()
        row['pax'] = tuple(tuple(pair) for pair in row['pax'])
        entry = FileEntry(**row)
        if entry.path in entries:
            raise ValueError('duplicate cached path')
        entries[entry.path] = entry
    validate_flat_index(entries)
    return entries


def cached_anchor(client, repository, layer, cache, scratch, *, source_path=None):
    cache.mkdir(parents=True, exist_ok=True)
    key = layer['digest'].split(':')[1]
    with (cache / (key + '.lock')).open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        path = cache / (key + '.json.gz')
        if path.exists():
            document = json.loads(gzip.decompress(path.read_bytes()))
            payload = document['payload']
            if (payload['schema'] != 1 or payload['layer'] != layer
                    or hashlib.sha256(encode(payload)).hexdigest() != document['checksum']):
                raise ValueError('cached anchor identity mismatch')
            return deserialize(payload['entries'])
        blob = source_path or scratch / 'anchor.tar.gz'
        if source_path is None:
            with client.open_blob(repository, layer['digest']) as stream:
                copy_verified(stream, blob, layer)
        with verified_tar(blob, layer['digest']) as stream:
            index = index_flat_tar(stream)
        rows = []
        for entry in index.values():
            row = asdict(entry)
            row['kind'] = entry.kind.decode()
            rows.append(row)
        payload = {'schema': 1, 'layer': layer, 'entries': rows}
        data = gzip.compress(encode({'payload': payload, 'checksum': hashlib.sha256(encode(payload)).hexdigest()}), mtime=0)
        partial = path.with_suffix('.partial')
        partial.write_bytes(data)
        partial.replace(path)
        return index


class PublicLayerReader:
    def __init__(self, host):
        self.host = host

    def open_blob(self, repository, digest):
        endpoint = 'registry-1.docker.io' if self.host == 'docker.io' else self.host
        return request.build_opener(PublicBlobRedirect()).open(request.Request(
            f'https://{endpoint}/v2/{repository}/blobs/{digest}',
            headers=public_registry_headers(self.host, repository)), timeout=120)


def flat_anchor_source(resolved, expected, maximum):
    """Authenticate a flat filesystem index input, never a substitute runtime image."""
    from cache_source_receipts import validated
    if registry_parts(resolved['reference']) != registry_parts(expected):
        raise ValueError('anchor filesystem source differs from immutable pin')
    verified = validated(expected, resolved)
    if verified['layer_count'] != 1 or verified['onbuild'] or verified['compressed_bytes'] > maximum:
        raise ValueError('anchor filesystem source must be a bounded flat image')
    return verified['layers'][0]


def remove_abandoned_inputs(root, key):
    """Delete only this locked job's marked, known temporary input files."""
    allowed = {'.owner.json', 'anchor.tar.gz', 'target.tar.gz', 'delta.tar.gz', 'config.json'}
    for path in root.glob('inputs-*'):
        if path.is_symlink() or not path.is_dir() or path.stat().st_uid != os.getuid():
            continue
        entries = list(path.iterdir())
        if any(p.is_symlink() or not p.is_file() or p.name not in allowed for p in entries):
            continue
        marker = path / '.owner.json'
        try:
            if marker.stat().st_size > 256 or json.loads(marker.read_text()) != {'source_key': key}:
                continue
        except (OSError, ValueError):
            continue
        shutil.rmtree(path)


def complete_build(client, image, receipt_path, *, retry_failed=False):
    """Resume accepted work; explicitly retry a recorded terminal failure once."""
    build = json.loads(receipt_path.read_text()) if receipt_path.exists() else None
    for attempt in range(2):
        if build is None:
            build = client.submit_image_build(image, timeout_seconds=600)
            save(receipt_path, build)
        reused = build.get('status') == 'succeeded' and bool(build.get('image', {}).get('manifest_digest'))
        if not reused:
            build = client.wait_for_image_build(build['build_id'], timeout_seconds=1200, poll_interval_seconds=5)
            save(receipt_path, build)
        if build.get('status') == 'succeeded':
            return build, reused
        if retry_failed and attempt == 0 and build.get('status') in {'failed', 'cancelled'}:
            history = receipt_path.parent / 'attempts'
            history.mkdir(exist_ok=True)
            save(history / (hashlib.sha256(build['build_id'].encode()).hexdigest() + '.json'), build)
            build = None
            continue
        raise RuntimeError('shared image build failed: ' + str(build.get('error'))[-1000:])
    raise AssertionError('unreachable build retry state')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', required=True)
    parser.add_argument('--pinned-source', help='immutable public source for reproducible recovery')
    parser.add_argument('--family', action='append', default=[])
    parser.add_argument('--anchor', required=True, help='prepared private image pinned by manifest digest')
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--anchor-cache', type=Path, required=True)
    parser.add_argument('--filesystem-exports', type=Path, help='protected offline exports for multi-layer source qualification')
    parser.add_argument('--anchor-filesystem-source', help='pinned original flat source of a qualified compact anchor')
    parser.add_argument('--gateway', required=True)
    parser.add_argument('--sdk-wheel', type=Path, required=True)
    parser.add_argument('--config', type=Path, default=Path('/etc/ucloud-sandboxes/deployment.json'))
    parser.add_argument('--free-floor-gib', type=int, default=500)
    parser.add_argument('--max-layer-gib', type=int, default=2)
    parser.add_argument('--max-delta-mib', type=int, default=256)
    parser.add_argument('--retry-recorded-failures', action='store_true')
    parser.add_argument('--compression-level', type=int, choices=range(1, 10), default=9)
    args = parser.parse_args()
    args.export_inputs = read_filesystem_exports(args.filesystem_exports)
    if args.export_inputs and args.anchor_filesystem_source:
        parser.error('choose exported filesystems or a flat anchor source')
    if min(args.free_floor_gib, args.max_layer_gib, args.max_delta_mib) < 1:
        parser.error('storage limits must be positive')
    if '@sha256:' not in args.anchor:
        parser.error('anchor must be immutable')
    args.root.mkdir(parents=True, exist_ok=True)
    with (args.root / 'prepare.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        identity_path, resolved_path = args.root / 'identity.json', args.root / 'resolved.json'
        identity = source_identity(args.source, args.anchor, args.pinned_source, args.export_inputs,
                                   args.compression_level, args.anchor_filesystem_source)
        if identity_path.exists() and resolved_path.exists() and json.loads(identity_path.read_text()) == identity:
            resolved = json.loads(resolved_path.read_text())
            old_key = hashlib.sha256(encode({**identity, 'source': resolved['reference']})).hexdigest()
            remove_abandoned_inputs(args.root, old_key)
        prepare(args)


def prepare(args):
    sys.path.insert(0, str(args.sdk_wheel))
    import ucloud_sandboxes_sdk as sdk
    from ucloud_sandboxes.config import DeploymentConfig
    from ucloud_sandboxes.control_plane import _persist_registry_image_protection
    from ucloud_sandboxes.environment_artifact import bind_source_layers, load_image_environment, require_digest
    from ucloud_sandboxes.environment_config import environment_registry_from_deployment
    from ucloud_sandboxes.environment_dependencies import EnvironmentDependencyResolver
    from ucloud_sandboxes.host_locks import HOST_LOCKS
    from ucloud_sandboxes.image_rootfs import DockerImageConfig
    from ucloud_sandboxes.images import ImageRecord, ImageStore
    from ucloud_sandboxes.managed_registry import RegistryRequestError, RegistryUsageStore, registry_repository_tag_from_image_ref
    from ucloud_sandboxes.registry_disk import registry_disk_usage

    started = time.monotonic()
    c = DeploymentConfig.from_dict(json.loads(args.config.read_text()))
    HOST_LOCKS.configure(c.control_state_file().parent / 'gateway-locks')
    registry = environment_registry_from_deployment(c)
    usage = RegistryUsageStore(c.registry_usage_file())
    dependencies = EnvironmentDependencyResolver(registry)
    client = sdk.SandboxClient(args.gateway, api_token=c.sandbox_api_token_file().read_text().strip(), timeout_seconds=120)
    from urllib.parse import urlparse
    authority = urlparse(c.registry_worker_url).netloc
    if args.anchor.split('/')[0] != authority:
        raise ValueError('anchor must belong to the configured registry')
    anchor_repo, _ = registry_repository_tag_from_image_ref(args.anchor)
    anchor_digest = args.anchor.split('@')[1]
    _, anchor_environment = load_image_environment(registry, anchor_repo, anchor_digest)
    anchor_components = [registry.load(d) for d in anchor_environment.components]
    if any(component.format.get('layout') != 1 for component in anchor_components):
        raise ValueError('unknown anchor filesystem format')
    anchor_manifest, _ = registry.client.manifest_document(anchor_repo, anchor_digest)
    if len(anchor_manifest['layers']) != 1 and not (args.export_inputs or args.anchor_filesystem_source):
        raise ValueError('anchor must be a single flat OCI layer')
    if not anchor_manifest['layers']:
        raise ValueError('anchor has no filesystem layers')
    if args.anchor_filesystem_source and len(anchor_manifest['layers']) > 8:
        raise ValueError('compact anchor exceeds layer-chain bound')
    anchor_layer = anchor_manifest['layers'][0]
    require_digest(anchor_layer['digest'])
    maximum = (5 if args.export_inputs else args.max_layer_gib) * 1024**3
    if sum(layer['size'] for layer in anchor_manifest['layers']) > maximum:
        raise ValueError('anchor exceeds compressed input bound')
    # Leave enough space for inputs, decompressed metadata and concurrent ordinary builds.
    disk = registry_disk_usage(c)
    if disk is None or disk.available_bytes < args.free_floor_gib * 1024**3 + maximum * 2:
        raise RuntimeError('deferred: registry free-space reserve')
    if shutil.disk_usage(args.root).free < 10 * 1024**3 + maximum * 3:
        raise RuntimeError('deferred: preparation scratch reserve')
    anchor_data = registry.client.blob_bytes(anchor_repo, anchor_manifest['config']['digest'])
    if 'sha256:' + hashlib.sha256(anchor_data).hexdigest() != anchor_manifest['config']['digest']:
        raise ValueError('anchor config identity mismatch')
    anchor_config = json.loads(anchor_data)
    identity = source_identity(args.source, args.anchor, args.pinned_source, args.export_inputs,
                               args.compression_level, args.anchor_filesystem_source)
    identity_path = args.root / 'identity.json'
    if identity_path.exists() and json.loads(identity_path.read_text()) != identity:
        raise ValueError('work directory belongs to another source')
    save(identity_path, identity)
    resolved_path = args.root / 'resolved.json'
    if resolved_path.exists():
        resolved = json.loads(resolved_path.read_text())
    else:
        resolved = SourceResolver(c.control_state_file().parent / 'image-pool-locks', max_wait_seconds=60)(args.pinned_source or args.source)
        save(resolved_path, resolved)
    if args.pinned_source and registry_parts(resolved['reference']) != registry_parts(args.pinned_source):
        raise ValueError('resolved source differs from requested immutable pin')
    manifest_bytes = resolved['manifest_json'].encode()
    if 'sha256:' + hashlib.sha256(manifest_bytes).hexdigest() != resolved['reference'].split('@')[1]:
        raise ValueError('resolved source manifest identity mismatch')
    target_manifest = json.loads(manifest_bytes)
    config_bytes = resolved['config_json'].encode()
    descriptor = target_manifest['config']
    if len(config_bytes) != descriptor['size'] or 'sha256:' + hashlib.sha256(config_bytes).hexdigest() != descriptor['digest']:
        raise ValueError('resolved source config identity mismatch')
    target_config = json.loads(config_bytes)
    if ((not args.export_inputs and (len(target_manifest['layers']) != 1
                                    or len(target_config['rootfs']['diff_ids']) != 1
                                    or (not args.anchor_filesystem_source and len(anchor_config['rootfs']['diff_ids']) != 1)))
            or not target_manifest['layers'] or (target_config.get('config') or {}).get('OnBuild')
            or target_config.get('architecture') != 'amd64' or target_config.get('os') != 'linux'):
        raise ValueError('source requires unsupported image preparation')
    target_layer = target_manifest['layers'][0]
    require_digest(target_layer['digest'])
    target_compressed_bytes = sum(layer['size'] for layer in target_manifest['layers'])
    save(args.root / 'cost.json', {'source': args.source, 'source_reference': resolved['reference'],
                                   'source_compressed_bytes': target_compressed_bytes})
    if target_compressed_bytes > maximum:
        raise RuntimeError('deferred: source exceeds compressed input bound')
    if args.export_inputs:
        validate_filesystem_export(args.export_inputs['anchor'], args.anchor, anchor_manifest, anchor_config)
        validate_filesystem_export(args.export_inputs['target'], resolved['reference'], target_manifest, target_config)
    anchor_filesystem = None
    if args.anchor_filesystem_source:
        anchor_receipt = args.root / 'anchor-resolved.json'
        if anchor_receipt.exists():
            anchor_filesystem = json.loads(anchor_receipt.read_text())
        else:
            anchor_filesystem = SourceResolver(c.control_state_file().parent / 'image-pool-locks', max_wait_seconds=60)(args.anchor_filesystem_source)
            save(anchor_receipt, anchor_filesystem)
        anchor_layer = flat_anchor_source(anchor_filesystem, args.anchor_filesystem_source, maximum)
    key = hashlib.sha256(encode({**identity, 'source': resolved['reference']})).hexdigest()
    image_id, repository, tag = 'shared-task-' + key[:32], 'ucloud-shared-sources', 'flat-v1-' + key
    remove_abandoned_inputs(args.root, key)
    print(json.dumps({'source': args.source, 'status': 'indexing_verified_inputs'}), flush=True)
    with TemporaryDirectory(prefix='inputs-', dir=args.root) as temporary:
        scratch = Path(temporary)
        (scratch / '.owner.json').write_text(json.dumps({'source_key': key}))
        if args.export_inputs:
            anchor_export = args.export_inputs['anchor']
            anchor_index = cached_anchor(registry.client, anchor_repo, {
                'digest': 'sha256:' + anchor_export['export_sha256'], 'size': anchor_export['export_bytes']},
                args.anchor_cache, scratch, source_path=args.filesystem_exports / 'anchor/filesystem.tar.gz')
            blob = args.filesystem_exports / 'target/filesystem.tar.gz'
            input_digest = 'sha256:' + args.export_inputs['target']['export_sha256']
        else:
            if anchor_filesystem:
                anchor_host, original_repo, _ = registry_parts(anchor_filesystem['reference'])
                anchor_index = cached_anchor(PublicLayerReader(anchor_host), original_repo, anchor_layer, args.anchor_cache, scratch)
            else:
                anchor_index = cached_anchor(registry.client, anchor_repo, anchor_layer, args.anchor_cache, scratch)
            host, source_repo, _ = registry_parts(resolved['reference'])
            endpoint = 'registry-1.docker.io' if host == 'docker.io' else host
            url = f'https://{endpoint}/v2/{source_repo}/blobs/{target_layer["digest"]}'
            opener = request.build_opener(PublicBlobRedirect()).open
            blob = scratch / 'target.tar.gz'
            with opener(request.Request(url, headers=public_registry_headers(host, source_repo)), timeout=120) as stream:
                copy_verified(stream, blob, target_layer)
            input_digest = target_layer['digest']
        with verified_tar(blob, input_digest) as stream:
            target_index = index_flat_tar(stream)
        plan = plan_flat_delta(anchor_index, target_index)
        save(args.root / 'cost.json', {'source': args.source, 'source_reference': resolved['reference'],
                                      'source_compressed_bytes': target_compressed_bytes,
                                      'changed_regular_file_bytes': plan.regular_file_bytes,
                                      'changed_paths': len(plan.changed), 'removed_paths': len(plan.removed)})
        if plan.regular_file_bytes > args.max_delta_mib * 1024**2:
            raise RuntimeError('deferred: changed-file delta exceeds preparation bound')
        delta = scratch / 'delta.tar.gz'
        with delta.open('wb') as raw:
            with gzip.GzipFile(fileobj=raw, mode='wb', filename='', mtime=0, compresslevel=args.compression_level) as compressed:
                writer = CheckedWriter(compressed)
                with verified_tar(blob, input_digest) as stream:
                    write_flat_delta(stream, writer, target_index, plan)
        delta_hash = hashlib.sha256()
        with delta.open('rb') as stream:
            while chunk := stream.read(1024 * 1024):
                delta_hash.update(chunk)
        delta_digest = 'sha256:' + delta_hash.hexdigest()
        delta_size = delta.stat().st_size
        if delta_size > maximum:
            raise RuntimeError('deferred: delta exceeds storage bound')
        print(json.dumps({'source': args.source, 'status': 'delta_ready', 'compressed_bytes': delta_size}), flush=True)
        new_config = dict(target_config)
        new_config['rootfs'] = {'type': 'layers', 'diff_ids': anchor_config['rootfs']['diff_ids'] + ['sha256:' + writer.digest.hexdigest()]}
        new_config['history'] = anchor_config.get('history', []) + [{'created_by': 'ucloud verified flat delta v1'}]
        config_data = encode(new_config)
        config_digest = 'sha256:' + hashlib.sha256(config_data).hexdigest()
        config_path = scratch / 'config.json'
        config_path.write_bytes(config_data)
        manifest = {'schemaVersion': 2, 'mediaType': 'application/vnd.oci.image.manifest.v1+json',
                    'config': {'mediaType': 'application/vnd.oci.image.config.v1+json', 'digest': config_digest, 'size': len(config_data)},
                    'layers': anchor_manifest['layers'] + [{'mediaType': 'application/vnd.oci.image.layer.v1.tar+gzip',
                                                           'digest': delta_digest, 'size': delta_size}]}
        data = encode(manifest)
        digest = 'sha256:' + hashlib.sha256(data).hexdigest()
        reference = authority + '/' + repository + ':' + tag + '@' + digest
        try:
            previous_digest = registry.client.manifest_digest(repository, tag)
        except RegistryRequestError as error:
            if error.status_code != 404:
                raise
            previous_digest = None
        if previous_digest is not None and previous_digest != digest:
            raise ValueError('immutable shared-source tag conflict')
        if not _persist_registry_image_protection(usage, reference, 'shared-source:' + key, touch=True, persistent=True):
            raise RuntimeError('shared source retention failed')
        for layer in anchor_manifest['layers']:
            if not registry.client.mount_blob(repository, anchor_repo, layer['digest']):
                raise RuntimeError('anchor blob mount failed')
        registry.client.upload_blob_file(repository, delta, delta_digest, delta_size)
        registry.client.upload_blob_file(repository, config_path, config_digest, len(config_data))
        if registry.client.put_manifest(repository, tag, data) != digest:
            raise ValueError('registry changed shared-source identity')
        registry.client.ensure_digest_protection_tag(repository, digest)
        mount_source_layers(registry.client, repository, manifest['layers'],
                            publication_repositories(image_id, c.registry_worker_url, c.builder.buildx_cache_ref))
        context = args.root / 'context'
        context.mkdir(exist_ok=True)
        (context / 'Dockerfile').write_text('FROM ' + reference + '\n')
        receipt_path = args.root / 'build.json'
        build, artifact_reused = complete_build(
            client, sdk.Image.from_dockerfile(name=image_id, context_path=context), receipt_path,
            retry_failed=args.retry_recorded_failures)
        published = build['image']
        prepared = published['tag'].split('@')[0] + '@' + published['manifest_digest']
        prepared_repo, _ = registry_repository_tag_from_image_ref(prepared)
        environment_root, environment = load_image_environment(registry, prepared_repo, published['manifest_digest'])
        components = [registry.load(d) for d in environment.components]
        if DockerImageConfig.from_inspection(environment.image_config) != DockerImageConfig.from_inspection(target_config['config']):
            raise ValueError('prepared runtime configuration differs from source')
        if not _persist_registry_image_protection(usage, prepared, 'shared-task:' + key, touch=True, persistent=True,
                                                 dependency_resolver=dependencies):
            raise RuntimeError('prepared artifact retention failed')
        final_config_data = registry.client.blob_bytes(prepared_repo, environment.source_image)
        if 'sha256:' + hashlib.sha256(final_config_data).hexdigest() != environment.source_image:
            raise ValueError('prepared OCI config identity mismatch')
        final_config = json.loads(final_config_data)
        if final_config['rootfs']['diff_ids'] != new_config['rootfs']['diff_ids']:
            raise ValueError('prepared source layers changed')
        bind_source_layers(components, final_config['rootfs']['diff_ids'])
        worker_hash = hashlib.sha256()
        with c.sandbox_node_package_bundle().open('rb') as stream:
            while chunk := stream.read(1024 * 1024):
                worker_hash.update(chunk)
        certificate_key = qualification_key(target_index, components, environment.image_config, worker_hash.hexdigest())
        certificates = args.anchor_cache.parent / 'filesystem-qualifications'
        certificates.mkdir(exist_ok=True)
        certificate_path = certificates / (certificate_key + '.json')
        print(json.dumps({'source': args.source, 'status': 'qualifying_source_filesystem'}), flush=True)
        with certificate_path.with_suffix('.lock').open('a') as certificate_lock:
            fcntl.flock(certificate_lock, fcntl.LOCK_EX)
            certificate = None
            if certificate_path.exists():
                saved = json.loads(certificate_path.read_text())
                payload = saved['payload']
                if hashlib.sha256(encode(payload)).hexdigest() != saved['checksum'] or payload['key'] != certificate_key:
                    raise ValueError('filesystem qualification certificate identity mismatch')
                if payload['expires_at'] > time.time() and payload['qualification']['equivalent']:
                    certificate = payload
            sandbox = 'shared-source-check-' + uuid4().hex[:16]
            try:
                client.create_sandbox(sdk.SandboxSpec(id=sandbox, image=sdk.Image.from_registry(prepared), command=['sleep', '1200'],
                    cpus=1, memory_mb=2048, disk_mb=512, ttl_seconds=1200,
                    security=sdk.SandboxSecuritySpec(user='0:0', cap_add=('DAC_READ_SEARCH',))), request_timeout_seconds=300)
                if certificate is None:
                    scanned = client.exec(sandbox, ['python3', '-B', '-c', SCANNER], timeout_seconds=600)
                    if scanned.exit_code != 0:
                        raise RuntimeError('source qualification scan failed: ' + scanned.stderr[-1000:])
                    snapshot = json.loads(gzip.decompress(client.download_file(sandbox, '/tmp/ucloud-filesystem-proof.json.gz')))
                    qualification = compare_snapshot(target_index, snapshot, layer_formats=[component.format for component in components])
                    save(args.root / 'qualification.json', qualification)
                    if not qualification['equivalent']:
                        raise ValueError('shared image differs from authenticated source')
                    qualification['mode'] = 'full_source_scan'
                    certificate = {'key': certificate_key, 'reference': prepared, 'qualification': qualification,
                                   'expires_at': time.time() + 24 * 3600}
                    save(certificate_path, {'payload': certificate, 'checksum': hashlib.sha256(encode(certificate)).hexdigest()})
                else:
                    # Still exercise each new signed image's mount and executable path.
                    checked = client.exec(sandbox, ['/bin/sh', '-c', 'test -r /etc/os-release && test -d /'], timeout_seconds=30)
                    if checked.exit_code != 0:
                        raise RuntimeError('byte-identical filesystem smoke failed')
                    qualification = {**certificate['qualification'], 'mode': 'identical_filesystem_certificate',
                                     'certificate_reference': certificate['reference']}
                qualification['certificate_key'] = certificate_key
                save(args.root / 'qualification.json', qualification)
            finally:
                client.delete_sandbox(sandbox)
        store = ImageStore(c.image_file())
        record = ImageRecord.from_dict({k: v for k, v in published.items() if k in ImageRecord.__dataclass_fields__})
        store.upsert_if_changed(record)
        aliases = {args.source, resolved['reference']}
        original_host, original_repo, selector = registry_parts(args.source)
        separator = '@' if selector.startswith('sha256:') else ':'
        aliases.add(original_host + '/' + original_repo + separator + selector)
        if original_host == 'docker.io':
            aliases.add(original_repo + separator + selector)
        result = {'source': args.source, 'retention_owner': 'shared-task:' + key, 'status': 'ready', 'preparation': 'source',
                  'method': 'verified-oci-delta-v1' if args.export_inputs else 'verified-flat-delta-v1',
                  'source_reference': resolved['reference'], 'anchor': args.anchor, 'reference': prepared, 'image_id': image_id,
                  'environment_root': environment_root, 'key': key,
                  'components': [{'digest': component.image_digest, 'bytes': component.image_size} for component in components],
                  'delta_compressed_bytes': delta_size, 'changed_regular_file_bytes': plan.regular_file_bytes,
                  'original_compressed_bytes_not_retained': target_compressed_bytes, 'qualification': qualification,
                  'import_aliases': {alias: register_import_alias(store, record, alias) for alias in sorted(aliases)},
                  'seconds': time.monotonic() - started, 'artifact_reused': artifact_reused, 'timings': build.get('timings', {})}
        if args.anchor_filesystem_source:
            result['anchor_filesystem_source'] = args.anchor_filesystem_source
        if args.family:
            result['families'] = args.family
        save(args.root / 'catalog.json', {'schema': 1, 'images': {args.source: result}})
        print(json.dumps({k: result[k] for k in ('source', 'status', 'delta_compressed_bytes', 'original_compressed_bytes_not_retained', 'seconds')}), flush=True)


if __name__ == '__main__':
    main()
