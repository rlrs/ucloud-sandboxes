"""Choose a retained dependency base once, before a preparation gets an identity."""
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import shutil
import sqlite3
from tempfile import TemporaryDirectory

from cache_source_receipts import validated
from prepare_image_pool import SourceResolver, registry_parts
from prepare_shared_task_image import (
    PublicLayerReader, copy_verified, encode, read_flat_index, remove_abandoned_inputs,
    save, source_identity, verified_tar,
)
from shared_dependency_index import rank_anchors
from ucloud_sandboxes.oci_flat_delta import index_flat_tar, plan_flat_delta


def request_identity(args):
    return source_identity(args.source, args.anchor, args.pinned_source, None,
                           args.compression_level, args.anchor_filesystem_source)


def restore_selection(args, selection):
    if selection['schema'] != 1 or selection['request'] != request_identity(args):
        raise ValueError('dependency selection belongs to another request')
    chosen = selection['chosen']
    args.anchor = chosen['reference']
    args.anchor_filesystem_source = None if chosen.get('original') else chosen['source_reference']
    args._dependency_selection = selection


@contextmanager
def dependency_selection(args):
    """Existing assignments are immutable; new jobs reuse their one source download."""
    receipt = args.root / 'dependency-selection.json'
    requested = request_identity(args)
    request_path = args.root / 'selection-request.json'
    if request_path.exists() and json.loads(request_path.read_text()) != requested:
        raise ValueError('dependency selection belongs to another request')
    scratch_key = hashlib.sha256(encode({'dependency_selection': 1, 'request': requested})).hexdigest()
    remove_abandoned_inputs(args.root, scratch_key)
    if receipt.exists():
        selection = json.loads(receipt.read_text())
        resolved = validated(args.source, json.loads((args.root / 'selection-source.json').read_text()))
        if resolved['reference'] != selection['source_reference']:
            raise ValueError('saved dependency selection source changed')
        restore_selection(args, selection)
        args._dependency_source_metadata = resolved
        yield
        return
    # The flag affects new jobs only. It never rebases an accepted build or
    # changes legacy jobs' identities while a coordinator is resuming.
    if args.dependency_index is None or (args.root / 'identity.json').exists():
        yield
        return
    if args.export_inputs:
        raise ValueError('dependency selection currently requires flat sources')
    save(request_path, requested)
    with sqlite3.connect(args.dependency_index.resolve().as_uri() + '?mode=ro', uri=True) as db:
        candidates = [json.loads(row[0]) for row in db.execute('SELECT payload FROM anchors')]
    from ucloud_sandboxes.config import DeploymentConfig
    from ucloud_sandboxes.managed_registry import RegistryClient, registry_repository_tag_from_image_ref
    from ucloud_sandboxes.registry_disk import registry_disk_usage
    config = DeploymentConfig.from_file(args.config)
    baseline = next((r for r in candidates if r['reference'] == args.anchor), None)
    if baseline is None and not args.anchor_filesystem_source:
        from urllib.parse import urlparse
        if args.anchor.split('/')[0] != urlparse(config.registry_worker_url).netloc:
            raise ValueError('dependency fallback must belong to the configured registry')
        repo, _ = registry_repository_tag_from_image_ref(args.anchor)
        manifest, _ = RegistryClient(config.registry_url).manifest_document(repo, args.anchor.split('@')[1])
        if len(manifest['layers']) == 1:
            layer = manifest['layers'][0]
            path = args.anchor_cache / (layer['digest'].split(':')[1] + '.json.gz')
            if path.exists():
                baseline = {'source': args.anchor, 'reference': args.anchor, 'original': True,
                            'index_path': str(path), 'layer': layer}
    if baseline is None:
        yield
        return
    maximum = args.max_layer_gib * 1024**3
    disk = registry_disk_usage(config)
    if disk is None or disk.available_bytes < args.free_floor_gib * 1024**3 + maximum * 2:
        raise RuntimeError('deferred: registry free-space reserve')
    if shutil.disk_usage(args.root).free < 10 * 1024**3 + maximum * 3:
        raise RuntimeError('deferred: preparation scratch reserve')
    resolved_path = args.root / 'selection-source.json'
    if resolved_path.exists():
        resolved = validated(args.source, json.loads(resolved_path.read_text()))
    elif (args.root / 'resolved.json').exists():
        resolved = validated(args.source, json.loads((args.root / 'resolved.json').read_text()))
        save(resolved_path, resolved)
    else:
        resolved = validated(args.source, SourceResolver(config.control_state_file().parent / 'image-pool-locks',
                             max_wait_seconds=60)(args.pinned_source or args.source))
        save(resolved_path, resolved)
    if args.pinned_source and registry_parts(args.pinned_source) != registry_parts(resolved['reference']):
        raise ValueError('dependency selection source differs from requested pin')
    save(args.root / 'cost.json', {'source': args.source, 'source_reference': resolved['reference'],
                                 'source_compressed_bytes': resolved['compressed_bytes']})
    if resolved['layer_count'] != 1 or resolved['onbuild']:
        raise ValueError('dependency selection needs an original flat source')
    if resolved['compressed_bytes'] > maximum:
        raise RuntimeError('deferred: source exceeds compressed input bound')
    with TemporaryDirectory(prefix='inputs-', dir=args.root) as temporary:
        scratch = Path(temporary)
        save(scratch / '.owner.json', {'source_key': scratch_key})
        blob = scratch / 'target.tar.gz'
        host, repository, _ = registry_parts(resolved['reference'])
        with PublicLayerReader(host).open_blob(repository, resolved['layers'][0]['digest']) as stream:
            copy_verified(stream, blob, resolved['layers'][0])
        with verified_tar(blob, resolved['layers'][0]['digest']) as stream:
            target = index_flat_tar(stream)
        baseline_index = read_flat_index(Path(baseline['index_path']), baseline['layer'])
        previous = plan_flat_delta(baseline_index, target).regular_file_bytes
        ranked = rank_anchors(args.dependency_index, target, excluded_source=args.source, limit=4)
        chosen = baseline
        cost = previous
        if ranked and ranked[0]['changed_regular_file_bytes'] < previous:
            candidate = ranked[0]
            saving = previous - candidate['changed_regular_file_bytes']
            # Keep the existing base unless savings justify changing the chain.
            if saving >= 16 * 1024**2 and saving >= previous * .2:
                chosen, cost = candidate, candidate['changed_regular_file_bytes']
        selection = {'schema': 1, 'request': requested, 'source_reference': resolved['reference'],
                     'chosen': chosen, 'previous_changed_bytes': previous, 'selected_changed_bytes': cost}
        save(receipt, selection)
        restore_selection(args, selection)
        args._verified_source_input = (blob, target, resolved)
        print(json.dumps({'source': args.source, 'status': 'dependency_base_selected',
                          'anchor_source': chosen['source'], 'previous_changed_bytes': previous,
                          'selected_changed_bytes': cost}), flush=True)
        yield
