#!/usr/bin/env python3
"""Run a fixed base expansion sequentially under one persistent disk baseline.

Metadata failures defer individual sources. No dataset membership, task-completion
queue, serving service, volume size, or registry garbage collection is changed.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import fcntl
import hashlib
import json
from pathlib import Path
import subprocess
import sys

from cache_source_receipts import load, select, validated
from plan_base_expansion import shared_sources
from prepare_image_pool import GIB, SourceResolver, save


def initialize_budget(path, used_bytes, identity):
    if path.exists():
        state = json.loads(path.read_text())
        if state['identity'] != identity:
            raise ValueError('expansion inputs changed; use a fresh root')
        return state
    state = {'schema': 1, 'identity': identity, 'initial_used_bytes': used_bytes, 'completed_stages': []}
    save(path, state)
    return state


def stage_catalog(root, baseline, kind):
    path = root / ('budget.json' if kind == 'shared' else 'catalog.json')
    if path.exists():
        if json.loads(path.read_text())['initial_used_bytes'] != baseline:
            raise ValueError('stage storage baseline differs from expansion')
        return
    value = {'initial_used_bytes': baseline}
    if kind != 'shared':
        value.update(schema=1, **{'foundations' if kind == 'prefix' else 'images': {}})
    save(path, value)


def translated_growth_cap(baseline, old_baseline, cumulative_gib):
    cap = (baseline-old_baseline)//GIB+cumulative_gib
    if cap < 1:
        raise ValueError('ScaleSWE budget incompatible with expansion baseline')
    return cap


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for name in ['root', 'source_receipts', 'base_catalog', 'sdk_wheel', 'dependency_index']:
        p.add_argument('--'+name.replace('_', '-'), type=Path, required=True)
    p.add_argument('--gateway', required=True)
    p.add_argument('--fallback-anchor-source', required=True)
    p.add_argument('--config', type=Path, default=Path('/etc/ucloud-sandboxes/deployment.json'))
    p.add_argument('--prepared-prefix-root', type=Path, action='append', default=[])
    p.add_argument('--scale-campaign-root', type=Path,
                   help='resume existing ScaleSWE seeds last, within a cumulative 192 GiB cap')
    args = p.parse_args()
    from ucloud_sandboxes.config import DeploymentConfig
    from ucloud_sandboxes.registry_disk import registry_disk_usage
    c = DeploymentConfig.from_file(args.config)
    root = args.root
    with (root / 'expansion.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        inputs = [root/'sources/plan.json', args.source_receipts, args.base_catalog, args.dependency_index]
        inputs.extend(sorted(root.glob('*/plan.json')))
        if args.scale_campaign_root:
            inputs.extend(sorted((args.scale_campaign_root/'inputs').glob('*.json')))
            inputs.append(args.scale_campaign_root/'budget.json')
        identity = {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in inputs}
        # Staged plans live below stages/, so they cannot alter this identity.
        disk = registry_disk_usage(c)
        if disk is None:
            raise ValueError('registry disk usage unavailable')
        state_path = root/'run-state.json'
        state = initialize_budget(state_path, disk.used_bytes, identity)
        baseline = state['initial_used_bytes']
        metadata_root = root/'metadata'
        metadata_root.mkdir(exist_ok=True)
        selected = json.loads((root/'sources/plan.json').read_text())
        original_shared = root/'shared-sources/plan.json'
        if original_shared.exists():
            selected['images'].extend(json.loads(original_shared.read_text())['images'])
        if len({r['source'] for r in selected['images']}) != len(selected['images']):
            raise ValueError('duplicate selected sources')
        known = load(args.source_receipts)
        resolver = SourceResolver(c.control_state_file().parent/'image-pool-locks', max_wait_seconds=30)

        def preflight(item):
            source = item['source']
            path = metadata_root/(hashlib.sha256(source.encode()).hexdigest()+'.json')
            if path.exists():
                previous = json.loads(path.read_text())
                if 'public registry cooldown' not in (previous.get('reason') or ''):
                    return previous
            try:
                resolved = select(source, item.get('pinned_source'), known.get(source, {}))
                if resolved is None:
                    resolved = resolver(item.get('pinned_source', source))
                resolved = validated(source, resolved)
                if item.get('pinned_source') and item['pinned_source'] != resolved['reference']:
                    raise ValueError('source pin differs from resolved metadata')
                reason = ('compressed input exceeds 2 GiB' if resolved['compressed_bytes'] > 2*GIB else
                          'inherited ONBUILD needs separate preparation' if resolved['onbuild'] else None)
                row = {'source': source, 'status': 'deferred' if reason else 'admitted',
                       'reason': reason, 'resolved': resolved}
            except Exception as exc:
                row = {'source': source, 'status': 'deferred', 'reason': str(exc)[-500:]}
            save(path, row)
            print(json.dumps({k: row[k] for k in ['source', 'status', 'reason']}), flush=True)
            return row

        stages = root/'stages'
        if 'metadata' not in state['completed_stages']:
            with ThreadPoolExecutor(max_workers=2) as workers:
                rows = list(workers.map(preflight, selected['images']))
            by_source = {r['source']: r for r in rows}
            admitted = {**selected, 'images': [{**r, 'pinned_source': by_source[r['source']]['resolved']['reference']}
                for r in selected['images'] if by_source[r['source']]['status'] == 'admitted']}
            resolutions = {r['source']: {r['resolved']['reference']: r['resolved']}
                           for r in rows if r['status'] == 'admitted'}
            normal, shared = shared_sources(admitted, json.loads(args.base_catalog.read_text()),
                                             resolutions, args.fallback_anchor_source)
            for name, plan in [('sources', normal), ('shared-sources', shared)]:
                work = stages/name
                work.mkdir(parents=True, exist_ok=True)
                save(work/'plan.json', plan)
                for item in plan['images']:
                    key = hashlib.sha256(item['source'].encode()).hexdigest()
                    path = work/(key+'.json') if name == 'sources' else work/'work'/key/'resolved.json'
                    path.parent.mkdir(parents=True, exist_ok=True)
                    resolved = by_source[item['source']]['resolved']
                    save(path, {'source': item['source'], 'resolved': resolved} if name == 'sources' else resolved)
            state['metadata_counts'] = {s: sum(r['status'] == s for r in rows) for s in ['admitted', 'deferred']}
            state['completed_stages'].append('metadata')
            save(state_path, state)

        # The same baseline covers every stage. Unused earlier allowance may
        # carry forward, but restarting a stage never creates another budget.
        for name, cap in [('shared-sources', 64), ('sources', 64), ('terminal-prefix', 96), ('tmax-inline', 128)]:
            if name in state['completed_stages']:
                continue
            work = stages/name if 'sources' in name else root/name
            plan_path = work/'plan.json'
            if not plan_path.exists():
                continue
            shared = name == 'shared-sources'
            prefix = 'sources' not in name
            plan = json.loads(plan_path.read_text())
            count = len(plan['foundations' if prefix else 'images'])
            if not count:
                continue
            stage_catalog(work, baseline, 'prefix' if prefix else 'shared' if shared else 'normal')
            cmd = [sys.executable, str(Path(__file__).with_name('prepare_image_foundations.py' if prefix else
                'prepare_shared_task_pool.py' if shared else 'prepare_image_pool.py')),
                '--root', str(work), '--config', str(args.config), '--gateway', args.gateway,
                '--sdk-wheel', str(args.sdk_wheel), '--workers', '2', '--limit', str(count),
                '--growth-limit-gib', str(cap), '--free-floor-gib', '500']
            if shared:
                cmd += ['--dependency-index', str(args.dependency_index), '--record-source-index',
                        '--max-delta-mib', '1024', '--compression-level', '6']
            elif prefix:
                cmd += ['--base-catalog', str(args.base_catalog), '--reservation-gib', '8']
                for existing in args.prepared_prefix_root:
                    cmd += ['--prepared-prefix-root', str(existing)]
            else:
                cmd += ['--stage-upstream', '--max-image-gib', '2']
            state['active_stage'] = name
            save(state_path, state)
            print(json.dumps({'stage': name, 'cumulative_growth_cap_gib': cap, 'planned': count}), flush=True)
            subprocess.run(cmd, check=True)
            state['completed_stages'].append(name)
            save(state_path, state)
        if args.scale_campaign_root and 'scale-seeds' not in state['completed_stages']:
            campaign = args.scale_campaign_root
            old_baseline = json.loads((campaign/'budget.json').read_text())['initial_used_bytes']
            # Preserve the old campaign baseline and cap total new growth from
            # this expansion's baseline. Round DOWN, never grant extra bytes.
            cap = translated_growth_cap(baseline, old_baseline, 192)
            inputs = campaign/'inputs'
            cmd = [sys.executable, str(Path(__file__).with_name('prepare_project_image_campaign.py')),
                '--root', str(campaign), '--inventory', str(inputs/'inventory.json'),
                '--catalog', str(inputs/'project-campaign-initial-catalog.json'),
                '--fallback-anchor-source', args.fallback_anchor_source, '--gateway', args.gateway,
                '--sdk-wheel', str(args.sdk_wheel), '--config', str(args.config), '--workers', '2',
                '--growth-limit-gib', str(cap), '--free-floor-gib', '500', '--compression-level', '6',
                '--seed-max-delta-mib', '1024', '--dependency-index', str(args.dependency_index), '--seeds-only']
            for name in ['same-project-plan.json', 'compact-anchor-canary-plan-r2.json', 'project-campaign-pilot-exclusions.json']:
                cmd += ['--exclude-plan', str(inputs/name)]
            state['active_stage'] = 'scale-seeds'
            save(state_path, state)
            subprocess.run(cmd, check=True)
            state['completed_stages'].append('scale-seeds')
        state['active_stage'] = None
        state['status'] = 'finished; inspect individual deferred and failed receipts'
        save(state_path, state)


if __name__ == '__main__':
    main()
