#!/usr/bin/env python3
"""Prepare one compact base per missing project, then its remaining task images.

Both phases preserve the same observed registry-growth baseline. A source is
ready only after the shared preparer's full filesystem qualification succeeds.
"""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys

from plan_shared_task_pool import plan_shared
from prepare_image_pool import save


def project(source):
    return re.sub(r'_pr\d+$', '', source)


def seed_plan(inventory, catalogs, fallback, excluded):
    candidates = plan_shared(inventory, catalogs, fallback_anchor_source=fallback,
                             exclude_sources=excluded, allow_compact_anchors=True)
    selected = {}
    for row in candidates['images']:
        if row['anchor_strategy'] == 'shared_fallback':
            selected.setdefault(project(row['source']), row)
    return {'schema': 1, 'scope': 'One source-qualified compact base per missing project; not coverage.',
            'images': list(selected.values())}


def stage_inputs(root, plan, baseline):
    root.mkdir(exist_ok=True)
    budget = root / 'budget.json'
    if budget.exists() and json.loads(budget.read_text()) != baseline:
        raise ValueError('stage storage baseline differs from campaign baseline')
    if not budget.exists():
        save(budget, baseline)
    path = root / 'plan.json'
    if path.exists():
        previous = json.loads(path.read_text())
        assigned = {row['source'] for row in previous['images']}
        # New successful seeds may unlock another project on resume. Extend
        # the tail without changing any already assigned source's anchor.
        previous['images'].extend(row for row in plan['images'] if row['source'] not in assigned)
        save(path, previous)
        return previous
    save(path, plan)
    return plan


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', required=True, type=Path)
    parser.add_argument('--inventory', required=True, type=Path)
    parser.add_argument('--catalog', required=True, action='append', type=Path)
    parser.add_argument('--exclude-plan', action='append', type=Path, default=[])
    parser.add_argument('--fallback-anchor-source', required=True)
    parser.add_argument('--config', type=Path, default=Path('/etc/ucloud-sandboxes/deployment.json'))
    parser.add_argument('--gateway', required=True)
    parser.add_argument('--sdk-wheel', required=True, type=Path)
    parser.add_argument('--workers', type=int, default=2)
    parser.add_argument('--growth-limit-gib', type=int, default=256)
    parser.add_argument('--free-floor-gib', type=int, default=500)
    parser.add_argument('--compression-level', type=int, choices=range(1, 10), default=6)
    parser.add_argument('--seed-max-delta-mib', type=int, default=256,
                        help='larger one-time project seeds; task deltas retain the 256 MiB bound')
    parser.add_argument('--dependency-index', type=Path, help='qualified cross-project dependency bases for new seeds')
    parser.add_argument('--seeds-only', action='store_true',
                        help='expand project bases without proceeding to per-task preparation')
    args = parser.parse_args()
    if (not 1 <= args.workers <= 8 or not 1 <= args.seed_max_delta_mib <= 1024
            or min(args.growth_limit_gib, args.free_floor_gib) < 1):
        parser.error('invalid campaign bounds')
    from ucloud_sandboxes.config import DeploymentConfig
    from ucloud_sandboxes.registry_disk import registry_disk_usage
    args.root.mkdir(parents=True, exist_ok=True)
    with (args.root / 'campaign.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        inputs = [args.inventory, *args.catalog, *args.exclude_plan]
        identity = {'inputs': [hashlib.sha256(p.read_bytes()).hexdigest() for p in inputs],
                    'fallback_anchor_source': args.fallback_anchor_source}
        identity_path = args.root / 'identity.json'
        if identity_path.exists() and json.loads(identity_path.read_text()) != identity:
            raise ValueError('campaign inputs changed; use immutable catalog snapshots')
        save(identity_path, identity)
        budget_path = args.root / 'budget.json'
        if budget_path.exists():
            baseline = json.loads(budget_path.read_text())
        else:
            disk = registry_disk_usage(DeploymentConfig.from_file(args.config))
            if disk is None:
                raise ValueError('campaign needs measurable registry storage')
            baseline = {'initial_used_bytes': disk.used_bytes}
            save(budget_path, baseline)
        inventory = json.loads(args.inventory.read_text())
        catalogs = [json.loads(p.read_text()) for p in args.catalog]
        excluded = set()
        for path in args.exclude_plan:
            plan = json.loads(path.read_text())
            if plan.get('schema') != 1:
                raise ValueError('unsupported exclusion plan')
            excluded.update(row['source'] for row in plan['images'])
        phases = ('seeds',) if args.seeds_only else ('seeds', 'tasks')
        for phase in phases:
            root = args.root / phase
            if phase == 'seeds':
                plan = seed_plan(inventory, catalogs, args.fallback_anchor_source, excluded)
            else:
                plan = plan_shared(inventory, catalogs, exclude_sources=excluded, allow_compact_anchors=True,
                                   max_anchor_bytes=1024**3 + args.seed_max_delta_mib * 1024**2)
            plan = stage_inputs(root, plan, baseline)
            save(args.root / 'progress.json', {'phase': phase, 'planned': len(plan['images']), 'status': 'running'})
            if plan['images']:
                cmd = [sys.executable, str(Path(__file__).with_name('prepare_shared_task_pool.py')),
                       '--root', str(root), '--config', str(args.config), '--gateway', args.gateway,
                       '--sdk-wheel', str(args.sdk_wheel), '--workers', str(args.workers),
                       '--limit', str(len(plan['images'])), '--growth-limit-gib', str(args.growth_limit_gib),
                       '--free-floor-gib', str(args.free_floor_gib), '--compression-level', str(args.compression_level),
                       '--retry-failed']
                if phase == 'seeds':
                    cmd.extend(['--record-source-index', '--max-delta-mib', str(args.seed_max_delta_mib)])
                    if args.dependency_index:
                        cmd.extend(['--dependency-index', str(args.dependency_index)])
                print(json.dumps({'phase': phase, 'planned': len(plan['images'])}), flush=True)
                subprocess.run(cmd, check=True)
                catalogs.append(json.loads((root / 'catalog.json').read_text()))
            else:
                save(root / 'catalog.json', {'schema': 1, 'images': {}})
        outcomes = [r for phase in phases
                    for r in json.loads((args.root / phase / 'catalog.json').read_text())['images'].values()]
        save(args.root / 'progress.json', {'phase': 'finished', 'counts': {
            status: sum(r['status'] == status for r in outcomes) for status in ('ready', 'failed', 'deferred')},
            'scope': 'This campaign only; failed or deferred sources remain uncovered.'})


if __name__ == '__main__':
    main()
