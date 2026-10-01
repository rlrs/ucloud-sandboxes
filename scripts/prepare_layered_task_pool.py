#!/usr/bin/env python3
"""Serial, disk-bounded layered preparation against one qualified project anchor.

Run as root for isolated offline exports. Source qualification runs as the service
user. Completed exports are removed; authenticated source receipts and outcomes
remain. The immutable plan must exclude sources assigned to other campaigns.
"""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import pwd
import re
import shutil
import subprocess
import sys
import time

from cache_source_receipts import validated
from prepare_image_pool import RegistryHealthGate, SourceResolver, admission, save


def resolve_as_user(resolver, source, uid, gid):
    """Keep shared quota-state ownership compatible with unprivileged queues."""
    old_uid, old_gid = os.geteuid(), os.getegid()
    try:
        os.setegid(gid)
        os.seteuid(uid)
        return resolver(source)
    finally:
        os.seteuid(old_uid)
        os.setegid(old_gid)


def remove_export(directory, mountinfo=Path('/proc/self/mountinfo')):
    """Never traverse an active unpacker mount, including after interruption."""
    if directory.is_symlink():
        raise ValueError('export directory must not be a symlink')
    target = directory.resolve()
    for line in mountinfo.read_text().splitlines():
        name = re.sub(r'\\([0-7]{3})', lambda m: chr(int(m[1], 8)), line.split()[4])
        mount = Path(name)
        if mount == target or target in mount.parents:
            raise ValueError('export still contains a mounted filesystem; manual recovery required')
    if directory.exists():
        shutil.rmtree(directory)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', required=True, type=Path)
    parser.add_argument('--anchor-exports', required=True, type=Path)
    parser.add_argument('--umoci', required=True, type=Path)
    parser.add_argument('--umoci-sha256', required=True)
    parser.add_argument('--gateway', required=True)
    parser.add_argument('--sdk-wheel', required=True, type=Path)
    parser.add_argument('--config', type=Path, default=Path('/etc/ucloud-sandboxes/deployment.json'))
    parser.add_argument('--service-user', default='ucloud')
    parser.add_argument('--growth-limit-gib', type=int, default=64)
    parser.add_argument('--free-floor-gib', type=int, default=500)
    parser.add_argument('--max-delta-mib', type=int, default=512)
    args = parser.parse_args()
    if (os.getuid() != 0 or not args.root.is_absolute()
            or not 1 <= args.max_delta_mib <= 1024 or min(args.growth_limit_gib, args.free_floor_gib) < 1):
        parser.error('requires root, an absolute work directory and valid storage bounds')
    from ucloud_sandboxes.config import DeploymentConfig
    from ucloud_sandboxes.registry_disk import registry_disk_usage
    config = DeploymentConfig.from_file(args.config)
    account = pwd.getpwnam(args.service_user)
    uid, gid = account.pw_uid, account.pw_gid
    root = args.root
    if root.stat().st_uid != 0 or root.stat().st_mode & 0o022:
        raise ValueError('campaign directory must be protected and root owned')
    os.chown(root, 0, gid)
    root.chmod(0o750)
    plan_bytes = (root / 'plan.json').read_bytes()
    plan = json.loads(plan_bytes)
    if plan.get('schema') != 1 or '@sha256:' not in plan['anchor']:
        raise ValueError('plan requires a pinned anchor')
    if len({r['source'] for r in plan['images']}) != len(plan['images']):
        raise ValueError('duplicate source assignments')
    anchor_receipt = json.loads((args.anchor_exports / 'export.json').read_text())
    if anchor_receipt['reference'] != plan['anchor']:
        raise ValueError('anchor export belongs to another image')
    identity = {'plan_sha256': hashlib.sha256(plan_bytes).hexdigest(),
                'anchor_export_sha256': anchor_receipt['export_sha256'], 'compression_level': 6}
    resolver = SourceResolver(config.control_state_file().parent / 'image-pool-locks', max_wait_seconds=60)
    health = RegistryHealthGate(config.registry_url)
    with (root / 'pool.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        identity_path = root / 'identity.json'
        if identity_path.exists() and json.loads(identity_path.read_text()) != identity:
            raise ValueError('campaign input identity changed')
        save(identity_path, identity)
        disk = registry_disk_usage(config)
        if disk is None:
            raise ValueError('registry disk cannot be measured')
        budget_path = root / 'budget.json'
        budget = json.loads(budget_path.read_text()) if budget_path.exists() else {'initial_used_bytes': disk.used_bytes}
        save(budget_path, budget)
        (root / 'results').mkdir(exist_ok=True)
        (root / 'work').mkdir(exist_ok=True)
        os.chown(root / 'work', 0, gid)
        (root / 'work').chmod(0o750)
        results = {r['source']: r for p in (root / 'results').glob('*.json') if (r := json.loads(p.read_text()))}
        def checkpoint(active=None, blocked=None):
            save(root / 'catalog.json', {'schema': 1, 'images': results})
            save(root / 'progress.json', {'planned': len(plan['images']), 'completed': len(results),
                 'active': active, 'admission_block': blocked, 'updated_at_unix': time.time(),
                 'counts': {s: sum(r['status'] == s for r in results.values()) for s in ('ready', 'failed', 'deferred')}})
        checkpoint()
        for item in plan['images']:
            source = item['source']
            if source in results:
                continue
            key = hashlib.sha256(source.encode()).hexdigest()
            job = root / 'work' / key
            job.mkdir(exist_ok=True)
            os.chown(job, 0, gid)
            job.chmod(0o750)
            resolved_path = root / (key + '.json')
            checkpoint(source)
            while True:
                disk = registry_disk_usage(config)
                if disk is None:
                    raise ValueError('registry disk became unmeasurable')
                reason = admission(disk.used_bytes, disk.available_bytes, budget['initial_used_bytes'], 0,
                    growth_limit=args.growth_limit_gib * 1024**3, free_floor=args.free_floor_gib * 1024**3,
                    estimate=4 * 1024**3)
                if reason:
                    checkpoint(blocked=reason)
                    return
                if not health.ready():
                    checkpoint(source, 'registry unavailable')
                    time.sleep(5)
                    continue
                try:
                    if resolved_path.exists():
                        receipt = json.loads(resolved_path.read_text())
                    else:
                        receipt = {'source': source, 'resolved': resolve_as_user(
                            resolver, item.get('pinned_source') or source, uid, gid)}
                        save(resolved_path, receipt)
                    resolved = validated(source, receipt['resolved'])
                    if item.get('pinned_source') and item['pinned_source'] != resolved['reference']:
                        raise ValueError('source differs from immutable plan pin')
                    if resolved['compressed_bytes'] > 5 * 1024**3 or resolved['onbuild']:
                        raise ValueError('deferred: source exceeds supported layered input bounds')
                    break
                except RuntimeError as error:
                    if 'deferred: public registry cooldown' not in str(error):
                        raise
                    checkpoint(source, 'public registry cooldown')
                    time.sleep(30)
                except Exception as error:
                    results[source] = {**item, 'status': 'deferred' if 'deferred:' in str(error) else 'failed', 'error': str(error)}
                    break
            if source in results:
                save(root / 'results' / (key + '.json'), results[source])
                checkpoint()
                continue
            exports = job / 'exports'
            # A prior interruption may leave scratch; refusing a mounted tree is
            # deliberate. Never delete persistent build receipts to resume.
            remove_export(exports)
            exports.mkdir(mode=0o750)
            os.chown(exports, 0, gid)
            (exports / 'anchor').mkdir(mode=0o750)
            os.chown(exports / 'anchor', 0, gid)
            for name in ('export.json', 'filesystem.tar.gz'):
                os.link(args.anchor_exports / name, exports / 'anchor' / name)
            try:
                command = [sys.executable, str(Path(__file__).with_name('export_oci_filesystem.py')),
                           '--reference', resolved['reference'], '--resolved', str(resolved_path),
                           '--root', str(exports / 'target'), '--umoci', str(args.umoci),
                           '--umoci-sha256', args.umoci_sha256, '--config', str(args.config)]
                with (job / 'export.log').open('w') as output:
                    subprocess.run(command, check=True, stdout=output, stderr=subprocess.STDOUT, timeout=1800)
                for p in [exports / 'target', exports / 'target/export.json', exports / 'target/filesystem.tar.gz']:
                    os.chown(p, 0, gid)
                    p.chmod(0o750 if p.is_dir() else 0o640)
                work = job / 'prepared'
                work.mkdir(exist_ok=True)
                os.chown(work, uid, gid)
                source_path = work / 'resolved.json'
                if not source_path.exists():
                    save(source_path, resolved)
                    os.chown(source_path, uid, gid)
                command = ['runuser', '-u', args.service_user, '--', sys.executable,
                           str(Path(__file__).with_name('prepare_shared_task_image.py')),
                           '--source', source, '--pinned-source', resolved['reference'], '--anchor', plan['anchor'],
                           '--root', str(work), '--anchor-cache', str(work / 'anchor-cache'),
                           '--filesystem-exports', str(exports), '--max-delta-mib', str(args.max_delta_mib),
                           '--free-floor-gib', str(args.free_floor_gib), '--compression-level', '6',
                           '--gateway', args.gateway, '--sdk-wheel', str(args.sdk_wheel), '--config', str(args.config)]
                for family in item.get('families', []):
                    command.extend(['--family', family])
                with (job / 'prepare.log').open('w') as output:
                    subprocess.run(command, check=True, stdout=output, stderr=subprocess.STDOUT, timeout=1800)
                result = json.loads((work / 'catalog.json').read_text())['images'][source]
                if not result['qualification']['equivalent'] or result['status'] != 'ready':
                    raise ValueError('source qualification failed')
            except Exception as error:
                log = job / ('prepare.log' if (job / 'prepare.log').exists() else 'export.log')
                detail = log.read_text()[-1800:] if log.exists() else str(error)
                result = {**item, 'status': 'deferred' if 'deferred:' in detail else 'failed', 'error': detail}
            finally:
                remove_export(exports)
            results[source] = result
            save(root / 'results' / (key + '.json'), result)
            checkpoint()
            print(json.dumps({'source': source, 'status': result['status']}), flush=True)


if __name__ == '__main__':
    main()
