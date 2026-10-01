#!/usr/bin/env python3
"""Run bounded source-qualified flat-image preparations with resumable journals."""
from __future__ import annotations

import argparse
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
import fcntl
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time

from prepare_image_pool import SourceResolver, admission, registry_parts


def save(path, value):
    temporary = path.with_suffix('.partial')
    temporary.write_text(json.dumps(value, sort_keys=True, separators=(',', ':')))
    temporary.replace(path)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', required=True, type=Path)
    parser.add_argument('--gateway', required=True)
    parser.add_argument('--sdk-wheel', required=True, type=Path)
    parser.add_argument('--config', type=Path, default=Path('/etc/ucloud-sandboxes/deployment.json'))
    parser.add_argument('--workers', type=int, default=2)
    parser.add_argument('--limit', type=int, default=48)
    parser.add_argument('--growth-limit-gib', type=int, default=16)
    parser.add_argument('--free-floor-gib', type=int, default=500)
    parser.add_argument('--retry-failed', action='store_true', help='retry failed rows once, preserving their previous journal')
    args = parser.parse_args()
    if not 1 <= args.workers <= 4 or min(args.limit, args.growth_limit_gib, args.free_floor_gib) < 1:
        parser.error('invalid bounds')
    from ucloud_sandboxes.config import DeploymentConfig
    from ucloud_sandboxes.registry_disk import registry_disk_usage
    c = DeploymentConfig.from_dict(json.loads(args.config.read_text()))
    resolver = SourceResolver(c.control_state_file().parent / 'image-pool-locks', clock=time.time)
    root = args.root
    plan = json.loads((root / 'plan.json').read_text())
    if plan.get('schema') != 1:
        raise ValueError('unsupported shared-image plan')
    items = plan['images'][:args.limit]
    if len({r['source'] for r in items}) != len(items):
        raise ValueError('duplicate plan sources')
    (root / 'results').mkdir(exist_ok=True)
    (root / 'work').mkdir(exist_ok=True)
    reservation = 2 * 1024**3
    with (root / 'pool.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        disk = registry_disk_usage(c)
        if disk is None:
            raise ValueError('registry storage cannot be measured')
        state_path = root / 'budget.json'
        state = json.loads(state_path.read_text()) if state_path.exists() else {'initial_used_bytes': disk.used_bytes}
        save(state_path, state)
        results = {}
        pending = []
        for item in items:
            key = hashlib.sha256(item['source'].encode()).hexdigest()
            path = root / 'results' / (key + '.json')
            if path.exists():
                row = json.loads(path.read_text())
                if row['source'] != item['source'] or row['anchor'] != item['anchor']:
                    raise ValueError('result belongs to another preparation')
                results[item['source']] = row
                if ((args.retry_failed and row['status'] == 'failed')
                        or (row['status'] == 'deferred' and (
                            'deferred: public registry cooldown' in row.get('error', '')
                            or row.get('error') in {'deferred: batch storage budget', 'deferred: free-space reserve'}))):
                    history = root / 'attempts'
                    history.mkdir(exist_ok=True)
                    save(history / (key + '-' + str(time.time_ns()) + '.json'), row)
                    pending.append(item)
            else:
                pending.append(item)
        active, offset, completed_count = {}, 0, 0
        admission_block = None
        def checkpoint():
            save(root / 'catalog.json', {'schema': 1, 'images': results})
            counts = {status: sum(row['status'] == status for row in results.values())
                      for status in ('ready', 'failed', 'deferred')}
            save(root / 'progress.json', {'updated_at_unix': time.time(), 'counts': counts,
                                         'planned': len(items), 'unprocessed': len(items) - len(results),
                                         'queued': len(pending) - offset, 'active': len(active),
                                         'completed_current_run': completed_count, 'admission_block': admission_block})
            print(json.dumps(counts), flush=True)
        def run(item):
            key = hashlib.sha256(item['source'].encode()).hexdigest()
            work = root / 'work' / key
            command = [sys.executable, str(Path(__file__).with_name('prepare_shared_task_image.py')),
                       '--source', item['source'], '--anchor', item['anchor'], '--root', str(work),
                       '--anchor-cache', str(root / 'anchor-cache'), '--gateway', args.gateway,
                       '--sdk-wheel', str(args.sdk_wheel), '--config', str(args.config),
                       '--free-floor-gib', str(args.free_floor_gib)]
            for family in item.get('families', []):
                command.extend(['--family', family])
            if item.get('pinned_source'):
                command.extend(['--pinned-source', item['pinned_source']])
            if args.retry_failed:
                command.append('--retry-recorded-failures')
            log_path = root / 'work' / (key + '.log')
            try:
                with log_path.open('w') as output:
                    completed = subprocess.run(command, stdout=output, stderr=subprocess.STDOUT, timeout=1800)
                if completed.returncode:
                    tail = log_path.read_text()[-1800:]
                    raise RuntimeError(tail)
                row = json.loads((work / 'catalog.json').read_text())['images'][item['source']]
                result = {**item, **row}
                if result['status'] != 'ready' or not result['qualification']['equivalent']:
                    raise ValueError('preparer did not qualify source equivalence')
            except Exception as error:
                detail = str(error)[-1800:]
                result = {**item, 'status': 'deferred' if 'deferred:' in detail else 'failed', 'error': detail}
            save(root / 'results' / (key + '.json'), result)
            return result
        checkpoint()
        attempts = {}
        last_cooldown = None
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            while offset < len(pending) or active:
                while offset < len(pending) and len(active) < args.workers:
                    current = registry_disk_usage(c)
                    if current is None:
                        raise ValueError('registry storage became unmeasurable')
                    reason = admission(current.used_bytes, current.available_bytes, state['initial_used_bytes'],
                                       len(active) * reservation, growth_limit=args.growth_limit_gib * 1024**3,
                                       free_floor=args.free_floor_gib * 1024**3, estimate=reservation)
                    if reason:
                        admission_block = reason
                        for item in pending[offset:]:
                            key = hashlib.sha256(item['source'].encode()).hexdigest()
                            row = {**item, 'status': 'deferred', 'error': 'deferred: ' + reason}
                            results[item['source']] = row
                            save(root / 'results' / (key + '.json'), row)
                        offset = len(pending)
                        break
                    item = pending[offset]
                    host, _, _ = registry_parts(item['source'])
                    key = hashlib.sha256(item['source'].encode()).hexdigest()
                    needs_resolution = not (root / 'work' / key / 'resolved.json').exists()
                    delay = resolver.cooldown_seconds(item['source']) if needs_resolution else 0
                    if delay > 0:
                        resume_after = round(time.time() + delay)
                        if resume_after != last_cooldown:
                            last_cooldown = resume_after
                            note = {'status': 'public_registry_cooldown', 'registry': host, 'resume_after_unix': last_cooldown}
                            save(root / 'cooldown.json', note)
                            print(json.dumps(note), flush=True)
                        if not active:
                            time.sleep(min(30, delay))
                        break
                    offset += 1
                    attempts[item['source']] = attempts.get(item['source'], 0) + 1
                    active[executor.submit(run, item)] = item
                if active:
                    done, _ = wait(active, return_when=FIRST_COMPLETED)
                    for future in done:
                        item = active.pop(future)
                        result = future.result()
                        if ('deferred: public registry cooldown' in result.get('error', '')
                                and attempts[item['source']] < 3):
                            pending.append(item)
                        results[result['source']] = result
                        completed_count += 1
                        print(json.dumps({'source': result['source'], 'status': result['status']}), flush=True)
                        if completed_count % 4 == 0:
                            checkpoint()
        checkpoint()


if __name__ == '__main__':
    main()
