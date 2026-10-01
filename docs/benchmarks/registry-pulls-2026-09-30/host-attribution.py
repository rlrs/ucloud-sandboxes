#!/usr/bin/env python3
"""Local-only phase, busy-window and completion-tail analysis from raw counters."""
import argparse
from collections import defaultdict
from datetime import datetime, timezone
import gzip
import hashlib
import importlib.util
import json
import math
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]


def module(name, filename):
    spec = importlib.util.spec_from_file_location(name, REPO / 'scripts' / filename)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


TELEMETRY = module('telemetry', 'build_load_telemetry.py')
REPORT = module('report', 'build_load_report.py')
GROUPS = TELEMETRY.GROUPS


def iso(value):
    return datetime.fromtimestamp(value, timezone.utc).isoformat()


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_host(path):
    previous = metadata = None
    intervals, health = [], []
    with gzip.open(path, 'rt') as stream:
        for line in stream:
            row = json.loads(line)
            if row.get('type') == 'metadata':
                metadata = row
            if row.get('type') != 'sample':
                continue
            if previous is not None:
                delta = TELEMETRY.derive(previous, row, metadata['clock_ticks'])
                if delta:
                    intervals.append(delta)
            previous = row
            if 'health' in row:
                health.append((row['unix_seconds'], row['health']))
    return {'source': str(path), 'sha256': sha(path), 'metadata': metadata,
            'intervals': intervals, 'health': health}


def inside(host, start, end):
    return [row for row in host['intervals'] if row['start_unix_seconds'] >= start
            and row['end_unix_seconds'] <= end]


def stats(rows, key, divisor=1):
    points = [(row['seconds'], row['values'][key] / divisor)
              for row in rows if key in row['values']]
    if not points:
        return {'samples': 0}
    values = sorted(value for _, value in points)
    seconds = sum(weight for weight, _ in points)
    integral = sum(weight * value for weight, value in points)
    return {'samples': len(points), 'covered_seconds': seconds, 'mean': integral / seconds,
            'p95': values[math.ceil(.95 * len(values)) - 1], 'min': values[0], 'max': values[-1],
            'integral': integral}


def summarize(host, start, end):
    rows = inside(host, start, end)
    disk = 'sdb' if host['metadata']['label'] == 'gateway' else 'sda'
    groups = {group: stats(rows, f'process/{group}/cpu_cores') for group in GROUPS}
    totals = [stats(rows, f'disk/{disk}/{key}_per_second').get('integral')
              for key in ('read_ms', 'write_ms', 'reads', 'writes')]
    await_ms = (sum(totals[:2]) / sum(totals[2:])
                if all(value is not None for value in totals) and sum(totals[2:]) else None)
    health = [row for at, row in host['health'] if start <= at <= end]
    return {'started_at': iso(start), 'finished_at': iso(end),
            'covered_seconds': sum(row['seconds'] for row in rows),
            'coverage_fraction': sum(row['seconds'] for row in rows) / (end - start),
            'host_cpu_cores': stats(rows, 'cpu/busy_cores'),
            'iowait_cores': stats(rows, 'cpu/iowait_cores'), 'process_cpu_cores': groups,
            'cpu_psi_percent': stats(rows, 'pressure/cpu/some_percent'),
            'io_psi_percent': stats(rows, 'pressure/io/some_percent'),
            'memory_psi_percent': stats(rows, 'pressure/memory/some_percent'),
            'available_memory_gib': stats(rows, 'memory/MemAvailable_bytes', 2**30),
            'disk': {'device': disk, 'await_io_weighted_ms': await_ms,
                     'write_mib_per_second': stats(rows, f'disk/{disk}/write_bytes_per_second', 2**20),
                     'read_mib_per_second': stats(rows, f'disk/{disk}/read_bytes_per_second', 2**20),
                     'queue_depth': stats(rows, f'disk/{disk}/average_queue_depth')},
            'oom_delta': stats(rows, 'vmstat/oom_kill_per_second').get('integral'),
            'swap_in_pages': stats(rows, 'vmstat/pswpin_per_second').get('integral'),
            'swap_out_pages': stats(rows, 'vmstat/pswpout_per_second').get('integral'),
            'health_probes': len(health), 'health_failures': sum(not row['ok'] for row in health),
            'health_latency_ms': REPORT.distribution([row['latency_ms'] for row in health])}


def busiest(host, start, end, key):
    if end - start < 30:
        return {'selection': 'phase shorter than 30 seconds', **summarize(host, start, end)}
    candidates = []
    for row in inside(host, start, end):
        left = row['start_unix_seconds']
        if left + 30 > end:
            continue
        points = inside(host, left, left + 30)
        if sum(point['seconds'] for point in points) < 27:
            continue
        value = stats(points, key).get('mean')
        if value is not None:
            candidates.append((value, left))
    if not candidates:
        return {'selection': 'no 30-second window with at least 90% coverage'}
    _, left = max(candidates)
    return {'selection': 'largest time-weighted mean ' + key,
            **summarize(host, left, left + 30)}


def phase_metrics(records):
    sections = defaultdict(lambda: defaultdict(list))
    for record in records:
        timings = (record.get('build') or {}).get('timings') or {}
        for section in ('phases', 'environment'):
            for key, value in timings.get(section, {}).items():
                if type(value) in (int, float):
                    sections[section][key].append(value)
    return {section: {key: REPORT.distribution(values) for key, values in sorted(fields.items())}
            for section, fields in sections.items()}


def markdown(result):
    def fmt(value):
        return f'{value:.3f}' if isinstance(value, (int, float)) else 'N/A'

    def row(cells):
        return '| ' + ' | '.join(str(cell) for cell in cells) + ' |'

    lines = ['# Resource attribution', '',
        'Reproduce with `python3 docs/benchmarks/registry-pulls-2026-09-30/host-attribution.py`. '
        'The JSON binds the exact input files. Candidate results require telemetry covering their windows; '
        'zero coverage is missing evidence. CPU units are occupied cores. Disk busy counters are excluded.', '',
        '| Phase | Wall s | Gateway coverage | Host / API / registry / driver mean cores | Registry written GiB | I/O-weighted await ms | I/O PSI mean / p95 % |',
        '|---|---:|---:|---:|---:|---:|---:|']
    for phase in result['phases']:
        host = next(host for host in phase['hosts'] if host['label'] == 'gateway')['phase']
        cpu = [host['host_cpu_cores'].get('mean')] + [host['process_cpu_cores'][key].get('mean')
                for key in ('gateway', 'registry', 'benchmark_driver')]
        written = host['disk']['write_mib_per_second'].get('integral')
        lines.append(row([phase['phase'], fmt(phase['batch_wall_seconds']),
            f"{host['coverage_fraction']:.1%}", ' / '.join(map(fmt, cpu)),
            fmt(written / 1024 if written is not None else None), fmt(host['disk']['await_io_weighted_ms']),
            ' / '.join(fmt(host['io_psi_percent'].get(key)) for key in ('mean', 'p95'))]))
    lines += ['', '## Cold-arm busiest 30-second windows', '',
        'Each host selects its own busiest CPU window; the gateway row selects its busiest I/O PSI window. '
        'These rows must not be added as a simultaneous fleet total.', '',
        '| Phase / host | Window start UTC | CPU mean | BuildKit / dockerd mean cores | CPU / I/O PSI mean % | Disk write MiB/s | I/O-weighted await ms |',
        '|---|---|---:|---:|---:|---:|---:|']
    for phase in result['phases']:
        if phase['mode'] != 'cold':
            continue
        for host in phase['hosts']:
            window = host['busiest_io_psi_30s' if host['label'] == 'gateway' else 'busiest_cpu_30s']
            if 'started_at' not in window:
                continue
            lines.append(row([phase['phase'] + ' / ' + host['label'], window['started_at'],
                fmt(window['host_cpu_cores'].get('mean')),
                ' / '.join(fmt(window['process_cpu_cores'][key].get('mean')) for key in ('buildkit', 'dockerd')),
                ' / '.join(fmt(window[key].get('mean')) for key in ('cpu_psi_percent', 'io_psi_percent')),
                fmt(window['disk']['write_mib_per_second'].get('mean')), fmt(window['disk']['await_io_weighted_ms'])]))
    lines += ['', '## Cold publication attribution', '',
        'All times below are per-build means in seconds. Missing pull measurements stay N/A. '
        'Nested timings cannot be added to their parent publication total.', '',
        '| Phase / recipe | Publication | Docker pull (reporting records) | Selective child | Squash | mkfs | Component publish | Finishing wait |',
        '|---|---:|---:|---:|---:|---:|---:|---:|']
    for phase in result['phases']:
        if phase['mode'] != 'cold':
            continue
        for recipe, values in phase['by_recipe'].items():
            env = values.get('environment', {})
            def seconds(key):
                value = env.get(key, {}).get('mean')
                return fmt(value / 1000 if value is not None else None)
            wait = values.get('phases', {}).get('finishing_wait_ms', {}).get('mean')
            lines.append(row([phase['phase'] + ' / ' + recipe, seconds('total_ms'),
                seconds('docker_pull_ms') + f" ({env.get('docker_pull_ms', {}).get('count', 0)})",
                *[seconds(key) for key in ('selective_subprocess_ms', 'squash_ms', 'mkfs_ms', 'publish_component_ms')],
                fmt(wait / 1000 if wait is not None else None)]))
    lines += ['', '## Cold completion tails', '',
        'Builder ranges compare per-host means within the stated window; they are not percentiles.', '',
        '| Phase / window | Gateway CPU mean | Registry writes MiB/s | Registry await ms | Builder CPU mean range | Builder dockerd mean range |',
        '|---|---:|---:|---:|---:|---:|']
    for phase in result['phases']:
        if phase['mode'] != 'cold':
            continue
        for name in ('completion_tail_60s', 'post_completion_60s'):
            gateway = next(host for host in phase['hosts'] if host['label'] == 'gateway')[name]
            builders = [host[name] for host in phase['hosts'] if host['label'] != 'gateway']
            ranges = []
            for values in ([host['host_cpu_cores'].get('mean') for host in builders],
                           [host['process_cpu_cores']['dockerd'].get('mean') for host in builders]):
                values = [value for value in values if value is not None]
                ranges.append(f'{min(values):.3f}–{max(values):.3f}' if values else 'N/A')
            lines.append(row([phase['phase'] + ' / ' + name,
                fmt(gateway['host_cpu_cores'].get('mean')),
                fmt(gateway['disk']['write_mib_per_second'].get('mean')),
                fmt(gateway['disk']['await_io_weighted_ms']), *ranges]))
    lines += ['', 'The JSON also includes per-host CPU attribution, memory/PSI, OOM/swap counters and sampled HTTPS health latency. '
        'Gateway-local HTTPS probes are not external sandbox capacity tests.', '', 'Limits:', '']
    lines += ['- ' + value for value in result['limitations']]
    return '\n'.join(lines) + '\n'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parent)
    args = parser.parse_args()
    hosts = [load_host(path) for path in sorted((args.root / 'telemetry').glob('*.jsonl.gz'))]
    result = {'generated_at': datetime.now(timezone.utc).isoformat(), 'sources': [
        {key: host[key] for key in ('source', 'sha256')} | {
            'label': host['metadata']['label'], 'health_url': host['metadata'].get('health_url')}
        for host in hosts], 'phases': [], 'limitations': [
            'Local read-only analysis; no new samples or production requests.',
            'Only complete raw counter intervals are used; boundary time is excluded.',
            'Busy 30-second windows are selected separately per host/metric, not simultaneous fleet totals.',
            'Disk busy counters are deliberately excluded; await is weighted by completed I/O counts.',
            'Last 60 seconds may contain fewer builds; post 60 seconds includes writeback and any unrelated activity.',
            'Timing subphases are nested and overlap; do not add environment counters to their parent total.',
            'Cold means invalidated dependency RUN, not uncached base images, package downloads or empty disks.',
            'Gateway-local HTTPS health probes validate TLS but are not external end-to-end sandbox capacity tests.',
            'The baseline arm ran first; shared package/base caches and host writeback may differ despite fresh per-case dependency nonces.',
        ]}
    for path in sorted(args.root.glob('slotq-*/summary.json'),
                       key=lambda item: json.loads(item.read_text())['started_at']):
        source = json.loads(path.read_text())
        start, end = (REPORT.timestamp(source[key]) for key in ('started_at', 'finished_at'))
        records = source['records']
        phase = {'phase': source['phase'], 'source_sha256': sha(path),
                 'started_at': iso(start), 'finished_at': iso(end),
                 'batch_wall_seconds': source['batch_wall_seconds'],
                 'mode': source['mode'], 'declared_slots': source['declared_slots'],
                 'client_errors': source['client_errors'], 'deadline_misses': source['deadline_misses'],
                 'build_metrics': phase_metrics(records), 'by_recipe': {}, 'hosts': []}
        for recipe in sorted({record['recipe'] for record in records}):
            phase['by_recipe'][recipe] = phase_metrics([row for row in records if row['recipe'] == recipe])
        for host in hosts:
            phase['hosts'].append({'label': host['metadata']['label'],
                'phase': summarize(host, start, end),
                'busiest_cpu_30s': busiest(host, start, end, 'cpu/busy_cores'),
                'busiest_io_psi_30s': busiest(host, start, end, 'pressure/io/some_percent'),
                'completion_tail_60s': summarize(host, max(start, end - 60), end),
                'post_completion_60s': summarize(host, end, end + 60)})
        result['phases'].append(phase)
    output = args.root / 'resource-attribution.json'
    output.write_text(json.dumps(result, indent=2, allow_nan=False) + '\n')
    output.with_suffix('.md').write_text(markdown(result))
    print(json.dumps({'output': str(output), 'phases': len(result['phases'])}))


if __name__ == '__main__':
    main()
