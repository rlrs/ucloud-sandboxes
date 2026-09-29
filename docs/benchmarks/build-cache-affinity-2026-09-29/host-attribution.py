#!/usr/bin/env python3
"""Correct this run's unavailable SDK process attribution in derived reports.

Run after both generic report generators. Raw telemetry is never changed. The
seed/repeat driver ran through SSH, outside the sampler's named service cgroup.
"""
from __future__ import annotations

import argparse
from copy import deepcopy
import hashlib
import importlib.util
import json
from pathlib import Path


PHASES = {'affinity-seed', 'affinity-repeat'}
NOTE = ('For affinity-seed and affinity-repeat, the SDK driver was launched through SSH, '
        'not a ucloud-build-load-client*.service cgroup recognized by the sampler. '
        'Gateway benchmark_driver attribution is unavailable, not zero CPU. '
        'Whole-host CPU includes this work; API and registry attribution remain valid.')
UNAVAILABLE = {'available': False, 'reason': 'driver_not_in_classified_service_cgroup',
               'launch_method': 'ssh', 'expected_cgroup_prefix': 'ucloud-build-load-client'}


def correct(report, *, host_report):
    if {phase['phase'] for phase in report['phases']} != PHASES:
        raise ValueError('This correction applies only to the exact two R1 affinity phases')
    changed = 0
    for phase in report['phases']:
        hosts = phase['hosts' if host_report else 'telemetry']
        gateways = [host for host in hosts if (host.get('label') if host_report else
                    host.get('metadata', {}).get('label')) == 'gateway']
        if len(gateways) != 1:
            raise ValueError('Expected one gateway capture per phase')
        host = gateways[0]
        if host_report:
            host['process_cpu_cores']['benchmark_driver'] = dict(UNAVAILABLE)
        else:
            keys = [key for key in host['summary'] if key.startswith('process/benchmark_driver/')]
            if not keys:
                raise ValueError('Expected original driver process counters')
            for key in keys:
                host['summary'][key] = dict(UNAVAILABLE)
        host['attribution_provenance'] = {'benchmark_driver': dict(UNAVAILABLE)}
        changed += 1
    report['attribution_provenance'] = {'phases': sorted(PHASES), 'note': NOTE,
        'raw_telemetry_modified': False, 'whole_host_api_registry_metrics_modified': False}
    report['limitations'] = list(dict.fromkeys([*report['limitations'], NOTE]))
    return changed


def unavailable_cells(text, column, *, host_report):
    lines, in_table, column_index, changed = [], False, None, 0
    for line in text.splitlines():
        if line.startswith('|') and column in line:
            cells = [cell.strip() for cell in line.split('|')[1:-1]]
            column_index, in_table = cells.index(column), True
        elif in_table and not line.startswith('|'):
            in_table = False
        elif in_table:
            cells = [cell.strip() for cell in line.split('|')[1:-1]]
            labels = PHASES if host_report else {phase + ' / gateway' for phase in PHASES}
            if cells and cells[0] in labels:
                cells[column_index] = 'N/A'
                line = '| ' + ' | '.join(cells) + ' |'
                changed += 1
        lines.append(line)
    if changed != 2:
        raise ValueError('Expected exactly two gateway driver cells')
    text = '\n'.join(lines) + '\n'
    text = text.replace('The SDK driver runs on the gateway and has its own attribution.', NOTE)
    text = text.replace('driver CPU is reported separately.', 'driver CPU attribution is unavailable for these SSH-launched phases (N/A).')
    return text


def load_module(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def apply(root):
    scripts = Path(__file__).resolve().parents[3] / 'scripts'
    load_renderer = load_module(scripts / 'build_load_report.py', 'affinity_load_renderer')
    host_renderer = load_module(scripts / 'build_load_host_analysis.py', 'affinity_host_renderer')
    load_path, host_path = root / 'build-load-report.json', root / 'host-findings.json'
    load_report, host_report = json.loads(load_path.read_text()), json.loads(host_path.read_text())
    correct(load_report, host_report=False)
    correct(host_report, host_report=True)
    load_bytes = (json.dumps(load_report, indent=2, allow_nan=False) + '\n').encode()
    host_report['report_fingerprint'] = {'bytes': len(load_bytes), 'sha256': hashlib.sha256(load_bytes).hexdigest()}
    load_markdown = unavailable_cells(load_renderer.markdown(load_report), 'Driver CPU', host_report=False)
    host_markdown = unavailable_cells(host_renderer.markdown(host_report), 'SDK driver CPU', host_report=True)
    command = 'python3 ' + str(Path(__file__).relative_to(Path(__file__).resolve().parents[3]))
    host_markdown = host_markdown.replace(
        'python3 scripts/build_load_host_analysis.py --root ' + str(root) + '\n```',
        'python3 scripts/build_load_host_analysis.py --root ' + str(root) + '\n' + command + '\n```')
    # All validation/rendering completes before changing any derived artifact.
    load_path.write_bytes(load_bytes)
    host_path.write_text(json.dumps(host_report, indent=2, allow_nan=False) + '\n')
    (root / 'build-load-report.md').write_text(load_markdown)
    (root / 'host-findings.md').write_text(host_markdown)
    print(json.dumps({'gateway_phases_corrected': 2, 'raw_files_modified': 0,
                      'driver_attribution': 'unavailable'}))


def self_test():
    load = {'phases': [{'phase': phase, 'telemetry': [
        {'metadata': {'label': 'gateway'}, 'summary': {'process/benchmark_driver/cpu_cores': {'mean': 0},
         'cpu/busy_cores': {'mean': 1.5}, 'process/registry/cpu_cores': {'mean': .2}}},
        {'metadata': {'label': 'builder-1'}, 'summary': {'process/benchmark_driver/cpu_cores': {'mean': 0}}}]
        } for phase in sorted(PHASES)], 'limitations': []}
    original = deepcopy(load)
    assert correct(load, host_report=False) == 2
    for phase, old in zip(load['phases'], original['phases']):
        host = phase['telemetry'][0]['summary']
        assert host['process/benchmark_driver/cpu_cores']['available'] is False
        assert 'mean' not in host['process/benchmark_driver/cpu_cores']
        assert host['cpu/busy_cores'] == old['telemetry'][0]['summary']['cpu/busy_cores']
        assert host['process/registry/cpu_cores'] == old['telemetry'][0]['summary']['process/registry/cpu_cores']
        assert phase['telemetry'][1] == old['telemetry'][1]
    once = deepcopy(load)
    correct(load, host_report=False)
    assert load == once
    text = '| Phase | SDK driver CPU |\n|---|---|\n| affinity-seed | 0 |\n| affinity-repeat | 0 |\n'
    assert unavailable_cells(text, 'SDK driver CPU', host_report=True).count('N/A') == 2
    print(json.dumps({'self_test': 'passed', 'network_calls': 0, 'production_calls': 0}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path('docs/benchmarks/build-cache-affinity-2026-09-29'))
    parser.add_argument('--self-test', action='store_true')
    args = parser.parse_args()
    self_test() if args.self_test else apply(args.root)


if __name__ == '__main__':
    main()
