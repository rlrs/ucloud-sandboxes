#!/usr/bin/env python3
"""Compare only local frozen eight-import and single-import build receipts."""
from __future__ import annotations

import argparse
import importlib.util
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parent
PRIOR = ROOT.parent / 'build-cache-affinity-2026-09-29'


def prior_reader():
    spec = importlib.util.spec_from_file_location('affinity_performance_reader', PRIOR / 'performance-report.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def report(progress_path):
    reader = prior_reader()
    baseline_path = PRIOR / 'affinity-repeat/summary.json'
    baseline_summary, _ = reader.read(baseline_path)
    frozen = reader.identities(baseline_summary)
    baseline = reader.phase(baseline_path, PRIOR / 'repeat-buildkit-progress.json', frozen)
    baseline['progress_path'] = 'build-cache-affinity-2026-09-29/repeat-buildkit-progress.json'
    phases = {'eight_imports': baseline}
    summary_path = ROOT / 'single-import-repeat/summary.json'
    if summary_path.exists() and progress_path.exists():
        phases['single_import'] = reader.phase(summary_path, progress_path, frozen)
        phases['single_import']['progress_path'] = str(progress_path.relative_to(ROOT.parent))
    comparison = None
    if 'single_import' in phases:
        candidate = phases['single_import']
        comparison = {'batch_wall_change_percent': reader.percentage(baseline['batch_wall_seconds'], candidate['batch_wall_seconds']),
                      'p95_change_percent': {key: reader.percentage(value['p95'], candidate['metrics_seconds'][key]['p95'])
                                             for key, value in baseline['metrics_seconds'].items()}}
    return {'schema_version': 1, 'candidate_complete': 'single_import' in phases,
            'phases': phases, 'comparison': comparison,
            'limitations': ['The same 48 recipe/variant/context identities are verified; each pool starts with empty local BuildKit caches.',
                'Sequential fleet runs retain placement, registry cache-history, host cache and scheduling differences.',
                'Aggregate phase and vertex durations overlap; they are not independent CPU time or additive batch-wall components.',
                'Cache prepare/mount are included in build/push; child preparation stages are nested within environment publication.',
                'Application execution observations come from per-request progress and can include shared or replayed vertices.',
                'An affinity match is cache selection evidence; actual application RUN reuse is a separate observation.',
                'SDK 0.4.33 and frozen harness are unchanged; SDK process attribution was unavailable in the SSH-launched baseline.',
                'These synthetic build tests do not qualify running-agent capacity or establish a universal workload speedup.']}


def markdown(data):
    phases = data['phases']
    order = list(phases)
    lines = ['# Single-import performance comparison', '']
    if data['candidate_complete']:
        before, after = phases['eight_imports'], phases['single_import']
        lines += [f"Both phases completed 48/48 builds with the same frozen contexts. Batch time changed from {before['batch_wall_seconds']:.3f} to {after['batch_wall_seconds']:.3f} seconds ({data['comparison']['batch_wall_change_percent']:+.2f}%). Tail and phase changes below remain part of the result.", '']
    else:
        lines += ['**Candidate summary/progress is pending. No candidate performance result is claimed.**', '']
    lines += ['| Measurement | ' + ' | '.join(order) + ' |', '| --- | ' + ' | '.join('---:' for _ in order) + ' |']
    fields = [('Batch wall seconds', lambda x: x['batch_wall_seconds']),
              ('Client p95 seconds', lambda x: x['metrics_seconds']['client_wall_seconds']['p95']),
              ('Client maximum seconds', lambda x: x['metrics_seconds']['client_wall_seconds']['max']),
              ('Submission p95 seconds', lambda x: x['metrics_seconds']['submission_seconds']['p95']),
              ('Build/push median seconds', lambda x: x['metrics_seconds']['docker_build_and_push_seconds']['p50']),
              ('Build/push p95 seconds', lambda x: x['metrics_seconds']['docker_build_and_push_seconds']['p95']),
              ('Build/push maximum seconds', lambda x: x['metrics_seconds']['docker_build_and_push_seconds']['max']),
              ('Environment p95 seconds', lambda x: x['metrics_seconds']['immutable_environment_seconds']['p95']),
              ('Submit HTTP503 observations', lambda x: x['submit_http_status_counts'].get('503', 0))]
    for name, value in fields:
        lines.append('| ' + name + ' | ' + ' | '.join(f'{value(phases[key]):.3f}' for key in order) + ' |')
    lines += ['', 'Application observations below describe retained request logs, not necessarily distinct worker executions. Cached materialization and nested exporter operations are not added to enclosing phases.', '',
              '| Phase / recipe | Executed application vertex observations | Mean observed seconds | Layer materialization mean per build | Image export mean | Cache export mean |',
              '| --- | ---: | ---: | ---: | ---: | ---: |']
    for name in order:
        for recipe, row in phases[name]['recipes'].items():
            activity = 'python_compile_smoke' if recipe == 'python-agent' else 'node_lint_build_test_smoke'
            executed = row['run_activities'][activity]['executed_vertex_seconds']
            mean = 'N/A' if executed['mean'] is None else f"{executed['mean']:.3f}"
            categories = row['completed_vertex_seconds_per_build_mean']
            lines.append(f"| {name} / {recipe} | {executed['count']} | {mean} | {row['layer_materialization_seconds_per_build_mean']:.3f} | {categories.get('image_export', 0):.3f} | {categories.get('cache_export', 0):.3f} |")
    lines += ['', 'Limits:', '', *['- ' + value for value in data['limitations']], '',
              'Reproduce: `python3 docs/benchmarks/build-cache-single-import-2026-09-29/performance-report.py`.', '']
    return '\n'.join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--progress', type=Path, default=ROOT / 'candidate-buildkit-progress.json')
    args = parser.parse_args()
    data = report(args.progress.resolve())
    (ROOT / 'performance-comparison.json').write_text(json.dumps(data, indent=2, allow_nan=False) + '\n')
    (ROOT / 'performance-comparison.md').write_text(markdown(data))
    print(json.dumps({'candidate_complete': data['candidate_complete'], 'phases': list(data['phases'])}))


if __name__ == '__main__':
    main()
