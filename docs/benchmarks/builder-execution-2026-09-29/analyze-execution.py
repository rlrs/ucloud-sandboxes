#!/usr/bin/env python3
"""Compare local R2 execution costs against R1, without network/service calls."""
from __future__ import annotations

import argparse
from collections import Counter
import gzip
import importlib.util
import json
from pathlib import Path


HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
SPEC = importlib.util.spec_from_file_location('preparation_analysis',
    HERE.parent / 'builder-preparation-2026-09-29/analyze-preparation.py')
PREP = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PREP)
PREP.ENVIRONMENT = ('selective_subprocess_ms', *PREP.ENVIRONMENT)
LIMITATIONS = [*PREP.LIMITATIONS,
    'selective_subprocess_ms includes fresh child startup, extraction, squash and IPC; selective_materialization_ms and squash_ms are nested child stages.',
    'Residual overhead is calculated per build as subprocess wall minus both child stage wall times. It includes startup/imports/IPC and any uninstrumented work, not only process startup.',
    'Both runs use fresh builder pools, but registry cache history, placement and chronology differ. Runtime effects are not isolated by this historical comparison.']


def residual(records):
    values, negative = [], []
    for record in records:
        row = PREP.REPORT.nested(record, ('build', 'timings', 'environment')) or {}
        timings = [row.get(name) for name in ('selective_subprocess_ms', 'selective_materialization_ms', 'squash_ms')]
        if not all(PREP.REPORT.number(value) for value in timings):
            continue
        elapsed = timings[0] - timings[1] - timings[2]
        if elapsed < 0:
            negative.append({'build_id': record.get('build_id'), 'residual_ms': elapsed})
        else:
            values.append(elapsed)
    return {'records_expected': len(records), 'records_reporting_nonnegative': len(values),
            'negative_residuals': negative, **PREP.REPORT.distribution(values)}


def invocation_coverage(records):
    invoked, bypassed, unresolved = [], [], []
    for record in records:
        metric = PREP.REPORT.nested(record, ('build', 'timings', 'environment')) or {}
        identity = {'build_id': record.get('build_id'), 'image_id': record.get('image_id'), 'recipe': record.get('recipe')}
        if PREP.REPORT.number(metric.get('selective_subprocess_ms')) and metric['selective_subprocess_ms'] > 0:
            invoked.append(identity)
        elif (metric.get('docker_pull_skipped') == 1 and PREP.REPORT.number(metric.get('groups_reused'))
              and metric['groups_reused'] > 0 and set(metric) <= {'docker_pull_skipped', 'groups_reused', 'preflight_ms', 'total_ms'}):
            # This is the explicit metric shape emitted by the all-component-hit
            # return path; missing arbitrary metrics alone would not prove a hit.
            bypassed.append(identity)
        else:
            unresolved.append(identity)
    return {'records': len(records), 'child_invocations': len(invoked), 'complete_cache_hit_bypasses': len(bypassed),
            'unresolved': unresolved, 'bypass_identities': bypassed,
            'classification': 'Positive child timer, or the documented full-component-hit metric shape with positive reuse/pull-skip; arbitrary missing fields are not treated as zero.'}


def health(path, summary_path):
    if not path.is_file():
        return {'source': str(path.relative_to(ROOT)), 'health_url': None, 'response_status_counts': {},
                'samples': 0, 'failures': None, 'valid_healthz_endpoint_and_samples': False,
                'interpretation': 'Telemetry has not been copied yet; regenerate after it arrives. No in-burst health conclusion.'}
    rows = json.loads(summary_path.read_text())['records']
    begin = min(PREP.REPORT.timestamp(row['started_at']) for row in rows)
    end = max(PREP.REPORT.timestamp(row['finished_at']) for row in rows)
    statuses, failures, samples, url = Counter(), 0, 0, None
    with gzip.open(path, 'rt') as stream:
        for line in stream:
            row = json.loads(line)
            if row.get('type') == 'metadata':
                url = row.get('health_url')
            probe = row.get('health') or {}
            at = PREP.REPORT.timestamp(probe.get('at'))
            if at is not None and begin <= at <= end:
                statuses[str(probe.get('status', 'no_status'))] += 1
                samples += 1
                failures += probe.get('ok') is not True
    endpoint_ok = isinstance(url, str) and url.endswith('/healthz')
    valid = endpoint_ok and samples > 0
    return {'source': str(path.relative_to(ROOT)), 'sha256': PREP.checksum(path),
            'window': {'started_at': PREP.REPORT.iso(begin), 'finished_at': PREP.REPORT.iso(end)},
            'health_url': url, 'response_status_counts': dict(statuses), 'samples': samples,
            'failures': failures, 'valid_healthz_endpoint_and_samples': valid,
            'interpretation': ('These are gateway-origin /healthz observations, not external capacity evidence.'
                               if valid else 'Health gate unavailable: require /healthz and nonempty in-window samples.')}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline', type=Path, default=HERE.parent / 'builder-preparation-2026-09-29/prep-repeat/summary.json')
    parser.add_argument('--candidate', type=Path, default=HERE / 'exec-repeat/summary.json')
    parser.add_argument('--telemetry', type=Path, default=HERE / 'telemetry/gateway.jsonl.gz')
    parser.add_argument('--output-prefix', type=Path, default=HERE / 'phase-costs')
    parser.add_argument('--self-test', action='store_true')
    args = parser.parse_args()
    if args.self_test:
        records = [{'build_id': 'synthetic', 'build': {'timings': {'environment': {
            'selective_subprocess_ms': 300, 'selective_materialization_ms': 100, 'squash_ms': 150}}}},
            {'build_id': 'missing', 'build': {'timings': {}}}]
        stats = residual(records)
        assert stats['records_reporting_nonnegative'] == 1 and stats['p50'] == 50
        assert PREP.describe(records)['environment']['selective_subprocess_ms']['records_reporting'] == 1
        records[0]['build']['timings']['environment']['selective_subprocess_ms'] = 200
        assert residual(records)['count'] == 0 and len(residual(records)['negative_residuals']) == 1
        assert invocation_coverage(records)['child_invocations'] == 1
        assert len(invocation_coverage(records)['unresolved']) == 1
        records[1]['build']['timings']['environment'] = {'docker_pull_skipped': 1, 'groups_reused': 5,
                                                       'preflight_ms': 20, 'total_ms': 30}
        assert invocation_coverage(records)['complete_cache_hit_bypasses'] == 1
        print(json.dumps({'self_test': 'passed', 'network_calls': 0}))
        return
    inputs = tuple(path.resolve() for path in (args.baseline, args.candidate, args.telemetry))
    outputs = tuple(args.output_prefix.with_suffix(suffix).resolve() for suffix in ('.json', '.md'))
    if any(path in inputs for path in outputs):
        parser.error('Output cannot overwrite an input')
    records = json.loads(inputs[1].read_text())['records']
    report = {'schema': 1, 'unit': 'milliseconds', 'baseline': PREP.summarize(inputs[0]),
              'candidate': PREP.summarize(inputs[1]), 'candidate_health': health(inputs[2], inputs[1]),
              'candidate_subprocess_residual_ms': residual(records),
              'candidate_invocation_coverage': invocation_coverage(records), 'limitations': LIMITATIONS}
    text = PREP.markdown(report).replace('# Builder preparation: retained phase costs', '# Builder execution: retained phase costs', 1)
    overhead = report['candidate_subprocess_residual_ms']
    coverage = report['candidate_invocation_coverage']
    text += ('\n## Child invocation coverage\n\n'
             f"{coverage['child_invocations']}/{len(records)} records report a positive child timer; "
             f"{coverage['complete_cache_hit_bypasses']} use the complete-component-cache-hit metric shape, "
             f"and {len(coverage['unresolved'])} are unresolved. Cache-hit bypasses have no child startup or child timer; "
             'do not insert zero timings into the child-stage distributions. Exact bypass identities are in JSON.\n')
    text += ('\n## Child-invocation residual\n\n'
             f"Nonnegative residual coverage: {overhead['records_reporting_nonnegative']}/{len(records)}; "
             f"negative residuals: {len(overhead['negative_residuals'])}. "
             f"Median / p95 / maximum: {PREP.triple(overhead)} ms. "
             'This subtracts the two nested child stages per record before aggregation; it is not added to subprocess wall time.\n')
    outputs[0].write_text(json.dumps(report, indent=2, allow_nan=False) + '\n')
    outputs[1].write_text(text)
    print(json.dumps({'json': str(outputs[0]), 'markdown': str(outputs[1])}))


if __name__ == '__main__':
    main()
