from __future__ import annotations
import argparse
from datetime import datetime
import gzip
import json
from pathlib import Path
import statistics


def timestamp(value):
    return datetime.fromisoformat(value.replace('Z', '+00:00')).timestamp()


def percentile(values, fraction):
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered)-1)*fraction
    lower = int(position)
    upper = min(lower+1, len(ordered)-1)
    return ordered[lower] + (ordered[upper]-ordered[lower])*(position-lower)


def summarize(rows, window):
    start, end = map(timestamp, window)
    samples = [r for r in rows if r['type'] == 'sample']
    within = [r for r in samples if start <= timestamp(r['at']) <= end]
    errors = [r for r in rows if r['type'] == 'error' and start <= timestamp(r['at']) <= end]
    before = [r for r in samples if timestamp(r['at']) <= start]
    after = [r for r in samples if timestamp(r['at']) >= end]
    nearby = ([before[-1]] if before else []) + within + ([after[0]] if after else [])
    times = sorted(set(timestamp(r['at']) for r in nearby))
    gaps = [b-a for a,b in zip(times,times[1:])]
    probes = [r['probe_ms'] for r in within]
    return {
        'window_start':window[0], 'window_end':window[1],
        'samples':len(within), 'error_samples':len(errors),
        'violating_samples':sum(bool(r['violations']) for r in within),
        'sample_before_start':bool(before), 'sample_after_end':bool(after),
        'max_sample_gap_seconds':max(gaps,default=None),
        'observed_maxima':{k:max((r[k] for r in within),default=None) for k in
             ('active_builds','preparing_solving_builds','finishing_builds','terminal_cleanup_builds')},
        'probe_ms':{'mean':statistics.mean(probes) if probes else None,
                    'p95':percentile(probes,.95),'max':max(probes,default=None)},
        'at_least_five_owned_samples':sum(r['active_builds']>=5 for r in within),
        'six_owned_samples':sum(r['active_builds']==6 for r in within),
    }


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--input', type=Path, required=True)
    parser.add_argument('--windows', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args=parser.parse_args()
    windows=json.loads(args.windows.read_text())
    result={'schema_version':1,'sample_interval_seconds':2,
      'bounds':{'active_builds':6,'preparing_solving_builds':4,'finishing_builds':2},
      'interpretation':'Read-only samples establish observed ownership and coverage; intervals between samples are not a continuous proof. Atomic admission tests independently check bounds.',
      'nodes':[]}
    for path in sorted(args.input.glob('*.jsonl.gz')):
        with gzip.open(path, 'rt') as f:
            rows=[json.loads(line) for line in f]
        metadata=[r for r in rows if r['type']=='metadata']
        assert len(metadata)==1 and metadata[0]['read_only'] and metadata[0]['indexed']
        samples=[r for r in rows if r['type']=='sample']
        result['nodes'].append({'job_id':metadata[0]['job_id'],'source':path.name,
            'first_sample':samples[0]['at'],'last_sample':samples[-1]['at'],
            'total_samples':len(samples),'total_error_samples':sum(r['type']=='error' for r in rows),
            'total_violating_samples':sum(bool(r['violations']) for r in samples),
            'phases':{name:summarize(rows,window) for name,window in windows.items()}})
    assert len(result['nodes'])==4
    result['all_phase_checks_pass']=all(
        phase['samples']>0 and phase['error_samples']==0 and phase['violating_samples']==0
        and phase['sample_before_start'] and phase['sample_after_end']
        and phase['max_sample_gap_seconds']<=3
        for node in result['nodes'] for phase in node['phases'].values())
    args.output.write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps({'nodes':len(result['nodes']),'all_phase_checks_pass':result['all_phase_checks_pass']}))


if __name__=='__main__':
    main()
