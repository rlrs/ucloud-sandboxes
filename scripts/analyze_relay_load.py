#!/usr/bin/env python3
"""Join a load report to retained Tempo spans and relay timestamps (read only).

Guest/driver/worker wall-clock differences require synchronized clocks. Keep
negative intervals visible; never silently clamp them or sum phase percentiles.
No payload, credential, lease token or connection string is exported.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import json
from pathlib import Path
import statistics
import time
import urllib.parse
import urllib.request


def attributes(items):
    return {v['key']: next(iter(v['value'].values())) for v in items}


def summarize(values):
    values = sorted(v for v in values if v is not None)
    return {'count': len(values), 'mean': statistics.mean(values) if values else None,
            'p95': values[int(.95 * (len(values) - 1))] if values else None,
            'max': max(values) if values else None,
            'negative_count': sum(v < 0 for v in values)}


def phases(cycle, wake, request):
    def ms(end, start):
        return None if end is None or start is None else (end - start) * 1000
    start, end = (wake['start'], wake['end']) if wake else (None, None)
    ready = cycle['response_ready_unix']
    submitted = ready + cycle['response_ready_to_submit_seconds']
    committed = request.get('completed_at')
    released = request.get('delivery_released_at')
    response_ready = request.get('response_wait_finished_at')
    received = cycle.get('guest_response_received_unix')
    tool = cycle.get('guest_tool_finished_unix')
    receipt = cycle.get('guest_receipt_started_unix')
    enqueued = cycle.get('observer_request_created_unix')
    observed = cycle['guest_continuation_observed_unix']
    return {
        'ready_to_submit_ms': ms(submitted, ready),
        'submit_to_durable_commit_ms': ms(committed, submitted),
        'commit_to_worker_start_ms': ms(start, committed),
        'worker_wake_ms': ms(end, start),
        'commit_to_release_without_wake_span_ms': ms(released, committed) if wake is None else None,
        'worker_end_to_delivery_release_ms': ms(released, end),
        'release_to_response_ready_ms': ms(response_ready, released),
        'response_ready_to_guest_receive_ms': ms(received, response_ready),
        'guest_tool_ms': ms(tool, received),
        'guest_receipt_prepare_ms': ms(receipt, tool),
        'receipt_transport_ms': ms(enqueued, receipt),
        'observer_queue_and_poll_ms': ms(observed, enqueued),
        'observed_to_exec_ms': cycle['post_continuation_exec_seconds'] * 1000,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('report', type=Path)
    parser.add_argument('--detail-traces', type=int, default=32, help='Fetch detailed events for the slowest worker wakes; durations cover all indexed spans')
    parser.add_argument('--reuse-traces', action='store_true', help='Reuse previously fetched wake spans; coverage remains explicit')
    parser.add_argument('--tempo-url', default='http://127.0.0.1:3200')
    parser.add_argument('--deployment-config', type=Path, required=True)
    args = parser.parse_args()
    # An unready ingester can return an empty search rather than an error.
    with urllib.request.urlopen(args.tempo_url + '/ready', timeout=10) as response:
        response.read()
    report = json.loads(args.report.read_text())
    start = int(datetime.fromisoformat(report['started_at']).timestamp())
    end = int(datetime.fromisoformat(report['finished_at']).timestamp()) + 10
    wake_metadata = {}
    response_ends = {}
    for t in range(start, end, 10):
        query = urllib.parse.urlencode(dict(q='{ name = "sandbox.wake" } | select(span.sandbox.id)', start=t,
                                            end=min(t + 10, end), limit=1000, spss=100))
        with urllib.request.urlopen(args.tempo_url + '/api/search?' + query, timeout=60) as response:
            found = json.load(response).get('traces', [])
        if len(found) >= 1000:
            raise RuntimeError('trace search truncated; shorten search windows')
        for row in found:
            for span_set in row.get('spanSets', [row.get('spanSet', {})]):
                if span_set.get('matched', 0) > len(span_set.get('spans', [])):
                    raise RuntimeError('wake span set truncated')
                for span in span_set.get('spans', []):
                    sid = attributes(span.get('attributes', [])).get('sandbox.id')
                    if sid:
                        wake_metadata[(sid, int(span['startTimeUnixNano']))] = {
                            'start': int(span['startTimeUnixNano']) / 1e9,
                            'end': (int(span['startTimeUnixNano']) + int(span['durationNanos'])) / 1e9,
                            'trace_id': row['traceID'], 'timings': {}, 'retention': {},
                        }
        query = urllib.parse.urlencode(dict(
            q='{ name = "relay.wait_for_worker" && span.relay.outcome = "completed" } | select(span.relay.request.id)',
            start=t, end=min(t + 10, end), limit=1000, spss=100))
        with urllib.request.urlopen(args.tempo_url + '/api/search?' + query, timeout=60) as response:
            found = json.load(response).get('traces', [])
        if len(found) >= 1000:
            raise RuntimeError('response trace search truncated')
        for row in found:
            for span_set in row.get('spanSets', [row.get('spanSet', {})]):
                for span in span_set.get('spans', []):
                    request_id = attributes(span.get('attributes', [])).get('relay.request.id')
                    if request_id:
                        response_ends[request_id] = max(response_ends.get(request_id, 0),
                            (int(span['startTimeUnixNano']) + int(span['durationNanos'])) / 1e9)


    cache = args.report.with_suffix('.trace-cache')
    cache.mkdir(exist_ok=True)
    fetch_errors = []

    def fetch(trace_id):
        target = cache / (trace_id + '.json')
        if target.exists():
            return json.loads(target.read_text())
        for attempt in range(3):
            try:
                with urllib.request.urlopen(args.tempo_url + '/api/traces/' + trace_id, timeout=20) as response:
                    trace = json.load(response)
                target.write_text(json.dumps(trace))
                return trace
            except (OSError, ValueError) as exc:
                if attempt == 2:
                    fetch_errors.append({'trace_id': trace_id, 'error': type(exc).__name__})
                    return {}
                time.sleep(.2 * (attempt + 1))
    selected = sorted(wake_metadata.values(), key=lambda w: w['end'] - w['start'], reverse=True)[:max(0, args.detail_traces)]
    trace_ids = {w['trace_id'] for w in selected}
    if args.reuse_traces:
        traces = json.loads(args.report.with_suffix('.traces.json').read_text())
    else:
        with ThreadPoolExecutor(4) as pool:
            traces = list(pool.map(fetch, sorted(trace_ids)))
    # Full traces remain beside the report for reproducibility; curated output
    # contains only timings and stable sandbox/request identities.
    args.report.with_suffix('.traces.json').write_text(json.dumps(traces))
    for trace in traces:
        for batch in trace.get('batches', []):
            for scope in batch.get('scopeSpans', []):
                for span in scope.get('spans', []):
                    if span['name'] != 'sandbox.wake':
                        continue
                    attrs = attributes(span.get('attributes', []))
                    key = (attrs.get('sandbox.id'), int(span['startTimeUnixNano']))
                    if key not in wake_metadata:
                        continue
                    events = {e['name']: attributes(e.get('attributes', [])) for e in span.get('events', [])}
                    wake_metadata[key].update(
                        timings=events.get('sandbox.wake.timings', {}),
                        retention={k: v for k, v in events.items() if k.startswith('memory.retain.')})
    wakes = {}
    for (sid, _), wake in wake_metadata.items():
        wakes.setdefault(sid, []).append(wake)
    import psycopg
    from psycopg import sql
    from psycopg.rows import dict_row
    cfg = json.loads(args.deployment_config.read_text())
    pg = cfg['relay_postgres']
    ids = [c['request_id'] for c in report['cycles']]
    with psycopg.connect(Path(pg['dsn_file']).read_text().strip(), row_factory=dict_row) as conn:
        conn.execute('SET TRANSACTION READ ONLY')
        requests = conn.execute(sql.SQL('SELECT request_id,completed_at,delivery_released_at FROM {}.relay_requests WHERE deployment_id=%s AND request_id=ANY(%s)').format(sql.Identifier(pg['schema'])), (cfg['deployment_id'], ids)).fetchall()
        lifecycle = conn.execute(sql.SQL('SELECT request_id,action,attempts,done,last_error FROM {}.relay_lifecycle WHERE deployment_id=%s AND request_id=ANY(%s)').format(sql.Identifier(pg['schema'])), (cfg['deployment_id'], ids)).fetchall()
    requests = {r['request_id']: {**r, 'response_wait_finished_at': response_ends.get(r['request_id'])} for r in requests}
    rows = []
    for cycle in report['cycles']:
        candidates = [w for w in wakes.get(cycle['sandbox_id'], [])
                      if cycle['response_ready_unix'] - .1 <= w['start'] <= cycle['guest_continuation_observed_unix'] + .1]
        wake = candidates[0] if len(candidates) == 1 else None
        rows.append({'sandbox_id': cycle['sandbox_id'], 'request_id': cycle['request_id'],
                     'cycle': cycle['cycle'], 'wake_matches': len(candidates),
                     'continuation_ms': cycle['response_ready_to_guest_continuation_seconds'] * 1000,
                     'phases': phases(cycle, wake, requests.get(cycle['request_id'], {})),
                     'worker': wake})
    measured = [r for r in rows if r['cycle'] >= report['configuration']['warmup_cycles']]
    result = {'detail_selection': 'slowest worker wakes; do not treat their phase timings as population percentiles',
              'detailed_wakes': sum(bool(w['timings']) for w in wake_metadata.values()), 'trace_fetch_errors': fetch_errors, 'cycles': len(rows), 'matched_wakes': sum(r['wake_matches'] == 1 for r in rows),
              'relay_response_spans_found': sum(i in response_ends for i in ids), 'relay_requests_found': len(requests), 'clock_warning': 'Cross-host wall timestamps; inspect negative intervals and clock synchronization before interpreting small differences.',
              'summary_ms': {k: summarize(r['phases'][k] for r in measured) for k in measured[0]['phases']} if measured else {},
              'lifecycle': lifecycle, 'rows': sorted(rows, key=lambda r: -r['continuation_ms'])}
    args.report.with_suffix('.analysis.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({k: v for k, v in result.items() if k not in ('rows', 'lifecycle')}, indent=2))


if __name__ == '__main__':
    main()
