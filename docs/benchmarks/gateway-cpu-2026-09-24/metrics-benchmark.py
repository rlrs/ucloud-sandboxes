"""Compare buffered metrics encoding work; excludes SQLite and queue locks."""
import json
from statistics import median
from time import process_time

from ucloud_sandboxes.metrics import MetricEvent, _EncodedMetricEvent, _metric_event_bytes


def previous(event):
    payload = json.dumps(event.to_dict(), sort_keys=True, separators=(',', ':'))
    size = len(payload.encode('utf-8'))
    detached = MetricEvent(**json.loads(payload))
    data = json.dumps(detached.data, sort_keys=True, separators=(',', ':'))
    return data, _metric_event_bytes(detached), size


def current(event):
    encoded = _EncodedMetricEvent.from_event(event)
    return encoded.data_json, encoded.payload_bytes, encoded.queue_bytes


for fields in (8, 32, 128):
    event = MetricEvent('2026-09-24T16:00:00+00:00', 'gateway.request', {
        f'field_{i}': {'duration_ms': i * 1.2, 'status': 'ok', 'count': i}
        for i in range(fields)
    })
    assert previous(event) == current(event)
    samples = {previous: [], current: []}
    for _ in range(7):
        for fn in (previous, current):
            start = process_time()
            for _ in range(2000):
                fn(event)
            samples[fn].append((process_time() - start) / 2000 * 1e6)
    before, after = median(samples[previous]), median(samples[current])
    print(json.dumps({'fields': fields, 'before_cpu_us': before,
                      'after_cpu_us': after, 'reduction_percent': 100 * (1 - after / before)}))
